#!/usr/bin/env python3
# Лаба 2, часть 1: бенчмарк 8 комбинаций "формат x кодек" по гайду 3.
# Пишем lineitem из tpch.sfN в bench.lakehouse, меряем размер, число файлов,
# время записи, метаданные и агрегацию (холодная/тёплая).
#
# Запуск: python3 bench8.py [sf10]

import csv
import json
import subprocess
import sys
import time
import urllib.request

TRINO = "http://localhost:8080"
SF = sys.argv[1] if len(sys.argv) > 1 else "sf10"
OUT = f"results_bench8_{SF}.csv"

COMBOS = [
    ("PARQUET", "NONE"),
    ("PARQUET", "SNAPPY"),
    ("PARQUET", "GZIP"),
    ("PARQUET", "ZSTD"),
    ("ORC", "NONE"),
    ("ORC", "SNAPPY"),
    ("ORC", "GZIP"),
    ("ORC", "ZSTD"),
]


def run(sql, session=None, catalog="bench", schema="lakehouse"):
    """Выполняет SQL в Trino, возвращает (строки, время выполнения в секундах)."""
    headers = {
        "X-Trino-User": "trino",
        "X-Trino-Catalog": catalog,
        "X-Trino-Schema": schema,
        "Content-Type": "text/plain",
    }
    if session:
        headers["X-Trino-Session"] = session

    req = urllib.request.Request(
        TRINO + "/v1/statement", data=sql.encode("utf-8"), headers=headers, method="POST"
    )
    resp = json.load(urllib.request.urlopen(req))

    rows, stats = [], resp.get("stats", {})
    while True:
        rows.extend(resp.get("data", []))
        stats = resp.get("stats", stats)
        if resp.get("error"):
            raise RuntimeError(resp["error"].get("message", "unknown trino error"))
        nxt = resp.get("nextUri")
        if not nxt:
            break
        for attempt in range(60):
            try:
                resp = json.load(urllib.request.urlopen(nxt))
                break
            except Exception:
                # Trino иногда рвёт соединение под нагрузкой - ждём и пробуем снова
                if attempt == 59:
                    raise
                time.sleep(2)

    return rows, stats.get("elapsedTimeMillis", 0) / 1000.0


def wait_trino(timeout=300):
    """Ждёт, пока Trino снова начнёт отвечать после перезапуска."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(TRINO + "/v1/info", timeout=5) as r:
                if json.load(r).get("starting") is False:
                    return True
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError("Trino не поднялся после перезапуска")


def agg_sql(table):
    # Запрос из п.7 гайда - TPC-H Q1 без фильтра, полный проход по таблице.
    return f"""
SELECT returnflag, linestatus,
       count(*)                            AS cnt,
       sum(quantity)                       AS sum_qty,
       sum(extendedprice)                  AS sum_price,
       sum(discount)                       AS sum_disc,
       sum(extendedprice * (1 - discount)) AS sum_charge
FROM bench.lakehouse.{table}
GROUP BY returnflag, linestatus
"""


results = {}
run("CREATE SCHEMA IF NOT EXISTS bench.lakehouse")

# --- Фаза 1: запись всех 8 таблиц ---
print(f"### Фаза 1: запись 8 таблиц из tpch.{SF}.lineitem\n", flush=True)
for fmt, codec in COMBOS:
    table = f"lineitem_{fmt.lower()}_{codec.lower()}"
    print(f"[{table}] пишу...", flush=True)
    try:
        run(f"DROP TABLE IF EXISTS bench.lakehouse.{table}")
        _, wt = run(
            f"CREATE TABLE bench.lakehouse.{table} WITH (format = '{fmt}') "
            f"AS SELECT * FROM tpch.{SF}.lineitem",
            session=f"bench.compression_codec={codec}",
        )
        meta, _ = run(
            f'SELECT count(*), sum(file_size_in_bytes) FROM bench.lakehouse."{table}$files"'
        )
        man, _ = run(
            f'SELECT count(*), sum(length) FROM bench.lakehouse."{table}$manifests"'
        )
        results[table] = {
            "формат": fmt,
            "кодек": codec,
            "размер_гб": round(int(meta[0][1]) / 1024 ** 3, 4),
            "файлов": int(meta[0][0]),
            "запись_с": round(wt, 3),
            "манифестов": int(man[0][0]),
            "метаданные_кб": round(int(man[0][1]) / 1024, 1),
        }
        r = results[table]
        print(f"  готово: {r['размер_гб']} ГБ, {r['файлов']} файлов, {r['запись_с']} с\n",
              flush=True)
    except Exception as e:
        print(f"  ОШИБКА: {e}\n", flush=True)
        results[table] = {"формат": fmt, "кодек": codec, "ошибка": str(e)[:200]}

# --- Фаза 2: агрегации, холодная и тёплая ---
print(f"\n### Фаза 2: агрегация (перед каждой таблицей сбрасываем кэш)\n", flush=True)
for fmt, codec in COMBOS:
    table = f"lineitem_{fmt.lower()}_{codec.lower()}"
    if "ошибка" in results.get(table, {}):
        continue
    # Холодный замер по п.9 гайда: перезапуск Trino (кэш метаданных)
    # и MinIO (page cache ОС), иначе первый прогон будет уже прогретым.
    subprocess.run(["docker", "restart", "trino", "lakehouse-minio"],
                   capture_output=True, check=False)
    wait_trino()
    time.sleep(5)
    try:
        _, cold = run(agg_sql(table))
        _, warm = run(agg_sql(table))
        results[table]["агрегация_cold_с"] = round(cold, 3)
        results[table]["агрегация_warm_с"] = round(warm, 3)
        print(f"[{table}] cold {cold:.3f} с / warm {warm:.3f} с", flush=True)
    except Exception as e:
        print(f"[{table}] ОШИБКА агрегации: {e}", flush=True)

# --- Итог ---
cols = ["формат", "кодек", "размер_гб", "файлов", "запись_с",
        "агрегация_cold_с", "агрегация_warm_с", "манифестов", "метаданные_кб"]
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["таблица"] + cols)
    for table, r in results.items():
        w.writerow([table] + [r.get(c, "") for c in cols])

print(f"\n### Итог ({SF}) - записано в {OUT}\n", flush=True)
head = f"{'вариант':24s} {'ГБ':>8s} {'файлов':>7s} {'запись,с':>10s} {'cold,с':>8s} {'warm,с':>8s}"
print(head)
print("-" * len(head))
for table, r in results.items():
    if "ошибка" in r:
        print(f"{table:24s}  ОШИБКА: {r['ошибка'][:60]}")
        continue
    print(f"{table:24s} {r['размер_гб']:8.4f} {r['файлов']:7d} {r['запись_с']:10.1f} "
          f"{r.get('агрегация_cold_с', 0):8.3f} {r.get('агрегация_warm_с', 0):8.3f}")
