#!/usr/bin/env python3
# Практическая 1, п.5 отчёта: как кодек на Bronze влияет на размер, запись и на время запроса Q16.
# Пересоздаём Bronze с каждым кодеком и гоняем свой запрос по 3 раза холодным и 3 раза тёплым,
# в отчёт идёт медиана (одиночный замер у меня бывал с выбросами).
#
# Запуск: python3 scripts/p1_bench_bronze.py [sf10] [кодеки через запятую]
# Результат: results/p1_bronze_codecs_sf10.csv

import csv
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request

TRINO = "http://localhost:8080"
SF = sys.argv[1] if len(sys.argv) > 1 else "sf10"
CODECS = sys.argv[2].split(",") if len(sys.argv) > 2 else ["NONE", "SNAPPY", "GZIP", "ZSTD"]
RUNS = 3
OUT = os.path.join(os.path.dirname(__file__), "..", "results", f"p1_bronze_codecs_{SF}.csv")


def run(sql, session=None):
    headers = {"X-Trino-User": "trino", "Content-Type": "text/plain"}
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
            raise RuntimeError(resp["error"].get("message", "trino error"))
        nxt = resp.get("nextUri")
        if not nxt:
            break
        resp = json.load(urllib.request.urlopen(nxt))
    return rows, stats.get("elapsedTimeMillis", 0) / 1000.0


def wait_trino(timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(TRINO + "/v1/info", timeout=5) as r:
                if json.load(r).get("starting") is False:
                    break
        except Exception:
            pass
        time.sleep(5)
    # после рестарта trino какое-то время пишет "No nodes available".
    # SELECT 1 проходит раньше (ему не нужен воркер), поэтому проверяем запросом по данным
    while time.time() < deadline:
        try:
            run("SELECT count(*) FROM tpch.tiny.nation")
            return
        except Exception:
            time.sleep(3)
    raise RuntimeError("Trino не поднялся")


def run_retry(sql, tries=10):
    for i in range(tries):
        try:
            return run(sql)
        except RuntimeError as e:
            if "No nodes available" not in str(e) or i == tries - 1:
                raise
            time.sleep(3)


def cold_restart():
    # сбрасываем кэш: trino (метаданные) и minio (page cache), как в п.9 гайда 3
    subprocess.run(["docker", "restart", "trino", "lakehouse-minio"],
                   capture_output=True, check=False)
    wait_trino()


# Запрос варианта 4 прямо по Bronze - без Silver/Gold, чтобы мерить именно чтение сырого слоя.
Q16 = """
SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
FROM bronze.lakehouse.raw_partsupp ps
JOIN bronze.lakehouse.raw_part p ON p.partkey = ps.partkey
WHERE p.brand <> 'Brand#45'
  AND p.type NOT LIKE 'MEDIUM POLISHED%'
  AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps.suppkey NOT IN (
    SELECT suppkey FROM bronze.lakehouse.raw_supplier
    WHERE comment LIKE '%Customer%Complaints%'
  )
GROUP BY p.brand, p.type, p.size
ORDER BY supplier_cnt DESC, p.brand, p.type, p.size
"""

results = []
for codec in CODECS:
    print(f"\n=== Bronze на {codec} ===", flush=True)
    total_mb, total_write = 0.0, 0.0
    for t in ("part", "partsupp", "supplier"):
        run(f"DROP TABLE IF EXISTS bronze.lakehouse.raw_{t}")
        _, wt = run(
            f"CREATE TABLE bronze.lakehouse.raw_{t} WITH (format = 'PARQUET') AS "
            f"SELECT *, current_timestamp AS loaded_at, 'tpch.{SF}' AS source_system "
            f"FROM tpch.{SF}.{t}",
            session=f"bronze.compression_codec={codec}",
        )
        meta, _ = run(f'SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."raw_{t}$files"')
        mb = int(meta[0][0]) / 1024 ** 2
        total_mb += mb
        total_write += wt
        print(f"  raw_{t}: {mb:.2f} МБ, запись {wt:.2f} с", flush=True)

    colds, warms, nrows = [], [], None
    for i in range(RUNS):
        cold_restart()
        rows, c = run_retry(Q16)
        _, w = run_retry(Q16)
        colds.append(c)
        warms.append(w)
        nrows = len(rows)
        print(f"  прогон {i + 1}: холодный {c:.3f} с, повторный {w:.3f} с", flush=True)

    r = {"кодек": codec, "размер_мб": round(total_mb, 2), "запись_с": round(total_write, 2),
         "cold": [round(x, 3) for x in colds], "warm": [round(x, 3) for x in warms],
         "cold_мед": round(statistics.median(colds), 3),
         "warm_мед": round(statistics.median(warms), 3), "строк_ответа": nrows}
    results.append(r)
    print(f"  медиана: холодный {r['cold_мед']} с, повторный {r['warm_мед']} с, "
          f"строк в ответе {nrows}", flush=True)

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["кодек", "размер_bronze_мб", "запись_с",
                "cold_1", "cold_2", "cold_3", "warm_1", "warm_2", "warm_3",
                "cold_медиана", "warm_медиана", "строк_в_ответе"])
    for r in results:
        w.writerow([r["кодек"], r["размер_мб"], r["запись_с"], *r["cold"], *r["warm"],
                    r["cold_мед"], r["warm_мед"], r["строк_ответа"]])

print(f"\n=== п.5, медианы из {RUNS} прогонов (записано в {os.path.normpath(OUT)}) ===")
print(f"{'кодек':8s} {'МБ':>8s} {'запись,с':>9s} {'cold,с':>8s} {'warm,с':>8s}")
for r in results:
    print(f"{r['кодек']:8s} {r['размер_мб']:8.2f} {r['запись_с']:9.2f} "
          f"{r['cold_мед']:8.3f} {r['warm_мед']:8.3f}")
