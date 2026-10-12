#!/usr/bin/env python3
# Практическая 1, вариант 4 (TPC-H Q16 "Parts/Supplier Relationship").
# Строит медальон Bronze -> Silver -> Gold и собирает цифры для отчёта:
# строки, вес в МБ, время создания каждой таблицы.
#
# Запуск: python3 scripts/p1_medallion.py [sf10]
# Сводку пишет в results/p1_layers_sf10.csv

import csv
import json
import os
import sys
import urllib.request

TRINO = "http://localhost:8080"
SF = sys.argv[1] if len(sys.argv) > 1 else "sf10"


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


# Кодек на слой - по занятию 7: холодный слой жмём сильно, горячий быстро.
LAYERS = {"bronze": "GZIP", "silver": "ZSTD", "gold": "SNAPPY"}

for layer in LAYERS:
    run(f"CREATE SCHEMA IF NOT EXISTS {layer}.lakehouse")

stats = []


def build(layer, table, select_sql):
    """Создаёт таблицу слоя и собирает статистику для отчёта."""
    codec = LAYERS[layer]
    full = f"{layer}.lakehouse.{table}"
    run(f"DROP TABLE IF EXISTS {full}")
    _, t = run(
        f"CREATE TABLE {full} WITH (format = 'PARQUET') AS {select_sql}",
        session=f"{layer}.compression_codec={codec}",
    )
    cnt, _ = run(f"SELECT count(*) FROM {full}")
    meta, _ = run(f'SELECT count(*), sum(file_size_in_bytes) FROM {layer}.lakehouse."{table}$files"')
    stats.append({
        "слой": layer, "таблица": table, "строк": int(cnt[0][0]),
        "файлов": int(meta[0][0]), "мб": round(int(meta[0][1]) / 1024 ** 2, 2),
        "кодек": codec, "сек": round(t, 2),
    })
    s = stats[-1]
    print(f"[{layer}] {table}: {s['строк']:,} строк, {s['мб']} МБ, {s['сек']} с", flush=True)


print(f"=== Bronze: сырые таблицы из tpch.{SF} ===", flush=True)
# Bronze = данные как пришли + служебные поля загрузки (занятие 4).
for t in ("part", "partsupp", "supplier"):
    build("bronze", f"raw_{t}",
          f"SELECT *, current_timestamp AS loaded_at, 'tpch.{SF}' AS source_system "
          f"FROM tpch.{SF}.{t}")

print(f"\n=== Silver: чистим и соединяем ===", flush=True)
# Обогащение: считаем признак жалобы отдельной колонкой, а не выкидываем строку молча.
build("silver", "dim_supplier", """
SELECT suppkey,
       name,
       nationkey,
       comment LIKE '%Customer%Complaints%' AS has_complaint
FROM bronze.lakehouse.raw_supplier
""")

# Факт "деталь-поставщик": только поставщики без жалоб, только нужные колонки.
# Нормализация: type ("STANDARD PLATED TIN") раскладываем на 3 колонки.
build("silver", "fct_part_supply", """
SELECT ps.partkey,
       ps.suppkey,
       p.brand,
       split_part(p.type, ' ', 1) AS type_class,
       split_part(p.type, ' ', 2) AS type_finish,
       split_part(p.type, ' ', 3) AS type_material,
       p.size
FROM bronze.lakehouse.raw_partsupp ps
JOIN bronze.lakehouse.raw_part p ON p.partkey = ps.partkey
JOIN silver.lakehouse.dim_supplier s ON s.suppkey = ps.suppkey
WHERE s.has_complaint = false
""")

print(f"\n=== Gold: витрина под Q16 ===", flush=True)
build("gold", "mart_part_suppliers", """
SELECT brand,
       type_class || ' ' || type_finish || ' ' || type_material AS type,
       size,
       count(DISTINCT suppkey) AS supplier_cnt
FROM silver.lakehouse.fct_part_supply
WHERE brand <> 'Brand#45'
  AND NOT (type_class = 'MEDIUM' AND type_finish = 'POLISHED')
  AND size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY brand, type_class, type_finish, type_material, size
""")

print("\n=== Проверка: Gold отвечает на запрос варианта 4 ===", flush=True)
top, t = run("""
SELECT brand, type, size, supplier_cnt
FROM gold.lakehouse.mart_part_suppliers
ORDER BY supplier_cnt DESC, brand, type, size
LIMIT 10
""")
print(f"(запрос к Gold: {t:.3f} с)")
for r in top:
    print(f"  {r[0]:10s} {r[1]:28s} {r[2]:>3} -> {r[3]}")

print("\n=== Сводка для отчёта (п.4) ===")
head = f"{'слой':8s} {'таблица':22s} {'строк':>12s} {'МБ':>9s} {'файлов':>7s} {'кодек':>7s} {'сек':>7s}"
print(head)
print("-" * len(head))
for s in stats:
    print(f"{s['слой']:8s} {s['таблица']:22s} {s['строк']:12,d} {s['мб']:9.2f} "
          f"{s['файлов']:7d} {s['кодек']:>7s} {s['сек']:7.2f}")

out = os.path.join(os.path.dirname(__file__), "..", "results", f"p1_layers_{SF}.csv")
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["слой", "таблица", "строк", "мб", "файлов", "кодек", "создание_с"])
    for s in stats:
        w.writerow([s["слой"], s["таблица"], s["строк"], s["мб"], s["файлов"], s["кодек"], s["сек"]])
print(f"\nзаписано в {os.path.normpath(out)}")
