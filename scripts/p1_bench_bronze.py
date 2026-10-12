#!/usr/bin/env python3
# Практическая 1, п.5 отчёта: как кодек на Bronze влияет на размер и на время запроса Q16.
# Пересоздаём Bronze с каждым кодеком и гоняем свой запрос (холодный и тёплый прогон).

import json
import subprocess
import sys
import time
import urllib.request

TRINO = "http://localhost:8080"
SF = sys.argv[1] if len(sys.argv) > 1 else "sf10"
CODECS = ["GZIP", "ZSTD"]


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
                    return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError("Trino не поднялся")


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

    # холодный прогон - после сброса кэшей, как в п.9 гайда 3
    subprocess.run(["docker", "restart", "trino", "lakehouse-minio"],
                   capture_output=True, check=False)
    wait_trino()
    time.sleep(5)
    rows, cold = run(Q16)
    _, warm = run(Q16)
    print(f"  Q16: холодный {cold:.3f} с, тёплый {warm:.3f} с, строк в ответе {len(rows)}",
          flush=True)

    results.append({"кодек": codec, "мб": round(total_mb, 2),
                    "запись_с": round(total_write, 2),
                    "cold": round(cold, 3), "warm": round(warm, 3)})

print("\n=== Таблица для п.5 отчёта ===")
print(f"{'Сжатие на Bronze':18s} {'Размер Bronze, МБ':>18s} {'Запись, с':>11s} "
      f"{'Q16 холодный, с':>16s} {'Q16 тёплый, с':>14s}")
for r in results:
    print(f"{r['кодек']:18s} {r['мб']:18.2f} {r['запись_с']:11.2f} "
          f"{r['cold']:16.3f} {r['warm']:14.3f}")
