#!/usr/bin/env python3
# Практическая 1, п.6.2: Spark подключается к lakehouse и работает с Bronze.
# Spark ходит в тот же каталог Lakekeeper (warehouse bronze) и читает parquet из MinIO
# напрямую - Trino ему для этого не нужен.

import time

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("p1-bronze")
    # в образе по умолчанию прописан чужой каталог demo -> делаем своим основным bronze
    .config("spark.sql.defaultCatalog", "bronze")
    .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.bronze.type", "rest")
    .config("spark.sql.catalog.bronze.uri", "http://host.docker.internal:8181/catalog")
    .config("spark.sql.catalog.bronze.warehouse", "bronze")
    .config("spark.sql.catalog.bronze.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.bronze.s3.endpoint", "http://host.docker.internal:9000")
    .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
    .config("spark.sql.catalog.bronze.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.bronze.s3.secret-access-key", "minioadmin")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("### 1. Какие таблицы Spark видит в Bronze")
spark.sql("SHOW TABLES IN bronze.lakehouse").show(truncate=False)

print("### 2. Сколько строк в каждой таблице")
for t in ("raw_part", "raw_partsupp", "raw_supplier"):
    n = spark.table(f"bronze.lakehouse.{t}").count()
    print(f"  {t}: {n:,}")

print("\n### 3. Схема raw_supplier (служебные поля loaded_at и source_system на месте)")
spark.table("bronze.lakehouse.raw_supplier").printSchema()

print("### 4. Пример строк")
spark.sql("""
SELECT suppkey, name, substr(comment, 1, 40) AS comment_start, loaded_at, source_system
FROM bronze.lakehouse.raw_supplier
ORDER BY suppkey LIMIT 3
""").show(truncate=False)

print("### 5. Запрос варианта 4 (Q16) в Spark прямо по Bronze")
t0 = time.time()
res = spark.sql("""
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
""")
rows = res.collect()
print(f"  строк в ответе: {len(rows)}, время: {time.time() - t0:.2f} с")
print(f"  сумма supplier_cnt: {sum(r['supplier_cnt'] for r in rows)}")
for r in rows[:5]:
    print(f"  {r['brand']:10s} {r['type']:28s} {r['size']:>3} -> {r['supplier_cnt']}")

spark.stop()
