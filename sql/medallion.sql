-- Практическая 1, вариант 4: TPC-H Q16 "Parts/Supplier Relationship"
-- Медальон bronze -> silver -> gold, масштаб sf10.
-- Можно запускать повторно: перед каждой таблицей DROP IF EXISTS.
-- Запуск: docker exec -i trino trino < sql/medallion.sql

CREATE SCHEMA IF NOT EXISTS bronze.lakehouse;
CREATE SCHEMA IF NOT EXISTS silver.lakehouse;
CREATE SCHEMA IF NOT EXISTS gold.lakehouse;

-- ===== BRONZE: как пришло + когда и откуда загрузили. Parquet + GZIP =====
SET SESSION bronze.compression_codec = 'GZIP';

DROP TABLE IF EXISTS bronze.lakehouse.raw_part;
CREATE TABLE bronze.lakehouse.raw_part WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.part;

DROP TABLE IF EXISTS bronze.lakehouse.raw_partsupp;
CREATE TABLE bronze.lakehouse.raw_partsupp WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.partsupp;

DROP TABLE IF EXISTS bronze.lakehouse.raw_supplier;
CREATE TABLE bronze.lakehouse.raw_supplier WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.supplier;

-- ===== SILVER: чистка, нормализация, обогащение. Parquet + ZSTD =====
SET SESSION silver.compression_codec = 'ZSTD';

-- обогащение: из длинного comment делаем флаг "есть жалобы"
-- (строки не выкидываем, а помечаем)
DROP TABLE IF EXISTS silver.lakehouse.dim_supplier;
CREATE TABLE silver.lakehouse.dim_supplier WITH (format = 'PARQUET') AS
SELECT suppkey,
       name,
       nationkey,
       comment LIKE '%Customer%Complaints%' AS has_complaint
FROM bronze.lakehouse.raw_supplier;

-- нормализация: в type лежат три значения в одной строке ("STANDARD PLATED TIN"),
-- раскладываем на класс / обработку / материал. Все 2 млн значений из 3 слов, проверял.
-- очистка: убираем поставщиков с жалобами и лишние колонки
DROP TABLE IF EXISTS silver.lakehouse.fct_part_supply;
CREATE TABLE silver.lakehouse.fct_part_supply WITH (format = 'PARQUET') AS
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
WHERE s.has_complaint = false;

-- ===== GOLD: витрина под запрос. Parquet + SNAPPY =====
SET SESSION gold.compression_codec = 'SNAPPY';

-- фильтр "type NOT LIKE 'MEDIUM POLISHED%'" из Q16 теперь по отдельным колонкам,
-- а type собираем обратно для ответа
DROP TABLE IF EXISTS gold.lakehouse.mart_part_suppliers;
CREATE TABLE gold.lakehouse.mart_part_suppliers WITH (format = 'PARQUET') AS
SELECT brand,
       type_class || ' ' || type_finish || ' ' || type_material AS type,
       size,
       count(DISTINCT suppkey) AS supplier_cnt
FROM silver.lakehouse.fct_part_supply
WHERE brand <> 'Brand#45'
  AND NOT (type_class = 'MEDIUM' AND type_finish = 'POLISHED')
  AND size IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY brand, type_class, type_finish, type_material, size;

-- ответ на вопрос варианта (первые 10)
SELECT * FROM gold.lakehouse.mart_part_suppliers
ORDER BY supplier_cnt DESC, brand, type, size
LIMIT 10;

-- ===== проверка: gold совпадает с оригинальным Q16 по сырым данным =====
-- должно быть: строк поровну, 0 и 0 расхождений, суммы равны
WITH orig AS (
  SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
  FROM tpch.sf10.partsupp ps JOIN tpch.sf10.part p ON p.partkey = ps.partkey
  WHERE p.brand <> 'Brand#45' AND p.type NOT LIKE 'MEDIUM POLISHED%'
    AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.suppkey NOT IN (SELECT suppkey FROM tpch.sf10.supplier
                           WHERE comment LIKE '%Customer%Complaints%')
  GROUP BY p.brand, p.type, p.size
),
gold AS (SELECT brand, type, size, supplier_cnt FROM gold.lakehouse.mart_part_suppliers)
SELECT (SELECT count(*) FROM orig) AS rows_orig,
       (SELECT count(*) FROM gold) AS rows_gold,
       (SELECT count(*) FROM (SELECT * FROM orig EXCEPT SELECT * FROM gold)) AS only_in_orig,
       (SELECT count(*) FROM (SELECT * FROM gold EXCEPT SELECT * FROM orig)) AS only_in_gold,
       (SELECT sum(supplier_cnt) FROM orig) AS sum_orig,
       (SELECT sum(supplier_cnt) FROM gold) AS sum_gold;
