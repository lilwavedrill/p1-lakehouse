# Практическая 1 - локальный Lakehouse, медальон и бенчмарк кодеков

Кривонос Никита Николаевич, БИВТ-24-АЭ-1. Вариант 4 - TPC-H Q16 "Parts/Supplier Relationship", масштаб sf10.

Стек: MinIO -> Lakekeeper (Iceberg REST) -> Trino, плюс Spark для п.6.2. Все в Docker (у меня через colima на маке, 8 ГБ).

Сам отчет - в [отчет.md](отчет.md).

## Что где лежит

| | |
|---|---|
| `stand/up.sh` | поднять весь стек одной командой |
| `stand/catalog/` | каталоги Trino: bronze, silver, gold, bench, tpch |
| `stand/conf/` | jvm.config и config.properties для Trino (урезал память) |
| `sql/medallion.sql` | все слои bronze -> silver -> gold (в silver чистка, нормализация type, флаг жалоб) + проверка gold против оригинального Q16 |
| `scripts/p1_medallion.py` | то же, что medallion.sql, но еще собирает строки/МБ/время для отчета |
| `scripts/p1_bench_bronze.py` | п.5: Bronze на GZIP и на ZSTD, размер и время Q16 |
| `scripts/bench8.py` | бенчмарк 8 вариантов формат x кодек на lineitem sf10 |
| `scripts/p1_evolution.sh` | п.6.1: schema evolution и откат из снапшота |
| `scripts/p1_spark_bronze.py` | п.6.2: Spark читает Bronze через Lakekeeper |
| `results/` | что получилось у меня |

## Как запустить

```bash
cd stand && ./up.sh
docker exec -i trino trino < ../sql/medallion.sql
```

`medallion.sql` можно запускать повторно, таблицы пересоздаются.

Скрипты для Trino (`p1_medallion.py`, `p1_bench_bronze.py`, `bench8.py`) - обычный python3, без библиотек, ходят в Trino по HTTP:
`python3 scripts/p1_medallion.py sf10`.

`p1_spark_bronze.py` нужен pyspark, поэтому он запускается внутри контейнера со Spark - команда в отчете, п.6.2.
Trino на время запуска Spark лучше остановить, на 8 ГБ они вдвоем не помещаются.
