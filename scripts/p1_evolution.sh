#!/bin/bash
# Практическая 1, п.6.1: эволюция схемы на Silver и восстановление из снапшота (time travel).
# Таблица: silver.lakehouse.dim_supplier

T="silver.lakehouse.dim_supplier"
q() { docker exec trino trino --output-format ALIGNED --execute "$1" 2>/dev/null; }
v() { docker exec trino trino --execute "$1" 2>/dev/null | tr -d '"'; }

echo "### 0. Исходная схема и данные"
q "DESCRIBE $T"
q "SELECT suppkey, name, has_complaint FROM $T ORDER BY suppkey LIMIT 3"

echo "### 1. ADD COLUMN - добавили колонку region"
q "ALTER TABLE $T ADD COLUMN region varchar"
q "SELECT suppkey, name, region FROM $T ORDER BY suppkey LIMIT 3"
echo "-> у старых строк region = NULL, файлы не переписывались"
q "SELECT count(*) AS files_now FROM \"silver\".\"lakehouse\".\"dim_supplier\$files\""

echo "### 2. RENAME COLUMN - name -> supplier_name"
q "ALTER TABLE $T RENAME COLUMN name TO supplier_name"
q "SELECT suppkey, supplier_name FROM $T ORDER BY suppkey LIMIT 3"
echo "-> старые файлы читаются под новым именем: Iceberg хранит колонки по id, а не по имени"

echo "### 3. Снапшоты до аварии"
q "SELECT snapshot_id, operation, committed_at FROM \"silver\".\"lakehouse\".\"dim_supplier\$snapshots\" ORDER BY committed_at"
GOOD=$(v "SELECT snapshot_id FROM \"silver\".\"lakehouse\".\"dim_supplier\$snapshots\" ORDER BY committed_at DESC LIMIT 1")
echo "запомнили хороший снапшот: $GOOD"
q "SELECT count(*) AS rows_before FROM $T"

echo "### 4. Авария: случайно удалили всех поставщиков без жалоб"
q "DELETE FROM $T WHERE has_complaint = false"
q "SELECT count(*) AS rows_after_delete FROM $T"

echo "### 5. Time travel - смотрим таблицу на момент до удаления"
q "SELECT count(*) AS rows_in_old_snapshot FROM $T FOR VERSION AS OF $GOOD"

echo "### 6. Восстановление: откат таблицы на хороший снапшот"
q "ALTER TABLE $T EXECUTE rollback_to_snapshot($GOOD)" || true
q "SELECT count(*) AS rows_after_rollback FROM $T"
q "SELECT snapshot_id, operation, committed_at FROM \"silver\".\"lakehouse\".\"dim_supplier\$snapshots\" ORDER BY committed_at"
