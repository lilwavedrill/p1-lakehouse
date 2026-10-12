#!/bin/bash
# Поднимает стек MinIO -> Postgres -> Lakekeeper -> Trino.
# Запускать из папки stand/. Docker на маке у меня через colima (Docker Desktop не ставил).
#
# Отличия от гайда, которые пришлось сделать:
#  - quay.io/minio/minio отдаёт 401, поэтому MinIO из bitnamilegacy/minio
#  - на маке уже стоит свой postgres 14 на 5432, контейнеры лезли в него
#    и миграции lakekeeper падали -> контейнерный postgres на 5433
#  - trino по умолчанию берёт 80% памяти и его убивало, поэтому свой jvm.config (2 ГБ)
set -e
cd "$(dirname "$0")"

# MinIO - объектное хранилище, сюда падают parquet-файлы
docker run -d --name lakehouse-minio \
  --add-host host.docker.internal:host-gateway \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/bitnami/minio/data \
  bitnamilegacy/minio:latest

# Postgres - тут lakekeeper хранит свои метаданные (порт 5433, см. выше)
docker run -d --name lakehouse-pg \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_PASSWORD=postgres -p 5433:5432 \
  -v pg-data:/var/lib/postgresql/data postgres:17

until docker exec lakehouse-pg pg_isready | grep -q accepting; do sleep 2; done

PG='postgresql://postgres:postgres@host.docker.internal:5433/postgres'

# Lakekeeper - сначала миграции, потом сам сервис (Iceberg REST Catalog)
docker run --rm --add-host host.docker.internal:host-gateway \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG" \
  quay.io/lakekeeper/catalog:latest migrate

docker run -d --name lakekeeper --add-host host.docker.internal:host-gateway \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG" \
  -p 8181:8181 quay.io/lakekeeper/catalog:latest serve

until curl -sf http://localhost:8181/health >/dev/null; do sleep 2; done
curl -s -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" -d '{"accept-terms-of-use": true}' || true

# бакеты: bench для бенчмарка, bronze/silver/gold для медальона
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway pgsty/mc:latest \
  -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && \
      for b in bench bronze silver gold; do mc mb --ignore-existing m/\$b; done"

# по warehouse в lakekeeper на каждый бакет
for b in bench bronze silver gold; do
  curl -s -X POST http://localhost:8181/management/v1/warehouse \
    -H "Content-Type: application/json" -d "{
    \"warehouse-name\": \"$b\",
    \"project-id\": \"00000000-0000-0000-0000-000000000000\",
    \"storage-profile\": {\"type\":\"s3\",\"bucket\":\"$b\",\"key-prefix\":\"\",
      \"endpoint\":\"http://host.docker.internal:9000\",\"region\":\"us-east-1\",
      \"path-style-access\":true,\"flavor\":\"s3-compat\",\"sts-enabled\":false},
    \"storage-credential\": {\"type\":\"s3\",\"credential-type\":\"access-key\",
      \"access-key-id\":\"minioadmin\",\"secret-access-key\":\"minioadmin\"}}"
  echo
done

# Trino - SQL-движок, каталоги берёт из папки catalog/
docker run -d --name trino --add-host host.docker.internal:host-gateway \
  -m 2900m -p 8080:8080 \
  -v "$(pwd)/catalog:/etc/trino/catalog" \
  -v "$(pwd)/conf/jvm.config:/etc/trino/jvm.config" \
  -v "$(pwd)/conf/config.properties:/etc/trino/config.properties" \
  trinodb/trino:476

echo "ждём trino..."
until curl -sf http://localhost:8080/v1/info | grep -q '"starting":false'; do sleep 5; done
docker exec trino trino --execute "SHOW CATALOGS"
