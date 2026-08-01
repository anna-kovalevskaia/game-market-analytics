#!/bin/bash
set -e

echo "=== ClickHouse initialization started ==="

# Guard that all required environment variables are set
if [ ! -f /etc/clickhouse-server/users.d/01-admin.xml ]; then
    echo "ERROR: 01-admin.xml not found. Run setup.md Step 6 first."
    exit 1
fi

if [ -z "$CLICKHOUSE_ADMIN_PASSWORD" ]; then
    echo "ERROR: CLICKHOUSE_ADMIN_PASSWORD is not set"
    exit 1
fi

if [ -z "$CLICKHOUSE_AIRFLOW_PASSWORD" ]; then
    echo "ERROR: CLICKHOUSE_AIRFLOW_PASSWORD is not set"
    exit 1
fi

until clickhouse-client \
    --user="${CLICKHOUSE_ADMIN_USER:-admin}" \
    --password="${CLICKHOUSE_ADMIN_PASSWORD}" \
    --query "SELECT 1" > /dev/null 2>&1; do
    echo "Waiting for ClickHouse to be ready..."
    sleep 2
done

CH_ARGS=(
    "--user=${CLICKHOUSE_ADMIN_USER:-admin}"
    "--password=${CLICKHOUSE_ADMIN_PASSWORD}"
)

# Warehouse layers — each is a separate ClickHouse database (ClickHouse has no
# schemas-within-a-database). Keep CLICKHOUSE_DB (dbt/airflow default) inside
# this list; edit here to add/remove layers.
CH_DATABASES=(meta raw raw_dq staging dbt_dq core marts)

AIRFLOW_USER="${CLICKHOUSE_AIRFLOW_USER:-airflow_user}"

clickhouse-client "${CH_ARGS[@]}" \
    --query "CREATE USER IF NOT EXISTS ${AIRFLOW_USER} \
             IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_AIRFLOW_PASSWORD}'"

for db in "${CH_DATABASES[@]}"; do
    clickhouse-client "${CH_ARGS[@]}" \
        --query "CREATE DATABASE IF NOT EXISTS ${db}"
    clickhouse-client "${CH_ARGS[@]}" \
        --query "GRANT SELECT, INSERT, CREATE, ALTER, DROP, TRUNCATE, SHOW \
                 ON ${db}.* TO ${AIRFLOW_USER}"
    echo "✓ database ${db} ready, ${AIRFLOW_USER} granted"
done

echo "=== ClickHouse initialization completed ==="