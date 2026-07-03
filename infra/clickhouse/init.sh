#!/bin/bash
set -e

echo "=== ClickHouse initialization started ==="

# Guard that all required environment variables are set
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

clickhouse-client "${CH_ARGS[@]}" \
    --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB:-analytics}"

clickhouse-client "${CH_ARGS[@]}" \
    --query "CREATE USER IF NOT EXISTS ${CLICKHOUSE_AIRFLOW_USER:-airflow_user} \
             IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_AIRFLOW_PASSWORD}'"

clickhouse-client "${CH_ARGS[@]}" \
    --query "GRANT SELECT, INSERT, CREATE, ALTER, DROP, TRUNCATE, SHOW \
             ON ${CLICKHOUSE_DB:-analytics}.* \
             TO ${CLICKHOUSE_AIRFLOW_USER:-airflow_user}"

echo "✓ airflow_user created and granted"
echo "=== ClickHouse initialization completed ==="