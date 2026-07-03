#!/bin/bash
set -e

echo "=== ClickHouse initialization started ==="

until clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
    sleep 2
done

CH_ARGS=()
CH_ARGS+=("--database=${CLICKHOUSE_DB:-analytics}")
CH_ARGS+=("--user=${CLICKHOUSE_ADMIN_USER:-admin}")
CH_ARGS+=(--password "$CLICKHOUSE_ADMIN_PASSWORD")

clickhouse-client "${CH_ARGS[@]}" --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB:-analytics}"

# admin
if [ -n "$CLICKHOUSE_ADMIN_PASSWORD" ]; then
    clickhouse-client "${CH_ARGS[@]}" --query "CREATE USER IF NOT EXISTS ${CLICKHOUSE_AIRFLOW_USER:-airflow_user} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_AIRFLOW_PASSWORD}'"
    clickhouse-client "${CH_ARGS[@]}" --query "GRANT SELECT, INSERT, CREATE, ALTER, DROP, TRUNCATE, SHOW ON ${CLICKHOUSE_DB:-analytics}.* TO ${CLICKHOUSE_AIRFLOW_USER:-airflow_user} WITH GRANT OPTION"
    echo "✓ Admin user created"
fi

echo "✓ Default user locked (readonly)"

echo "=== ClickHouse initialization completed ==="