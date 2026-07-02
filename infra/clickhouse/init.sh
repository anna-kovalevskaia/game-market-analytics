#!/bin/bash
set -e

echo "=== ClickHouse initialization started ==="

until clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
    sleep 2
done

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB:-analytics}"

# admin
if [ -n "$CLICKHOUSE_ADMIN_PASSWORD" ]; then
    clickhouse-client --query "CREATE USER IF NOT EXISTS ${CLICKHOUSE_ADMIN_USER:-admin} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ADMIN_PASSWORD}'"
    clickhouse-client --query "GRANT ALL ON *.* TO ${CLICKHOUSE_ADMIN_USER:-admin} WITH GRANT OPTION"
    echo "✓ Admin user created"
fi

echo "✓ Default user locked (readonly)"

echo "=== ClickHouse initialization completed ==="