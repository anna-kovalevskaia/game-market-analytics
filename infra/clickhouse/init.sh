#!/bin/bash
# ============================================================
# ClickHouse initialization script
# Runs automatically on first container start
# Creates users and grants permissions
# ============================================================

set -e

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS analytics;"

clickhouse-client --query "ALTER USER admin IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ADMIN_PASSWORD}';"

clickhouse-client --query "CREATE USER IF NOT EXISTS dbt_user IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_DBT_PASSWORD}';"

clickhouse-client --query "GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ON analytics.* TO dbt_user;"

echo "ClickHouse initialization completed successfully."