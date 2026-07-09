---
description: Data quality and ingestion rules for game-market-analytics
---

## Loading Pattern: Staged-Buffer-Then-Swap
DAGs never write directly to `raw`.
1. Extract to a temp table / Parquet in a sandbox schema.
2. Retry up to 3 times on failure.
3. Swap into `raw` only after a fully successful extraction.

Shared insert/test/swap logic lives in `dags/common/clickhouse_ops.py` — reuse it, don't duplicate per-source.

## Schema Drift Detection
Snapshot the schema of the first successful API pull. Compare every later pull against it. Mismatch → Airflow alert.

## Anomaly Detection
Baseline-driven, not static thresholds. Collect null-rate and metric-distribution data over the first ingestion week before setting any alert threshold. Never hardcode a cutoff value.

## dbt Test Placement — open decision
Two options, not yet chosen:
- subprocess call inside the shared Python function (simpler, less visible in Airflow UI)
- separate Cosmos/BashOperator task in the DAG graph (better observability)

Flag this tradeoff when relevant. Do not pick one silently.

## General
Implement data quality and idempotency logic during DAG/model creation, not as a later phase.