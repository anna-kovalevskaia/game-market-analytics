---
description: Analytics Engineer project context for game-market-analytics
---

You are an Analytics Engineer assistant for the game-market-analytics project.

## Tech Stack
- ClickHouse (OLAP, local DWH, self-hosted)
- dbt-core + dbt-clickhouse adapter
- Apache Airflow 3.x, LocalExecutor, TaskFlow API
- Astronomer Cosmos, VIRTUALENV mode (/opt/airflow/dbt_venv)
- PostgreSQL (Airflow metadata only)
- Python 3.10

## Repository Layout
- infra/ — docker-compose, Dockerfiles, ClickHouse config
- dags/ — Airflow DAGs; shared logic in dags_utils/
- dags_utils/ -  dags logic (sources, operations, checks, utils)
- dbt/ — dbt project (models, macros, tests)

## Data Layers
- tmp: temporary sources for data from APIs
- raw: raw data from collected from tmp
- staging: deduplicated  in type of views data from raw
- core: business logic, joins, cleaning
- marts: aggregated, dashboard-ready tables

## Data Sources
Steam Web API, SteamSpy, IGDB API, Twitch API.

## Rules
- ClickHouse-compatible SQL only; never suggest PostgreSQL solutions for ClickHouse.
- snake_case naming everywhere.
- One dbt model per file; every model documented with description + unique/not-null tests on primary keys.
- Airflow DAGs use TaskFlow API.
- All transformations live in dbt — no raw SQL inside Airflow tasks.
- Python: type hints required.
- ClickHouse ports bind to 127.0.0.1 locally, not 0.0.0.0.
- YAML anchors in docker-compose merge map keys only, not lists — a service's own volumes: fully replaces the anchor's.

Data quality / ingestion rules (schema drift, anomaly detection, staged-buffer-then-swap, dbt test placement) live in data-quality.instructions.md — check it before writing DAG or loading logic.