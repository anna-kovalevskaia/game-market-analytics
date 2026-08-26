[🇷🇺 Читать на русском](README.ru.md)

# Game Market Analytics System (via Open-Source Data)

An analytical system for researching the gaming market based on open-source data. Built as an end-to-end data platform, from raw API ingestion to interactive dashboards updated daily. Work in progress: the ingestion and staging layers are running, the analytical layers are not built yet - see [Development Roadmap](#development-roadmap) for what is done and what is next.

## Contents

- [Goal](#goal)
- [Research Questions](#research-questions)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Development Roadmap](#development-roadmap)
- [Development Environment](#development-environment)
- [Project Structure](#project-structure)
- [Limitations](#limitations)


## Goal

Investigate historical data from the gaming industry and analyze how popularity metrics, genre characteristics, and media interest correlate with indirect commercial performance - across studios of different scales.


## Research Questions

- **Investment signals** - What are the indirect markers of commercial viability for different genres on the current market? What is the visibility threshold for games of different scale?
- **Market concentration** - How is user traffic distributed within popular genres? Are genres genuinely accessible to newcomers, or are their metrics inflated by 2-3 long-standing mega-hits?
- **Genre trends** - How has audience interest in key genres and tags shifted over the past 3 years?
- **Black swans** - Identifying cases that achieved high popularity despite general historical trends or minimal starting resources.
- **Media hype (Attention Decay Rate)** - Analyzing the relationship between Twitch viewing hours and Steam CCU dynamics, and measuring how quickly media hype decays.
- **Developer scale** - Studying release density and audience retention across studio categories: major publishers, mid-tier studios, and indie developers.


## Data Sources

### Phase 1 - MVP (in progress)

| Source | Status | What it provides | Why |
|---|---|---|---|
| Steam Store API | done | Catalog, game cards, prices, editions, discounts, user tags, review summaries | Official, stable, free |
| Steam Web API | done | Concurrent players per game | Official, stable, free |
| Twitch API | next | Viewing hours, stream engagement | Measures media weight of game titles |
| IGDB API | planned | Structured genre classification, critic ratings | Steam tags are unstructured; IGDB provides clean taxonomy |


## Tech Stack

| Tool | Role | Why this, not alternatives |
|---|---|---|
| Apache Airflow | Orchestration | Python-native DAGs with dependency management, retry logic, UI, and alerting. (It might be better to use Dagster or Prefect for a project like this) |
| ClickHouse | Data warehouse | Columnar OLAP database optimized for sub-second aggregations on append-only time-series data. PostgreSQL is OLTP and slow on analytical queries.|
| dbt | Transformation layer | Data-as-Code: Staging -> Core -> Marts, with auto-generated lineage graph, documentation, Jinja macros, and data quality tests. SQL models are readable and maintainable unlike raw ETL scripts. |
| Google Sheets | Cloud buffer | Lightweight, free cloud layer for publishing dashboard-ready marts from local DWH to the BI layer - no server required. Only pre-aggregated, dashboard-ready data is pushed to the cloud. |
| Tableau Public | Visualization | Connected to Google Sheets for automatic daily refresh of the public dashboard. |


## Architecture

```
  [Steam API]       [IGDB]       [Twitch API]
       |               |               |
       +---------------+---------------+
                       |
                  [Airflow DAGs]
                  (Python ETL, daily schedule,
                   retries, alerting)
                       |
              [ClickHouse - local DWH]
              (raw -> staging -> core -> marts)
                       |
                    [dbt]
              (transformations, tests,
               documentation, lineage)
                       |
              [Google Sheets - cloud buffer]
              (dashboard-ready marts only)
                       |
          [Tableau Public - live dashboard]

```

**Design principle: Serverless & Local-First BI**

All heavy computation (ETL, dbt materializations, columnar compression) runs locally on ClickHouse. Only compact, dashboard-ready marts are pushed to the cloud layer. This keeps infrastructure cost at $0 while making the dashboard publicly accessible.


## Development Roadmap

**Phase 1 - Local-First MVP** <- in progress

Done:
- Steam Store API + Steam Web API -> ClickHouse
- Airflow DAGs with their own schedules, retries and self-refreshing work queues
- Data quality layer: metrics per load, drift thresholds, metrics history in ClickHouse
- dbt staging models over every raw table, plus meta views that decide what to fetch next

Next:
- Twitch and IGDB ingestion
- dbt core and marts layers
- Google Sheets as cloud buffer
- Tableau Public with automatic daily refresh

**Phase 2 - Analytical Depth**
- Extended dbt layer: cohort analysis, genre lifecycle metrics

**Phase 3 - Remote deployment (planned)**
- Airflow + ClickHouse on a rented server, access over VPN
- DAGs build JSON marts -> object storage (S3), each run overwrites the previous file
- Dashboard: static HTML in the same storage, reads the JSON
- No managed services, no BI server

**Phase 4 - Intelligence Layer (a plan)**
- MCP server over ClickHouse
- BI assistant: strict chain without LangChain, dbt schema as context, SQL generated under the hood
- Response as chart or table depending on result size
- Continue.dev integration for development assistance inside IDE


## Development Environment

- **IDE:** VS Code
- **AI Assistant:** Continue.dev with local & cloud AI models via Ollama
- **Containerization:** Docker Desktop — for orchestrating Airflow, ClickHouse, and dbt
- **Python:** 3.10 in an isolated virtual environment (.venv)
- **Environment Setup:** see [infra/setup.md](infra/setup.md)

#### Supported AI Models

Models are configured via Ollama and accessed through Continue:

**Local Models:**
- `deepseek-coder:33b` — Local coding model with full capabilities
- `qwen2.5-coder:32b` — Larger, more capable local model
- `qwen2.5-coder:7b` — Lightweight local model for faster responses
- `qwen2.5-coder:1.5b` — Ultra-lightweight for inline code suggestions

**Cloud Models (via Ollama with free tier):**
- `qwen3-coder:480b-cloud` — Cloud-based large model
- `minimax-m3:cloud` — Cloud-based model with free limits

**Embedding Model:**
- `nomic-embed-text` — For codebase indexing and semantic search

For detailed setup instructions, see [infra/setup.md](infra/README.md#6-ai-assistant-setup-with-continue-and-ollama).


## Project Structure

```
game-market-analytics/
  |- README.md
  |- README.ru.md
  |- pyproject.toml
  |- requirements.txt
  |- infra/
  |    |- setup.md
  |    |- docker-compose.yml
  |    |- Dockerfile.airflow
  |    |- requirements-airflow.txt
  |    |- .env.example
  |    +- clickhouse/
  |- dags/
  |    +- *.py
  |- dags_utils/
  |    |- sources/
  |    |- operations/
  |    |- checks/
  |    |- commons/
  |- data_models/
  |- clickhouse_ddl/
  |    |- create_raw_ddl_from_data_model.py
  |    |- raw/
  |    +- raw_dq/
  |- dbt/
  |    |- dbt_project.yml
  |    |- profiles.yml
  |    |- models/
  |    |    |- staging/
  |    |    |- core/
  |    |    |- marts/
  |    |    +- meta/
  |    |- macros/
  |    +- tests/
  +- .github/
       +- workflows/
```


## Limitations

- Revenue proxy is approximate and based on owners x price
- Correlation between metrics does not imply causation
- Data availability depends on Steam profile privacy settings
