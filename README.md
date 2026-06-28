[🇷🇺 Читать на русском](README.ru.md)

# Game Market Analytics System (via Open-Source Data)

An analytical system for researching the gaming market based on open-source data. Built as an end-to-end data platform: from raw API ingestion to interactive dashboards updated daily.

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

### Phase 1 - MVP (current)

| Source | What it provides | Why |
|---|---|---|
| Steam Web API / Store API | Game metadata, price tiers, CCU | Official, stable, free |
| SteamSpy | Owners estimate, revenue proxy | Industry standard for Steam analytics |
| IGDB API | Structured genre classification, critic ratings | Steam tags are unstructured; IGDB provides clean taxonomy |
| Twitch API | Viewing hours, stream engagement | Measures media weight of game titles |

### Phase 2 - Planned

| Source | What it provides |
|---|---|
| SteamDB (scraping) | Historical price changes and discount campaigns |
| Epic Games Store API | Free giveaway history and its impact on Steam metrics |
| RAWG.io API | Cross-platform data (console market) |
| Itch.io API / RSS | Indie ecosystem and early-stage micro-trend detection |
| Reddit & YouTube API | Community sentiment and discussion activity |
| Google Trends API | Global search interest dynamics |


## Tech Stack

| Tool | Role | Why this, not alternatives |
|---|---|---|
| Apache Airflow | Orchestration | Python-native DAGs with dependency management, retry logic, UI, and alerting. Chosen over Prefect (official dbt integration abandoned in 2024) and Dagster (less common in job market). Rich ecosystem of providers. |
| ClickHouse | Data warehouse | Columnar OLAP database optimized for sub-second aggregations on append-only time-series data. PostgreSQL is OLTP and slow on analytical queries. Cloud solutions (AWS, Databricks) are unnecessary overhead without a public assistant - added in Phase 3 when public access requires it. |
| dbt | Transformation layer | Data-as-Code: Staging -> Core -> Marts, with auto-generated lineage graph, documentation, Jinja macros, and data quality tests. SQL models are readable and maintainable unlike raw ETL scripts. |
| Google Sheets | Cloud buffer | Lightweight, free cloud layer for publishing dashboard-ready marts from local DWH to the BI layer - no server required. Only pre-aggregated, dashboard-ready data is pushed to the cloud. |
| Tableau Public | Visualization | Connected to Google Sheets for automatic daily refresh of the public dashboard. |


## Architecture

```
[Steam API]  [SteamSpy]  [IGDB]  [Twitch API]
       |           |        |          |
       +-----------+--------+----------+
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

**Phase 1 - Local-First MVP** <- current
- Steam API + SteamSpy + IGDB + Twitch -> ClickHouse
- Airflow DAGs with daily schedule, retries, alerting
- dbt models: staging -> core -> marts
- Google Sheets as cloud buffer
- Tableau Public with automatic daily refresh

**Phase 2 - Data Expansion**
- Additional sources: SteamDB, Epic Games Store, RAWG, Itch.io, Reddit, YouTube, Google Trends
- Extended dbt layer: cohort analysis, genre lifecycle metrics
- Crunchbase + SEC EDGAR for studio-level enrichment

**Phase 3 - Cloud Migration**
- Trigger: public assistant requires cloud infrastructure
- ClickHouse -> AWS (S3 + Redshift Serverless) or ClickHouse Cloud
- Airflow -> MWAA or retain local
- Tableau Public -> Superset or Tableau Server

**Phase 4 - Intelligence Layer**
- MCP server for Redshift / ClickHouse Cloud
- Public BI assistant: strict chain without LangChain, dbt schema as context, SQL generated under the hood
- Response as chart or table depending on result size
- Continue.dev integration for development assistance inside IDE


## Development Environment

- **IDE:** VS Code
- **AI Assistant:** Continue.dev with local & cloud AI models via Ollama
- **Containerization:** Docker Desktop — for orchestrating Airflow, ClickHouse, and dbt
- **Python:** 3.12 in an isolated virtual environment (.venv)
- **Environment Setup:** see [infra/README.md](infra/README.md)

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

For detailed setup instructions, see [infra/README.md](infra/README.md#6-ai-assistant-setup-with-continue-and-ollama).


## Project Structure

```
game-market-analytics/
  |- README.md
  |- README.ru.md
  |- infra/
  |    |- README.md
  |    |- docker-compose.yml
  |    |- Dockerfile.airflow
  |    |- Dockerfile.dbt
  |    +- .env.example
  |- dags/
  |    |- README.md
  |    |- sources/
  |    |    |- steam/
  |    |    |- steamspy/
  |    |    |- igdb/
  |    |    +- twitch/
  |    |- exports_bi_marts/
  |    +- *.py
  |- dbt/
  |    |- README.md
  |    |- dbt_project.yml
  |    |- profiles.yml
  |    |- models/
  |    |    |- staging/
  |    |    |- core/
  |    |    +- marts/
  |    |- macros/
  |    +- tests/
  +- docs/
```


## Limitations

- SteamSpy provides estimates, not actual sales figures
- Revenue proxy is approximate and based on owners x price
- Correlation between metrics does not imply causation
- Data availability depends on Steam profile privacy settings
