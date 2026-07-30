# Rule: new/changed model in data_models/ → DDL + dbt source

## When to apply
A pydantic model in `data_models/<name>.py` was added or changed.

## Model structure (invariant)
`data_models/<name>.py` must contain:
- exactly one `BaseModel` subclass;
- a module-level `class Meta` with: `order_by` (tuple), `partition_by`, `engine`, `schema`;
- column descriptions in `Field(description="...")` — the single source of truth for descriptions.

## Step 1 — DDL (deterministic, never hand-written)
Run the generator, do NOT write DDL by hand:
docker compose exec airflow-scheduler python /opt/airflow/clickhouse_ddl/create_raw_ddl.py <model_name>

Output: `clickhouse_ddl/<schema>/<model>/<model>_ddl.sql`.

## Step 2 — dbt source yml
Create/update `dbt/models/<source>_source.yml`:
- source `name` = data provider (e.g. `steamspy`), NOT the schema name;
- `schema` = `Meta.schema` (e.g. `raw`);
- `table.name` = model name;
- columns = ALL model fields in order + auto columns `row_hash`, `last_update`;
- each column `description` comes from `Field(description)`; if empty, write a concise description in English;
- tests: `not_null` on the key (`appid`). Do NOT add `unique` — raw is versioned (ReplacingMergeTree), the key repeats;
- if the yml already exists: add/replace ONLY this table, leave others untouched.

## Constraints
- Do not modify the production DAG to write files — generation is a dev step.
- Never hardcode DDL; only via the generator.
- Never add `unique` on raw tables.
- All generated column descriptions must be in English.

## Output
Briefly report: which DDL file was created/updated, and which table was added/updated in which yml.