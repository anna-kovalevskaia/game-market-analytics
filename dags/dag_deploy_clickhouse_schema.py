from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime

from dags_utils.commons.assets import table_asset_watcher
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_clickhouse_columns
from dags_utils.create_raw_ddl_from_data_model import DEPLOY_MARKER, get_models_details


@task
def get_changed_file_path() -> list[str]:

    ch_client = ClickHouseClient()
    changed_models = []
    for path in Path("data_models").glob("*.py"):
        if path.stem.startswith("_"):
            continue

        model, table_config = get_models_details(path.stem)
        schema, table = table_config.schema, table_config.table_name
        origin = dict(ch_client.get_column_details(schema, table))
        origin.pop("last_update", None)
        changed = dict(model_to_clickhouse_columns(model))

        if origin != changed:
            changed_models.append(f"{schema}/{table}")
    return changed_models


@task
def apply_ddl(changed_models: list[str]) -> None:
    ch_client = ClickHouseClient()

    for changed_model in changed_models:
        path = Path("clickhouse_ddl") / changed_model
        files = list(path.glob("*.sql"))

        last_num = max(int(f.name.split("_", 1)[0]) for f in files)
        steps = sorted(f for f in files if f.name.startswith(f"{last_num}_"))

        for step in steps:
            ch_client.execute_sql(step.read_text(encoding="utf-8"))


@dag(
    dag_id="create_change_ddl",
    schedule=[
        table_asset_watcher(
            file_path=str(DEPLOY_MARKER),
            asset_name="ddl_deploy",
            asset_watcher_name="deploy_trigger",
        )
    ],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def create_change_proccess():
    apply_ddl(get_changed_file_path())


create_change_proccess()
