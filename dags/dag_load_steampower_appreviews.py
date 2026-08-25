from pathlib import Path

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime, parse

from dags_utils.checks.check_metrics import Check
from dags_utils.commons.assets import table_asset
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.operations.steampower_appreviews_ops import (
    get_appids_to_fetch,
    steamappreviews_extract_to_tmp,
    steamappreviews_parquet_to_clickhouse,
)
from dags_utils.sources.steampower import SteamPowerClient
from data_models.metrics_status import TableConfig as MetricsStatusTable
from data_models.steampower_appreviews import TableConfig as SteamPowerAppreviewsTable


@task
def steamappreviews_extract() -> str:

    ctx = get_current_context()
    run_id = ctx["run_id"]
    dag_id = ctx["dag"].dag_id
    par_path = Variable.get("tmp_dir")
    run_id_path = Path(par_path) / dag_id / run_id

    steampower_client = SteamPowerClient(timeout=10)
    appids = get_appids_to_fetch(ClickHouseClient())
    params = {"delay_seconds": 1.1, "appids": appids, "batch_size": 1000}
    steamappreviews_extract_to_tmp(client=steampower_client, run_id_path=run_id_path, **params)

    return str(run_id_path)


@task(outlets=[table_asset(SteamPowerAppreviewsTable)])
def steamappreviews_insert_to_clickhouse(run_id_path: str) -> None:

    fixed_run_id_path = Path(run_id_path)

    ctx = get_current_context()
    run_datetime = parse(ctx["dag_run"].conf.get("run_date") or ctx["ts"]).in_timezone("UTC")
    cur_date = run_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    ch_client = ClickHouseClient()

    steamappreviews_parquet_to_clickhouse(
        client=ch_client,
        run_id_path=fixed_run_id_path,
        raw=SteamPowerAppreviewsTable,
        raw_dq=MetricsStatusTable,
        check=Check(),
        cur_date=cur_date,
        dag_id=ctx["dag"].dag_id,
        batch_size=1000,
    )


@dag(
    dag_id="steamappreviews_raw_data",
    schedule="20 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def steamappreviews__process():
    tmp_dir = steamappreviews_extract()
    steamappreviews_insert_to_clickhouse(run_id_path=tmp_dir)


steamappreviews__process()
