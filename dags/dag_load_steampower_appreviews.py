from pathlib import Path

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime, parse

from dags_utils.checks.check_metrics import Check
from dags_utils.commons.assets import table_asset
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.operations.steampower_appreviews_ops import (
    get_appids_to_fetch,
    steamappreviews_extract_to_tmp,
    steamappreviews_metrics_validate,
    steamappreviews_parquet_to_clickhouse,
    steamappreviews_update_raw_dq,
)
from dags_utils.sources.steampower import SteamPowerClient
from data_models.metrics_status import TableConfig as MetricsStatusTable
from data_models.steampower_appreviews import TableConfig as SteamPowerAppreviewsTable
from data_models.steampower_appreviews_details import (
    TableConfig as SteamPowerAppreviewsDetailsTable,
)

TABLE_CHECKS = (
    (
        SteamPowerAppreviewsTable,
        Check(
            WARN_THRESHOLD=0.2,
            ERROR_THRESHOLD=3,
        ),
    ),
    (
        SteamPowerAppreviewsDetailsTable,
        Check(
            WARN_THRESHOLD=0.2,
            ERROR_THRESHOLD=3,
        ),
    ),
)


def _cur_date(ctx) -> str:
    run_datetime = parse(ctx["dag_run"].conf.get("run_date") or ctx["ts"]).in_timezone("UTC")
    return run_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@task.short_circuit
def steamappreviews_extract() -> str:

    steampower_client = SteamPowerClient(timeout=10)
    appids = get_appids_to_fetch(ClickHouseClient())
    if not appids:
        return ""

    ctx = get_current_context()
    run_id = ctx["run_id"]
    dag_id = ctx["dag"].dag_id
    par_path = Variable.get("tmp_dir")
    run_id_path = Path(par_path) / dag_id / run_id

    params = {"delay_seconds": 1.1, "appids": appids, "batch_size": 1000}

    steamappreviews_extract_to_tmp(
        client=steampower_client,
        run_id_path=run_id_path,
        **params,
    )

    return str(run_id_path)


@task
def steamappreviews_validate(run_id_path: str) -> list[dict]:

    ctx = get_current_context()
    ch_client = ClickHouseClient()
    cur_date = _cur_date(ctx)
    dag_id = ctx["dag"].dag_id

    metrics: list[dict] = []
    for tables_config, check in TABLE_CHECKS:
        metrics.extend(
            steamappreviews_metrics_validate(
                client=ch_client,
                run_id_path=Path(run_id_path) / tables_config.table_name,
                raw=tables_config,
                raw_dq=MetricsStatusTable,
                check=check,
                cur_date=cur_date,
                dag_id=dag_id,
            )
        )
    return metrics


@task
def steamappreviews_insert_to_clickhouse(run_id_path: str) -> None:

    ch_client = ClickHouseClient()

    for tables_config, _ in TABLE_CHECKS:
        steamappreviews_parquet_to_clickhouse(
            client=ch_client,
            run_id_path=Path(run_id_path) / tables_config.table_name,
            batch_size=1000,
            raw=tables_config,
        )


@task(outlets=[table_asset(SteamPowerAppreviewsTable)])
def update_raw_dq_metrics_states(run_id_path: str, metrics: list[dict]) -> None:
    """Update the raw_dq_metrics table with the latest metrics."""
    steamappreviews_update_raw_dq(
        client=ClickHouseClient(),
        run_id_path=Path(run_id_path),
        raw_dq=MetricsStatusTable,
        metrics=metrics,
    )


@dag(
    dag_id="steamappreviews_raw_data",
    schedule="40 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def steamappreviews__process():
    tmp_dir = steamappreviews_extract()
    validate_metrics = steamappreviews_validate(tmp_dir)
    inserts = steamappreviews_insert_to_clickhouse(run_id_path=tmp_dir)
    update_raw_dq = update_raw_dq_metrics_states(run_id_path=tmp_dir, metrics=validate_metrics)

    validate_metrics >> inserts >> update_raw_dq


steamappreviews__process()
