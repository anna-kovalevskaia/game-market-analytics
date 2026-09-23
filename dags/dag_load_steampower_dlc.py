from pathlib import Path

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime, parse

from dags_utils.checks.check_metrics import Check
from dags_utils.commons.assets import table_asset
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.operations.steampower_dlc_ops import (
    get_appids_to_fetch,
    steamdlc_extract_to_tmp,
    steamdlc_metrics_validate,
    steamdlc_update_raw_dq,
)
from dags_utils.sources.steampower import SteamPowerClient
from data_models.metrics_status import TableConfig as MetricsStatusTable
from data_models.steampower_dlc_details import TableConfig as SteamPowerDlcDetailsTable
from data_models.steampower_dlc_packages import TableConfig as SteamPowerDlcPackagesTable
from data_models.steampower_dlc_price import TableConfig as SteamPowerDlcPriceTable

TABLE_CHECKS = (
    (SteamPowerDlcDetailsTable, Check()),
    (
        SteamPowerDlcPriceTable,
        Check(
            MEDIAN_COLUMNS=["price_initial", "price_final", "discount_percent"],
            WARN_THRESHOLD=0.5,
            ERROR_THRESHOLD=3,
        ),
    ),
    (
        SteamPowerDlcPackagesTable,
        Check(
            MEDIAN_COLUMNS=["package_price_with_discount"],
            WARN_THRESHOLD=0.5,
            ERROR_THRESHOLD=3,
        ),
    ),
)


def _cur_date(ctx) -> str:
    run_datetime = parse(ctx["dag_run"].conf.get("run_date") or ctx["ts"]).in_timezone("UTC")
    return run_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@task.short_circuit
def steamdlc_extract() -> str:

    steampower_client = SteamPowerClient(timeout=10)
    appids = get_appids_to_fetch(ClickHouseClient())
    if not appids:
        return ""

    ctx = get_current_context()
    run_id = ctx["run_id"]
    dag_id = ctx["dag"].dag_id
    par_path = Variable.get("tmp_dir")
    run_id_path = Path(par_path) / dag_id / run_id

    params = {"delay_seconds": 1.3, "appids": appids, "batch_size": 500}

    steamdlc_extract_to_tmp(
        client=steampower_client,
        run_id_path=run_id_path,
        **params,
    )

    return str(run_id_path)


@task
def steam_dlc_validate(run_id_path: str) -> list[dict]:

    ctx = get_current_context()
    ch_client = ClickHouseClient()
    cur_date = _cur_date(ctx)
    dag_id = ctx["dag"].dag_id

    metrics: list[dict] = []
    for tables_config, check in TABLE_CHECKS:
        metrics.extend(
            steamdlc_metrics_validate(
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
def steamdlc_insert_to_clickhouse(run_id_path: str) -> None:

    ch_client = ClickHouseClient()

    for tables_config, _ in TABLE_CHECKS:
        ch_client.insert_parquet_to_ch_batch(
            tables_config.schema,
            tables_config.table_name,
            Path(run_id_path) / tables_config.table_name,
            2000,
        )


@task(outlets=[table_asset(SteamPowerDlcDetailsTable)])
def update_raw_dq_metrics_states(run_id_path: str, metrics: list[dict]) -> None:
    """Update the raw_dq_metrics table with the latest metrics."""
    steamdlc_update_raw_dq(
        client=ClickHouseClient(),
        run_id_path=Path(run_id_path),
        raw_dq=MetricsStatusTable,
        metrics=metrics,
    )


@dag(
    dag_id="steamdlc_raw_data",
    schedule="30 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def steamdlc_process():
    tmp_dir = steamdlc_extract()
    validate_metrics = steam_dlc_validate(tmp_dir)
    inserts = steamdlc_insert_to_clickhouse(run_id_path=tmp_dir)
    update_raw_dq = update_raw_dq_metrics_states(run_id_path=tmp_dir, metrics=validate_metrics)

    validate_metrics >> inserts >> update_raw_dq


steamdlc_process()
