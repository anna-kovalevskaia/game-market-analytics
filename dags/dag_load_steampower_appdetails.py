from pathlib import Path

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime, parse

from dags_utils.checks.check_metrics import Check
from dags_utils.commons.assets import table_asset
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.operations.steampower_appdetails_ops import (
    get_appids_to_fetch,
    steamappdetails_extract_to_tmp,
    steamappdetails_parquet_to_clickhouse,
)
from dags_utils.sources.steampower import SteamPowerClient
from data_models.metrics_status import TableConfig as MetricsStatusTable
from data_models.steampower_appdetails import TableConfig as SteamPowerDetailsTable
from data_models.steampower_packages import TableConfig as SteamPowerPackagesTable
from data_models.steampower_price import TableConfig as SteamPowerPriceTable


def _cur_date(ctx) -> str:
    run_datetime = parse(ctx["dag_run"].conf.get("run_date") or ctx["ts"]).in_timezone("UTC")
    return run_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@task
def steamappdetails_extract() -> str:

    ctx = get_current_context()
    run_id = ctx["run_id"]
    dag_id = ctx["dag"].dag_id
    par_path = Variable.get("tmp_dir")
    run_id_path = Path(par_path) / dag_id / run_id

    steampower_client = SteamPowerClient(timeout=10)
    appids = get_appids_to_fetch(ClickHouseClient())
    params = {"delay_seconds": 1.3, "appids": appids, "batch_size": 500}

    steamappdetails_extract_to_tmp(
        client=steampower_client,
        run_id_path=run_id_path,
        **params,
    )

    return str(run_id_path)


@task(outlets=[table_asset(SteamPowerDetailsTable)])
def steamappdetails_insert_to_clickhouse(run_id_path: str) -> None:

    ctx = get_current_context()

    steamappdetails_parquet_to_clickhouse(
        client=ClickHouseClient(),
        run_id_path=Path(run_id_path) / SteamPowerDetailsTable.table_name,
        batch_size=2000,
        raw=SteamPowerDetailsTable,
        raw_dq=MetricsStatusTable,
        check=Check(),
        cur_date=_cur_date(ctx),
        dag_id=ctx["dag"].dag_id,
    )


@task
def steamprice_insert_to_clickhouse(run_id_path: str) -> None:

    ctx = get_current_context()

    steamappdetails_parquet_to_clickhouse(
        client=ClickHouseClient(),
        run_id_path=Path(run_id_path) / SteamPowerPriceTable.table_name,
        batch_size=2000,
        raw=SteamPowerPriceTable,
        raw_dq=MetricsStatusTable,
        check=Check(MEDIAN_COLUMNS=["price_initial", "price_final", "discount_percent"]),
        cur_date=_cur_date(ctx),
        dag_id=ctx["dag"].dag_id,
    )


@task
def steampackages_insert_to_clickhouse(run_id_path: str) -> None:

    ctx = get_current_context()

    steamappdetails_parquet_to_clickhouse(
        client=ClickHouseClient(),
        run_id_path=Path(run_id_path) / SteamPowerPackagesTable.table_name,
        batch_size=2000,
        raw=SteamPowerPackagesTable,
        raw_dq=MetricsStatusTable,
        check=Check(MEDIAN_COLUMNS=["package_price_with_discount"]),
        cur_date=_cur_date(ctx),
        dag_id=ctx["dag"].dag_id,
    )


@dag(
    dag_id="steamappdetails_raw_data",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def steamappdetails__process():
    tmp_dir = steamappdetails_extract()
    steamappdetails_insert_to_clickhouse(run_id_path=tmp_dir)
    steamprice_insert_to_clickhouse(run_id_path=tmp_dir)
    steampackages_insert_to_clickhouse(run_id_path=tmp_dir)


steamappdetails__process()
