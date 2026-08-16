"""
DEPRECATED — the SteamSpy load is switched off; see dags_utils/sources/steamspy.py
for the measurements behind that call. The file must keep the words "dag" and
"airflow" so DagBag does not skip it silently.

The DAG is not registered: the call that builds it at the bottom is commented out.
Note that cosmos_dbt_dag is scheduled on the raw.steamspy_all asset, which only
this DAG produces — with the load off, dbt has no trigger left.
"""

import logging
from pathlib import Path

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime, parse

from dags_utils.checks.check_metrics import Check
from dags_utils.commons.assets import table_asset
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.operations.steamspy_all_ops import (
    steamspy_all_extract_to_tmp,
    steamspy_all_parquet_to_clickhouse,
)
from dags_utils.sources.steamspy import SteamSpyClient
from data_models.steamspy_all import TableConfig as SteamSpyAllTable

logger = logging.getLogger(__name__)


@task
def steamspy_all_extract() -> str:

    steamspy_client = SteamSpyClient(timeout=10)
    params = {"max_pages": 1000, "stop_after_empty_pages": 3, "delay_seconds": 63}
    ctx = get_current_context()
    run_id = ctx["run_id"]
    par_path = Variable.get("steamspy_all_tmp_dir")
    run_id_path = Path(par_path) / run_id

    steamspy_all_extract_to_tmp(client=steamspy_client, run_id_path=run_id_path, **params)

    return str(run_id_path)


@task(outlets=[table_asset(SteamSpyAllTable)])
def steamspy_all_insert_to_clickhouse(run_id_path: str) -> None:

    fixed_run_id_path = Path(run_id_path)

    ctx = get_current_context()
    run_datetime = parse(ctx["dag_run"].conf.get("run_date") or ctx["ts"]).in_timezone("UTC")
    cur_date = run_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    ch_client = ClickHouseClient()

    steamspy_all_parquet_to_clickhouse(
        client=ch_client,
        run_id_path=fixed_run_id_path,
        batch_size=100,
        raw=SteamSpyAllTable,
        cur_date=cur_date,
        check=Check(),
    )


@dag(
    dag_id="steamspy_all_raw_data",
    schedule="30 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def steamspy_all_process():
    tmp_dir = steamspy_all_extract()
    steamspy_all_insert_to_clickhouse(run_id_path=tmp_dir)


# DEPRECATED: source dropped, the DAG is intentionally left unregistered.
# steamspy_all_process()
