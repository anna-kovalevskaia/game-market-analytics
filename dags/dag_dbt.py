# This file must contain the words "dag" and "airflow": with safe mode on (the
# default), DagBag skips files lacking either one — silently, without an import error.
import os
from pathlib import Path

from airflow.sdk import AssetAll
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode
from pendulum import datetime

from dags_utils.commons.assets import table_asset
from data_models.steampower_active_users import TableConfig as ISteamPlayersTable
from data_models.steampower_appdetails import TableConfig as SteamPowerDetailsTable
from data_models.steampower_appid import TableConfig as SteamPowerAppidTable
from data_models.steampower_specials import TableConfig as SteamPowerSpecialsTable

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"  # dbt/ sits next to dags/ in both envs
DBT_BIN = os.getenv("DBT_EXECUTABLE_PATH")  # image: dbt_venv/bin/dbt, CI: dbt from PATH

dbt_dag = DbtDag(
    project_config=ProjectConfig(DBT_DIR),
    profile_config=ProfileConfig(
        profile_name="game_market_analytics",
        target_name="local",
        profiles_yml_filepath=DBT_DIR / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=DBT_BIN,
    ),
    render_config=RenderConfig(dbt_executable_path=DBT_BIN),
    operator_args={
        "vars": {
            "run_date": (
                "{{ dag_run.conf.get('run_date')" " or dag_run.run_after.strftime('%Y-%m-%d') }}"
            )
        }
    },
    schedule=AssetAll(
        table_asset(SteamPowerAppidTable),
        table_asset(SteamPowerSpecialsTable),
        table_asset(SteamPowerDetailsTable),
        table_asset(ISteamPlayersTable),
    ),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dag_id="cosmos_dbt_dag",
    tags=["dbt", "transform"],
)
