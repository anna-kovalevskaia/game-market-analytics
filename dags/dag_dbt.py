import os
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode
from pendulum import datetime

DBT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_BIN = os.getenv("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")

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
    operator_args={"vars": {"run_date": "{{ dag_run.conf.get('run_date', ds) }}"}},
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dag_id="cosmos_dbt_dag",
    tags=["dbt", "transform"],
)
