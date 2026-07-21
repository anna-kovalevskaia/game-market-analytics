import logging
from datetime import timedelta

from airflow.sdk import Variable, dag, task
from pendulum import datetime

logger = logging.getLogger(__name__)


@task.short_circuit
def wait_for_main_changes() -> dict[str, str] | bool:
    """Poll main for a new commit. Detection only — no side effects."""
    # Imported inside the task, not at module top level, to keep DAG parsing
    # lightweight — heavy deps (requests) load only at run time.
    from dags_utils.schema_deploy import check_main_new_commit
    from dags_utils.sources.github import GitHubClient

    last_sha = Variable.get("schema_deploy_last_sha")

    client = GitHubClient()

    result = check_main_new_commit(client, last_sha=last_sha)
    if result is None:
        return False

    current_sha, previous_sha = result
    logger.info("New commit on main: %s -> %s", previous_sha, current_sha)

    return {"base_sha": previous_sha, "head_sha": current_sha}


@task
def deploy_changed_schemas(sha_pair: dict[str, str]) -> None:
    # Lazy imports: clickhouse_connect / polars / pydantic load at run time only.
    from dags_utils.commons.clickhouse import ClickHouseClient
    from dags_utils.schema_deploy import deploy_models, get_changed_models
    from dags_utils.sources.github import GitHubClient

    github_client = GitHubClient()
    models = get_changed_models(
        github_client, base_sha=sha_pair["base_sha"], head_sha=sha_pair["head_sha"]
    )

    if not models:
        logger.info("Commit diff touched data_models/ but nothing to deploy")
        return

    clickhouse_client = ClickHouseClient()
    deploy_models(clickhouse_client, models)


@task
def record_deployed_sha(sha_pair: dict[str, str]) -> None:

    head_sha = sha_pair["head_sha"]
    Variable.set("schema_deploy_last_sha", head_sha)
    logger.info("Recorded deployed SHA: %s", head_sha)


@dag(
    dag_id="deploy_clickhouse_schemas",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "schema", "ops"],
)
def deploy_clickhouse_schemas():
    sha_pair = wait_for_main_changes()
    deployed = deploy_changed_schemas(sha_pair=sha_pair)
    recorded = record_deployed_sha(sha_pair=sha_pair)
    deployed >> recorded


deploy_clickhouse_schemas()
