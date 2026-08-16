"""
DEPRECATED — schema deployment does not belong in Airflow.

The design polls main for new commits every 30 minutes and keeps the last
deployed SHA in an Airflow Variable. That makes deployment depend on scheduler
state: losing the Variable, editing it by hand or force-pushing silently skips a
range of commits, and the failure only surfaces later as a missing table.

It also cannot do what its name promises. The generated statement is
CREATE TABLE IF NOT EXISTS, so a changed model deploys "successfully" and does
nothing at all — the live table keeps the old columns.

Planned replacement: create the first DDL by hand, then keep numbered migration
files (001_*.sql, 002_*.sql, ...) applied deliberately, not on a timer.
"""

import importlib
import logging
import types

from pydantic import BaseModel

from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import create_ddl_from_data_model, model_to_clickhouse_columns
from dags_utils.sources.github import GitHubClient

logger = logging.getLogger(__name__)


class SchemaDeployError(Exception):
    """Raised when model discovery or deployment fails."""


def _get_module_table_config(module: types.ModuleType) -> type:
    table_config = getattr(module, "TableConfig", None)
    if not isinstance(table_config, type) or table_config.__module__ != module.__name__:
        raise SchemaDeployError(f"module {module.__name__!r} must define a local TableConfig class")
    if not getattr(table_config, "order_by", None):
        raise SchemaDeployError(f"{module.__name__!r} TableConfig must define non-empty order_by")
    if not getattr(table_config, "table_name", None):
        raise SchemaDeployError(f"{module.__name__!r} TableConfig must define non-empty table_name")
    if not getattr(table_config, "schema", None):
        raise SchemaDeployError(f"{module.__name__!r} TableConfig must define non-empty schema")
    return table_config


def _get_module_model(module: types.ModuleType) -> type[BaseModel]:
    """
    Return the single BaseModel subclass defined in `module`.

    Convention: exactly one model per module in data_models/. The
    `obj.__module__ == module.__name__` filter is still required even
    with that convention — pydantic's own BaseModel, ConfigDict, Field
    etc. are also present in the module namespace via
    `from pydantic import ...` and must not be mistaken for the model.
    """
    local_models = [
        obj
        for obj in vars(module).values()
        if isinstance(obj, type)
        and issubclass(obj, BaseModel)
        and obj is not BaseModel
        and obj.__module__ == module.__name__
    ]

    if len(local_models) != 1:
        raise SchemaDeployError(
            f"module {module.__name__!r} must define exactly one BaseModel "
            f"subclass, found {len(local_models)}"
        )
    return local_models[0]


def _file_path_to_module_name(file_path: str) -> str:
    """'data_models/steamspy_all.py' -> 'data_models.steamspy_all'."""
    if not file_path.endswith(".py"):
        raise SchemaDeployError(f"not a python module path: {file_path!r}")
    return file_path.removesuffix(".py").replace("data_models/", "")


def check_main_new_commit(client: GitHubClient, last_sha: str) -> tuple[str, str] | None:

    current_sha = client.get_latest_commit_sha()
    if current_sha == last_sha:
        logger.info("no new commits")
        return None
    return (current_sha, last_sha)


def get_changed_models(
    client: GitHubClient, base_sha: str, head_sha: str
) -> dict[str, tuple[type[BaseModel], type]]:

    changed_files = client.get_changed_files(base_sha=base_sha, head_sha=head_sha)

    models: dict[str, tuple[type[BaseModel], type]] = {}
    for file_path in changed_files:
        if not file_path.startswith("data_models/"):
            continue
        module_name = _file_path_to_module_name(file_path)
        if module_name.rsplit(".", 1)[-1].startswith("_"):
            continue
        module = importlib.import_module(f"data_models.{module_name}")
        models[module_name] = (_get_module_model(module), _get_module_table_config(module))

    return models


def deploy_models(
    client: ClickHouseClient, models: dict[str, tuple[type[BaseModel], type]]
) -> None:

    for module_name, (model, table_config) in models.items():
        columns = model_to_clickhouse_columns(model)
        order_by = ", ".join(table_config.order_by)
        partition_by = getattr(table_config, "partition_by", "toStartOfMonth(last_update)")
        engine = getattr(table_config, "engine", "MergeTree")
        ddl = create_ddl_from_data_model(
            schema=table_config.schema,
            table_name=table_config.table_name,
            columns=columns,
            order_by=order_by,
            engine=engine,
            partition_by=partition_by,
        )

        client.execute_sql(ddl)
        logger.info(
            "Deployed table %s.%s from model %r (data_models/%s.py)",
            table_config.schema,
            table_config.table_name,
            model.__name__,
            module_name,
        )
