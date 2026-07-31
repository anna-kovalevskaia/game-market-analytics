import importlib
import logging
import types

from pydantic import BaseModel

from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_clickhouse_columns
from dags_utils.sources.github import GitHubClient

logger = logging.getLogger(__name__)


class SchemaDeployError(Exception):
    """Raised when model discovery or deployment fails."""


def _get_module_meta(module: types.ModuleType) -> type:
    meta = getattr(module, "Meta", None)
    if not isinstance(meta, type) or meta.__module__ != module.__name__:
        raise SchemaDeployError(f"module {module.__name__!r} must define a local Meta class")
    if not getattr(meta, "order_by", None):
        raise SchemaDeployError(f"{module.__name__!r} Meta must define non-empty order_by")
    return meta


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
        models[module_name] = (_get_module_model(module), _get_module_meta(module))

    return models


def deploy_models(
    client: ClickHouseClient, models: dict[str, tuple[type[BaseModel], type]]
) -> None:

    for module_name, (model, meta) in models.items():
        columns = model_to_clickhouse_columns(model)
        order_by = ", ".join(meta.order_by)
        partition_by = getattr(meta, "partition_by", "toStartOfMonth(last_update)")
        engine = getattr(meta, "engine", "MergeTree")
        ddl = client.create_ddl_from_data_model(
            schema=meta.schema,
            table_name=module_name,
            columns=columns,
            order_by=order_by,
            engine=engine,
            partition_by=partition_by,
        )

        client.execute_sql(ddl)
        logger.info("Deployed table %r from model %r", module_name, model.__name__)
