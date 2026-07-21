import importlib
import logging
import types
from datetime import datetime
from types import MappingProxyType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.sources.github import GitHubClient

logger = logging.getLogger(__name__)


class SchemaMappingError(Exception):
    """Raised when a Pydantic field cannot be mapped to a ClickHouse type."""


class SchemaDeployError(Exception):
    """Raised when model discovery or deployment fails."""


_BASE_TYPE_MAP: MappingProxyType[Any, str] = MappingProxyType(
    {
        int: "Int64",
        float: "Float64",
        str: "String",
        bool: "UInt8",
        datetime: "DateTime64(6)",
        list[str]: "Array(String)",
        list[int]: "Array(Int64)",
        list[float]: "Array(Float64)",
    }
)

_ARRAY_TYPES = frozenset({list[str], list[int], list[float]})


def _resolve_ch_type(annotation: Any) -> str | None:

    origin = get_origin(annotation)

    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)

        if len(args) > 2:
            raise SchemaMappingError("You cannot set up more then 2 types for Union/Optional.")
        if not any(a is type(None) for a in args):
            raise SchemaMappingError("One of the types must be None.")

        inner_annotation = next(arg for arg in args if arg is not type(None))

        if inner_annotation in _ARRAY_TYPES:
            return _BASE_TYPE_MAP.get(inner_annotation)

        base_type = _BASE_TYPE_MAP.get(inner_annotation)
        if base_type is None:
            raise SchemaMappingError(f"unsupported inner type in Optional: {inner_annotation!r}")

        return f"Nullable({base_type})"
    elif origin in (None, list):
        return _BASE_TYPE_MAP.get(annotation)


def _model_to_clickhouse_columns(model: type[BaseModel]) -> list[tuple[str, str]]:
    """Map every field of a Pydantic model to a (column_name, clickhouse_type) pair."""
    columns: list[tuple[str, str]] = []
    for name, field in model.model_fields.items():
        ch_type = _resolve_ch_type(field.annotation)
        if ch_type is None:
            raise SchemaMappingError(f"unsupported field {name!r}: {field.annotation!r}")
        columns.append((name, ch_type))

    logger.info("ClickHouse columns resolved for %s: %s", model.__name__, columns)
    return columns


def _get_order_by_columns(model: type[BaseModel]) -> list[str]:
    """
    Pick candidate ORDER BY columns: fields whose resolved ClickHouse type
    is neither Nullable(...) nor Array(...). Raises if none qualify —
    callers must not silently fall back to an arbitrary key.
    """
    columns: list[str] = []
    for name, field in model.model_fields.items():
        ch_type = _resolve_ch_type(field.annotation)
        if ch_type is None:
            raise SchemaMappingError(f"unsupported field {name!r}: {field.annotation!r}")
        if ch_type.startswith("Nullable(") or ch_type.startswith("Array("):
            continue
        columns.append(name)

    if not columns:
        raise SchemaDeployError(
            f"model {model.__name__!r} has no non-nullable, non-array field "
            "eligible for ORDER BY"
        )
    return columns


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
    return file_path.removesuffix(".py").replace("/", ".")


def check_new_commit(
    client: GitHubClient, last_sha: str, branch: str = "main"
) -> tuple[str, str] | None:

    current_sha = client.get_latest_commit_sha(branch)
    if current_sha == last_sha:
        return None
    return (current_sha, last_sha)


def get_changed_models(
    client: GitHubClient, base_sha: str, head_sha: str
) -> dict[str, type[BaseModel]]:
    """
    Return the Pydantic models whose module file changed between base_sha
    (older, previously deployed) and head_sha (newer, current).

    Keys are table names (the module's last name segment, e.g.
    "steamspy_all" for data_models/steamspy_all.py).
    """
    changed_files = client.get_changed_files(base_sha=base_sha, head_sha=head_sha)

    models: dict[str, type[BaseModel]] = {}
    for file_path in changed_files:
        if not file_path.startswith("data_models/"):
            continue
        table_name = _file_path_to_module_name(file_path)
        module = importlib.import_module(f"data_models.{table_name}")
        models[table_name] = _get_module_model(module)

    return models


def deploy_models(client: ClickHouseClient, models: dict[str, type[BaseModel]]) -> None:

    for table_name, model in models.items():
        columns = _model_to_clickhouse_columns(model)
        order_by = ", ".join(_get_order_by_columns(model))
        client.create_table_from_data_model(
            table_name=table_name,
            columns=columns,
            order_by=order_by,
        )
        logger.info("Deployed table %r from model %r", table_name, model.__name__)
