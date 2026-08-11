"""
Map pydantic model fields onto storage and dataframe types.

Both the ClickHouse DDL builder and the polars writer need the same first step:
unwrap Optional and reject unions we cannot represent. Keeping that step in one
place stops the two mappings from drifting apart when a model gains a field.
"""

import logging
import types
from datetime import datetime
from types import MappingProxyType
from typing import Any, Union, get_args, get_origin

import polars as pl
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SchemaMappingError(Exception):
    """Raised when a Pydantic field cannot be mapped to a target type."""


_CH_TYPE_MAP: MappingProxyType[Any, str] = MappingProxyType(
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

_PL_TYPE_MAP: MappingProxyType[Any, Any] = MappingProxyType(
    {
        int: pl.Int64,
        float: pl.Float64,
        str: pl.Utf8,
        bool: pl.Boolean,
        datetime: pl.Datetime("us"),
        list[str]: pl.List(pl.Utf8),
        list[int]: pl.List(pl.Int64),
        list[float]: pl.List(pl.Float64),
    }
)

_ARRAY_TYPES = frozenset({list[str], list[int], list[float]})


def _unwrap_optional(field_name: str, annotation: Any) -> tuple[Any, bool]:
    """
    Return (inner annotation, is_optional).

    Only `X | None` is accepted: a union of two unrelated types has no single
    column type, so it is rejected here rather than silently picking one.
    """
    if get_origin(annotation) not in (Union, types.UnionType):
        return annotation, False

    args = get_args(annotation)
    if len(args) > 2:
        raise SchemaMappingError(
            f"field {field_name!r}: Union of more than 2 types is not supported"
        )
    if not any(arg is type(None) for arg in args):
        raise SchemaMappingError(f"field {field_name!r}: one of the Union types must be None")

    return next(arg for arg in args if arg is not type(None)), True


def model_to_clickhouse_columns(model: type[BaseModel]) -> list[tuple[str, str]]:
    """Map every field of a model to a (column_name, clickhouse_type) pair."""
    columns: list[tuple[str, str]] = []
    for name, field in model.model_fields.items():
        inner, optional = _unwrap_optional(name, field.annotation)

        ch_type = _CH_TYPE_MAP.get(inner)
        if ch_type is None:
            raise SchemaMappingError(f"field {name!r}: unsupported type {inner!r}")

        # Arrays carry their own emptiness — Nullable(Array(...)) is invalid in ClickHouse.
        if optional and inner not in _ARRAY_TYPES:
            ch_type = f"Nullable({ch_type})"

        columns.append((name, ch_type))

    logger.info("ClickHouse columns resolved for %s: %s", model.__name__, columns)
    return columns


def create_ddl_from_data_model(
    schema: str,  # ClickHouse schema/database name
    table_name: str,
    columns: list[tuple[str, str]],  # [(name, clickhouse_type), ...]
    order_by: str,
    engine: str = "MergeTree",
    partition_by: str = "toStartOfMonth(last_update)",
) -> str:
    """Build a CREATE TABLE statement from model-derived columns.

    Lives here rather than on the ClickHouse client: building the statement is
    pure string work over the model's columns and needs no connection.
    """
    cols_sql = ",\n    ".join([f"{name} {clickhouse_type}" for name, clickhouse_type in columns])

    hash_args = ", ".join(f"ifNull(toString({name}), '\\\\N')" for name, _ in columns)
    cols_sql += f",\n    row_hash UInt64 MATERIALIZED cityHash64({hash_args})"
    cols_sql += ",\n    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC')"
    cols_sql += ",\n    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)"

    parts = [
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n    {cols_sql}\n)",
        f"ENGINE = {engine}",
    ]
    if partition_by:
        parts.append(f"PARTITION BY {partition_by}")
    parts.append(f"ORDER BY ({order_by})")
    return "\n".join(parts)


def model_to_polars_schema(model: type[BaseModel]) -> dict[str, Any]:
    """
    Map every field of a model to its polars dtype.

    Passing this schema to `pl.DataFrame` disables type inference: without it a
    page where every value of a sparse column is None is typed as Null, and
    concatenating it with a page holding real values fails.
    """
    schema: dict[str, Any] = {}
    for name, field in model.model_fields.items():
        inner, _ = _unwrap_optional(name, field.annotation)

        pl_type = _PL_TYPE_MAP.get(inner)
        if pl_type is None:
            raise SchemaMappingError(f"field {name!r}: unsupported type {inner!r}")

        schema[name] = pl_type

    return schema
