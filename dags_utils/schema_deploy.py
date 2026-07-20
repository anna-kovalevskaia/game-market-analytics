import logging
import types
from datetime import datetime
from types import MappingProxyType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SchemaMappingError(Exception):
    """Raised when a Pydantic field cannot be mapped to a ClickHouse type."""


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

_ARRAY_TYPES = {list[str], list[int], list[float]}


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


def model_to_clickhouse_columns(model: type[BaseModel]) -> str:
    columns: list[str] = []
    for name, field in model.model_fields.items():
        ch_type = _resolve_ch_type(field.annotation)
        if ch_type is None:
            raise SchemaMappingError(f"unsupported field {name!r}: {field.annotation!r}")
        columns.append(f"{name} {ch_type}")

    logger.info("ClickHouse columns were created: %s", columns)
    return columns


def get_changed_models():

    pass


def create_ddl():
    pass
