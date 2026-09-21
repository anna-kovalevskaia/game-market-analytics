import importlib
import logging
import sys
import types
from pathlib import Path

from pydantic import BaseModel

from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import create_ddl_from_data_model, model_to_clickhouse_columns

logger = logging.getLogger(__name__)

DDL_DIR = Path("clickhouse_ddl")
DEPLOY_MARKER = DDL_DIR / ".deploy"


class SchemaDeployError(Exception):
    """Raised when model discovery or deployment fails."""


def get_module_table_config(module: types.ModuleType) -> type:
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


def get_module_model(module: types.ModuleType) -> type[BaseModel]:
    """
    Return the single BaseModel subclass defined in `module`.
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


def _get_last_change_number(path: Path):
    """Return the last change number for the table."""
    numc = [int(p.name.split("_", 1)[0]) for p in path.glob("*.sql")]
    return max(numc, default=-1)


def _get_ddl(table_config: type, model: type[BaseModel], last_change_num: bool) -> str:
    """Generate the DDL for the table."""
    ddl = create_ddl_from_data_model(
        schema=table_config.schema,
        table_name=table_config.table_name + "_tmp" * last_change_num,
        columns=model_to_clickhouse_columns(model),
        order_by=", ".join(table_config.order_by),
        engine=getattr(table_config, "engine", "MergeTree"),
        partition_by=getattr(table_config, "partition_by", "toStartOfMonth(last_update)"),
    )
    return ddl


def _change_steps(
    schema: str, table_name: str, last_change_num: int, ddl: str, model_cols: set[str]
) -> dict:
    """Generate the steps to change the table."""
    origin_cols = dict(ClickHouseClient().get_column_details(schema, table_name))
    common_col_names = ", ".join(name for name in origin_cols if name in model_cols)

    table = f"{schema}.{table_name}"
    tmp = f"{table}_tmp"

    if not last_change_num:
        return {f"{last_change_num}_{table_name}_ddl.sql": ddl}

    return {
        f"{last_change_num}_1_drop_stale_tmp_table.sql": f"DROP TABLE IF EXISTS {tmp}",
        f"{last_change_num}_2_create_tmp_table.sql": ddl,
        f"{last_change_num}_3_copy_data.sql": (
            f"INSERT INTO {tmp} ({common_col_names}) SELECT {common_col_names} FROM {table}"
        ),
        f"{last_change_num}_4_exchange_tables.sql": f"EXCHANGE TABLES {tmp} AND {table}",
        f"{last_change_num}_5_drop_tmp_table.sql": f"DROP TABLE {tmp}",
    }


def get_models_details(module_name: str) -> tuple[type[BaseModel], type]:

    module = importlib.import_module(f"data_models.{module_name}")
    model = get_module_model(module)
    table_config = get_module_table_config(module)
    return model, table_config


def gen_ddl(module_name: str) -> None:
    """`module_name` selects the file under data_models/; TableConfig decides the table name."""
    model, table_config = get_models_details(module_name)

    parent_path = DDL_DIR / table_config.schema / table_config.table_name
    parent_path.mkdir(parents=True, exist_ok=True)

    last_change_num = _get_last_change_number(parent_path) + 1
    ddl = _get_ddl(table_config, model, bool(last_change_num))

    model_cols = {name for name, _ in model_to_clickhouse_columns(model)} | {"last_update"}
    change_steps = _change_steps(
        table_config.schema, table_config.table_name, last_change_num, ddl, model_cols
    )
    for file_name, query in change_steps.items():
        out_file = parent_path / file_name
        out_file.write_text(query, encoding="utf-8")
        logger.info("wrote %s", out_file)

    DEPLOY_MARKER.touch()
    logger.info("wrote %s", DEPLOY_MARKER)


if __name__ == "__main__":
    gen_ddl(sys.argv[1])

# ...game-market-analytics/infra> docker compose exec airflow-scheduler \
#  python /opt/airflow/dags_utils/create_raw_ddl_from_data_model.py your_model_name
