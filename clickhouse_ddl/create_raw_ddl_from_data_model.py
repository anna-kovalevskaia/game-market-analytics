# scripts/gen_ddl.py
import importlib
import sys
from pathlib import Path

from dags_utils.commons.model_types import create_ddl_from_data_model, model_to_clickhouse_columns
from dags_utils.schema_deploy import _get_module_model, _get_module_table_config


def gen_ddl(module_name: str) -> None:
    """`module_name` selects the file under data_models/; TableConfig decides the table name."""
    module = importlib.import_module(f"data_models.{module_name}")
    model = _get_module_model(module)
    table_config = _get_module_table_config(module)

    ddl = create_ddl_from_data_model(
        schema=table_config.schema,
        table_name=table_config.table_name,
        columns=model_to_clickhouse_columns(model),
        order_by=", ".join(table_config.order_by),
        engine=getattr(table_config, "engine", "MergeTree"),
        partition_by=getattr(table_config, "partition_by", "toStartOfMonth(last_update)"),
    )
    parent_path = Path("clickhouse_ddl") / table_config.schema / table_config.table_name
    parent_path.mkdir(parents=True, exist_ok=True)
    out_file = parent_path / f"{table_config.table_name}_ddl.sql"
    out_file.write_text(ddl, encoding="utf-8")

    print(f"wrote {out_file}")


if __name__ == "__main__":
    gen_ddl(sys.argv[1])

# docker compose exec airflow-scheduler python /opt/airflow/clickhouse_ddl/create_raw_ddl_from_data_model.py your_model_name
