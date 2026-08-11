# scripts/gen_ddl.py
import importlib
import sys
from pathlib import Path

from dags_utils.commons.model_types import create_ddl_from_data_model, model_to_clickhouse_columns
from dags_utils.schema_deploy import _get_module_meta, _get_module_model


def gen_ddl(module_name: str) -> None:
    """`module_name` selects the file under data_models/; Meta decides the table name."""
    module = importlib.import_module(f"data_models.{module_name}")
    model = _get_module_model(module)
    meta = _get_module_meta(module)

    ddl = create_ddl_from_data_model(
        schema=meta.schema,
        table_name=meta.table_name,
        columns=model_to_clickhouse_columns(model),
        order_by=", ".join(meta.order_by),
        engine=getattr(meta, "engine", "MergeTree"),
        partition_by=getattr(meta, "partition_by", "toStartOfMonth(last_update)"),
    )
    parent_path = Path("clickhouse_ddl") / meta.schema / meta.table_name
    parent_path.mkdir(parents=True, exist_ok=True)
    out_file = parent_path / f"{meta.table_name}_ddl.sql"
    out_file.write_text(ddl, encoding="utf-8")

    print(f"wrote {out_file}")


if __name__ == "__main__":
    gen_ddl(sys.argv[1])

# docker compose exec airflow-scheduler python /opt/airflow/clickhouse_ddl/create_raw_ddl_from_data_model.py your_model_name
