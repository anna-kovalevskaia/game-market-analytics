# scripts/gen_ddl.py
import importlib
import sys
from pathlib import Path

from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.schema_deploy import (
    _get_module_meta,
    _get_module_model,
    _model_to_clickhouse_columns,
)

def gen_ddl(model_name: str) -> None:
    module = importlib.import_module(f"data_models.{model_name}")
    model = _get_module_model(module)
    meta = _get_module_meta(module)

    ddl = ClickHouseClient.create_ddl_from_data_model(
        schema=meta.schema,
        table_name=model_name,
        columns=_model_to_clickhouse_columns(model),
        order_by=", ".join(meta.order_by),
        engine=getattr(meta, "engine", "MergeTree"),
        partition_by=getattr(meta, "partition_by", "toStartOfMonth(last_update)"),
    )
    parent_path = Path("clickhouse_ddl") / meta.schema / model_name
    parent_path.mkdir(parents=True, exist_ok=True)
    (parent_path / f"{model_name}_ddl.sql").write_text(ddl, encoding="utf-8")

    print(f"wrote {parent_path / f'{model_name}_ddl.sql'}")


if __name__ == "__main__":
    gen_ddl(sys.argv[1])

#docker compose exec airflow-scheduler python /opt/airflow/clickhouse_ddl/create_raw_ddl.py your_model_name