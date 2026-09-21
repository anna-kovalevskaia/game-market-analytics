from typing import Annotated
from pydantic import BaseModel, ConfigDict


class MetricsStatusModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    dag_id: Annotated[str, "LowCard"]
    schema_name: str
    table_name: str
    metrics_name: str
    agg_type: str
    metrics_value: float

class TableConfig:
    schema: str = "raw_dq"
    table_name: str = "metrics_status"
    order_by = ("row_hash",)
    engine = "MergeTree()"
