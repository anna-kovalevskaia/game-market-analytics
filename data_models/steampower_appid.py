from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SteamPowerAppidModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name: str

class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_appid"
    order_by = ("appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
