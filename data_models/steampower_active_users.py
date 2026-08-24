from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerPackagesModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name:  str | None = Field(default=None)
    player_count: int = Field(ge=0)


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_active_users"
    order_by = ("toStartOfHour(last_update)", "appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
