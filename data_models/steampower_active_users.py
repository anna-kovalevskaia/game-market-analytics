from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ISteamActiveUsersModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    player_count: int | None = Field(default=None, ge=0)

    @field_validator(
        "player_count",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_active_users"
    order_by = ("toStartOfHour(last_update)", "appid")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
