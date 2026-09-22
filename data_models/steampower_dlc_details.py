from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerDlcDetailsModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    dlc_appid: int = Field(ge=0)
    dlc_name: str
    type: str
    is_free: bool
    release_date: datetime | None = Field(default=None)
    platform_windows: bool
    platform_mac: bool
    platform_linux: bool

    @field_validator(
        "release_date",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_dlc_details"
    order_by = ("appid", "dlc_appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
