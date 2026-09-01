from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerAppReviewDetailsModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)

    timestamp_created: datetime
    timestamp_updated: datetime
    voted_up: bool
    language: str | None = Field(default=None)
    steam_purchase: bool | None = Field(default=None)
    received_for_free: bool | None = Field(default=None)
    written_during_early_access: bool | None = Field(default=None)
    refunded: bool | None = Field(default=None)
    primarily_steam_deck: bool | None = Field(default=None)

    playtime_at_review: int | None = Field(default=None, ge=0)      # minutes
    playtime_forever: int | None = Field(default=None, ge=0)        # minutes
    playtime_last_two_weeks: int | None = Field(default=None, ge=0) # minutes
    last_played: datetime | None = Field(default=None)


    @field_validator(
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "refunded",
        "primarily_steam_deck",
        "playtime_at_review",
        "playtime_forever",
        "playtime_last_two_weeks",
        "last_played",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v

class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_appreviews_details"
    order_by = ("appid", "row_hash")
    partition_by = "toStartOfMonth(timestamp_created)"
    engine = "ReplacingMergeTree(ver)"