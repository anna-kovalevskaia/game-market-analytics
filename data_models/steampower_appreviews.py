from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerAppreviews(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    review_score: int | None = Field(default=None, ge=0)
    review_score_desc: str | None = Field(default=None)
    total_positive: int | None = Field(default=None, ge=0)
    total_negative: int | None = Field(default=None, ge=0)
    total_reviews: int | None = Field(default=None, ge=0)


    @field_validator(
        "review_score",
        "total_positive",
        "total_negative",
        "total_reviews",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_appreviews"
    order_by = ("appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
