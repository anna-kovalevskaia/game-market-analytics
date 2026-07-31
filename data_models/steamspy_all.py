from typing import Any
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamSpyAllModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name: str
    developer: str | None = Field(default=None)
    publisher: str | None = Field(default=None)
    score_rank: int | None = Field(default=None, ge=0)
    positive: int | None = Field(default=None, ge=0)
    negative: int | None = Field(default=None, ge=0)
    userscore: int | None = Field(default=None)
    owners: str | None = Field(default=None)
    average_forever: float | None = Field(default=None, ge=0)
    average_2weeks: float | None = Field(default=None, ge=0)
    median_forever: float | None = Field(default=None, ge=0)
    median_2weeks: float | None = Field(default=None, ge=0)
    price: float | None = Field(default=None, ge=0)
    initialprice: float | None = Field(default=None, ge=0)
    discount: float | None = Field(default=None, ge=0)
    ccu: int | None = Field(default=None, ge=0)

    @field_validator(
        "score_rank",
        "positive",
        "negative",
        "userscore",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "price",
        "initialprice",
        "discount",
        "ccu",
        mode="before"
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v

class Meta:
    order_by = ("appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(last_update)"
    schema: str = "raw"
