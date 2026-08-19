from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerPriceModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name: str
    currency: str | None = Field(default=None)
    price_initial: int | None = Field(default=None, ge=0)  # cents
    price_final: int | None = Field(default=None, ge=0)  # cents
    discount_percent: int | None = Field(default=None, ge=0)

    @field_validator(
        "currency",
        "price_initial",
        "price_final",
        "discount_percent",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_price"
    order_by = ("toDate(last_update)", "appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
