from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerPackagesModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name:  str | None = Field(default=None)
    packageid: int = Field(ge=0)
    package_option_text: str | None = Field(default=None)
    package_price_with_discount: int | None = Field(default=None, ge=0)  # cents

    @field_validator(
        "package_price_with_discount",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_packages"
    order_by = ("appid", "packageid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
