from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerAppdetailsModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    success: bool
    appid: int = Field(ge=0)
    name: str | None = Field(default=None)
    required_age: int | None = Field(default=None, ge=0)
    is_free: bool | None = Field(default=None)
    supported_languages: str | None = Field(default=None)
    website: str | None = Field(default=None)
    pc_requirements: str | None = Field(default=None)
    mac_requirements: str | None = Field(default=None)
    linux_requirements: str | None = Field(default=None)
    developers: list[str] | None = Field(default=None)
    publishers: list[str] | None = Field(default=None)
    categories_id: list[int] | None = Field(default=None)
    categories_description: list[str] | None = Field(default=None)
    genres_id: list[str] | None = Field(default=None)
    genres_description: list[str] | None = Field(default=None)
    release_date: datetime | None = Field(default=None)


    @field_validator(
        "required_age",
        "release_date",
        mode="before"
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v

class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_appdetails"
    order_by = ("appid", "row_hash")
    partition_by = "toStartOfMonth(last_update)"
    engine = "ReplacingMergeTree(ver)"
