from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SteamPowerAppTagModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    tagid: int | None = Field(default=None, ge=0)
    tag_name: str | None = Field(default=None)
    count: int | None = Field(default=None, ge=0)
    browseable: int | None = Field(default=None, ge=0)


    @field_validator(
        "tagid",
        "count",
        "browseable",
        mode="before",
    )
    @classmethod
    def _empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and not v.strip() else v


class TableConfig:
    schema: str = "raw"
    table_name: str = "steampower_apptag"
    order_by = ("toDate(last_update)", "appid", "row_hash")
    partition_by = None
    engine = "ReplacingMergeTree(ver)"
