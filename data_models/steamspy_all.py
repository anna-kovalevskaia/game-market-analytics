from pydantic import BaseModel, ConfigDict, Field

class SteamSpyAllModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    appid: int = Field(ge=0)
    name: str
    developer: str | None = Field(default=None)
    publisher: str | None = Field(default=None)
    score_rank: str | None = Field(default=None)
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


