from typing import Optional

from pydantic import BaseModel, ConfigDict


class SteamSpyAllModel(BaseModel):

    model_config = ConfigDict(extra="forbid")

    appid: int
    name: str
    developer: Optional[str] = None
    publisher: Optional[str] = None
    score_rank: Optional[str] = None
    positive: Optional[int] = None
    negative: Optional[int] = None
    userscore: Optional[int] = None
    owners: Optional[str] = None
    average_forever: Optional[float] = None
    average_2weeks: Optional[float] = None
    median_forever: Optional[float] = None
    median_2weeks: Optional[float] = None
    price: Optional[float] = None
    initialprice: Optional[float] = None
    discount: Optional[float] = None
    ccu: Optional[int] = None