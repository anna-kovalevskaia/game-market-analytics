import logging
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict

from pathlib import Path

from dags_utils.sources.steamspy import SteamSpyClient

logger = logging.getLogger(__name__)


class IterArguments(BaseModel):
    max_pages: int
    stop_after_empty_pages: int
    delay_seconds: float



class SteamSpyAllModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

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


def _steamspy_write_to_tmp(data: list[SteamSpyAllModel], full_file_path: Path):
    """Write SteamSpy data to a temporary file."""
    models = [row.model_dump() for row in data]
    df = pl.DataFrame(models)
    df.write_parquet(full_file_path)


def steamspy_all_extract_to_tmp(
        client: SteamSpyClient,
        run_id: str,
        file_path: str,
        **kwargs
    ) -> str:
    """Process SteamSpy API data."""
    parent_path = Path(file_path)/run_id
    parent_path.mkdir(parents=True, exist_ok=True)
  
    for page_num, page_data in client.steamspy_iter_all(IterArguments(**kwargs)):

        validate_result = [SteamSpyAllModel(**row) for row in page_data.values()]
        logger.info("SteamSpy validated page=%s records=%s", page_num, len(validate_result))

        full_file_path = parent_path/f"page_{page_num}.parquet"

        _steamspy_write_to_tmp(validate_result, full_file_path)
        logger.info("SteamSpy written %s", page_num)

    return str(parent_path)
