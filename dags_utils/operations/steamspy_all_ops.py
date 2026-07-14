import logging
from typing import Optional, Any

import polars as pl
from pydantic import BaseModel

from pathlib import Path

from dags_utils.sources.steamspy import SteamSpyClient

logger = logging.getLogger(__name__)


class SteamSpyAllModel(BaseModel):

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


def _steamspy_all_parse_json(data: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse JSON response from SteamSpy API."""
    parse_data = [
        {
            "appid": record["appid"],
            "name": record["name"],
            "developer": record.get("developer"),
            "publisher": record.get("publisher"),
            "score_rank": record.get("score_rank"),
            "positive": record.get("positive"),
            "negative": record.get("negative"),
            "userscore": record.get("userscore"),
            "owners": record.get("owners"),
            "average_forever": record.get("average_forever"),
            "average_2weeks": record.get("average_2weeks"),
            "median_forever": record.get("median_forever"),
            "median_2weeks": record.get("median_2weeks"),
            "price": record.get("price"),
            "initialprice": record.get("initialprice"),
            "discount": record.get("discount"),
            "ccu": record.get("ccu")
        }
        for record in data.values()
    ]

    return parse_data


def _steamspy_write_to_tmp(data: list[SteamSpyAllModel], full_file_path: str):
    """Write SteamSpy data to a temporary file."""
    models = [row.model_dump() for row in data]
    df = pl.DataFrame(models)
    df.write_parquet(full_file_path)

    logger.info("SteamSpy written %s", len(df))


def steamspy_all_extract_to_tmp(
        client: SteamSpyClient,
        run_id: str,
        file_path: str,
        max_pages: int,
        allowed_empty_page: int,
        delay_seconds: int
    ) -> str:
    """Process SteamSpy API data."""
  
    for page_num, page_data in client.steamspy_iter_all(max_pages=max_pages, stop_after_empty_pages=allowed_empty_page, delay_seconds=delay_seconds):
        parse_result = _steamspy_all_parse_json(page_data)
        logger.info("SteamSpy parsed %s", page_num)

        type_validate_result = [SteamSpyAllModel(**row) for row in parse_result]
        logger.info("SteamSpy validated %s", page_num)

        parent_path = f"{file_path}/{run_id}"
        Path(parent_path).mkdir(parents=True, exist_ok=True)
        full_file_path = f"{parent_path}/page_{page_num}.parquet"
        _steamspy_write_to_tmp(type_validate_result, full_file_path)
        logger.info("SteamSpy written %s", page_num)
    return parent_path
