import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.sources.steamspy import SteamSpyClient
from data_models.steamspy_all import SteamSpyAllModel

logger = logging.getLogger(__name__)


class IterArguments(BaseModel):
    max_pages: int
    stop_after_empty_pages: int
    delay_seconds: float


def _steamspy_write_to_tmp(data: list[SteamSpyAllModel], full_file_path: Path) -> None:
    """Write SteamSpy data to a temporary file."""
    models = [row.model_dump() for row in data]
    df = pl.DataFrame(models)
    df.write_parquet(full_file_path)


def steamspy_all_extract_to_tmp(
    client: SteamSpyClient, run_id: str, file_path: str, **kwargs
) -> str:
    """Process SteamSpy API data."""
    parent_path = Path(file_path) / run_id
    parent_path.mkdir(parents=True, exist_ok=True)

    iter_args = IterArguments(**kwargs)
    for page_num, page_data in client.steamspy_iter_all(
        iter_args.max_pages, iter_args.stop_after_empty_pages, iter_args.delay_seconds
    ):
        validate_result = [SteamSpyAllModel(**row) for row in page_data.values()]
        logger.info("SteamSpy validated page=%s records=%s", page_num, len(validate_result))

        full_file_path = parent_path / f"page_{page_num}.parquet"
        _steamspy_write_to_tmp(validate_result, full_file_path)
        logger.info("SteamSpy written %s", page_num)

    return str(parent_path)
