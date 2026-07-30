import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.steamspy_all_check import steamspy_all_check_values
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.sources.steamspy import SteamSpyClient
from data_models.steamspy_all import SteamSpyAllModel

logger = logging.getLogger(__name__)


class CheckError(Exception):
    """Raised when a check fails."""


class IterArguments(BaseModel):
    max_pages: int
    stop_after_empty_pages: int
    delay_seconds: float


class CheckIterArguments(BaseModel):
    client: ClickHouseClient
    schema_name: str
    table_name: str
    meta_check_tb_name: str
    cur_datetime: str
    warn_threshold: float
    error_threshold: float


def _steamspy_write_to_tmp(data: list[SteamSpyAllModel], full_file_path: Path) -> None:
    """Write SteamSpy data to a temporary file."""
    models = [row.model_dump() for row in data]
    df = pl.DataFrame(models)
    df.write_parquet(full_file_path)


def steamspy_all_extract_to_tmp(
    client: SteamSpyClient, run_id: str, file_path: str, **kwargs
) -> str:
    """Process SteamSpy API data."""
    """Exstract SteamSpy data"""
    """Validate SteamSpy col types"""
    """Write SteamSpy data to a temporary file."""

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


def steamspy_all_parquet_to_clickhouse(result: str, batch_size: int, **kwargs) -> None:
    """Validate SteamSpy values"""
    """Insert data to clickhouse by batches"""
    """Update meta.steamspy_check"""
    dir_path = result

    logger.info("SteamSpy all started to check values %s")
    f_arguments = CheckIterArguments(**kwargs)

    new_values_check = steamspy_all_check_values(
        f_arguments.client,
        f_arguments.schema_name,
        f_arguments.table_name,
        f_arguments.cur_datetime,
        f_arguments.warn_threshold,
        f_arguments.error_threshold,
    )

    f_arguments.client.batch_insert_from_parquet(
        f_arguments.schema_name, f_arguments.table_name, dir_path, batch_size
    )

    f_arguments.client.insert_polars_to_cl(
        f_arguments.schema_name, f_arguments.meta_check_tb_name, new_values_check
    )
