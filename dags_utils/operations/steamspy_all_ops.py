import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.steamspy_all_check import steamspy_all_check_values
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_polars_schema
from dags_utils.sources.steamspy import SteamSpyClient
from data_models.steamspy_all import SteamSpyAllModel

logger = logging.getLogger(__name__)


class IterArguments(BaseModel):
    max_pages: int
    stop_after_empty_pages: int
    delay_seconds: float


class CheckIterArguments(BaseModel):
    schema_name: str
    table_name: str
    meta_schema_name: str
    meta_check_tb_name: str
    cur_date: datetime
    warn_threshold: float
    error_threshold: float


def _steamspy_write_to_tmp(data: list[SteamSpyAllModel], full_file_path: Path) -> None:
    """Write SteamSpy data to a temporary file."""
    models = [row.model_dump() for row in data]

    df = pl.DataFrame(models, schema=model_to_polars_schema(SteamSpyAllModel))
    df.write_parquet(full_file_path)


def steamspy_all_extract_to_tmp(client: SteamSpyClient, run_id_path: Path, **kwargs) -> None:
    """Exstract SteamSpy data"""
    """Validate SteamSpy col types"""
    """Write SteamSpy data to a temporary file."""

    run_id_path.mkdir(parents=True, exist_ok=True)

    iter_args = IterArguments(**kwargs)

    for page_num, page_data in client.steamspy_iter_all(
        iter_args.max_pages, iter_args.stop_after_empty_pages, iter_args.delay_seconds
    ):
        validate_result = [SteamSpyAllModel(**row) for row in page_data.values()]
        logger.info("SteamSpy validated page=%s records=%s", page_num, len(validate_result))

        full_file_path = run_id_path / f"page_{page_num}.parquet"
        _steamspy_write_to_tmp(validate_result, full_file_path)
        logger.info("SteamSpy written %s", page_num)


def steamspy_all_parquet_to_clickhouse(
    client: ClickHouseClient, run_id_path: Path, batch_size: int, **kwargs
) -> None:
    """Validate SteamSpy values"""
    """Insert data to clickhouse by batches"""
    """Update meta.steamspy_check"""

    f_arguments = CheckIterArguments(**kwargs)

    logger.info("SteamSpy all started to check values in %s", run_id_path)

    new_values_check = steamspy_all_check_values(
        run_id_path,
        client,
        f_arguments.schema_name,
        f_arguments.table_name,
        f_arguments.meta_schema_name,
        f_arguments.meta_check_tb_name,
        f_arguments.cur_date,
        f_arguments.warn_threshold,
        f_arguments.error_threshold,
    )

    client.insert_parquet_to_ch_batch(
        f_arguments.schema_name, f_arguments.table_name, run_id_path, batch_size
    )

    client.insert_polars_to_ch(
        f_arguments.meta_schema_name, f_arguments.meta_check_tb_name, new_values_check
    )

    shutil.rmtree(run_id_path, ignore_errors=True)
    logger.info("SteamSpy tmp cleaned up: %s", run_id_path)
