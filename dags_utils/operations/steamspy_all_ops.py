"""
DEPRECATED — operations for the SteamSpy load, which is no longer scheduled.

See dags_utils/sources/steamspy.py for why the source was dropped. Kept as the
reference implementation of the extract -> validate -> parquet -> ClickHouse
chain that the Steam sources follow.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.check_metrics import Check, check_metrics
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_polars_schema
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
    client: ClickHouseClient,
    run_id_path: Path,
    batch_size: int,
    raw: type,
    meta: type,
    cur_date: datetime,
    check: Check,
) -> None:
    """Validate the staged values, insert them in batches, record the DQ metrics."""

    logger.info("SteamSpy all started to check values in %s", run_id_path)

    metrics = check_metrics(run_id_path, client, raw, meta, check, cur_date)

    client.insert_parquet_to_ch_batch(raw.schema, raw.table_name, run_id_path, batch_size)

    client.insert_polars_to_ch(meta.schema, meta.table_name, metrics)

    shutil.rmtree(run_id_path, ignore_errors=True)
    logger.info("SteamSpy tmp cleaned up: %s", run_id_path)
