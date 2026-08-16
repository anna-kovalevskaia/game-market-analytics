import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.check_metrics import Check, check_metrics
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_polars_schema
from dags_utils.sources.steampower import SteamPowerClient
from data_models.steampower_appid import SteamPowerAppidModel

logger = logging.getLogger(__name__)


class IterArguments(BaseModel):
    delay_seconds: float
    count: int
    sort_by: str
    max_rows: int | None = None


def _steamappid_write_to_tmp(data: list[SteamPowerAppidModel], full_file_path: Path) -> None:
    """Write Steam Search Appid data to a temporary file."""
    models = [row.model_dump() for row in data]

    df = pl.DataFrame(models, schema=model_to_polars_schema(SteamPowerAppidModel))
    df.write_parquet(full_file_path)


def steamappid_extract_to_tmp(client: SteamPowerClient, run_id_path: Path, **kwargs) -> None:
    """Exstract SteamPower data"""
    """Validate SteamPower col types"""
    """Write SteamPower data to a temporary file."""

    run_id_path.mkdir(parents=True, exist_ok=True)

    iter_args = IterArguments(**kwargs)

    pages = client.steampower_iter_search(
        iter_args.delay_seconds, iter_args.count, iter_args.sort_by, iter_args.max_rows
    )

    for page_num, page_rows in enumerate(pages):
        validate_result = [SteamPowerAppidModel(**row) for row in page_rows]
        logger.info("SteamPower validated page=%s records=%s", page_num, len(validate_result))

        full_file_path = run_id_path / f"page_{page_num}.parquet"
        _steamappid_write_to_tmp(validate_result, full_file_path)
        logger.info("SteamPower written %s", page_num)


def steamappid_parquet_to_clickhouse(
    client: ClickHouseClient,
    run_id_path: Path,
    batch_size: int,
    raw: type,
    raw_dq: type,
    check: Check | None,
    cur_date: datetime,
) -> None:
    """Validate the staged values, insert them in batches, record the DQ metrics."""

    logger.info("SteamPowerAppid all started to check values in %s", run_id_path)

    if check:
        metrics = check_metrics(run_id_path, client, raw, raw_dq, check, cur_date)

    client.insert_parquet_to_ch_batch(raw.schema, raw.table_name, run_id_path, batch_size)

    if check:
        client.insert_polars_to_ch(raw_dq.schema, raw_dq.table_name, metrics)

    shutil.rmtree(run_id_path, ignore_errors=True)
    logger.info("SteamPower tmp cleaned up: %s", run_id_path)
