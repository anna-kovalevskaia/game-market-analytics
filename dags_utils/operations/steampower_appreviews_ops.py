import logging
import shutil
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.check_metrics import Check, check_metrics
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_polars_schema
from dags_utils.sources.steampower import SteamPowerClient
from data_models.steampower_appreviews import SteamPowerAppreviews

logger = logging.getLogger(__name__)


class IterArguments(BaseModel):
    delay_seconds: float
    appids: list[int]
    batch_size: int


def get_appids_to_fetch(ch_client: ClickHouseClient) -> list[int]:
    table = ch_client.sql_to_arrow("SELECT appid FROM meta.appid_fetch_appreviews")
    return table["appid"].to_pylist()


def _steamappreviews_write_to_tmp(data: list[BaseModel], full_file_path: Path) -> None:
    """Write one page of player counts to a temporary parquet file."""
    models = [row.model_dump() for row in data]

    df = pl.DataFrame(models, schema=model_to_polars_schema(SteamPowerAppreviews))
    df.write_parquet(full_file_path)


def steamappreviews_extract_to_tmp(client: SteamPowerClient, run_id_path: Path, **kwargs) -> None:
    """Exstract Appreviews players data"""
    """Validate Appreviews col types"""
    """Write Appreviews data to a temporary file."""

    run_id_path.mkdir(parents=True, exist_ok=True)

    iter_args = IterArguments(**kwargs)

    pages = client.steampower_get_appreviews(
        iter_args.delay_seconds,
        iter_args.appids,
        iter_args.batch_size,
    )

    for page_num, page_rows in enumerate(pages):
        validate_result = [SteamPowerAppreviews(**row) for row in page_rows]
        logger.info("Appreviews validated page=%s records=%s", page_num, len(validate_result))

        full_file_path = run_id_path / f"page_{page_num}.parquet"
        _steamappreviews_write_to_tmp(validate_result, full_file_path)
        logger.info("Appreviews written page - %s", page_num)


def steamappreviews_parquet_to_clickhouse(
    client: ClickHouseClient,
    run_id_path: Path,
    raw: type,
    raw_dq: type,
    check: Check | None,
    cur_date: str,
    dag_id: str,
    batch_size: int,
) -> None:
    """
    Validate the staged values
    Insert the staged values in batches.
    Record the DQ metrics, then drop the staged files.
    """
    metrics = (
        check_metrics(run_id_path, client, raw, raw_dq, check, cur_date, dag_id) if check else None
    )

    client.insert_parquet_to_ch_batch(raw.schema, raw.table_name, run_id_path, batch_size)

    if metrics is not None:
        client.insert_list_to_ch(raw_dq.schema, raw_dq.table_name, metrics.to_dicts())
        logger.info(
            "Appreviews active users %s.%s was updated by %s rows",
            raw_dq.schema,
            raw_dq.table_name,
            len(metrics),
        )

    shutil.rmtree(run_id_path, ignore_errors=True)
    logger.info("Appreviews tmp cleaned up: %s", run_id_path)
