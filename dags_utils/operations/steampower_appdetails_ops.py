import logging
import shutil
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from dags_utils.checks.check_metrics import Check, check_metrics
from dags_utils.commons.clickhouse import ClickHouseClient
from dags_utils.commons.model_types import model_to_polars_schema
from dags_utils.sources.steampower import SteamPowerClient
from data_models.steampower_appdetails import SteamPowerAppdetailsModel
from data_models.steampower_appdetails import TableConfig as SteamPowerDetailsTable
from data_models.steampower_packages import SteamPowerPackagesModel
from data_models.steampower_packages import TableConfig as SteamPowerPackagesTable
from data_models.steampower_price import SteamPowerPriceModel
from data_models.steampower_price import TableConfig as SteamPowerPriceTable

logger = logging.getLogger(__name__)

# The order of the elements must match the result in steampower_get_appdetails.
STREAMS: tuple[tuple[type[BaseModel], type], ...] = (
    (SteamPowerAppdetailsModel, SteamPowerDetailsTable),
    (SteamPowerPriceModel, SteamPowerPriceTable),
    (SteamPowerPackagesModel, SteamPowerPackagesTable),
)


class IterArguments(BaseModel):
    delay_seconds: float
    appids: list[int]
    batch_size: int


def get_appids_to_fetch(ch_client: ClickHouseClient) -> list[int] | None:
    table = ch_client.sql_to_arrow("SELECT appid FROM meta.appid_fetch_details ORDER BY appid")
    if not table:
        logger.warning("No appids to fetch.")
        return None
    return table["appid"].to_pylist()


def _steamappdetails_write_to_tmp(
    data: list[BaseModel],
    model: type[BaseModel],
    full_file_path: Path,
) -> None:
    """Write one Steam appdetails stream to a temporary parquet file."""
    models = [row.model_dump() for row in data]

    df = pl.DataFrame(models, schema=model_to_polars_schema(model))
    df.write_parquet(full_file_path)


def steamappdetails_extract_to_tmp(client: SteamPowerClient, run_id_path: Path, **kwargs) -> None:
    """Exstract SteamPower data"""
    """Validate SteamPower col types"""
    """Write SteamPower data to a temporary file."""

    iter_args = IterArguments(**kwargs)

    for _, table in STREAMS:
        (run_id_path / table.table_name).mkdir(parents=True, exist_ok=True)

    pages = client.steampower_get_appdetails(
        iter_args.delay_seconds,
        iter_args.appids,
        iter_args.batch_size,
    )

    for page_num, page_rows in enumerate(pages):
        for rows, (model, table) in zip(page_rows, STREAMS, strict=True):
            validate_result = [model(**row) for row in rows]
            logger.info(
                "SteamPower validated page=%s table=%s records=%s",
                page_num,
                table.table_name,
                len(validate_result),
            )

            full_file_path = run_id_path / table.table_name / f"page_{page_num}.parquet"
            _steamappdetails_write_to_tmp(validate_result, model, full_file_path)
            logger.info("SteamPower written %s to %s", page_num, table.table_name)


def steamappdetails_metrics_validate(
    client: ClickHouseClient,
    run_id_path: Path,
    raw: type,
    raw_dq: type,
    check: Check | None,
    cur_date: str,
    dag_id: str,
) -> list[dict]:
    """Validate the staged values"""
    if not check:
        return []
    metrics = check_metrics(run_id_path, client, raw, raw_dq, check, cur_date, dag_id)
    return metrics.to_dicts()


def steamappdetails_parquet_to_clickhouse(
    client: ClickHouseClient, run_id_path: Path, batch_size: int, raw: type
) -> None:
    """Insert them in batches, record the DQ metrics."""

    client.insert_parquet_to_ch_batch(raw.schema, raw.table_name, run_id_path, batch_size)


def steamappdetails_update_raw_dq(
    client: ClickHouseClient, run_id_path: Path, raw_dq: type, metrics: list[dict]
) -> None:
    """Record the DQ metrics of every stream, then drop the staged files."""
    if metrics:
        client.insert_list_to_ch(raw_dq.schema, raw_dq.table_name, metrics)
        logger.info(
            "SteamPower %s.%s was updated by %s rows",
            raw_dq.schema,
            raw_dq.table_name,
            len(metrics),
        )

    shutil.rmtree(run_id_path, ignore_errors=True)
    logger.info("SteamPower tmp cleaned up: %s", run_id_path)
