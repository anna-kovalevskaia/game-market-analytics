import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict

from dags_utils.commons.clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)


class CheckError(Exception):
    """Raised when a metric drifts past the error threshold."""


class Check(BaseModel):

    model_config = ConfigDict(frozen=True)

    MEDIAN_COLUMNS: tuple[str, ...] = ()
    WARN_THRESHOLD: float = 0.20
    ERROR_THRESHOLD: float = 0.50
    COUNT_METRIC: str = "row_count"


def collect_metrics(
    path: Path, raw: type, check: Check, cur_date: datetime, dag_id: str
) -> pl.DataFrame:

    aggregates = [pl.len().cast(pl.Float64).alias(check.COUNT_METRIC)]
    if check.MEDIAN_COLUMNS:
        aggregates.append(pl.col(list(check.MEDIAN_COLUMNS)).median())

    return (
        pl.scan_parquet(path / "*.parquet")
        .select(*aggregates)
        .collect()
        .unpivot(variable_name="metrics_name", value_name="metrics_value")
        .with_columns(
            pl.lit(dag_id).alias("dag_id"),
            pl.lit(raw.schema).alias("schema_name"),
            pl.lit(raw.table_name).alias("table_name"),
            pl.when(pl.col("metrics_name") == check.COUNT_METRIC)
            .then(pl.lit("count"))
            .otherwise(pl.lit("median"))
            .alias("agg_type"),
            pl.lit(cur_date).alias("last_update"),
        )
    )


def fetch_previous_metrics(
    client: ClickHouseClient,
    raw: type,
    raw_dq: type,
    check: Check,
    cur_date: datetime,
    dag_id: str,
) -> dict[str, float]:

    metric_names = [check.COUNT_METRIC, *check.MEDIAN_COLUMNS]
    metrics_list = ", ".join(f"'{name}'" for name in metric_names)

    previous = pl.from_arrow(client.sql_to_arrow(f"""
            WITH (
                SELECT max(last_update)
                FROM {raw_dq.schema}.{raw_dq.table_name}
                WHERE dag_id = '{dag_id}'
                  AND schema_name = '{raw.schema}'
                  AND table_name  = '{raw.table_name}'
                  AND last_update <= toDateTime64('{cur_date}', 3, 'UTC')
            ) AS last_check

            SELECT metrics_name, metrics_value
            FROM {raw_dq.schema}.{raw_dq.table_name}
            PREWHERE dag_id = '{dag_id}'
                 AND schema_name = '{raw.schema}'
                 AND table_name = '{raw.table_name}'
            WHERE toDate(last_update) = last_check
              AND metrics_name IN ({metrics_list})
            """))

    return dict(previous.iter_rows())


def compare_metrics(
    metrics: pl.DataFrame, previous: dict[str, float], raw: type, check: Check
) -> None:

    failures: list[str] = []

    for row in metrics.iter_rows(named=True):
        name = row["metrics_name"]
        new_value = row["metrics_value"]
        old_value = previous.get(name)

        if not old_value or not new_value:
            continue

        drift = abs(max(new_value, old_value) / min(new_value, old_value) - 1)

        if drift > check.ERROR_THRESHOLD:
            failures.append(f"{name}: {drift:.1%} ({old_value} -> {new_value})")
        elif drift > check.WARN_THRESHOLD:
            logger.warning(
                "%s.%s %s drifted %.1f%% (%s -> %s)",
                raw.schema,
                raw.table_name,
                name,
                drift * 100,
                old_value,
                new_value,
            )

    if failures:
        raise CheckError(
            f"{raw.schema}.{raw.table_name} drifted more than "
            f"{check.ERROR_THRESHOLD:.0%}:\n" + "\n".join(failures)
        )


def check_metrics(
    path: Path,
    client: ClickHouseClient,
    raw: type,
    raw_dq: type,
    check: Check,
    cur_date: datetime,
    dag_id: str,
) -> pl.DataFrame:

    logger.info(
        "Checking metrics for %s.%s in %s (dag=%s)", raw.schema, raw.table_name, path, dag_id
    )

    metrics = collect_metrics(path, raw, check, cur_date, dag_id)
    previous = fetch_previous_metrics(client, raw, raw_dq, check, cur_date, dag_id)

    if not previous:
        logger.info(
            "No previous metrics for %s from %s, nothing to compare", raw.table_name, dag_id
        )
        return metrics

    compare_metrics(metrics, previous, raw, check)
    return metrics
