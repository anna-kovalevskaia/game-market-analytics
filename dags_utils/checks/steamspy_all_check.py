import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict

from dags_utils.commons.clickhouse import ClickHouseClient

log = logging.getLogger(__name__)


class CheckError(Exception):
    """Raised when a check fails."""


class Check(BaseModel):
    """DQ thresholds and the table the metrics are written to."""

    model_config = ConfigDict(frozen=True)

    meta_schema_name: str = "raw_dq"
    meta_check_tb_name: str = "steamspy_check"
    warn_threshold: float = 0.20
    error_threshold: float = 0.50


def steamspy_all_check_values(
    path: Path,
    client: ClickHouseClient,
    schema: str,
    table_name: str,
    meta_schema: str,
    meta_table_name: str,
    cur_date: datetime,
    warn_threshold: float = 0.20,
    error_threshold: float = 0.50,
) -> pl.DataFrame:
    median_cols: list[str] = [
        "price",
        "positive",
        "negative",
    ]
    metric_names = ["row_count", *median_cols]

    new_check = (
        pl.scan_parquet(path / "*.parquet")
        .select(
            pl.len().cast(pl.Float64).alias("row_count"),
            pl.col(median_cols).median(),
        )
        .collect()
        .unpivot(variable_name="metrics_name", value_name="metrics_value")
        .with_columns(
            pl.lit(schema).alias("schema_name"),
            pl.lit(table_name).alias("table_name"),
            pl.when(pl.col("metrics_name") == "row_count")
            .then(pl.lit("count"))
            .otherwise(pl.lit("median"))
            .alias("agg_type"),
            pl.lit(cur_date).alias("checked_at"),
        )
    )

    metrics_list = ", ".join(f"'{m}'" for m in metric_names)

    meta_table = f"{meta_schema}.{meta_table_name}"

    prev_check = pl.from_arrow(client.sql_to_arrow(f"""
            WITH (
                SELECT toDate(max(checked_at))
                FROM {meta_table}
                WHERE schema_name = '{schema}'
                  AND table_name  = '{table_name}'
                  AND checked_at <= toDateTime64('{cur_date}', 3, 'UTC')
            ) AS last_check

            SELECT metrics_name, metrics_value
            FROM {meta_table}
            PREWHERE schema_name = '{schema}' AND table_name = '{table_name}'
            WHERE toDate(checked_at) = last_check
              AND metrics_name IN ({metrics_list})
            """))

    if prev_check.is_empty():
        return new_check

    prev = dict(prev_check.iter_rows())
    errors = []

    for row in new_check.iter_rows(named=True):
        metrics_name = row["metrics_name"]
        new_val = row["metrics_value"]
        old_val = prev.get(metrics_name)

        if old_val is None or new_val is None or old_val == 0 or new_val == 0:
            continue

        dev = abs(max(new_val, old_val) / min(new_val, old_val) - 1)

        if dev > error_threshold:
            errors.append(f"{metrics_name}: {dev:.1%} ({old_val} -> {new_val})")
        elif dev > warn_threshold:
            log.warning(
                "%s.%s %s: отклонение %.1f%% (%s -> %s)",
                schema,
                table_name,
                metrics_name,
                dev * 100,
                old_val,
                new_val,
            )

    if errors:
        raise CheckError(
            f"{schema}.{table_name}: отклонение >{error_threshold:.0%}:\n" + "\n".join(errors)
        )

    return new_check
