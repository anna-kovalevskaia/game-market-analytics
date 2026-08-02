import logging
from pathlib import Path

import clickhouse_connect
import polars as pl
import pyarrow as pa
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class ClickHouseParameterError(Exception):
    """Raised on invalid parameters passed to ClickHouseClient methods."""


class ClickHouseOperationError(Exception):
    """Raised when a ClickHouse operation (query/insert) fails."""


class ClickHouseClient:

    def __init__(self, conn_id: str = "clickhouse_default") -> None:
        conn = BaseHook.get_connection(conn_id)
        self._client = clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password or "",
            secure=conn.extra_dejson.get("secure", False),
        )

    def execute_sql(self, sql: str) -> None:
        logger.info("ClickHouse execute: %s", sql[:200])
        self._client.command(sql)

    def sql_to_arrow(self, sql: str) -> pa.Table:
        """Converts a SQL query to an Arrow table."""
        logger.info("ClickHouse execute: %s", sql[:200])
        self._client.query_arrow(sql)

    @staticmethod
    def create_ddl_from_data_model(
        schema: str,  # ClickHouse schema/database name
        table_name: str,
        columns: list[tuple[str, str]],  # [(name, clickhouse_type), ...]
        order_by: str,
        engine: str = "MergeTree",
        partition_by: str = "toStartOfMonth(last_update)",
    ) -> str:

        cols_sql = ",\n    ".join(
            [f"{name} {clickhouse_type}" for name, clickhouse_type in columns]
        )

        hash_args = ", ".join(f"ifNull(toString({name}), '\\\\N')" for name, _ in columns)
        cols_sql += f",\n    row_hash UInt64 MATERIALIZED cityHash64({hash_args})"
        cols_sql += ",\n    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC')"
        cols_sql += ",\n    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)"

        parts = [
            f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n    {cols_sql}\n)",
            f"ENGINE = {engine}",
        ]
        if partition_by:
            parts.append(f"PARTITION BY {partition_by}")
        parts.append(f"ORDER BY ({order_by})")
        return "\n".join(parts)

    def drop_table(self, schema: str, table_name: str, if_exists: bool = True) -> None:
        """DROP TABLE [IF EXISTS]."""
        clause = "IF EXISTS " if if_exists else ""
        self.execute_sql(f"DROP TABLE {clause} {schema}.{table_name} ")

    def insert_polars_to_ch(self, schema: str, table_name: str, pl_df: pl.DataFrame) -> None:
        """Insert polars DataFrame to ClickHouse table."""
        self._client.insert(f"{schema}.{table_name}", pl_df.rows(), column_names=pl_df.columns)

    def insert_parquet_to_ch_batch(
        self, schema: str, table_name: str, dir_path: str | Path, batch_size: int
    ) -> None:
        """Insert data from Parquet files in a directory into a ClickHouse table."""
        if batch_size < 1:
            raise ClickHouseParameterError(f"batch_size must be >= 1, got {batch_size}")

        root = Path(dir_path)
        if not root.is_dir():
            raise ClickHouseParameterError(f"dir_path is not a directory: {dir_path}")

        files = sorted(root.glob("*.parquet"))
        if not files:
            logger.warning("no parquet files in %s", dir_path)
            return

        for i in range(0, len(files), batch_size):
            chunk = files[i : i + batch_size]
            # "vertical_relaxed" resolves a common supertype instead of demanding
            # identical schemas, so a file written before a model change (e.g. an
            # all-null column typed as Null) still concatenates.
            df = pl.concat([pl.read_parquet(f) for f in chunk], how="vertical_relaxed")
            try:
                self.insert_polars_to_ch(schema=schema, table_name=table_name, pl_df=df)
            except Exception as exc:
                raise ClickHouseOperationError(
                    f"insert failed for batch {i}-{i + len(chunk)} into {schema}.{table_name!r}"
                ) from exc
            logger.info(
                "ClickHouse inserted batch %s-%s (%s files) into %s",
                i,
                i + len(chunk),
                len(chunk),
                f"{schema}.{table_name}",
            )
