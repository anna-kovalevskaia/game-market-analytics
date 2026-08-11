import logging
from pathlib import Path

import clickhouse_connect
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
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
        return self._client.query_arrow(sql)

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

            table = pa.concat_tables([pq.read_table(f) for f in chunk])
            try:
                self._client.insert_arrow(f"{schema}.{table_name}", table)
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
