import logging
from pathlib import Path

import clickhouse_connect
import polars as pl
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
            database=conn.schema,
            secure=conn.extra_dejson.get("secure", False),
        )

    def execute_sql(self, sql: str) -> None:
        logger.info("ClickHouse execute: %s", sql[:200])
        self._client.command(sql)

    def create_table_from_data_model(
        self,     
        table_name: str,
        columns: list[str],  # [(name, clickhouse_type), ...]
        order_by: str,
        engine: str = "MergeTree",
        partition_by: str = "toStartOfMonth(last_update)"
    ) -> None:

        cols_sql = ",\n    ".join(columns)
        cols_sql += ",\n    last_update DateTime64(6) DEFAULT now64(6)"

        parts = [
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols_sql}\n)",
            f"ENGINE = {engine}",
        ]
        if partition_by:
            parts.append(f"PARTITION BY {partition_by}")
        parts.append(f"ORDER BY ({order_by})")
        self.execute_sql("\n".join(parts))

    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """DROP TABLE [IF EXISTS]."""
        clause = "IF EXISTS " if if_exists else ""
        self.execute_sql(f"DROP TABLE {clause}`{table_name}`")

    def insert_from_parquet_dir(self, table_name: str, dir_path: str, batch_size: int) -> None:
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
            df = pl.concat([pl.read_parquet(f) for f in chunk])
            try:
                self._client.insert(table_name, df.rows(), column_names=df.columns)
            except Exception as exc:
                raise ClickHouseOperationError(
                    f"insert failed for batch {i}-{i + len(chunk)} into {table_name!r}"
                ) from exc
            logger.info(
                "ClickHouse inserted batch %s-%s (%s files) into %s",
                i,
                i + len(chunk),
                len(chunk),
                table_name,
            )
