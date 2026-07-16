from typing import Any, List, Sequence

import clickhouse_connect  # или clickhouse_driver, или requests
from clickhouse_connect.driver.client import Client

from dags_utils.sources.steamspy import SteamSpyAllModel


class ClickHouseClient:

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        secure: bool = False,
    ) -> None:
        self._client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=secure,
        )

    def model_to_clickhouse_columns(self, model: type[SteamSpyAllModel]) -> List[str]:

        return [
            f"{name} Nullable(String)" if annotation == str else
            f"{name} Nullable(Int64)" if annotation == int else
            f"{name} Nullable(Float64)" if annotation == float else
            f"{name} Nullable(String)"  # fallback
            for name, annotation in model.model_fields.items()
        ]

    def create_table_if_not_exists(self, table_name: str, model: type[SteamSpyAllModel]) -> None:
        columns_sql = ", ".join(self.model_to_clickhouse_columns(model))
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        ({columns_sql})
        ENGINE = MergeTree()
        ORDER BY appid
        """
        self._client.command(sql)

    def insert_data(self, table_name: str, data: Sequence[SteamSpyAllModel]) -> None:
        """
        Вставляет список Pydantic-моделей в таблицу.
        clickhouse_connect умеет принимать список dict'ов.
        """
        rows = [row.model_dump() for row in data]
        self._client.insert(table=table_name, data=rows, column_names=[f.name for f in SteamSpyAllModel.model_fields])

    def drop_table_if_exists(self, table_name: str) -> None:
        self._client.command(f"DROP TABLE IF EXISTS {table_name}")