from airflow.sdk import Asset


def table_asset(table_config: type) -> Asset:
    """Asset URI for the table a model writes to: '<schema>.<table_name>'."""
    return Asset(f"{table_config.schema}.{table_config.table_name}")
