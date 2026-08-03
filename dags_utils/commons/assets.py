from airflow.sdk import Asset


def table_asset(meta: type) -> Asset:
    """Asset URI for the table a model writes to: '<schema>.<table_name>'."""
    return Asset(f"{meta.schema}.{meta.table_name}")
