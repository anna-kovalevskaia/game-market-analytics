from airflow.sdk import Asset


def table_asset(meta: type) -> Asset:

    table_name = meta.__module__.rsplit(".", 1)[-1]
    return Asset(f"{meta.schema}.{table_name}")
