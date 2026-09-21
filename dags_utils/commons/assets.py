
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import Asset, AssetWatcher


def table_asset(table_config: type) -> Asset:
    """Asset URI for the table a model writes to: '<schema>.<table_name>'."""
    return Asset(f"{table_config.schema}.{table_config.table_name}")


def table_asset_watcher(file_path: str, asset_name: str, asset_watcher_name: str) -> Asset:
    trigger_filedelete = FileDeleteTrigger(filepath=file_path, poke_interval=30)
    return Asset(
        asset_name,
        watchers=[
            AssetWatcher(
                name=asset_watcher_name,
                trigger=trigger_filedelete
            )
        ],

    )

