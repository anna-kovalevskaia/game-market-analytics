CREATE TABLE IF NOT EXISTS meta.steamspy_check (
    schema_name String,
    table_name String,
    metrics_name String,
    agg_type String,
    metrics_value Float64,
    checked_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (table_name, checked_at)