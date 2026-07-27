CREATE TABLE IF NOT EXISTS meta.check_meta (
    id UInt64,
    schema_name String,
    table_name String,
    metrics_name String,
    agg_type String,
    checked_at DateTime64(6) DEFAULT now64(6)
)
ENGINE = MergeTree
ORDER BY (id)