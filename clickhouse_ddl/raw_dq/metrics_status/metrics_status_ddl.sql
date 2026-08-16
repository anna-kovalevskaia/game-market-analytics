CREATE TABLE IF NOT EXISTS raw_dq.metrics_status (
    schema_name String,
    table_name String,
    metrics_name String,
    agg_type String,
    metrics_value Float64,
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(schema_name), '\\N'), ifNull(toString(table_name), '\\N'), ifNull(toString(metrics_name), '\\N'), ifNull(toString(agg_type), '\\N'), ifNull(toString(metrics_value), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = MergeTree()
PARTITION BY toStartOfMonth(last_update)
ORDER BY (row_hash)