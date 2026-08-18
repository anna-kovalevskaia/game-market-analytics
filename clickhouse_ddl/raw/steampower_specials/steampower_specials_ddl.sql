CREATE TABLE IF NOT EXISTS raw.steampower_specials (
    appid Int64,
    name String,
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(name), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (toDate(last_update), appid, row_hash)