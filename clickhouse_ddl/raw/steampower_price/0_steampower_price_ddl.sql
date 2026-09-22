CREATE TABLE IF NOT EXISTS raw.steampower_price (
    appid Int64,
    name String,
    currency Nullable(String),
    price_initial Nullable(Int64),
    price_final Nullable(Int64),
    discount_percent Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(name), '\\N'), ifNull(toString(currency), '\\N'), ifNull(toString(price_initial), '\\N'), ifNull(toString(price_final), '\\N'), ifNull(toString(discount_percent), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (toDate(last_update), appid, row_hash)