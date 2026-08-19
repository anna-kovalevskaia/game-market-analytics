CREATE TABLE IF NOT EXISTS raw.steampower_packages (
    appid Int64,
    packageid Int64,
    package_option_text Nullable(String),
    package_price_with_discount Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(packageid), '\\N'), ifNull(toString(package_option_text), '\\N'), ifNull(toString(package_price_with_discount), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, packageid, row_hash)