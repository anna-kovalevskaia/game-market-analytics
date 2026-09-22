CREATE TABLE IF NOT EXISTS raw.steampower_dlc_details (
    appid Int64,
    dlc_appid Int64,
    dlc_name String,
    type String,
    is_free UInt8,
    release_date Nullable(DateTime64(6)),
    platform_windows UInt8,
    platform_mac UInt8,
    platform_linux UInt8,
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(dlc_appid), '\\N'), ifNull(toString(dlc_name), '\\N'), ifNull(toString(type), '\\N'), ifNull(toString(is_free), '\\N'), ifNull(toString(release_date), '\\N'), ifNull(toString(platform_windows), '\\N'), ifNull(toString(platform_mac), '\\N'), ifNull(toString(platform_linux), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, dlc_appid, row_hash)