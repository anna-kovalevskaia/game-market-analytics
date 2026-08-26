CREATE TABLE IF NOT EXISTS raw.steampower_apptag (
    appid Int64,
    tagid Nullable(Int64),
    tag_name Nullable(String),
    count Nullable(Int64),
    browseable Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(tagid), '\\N'), ifNull(toString(tag_name), '\\N'), ifNull(toString(count), '\\N'), ifNull(toString(browseable), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY (toDate(last_update), appid, row_hash)