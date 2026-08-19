CREATE TABLE IF NOT EXISTS raw.steampower_appdetails (
    success UInt8,
    appid Int64,
    name Nullable(String),
    required_age Nullable(Int64),
    is_free Nullable(UInt8),
    supported_languages Nullable(String),
    website Nullable(String),
    pc_requirements Nullable(String),
    mac_requirements Nullable(String),
    linux_requirements Nullable(String),
    developers Array(String),
    publishers Array(String),
    categories_id Array(Int64),
    categories_description Array(LowCardinality(String)),
    genres_id Array(LowCardinality(String)),
    genres_description Array(LowCardinality(String)),
    release_date Nullable(DateTime64(6)),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(success), '\\N'), ifNull(toString(appid), '\\N'), ifNull(toString(name), '\\N'), ifNull(toString(required_age), '\\N'), ifNull(toString(is_free), '\\N'), ifNull(toString(supported_languages), '\\N'), ifNull(toString(website), '\\N'), ifNull(toString(pc_requirements), '\\N'), ifNull(toString(mac_requirements), '\\N'), ifNull(toString(linux_requirements), '\\N'), ifNull(toString(developers), '\\N'), ifNull(toString(publishers), '\\N'), ifNull(toString(categories_id), '\\N'), ifNull(toString(categories_description), '\\N'), ifNull(toString(genres_id), '\\N'), ifNull(toString(genres_description), '\\N'), ifNull(toString(release_date), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, row_hash)