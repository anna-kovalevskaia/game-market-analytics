CREATE TABLE IF NOT EXISTS raw.steamspy_all (
    appid Int64,
    name String,
    developer Nullable(String),
    publisher Nullable(String),
    score_rank Nullable(Int64),
    positive Nullable(Int64),
    negative Nullable(Int64),
    userscore Nullable(Int64),
    owners Nullable(String),
    price Nullable(Float64),
    initialprice Nullable(Float64),
    discount Nullable(Float64),
    ccu Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(name), '\\N'), ifNull(toString(developer), '\\N'), ifNull(toString(publisher), '\\N'), ifNull(toString(score_rank), '\\N'), ifNull(toString(positive), '\\N'), ifNull(toString(negative), '\\N'), ifNull(toString(userscore), '\\N'), ifNull(toString(owners), '\\N'), ifNull(toString(price), '\\N'), ifNull(toString(initialprice), '\\N'), ifNull(toString(discount), '\\N'), ifNull(toString(ccu), '\\N')),
    last_update DateTime64(3, 'UTC') MATERIALIZED toDateTime(now64(6),'UTC')
)
ENGINE = ReplacingMergeTree(last_update)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, row_hash)