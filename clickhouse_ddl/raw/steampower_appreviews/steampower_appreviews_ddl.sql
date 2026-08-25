CREATE TABLE IF NOT EXISTS raw.steampower_appreviews (
    appid Int64,
    review_score Nullable(Int64),
    review_score_desc Nullable(String),
    total_positive Nullable(Int64),
    total_negative Nullable(Int64),
    total_reviews Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(review_score), '\\N'), ifNull(toString(review_score_desc), '\\N'), ifNull(toString(total_positive), '\\N'), ifNull(toString(total_negative), '\\N'), ifNull(toString(total_reviews), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, row_hash)