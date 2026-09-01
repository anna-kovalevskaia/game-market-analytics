CREATE TABLE IF NOT EXISTS raw.steampower_appreviews_details (
    appid Int64,
    timestamp_created DateTime64(6),
    timestamp_updated DateTime64(6),
    voted_up UInt8,
    language Nullable(String),
    steam_purchase Nullable(UInt8),
    received_for_free Nullable(UInt8),
    written_during_early_access Nullable(UInt8),
    refunded Nullable(UInt8),
    primarily_steam_deck Nullable(UInt8),
    playtime_at_review Nullable(Int64),
    playtime_forever Nullable(Int64),
    playtime_last_two_weeks Nullable(Int64),
    last_played Nullable(DateTime64(6)),
    row_hash UInt64 MATERIALIZED cityHash64(ifNull(toString(appid), '\\N'), ifNull(toString(timestamp_created), '\\N'), ifNull(toString(timestamp_updated), '\\N'), ifNull(toString(voted_up), '\\N'), ifNull(toString(language), '\\N'), ifNull(toString(steam_purchase), '\\N'), ifNull(toString(received_for_free), '\\N'), ifNull(toString(written_during_early_access), '\\N'), ifNull(toString(refunded), '\\N'), ifNull(toString(primarily_steam_deck), '\\N'), ifNull(toString(playtime_at_review), '\\N'), ifNull(toString(playtime_forever), '\\N'), ifNull(toString(playtime_last_two_weeks), '\\N'), ifNull(toString(last_played), '\\N')),
    last_update DateTime64(3, 'UTC') Default toDateTime(now64(6),'UTC'),
    ver Int64 MATERIALIZED -toUnixTimestamp64Milli(last_update)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toStartOfMonth(timestamp_created)
ORDER BY (appid, row_hash)