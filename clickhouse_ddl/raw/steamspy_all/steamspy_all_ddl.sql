CREATE TABLE IF NOT EXISTS raw.steamspy_all (
    appid Int64,
    name String,
    developer Nullable(String),
    publisher Nullable(String),
    score_rank Nullable(String),
    positive Nullable(Int64),
    negative Nullable(Int64),
    userscore Nullable(Int64),
    owners Nullable(String),
    average_forever Nullable(Float64),
    average_2weeks Nullable(Float64),
    median_forever Nullable(Float64),
    median_2weeks Nullable(Float64),
    price Nullable(Float64),
    initialprice Nullable(Float64),
    discount Nullable(Float64),
    ccu Nullable(Int64),
    row_hash UInt64 MATERIALIZED cityHash64(appid,name,developer,publisher,score_rank,positive,negative,userscore,owners,average_forever,average_2weeks,median_forever,median_2weeks,price,initialprice,discount,ccu),
    last_update DateTime64(6) DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(last_update)
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, row_hash)