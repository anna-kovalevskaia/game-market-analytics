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
    last_update DateTime64(6) DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(last_update)
ORDER BY (appid, name)