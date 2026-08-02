{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    name,
    developer,
    publisher,
    score_rank,
    positive,
    negative,
    userscore,
    owners,
    price,
    initialprice,
    discount,
    ccu,
    row_hash,
    last_update
FROM {{ source('raw', 'steamspy_all') }}
ORDER BY appid, row_hash, ver DESC
LIMIT 1 by appid, row_hash
