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
    discount
FROM {{ source('raw', 'steamspy_all') }}
LIMIT 1 by appid, row_hash
