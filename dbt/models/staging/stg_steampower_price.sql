{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    name,
    currency,
    price_initial,
    price_final,
    discount_percent,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_price') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash, toDate(last_update)
