{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    dlc_appid,
    currency,
    price_initial,
    price_final,
    discount_percent,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_dlc_price') }}
ORDER BY ver DESC
LIMIT 1 by row_hash, toDate(last_update)
