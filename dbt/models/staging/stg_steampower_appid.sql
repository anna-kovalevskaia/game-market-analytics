{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    name,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_appid') }}
ORDER BY appid, row_hash, ver DESC
LIMIT 1 by appid, row_hash
