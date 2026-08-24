{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    name,
    player_count,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_active_users') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash, toStartOfHour(last_update)
