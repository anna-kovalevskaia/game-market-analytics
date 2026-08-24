{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    player_count,
    last_update
FROM {{ source('steampower', 'steampower_active_users') }}
ORDER BY ver DESC
LIMIT 1 by appid, toStartOfHour(last_update)
