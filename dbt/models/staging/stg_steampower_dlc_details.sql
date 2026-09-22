{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    dlc_appid,
    dlc_name,
    type,
    is_free,
    release_date,
    platform_windows,
    platform_mac,
    platform_linux,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_dlc_details') }}
ORDER BY ver DESC
LIMIT 1 by row_hash
