{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    tagid,
    tag_name,
    count,
    browseable,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_apptag') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash
