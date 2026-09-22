{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    dlc_appid,
    packageid,
    package_option_text,
    package_price_with_discount,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_dlc_packages') }}
ORDER BY ver DESC
LIMIT 1 by row_hash
