{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    packageid,
    package_option_text,
    package_price_with_discount,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_packages') }}
ORDER BY ver DESC
LIMIT 1 by appid, packageid, row_hash
