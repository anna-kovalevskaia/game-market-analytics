{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    success,
    appid,
    name,
    required_age,
    is_free,
    supported_languages,
    website,
    pc_requirements,
    mac_requirements,
    linux_requirements,
    developers,
    publishers,
    categories_id,
    categories_description,
    genres_id,
    genres_description,
    release_date,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_appdetails') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash
