{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    timestamp_created,
    timestamp_updated,
    voted_up,
    language,
    steam_purchase,
    received_for_free,
    written_during_early_access,
    refunded,
    primarily_steam_deck,
    playtime_at_review,
    playtime_forever,
    playtime_last_two_weeks,
    last_played,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_appreviews_details') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash
