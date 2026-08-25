{{
    config(
    schema = 'staging',
    materialized = 'view'
    )
}}

SELECT
    appid,
    review_score,
    review_score_desc,
    total_positive,
    total_negative,
    total_reviews,
    row_hash,
    last_update
FROM {{ source('steampower', 'steampower_appreviews') }}
ORDER BY ver DESC
LIMIT 1 by appid, row_hash
