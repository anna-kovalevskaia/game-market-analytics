{{
    config(
    schema = 'meta',
    materialized = 'view'
    )
}}

WITH
appids AS (-- appid can have both 0 & 1 values. We want only 1.
    SELECT
        appid, 
        max(last_update) AS a_last_update
    FROM {{ ref('stg_steampower_appdetails') }}
    GROUP BY appid
    HAVING argMax(success)=1
),
reviews AS (
    SELECT
        appid,
        max(last_update) AS p_last_update,
        argMax(total_reviews, last_update) last_total_reviews
    FROM {{ ref('stg_steampower_appreviews') }}
    GROUP BY appid
)
-- never polled
SELECT appid
FROM appids
LEFT ANTI JOIN reviews
USING (appid)
ORDER BY a_last_update DESC, appid
LIMIT 2000 -- to incremental update and avoid too many requests and time limit

UNION DISTINCT
-- most popular games
SELECT appid
FROM reviews
WHERE toDate(p_last_update) != toDate(now(), 'UTC')
ORDER BY last_total_reviews DESC, appid
LIMIT 1000 -- to incremental update and avoid too many requests and time limit

UNION DISTINCT
-- games not polled for the longest time
SELECT appid
FROM reviews
WHERE toDate(p_last_update) != toDate(now(), 'UTC')
ORDER BY p_last_update, appid
LIMIT 1000 -- to incremental update and avoid too many requests and time limit
