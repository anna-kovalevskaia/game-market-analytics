{{
    config(
    schema = 'meta',
    materialized = 'view'
    )
}}

WITH
appids AS (-- appid can have both 0 & 1 values. We want only the last ones.
    SELECT
        appid, 
        max(last_update) AS a_last_update
    FROM {{ ref('stg_steampower_appdetails') }}
    GROUP BY appid
    HAVING argMax(success, last_update)=1
),
tags AS (
    SELECT
        appid,
        max(last_update) AS t_last_update
    FROM {{ ref('stg_steampower_apptag') }}
    GROUP BY appid
)
-- never polled
SELECT
    appid,
    'never polled' AS reson
FROM appids
LEFT ANTI JOIN tags
USING (appid)
ORDER BY a_last_update DESC, appid
LIMIT 2000 -- to incremental update and avoid too many requests and time limit

UNION ALL
-- games not polled for the longest time
SELECT
    appid,
    'not polled for the longest time' AS reson
FROM tags
ORDER BY t_last_update, appid
LIMIT 1500 -- to incremental update and avoid too many requests and time limit
