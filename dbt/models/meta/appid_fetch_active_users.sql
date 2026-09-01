{{
    config(
    schema = 'meta',
    materialized = 'view'
    )
}}

WITH
appids AS (-- success can be both 0 and 1 for an appid. We want only the last one.
    SELECT
        appid,
        max(last_update) AS a_last_update
    FROM {{ ref('stg_steampower_appdetails') }}
    GROUP BY appid
    HAVING argMax(success, last_update)=1
),
players AS (
    SELECT
        appid,
        max(last_update) AS p_last_update,
        argMax(player_count, last_update) last_player_count
    FROM {{ ref('stg_steampower_active_users') }}
    GROUP BY appid
)
-- never polled
SELECT appid
FROM appids
LEFT ANTI JOIN players
USING (appid)
ORDER BY a_last_update DESC, appid
LIMIT 2000 -- to update incrementally and avoid too many requests and the time limit

UNION DISTINCT
-- most popular games
SELECT appid
FROM players
ORDER BY last_player_count DESC, appid
LIMIT 1000 -- to update incrementally and avoid too many requests and the time limit

UNION DISTINCT
-- games not polled for the longest time
SELECT appid
FROM players
ORDER BY p_last_update, appid
LIMIT 1000 -- to update incrementally and avoid too many requests and the time limit
