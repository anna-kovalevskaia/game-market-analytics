{{
    config(
    schema = 'meta',
    materialized = 'view'
    )
}}

WITH
appids AS (-- appid can have both 0 & 1 values. We want only the last ones.
    SELECT appid
    FROM {{ ref('stg_steampower_appdetails') }}
    GROUP BY appid
    HAVING argMax(success, last_update)=1
    ORDER BY max(last_update) DESC, appid
),
players AS (
    SELECT appid
    FROM {{ ref('stg_steampower_active_users') }}
    GROUP BY appid
    ORDER BY argMax(player_count, last_update) DESC
    LIMIT 500 -- to incremental update and avoid too many requests and time limit
)
SELECT appid
FROM appids
LEFT ANTI JOIN players
USING (appid)
LIMIT 2000 -- to incremental update and avoid too many requests and time limit

UNION ALL

SELECT appid
FROM players
