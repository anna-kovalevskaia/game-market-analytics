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
        argMax(dlc, last_update) AS dlc,
        max(last_update) AS a_last_update
    FROM {{ ref('stg_steampower_appdetails') }}
    GROUP BY appid
    HAVING max(success)=1
),
game_dlc AS (-- one row per add-on: the card holds them as an array
    SELECT
        appid,
        arrayJoin(dlc) AS dlc_appid,
        a_last_update
    FROM appids
),
fetched AS (
    SELECT
        appid,
        dlc_appid,
        max(last_update) AS d_last_update
    FROM {{ ref('stg_steampower_dlc_details') }}
    GROUP BY appid, dlc_appid
),
specials AS (
    SELECT
        ap.dlc                                AS dlc,
        toDate(sp.last_update, 'UTC')         AS last_upd
    FROM {{ ref('stg_steampower_specials') }} AS sp
    JOIN appids AS ap
        ON ap.appid = sp.appid
    ORDER BY sp.appid, sp.last_update DESC
    LIMIT 2 BY sp.appid
)
-- never polled
SELECT dlc_appid
FROM game_dlc
LEFT ANTI JOIN fetched
USING (appid, dlc_appid)
ORDER BY a_last_update DESC, dlc_appid
LIMIT 7000 -- to update incrementally and avoid too many requests and the time limit

UNION DISTINCT
-- discounts and special offers
SELECT
    arrayJoin(dlc) AS dlc_appid
FROM specials
GROUP BY dlc
HAVING (
        max(last_upd) - min(last_upd) = 0 -- special offer just started
        OR max(last_upd) - min(last_upd) > 5 -- special offer ended and restarted
)
    AND max(last_upd) = toDate('{{ airflow_run_date() }}', 'UTC')

UNION DISTINCT
-- dlcs not polled for the longest time
SELECT dlc_appid
FROM fetched
WHERE toDate(d_last_update, 'UTC') != toDate('{{ airflow_run_date() }}', 'UTC')
ORDER BY d_last_update, dlc_appid
LIMIT 100 -- to update incrementally and avoid too many requests and the time limit
