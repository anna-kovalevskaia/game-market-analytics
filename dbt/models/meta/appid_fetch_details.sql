{{
    config(
    schema = 'meta',
    materialized = 'view'
    )
}}

WITH
appids AS (
    SELECT
        appid,
        success,
        release_date,
        max(last_update) OVER () max_last_update -- determine whether the last update was today
    FROM {{ ref('stg_steampower_appdetails') }}
    ORDER BY last_update DESC
    LIMIT 1 BY appid
),
specials AS (
    SELECT
        sp.appid                AS appid,
        toDate(sp.last_update, 'UTC')  AS last_upd
    FROM {{ ref('stg_steampower_specials') }} AS sp
    LEFT ANY JOIN appids AS ap
        ON ap.appid = sp.appid
    WHERE ap.success OR ap.appid = 0
    ORDER BY sp.appid, sp.last_update DESC
    LIMIT 2 BY sp.appid
)
SELECT
    appid,
    'discounts and special offers' AS reason
FROM specials
GROUP BY appid
HAVING (
        max(last_upd) - min(last_upd) = 0 -- special offer just started
        OR max(last_upd) - min(last_upd) > 5 -- special offer ended and restarted
)
    AND max(last_upd) = toDate('{{ airflow_run_date() }}', 'UTC')

UNION ALL

SELECT
    a.appid AS appid,
    'never fetched' AS reason
FROM {{ ref('stg_steampower_appid') }} AS a
LEFT ANTI JOIN appids AS ap
    ON a.appid = ap.appid
LEFT ANTI JOIN {{ ref('stg_steampower_specials') }} AS sp
    ON a.appid = sp.appid
ORDER BY a.last_update DESC, appid
LIMIT 2000 -- to incremental update and avoid too many requests and time limit

UNION ALL

SELECT
    appid,
    'weekly retry by not success and no release_date' AS reason
FROM appids
WHERE toDate('{{ airflow_run_date() }}', 'UTC') =
    toStartOfWeek(toDate('{{ airflow_run_date() }}', 'UTC'))
    AND toDate(max_last_update, 'UTC') != toDate(now(), 'UTC')
    AND (success=0 OR isNull(release_date))
    AND appid NOT IN ( SELECT appid FROM specials)
ORDER BY appid DESC
LIMIT 500 -- to incremental update and avoid too many requests and time limit
