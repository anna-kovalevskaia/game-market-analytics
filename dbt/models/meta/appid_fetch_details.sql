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
        last_update
    FROM {{ ref('stg_steampower_appdetails') }}
    ORDER BY last_update DESC
    LIMIT 1 BY appid
),
specials AS (
    SELECT
        sp.appid                              AS appid,
        toDate(sp.last_update, 'UTC')         AS last_upd
    FROM {{ ref('stg_steampower_specials') }} AS sp
    LEFT ANY JOIN appids AS ap
        ON ap.appid = sp.appid
    WHERE ap.success OR ap.appid = 0
    ORDER BY sp.appid, sp.last_update DESC
    LIMIT 2 BY sp.appid
)
--discounts and special offers
SELECT
    appid
FROM specials
GROUP BY appid
HAVING (
        max(last_upd) - min(last_upd) = 0 -- special offer just started
        OR max(last_upd) - min(last_upd) > 5 -- special offer ended and restarted
)
    AND max(last_upd) = toDate('{{ airflow_run_date() }}', 'UTC')

UNION DISTINCT
--never fetched
SELECT
    a.appid AS appid
FROM {{ ref('stg_steampower_appid') }} AS a
LEFT ANTI JOIN appids AS ap
    ON a.appid = ap.appid
LEFT ANTI JOIN specials AS sp
    ON a.appid = sp.appid
ORDER BY a.last_update DESC, appid
LIMIT 500 -- to incremental update and avoid too many requests and time limit

UNION DISTINCT
--not success or no release_date
SELECT
    appid
FROM appids
WHERE (success=0 OR isNull(release_date))
ORDER BY last_update, appid
LIMIT 7000 -- to incremental update and avoid too many requests and time limit
