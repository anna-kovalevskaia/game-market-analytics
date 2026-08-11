{{
    config(
    materialized = 'table',
    schema = 'meta',
    order_by = '(appid)',
    )
}}
{% set this = adapter.get_relation(this.database, this.schema, this.identifier) %}

{% if execute and this is not none -%}
WITH
    ifNull(this.row_hash_cur, 0) != nullIf(stg.row_hash, 0) AS compared, -- hash differs from the stored one; NULL when the game is missing from the snapshot
    toDate(last_update) > (SELECT toDate(max(last_changed)) FROM {{ this }}) AS is_new_date,    -- rerun guard: false once this run_date is already recorded
    if(is_new_date, compared, this.is_changed) AS changed    -- use the fresh comparison on the first run for this date, the stored flag on a repeat run
SELECT
    appid,
    if(changed, stg.row_hash, this.row_hash_cur) as row_hash_cur,
    ifNull(changed, 0) as is_changed,
    if(changed, stg.last_update, this.last_changed) as last_changed
FROM {{ this }} as this
FULL OUTER JOIN (
    SELECT
        appid,
        row_hash,
        last_update
    FROM {{ ref('stg_steamspy_all') }}
    WHERE toDate(last_update) = toDate('{{ airflow_run_date() }}', 'UTC')
) AS stg
USING (appid)
{% else %}
SELECT
    appid,
    row_hash    AS row_hash_cur,
    true        AS is_changed,
    last_update AS last_changed
FROM {{ ref('stg_steamspy_all') }}
ORDER BY appid, last_update DESC
LIMIT 1 BY appid
{% endif %}
