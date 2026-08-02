
SELECT
    max(toDate(last_update)) AS last_observed_date,
    toDate('{{ airflow_run_date() }}', 'UTC') AS expected_date
FROM {{ ref('stg_steamspy_all') }}
HAVING abs(dateDiff('day', last_observed_date, expected_date)) > 1
