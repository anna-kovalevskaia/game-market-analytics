
{% macro airflow_run_date() %}
    {%- set run_date = var('run_date', none) -%}
    {%- if run_date is none -%}
        {{ log("run_date var not supplied (dbt is running outside Airflow) — falling back to 2026-01-01", info=True) }}
        {{- return('2026-01-01') -}}
    {%- endif -%}
    {{- return(run_date) -}}
{% endmacro %}
