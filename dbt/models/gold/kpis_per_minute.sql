{{ config(materialized='external', format='parquet',
          location=env_var('LAKE_ROOT', '/opt/airflow/data') ~ '/gold/kpis_per_minute.parquet') }}
select
    date_trunc('minute', event_ts)            as minute,
    count(*)                                  as txn_count,
    sum(amount)                               as total_amount,
    count(*) filter (where is_flagged)        as flagged_count,
    avg(fraud_score)                          as avg_fraud_score
from {{ ref('stg_transactions') }}
{% if var('run_date', '') %} where event_date = date '{{ var("run_date") }}' {% endif %}
group by 1
