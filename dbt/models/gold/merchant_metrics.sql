{{ config(materialized='external', format='parquet',
          location=env_var('LAKE_ROOT', '/opt/airflow/data') ~ '/gold/merchant_metrics.parquet') }}
select
    merchant_id,
    any_value(merchant_category)                           as merchant_category,
    count(*)                                               as txn_count,
    sum(amount)                                            as total_amount,
    count(*) filter (where is_flagged)                     as flagged_count,
    round(count(*) filter (where is_flagged) * 1.0 / count(*), 4) as flag_rate
from {{ ref('stg_transactions') }}
group by 1
