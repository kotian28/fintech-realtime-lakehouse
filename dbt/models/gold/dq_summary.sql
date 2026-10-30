{{ config(materialized='external', format='parquet',
          location=env_var('LAKE_ROOT', '/opt/airflow/data') ~ '/gold/dq_summary.parquet') }}
select rule, count(*) as failures
from (select unnest(failed_rules) as rule from {{ ref('stg_quarantine') }})
group by 1
