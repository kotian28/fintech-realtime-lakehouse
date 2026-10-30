{{ config(materialized='external', format='parquet',
          location=env_var('LAKE_ROOT', '/opt/airflow/data') ~ '/gold/fraud_alerts.parquet') }}
select transaction_id, user_id, merchant_id, amount, currency, card_country, merchant_country,
       channel, fraud_score, event_ts
from {{ ref('stg_transactions') }}
where is_flagged
order by event_ts desc
limit 1000
