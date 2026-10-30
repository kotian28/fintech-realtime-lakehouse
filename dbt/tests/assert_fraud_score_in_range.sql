select * from {{ ref('stg_transactions') }} where fraud_score < 0 or fraud_score > 100
