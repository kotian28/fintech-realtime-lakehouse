-- singular test: any returned row = failure
select * from {{ ref('fraud_alerts') }} where fraud_score < 50
