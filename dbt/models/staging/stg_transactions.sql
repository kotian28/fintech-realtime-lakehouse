select * from {{ source('lake', 'silver_transactions') }}
