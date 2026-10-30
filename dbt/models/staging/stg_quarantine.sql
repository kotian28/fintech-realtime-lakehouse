select * from {{ source('lake', 'quarantine_transactions') }}
