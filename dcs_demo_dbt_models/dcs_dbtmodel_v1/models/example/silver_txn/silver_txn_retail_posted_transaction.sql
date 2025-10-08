{{ config(
    schema='dcs_silver_txn',
    tags=['silver']
) }}

with source_data as (

SELECT *
FROM {{ ref('silver_txn_full_posted_transaction') }}
WHERE posting_transaction_code IN ('GL491', 'IZ106', 'UB436', 'RA786', 'IX009', 'EE789')
)

select *
from source_data