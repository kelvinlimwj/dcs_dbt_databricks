{{ config(
    schema='dcs_silver_txn',
    tags=['silver']
) }}

with source_data as (

select *
from {{ source('bronze', 'bronze_transaction_code') }}

)

select *
from source_data