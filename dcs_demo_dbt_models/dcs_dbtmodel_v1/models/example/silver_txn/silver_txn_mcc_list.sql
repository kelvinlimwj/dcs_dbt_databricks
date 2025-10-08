{{ config(
    schema='dcs_silver_txn',
    tags=['silver']
) }}

with source_data as (

select *
from {{ source('bronze', 'bronze_mcc_list') }}

)

select *
from source_data