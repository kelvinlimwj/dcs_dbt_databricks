{{ config(
    schema='silver_txn',
    tags=['silver']
) }}

with source_data as (

select *
from {{ source('bronze', 'bronze_card_product_info') }}

)

select *
from source_data