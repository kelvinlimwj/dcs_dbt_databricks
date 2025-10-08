{{ config(
    schema='dcs_silver_txn',
    materialized = 'table',
    tags=['silver']
) }}

with source_data as (

select *
from {{ source('bronze', 'bronze_card_product_info') }}

)

select *
from source_data