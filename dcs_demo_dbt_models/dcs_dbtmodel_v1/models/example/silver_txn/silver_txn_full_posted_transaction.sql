{{ config(
    schema='dcs_silver_txn',
    tags=['silver']
) }}

with source_data as (

SELECT *
FROM {{ source('bronze', 'bronze_posted_transaction') }}

)

select *
from source_data