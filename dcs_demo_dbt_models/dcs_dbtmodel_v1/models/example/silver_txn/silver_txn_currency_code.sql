{{ config(
    schema = 'dcs_silver_txn',
    materialized = 'table',
    tags=['silver']
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_currency_code') }}
)

SELECT *
FROM source_data
