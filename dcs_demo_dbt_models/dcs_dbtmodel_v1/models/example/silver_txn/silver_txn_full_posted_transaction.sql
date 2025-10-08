{{ config(
    schema = 'silver_txn',
    tags=['silver']
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_posted_transaction') }}
),

filled_country AS (
    SELECT
        *,
        CASE 
            WHEN country_code IS NULL OR TRIM(country_code) = '' 
                THEN posting_currency_code
            ELSE country_code
        END AS filled_country_code,

        CASE 
            WHEN mcc IS NULL OR TRIM(mcc) = '' 
                THEN '6540'
            ELSE mcc
        END AS updated_mcc
    FROM source_data
),

standardized AS (
    SELECT
        sd.* EXCEPT (country_code, mcc, filled_country_code, updated_mcc),
        COALESCE(
            cc.numeric_country_code,  
            CASE 
                WHEN REGEXP_LIKE(filled_country_code, '^[0-9]+$') THEN filled_country_code 
                ELSE NULL
            END
        ) AS country_code,
        updated_mcc AS mcc
    FROM filled_country AS sd
    LEFT JOIN {{ ref('silver_txn_country_code') }} AS cc
        ON LOWER(sd.filled_country_code) IN (
            LOWER(cc.alpha2_country_code),
            LOWER(cc.alpha3_country_code)
        )
)

SELECT *
FROM standardized
