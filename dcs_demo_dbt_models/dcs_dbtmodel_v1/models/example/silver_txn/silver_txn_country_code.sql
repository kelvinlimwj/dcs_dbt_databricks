{{ config(
    schema = 'dcs_silver_txn',
    materialized = 'table',
    tags=['silver']
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_country_code') }}
),

legacy_mapping AS (
    SELECT '280' AS old_code, '276' AS new_code  -- Germany
    UNION ALL
    SELECT '901' AS old_code, '036' AS new_code  -- Australia
),

legacy_rows AS (
    SELECT
        sd.alpha3_country_code,
        sd.alpha2_country_code,
        lm.old_code AS numeric_country_code,
        sd.description
    FROM source_data sd
    INNER JOIN legacy_mapping lm
        ON sd.numeric_country_code = lm.new_code
),

-- Add missing countries
new_rows AS (
    SELECT 'MUS' AS alpha3_country_code, 'MU' AS alpha2_country_code, '230' AS numeric_country_code, 'Mauritius' AS description
),

final AS (
    SELECT * FROM source_data
    UNION ALL
    SELECT * FROM legacy_rows
    UNION ALL
    SELECT * FROM new_rows
)

SELECT *
FROM final