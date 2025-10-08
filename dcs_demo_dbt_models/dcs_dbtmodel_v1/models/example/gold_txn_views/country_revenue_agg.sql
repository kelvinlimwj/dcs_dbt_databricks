{{ config(
    schema='dcs_gold_txn',
    materialized='view',
    tags=['gold']
) }}

WITH joined AS (
    SELECT
        a.posting_date,
        b.description AS country_description,
        a.product_number,
        a.posting_amount,
        cpi.description AS card_description
    FROM {{ ref('silver_txn_retail_posted_transaction') }} a
    LEFT JOIN {{ ref('silver_txn_country_code') }} b
        ON a.country_code = b.numeric_country_code
    LEFT JOIN {{ ref('silver_txn_card_product_info') }} AS cpi
        ON a.product_number = cpi.product_number
)

SELECT
    DATE_TRUNC('month', posting_date) AS posting_month,
    country_description,
    CONCAT(card_description, ' (', product_number, ')') AS card,
    SUM(posting_amount) AS total_posting_amount
FROM joined
GROUP BY
    DATE_TRUNC('month', posting_date),
    country_description,
    CONCAT(card_description, ' (', product_number, ')')
ORDER BY
    posting_month,
    country_description,
    card
