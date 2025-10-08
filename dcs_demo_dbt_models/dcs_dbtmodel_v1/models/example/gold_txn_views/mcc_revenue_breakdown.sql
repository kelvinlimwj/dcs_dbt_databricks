{{ config(
    schema='gold_txn',
    materialized = 'view',
    tags=['gold']
) }}

WITH joined AS (
    SELECT
        st.posting_date,
        st.posting_amount,
        cc.description AS country,
        cpi.description AS card_description,
        cpi.product_number,
        ml.dxs_description
    FROM {{ ref('silver_txn_retail_posted_transaction') }} AS st
    LEFT JOIN {{ ref('silver_txn_mcc_list') }} AS ml
        ON st.mcc = ml.mcc_code
    LEFT JOIN {{ ref('silver_txn_country_code') }} AS cc
        ON st.country_code = cc.numeric_country_code
    LEFT JOIN {{ ref('silver_txn_card_product_info') }} AS cpi
        ON st.product_number = cpi.product_number
)

SELECT
    EXTRACT(YEAR FROM posting_date) AS posting_year,
    EXTRACT(MONTH FROM posting_date) AS posting_month,
    country,
    CONCAT(card_description, ' (', product_number, ')') AS card,
    dxs_description,
    SUM(posting_amount) AS total_posting_amount
FROM joined
GROUP BY
    posting_year,
    posting_month,
    country,
    card,
    dxs_description
ORDER BY
    posting_year,
    posting_month,
    country,
    card
