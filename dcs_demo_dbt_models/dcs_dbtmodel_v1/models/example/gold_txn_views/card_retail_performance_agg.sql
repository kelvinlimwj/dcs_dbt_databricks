{{ config(
    schema='dcs_gold_txn',
    materialized = 'view',
    tags=['gold']
) }}

WITH joined AS (
    SELECT
        st.product_number,
        st.posting_amount,
        st.posting_date,
        cpi.description AS product_description
    FROM {{ ref('silver_txn_retail_posted_transaction') }} AS st
    LEFT JOIN {{ ref('silver_txn_card_product_info') }} AS cpi
        ON st.product_number = cpi.product_number
)

SELECT
    product_description,
    posting_date,
    SUM(posting_amount) AS total_posting_amount
FROM joined
GROUP BY 1, 2
ORDER BY total_posting_amount DESC