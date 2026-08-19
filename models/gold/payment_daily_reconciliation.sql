{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(transaction_time) AS transaction_date,
    payment_method,
    payment_status,
    COUNT(DISTINCT payment_id) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_paying_customers,
    ROUND(SUM(amount), 2) AS total_processed_amount,
    ROUND(AVG(amount), 2) AS avg_transaction_amount
FROM 
    {{ source('source_silver', 'payments') }}
GROUP BY 
    1, 2, 3
ORDER BY 
    transaction_date DESC, total_processed_amount DESC
