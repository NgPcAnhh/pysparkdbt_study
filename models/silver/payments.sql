{{
    config(
        materialized = 'incremental',
        unique_key = 'payment_id'
    )
}}

SELECT 
    payment_id,
    trip_id,
    customer_id,
    payment_method,
    payment_status,
    amount,
    transaction_time,
    payment_status AS online_payment_status,
    last_updated_timestamp
FROM 
    {{ source('source_bronze', 'payments') }}

{% if is_incremental() %}
WHERE
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp), TIMESTAMP '1900-01-01 00:00:00') FROM {{ this }})
{% endif %}
