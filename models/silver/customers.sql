{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_id'
    )
}}

SELECT 
    customer_id,
    email,
    phone_number,
    city,
    signup_date,
    concat(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) AS full_name,
    split(email, '@')[1] AS domain,
    last_updated_timestamp
FROM 
    {{ source('source_bronze', 'customers') }}

{% if is_incremental() %}
WHERE
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp), TIMESTAMP '1900-01-01 00:00:00') FROM {{ this }})
{% endif %}
