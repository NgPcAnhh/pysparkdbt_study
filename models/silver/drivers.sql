{{
    config(
        materialized = 'incremental',
        unique_key = 'driver_id'
    )
}}

SELECT 
    driver_id,
    phone_number,
    vehicle_id,
    driver_rating,
    city,
    concat(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) AS full_name,
    last_updated_timestamp
FROM 
    {{ source('source_bronze', 'drivers') }}

{% if is_incremental() %}
WHERE
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp), TIMESTAMP '1900-01-01 00:00:00') FROM {{ this }})
{% endif %}
