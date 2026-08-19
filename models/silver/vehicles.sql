{{
    config(
        materialized = 'incremental',
        unique_key = 'vehicle_id'
    )
}}

SELECT 
    vehicle_id,
    license_plate,
    model,
    make,
    year,
    vehicle_type,
    last_updated_timestamp
FROM 
    {{ source('source_bronze', 'vehicles') }}

{% if is_incremental() %}
WHERE
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp), TIMESTAMP '1900-01-01 00:00:00') FROM {{ this }})
{% endif %}
