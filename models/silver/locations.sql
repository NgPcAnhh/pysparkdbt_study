{{
    config(
        materialized = 'incremental',
        unique_key = 'location_id'
    )
}}

SELECT 
    location_id,
    city,
    state,
    country,
    latitude,
    longitude,
    last_updated_timestamp
FROM 
    {{ source('source_bronze', 'locations') }}

{% if is_incremental() %}
WHERE
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp), TIMESTAMP '1900-01-01 00:00:00') FROM {{ this }})
{% endif %}
