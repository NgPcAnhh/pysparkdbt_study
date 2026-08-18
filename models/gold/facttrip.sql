{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    trip_id,
    vehicle_id,
    driver_id,
    customer_id,
    trip_start_time,
    trip_end_time,
    start_location,
    end_location,
    distance_km,
    fare_amount,
    payment_method,
    trip_status,
    last_updated_timestamp
FROM 
    {{ ref('trips') }}
