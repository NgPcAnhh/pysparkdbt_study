{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(f.trip_start_time) AS trip_date,
    f.start_location AS location_name,
    COUNT(DISTINCT f.trip_id) AS daily_pickup_trips,
    COUNT(DISTINCT f.customer_id) AS daily_unique_customers,
    COUNT(DISTINCT f.driver_id) AS daily_active_drivers,
    ROUND(SUM(f.fare_amount), 2) AS daily_location_revenue,
    ROUND(AVG(f.fare_amount), 2) AS daily_avg_fare,
    ROUND(AVG(f.distance_km), 2) AS daily_avg_distance_km
FROM 
    {{ ref('facttrip') }} f
WHERE 
    upper(f.trip_status) = 'COMPLETED'
GROUP BY 
    1, 2
ORDER BY 
    trip_date DESC, daily_location_revenue DESC
