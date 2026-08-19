{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(f.trip_start_time) AS trip_date,
    f.driver_id,
    d.full_name AS driver_name,
    d.city,
    d.driver_rating,
    COUNT(DISTINCT f.trip_id) AS daily_total_trips,
    COUNT(DISTINCT CASE WHEN f.trip_status = 'COMPLETED' THEN f.trip_id END) AS daily_completed_trips,
    COUNT(DISTINCT CASE WHEN f.trip_status = 'CANCELLED' THEN f.trip_id END) AS daily_cancelled_trips,
    COUNT(DISTINCT f.customer_id) AS daily_unique_customers_served,
    ROUND(SUM(f.distance_km), 2) AS daily_driven_km,
    ROUND(SUM(f.fare_amount), 2) AS daily_total_earnings,
    ROUND(AVG(f.fare_amount), 2) AS daily_avg_fare_per_trip,
    ROUND(SUM(f.fare_amount) / NULLIF(SUM(f.distance_km), 0), 2) AS daily_earnings_per_km
FROM 
    {{ ref('facttrip') }} f
LEFT JOIN 
    {{ source('source_silver', 'drivers') }} d ON f.driver_id = d.driver_id
GROUP BY 
    1, 2, 3, 4, 5
ORDER BY 
    trip_date DESC, daily_total_earnings DESC
