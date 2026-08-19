{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(trip_start_time) AS trip_date,
    payment_method,
    COUNT(DISTINCT trip_id) AS total_trips,
    COUNT(DISTINCT CASE WHEN trip_status = 'COMPLETED' THEN trip_id END) AS completed_trips,
    COUNT(DISTINCT CASE WHEN trip_status = 'CANCELLED' THEN trip_id END) AS cancelled_trips,
    ROUND(
        COUNT(DISTINCT CASE WHEN trip_status = 'COMPLETED' THEN trip_id END) * 100.0 / NULLIF(COUNT(DISTINCT trip_id), 0), 2
    ) AS completion_rate_pct,
    COUNT(DISTINCT customer_id) AS total_unique_customers,
    COUNT(DISTINCT driver_id) AS total_active_drivers,
    ROUND(SUM(distance_km), 2) AS total_distance_km,
    ROUND(SUM(fare_amount), 2) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip,
    ROUND(SUM(fare_amount) / NULLIF(SUM(distance_km), 0), 2) AS revenue_per_km,
    ROUND(MIN(fare_amount), 2) AS min_fare,
    ROUND(MAX(fare_amount), 2) AS max_fare
FROM 
    {{ ref('facttrip') }}
GROUP BY 
    1, 2
ORDER BY 
    trip_date DESC, total_revenue DESC
