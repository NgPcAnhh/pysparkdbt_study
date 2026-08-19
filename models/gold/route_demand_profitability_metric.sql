{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    f.start_location AS origin_location,
    f.end_location AS destination_location,
    COUNT(DISTINCT f.trip_id) AS total_route_trips,
    COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.trip_id END) AS completed_route_trips,
    COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'CANCELLED' THEN f.trip_id END) AS cancelled_route_trips,
    ROUND(
        COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.trip_id END) * 100.0 / NULLIF(COUNT(DISTINCT f.trip_id), 0), 2
    ) AS route_completion_rate_pct,
    COUNT(DISTINCT f.customer_id) AS unique_customers_on_route,
    COUNT(DISTINCT f.driver_id) AS unique_drivers_on_route,
    ROUND(AVG(f.distance_km), 2) AS avg_route_distance_km,
    ROUND(SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.fare_amount ELSE 0 END), 2) AS total_route_revenue,
    ROUND(AVG(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.fare_amount ELSE NULL END), 2) AS avg_route_fare
FROM 
    {{ ref('facttrip') }} f
GROUP BY 
    1, 2
HAVING 
    COUNT(DISTINCT f.trip_id) >= 1
ORDER BY 
    total_route_revenue DESC
