{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(f.trip_start_time) AS trip_date,
    v.vehicle_type,
    v.make AS vehicle_make,
    v.model AS vehicle_model,
    COUNT(DISTINCT f.vehicle_id) AS daily_active_vehicles,
    COUNT(DISTINCT f.trip_id) AS daily_trips_count,
    ROUND(SUM(f.distance_km), 2) AS daily_fleet_distance_km,
    ROUND(SUM(f.fare_amount), 2) AS daily_fleet_revenue,
    ROUND(AVG(f.fare_amount), 2) AS daily_avg_fare_per_vehicle
FROM 
    {{ ref('facttrip') }} f
LEFT JOIN 
    {{ source('source_silver', 'vehicles') }} v ON f.vehicle_id = v.vehicle_id
WHERE 
    upper(f.trip_status) = 'COMPLETED'
GROUP BY 
    1, 2, 3, 4
ORDER BY 
    trip_date DESC, daily_fleet_revenue DESC
