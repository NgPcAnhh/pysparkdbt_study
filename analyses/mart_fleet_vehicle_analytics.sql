-- Mart 5: Phân tích Đội xe & Loại phương tiện (Fleet & Vehicle Analytics Mart)

-- Query 5.1: Doanh thu, số chuyến đi và hiệu suất theo Loại xe & Hãng xe
SELECT 
    v.vehicle_type,
    v.make AS vehicle_make,
    v.model AS vehicle_model,
    COUNT(DISTINCT v.vehicle_id) AS total_vehicles,
    COUNT(DISTINCT f.trip_id) AS total_trips,
    ROUND(SUM(f.fare_amount), 2) AS total_revenue,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare_per_trip,
    ROUND(SUM(f.distance_km), 2) AS total_distance_driven_km
FROM 
    {{ ref('facttrip') }} f
JOIN 
    {{ ref('DimVehicles') }} v ON f.vehicle_id = v.vehicle_id AND v.dbt_valid_to IS NULL
WHERE 
    f.trip_status = 'COMPLETED'
GROUP BY 
    v.vehicle_type, v.make, v.model
ORDER BY 
    total_revenue DESC;
