-- Mart 3: Phân tích Vị trí & Khu vực Nhu cầu cao (Location Analytics Mart)

-- Query 3.1: Tổng hợp doanh thu và số chuyến theo khu vực đón khách
SELECT 
    l.city,
    l.state,
    l.country,
    COUNT(DISTINCT f.trip_id) AS total_pickup_trips,
    ROUND(SUM(f.fare_amount), 2) AS total_regional_revenue,
    ROUND(AVG(f.distance_km), 2) AS avg_trip_distance_km
FROM 
    {{ ref('facttrip') }} f
JOIN 
    {{ ref('DimLocations') }} l ON f.start_location = l.location_id AND l.dbt_valid_to IS NULL
GROUP BY 
    l.city, l.state, l.country
ORDER BY 
    total_regional_revenue DESC;
