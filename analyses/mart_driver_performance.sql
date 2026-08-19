-- Mart 2: Phân tích Hiệu suất & Thu nhập Tài xế (Driver Performance Mart)

-- Query 2.1: Top 10 tài xế xuất sắc nhất về doanh thu và chỉ số hoạt động
SELECT 
    d.driver_id,
    d.full_name AS driver_name,
    d.city,
    d.driver_rating,
    COUNT(DISTINCT f.trip_id) AS completed_trips,
    ROUND(SUM(f.distance_km), 2) AS total_driven_km,
    ROUND(SUM(f.fare_amount), 2) AS total_earnings,
    ROUND(AVG(f.fare_amount), 2) AS avg_earnings_per_trip
FROM 
    {{ ref('facttrip') }} f
JOIN 
    {{ ref('DimDrivers') }} d ON f.driver_id = d.driver_id AND d.dbt_valid_to IS NULL
WHERE 
    f.trip_status = 'COMPLETED'
GROUP BY 
    d.driver_id, d.full_name, d.city, d.driver_rating
ORDER BY 
    total_earnings DESC
LIMIT 10;
