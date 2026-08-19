-- Mart 1: Phân tích Doanh thu & Hiệu suất Vận hành (Revenue & Operational Analytics Mart)

-- Query 1.1: Doanh thu, Cước trên mỗi km (Revenue/KM), Tỷ lệ hoàn thành chuyến đi theo Tháng
SELECT 
    date_format(trip_start_time, 'yyyy-MM') AS trip_month,
    payment_method,
    COUNT(DISTINCT trip_id) AS total_trips,
    COUNT(DISTINCT CASE WHEN trip_status = 'COMPLETED' THEN trip_id END) AS completed_trips,
    COUNT(DISTINCT CASE WHEN trip_status = 'CANCELLED' THEN trip_id END) AS cancelled_trips,
    ROUND(
        COUNT(DISTINCT CASE WHEN trip_status = 'COMPLETED' THEN trip_id END) * 100.0 / NULLIF(COUNT(DISTINCT trip_id), 0), 2
    ) AS completion_rate_percentage,
    SUM(fare_amount) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip,
    ROUND(SUM(fare_amount) / NULLIF(SUM(distance_km), 0), 2) AS revenue_per_km
FROM 
    {{ ref('facttrip') }}
GROUP BY 
    1, 2
ORDER BY 
    trip_month DESC, total_revenue DESC;
