-- Mart 4: Phân tích Tần suất & Giá trị Khách hàng (Customer Retention Mart)

-- Query 4.1: Phân nhóm khách hàng theo tần suất đi lại (RFM - Frequency & Monetary)
WITH customer_stats AS (
    SELECT 
        c.customer_id,
        c.full_name AS customer_name,
        c.city,
        COUNT(DISTINCT f.trip_id) AS total_trips,
        ROUND(SUM(f.fare_amount), 2) AS total_spent,
        MAX(f.trip_start_time) AS last_trip_time
    FROM 
        {{ ref('facttrip') }} f
    JOIN 
        {{ ref('DimCustomers') }} c ON f.customer_id = c.customer_id AND c.dbt_valid_to IS NULL
    GROUP BY 
        c.customer_id, c.full_name, c.city
)

SELECT 
    customer_id,
    customer_name,
    city,
    total_trips,
    total_spent,
    last_trip_time,
    CASE 
        WHEN total_spent >= 1000000 AND total_trips >= 20 THEN 'VIP Customer'
        WHEN total_trips >= 10 THEN 'Loyal Customer'
        WHEN total_trips >= 3 THEN 'Regular Customer'
        ELSE 'New/Occasional Customer'
    END AS customer_segment
FROM 
    customer_stats
ORDER BY 
    total_spent DESC;
