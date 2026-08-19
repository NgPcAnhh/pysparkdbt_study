{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(f.trip_start_time) AS trip_date,
    f.customer_id,
    c.full_name AS customer_name,
    c.city,
    c.email,
    COUNT(DISTINCT f.trip_id) AS daily_trips_count,
    COUNT(DISTINCT f.driver_id) AS daily_unique_drivers_used,
    ROUND(SUM(f.distance_km), 2) AS daily_total_distance_km,
    ROUND(SUM(f.fare_amount), 2) AS daily_total_spent,
    ROUND(AVG(f.fare_amount), 2) AS daily_avg_trip_fare,
    COUNT(CASE WHEN upper(f.payment_method) = 'CARD' THEN 1 END) AS pay_card,
    COUNT(CASE WHEN upper(f.payment_method) = 'CASH' THEN 1 END) AS pay_cash,
    COUNT(CASE WHEN upper(f.payment_method) = 'WALLET' THEN 1 END) AS pay_wallet
FROM 
    {{ ref('facttrip') }} f
LEFT JOIN 
    {{ source('source_silver', 'customers') }} c ON f.customer_id = c.customer_id
WHERE 
    upper(f.trip_status) = 'COMPLETED'
GROUP BY 
    1, 2, 3, 4, 5
ORDER BY 
    trip_date DESC, daily_total_spent DESC
