{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    to_date(trip_start_time) AS trip_date,
    hour(trip_start_time) AS hour_of_day,
    date_format(trip_start_time, 'EEEE') AS day_of_week,
    CASE 
        WHEN date_format(trip_start_time, 'EEEE') IN ('Saturday', 'Sunday') THEN true 
        ELSE false 
    END AS is_weekend,
    CASE 
        WHEN hour(trip_start_time) BETWEEN 7 AND 9 THEN 'MORNING_RUSH'
        WHEN hour(trip_start_time) BETWEEN 17 AND 19 THEN 'EVENING_RUSH'
        WHEN hour(trip_start_time) BETWEEN 22 AND 23 OR hour(trip_start_time) BETWEEN 0 AND 4 THEN 'LATE_NIGHT'
        ELSE 'NORMAL_HOURS'
    END AS peak_demand_period,
    COUNT(DISTINCT trip_id) AS total_trips_demand,
    COUNT(DISTINCT CASE WHEN upper(trip_status) = 'COMPLETED' THEN trip_id END) AS completed_trips,
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(DISTINCT driver_id) AS active_drivers,
    ROUND(SUM(CASE WHEN upper(trip_status) = 'COMPLETED' THEN fare_amount ELSE 0 END), 2) AS hourly_revenue,
    ROUND(AVG(CASE WHEN upper(trip_status) = 'COMPLETED' THEN fare_amount ELSE NULL END), 2) AS avg_fare_per_trip
FROM 
    {{ ref('facttrip') }}
GROUP BY 
    1, 2, 3, 4, 5
ORDER BY 
    trip_date DESC, hour_of_day ASC
