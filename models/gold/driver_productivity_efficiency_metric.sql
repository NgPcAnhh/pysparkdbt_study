{{
    config(
        materialized = 'table'
    )
}}

WITH driver_metrics AS (
    SELECT 
        d.driver_id,
        d.full_name AS driver_name,
        d.city,
        d.driver_rating,
        COUNT(DISTINCT f.trip_id) AS total_trips_assigned,
        COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.trip_id END) AS completed_trips,
        COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'CANCELLED' THEN f.trip_id END) AS cancelled_trips,
        ROUND(
            COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.trip_id END) * 100.0 / NULLIF(COUNT(DISTINCT f.trip_id), 0), 2
        ) AS completion_rate_pct,
        ROUND(SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.distance_km ELSE 0 END), 2) AS total_driven_km,
        ROUND(SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.fare_amount ELSE 0 END), 2) AS total_revenue,
        ROUND(
            SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.fare_amount ELSE 0 END) / NULLIF(SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.distance_km ELSE 0 END), 0), 2
        ) AS earnings_per_km
    FROM 
        {{ source('source_silver', 'drivers') }} d
    LEFT JOIN 
        {{ ref('facttrip') }} f ON d.driver_id = f.driver_id
    GROUP BY 
        1, 2, 3, 4
)

SELECT 
    driver_id,
    driver_name,
    city,
    driver_rating,
    total_trips_assigned,
    completed_trips,
    cancelled_trips,
    completion_rate_pct,
    total_driven_km,
    total_revenue,
    earnings_per_km,
    ntile(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile,
    CASE 
        WHEN total_revenue >= 5000000 AND driver_rating >= 4.8 AND completion_rate_pct >= 90 THEN 'PLATINUM DRIVER'
        WHEN total_revenue >= 2000000 AND driver_rating >= 4.5 AND completion_rate_pct >= 80 THEN 'GOLD DRIVER'
        WHEN total_revenue >= 500000 THEN 'SILVER DRIVER'
        ELSE 'STANDARD / NEW DRIVER'
    END AS driver_tier
FROM 
    driver_metrics
ORDER BY 
    total_revenue DESC
