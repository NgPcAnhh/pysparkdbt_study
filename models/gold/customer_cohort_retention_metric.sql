{{
    config(
        materialized = 'table'
    )
}}

WITH customer_cohort AS (
    SELECT 
        customer_id,
        to_date(date_trunc('month', signup_date)) AS cohort_month
    FROM 
        {{ source('source_silver', 'customers') }}
),

trip_activity AS (
    SELECT 
        f.customer_id,
        to_date(date_trunc('month', f.trip_start_time)) AS activity_month,
        COUNT(DISTINCT f.trip_id) AS trips,
        SUM(f.fare_amount) AS revenue
    FROM 
        {{ ref('facttrip') }} f
    WHERE 
        upper(f.trip_status) = 'COMPLETED'
    GROUP BY 
        1, 2
)

SELECT 
    c.cohort_month,
    a.activity_month,
    timestampdiff(MONTH, c.cohort_month, a.activity_month) AS months_since_signup,
    COUNT(DISTINCT c.customer_id) AS cohort_size,
    COUNT(DISTINCT a.customer_id) AS active_customers,
    ROUND(COUNT(DISTINCT a.customer_id) * 100.0 / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS retention_rate_pct,
    ROUND(SUM(a.revenue), 2) AS total_cohort_revenue,
    ROUND(SUM(a.revenue) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS arpu_per_cohort_user
FROM 
    customer_cohort c
JOIN 
    trip_activity a ON c.customer_id = a.customer_id
GROUP BY 
    1, 2, 3
ORDER BY 
    cohort_month DESC, months_since_signup ASC
