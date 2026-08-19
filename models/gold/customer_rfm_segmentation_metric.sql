{{
    config(
        materialized = 'table'
    )
}}

WITH customer_summary AS (
    SELECT 
        c.customer_id,
        c.full_name AS customer_name,
        c.city,
        c.email,
        c.signup_date,
        datediff(current_date(), MAX(to_date(f.trip_start_time))) AS recency_days,
        COUNT(DISTINCT CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.trip_id END) AS frequency_trips,
        ROUND(SUM(CASE WHEN upper(f.trip_status) = 'COMPLETED' THEN f.fare_amount ELSE 0 END), 2) AS monetary_spent
    FROM 
        {{ source('source_silver', 'customers') }} c
    LEFT JOIN 
        {{ ref('facttrip') }} f ON c.customer_id = f.customer_id
    GROUP BY 
        1, 2, 3, 4, 5
),

rfm_scores AS (
    SELECT 
        *,
        ntile(5) OVER (ORDER BY recency_days ASC) AS r_score,
        ntile(5) OVER (ORDER BY frequency_trips ASC) AS f_score,
        ntile(5) OVER (ORDER BY monetary_spent ASC) AS m_score
    FROM 
        customer_summary
)

SELECT 
    customer_id,
    customer_name,
    city,
    email,
    signup_date,
    recency_days,
    frequency_trips,
    monetary_spent,
    r_score,
    f_score,
    m_score,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'CHAMPIONS (VIP)'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'LOYAL CUSTOMERS'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'NEW / RECENT CUSTOMERS'
        WHEN r_score <= 2 AND f_score >= 4 THEN 'AT RISK (HIGH SPENDER LEAVING)'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'HIBERNATING / LOST'
        ELSE 'POTENTIAL LOYALISTS'
    END AS rfm_segment
FROM 
    rfm_scores
ORDER BY 
    monetary_spent DESC
