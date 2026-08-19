{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    explode(sequence(to_date('2020-01-01'), to_date('2030-01-01'), interval 1 day)) AS date_day
