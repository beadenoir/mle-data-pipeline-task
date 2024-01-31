{{ config(materialized='table', schema='finance') }}

SELECT date(pickup_datetime) AS day, 
SUM(fare_amount
    + extra
    + mta_tax
    + tolls_amount
    + improvement_surcharge 
    + congestion_surcharge) AS total_revenue
FROM {{ ref('stg_gtaxi') }}
GROUP BY day
ORDER BY day ASC