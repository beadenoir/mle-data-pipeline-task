{{ config(materialized='table', schema='stg') }}

SELECT 
    "VendorID" AS vendor_id,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    store_and_fwd_flag,
    "RatecodeID" AS rate_code_id,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge

FROM {{ source('src_gtaxi','gtaxi_2021_qt1') }}