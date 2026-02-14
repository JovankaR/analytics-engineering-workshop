
SELECT 
--identifiers
cast(vendorid as INT64) as vendor_id,
cast(ratecodeid as INT64) as rate_code_id,
cast(pulocationid as INT64) as pickup_location_id,
cast(dolocationid as INT64) as dropoff_location_id,

--timestamps
cast(lpep_pickup_datetime as TIMESTAMP) as pickup_datetime,
cast(lpep_dropoff_datetime as TIMESTAMP) as dropoff_datetime,

--trip info
store_and_fwd_flag,
cast(passenger_count as INT64) as passenger_count,
cast(trip_distance as FLOAT64) as trip_distance,
cast(trip_type as INT64) as trip_type,

--financials
cast(fare_amount as FLOAT64) as fare_amount,
cast(extra as FLOAT64) as extra,
cast(mta_tax as FLOAT64) as mta_tax,
cast(tip_amount as FLOAT64) as tip_amount,
cast(tolls_amount as FLOAT64) as tolls_amount,
cast(improvement_surcharge as FLOAT64) as improvement_surcharge,
cast(total_amount as FLOAT64) as total_amount,
cast(payment_type as INT64) as payment_type

FROM {{ source('raw_data', 'green_tripdata') }}
WHERE vendorid IS NOT NULL

