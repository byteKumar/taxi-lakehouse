-- depends_on: {{ source('bronze', 'load_log') }}
-- Green taxi trips, renamed and typed to the common silver schema.
select
    'green'                               as service_type,
    year                                  as src_year,
    month                                 as src_month,
    file_row_number                       as src_row_number,
    cast(VendorID as integer)             as vendor_id,
    lpep_pickup_datetime                  as pickup_at,
    lpep_dropoff_datetime                 as dropoff_at,
    cast(passenger_count as integer)      as passenger_count,
    trip_distance                         as trip_distance_mi,
    cast(RatecodeID as integer)           as ratecode_id,
    store_and_fwd_flag,
    cast(PULocationID as integer)         as pu_location_id,
    cast(DOLocationID as integer)         as do_location_id,
    cast(payment_type as integer)         as payment_type,
    cast(trip_type as integer)            as trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    cast(null as double)                  as airport_fee,        -- yellow only
    cbd_congestion_fee,
    ehail_fee,
    cast(request_source as varchar)       as request_source,
    total_amount
from {{ bronze('green') }}
