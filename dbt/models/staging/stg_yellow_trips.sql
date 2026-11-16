-- depends_on: {{ source('bronze', 'load_log') }}
-- Yellow taxi trips, renamed and typed to the common silver schema.
-- One row per raw row; no filtering here.
select
    'yellow'                              as service_type,
    year                                  as src_year,
    month                                 as src_month,
    file_row_number                       as src_row_number,
    cast(VendorID as integer)             as vendor_id,
    tpep_pickup_datetime                  as pickup_at,
    tpep_dropoff_datetime                 as dropoff_at,
    cast(passenger_count as integer)      as passenger_count,
    trip_distance                         as trip_distance_mi,
    cast(RatecodeID as integer)           as ratecode_id,
    store_and_fwd_flag,
    cast(PULocationID as integer)         as pu_location_id,
    cast(DOLocationID as integer)         as do_location_id,
    cast(payment_type as integer)         as payment_type,
    cast(null as integer)                 as trip_type,          -- green only
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    Airport_fee                           as airport_fee,        -- capital A in the raw files
    cbd_congestion_fee,                                          -- exists from 2025-01
    cast(null as double)                  as ehail_fee,          -- green only
    cast(request_source as varchar)       as request_source,     -- exists from 2026-06
    total_amount
from {{ bronze('yellow') }}
