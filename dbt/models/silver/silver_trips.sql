{{ config(materialized='view') }}
-- Clean trips joined to the zone lookup. A view, not a table: the only large
-- materialized copy of the trips is gold.fct_trips (disk budget), and
-- partition filters on src_year/src_month push through to the Parquet scan.

select
    c.trip_id,
    c.service_type,
    c.source_month,
    c.src_year,
    c.src_month,
    c.src_row_number,                                           -- row position in the raw file
    c.vendor_id,
    c.pickup_at,
    c.dropoff_at,
    cast(c.pickup_at as date)                                   as pickup_date,
    date_diff('second', c.pickup_at, c.dropoff_at) / 60.0       as duration_min,
    c.passenger_count,
    c.trip_distance_mi,
    c.ratecode_id,
    c.store_and_fwd_flag,
    c.pu_location_id,
    pz.Borough                                                  as pickup_borough,
    pz.Zone                                                     as pickup_zone,
    c.do_location_id,
    dz.Borough                                                  as dropoff_borough,
    dz.Zone                                                     as dropoff_zone,
    (pz.service_zone in ('Airports', 'EWR') or dz.service_zone in ('Airports', 'EWR'))
                                                                as is_airport_trip,
    c.payment_type,
    c.trip_type,
    c.request_source,
    c.fare_amount,
    coalesce(c.extra, 0)                                        as extra,
    coalesce(c.mta_tax, 0)                                      as mta_tax,
    coalesce(c.tip_amount, 0)                                   as tip_amount,
    coalesce(c.tolls_amount, 0)                                 as tolls_amount,
    coalesce(c.improvement_surcharge, 0)                        as improvement_surcharge,
    -- Fees that did not exist for a service or period are 0, not unknown.
    coalesce(c.congestion_surcharge, 0)                         as congestion_surcharge,
    coalesce(c.airport_fee, 0)                                  as airport_fee,
    coalesce(c.cbd_congestion_fee, 0)                           as cbd_congestion_fee,
    coalesce(c.ehail_fee, 0)                                    as ehail_fee,
    c.total_amount
from {{ ref('int_trips_checked') }} c
join {{ ref('taxi_zone_lookup') }} pz on pz.LocationID = c.pu_location_id
join {{ ref('taxi_zone_lookup') }} dz on dz.LocationID = c.do_location_id
where c.reject_reason is null
