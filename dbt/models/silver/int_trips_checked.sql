{{ config(materialized='view') }}
-- Both services in one schema, with a surrogate key and a reject reason.
-- Every bronze row appears here exactly once, so
--   bronze rows = valid rows (reject_reason is null) + rejected rows.
-- Rules are checked in order and the first match wins, so each rejected row
-- has exactly one reason.

with unioned as (
    select * from {{ ref('stg_yellow_trips') }}
    union all
    select * from {{ ref('stg_green_trips') }}
),

zones as (
    select LocationID as location_id from {{ ref('taxi_zone_lookup') }}
)

select
    -- Deterministic key: service, source month and row position in the file.
    -- Stable across rebuilds as long as the file is unchanged.
    -- Cast first: src_year/src_month are INTEGER and 202412 * 10^8 overflows int32.
    cast(case u.service_type when 'yellow' then 1 else 2 end as bigint) * 1000000000000000
      + cast(u.src_year * 100 + u.src_month as bigint) * 100000000
      + u.src_row_number                                         as trip_id,
    make_date(u.src_year, u.src_month, 1)                        as source_month,
    u.*,
    case
        when u.pickup_at is null or u.dropoff_at is null           then 'MISSING_TIMESTAMP'
        when u.pickup_at <  make_date(u.src_year, u.src_month, 1)
          or u.pickup_at >= make_date(u.src_year, u.src_month, 1) + interval 1 month
                                                                     then 'PICKUP_OUTSIDE_FILE_MONTH'
        when u.dropoff_at < u.pickup_at                             then 'DROPOFF_BEFORE_PICKUP'
        when u.fare_amount < 0 or u.total_amount < 0                then 'NEGATIVE_FARE'
        when u.trip_distance_mi is null or u.trip_distance_mi <= 0  then 'ZERO_DISTANCE'
        when u.trip_distance_mi > {{ var('max_trip_miles', 100) }}  then 'ABSURD_DISTANCE'
        when pz.location_id is null or dz.location_id is null       then 'UNKNOWN_ZONE'
    end                                                          as reject_reason
from unioned u
left join zones pz on pz.location_id = u.pu_location_id
left join zones dz on dz.location_id = u.do_location_id
