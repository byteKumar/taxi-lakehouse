{% set parts = partitions_to_process(this.name) %}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    pre_hook = "{{ overwrite_partitions_pre_hook('pickup_month') }}",
    post_hook = "{{ record_watermark_post_hook(partitions_to_process(this.name)) }}",
) }}
-- Share of trips that start or end at an airport (JFK, LGA, EWR), per month
-- and service. A trip between two airports counts once in airport_trips and
-- once under each airport.

with flagged as (
    select
        f.pickup_month,
        f.service_type,
        f.total_amount,
        pz.airport_code as pu_airport,
        dz.airport_code as do_airport
    from {{ ref('fct_trips') }} f
    join {{ ref('dim_zone') }} pz on pz.zone_id = f.pu_location_id
    join {{ ref('dim_zone') }} dz on dz.zone_id = f.do_location_id
    where {{ partition_predicate(parts, 'target', 'pickup_month') }}
)

select
    pickup_month,
    service_type,
    count(*)                                                              as trips,
    count(*) filter (pu_airport is not null or do_airport is not null)    as airport_trips,
    round(airport_trips / count(*), 4)                                     as airport_trip_share,
    count(*) filter ('JFK' in (pu_airport, do_airport))                   as jfk_trips,
    count(*) filter ('LGA' in (pu_airport, do_airport))                   as lga_trips,
    count(*) filter ('EWR' in (pu_airport, do_airport))                   as ewr_trips,
    round(sum(total_amount) filter (pu_airport is not null or do_airport is not null), 2)
                                                                          as airport_revenue
from flagged
group by all
