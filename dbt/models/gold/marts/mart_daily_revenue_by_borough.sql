{% set parts = partitions_to_process(this.name) %}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    pre_hook = "{{ overwrite_partitions_pre_hook('pickup_month') }}",
    post_hook = "{{ record_watermark_post_hook(partitions_to_process(this.name)) }}",
) }}
-- Daily revenue per pickup borough and service. Incremental by the same
-- (service, month) partitions as fct_trips, so a new month only aggregates
-- that month's trips.

select
    f.pickup_month,
    f.pickup_date,
    f.service_type,
    z.borough                                  as pickup_borough,
    count(*)                                   as trips,
    round(sum(f.total_amount), 2)              as total_revenue,
    round(sum(f.fare_amount), 2)               as fare_revenue,
    round(sum(f.tip_amount), 2)                as tip_revenue,
    round(avg(f.total_amount), 2)              as avg_total_per_trip,
    round(sum(f.trip_distance_mi), 1)          as total_miles
from {{ ref('fct_trips') }} f
join {{ ref('dim_zone') }} z on z.zone_id = f.pu_location_id
where {{ partition_predicate(parts, 'target', 'pickup_month') }}
group by all
