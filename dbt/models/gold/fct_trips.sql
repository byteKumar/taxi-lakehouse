{% set parts = partitions_to_process(this.name) %}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    pre_hook = "{{ overwrite_partitions_pre_hook('pickup_month') }}",
    post_hook = "{{ record_watermark_post_hook(partitions_to_process(this.name)) }}",
) }}
-- One row per valid trip, partitioned by (service_type, pickup_month).
-- Silver rejects pickups outside the file's month, so pickup_month always
-- equals the source file's month and one raw file maps to one partition.
-- No global ORDER BY: sorting 88M wide rows ran out of memory at a 6 GB
-- limit. Incremental runs insert one month at a time, so those row groups are
-- naturally clustered by month; a full rebuild interleaves months.

select
    trip_id,
    service_type,
    source_month                as pickup_month,
    pickup_date,
    pickup_at,
    dropoff_at,
    duration_min,
    pu_location_id,
    do_location_id,
    is_airport_trip,
    vendor_id,
    passenger_count,
    trip_distance_mi,
    ratecode_id,
    payment_type,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    ehail_fee,
    total_amount
from {{ ref('silver_trips') }}
where {{ partition_predicate(parts, 'source') }}
