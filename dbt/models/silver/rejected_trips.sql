{% set parts = partitions_to_process(this.name) %}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    pre_hook = "{{ overwrite_partitions_pre_hook() }}",
    post_hook = "{{ record_watermark_post_hook(partitions_to_process(this.name)) }}",
) }}
-- Quarantined rows: every standardized column plus the reason code, so a rule
-- change can be re-checked without going back to the raw files.

select *, now() as rejected_at
from {{ ref('int_trips_checked') }}
where reject_reason is not null
  and {{ partition_predicate(parts, 'source') }}
