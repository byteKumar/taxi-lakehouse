-- Every trip sits in the partition of its own pickup month. Incremental
-- delete+insert by partition relies on this.
select trip_id, pickup_at, pickup_month
from {{ ref('fct_trips') }}
where date_trunc('month', pickup_at)::date <> pickup_month
