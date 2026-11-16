-- No valid trip may start or end in the future (TLC timestamps are local NYC
-- time; the machine's local clock is the reference).
select trip_id, pickup_at, dropoff_at
from {{ ref('fct_trips') }}
where pickup_at > now()::timestamp
   or dropoff_at > now()::timestamp
   or pickup_date > current_date
