-- Marts add up to the fact table per partition: same trip counts, and
-- revenue within half a cent per mart row (marts round each row to cents).

with fct as (
    select service_type, pickup_month, count(*) as trips, sum(total_amount) as revenue
    from {{ ref('fct_trips') }} group by all
),

rev as (
    select service_type, pickup_month, sum(trips) as trips, sum(total_revenue) as revenue, count(*) as mart_rows
    from {{ ref('mart_daily_revenue_by_borough') }} group by all
),

air as (
    select service_type, pickup_month, sum(trips) as trips
    from {{ ref('mart_airport_trip_share') }} group by all
)

select f.service_type, f.pickup_month, f.trips, rev.trips as rev_trips, air.trips as air_trips,
       f.revenue, rev.revenue as rev_revenue
from fct f
full join rev using (service_type, pickup_month)
full join air using (service_type, pickup_month)
where f.trips is distinct from rev.trips
   or f.trips is distinct from air.trips
   or abs(f.revenue - rev.revenue) > 0.005 * rev.mart_rows + 1e-6
