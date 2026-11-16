{{ config(materialized='table') }}
-- Every calendar day in the months that have been loaded. Rebuilt in full on
-- every run (a few hundred rows), so a new month extends it automatically.

with bounds as (
    select
        strptime(min(year_month) || '-01', '%Y-%m-%d')::date                         as first_day,
        (strptime(max(year_month) || '-01', '%Y-%m-%d') + interval 1 month)::date - 1 as last_day
    from {{ source('bronze', 'load_log') }}
    where status in ('loaded', 'replaced')
),

days as (
    select unnest(generate_series(first_day, last_day, interval 1 day))::date as date_day
    from bounds
)

select
    date_day,
    date_trunc('month', date_day)::date        as month_start,
    year(date_day)                             as year,
    quarter(date_day)                          as quarter,
    month(date_day)                            as month,
    day(date_day)                              as day_of_month,
    isodow(date_day)                           as iso_day_of_week,   -- 1 = Monday
    dayname(date_day)                          as day_name,
    isodow(date_day) in (6, 7)                 as is_weekend
from days
