-- Row-count reconciliation, per (service, month) partition:
--   bronze rows (load_log of the current file) = fct_trips rows + rejected_trips rows
-- Any returned row is a partition where rows were lost or duplicated, or one
-- that was loaded but not yet processed. Either way the run must fail.

with bronze as (
    select service, year_month, row_count
    from (
        select *, row_number() over (partition by service, year_month order by load_id desc) as rn
        from {{ source('bronze', 'load_log') }}
        where status in ('loaded', 'replaced')
    )
    where rn = 1
),

valid as (
    select service_type as service, strftime(pickup_month, '%Y-%m') as year_month, count(*) as n
    from {{ ref('fct_trips') }} group by all
),

rejected as (
    select service_type as service, strftime(source_month, '%Y-%m') as year_month, count(*) as n
    from {{ ref('rejected_trips') }} group by all
)

select
    coalesce(b.service, v.service, r.service)          as service,
    coalesce(b.year_month, v.year_month, r.year_month) as year_month,
    b.row_count                                        as bronze_rows,
    coalesce(v.n, 0)                                   as valid_rows,
    coalesce(r.n, 0)                                   as rejected_rows
from bronze b
full join valid v using (service, year_month)
full join rejected r using (service, year_month)
where b.row_count is distinct from coalesce(v.n, 0) + coalesce(r.n, 0)
