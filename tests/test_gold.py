"""Gold models: fct_trips, dims and marts, built by real dbt on hand-built rows."""

from __future__ import annotations

from tests.conftest import trip, write_trips

# 161 Midtown Center (Manhattan), 132 JFK (Queens), 138 LGA (Queens), 1 EWR, 7 Astoria (Queens)
YELLOW_MARCH = [
    trip("2025-03-01 08:00", "2025-03-01 08:20", total=20.0),                  # Manhattan
    trip("2025-03-01 09:00", "2025-03-01 09:40", pu=132, do=161, total=70.0),  # JFK pickup
    trip("2025-03-02 10:00", "2025-03-02 10:30", pu=161, do=138, total=40.0),  # LGA dropoff
    trip("2025-03-02 11:00", "2025-03-02 11:50", pu=132, do=1, total=120.0),   # JFK -> EWR
    trip("2025-03-02 12:00", "2025-03-02 12:10", distance=0, total=10.0),      # rejected
]
GREEN_MARCH = [trip("2025-03-01 07:00", "2025-03-01 07:10", pu=7, do=7, total=12.5)]


def build_march(lakehouse):
    write_trips(lakehouse.remote, "yellow", "2025-03", YELLOW_MARCH)
    write_trips(lakehouse.remote, "green", "2025-03", GREEN_MARCH)
    lakehouse.ingest(["2025-03"])
    lakehouse.dbt("build")


def test_fct_and_marts_match_silver(lakehouse):
    build_march(lakehouse)
    q = lakehouse.query

    assert q("select count(*) from gold.fct_trips")[0][0] == 5
    assert q("select count(*) from gold.fct_trips")[0][0] == q("select count(*) from silver.silver_trips")[0][0]

    revenue = q("""select service_type, pickup_borough, sum(trips), sum(total_revenue)
                   from gold.mart_daily_revenue_by_borough group by all order by all""")
    assert revenue == [("green", "Queens", 5 - 4, 12.5),
                       ("yellow", "Manhattan", 2, 60.0),
                       ("yellow", "Queens", 2, 190.0)]

    share = q("""select service_type, trips, airport_trips, airport_trip_share, jfk_trips, lga_trips, ewr_trips
                 from gold.mart_airport_trip_share order by 1""")
    assert share == [("green", 1, 0, 0.0, 0, 0, 0),
                     ("yellow", 4, 3, 0.75, 2, 1, 1)]


def test_dim_date_covers_loaded_months_and_every_trip_date(lakehouse):
    build_march(lakehouse)
    q = lakehouse.query
    assert q("select min(date_day), max(date_day), count(*) from gold.dim_date")[0][2] == 31
    missing = q("""select count(*) from gold.fct_trips f
                   left join gold.dim_date d on d.date_day = f.pickup_date where d.date_day is null""")
    assert missing[0][0] == 0


def test_new_month_only_touches_its_own_partitions(lakehouse):
    build_march(lakehouse)
    q = lakehouse.query
    march_marks = q("""select model, service, processed_at from meta.partition_watermark
                       where year_month = '2025-03' order by all""")

    write_trips(lakehouse.remote, "yellow", "2025-04", [trip("2025-04-01 08:00", "2025-04-01 08:20", total=30.0)])
    write_trips(lakehouse.remote, "green", "2025-04", [trip("2025-04-01 08:00", "2025-04-01 08:20", total=9.0)])
    lakehouse.ingest(["2025-04"])
    lakehouse.dbt("build")

    # March was not reprocessed by any model: its watermarks are untouched.
    assert q("""select model, service, processed_at from meta.partition_watermark
                where year_month = '2025-03' order by all""") == march_marks
    assert q("""select strftime(pickup_month, '%Y-%m'), count(*) from gold.fct_trips
                group by 1 order by 1""") == [("2025-03", 5), ("2025-04", 2)]
    assert q("""select strftime(pickup_month, '%Y-%m'), sum(trips) from gold.mart_airport_trip_share
                group by 1 order by 1""") == [("2025-03", 5), ("2025-04", 2)]
    assert q("select count(*) from gold.dim_date")[0][0] == 31 + 30
