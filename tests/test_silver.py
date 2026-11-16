"""Silver cleaning rules, run through real dbt on hand-built rows."""

from __future__ import annotations

from tests.conftest import trip, write_trips

YELLOW_2025_03 = [
    trip("2025-03-05 08:00", "2025-03-05 08:20"),                           # valid
    trip("2025-03-31 23:50", "2025-04-01 00:10"),                           # valid: dropoff may cross the month
    trip("2025-02-28 23:00", "2025-02-28 23:30"),                           # PICKUP_OUTSIDE_FILE_MONTH
    trip("2025-03-06 09:00", "2025-03-06 08:00"),                           # DROPOFF_BEFORE_PICKUP
    trip("2025-03-07 10:00", "2025-03-07 10:15", fare=-12.0, total=-15.0),  # NEGATIVE_FARE
    trip("2025-03-08 11:00", "2025-03-08 11:05", distance=0.0),             # ZERO_DISTANCE
    trip("2025-03-09 12:00", "2025-03-09 12:30", distance=150.0),           # ABSURD_DISTANCE
    trip("2025-03-10 13:00", "2025-03-10 13:30", pu=999),                   # UNKNOWN_ZONE
    trip("2025-03-11 14:00", "2025-03-11 13:00", fare=-1.0),                # two problems -> first rule wins
    trip("2025-03-12 15:00", "2025-03-12 15:40", pu=132, do=161, airport_fee=1.75),  # valid JFK pickup
]
GREEN_2024_12 = [  # pre-2025 schema: no cbd_congestion_fee / request_source columns
    trip("2024-12-01 10:00", "2024-12-01 10:10"),
    trip("2024-12-02 10:00", "2024-12-02 10:10", distance=-3.0),            # ZERO_DISTANCE (negative)
]


def test_reject_reasons_and_reconciliation(lakehouse):
    write_trips(lakehouse.remote, "yellow", "2025-03", YELLOW_2025_03)
    write_trips(lakehouse.remote, "green", "2024-12", GREEN_2024_12, new_columns=False)
    lakehouse.ingest(["2025-03"], ["yellow"])
    lakehouse.ingest(["2024-12"], ["green"])

    lakehouse.dbt("build")

    rejected = lakehouse.query(
        "select service_type, src_row_number, reject_reason from silver.rejected_trips order by 1, 2")
    assert rejected == [
        ("green", 1, "ZERO_DISTANCE"),
        ("yellow", 2, "PICKUP_OUTSIDE_FILE_MONTH"),
        ("yellow", 3, "DROPOFF_BEFORE_PICKUP"),
        ("yellow", 4, "NEGATIVE_FARE"),
        ("yellow", 5, "ZERO_DISTANCE"),
        ("yellow", 6, "ABSURD_DISTANCE"),
        ("yellow", 7, "UNKNOWN_ZONE"),
        ("yellow", 8, "DROPOFF_BEFORE_PICKUP"),
    ]
    valid = lakehouse.query("select count(*) from silver.silver_trips")[0][0]
    assert valid + len(rejected) == len(YELLOW_2025_03) + len(GREEN_2024_12)


def test_silver_schema_is_standardized_across_services_and_years(lakehouse):
    write_trips(lakehouse.remote, "yellow", "2025-03", YELLOW_2025_03)
    write_trips(lakehouse.remote, "green", "2024-12", GREEN_2024_12, new_columns=False)
    lakehouse.ingest(["2025-03"], ["yellow"])
    lakehouse.ingest(["2024-12"], ["green"])
    lakehouse.dbt("build")

    rows = dict(lakehouse.query("""
        select service_type || '|' || src_row_number,
               struct_pack(pickup_borough, is_airport_trip, cbd_congestion_fee, airport_fee, trip_type)
        from silver.silver_trips"""))
    jfk = rows["yellow|9"]
    assert jfk == {"pickup_borough": "Queens", "is_airport_trip": True, "cbd_congestion_fee": 0.75,
                   "airport_fee": 1.75, "trip_type": None}
    # Fee columns that did not exist in 2024 green files come through as 0.
    assert rows["green|0"]["cbd_congestion_fee"] == 0.0 and rows["green|0"]["airport_fee"] == 0.0
    assert rows["green|0"]["trip_type"] == 1


def test_incremental_run_processes_only_new_and_changed_months(lakehouse):
    # Both services must have at least one file: the staging views glob each service's folder.
    write_trips(lakehouse.remote, "yellow", "2025-03", YELLOW_2025_03)
    write_trips(lakehouse.remote, "green", "2025-03", [trip("2025-03-01 10:00", "2025-03-01 10:10")])
    lakehouse.ingest(["2025-03"])
    lakehouse.dbt("build")
    assert lakehouse.query("select count(*) from silver.rejected_trips")[0][0] == 7

    # Nothing new: incremental run leaves the table alone.
    lakehouse.dbt("run", "-s", "rejected_trips")
    marks = lakehouse.query("select service, year_month from meta.partition_watermark order by all")
    assert marks == [("green", "2025-03"), ("yellow", "2025-03")]

    # New month arrives: only that partition is processed.
    write_trips(lakehouse.remote, "yellow", "2025-04", [trip("2025-04-02 09:00", "2025-04-02 09:30", distance=0)])
    lakehouse.ingest(["2025-04"], ["yellow"])
    lakehouse.dbt("run", "-s", "rejected_trips")
    by_month = lakehouse.query(
        "select strftime(source_month, '%Y-%m'), count(*) from silver.rejected_trips group by 1 order by 1")
    assert by_month == [("2025-03", 7), ("2025-04", 1)]
    marks = lakehouse.query("select year_month, count(*) from meta.partition_watermark group by 1 order by 1")
    assert marks == [("2025-03", 2), ("2025-04", 1)]

    # TLC republishes March with only one bad row: March is replaced, not appended.
    import time; time.sleep(1.1)  # Last-Modified has 1 s resolution
    write_trips(lakehouse.remote, "yellow", "2025-03",
                [trip("2025-03-05 08:00", "2025-03-05 08:20", fare=-1.0, total=-1.0)])
    assert [r.status for r in lakehouse.ingest(["2025-03"], ["yellow"])] == ["replaced"]
    lakehouse.dbt("run", "-s", "rejected_trips")
    by_month = lakehouse.query(
        "select strftime(source_month, '%Y-%m'), count(*) from silver.rejected_trips group by 1 order by 1")
    assert by_month == [("2025-03", 1), ("2025-04", 1)]
