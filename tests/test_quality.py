"""Data-quality gates: a failing dbt test must fail the run (non-zero exit)
and stop downstream models from building."""

from __future__ import annotations

import duckdb

from tests.conftest import trip, write_trips

MARCH = [
    trip("2025-03-01 08:00", "2025-03-01 08:20"),
    trip("2025-03-01 09:00", "2025-03-01 09:40", pu=132, do=161),
    trip("2025-03-02 12:00", "2025-03-02 12:10", distance=0),  # rejected
]


def build_march(lakehouse):
    write_trips(lakehouse.remote, "yellow", "2025-03", MARCH)
    write_trips(lakehouse.remote, "green", "2025-03", MARCH[:1])
    lakehouse.ingest(["2025-03"])
    lakehouse.dbt("build")


def tamper(lakehouse, sql: str) -> None:
    con = duckdb.connect(str(lakehouse.warehouse))
    con.execute(sql)
    con.close()


def test_clean_build_passes_every_test(lakehouse):
    build_march(lakehouse)
    proc = lakehouse.dbt("test")
    assert proc.returncode == 0
    assert "ERROR=0" in proc.stdout and "FAIL" not in proc.stdout.replace("FAIL=0", "")


def test_lost_row_fails_reconciliation(lakehouse):
    build_march(lakehouse)
    tamper(lakehouse, "delete from silver.rejected_trips")

    proc = lakehouse.dbt("test", check=False)

    assert proc.returncode != 0
    assert "FAIL 1 assert_bronze_reconciles" in proc.stdout


def test_duplicate_trip_fails_unique_test(lakehouse):
    build_march(lakehouse)
    tamper(lakehouse, "insert into gold.fct_trips select * from gold.fct_trips limit 1")

    proc = lakehouse.dbt("test", check=False)

    assert proc.returncode != 0
    assert "FAIL 1 unique_fct_trips_trip_id" in proc.stdout
    assert "FAIL 1 assert_bronze_reconciles" in proc.stdout  # 1 extra row breaks the count too


def test_future_trip_fails_build_and_blocks_marts(lakehouse):
    build_march(lakehouse)
    # A file for a month that has not happened yet, with in-month (so not
    # rejected) but future timestamps.
    future = [trip("2099-01-05 08:00", "2099-01-05 08:20")]
    write_trips(lakehouse.remote, "yellow", "2099-01", future)
    write_trips(lakehouse.remote, "green", "2099-01", future)
    lakehouse.ingest(["2099-01"])

    proc = lakehouse.dbt("build", check=False)

    assert proc.returncode != 0
    assert "FAIL 2 assert_no_future_dates" in proc.stdout
    # fct_trips was written, but its children were skipped: the marts never saw 2099.
    assert lakehouse.query("select count(*) from gold.fct_trips where pickup_month = date '2099-01-01'")[0][0] == 2
    assert lakehouse.query(
        "select count(*) from gold.mart_airport_trip_share where pickup_month = date '2099-01-01'")[0][0] == 0
    assert "SKIP" in proc.stdout
