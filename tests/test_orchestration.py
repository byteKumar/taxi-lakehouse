"""Dagster job against a throwaway lakehouse: one partition, a single-run
backfill, and failures that must fail the run."""

from __future__ import annotations

import subprocess
import warnings

import duckdb
import pytest
from dagster import DagsterInstance

from pipeline import config
from tests.conftest import DBT_BIN, DBT_DIR, trip, write_trips

warnings.filterwarnings("ignore", module="dagster")


@pytest.fixture
def dagster_lakehouse(lakehouse, monkeypatch):
    """Point both the Python ingest and the dbt subprocess at the temp lakehouse."""
    monkeypatch.setattr(config, "RAW_DIR", lakehouse.raw)
    monkeypatch.setattr(config, "WAREHOUSE_PATH", lakehouse.warehouse)
    monkeypatch.setenv("TAXI_BASE_URL", lakehouse.base_url)
    monkeypatch.setenv("TAXI_RAW_DIR", str(lakehouse.raw))
    monkeypatch.setenv("TAXI_WAREHOUSE", str(lakehouse.warehouse))
    monkeypatch.setenv("TAXI_DUCKDB_TMP", str(lakehouse.tmp / "duckdb_tmp"))
    monkeypatch.setenv("TAXI_DUCKDB_MEMORY", "1GB")
    if not (DBT_DIR / "target" / "manifest.json").exists():  # e.g. a fresh CI checkout
        subprocess.run([str(DBT_BIN), "parse", "--profiles-dir", "."], cwd=DBT_DIR, check=True)
    for ym, n in [("2025-03", 3), ("2025-04", 2)]:
        rows = [trip(f"{ym}-0{d + 1} 08:00", f"{ym}-0{d + 1} 08:20") for d in range(n)]
        write_trips(lakehouse.remote, "yellow", ym, rows)
        write_trips(lakehouse.remote, "green", ym, rows[:1])
    from orchestration.definitions import defs
    return lakehouse, defs.get_job_def("monthly_pipeline")


def run(job, **kwargs):
    return job.execute_in_process(instance=DagsterInstance.ephemeral(), raise_on_error=False, **kwargs)


def fct_by_month(lakehouse):
    return lakehouse.query("select strftime(pickup_month, '%Y-%m'), count(*) from gold.fct_trips group by 1 order by 1")


def test_one_partition_run(dagster_lakehouse):
    lakehouse, job = dagster_lakehouse

    res = run(job, partition_key="2025-03")

    assert res.success
    assert fct_by_month(lakehouse) == [("2025-03", 4)]
    checks = [e.event_specific_data for e in res.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"]
    assert checks and all(c.passed for c in checks)


def test_backfill_range_is_one_run_and_idempotent(dagster_lakehouse):
    lakehouse, job = dagster_lakehouse
    assert run(job, partition_key="2025-03").success
    march_before = lakehouse.query("select sum(trip_id) from gold.fct_trips")[0][0]

    res = run(job, tags={"dagster/asset_partition_range_start": "2025-03",
                         "dagster/asset_partition_range_end": "2025-04"})

    assert res.success
    assert fct_by_month(lakehouse) == [("2025-03", 4), ("2025-04", 3)]
    # March was rebuilt by the backfill but holds exactly the same trips.
    assert lakehouse.query("select sum(trip_id) from gold.fct_trips where pickup_month = date '2025-03-01'")[0][0] \
        == march_before
    # Single-run backfill: one ingest step covering both months and one dbt step.
    starts = [e.step_key for e in res.all_events if e.event_type_value == "STEP_START"]
    assert sorted(starts) == ["bronze__load_log", "taxi_dbt_assets"]
    bronze = [e.event_specific_data.materialization for e in res.all_events
              if e.event_type_value == "ASSET_MATERIALIZATION" and e.step_key == "bronze__load_log"]
    assert [m.metadata["months"].value for m in bronze] == ["2025-03, 2025-04"] * len(bronze)


def test_missing_source_file_fails_run_before_dbt(dagster_lakehouse):
    lakehouse, job = dagster_lakehouse

    res = run(job, partition_key="2025-05")  # nothing published for May

    assert not res.success
    failed_steps = [e.step_key for e in res.all_events if e.event_type_value == "STEP_FAILURE"]
    assert failed_steps == ["bronze__load_log"]
    assert not lakehouse.warehouse.exists() or \
        lakehouse.query("select count(*) from information_schema.tables where table_name = 'fct_trips'")[0][0] == 0


def test_failing_dbt_test_fails_the_run(dagster_lakehouse):
    lakehouse, job = dagster_lakehouse
    assert run(job, tags={"dagster/asset_partition_range_start": "2025-03",
                          "dagster/asset_partition_range_end": "2025-04"}).success
    con = duckdb.connect(str(lakehouse.warehouse))
    con.execute("delete from gold.fct_trips where pickup_month = date '2025-04-01'")  # lose April's rows
    con.close()

    res = run(job, partition_key="2025-03")  # reconciliation covers every partition, not just March

    assert not res.success


def test_definitions_load_like_dagster_dev_without_dbt_on_path():
    """`dagster dev` sets DAGSTER_IS_DEV_CLI, which makes prepare_if_dev() run
    `dbt parse`. It must work when the venv is not activated (dbt not on PATH)."""
    import os
    import sys
    env = {**os.environ, "DAGSTER_IS_DEV_CLI": "1", "PATH": "/usr/bin:/bin"}
    proc = subprocess.run([sys.executable, "-W", "ignore", "-c", "import orchestration.definitions as d; print(len(d.defs.assets))"],
                          cwd=config.PROJECT_ROOT, env=env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert proc.stdout.strip() == "2"
