"""Dagster definitions: one monthly-partitioned asset graph.

    bronze/load_log  (Python: download the month's yellow + green files)
        -> dbt staging/silver views, rejected_trips, fct_trips, dims, marts
           (dbt build --vars '{"months": [...]}' for the selected partitions)

A partition is one calendar month. A run for month M re-ingests M (skipped if
the files are unchanged) and rebuilds only M's partitions in every incremental
dbt model. dbt tests become Dagster asset checks; a failing test fails the run.

Backfills use a single-run policy: selecting 12 months launches ONE run that
passes all 12 to the ingest and to one dbt build, instead of 12 runs that
would fight over the single DuckDB writer lock.
"""


import json
import os
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    BackfillPolicy,
    Definitions,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline import config, ingest

# dbt runs as a subprocess. Put this venv's bin/ on PATH: `dagster dev` calls
# prepare_if_dev(), which runs `dbt parse` through its own resource that looks
# dbt up on PATH and ignores dbt_executable below.
_venv_bin = str(Path(sys.executable).parent)
if _venv_bin not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = _venv_bin + os.pathsep + os.environ.get("PATH", "")

# Give dbt absolute paths so it does not depend on cwd.
os.environ.setdefault("TAXI_WAREHOUSE", str(config.WAREHOUSE_PATH))
os.environ.setdefault("TAXI_RAW_DIR", str(config.RAW_DIR))
os.environ.setdefault("TAXI_DUCKDB_TMP", str(config.DATA_DIR / "tmp" / "duckdb"))

# TLC publishes a month roughly 6-8 weeks after it ends, so the newest
# partition is the month before last (end_offset=-1): on 2026-11-16 that is
# 2026-09, which matches what the CDN serves.
monthly = MonthlyPartitionsDefinition(
    start_date=os.environ.get("TAXI_FIRST_MONTH", config.DEFAULT_START),
    fmt="%Y-%m",
    end_offset=-1,
)

dbt_project = DbtProject(project_dir=config.PROJECT_ROOT / "dbt", profiles_dir=config.PROJECT_ROOT / "dbt")
dbt_project.prepare_if_dev()  # runs `dbt parse` under `dagster dev` so the manifest is fresh


@asset(
    key=["bronze", "load_log"],
    group_name="bronze",
    partitions_def=monthly,
    backfill_policy=BackfillPolicy.single_run(),
    description="Raw TLC yellow + green Parquet files for the month (Hive layout under data/raw) "
                "and their bronze.load_log rows. Unchanged files are skipped.",
)
def bronze_trip_files(context: AssetExecutionContext) -> MaterializeResult:
    months = context.partition_keys
    results = ingest.ingest(months, config.SERVICES, base_url=os.environ.get("TAXI_BASE_URL"))
    statuses: dict[str, int] = {}
    for r in results:
        statuses[r.status] = statuses.get(r.status, 0) + 1
    return MaterializeResult(metadata={
        "months": ", ".join(months),
        "rows": sum(r.row_count or 0 for r in results),
        "statuses": json.dumps(statuses),
        "bytes": sum(r.size_bytes or 0 for r in results),
    })


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    partitions_def=monthly,
    backfill_policy=BackfillPolicy.single_run(),
)
def taxi_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    months = context.partition_keys
    context.log.info(f"dbt build for months {months}")
    yield from dbt.cli(["build", "--vars", json.dumps({"months": months})], context=context).stream()


monthly_pipeline = define_asset_job(
    "monthly_pipeline",
    selection=AssetSelection.all(),
    partitions_def=monthly,
    description="Ingest one month (or a backfill range) and rebuild its partitions in dbt.",
)

# On the 20th of each month, run the newest partition (two months back).
monthly_schedule = build_schedule_from_partitioned_job(
    monthly_pipeline, day_of_month=20, hour_of_day=6, minute_of_hour=0,
)

defs = Definitions(
    assets=[bronze_trip_files, taxi_dbt_assets],
    jobs=[monthly_pipeline],
    schedules=[monthly_schedule],
    # Use the dbt from this venv, which need not be on PATH.
    resources={"dbt": DbtCliResource(project_dir=dbt_project,
                                     dbt_executable=str(Path(sys.executable).parent / "dbt"))},
)
