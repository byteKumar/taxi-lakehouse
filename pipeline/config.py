"""Paths and defaults shared by the ingest code, dbt and Dagster."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Everything large lives under DATA_DIR (git-ignored). Override with env vars
# so tests and CI can point the pipeline at a temporary folder.
DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = Path(os.environ.get("TAXI_RAW_DIR", DATA_DIR / "raw"))
WAREHOUSE_PATH = Path(os.environ.get("TAXI_WAREHOUSE", DATA_DIR / "warehouse.duckdb"))

BASE_URL = os.environ.get("TAXI_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")

SERVICES = ("yellow", "green")

# Initial backfill range (inclusive). 2026-09 is held back to measure an
# incremental "one new month" run.
DEFAULT_START = "2024-09"
DEFAULT_END = "2026-08"


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' strings from start to end."""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    if (y, m) > (ey, em):
        raise ValueError(f"start {start} is after end {end}")
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def raw_path(service: str, year_month: str, raw_dir: Path | None = None) -> Path:
    """Hive-style location of one raw monthly file."""
    year, month = year_month.split("-")
    root = raw_dir or RAW_DIR
    return root / f"service={service}" / f"year={year}" / f"month={month}" / f"{service}_tripdata_{year_month}.parquet"


def source_url(service: str, year_month: str, base_url: str | None = None) -> str:
    return f"{base_url or BASE_URL}/{service}_tripdata_{year_month}.parquet"
