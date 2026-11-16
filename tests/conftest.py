"""Shared fixtures: a local HTTP server standing in for the TLC CDN, tiny
hand-built trip files, and a helper that runs dbt against a temp warehouse."""

from __future__ import annotations

import functools
import http.server
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import config, ingest

DBT_DIR = config.PROJECT_ROOT / "dbt"
DBT_BIN = Path(sys.executable).parent / "dbt"


@pytest.fixture
def server(tmp_path):
    """Serve tmp_path/remote over HTTP; yields (remote_dir, base_url)."""
    remote = tmp_path / "remote"
    remote.mkdir()
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(remote))
    handler.log_message = lambda *a, **k: None
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield remote, f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def trip(pickup: str, dropoff: str, distance=2.0, fare=10.0, total=14.0, pu=161, do=236, **extra) -> dict:
    """One raw trip with sensible defaults (161 = Midtown Center, 236 = Upper East Side North)."""
    return dict(pickup=datetime.fromisoformat(pickup), dropoff=datetime.fromisoformat(dropoff),
                distance=distance, fare=fare, total=total, pu=pu, do=do, **extra)


def write_trips(dir_: Path, service: str, ym: str, trips: list[dict], new_columns: bool = True) -> None:
    """Write a file with the same column names and types as the real TLC files.
    new_columns=False drops cbd_congestion_fee/request_source, like pre-2025 files."""
    p = "tpep" if service == "yellow" else "lpep"
    n = len(trips)
    cols = {
        "VendorID": pa.array([2] * n, pa.int32()),
        f"{p}_pickup_datetime": pa.array([t["pickup"] for t in trips], pa.timestamp("us")),
        f"{p}_dropoff_datetime": pa.array([t["dropoff"] for t in trips], pa.timestamp("us")),
        "passenger_count": pa.array([1] * n, pa.int64()),
        "trip_distance": pa.array([t["distance"] for t in trips], pa.float64()),
        "RatecodeID": pa.array([1] * n, pa.int64()),
        "store_and_fwd_flag": pa.array(["N"] * n, pa.large_string()),
        "PULocationID": pa.array([t["pu"] for t in trips], pa.int32()),
        "DOLocationID": pa.array([t["do"] for t in trips], pa.int32()),
        "payment_type": pa.array([1] * n, pa.int64()),
        "fare_amount": pa.array([t["fare"] for t in trips], pa.float64()),
        "extra": pa.array([0.0] * n), "mta_tax": pa.array([0.5] * n),
        "tip_amount": pa.array([2.0] * n), "tolls_amount": pa.array([0.0] * n),
        "improvement_surcharge": pa.array([1.0] * n),
        "total_amount": pa.array([t["total"] for t in trips], pa.float64()),
        "congestion_surcharge": pa.array([2.5] * n),
    }
    if service == "yellow":
        cols["Airport_fee"] = pa.array([t.get("airport_fee", 0.0) for t in trips])
    else:
        cols["ehail_fee"] = pa.array([None] * n, pa.float64())
        cols["trip_type"] = pa.array([1] * n, pa.int64())
    if new_columns:
        cols["cbd_congestion_fee"] = pa.array([0.75] * n)
        cols["request_source"] = pa.array(["app"] * n, pa.large_string())
    pq.write_table(pa.table(cols), dir_ / f"{service}_tripdata_{ym}.parquet")


class Lakehouse:
    """A throwaway raw dir + warehouse, filled through the real ingest code."""

    def __init__(self, tmp_path: Path, remote: Path, base_url: str):
        self.tmp, self.remote, self.base_url = tmp_path, remote, base_url
        self.raw = tmp_path / "raw"
        self.warehouse = tmp_path / "wh.duckdb"

    def ingest(self, months, services=config.SERVICES):
        con = ingest.connect(self.warehouse)
        try:
            return ingest.ingest(months, services, con=con, raw_dir=self.raw, base_url=self.base_url, workers=2)
        finally:
            con.close()

    def dbt(self, *args, check=True) -> subprocess.CompletedProcess:
        env = {**os.environ,
               "TAXI_WAREHOUSE": str(self.warehouse), "TAXI_RAW_DIR": str(self.raw),
               "TAXI_DUCKDB_TMP": str(self.tmp / "duckdb_tmp"), "TAXI_DUCKDB_MEMORY": "1GB",
               "DBT_TARGET_PATH": str(self.tmp / "target"), "DBT_LOG_PATH": str(self.tmp / "logs")}
        proc = subprocess.run([str(DBT_BIN), *args, "--profiles-dir", "."], cwd=DBT_DIR, env=env,
                              capture_output=True, text=True)
        if check and proc.returncode != 0:
            raise AssertionError(f"dbt {' '.join(args)} failed:\n{proc.stdout[-4000:]}\n{proc.stderr[-2000:]}")
        return proc

    def query(self, sql: str):
        con = duckdb.connect(str(self.warehouse), read_only=True)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()


@pytest.fixture
def lakehouse(tmp_path, server):
    remote, base_url = server
    return Lakehouse(tmp_path, remote, base_url)
