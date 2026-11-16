"""Bronze ingest tests against a real local HTTP server serving tiny Parquet
files, so the HEAD / download / checksum path is the same one used for TLC."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import config, ingest


def write_month(dir_: Path, service: str, ym: str, n_rows: int) -> None:
    table = pa.table({"VendorID": list(range(n_rows)), "fare_amount": [10.0] * n_rows})
    pq.write_table(table, dir_ / f"{service}_tripdata_{ym}.parquet")


@pytest.fixture
def env(tmp_path, server):
    remote, base_url = server
    raw = tmp_path / "raw"
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    con.execute(ingest.LOAD_LOG_DDL)

    def run(months, services=("yellow",)):
        return ingest.ingest(months, services, con=con, raw_dir=raw, base_url=base_url, workers=2)

    yield remote, raw, con, run
    con.close()


def statuses(results):
    return [r.status for r in results]


def test_month_range():
    assert config.month_range("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]
    with pytest.raises(ValueError):
        config.month_range("2025-02", "2025-01")


def test_first_load_writes_hive_path_and_log(env):
    remote, raw, con, run = env
    write_month(remote, "yellow", "2025-01", 7)
    write_month(remote, "green", "2025-01", 3)

    results = run(["2025-01"], ("yellow", "green"))

    assert statuses(results) == ["loaded", "loaded"]
    assert (raw / "service=yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet").exists()
    rows = con.execute("select service, row_count, length(sha256) from bronze.load_log order by service").fetchall()
    assert rows == [("green", 3, 64), ("yellow", 7, 64)]


def test_rerun_unchanged_is_skipped_and_file_untouched(env):
    remote, raw, con, run = env
    write_month(remote, "yellow", "2025-01", 5)
    run(["2025-01"])
    path = config.raw_path("yellow", "2025-01", raw)
    mtime = path.stat().st_mtime_ns

    results = run(["2025-01"])

    assert statuses(results) == ["skipped"]
    assert results[0].row_count == 5
    assert path.stat().st_mtime_ns == mtime
    good = con.execute("select count(*) from bronze.load_log where status in ('loaded','replaced')").fetchone()[0]
    assert good == 1


def test_changed_remote_file_is_replaced(env):
    remote, raw, con, run = env
    write_month(remote, "yellow", "2025-01", 5)
    run(["2025-01"])
    old_sha = con.execute("select sha256 from bronze.load_log").fetchone()[0]

    time.sleep(1.1)  # http.server's Last-Modified has 1 s resolution
    write_month(remote, "yellow", "2025-01", 9)
    results = run(["2025-01"])

    assert statuses(results) == ["replaced"]
    assert results[0].row_count == 9 and results[0].sha256 != old_sha
    assert pq.ParquetFile(config.raw_path("yellow", "2025-01", raw)).metadata.num_rows == 9


def test_corrupted_local_file_is_redownloaded(env):
    remote, raw, con, run = env
    write_month(remote, "yellow", "2025-01", 5)
    run(["2025-01"])
    path = config.raw_path("yellow", "2025-01", raw)
    path.write_bytes(b"not parquet")

    results = run(["2025-01"])

    assert statuses(results) == ["replaced"]
    assert pq.ParquetFile(path).metadata.num_rows == 5


def test_missing_remote_file_fails_loudly_and_leaves_no_partial(env):
    remote, raw, con, run = env
    write_month(remote, "yellow", "2025-01", 5)

    with pytest.raises(RuntimeError, match="yellow 2025-02"):
        run(["2025-01", "2025-02"])

    log = dict(con.execute("select year_month, status from bronze.load_log").fetchall())
    assert log == {"2025-01": "loaded", "2025-02": "failed"}
    assert not list(raw.rglob("*.part"))
    assert not config.raw_path("yellow", "2025-02", raw).exists()


def test_non_parquet_download_is_rejected(env):
    remote, raw, con, run = env
    (remote / "yellow_tripdata_2025-01.parquet").write_bytes(b"<html>error page</html>")

    with pytest.raises(RuntimeError):
        run(["2025-01"])

    assert not config.raw_path("yellow", "2025-01", raw).exists()
