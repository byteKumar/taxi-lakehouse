"""Bronze ingest: download TLC monthly Parquet files into a Hive-style folder
layout and record every attempt in bronze.load_log.

Bronze is the raw files themselves (data/raw/service=.../year=.../month=...).
DuckDB and dbt read them in place, so the raw data is never copied into the
warehouse.

Idempotency rules for one (service, month):
  * remote ETag and size match the last load AND the local file still has the
    recorded SHA-256  -> skip (no download, file untouched)
  * otherwise download to a temp file and hash it
      - same SHA-256 as the last load and local file intact -> skip
      - different (or first time)                           -> atomically
        replace the local file, status 'replaced' (or 'loaded')
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from pipeline import config

CHUNK = 1 << 20  # 1 MiB

LOAD_LOG_DDL = """
create schema if not exists bronze;
create sequence if not exists bronze.load_log_seq;
create table if not exists bronze.load_log (
    load_id      bigint primary key default nextval('bronze.load_log_seq'),
    service      varchar   not null,
    year_month   varchar   not null,   -- 'YYYY-MM' of the source file
    status       varchar   not null,   -- loaded | replaced | skipped | failed
    source_url   varchar   not null,
    local_path   varchar,              -- relative to the raw dir
    etag         varchar,
    size_bytes   bigint,
    sha256       varchar,
    row_count    bigint,
    started_at   timestamp not null,
    finished_at  timestamp not null,
    duration_s   double    not null,
    error        varchar
);
"""


@dataclass
class LoadResult:
    service: str
    year_month: str
    status: str
    source_url: str
    local_path: str | None
    etag: str | None
    size_bytes: int | None
    sha256: str | None
    row_count: int | None
    started_at: datetime
    finished_at: datetime
    error: str | None = None

    @property
    def duration_s(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


def connect(warehouse: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(warehouse or config.WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(LOAD_LOG_DDL)
    return con


def last_good_load(con, service: str, year_month: str) -> dict | None:
    """Most recent load that put a file on disk (loaded or replaced)."""
    row = con.execute(
        """
        select etag, size_bytes, sha256, row_count, local_path
        from bronze.load_log
        where service = ? and year_month = ? and status in ('loaded', 'replaced')
        order by load_id desc limit 1
        """,
        [service, year_month],
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["etag", "size_bytes", "sha256", "row_count", "local_path"], row))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def head(url: str, timeout: float = 30) -> tuple[str | None, int | None]:
    """Return (etag, size) from a HEAD request. Falls back to Last-Modified
    when the server sends no ETag (e.g. Python's http.server in tests)."""
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        etag = resp.headers.get("ETag") or resp.headers.get("Last-Modified")
        size = resp.headers.get("Content-Length")
        return etag, int(size) if size is not None else None


def download(url: str, dest: Path, timeout: float = 60) -> str:
    """Stream url to dest, return the SHA-256 of what was written."""
    h = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=timeout) as resp, open(dest, "wb") as out:
        while chunk := resp.read(CHUNK):
            h.update(chunk)
            out.write(chunk)
    return h.hexdigest()


def ingest_one(service: str, year_month: str, prev: dict | None, raw_dir: Path, base_url: str) -> LoadResult:
    """Network and file work for one month. Does not touch DuckDB, so it can
    run in a worker thread; the caller writes the load_log row."""
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    url = config.source_url(service, year_month, base_url)
    final = config.raw_path(service, year_month, raw_dir)
    rel = str(final.relative_to(raw_dir))

    def result(status, etag=None, size=None, sha=None, rows=None, error=None, path=rel):
        return LoadResult(service, year_month, status, url, path, etag, size, sha, rows,
                          started, datetime.now(timezone.utc).replace(tzinfo=None), error)

    tmp = final.with_name(final.name + ".part")
    try:
        etag, size = head(url)

        local_ok = prev is not None and final.exists() and sha256_file(final) == prev["sha256"]
        if local_ok and etag == prev["etag"] and size == prev["size_bytes"]:
            return result("skipped", etag, size, prev["sha256"], prev["row_count"])

        final.parent.mkdir(parents=True, exist_ok=True)
        sha = download(url, tmp)
        if local_ok and sha == prev["sha256"]:
            # Server metadata changed but the bytes did not.
            tmp.unlink()
            return result("skipped", etag, size, sha, prev["row_count"])

        rows = pq.ParquetFile(tmp).metadata.num_rows  # also validates the file is Parquet
        os.replace(tmp, final)  # atomic on the same filesystem
        status = "replaced" if prev is not None else "loaded"
        return result(status, etag, final.stat().st_size, sha, rows)
    except Exception as exc:  # recorded in load_log, re-raised by the caller
        tmp.unlink(missing_ok=True)
        return result("failed", error=f"{type(exc).__name__}: {exc}", path=None)


def write_log(con, r: LoadResult) -> None:
    con.execute(
        """
        insert into bronze.load_log (service, year_month, status, source_url, local_path, etag,
            size_bytes, sha256, row_count, started_at, finished_at, duration_s, error)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [r.service, r.year_month, r.status, r.source_url, r.local_path, r.etag, r.size_bytes,
         r.sha256, r.row_count, r.started_at, r.finished_at, r.duration_s, r.error],
    )


def ingest(months: list[str], services=config.SERVICES, *, con=None, raw_dir: Path | None = None,
           base_url: str | None = None, workers: int = 4) -> list[LoadResult]:
    """Ingest every (service, month). Raises RuntimeError if any failed, after
    logging all of them."""
    own_con = con is None
    con = con or connect()
    raw_dir = Path(raw_dir or config.RAW_DIR)
    base_url = base_url or config.BASE_URL
    jobs = [(s, m) for m in months for s in services]
    prevs = {job: last_good_load(con, *job) for job in jobs}

    results: list[LoadResult] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(ingest_one, s, m, prevs[(s, m)], raw_dir, base_url) for s, m in jobs]
        for fut in futures:
            r = fut.result()
            write_log(con, r)
            results.append(r)
            print(f"{r.service:6} {r.year_month} {r.status:8} rows={r.row_count} "
                  f"{r.duration_s:6.1f}s {r.error or ''}", flush=True)
    if own_con:
        con.close()

    failed = [r for r in results if r.status == "failed"]
    if failed:
        raise RuntimeError(f"{len(failed)} file(s) failed: " +
                           ", ".join(f"{r.service} {r.year_month}" for r in failed))
    return results


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Download TLC monthly files into the bronze layer.")
    p.add_argument("--start", default=config.DEFAULT_START, help="first month, YYYY-MM")
    p.add_argument("--end", default=config.DEFAULT_END, help="last month, YYYY-MM (inclusive)")
    p.add_argument("--services", nargs="+", default=list(config.SERVICES), choices=config.SERVICES)
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args(argv)

    t0 = time.perf_counter()
    try:
        results = ingest(config.month_range(args.start, args.end), args.services, workers=args.workers)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    print(f"done in {time.perf_counter() - t0:.1f}s: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
