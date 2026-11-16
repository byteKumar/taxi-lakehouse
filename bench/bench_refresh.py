"""Full rebuild vs. adding one new month.

  python -m bench.bench_refresh new-month 2026-09   # month must not be loaded yet
  python -m bench.bench_refresh full

new-month: run 1 is the real thing: download the month, then an incremental
`dbt build` that must process only that month. Runs 2..N replay the
processing step: the month's rows and watermarks are removed from the
incremental models, so `dbt build` sees exactly the same stale partition again.
The download is timed once (it is network-bound and only happens once).

full: `dbt build --full-refresh` over everything in bronze, N times.

Every run is reported, plus the median. dbt build includes all dbt tests.
"""

import statistics
import sys
import time

import duckdb

from bench.common import dbt, save, wait_for_quiet
from pipeline import config, ingest

INCREMENTAL = {"rejected_trips": ("silver", "source_month"), "fct_trips": ("gold", "pickup_month"),
               "mart_daily_revenue_by_borough": ("gold", "pickup_month"),
               "mart_airport_trip_share": ("gold", "pickup_month")}


def counts(month: str | None = None) -> dict:
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    where = f"where {{col}} = date '{month}-01'" if month else ""
    out = {name: con.execute(f"select count(*) from {schema}.{name} {where.format(col=col)}").fetchone()[0]
           for name, (schema, col) in INCREMENTAL.items()}
    out["bronze_rows"] = con.execute(
        "select sum(row_count) from (select row_count, row_number() over (partition by service, year_month "
        "order by load_id desc) rn from bronze.load_log where status in ('loaded','replaced') "
        + (f"and year_month = '{month}'" if month else "") + ") where rn = 1").fetchone()[0]
    out["months_in_fct"] = con.execute("select count(distinct pickup_month) from gold.fct_trips").fetchone()[0]
    con.close()
    return out


def processed_partitions(since) -> list[str]:
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    rows = con.execute("select distinct service || '|' || year_month from meta.partition_watermark "
                       "where processed_at >= ? order by 1", [since]).fetchall()
    con.close()
    return [r[0] for r in rows]


def forget_month(month: str) -> None:
    con = duckdb.connect(str(config.WAREHOUSE_PATH))
    for name, (schema, col) in INCREMENTAL.items():
        con.execute(f"delete from {schema}.{name} where {col} = date '{month}-01'")
    con.execute("delete from meta.partition_watermark where year_month = ?", [month])
    con.close()


def new_month(month: str, runs: int) -> None:
    con = ingest.connect()
    already = con.execute("select count(*) from bronze.load_log where year_month = ? and status in ('loaded','replaced')",
                          [month]).fetchone()[0]
    con.close()
    if already:
        raise SystemExit(f"{month} is already loaded; this benchmark needs a month that is new")
    before = counts()
    results = []

    quiet = wait_for_quiet()
    t0 = time.perf_counter()
    loads = ingest.ingest([month])
    download_s = time.perf_counter() - t0
    from datetime import datetime
    since = datetime.now()
    wall, detail = dbt("build")
    results.append({"run": 1, "kind": "real new month", **quiet, "dbt_build_s": round(wall, 2),
                    "processed_partitions": processed_partitions(since), **detail})
    print(f"run 1: download {download_s:.1f}s, dbt build {wall:.1f}s, processed {results[-1]['processed_partitions']}")

    for i in range(2, runs + 1):
        forget_month(month)
        quiet = wait_for_quiet()
        since = datetime.now()
        wall, detail = dbt("build")
        results.append({"run": i, "kind": "replay (month's rows + watermarks removed)", **quiet,
                        "dbt_build_s": round(wall, 2), "processed_partitions": processed_partitions(since), **detail})
        print(f"run {i}: dbt build {wall:.1f}s, processed {results[-1]['processed_partitions']}")

    after = counts()
    save("new_month", {
        "month": month,
        "download_s": round(download_s, 2),
        "download_bytes": sum(r.size_bytes or 0 for r in loads),
        "new_rows": sum(r.row_count or 0 for r in loads),
        "dbt_build_s_runs": [r["dbt_build_s"] for r in results],
        "dbt_build_s_median": round(statistics.median(r["dbt_build_s"] for r in results), 2),
        "counts_before": before, "counts_after": after, "month_counts": counts(month),
        "runs": results,
    })


def full(runs: int) -> None:
    results = []
    for i in range(1, runs + 1):
        quiet = wait_for_quiet()
        wall, detail = dbt("build", "--full-refresh")
        results.append({"run": i, **quiet, "dbt_build_s": round(wall, 2), **detail})
        print(f"run {i}: dbt build --full-refresh {wall:.1f}s ({detail['summary']})")
    save("full_rebuild", {
        "dbt_build_s_runs": [r["dbt_build_s"] for r in results],
        "dbt_build_s_median": round(statistics.median(r["dbt_build_s"] for r in results), 2),
        "counts_after": counts(),
        "runs": results,
    })


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "new-month":
        new_month(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 3)
    elif mode == "full":
        full(int(sys.argv[2]) if len(sys.argv) > 2 else 3)
    else:
        raise SystemExit(__doc__)
