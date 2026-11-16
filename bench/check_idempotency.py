"""Idempotency: run the Dagster job for one month twice and compare row counts.

  python -m bench.check_idempotency 2026-09

Each run re-ingests the month (files unchanged -> 'skipped') and rebuilds the
month's partitions in every incremental model (forced via the months var).
Counts for the month and for the whole warehouse must be identical before,
after run 1 and after run 2.
"""

import sys
import time
import warnings

import duckdb
from dagster import DagsterInstance

from bench.bench_refresh import counts
from bench.common import save
from pipeline import config

warnings.filterwarnings("ignore")


def trip_id_checksum(month: str) -> int:
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    s = con.execute(f"select sum(trip_id) from gold.fct_trips where pickup_month = date '{month}-01'").fetchone()[0]
    con.close()
    return int(s)


def load_statuses(month: str) -> list:
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    rows = con.execute("select service, status from bronze.load_log where year_month = ? order by load_id",
                       [month]).fetchall()
    con.close()
    return rows


def main(month: str) -> None:
    from orchestration.definitions import defs
    job = defs.get_job_def("monthly_pipeline")
    snapshots = [{"label": "before", "month": counts(month), "total": counts(), "trip_id_sum": trip_id_checksum(month)}]
    for i in (1, 2):
        t0 = time.perf_counter()
        res = job.execute_in_process(partition_key=month, instance=DagsterInstance.ephemeral(), raise_on_error=False)
        wall = time.perf_counter() - t0
        if not res.success:
            raise SystemExit(f"run {i} failed")
        snapshots.append({"label": f"after run {i}", "wall_s": round(wall, 2), "month": counts(month),
                          "total": counts(), "trip_id_sum": trip_id_checksum(month)})
        print(snapshots[-1])
    keys = ("month", "total", "trip_id_sum")
    unchanged = all(s[k] == snapshots[0][k] for s in snapshots for k in keys)
    save("idempotency", {"month": month, "unchanged": unchanged, "snapshots": snapshots,
                         "load_log_rows_for_month": load_statuses(month)})
    print("unchanged:", unchanged)


if __name__ == "__main__":
    main(sys.argv[1])
