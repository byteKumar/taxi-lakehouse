"""Five analytics queries on unpartitioned vs. month-partitioned Parquet.

  python -m bench.bench_partitioning export   # writes data/bench/ (two copies of fct_trips)
  python -m bench.bench_partitioning run      # 5 runs per query per layout, median
  python -m bench.bench_partitioning clean    # deletes data/bench/

Both copies come from gold.fct_trips with the same rows, columns, row order,
compression (zstd) and row-group size; the only difference is the layout:
  unpartitioned: one file, rows in table order (months interleaved, as a full
                 rebuild leaves them)
  partitioned:   Hive folders pickup_month=YYYY-MM-01/, one per month
Queries run in a fresh in-memory DuckDB (all 8 threads) that reads only the
Parquet files plus the 265-row zone CSV. Runs alternate layouts so neither
gets a systematically warmer cache. Each query's result is compared between
layouts and must be identical.
"""

import shutil
import statistics
import sys
import time

import duckdb
import pyarrow.parquet as pq

from bench.common import load1, save, wait_for_quiet
from pipeline import config

BENCH_DIR = config.DATA_DIR / "bench"
UNPART = BENCH_DIR / "unpartitioned" / "fct_trips.parquet"
PART = BENCH_DIR / "partitioned"
ZONES = config.PROJECT_ROOT / "dbt" / "seeds" / "taxi_zone_lookup.csv"
RUNS = 5

QUERIES = {
    "Q1 one month's revenue by pickup borough (2026-03)": """
        select z.Borough, count(*) as trips, round(sum(t.total_amount), 2) as revenue
        from trips t join zones z on z.LocationID = t.pu_location_id
        where t.pickup_month = date '2026-03-01'
        group by 1 order by 1""",
    "Q2 airport trip share by service (2026-03)": """
        select t.service_type, round(avg(t.is_airport_trip::int), 6) as airport_share, count(*) as trips
        from trips t where t.pickup_month = date '2026-03-01'
        group by 1 order by 1""",
    "Q3 daily trips for one quarter (2026-01..03)": """
        select t.pickup_date, count(*) as trips
        from trips t where t.pickup_month between date '2026-01-01' and date '2026-03-01'
        group by 1 order by 1""",
    "Q4 yellow tip rate by payment type (2025-07)": """
        select t.payment_type, round(sum(t.tip_amount) / nullif(sum(t.fare_amount), 0), 6) as tip_rate, count(*) as trips
        from trips t where t.service_type = 'yellow' and t.pickup_month = date '2025-07-01'
        group by 1 order by 1""",
    "Q5 control: revenue per month, all months (no filter)": """
        select t.pickup_month, round(sum(t.total_amount), 2) as revenue
        from trips t group by 1 order by 1""",
}


def size_gb(path) -> float:
    return round(sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e9, 3) if path.is_dir() \
        else round(path.stat().st_size / 1e9, 3)


def export() -> None:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    con.execute("set preserve_insertion_order = true")  # same row order in both copies
    con.execute(f"set temp_directory = '{config.DATA_DIR / 'tmp' / 'duckdb'}'")
    opts = "format parquet, compression zstd, row_group_size 122880"
    UNPART.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    con.execute(f"copy (select * from gold.fct_trips) to '{UNPART}' ({opts})")
    print(f"unpartitioned: {size_gb(UNPART)} GB in {time.perf_counter() - t0:.1f}s; free disk "
          f"{shutil.disk_usage(config.DATA_DIR).free / 1e9:.1f} GB", flush=True)
    if shutil.disk_usage(config.DATA_DIR).free / 1e9 - size_gb(UNPART) < 5:
        raise SystemExit("a second copy would leave < 5 GB free; stopping (run `clean`)")
    t0 = time.perf_counter()
    con.execute(f"copy (select * from gold.fct_trips) to '{PART}' ({opts}, partition_by (pickup_month))")
    print(f"partitioned:   {size_gb(PART)} GB in {time.perf_counter() - t0:.1f}s; "
          f"{len(list(PART.glob('*/*.parquet')))} files", flush=True)
    con.close()


def connect(layout: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if layout == "unpartitioned":
        con.execute(f"create view trips as select * from read_parquet('{UNPART}')")
    else:
        con.execute(f"""create view trips as select * from read_parquet('{PART}/*/*.parquet',
                        hive_partitioning = true, hive_types = {{'pickup_month': 'DATE'}})""")
    con.execute(f"create view zones as select * from read_csv('{ZONES}')")
    return con


def row_groups_for_month(month: str) -> dict:
    """How many of the unpartitioned file's row groups could contain the month,
    judging only by their min/max statistics (what DuckDB can skip)."""
    md = pq.ParquetFile(UNPART).metadata
    idx = md.schema.to_arrow_schema().get_field_index("pickup_month")
    hit = 0
    for i in range(md.num_row_groups):
        st = md.row_group(i).column(idx).statistics
        if str(st.min) <= month <= str(st.max):
            hit += 1
    return {"row_groups": md.num_row_groups, "row_groups_overlapping": hit}


def run() -> None:
    quiet = wait_for_quiet(timeout_s=int(__import__('os').environ.get('BENCH_MAX_WAIT_S', 3600)))
    times = {q: {"unpartitioned": [], "partitioned": []} for q in QUERIES}
    loads = []
    for r in range(RUNS):
        order = ["unpartitioned", "partitioned"] if r % 2 == 0 else ["partitioned", "unpartitioned"]
        for q, sql in QUERIES.items():
            results = {}
            for layout in order:
                con = connect(layout)
                t0 = time.perf_counter()
                results[layout] = con.execute(sql).fetchall()
                times[q][layout].append(round(time.perf_counter() - t0, 4))
                con.close()
            if results["unpartitioned"] != results["partitioned"]:
                raise SystemExit(f"results differ between layouts for {q}")
        loads.append(round(load1(), 2))
        print(f"round {r + 1}/{RUNS} done (load1 {loads[-1]})", flush=True)

    table = []
    for q in QUERIES:
        u, p = (statistics.median(times[q][k]) for k in ("unpartitioned", "partitioned"))
        table.append({"query": q, "unpartitioned_s_median": round(u, 4), "partitioned_s_median": round(p, 4),
                      "speedup": round(u / p, 2), "unpartitioned_s_runs": times[q]["unpartitioned"],
                      "partitioned_s_runs": times[q]["partitioned"]})
        print(f"{q:58} {u:8.3f}s {p:8.3f}s  x{u / p:.1f}")
    save("partitioning", {
        **quiet, "load1_after_each_round": loads, "runs_per_query_per_layout": RUNS,
        "unpartitioned_gb": size_gb(UNPART), "partitioned_gb": size_gb(PART),
        "partitioned_files": len(list(PART.glob("*/*.parquet"))),
        "unpartitioned_row_group_stats_for_2026_03": row_groups_for_month("2026-03-01"),
        "rows": duckdb.connect().execute(f"select count(*) from read_parquet('{UNPART}')").fetchone()[0],
        "queries": table,
    })


if __name__ == "__main__":
    {"export": export, "run": run, "clean": lambda: shutil.rmtree(BENCH_DIR, ignore_errors=True)}[sys.argv[1]]()
