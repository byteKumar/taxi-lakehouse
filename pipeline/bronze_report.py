"""Print what is in the bronze layer: load_log summary and how the raw file
schemas differ across months (input for the silver standardization)."""

from __future__ import annotations

import sys

import duckdb
import pyarrow.parquet as pq

from pipeline import config


def main() -> int:
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    print("== latest load per file ==")
    print(con.sql("""
        with latest as (
            select *, row_number() over (partition by service, year_month order by load_id desc) rn
            from bronze.load_log
        )
        select service, count(*) as months, min(year_month) as first, max(year_month) as last,
               sum(row_count) as rows, round(sum(size_bytes) / 1e9, 3) as raw_gb,
               string_agg(distinct status, ',') as statuses
        from latest where rn = 1 group by service order by service
    """))
    print(con.sql("select status, count(*) as attempts from bronze.load_log group by status order by status"))

    print("== schema variants (columns and types per group of months) ==")
    for service in config.SERVICES:
        variants: dict[tuple, list[str]] = {}
        for path in sorted(config.RAW_DIR.glob(f"service={service}/year=*/month=*/*.parquet")):
            schema = pq.read_schema(path)
            key = tuple((f.name, str(f.type)) for f in schema)
            variants.setdefault(key, []).append(path.stem.split("_")[-1])
        for key, months in variants.items():
            print(f"{service}: {len(months)} months ({months[0]} .. {months[-1]})")
            print("   " + ", ".join(f"{n}:{t}" for n, t in key))
    return 0


if __name__ == "__main__":
    sys.exit(main())
