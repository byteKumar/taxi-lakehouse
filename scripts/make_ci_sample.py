"""Cut the CI sample: the first N rows of a few real TLC files, keeping each
file's exact schema. Months are chosen to cover the schema variants
(2024-12: no cbd_congestion_fee; 2026-06: has cbd_congestion_fee and
request_source)."""

import sys

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
OUT = config.PROJECT_ROOT / "ci" / "sample"

for service in config.SERVICES:
    for ym in ("2024-12", "2026-06"):
        src = config.raw_path(service, ym)
        pf = pq.ParquetFile(src)
        batch = next(pf.iter_batches(batch_size=N))
        dest = OUT / src.name
        pq.write_table(pa.Table.from_batches([batch]), dest)
        print(dest.relative_to(config.PROJECT_ROOT), pq.ParquetFile(dest).metadata.num_rows, "rows",
              dest.stat().st_size, "bytes")
