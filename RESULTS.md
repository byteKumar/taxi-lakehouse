# Results

All numbers below were measured on 2026-11-16 by the scripts in `bench/`. Raw
per-run data (every run, load average at start, per-model timings) is in
`bench/results/*.json`. Nothing is estimated; where a benchmark ran more than
once, every run is listed and the median is reported.

## Machine

| | |
|---|---|
| Chip | Apple M3 |
| Cores | 8 (4 performance + 4 efficiency) |
| RAM | 16 GB |
| Disk | 245 GB internal SSD (APFS), 5.7–12 GB free during the runs |
| Software | macOS (Darwin 25.6), Python 3.12.10, DuckDB 2.0.0, dbt-core 1.12.7 + dbt-duckdb 1.11.1, Dagster 1.13.30 |
| DuckDB settings | `memory_limit=6GB`, 4 dbt threads, spill to `data/tmp` |

The Mac was shared with six other projects' sessions (load tests, model
training, C++ builds). Every timed run waited until no other benchmark was
running and the 1-minute load average was below 4, and records the load it
started at. That gate does not stop other work from starting mid-run, which is
the likely cause of the spread in the full-rebuild times below.

## Scale

| | |
|---|---|
| Months | 25 (2024-09 .. 2026-09): 24 for the initial load, then 2026-09 as the "new month" |
| Rows ingested | **98,275,510** (yellow 97,083,542, green 1,191,968) |
| Raw Parquet | **1.69 GB** (1,685,652,775 bytes, 50 files) |
| Warehouse (DuckDB) | 3.5 GB after a fresh build (3.6 GB after the idempotency runs) |

## Data quality

- **45 dbt tests**: 41 generic tests in schema YAML (`unique`, `not_null`,
  `accepted_values`, `relationships`, plus two custom generic tests,
  `expression_is_true` and `unique_combination_of_columns`) and 4 singular tests
  (`assert_bronze_reconciles`, `assert_no_future_dates`,
  `assert_fct_pickup_in_partition_month`, `assert_marts_match_fct`).
- Result on the full 25 months: **44 pass, 1 warns (by design), 0 fail.** The
  warning is `fct_trips_longer_than_24h`: 513 valid trips last more than 24 h
  (the longest is about 11.9 days). Long trips are not a reject rule in the spec, so
  this test has `severity: warn`.
- `dbt build` (models + tests) exits 0. A failing test exits non-zero and skips
  every downstream model; `tests/test_quality.py` and
  `tests/test_orchestration.py` prove this by tampering with the data.

**Rejected rows by reason** (each rejected row has exactly one reason; first matching rule wins):

| Reason | Yellow | Green | Total |
|---|---:|---:|---:|
| NEGATIVE_FARE | 3,330,054 | 3,578 | 3,333,632 |
| ZERO_DISTANCE | 2,520,713 | 48,439 | 2,569,152 |
| DROPOFF_BEFORE_PICKUP | 3,506 | 1,351 | 4,857 |
| ABSURD_DISTANCE (> 100 mi) | 4,293 | 355 | 4,648 |
| PICKUP_OUTSIDE_FILE_MONTH | 555 | 446 | 1,001 |
| UNKNOWN_ZONE | 0 | 0 | 0 |
| MISSING_TIMESTAMP | 0 | 0 | 0 |
| **Total** | | | **5,913,290** (6.0% of rows) |

**Reconciliation: OK.** bronze 98,275,510 = fct_trips 92,362,220 + rejected
5,913,290, checked per (service, month) partition by `assert_bronze_reconciles`
(all 50 partitions match).

## Full rebuild vs. one new month

Both are `dbt build`, so the times include all 45 dbt tests.

| | Runs | Median |
|---|---|---|
| **Full rebuild**, `dbt build --full-refresh`, 25 months, 98.3M rows | 23.37 s, 35.62 s, 49.82 s | **35.62 s (0.59 min)** |
| **One new month** (2026-09, 3.92M rows), incremental `dbt build` | 8.07 s, 6.54 s, 6.99 s | **6.99 s (0.12 min)** |
| Download of the new month (68.5 MB, both services) | 1.35 s (once) | |

- The incremental run processed **only** `yellow|2026-09` and `green|2026-09`
  (recorded from `meta.partition_watermark` for every run).
- Run 1 of "one new month" is the real thing (download + build). Runs 2–3
  replay the processing step by deleting the month's rows and watermarks so
  `dbt build` sees the same stale partition again.
- In the incremental run, the models take about 1.8 s; the rest is the dbt tests,
  which scan whole tables (for example, `unique` on 92M `trip_id`s). The tests are not
  incremental, so they set the floor for an incremental run.
- The three full-rebuild runs differ by about 2×. All started with load below 4, but the
  script had to wait 442 s and 564 s for quiet before runs 2 and 3, and
  `fct_trips` alone took 16.4 s, 17.3 s and 33.9 s on identical data. The spread
  comes from other work on the shared machine, not from the pipeline; the median is
  reported, not the best run.

## Partitioning: unpartitioned vs. month-partitioned Parquet

`fct_trips` (92,362,220 rows, all 27 columns) exported twice with identical rows,
row order, zstd compression and 122,880-row row groups: one 1.69 GB file vs. 25
Hive folders `pickup_month=YYYY-MM-01/` (1.75 GB). Each query ran 5 times per
layout in a fresh in-memory DuckDB, alternating layouts; results were checked to
be identical between layouts. Median of 5:

| Query | Unpartitioned s | Partitioned s | Speed-up |
|---|---:|---:|---:|
| Q1 one month's revenue by pickup borough (2026-03, joins zones) | 0.0437 | 0.0662 | **0.66× (slower)** |
| Q2 airport trip share by service (2026-03) | 0.0300 | 0.0078 | 3.85× |
| Q3 daily trips for one quarter (2026-01..03) | 0.0344 | 0.0098 | 3.51× |
| Q4 yellow tip rate by payment type (2025-07) | 0.0341 | 0.0110 | 3.10× |
| Q5 control: revenue per month, all months, no filter | 0.1431 | 0.0653 | 2.19× |

What explains these numbers (checked, not guessed):

- **The unpartitioned file was already mostly clustered by month.** Only 55 of its 752
  row groups can contain 2026-03 according to their min/max statistics, so DuckDB
  already skips about 93% of the file without partitioning. Partitioning still
  wins 3–4× on filtered queries without joins (Q2–Q4), but this baseline is
  much stronger than a randomly ordered file would be.
- **Q1 is slower when partitioned because of a planner estimate, not missing
  pruning.** EXPLAIN on a 3-month copy shows Q1 reads 1 of 3 files, as it
  should. But with the Hive filter, DuckDB estimates the trips side of the join at
  about 1 row, so it builds the hash table on the trips (millions of rows) and probes it
  with the 265 zones. On the single file it estimates about 179K rows and builds on
  the zones. Same 3-month data: 0.018 s vs 0.063 s. The query was left as is
  rather than rewritten to suit the partitioned layout.
- **Q5 (no filter) is still 2.2× faster partitioned** because `pickup_month`
  comes from the folder name, not from the file, so there is one column fewer to read
  and decode. Q5 is a control for pruning, not for layout.
- Exports took 27.5 s (single file) and 38.2 s (partitioned).

## Idempotency

The Dagster job for 2026-09 was run twice (`bench/check_idempotency.py`):

| | bronze rows | fct_trips | rejected_trips | revenue mart rows | airport mart rows | sum(trip_id) |
|---|---:|---:|---:|---:|---:|---:|
| before | 3,922,720 | 3,760,044 | 162,676 | 410 | 2 | 3880593882565870674400 |
| after run 1 (13.2 s) | 3,922,720 | 3,760,044 | 162,676 | 410 | 2 | 3880593882565870674400 |
| after run 2 (12.4 s) | 3,922,720 | 3,760,044 | 162,676 | 410 | 2 | 3880593882565870674400 |

**Row counts unchanged: yes.** Warehouse totals were also identical (92,362,220
fct rows). Both re-ingests were logged as `skipped` in `bronze.load_log`.

## What failed or surprised us

1. **Out of memory on the first gold build.** `ORDER BY pickup_month` over 88M wide
   rows hit the 6 GB DuckDB limit. The sort was removed; incremental runs insert one
   month at a time, so those row groups stay clustered.
2. **DuckDB never shrinks its file.** Three `--full-refresh` runs grew
   `warehouse.duckdb` from 3.1 to 6.95 GB (13,460 of 26,722 blocks free) and
   pushed free disk to 5.7 GB. The file was compacted by copying `bronze.load_log`
   to a fresh file and rebuilding; every other table can be rebuilt from the raw files.
3. **Free disk briefly fell to 2.7 GB** during the partitioning benchmark (our
   3.2 GB of copies plus about 2.3 GB written by another session at the same time). The
   query runs only read files; the copies were deleted right after.
4. **Schema drift hidden by the data.** `cbd_congestion_fee` (from 2025-01) and
   `request_source` (from 2026-06) exist only in some files. The full range worked
   by luck; a test with only 2024 files failed. The fix is an explicit schema contract
   (an empty typed `UNION ALL BY NAME` branch).
5. Smaller bugs found along the way:
   - `trip_id` int32 overflow
   - Hive auto-typing reading `month=09` as VARCHAR
   - relative paths baked into views
   - a CTE/alias name clash
   - an unbounded watermark table
   - `dagster dev` failing when the venv isn't on `PATH` (found while taking the lineage screenshot; there is now a regression test)
6. 3.6% of yellow rows have a negative fare or total (refunds and voids), by far
   the largest reject reason.

## Not done or not verified

- **GitHub Actions has not run:** the account is billing-locked.
  `.github/workflows/ci.yml` and `scripts/ci_pipeline.sh` are in place, and the
  script passes locally (8.3 s on the 8,000-row sample).
- Full-rebuild and new-month times are 3 runs each; downloads and exports were
  measured once.
- No write-audit-publish step: a failing test stops downstream models, but the
  model that failed its test is already written (see README).

## Results template (spec section 6)

```
MACHINE: chip=Apple M3 cores=8 (4P+4E) RAM=16 GB
SCALE: rows=98,275,510 months=25 raw GB=1.69
QUALITY: dbt tests=45 all passing? yes: 44 pass + 1 warn-severity (513 trips > 24 h), 0 fail ; rejected rows by reason: NEGATIVE_FARE 3,333,632, ZERO_DISTANCE 2,569,152, DROPOFF_BEFORE_PICKUP 4,857, ABSURD_DISTANCE 4,648, PICKUP_OUTSIDE_FILE_MONTH 1,001, UNKNOWN_ZONE 0, MISSING_TIMESTAMP 0 (total 5,913,290) ; reconciliation ok? yes (98,275,510 = 92,362,220 + 5,913,290, all 50 partitions)
FULL REBUILD=0.59 min (median of 3: 23.4/35.6/49.8 s) ; ONE NEW MONTH=0.12 min (median of 3: 8.1/6.5/7.0 s, + 1.35 s download)
PARTITIONING (median of 5): query | unpartitioned s | partitioned s
                            Q1 one month revenue by borough (join) | 0.0437 | 0.0662
                            Q2 one month airport share | 0.0300 | 0.0078
                            Q3 one quarter daily trips | 0.0344 | 0.0098
                            Q4 one month yellow tip rate | 0.0341 | 0.0110
                            Q5 control, all months, no filter | 0.1431 | 0.0653
IDEMPOTENT RE-RUN row counts unchanged? yes (2026-09 run twice via Dagster: 3,922,720 bronze = 3,760,044 fct + 162,676 rejected each time)
ANYTHING THAT FAILED OR SURPRISED YOU: Q1 slower partitioned (DuckDB estimates ~1 row after the Hive filter and builds the join hash table on the wrong side); unpartitioned baseline already ~93% skippable via row-group stats; OOM sorting fct_trips at 6 GB; DuckDB file grew 3.1 -> 6.95 GB after 3 full refreshes (free blocks never returned); full-rebuild times spread 23-50 s from other sessions' load; schema-drift bug hidden by the full data range; CI not run (account billing-locked)
REPO pushed? yes
```
