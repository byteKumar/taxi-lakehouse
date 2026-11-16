#!/usr/bin/env bash
# Run the real pipeline (ingest over HTTP -> dbt build with all tests) on the
# committed 8,000-row sample in ci/sample. Used by GitHub Actions; runs the
# same way locally. Exits non-zero if any step or dbt test fails.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
PY=${PYTHON:-$ROOT/.venv/bin/python}
DBT=${DBT:-$(dirname "$PY")/dbt}

WORK=$(mktemp -d)
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$WORK"' EXIT
export TAXI_DATA_DIR=$WORK TAXI_RAW_DIR=$WORK/raw TAXI_WAREHOUSE=$WORK/warehouse.duckdb
export TAXI_DUCKDB_TMP=$WORK/tmp TAXI_DUCKDB_MEMORY=1GB
export DBT_TARGET_PATH=$WORK/target DBT_LOG_PATH=$WORK/logs

PORT=${PORT:-8765}
"$PY" -m http.server "$PORT" --bind 127.0.0.1 --directory ci/sample >/dev/null 2>&1 &
SERVER_PID=$!
for _ in $(seq 50); do curl -sf -o /dev/null "http://127.0.0.1:$PORT/" && break; sleep 0.1; done
export TAXI_BASE_URL=http://127.0.0.1:$PORT

"$PY" -m pipeline.ingest --start 2024-12 --end 2024-12
"$PY" -m pipeline.ingest --start 2026-06 --end 2026-06
(cd dbt && "$DBT" build --profiles-dir .)
# Second run must be a no-op that still passes every test.
"$PY" -m pipeline.ingest --start 2024-12 --end 2024-12 | tail -1
(cd dbt && "$DBT" build --profiles-dir . | tail -1)
