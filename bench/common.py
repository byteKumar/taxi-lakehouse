"""Shared helpers for the benchmarks: wait for a quiet machine, run dbt, and
record results as JSON in bench/results/."""

import json
import os
import platform
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pipeline import config

RESULTS = config.PROJECT_ROOT / "bench" / "results"
DBT_DIR = config.PROJECT_ROOT / "dbt"
DBT = str(Path(sys.executable).parent / "dbt")
# Benchmarks / training runs of the other projects sharing this Mac.
OTHER_BENCH = r"k6 run|run-load|run-concurrency|scripts/train_|scripts/evaluate|node bench/|playwright|lighthouse|lsm_bench|db_bench"


def load1() -> float:
    return os.getloadavg()[0]


def wait_for_quiet(max_load: float = 4.0, timeout_s: int = 3600) -> dict:
    """Block until no other benchmark runs and the 1-min load is below max_load."""
    t0 = time.time()
    while True:
        others = subprocess.run(["pgrep", "-fl", OTHER_BENCH], capture_output=True, text=True).stdout.strip()
        others = "\n".join(l for l in others.splitlines() if "pgrep" not in l and "zsh -c" not in l)
        if not others and load1() < max_load:
            return {"load1_at_start": round(load1(), 2), "waited_s": round(time.time() - t0, 1)}
        if time.time() - t0 > timeout_s:
            raise SystemExit(f"machine never got quiet (load {load1():.1f}); other: {others or '-'}")
        print(f"  waiting: load1={load1():.1f} others={others[:80] or '-'}", flush=True)
        time.sleep(20)


def machine() -> dict:
    hw = subprocess.run(["system_profiler", "SPHardwareDataType"], capture_output=True, text=True).stdout
    chip = re.search(r"Chip: (.+)", hw)
    cores = re.search(r"Total Number of Cores: (.+)", hw)
    mem = re.search(r"Memory: (.+)", hw)
    st = os.statvfs(config.DATA_DIR)
    return {
        "chip": chip.group(1).strip() if chip else platform.processor(),
        "cores": cores.group(1).strip() if cores else os.cpu_count(),
        "memory": mem.group(1).strip() if mem else "?",
        "disk": "internal SSD (APFS)",
        "disk_free_gb": round(st.f_bavail * st.f_frsize / 1e9, 1),
        "os": platform.platform(),
        "python": platform.python_version(),
    }


def dbt(*args: str) -> tuple[float, dict]:
    """Run dbt from dbt/, return (wall seconds, per-node timings)."""
    env = {**os.environ, "TAXI_WAREHOUSE": str(config.WAREHOUSE_PATH), "TAXI_RAW_DIR": str(config.RAW_DIR),
           "TAXI_DUCKDB_TMP": str(config.DATA_DIR / "tmp" / "duckdb")}
    t0 = time.perf_counter()
    proc = subprocess.run([DBT, *args, "--profiles-dir", "."], cwd=DBT_DIR, env=env, capture_output=True, text=True)
    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        print(proc.stdout[-3000:])
        raise SystemExit(f"dbt {' '.join(args)} failed")
    rr = json.loads((DBT_DIR / "target" / "run_results.json").read_text())
    nodes = {r["unique_id"].split(".")[-1]: round(r["execution_time"], 2) for r in rr["results"]
             if r["unique_id"].startswith("model.")}
    tests_s = sum(r["execution_time"] for r in rr["results"] if r["unique_id"].startswith("test."))
    summary = re.search(r"Done\. (.+)", proc.stdout)
    return wall, {"models": nodes, "models_s_sum": round(sum(nodes.values()), 2),
                  "tests_s_sum": round(tests_s, 2), "summary": summary.group(1).strip() if summary else ""}


def save(name: str, payload: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.json"
    payload = {"recorded_at": datetime.now().isoformat(timespec="seconds"), "machine": machine(), **payload}
    path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {path.relative_to(config.PROJECT_ROOT)}")
    return path
