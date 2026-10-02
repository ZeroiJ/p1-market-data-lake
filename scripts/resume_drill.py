"""Resume drill: SIGKILL mid-backfill against a live API, then resume.

Not part of the normal suite — it hits the network and is meant to be run once,
by hand, with its numbers pasted into the README. A resume claim nobody
reproduced is not evidence.

Run:  uv run python scripts/resume_drill.py [days]

Drills against Binance, because Stooq is unreachable from this network behind a
JavaScript proof-of-work wall (see README "What breaks"). Binance is public,
keyless, and returns enough pages per day that a kill lands mid-unit rather than
between units — which is the case worth proving.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

WORKER = """
import sys
from datetime import date, timedelta
import requests
from de_lake.pipeline import ingest_range, sources_for
from de_lake.storage.filesystem import FilesystemStore

root, days = sys.argv[1], int(sys.argv[2])
store = FilesystemStore(root)
src = next(s for s in sources_for(requests.Session) if s.name == "binance")
start = date(2026, 1, 5)
ingest_range(src, store, start, start + timedelta(days=days - 1), unit_name="klines")
print("DONE", flush=True)
"""

METRICS = """
import json, sys
from pathlib import Path
import polars as pl

root = Path(sys.argv[1])
mart = [p for p in root.rglob("*.parquet") if "curated" in p.parts]
raw = [p for p in root.rglob("*.bin")]
files = [p for p in root.rglob("*") if p.is_file()]
rows = 0
keys = set()
for path in mart:
    frame = pl.read_parquet(path)
    rows += frame.height
    keys |= set(frame["_row_key"].to_list())
print(json.dumps({
    "mart_files": len(mart),
    "mart_rows": rows,
    "distinct_row_keys": len(keys),
    "raw_payloads": len(raw),
    "total_files": len(files),
    "bytes": sum(p.stat().st_size for p in files),
}))
"""


def _metrics(root: Path) -> dict:
    out = subprocess.run(
        [sys.executable, "-c", METRICS, str(root)],
        capture_output=True, text=True, check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def _run(root: Path, days: int) -> None:
    subprocess.run(
        [sys.executable, "-c", WORKER, str(root), str(days)],
        check=True, capture_output=True, timeout=900,
    )


def main() -> int:
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    kill_after = float(os.environ.get("DRILL_KILL_AFTER", "6"))

    clean = Path(tempfile.mkdtemp(prefix="drill-clean-"))
    print(f"1. baseline: uninterrupted run over {days} days ...")
    _run(clean, days)
    baseline = _metrics(clean)
    print(f"   {baseline}")

    dirty = Path(tempfile.mkdtemp(prefix="drill-killed-"))
    print(f"\n2. run again, SIGKILL after {kill_after}s (no cleanup, no signal handler) ...")
    child = subprocess.Popen(
        [sys.executable, "-c", WORKER, str(dirty), str(days)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    time.sleep(kill_after)
    if child.poll() is not None:
        print("   !! process already finished before the kill — raise DRILL_KILL_AFTER")
        return 2
    os.killpg(os.getpgid(child.pid), signal.SIGKILL)
    child.wait(timeout=30)
    partial = _metrics(dirty)
    print(f"   {partial}")
    if partial["mart_rows"] == 0:
        print("   !! nothing landed before the kill — raise DRILL_KILL_AFTER")

    print("\n3. resume: same command, same range ...")
    _run(dirty, days)
    resumed = _metrics(dirty)
    print(f"   {resumed}")

    print("\n--- verdict ---")
    checks = {
        "row counts match": resumed["mart_rows"] == baseline["mart_rows"],
        "no duplicate row keys": resumed["distinct_row_keys"] == resumed["mart_rows"],
        "no duplicate raw payloads": resumed["raw_payloads"] == baseline["raw_payloads"],
        "total files match": resumed["total_files"] == baseline["total_files"],
        "bytes match exactly": resumed["bytes"] == baseline["bytes"],
    }
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")

    everything = all(checks.values())
    print(f"\nRESUME MATCHES UNINTERRUPTED RUN: {everything}")
    if everything:
        print(
            f"\nPaste into README: {days} days, {baseline['mart_rows']} rows, "
            f"{baseline['raw_payloads']} raw payloads, {baseline['bytes']} bytes, "
            f"identical after SIGKILL + resume"
        )
    return 0 if everything else 1


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)
    raise SystemExit(main())