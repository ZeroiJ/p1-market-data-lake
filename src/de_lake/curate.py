"""Curation: turn a validated frame into partitioned Parquet in the mart.

Two orderings in this module are load-bearing, and both are covered by tests
that fail if they are inverted:

1. **Mart before quarantine.** Every good partition is written before any
   quarantine partition is written. If the mart write fails, nothing claims the
   rows were handled — the run retries cleanly. The reverse order would leave
   quarantined rows recorded against a mart that does not contain their
   siblings, so a retry would double-count.

2. **Content-hash filenames.** A partition file is named for the hash of its own
   rows, so re-curating unchanged data rewrites the identical key and
   `atomic_write` reports `created=False`. Idempotency is a property of the
   naming, not of bookkeeping.

Partitions are `curated/{source}/date={YYYY-MM-DD}/`. See
`docs/adr/0003-partition-by-source-and-date.md` for why date, and for the two
named ways this layout breaks.
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from de_lake.contracts import validate
from de_lake.storage.base import ObjectStore

MART_PREFIX = "curated"
QUARANTINE_PREFIX = "quarantine"

#: Which column each source partitions on.
PARTITION_COLUMNS: dict[str, str] = {
    "sec_edgar": "filing_date",
    "sec_xbrl": "filed",
    "binance": "open_time",
    "stooq": "date",
}


class UnknownPartitionSource(KeyError):
    """Raised when `curate` is called for a source with no partition column."""


@dataclass(frozen=True, slots=True)
class CurateResult:
    ok_keys: list[str]
    quarantine_keys: list[str]
    rows_ok: int
    rows_quarantined: int


def _partition_value(value: str | date | datetime) -> str:
    """Normalise a partition value to a YYYY-MM-DD string.

    Partition columns arrive as an ISO string (SEC, Stooq) or as a datetime
    (Binance), so both are accepted rather than forcing every source to format
    its own dates.
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value)[:10]


def _row_key(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()[:16]


def _write_frame(
    store: ObjectStore, prefix: str, frame: pl.DataFrame, partition: str
) -> list[str]:
    """Serialise one partition to Parquet and land it under a hashed filename."""
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    payload = buffer.getvalue()

    key = f"{prefix}/{partition}/part-{_row_key(payload)}.parquet"
    store.atomic_write(key, payload)
    return [key]


def curate(store: ObjectStore, source: str, frame: pl.DataFrame) -> CurateResult:
    """Validate, partition, and write `frame` into the mart.

    Good rows reach `curated/{source}/date=.../`; rows failing their contract
    reach `quarantine/{source}/date=.../` with a `reason` column naming the
    offending column. Nothing is ever silently dropped.
    """
    if source not in PARTITION_COLUMNS:
        raise UnknownPartitionSource(f"no partition column registered for {source!r}")
    if frame.height == 0:
        return CurateResult(
            ok_keys=[], quarantine_keys=[], rows_ok=0, rows_quarantined=0
        )

    partition_column = PARTITION_COLUMNS[source]
    result = validate(frame, source)

    ok_keys: list[str] = []
    for (value,), part in result.ok.group_by(partition_column, maintain_order=True):
        if value is None:
            continue  # contract already quarantined these; nothing can partition them
        ok_keys += _write_frame(
            store,
            f"{MART_PREFIX}/{source}",
            part,
            f"date={_partition_value(value)}",
        )

    # Mart is complete before quarantine is touched — see module docstring.
    quarantine_keys: list[str] = []
    if result.quarantine.height:
        annotated = result.quarantine.with_columns(
            pl.col("_row_key")
            .replace_strict(result.reasons, default="unspecified")
            .alias("reason")
        )
        for (value,), part in annotated.group_by(partition_column, maintain_order=True):
            partition = (
                "date=unknown" if value is None else f"date={_partition_value(value)}"
            )
            quarantine_keys += _write_frame(
                store, f"{QUARANTINE_PREFIX}/{source}", part, partition
            )

    return CurateResult(
        ok_keys=ok_keys,
        quarantine_keys=quarantine_keys,
        rows_ok=result.ok.height,
        rows_quarantined=result.quarantine.height,
    )
