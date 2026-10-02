"""Shared ingestion logic, deliberately separate from the Airflow DAG.

The DAG calls `ingest_range()`; it does not implement it. Two reasons:

1. The interesting behaviour — checkpoint resume, landing, curation — is then
   testable without standing up an Airflow scheduler.
2. Airflow operators stay thin, which is what makes them readable in the UI and
   safe to retry: a retry re-enters this function, which consults the
   checkpoint rather than redoing completed work.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date

from de_lake.checkpoint import Checkpoint
from de_lake.curate import PARTITION_COLUMNS, curate
from de_lake.landing import land
from de_lake.settings import SEC_USER_AGENT
from de_lake.sources.base import Source
from de_lake.storage.base import ObjectStore
from de_lake.storage.filesystem import FilesystemStore

log = logging.getLogger(__name__)

#: Sources keyed by the unit of work they resume on. `sec_xbrl` resumes per CIK,
#: so its unit name is `xbrl` rather than a date.
SOURCES: dict[str, tuple[str, str]] = {
    "sec_edgar": ("filings", "date"),
    "sec_xbrl": ("xbrl", "cik"),
    "binance": ("klines", "date"),
    "stooq": ("daily", "date"),
}


def pending_units(
    checkpoint: Checkpoint, source: Source, start: date, end: date
) -> list[str]:
    """Units of work still to do — the resume path, in one readable place."""
    return [unit for unit in source.units(start, end) if not checkpoint.is_done(unit)]


def ingest_range(
    source: Source,
    store: ObjectStore,
    start: date,
    end: date,
    unit_name: str | None = None,
    user_agent: str = SEC_USER_AGENT,
) -> dict[str, int]:
    """Ingest every pending unit of `source` between `start` and `end`.

    Returns counts of payloads landed, rows accepted and rows quarantined, so a
    caller — or the CLI — can report something other than "it ran".
    """
    unit_name = unit_name or SOURCES.get(source.name, ("units", "date"))[0]

    # Clear temp files abandoned by a previous hard kill before adding more.
    purge = getattr(store, "purge_temp_files", None)
    if callable(purge):
        orphans = purge()
        if orphans:
            log.info("removed %d orphaned temp file(s) from a previous run", orphans)

    checkpoint = Checkpoint.load(store, source.name, unit_name)

    stats = {"units": 0, "landed": 0, "rows_ok": 0, "rows_quarantined": 0, "resumed": 0}

    # Measured before the loop. Computed after, every unit would look resumed
    # because by then they all *are* done — which makes the number meaningless.
    todo = pending_units(checkpoint, source, start, end)
    stats["resumed"] = len(source.units(start, end)) - len(todo)

    for unit in todo:
        payloads = list(source.fetch(unit))
        for logical_name, payload in payloads:
            result = land(store, source.name, logical_name, payload)
            if result.created:
                stats["landed"] += 1
            curated = curate(store, source.name, _to_frame(source, payload))
            stats["rows_ok"] += curated.rows_ok
            stats["rows_quarantined"] += curated.rows_quarantined

        # Marked only after the unit's payloads have all landed and curated, so
        # a crash mid-unit replays that unit rather than skipping it.
        checkpoint.mark_done(unit)
        checkpoint.save(store)
        stats["units"] += 1
        log.info("%s: completed unit %s", source.name, unit)

    return stats


def _to_frame(source: Source, payload: bytes):
    """Turn a raw landed payload into the frame `curate` expects.

    JSON sources emit a list of already-contract-shaped records. CSV sources
    emit a flat file that still needs a header, a ticker and a row key, which is
    why this is a branch rather than one expression.
    """
    import io
    import json

    import polars as pl

    if source.name == "stooq":
        # Stooq ships capitalised headers (Date,Open,...); the contract is
        # lowercase. Normalise here rather than loosening the contract, and build
        # a genuinely unique row key from (ticker, date) — using the ticker alone
        # would give every row in the file the same key.
        frame = pl.read_csv(io.BytesIO(payload))
        frame = frame.rename({name: name.lower() for name in frame.columns})
        # Only `date` comes from the file; `ticker` is added just below, so
        # checking for it here would reject every real file.
        if frame.height == 0 or "date" not in frame.columns:
            return pl.DataFrame()
        frame = frame.with_columns(
            pl.lit(source._ticker).alias("ticker"),  # noqa: SLF001 — single-purpose adapter
            (pl.lit(source._ticker) + ":" + pl.col("date")).alias("_row_key"),  # noqa: SLF001
        )
        return frame

    records = json.loads(payload)
    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)


#: Defaults used by both the DAG and any ad-hoc caller, so a source is never
#: silently constructed with nothing to fetch.
DEFAULT_BINANCE_TICKERS = ("BTCUSDT",)
DEFAULT_STOOQ_TICKERS = ("aapl.us",)
DEFAULT_CIKS = ("0000320193",)


def sources_for(
    session_factory,
    binance_tickers: Iterable[str] = DEFAULT_BINANCE_TICKERS,
    stooq_tickers: Iterable[str] = DEFAULT_STOOQ_TICKERS,
    ciks: Iterable[str] = DEFAULT_CIKS,
):
    """Build every configured source against one HTTP session.

    Binance and Stooq take separate ticker lists on purpose. They share a
    notion of "symbol" but not a namespace — `aapl.us` is a Stooq symbol and
    would be a nonsense Binance one — and a single combined list produced a
    Binance source per Stooq ticker.
    """
    from de_lake.sources.binance import BinanceSource
    from de_lake.sources.sec_edgar import SecEdgarSource, SecXbrlSource
    from de_lake.sources.stooq import StooqSource

    session = session_factory()
    return [
        SecEdgarSource(session=session, user_agent=SEC_USER_AGENT),
        SecXbrlSource(session=session, user_agent=SEC_USER_AGENT, ciks=list(ciks)),
        *[BinanceSource(session=session, ticker=t) for t in binance_tickers],
        *[StooqSource(session=session, ticker=t) for t in stooq_tickers],
    ]


def default_store() -> ObjectStore:
    from de_lake.storage import get_store

    return get_store()


__all__ = [
    "DEFAULT_BINANCE_TICKERS",
    "DEFAULT_CIKS",
    "DEFAULT_STOOQ_TICKERS",
    "PARTITION_COLUMNS",
    "FilesystemStore",
    "SOURCES",
    "default_store",
    "ingest_range",
    "pending_units",
    "sources_for",
]