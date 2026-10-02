"""End-to-end coverage of the pipeline glue.

The per-module tests prove landing, curation, contracts and checkpoints each
behave. They do not prove the *seams* between them, and both real bugs found
during Task 9 lived in a seam:

- `StooqSource` read `response.content` while the JSON sources called
  `.json()`, so against a session offering only `.text` it silently yielded
  nothing — indistinguishable from a day with no data.
- Nothing normalised Stooq's capitalised `Date,Open,...` header to the
  contract's lowercase, so a CSV payload landed but never curated.

So these tests exist to cover the joins, not the parts.
"""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from de_lake.pipeline import SOURCES, ingest_range, pending_units, sources_for
from de_lake.storage.filesystem import FilesystemStore

STOOQ_CSV = b"Date,Open,High,Low,Close,Volume\n2026-01-05,1.0,2.0,0.5,1.5,100\n2026-01-06,1.5,2.5,1.0,2.0,120\n"


class Session:
    """Minimal session offering `text` only — the shape that broke Stooq."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get(self, url, headers=None, params=None, timeout=30):
        self.calls.append(url)
        status = 200
        text = ""

        if "stooq" in url:
            # Date-aware, so each unit is distinct content rather than the same
            # bytes landed under two different checkpoints.
            day = url.split("d1=")[1].split("&")[0]
            base = float(day[-2:])
            text = (
                "Date,Open,High,Low,Close,Volume\n"
                f"{day},{base},{base + 1},{base - 0.5},{base + 0.5},100\n"
            )
        elif "klines" in url:
            start = int(params["startTime"])
            rows = [
                [
                    start + i * 60_000,
                    "100",
                    "101",
                    "99",
                    "100.5",
                    "12.5",
                    start + i * 60_000 + 59_999,
                    "1",
                    "1",
                    "1",
                    "1",
                    "0",
                ]
                for i in range(params["limit"])
            ]
            text = json.dumps(rows)

        class Response:
            status_code = status

            def __init__(self, body: str) -> None:
                self.text = body

            def json(self):
                return json.loads(self.text)

        return Response(text)


@pytest.fixture
def store(tmp_path) -> FilesystemStore:
    return FilesystemStore(tmp_path)


# --- resume ---------------------------------------------------------------


def test_a_second_run_over_the_same_range_does_no_work(store):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    first = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily"
    )
    assert first["units"] == 2 and first["rows_ok"] == 2
    assert len(list(store.list_keys("curated/stooq/"))) == 2

    second = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily"
    )
    assert second["units"] == 0
    assert second["landed"] == 0
    assert second["rows_ok"] == 0
    assert second["resumed"] == 2


def test_the_lake_is_byte_identical_after_a_repeat_run(store, tmp_path):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily")
    before = {k: store.read(k) for k in sorted(store.list_keys(""))}
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily")
    after = {k: store.read(k) for k in sorted(store.list_keys(""))}
    assert before == after


def test_resume_continues_from_a_partially_completed_range(store):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 5), unit_name="daily")
    stats = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 7), unit_name="daily"
    )
    assert stats["units"] == 2, "should only process the two new days"
    assert stats["resumed"] == 1


def test_pending_units_lists_only_what_is_left(store):
    from de_lake.checkpoint import Checkpoint

    source = next(s for s in sources_for(Session) if s.name == "stooq")
    cp = Checkpoint.load(store, "stooq", "daily")
    cp.mark_done("2026-01-05")
    cp.save(store)
    left = pending_units(
        Checkpoint.load(store, "stooq", "daily"),
        source,
        date(2026, 1, 5),
        date(2026, 1, 7),
    )
    assert left == ["2026-01-06", "2026-01-07"]


# --- stooq seam -----------------------------------------------------------


def test_stooq_lands_and_curates_a_csv_payload(store):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    stats = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily"
    )
    assert stats["landed"] >= 1
    assert stats["rows_ok"] == 2
    assert list(store.list_keys("curated/stooq/"))


def test_stooq_capitalised_headers_are_normalised_to_the_contract(store):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily")
    key = next(k for k in store.list_keys("curated/stooq/") if k.endswith(".parquet"))
    frame = pl.read_parquet(store.local_path(key))
    assert "date" in frame.columns and "Date" not in frame.columns
    assert frame.schema["date"] == pl.String


def test_stooq_row_keys_are_unique_per_date(store):
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 6), unit_name="daily")
    keys = [
        pl.read_parquet(store.local_path(k))["_row_key"].to_list()
        for k in store.list_keys("curated/stooq/")
        if k.endswith(".parquet")
    ]
    flat = [key for part in keys for key in part]
    assert len(flat) == len(set(flat))


def test_a_session_offering_only_text_still_works(store):
    # The exact shape that made Stooq silently produce nothing.
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    stats = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 5), unit_name="daily"
    )
    assert stats["rows_ok"] == 1
    assert list(store.list_keys("curated/stooq/"))


# --- binance seam ---------------------------------------------------------


def test_binance_lands_a_full_minute_day(store):
    source = next(s for s in sources_for(Session) if s.name == "binance")
    stats = ingest_range(
        source, store, date(2026, 1, 5), date(2026, 1, 5), unit_name="klines"
    )
    assert stats["rows_ok"] == 1440
    assert stats["rows_quarantined"] == 0


def test_binance_pages_land_under_separate_raw_objects(store):
    source = next(s for s in sources_for(Session) if s.name == "binance")
    ingest_range(source, store, date(2026, 1, 5), date(2026, 1, 5), unit_name="klines")
    payloads = [k for k in store.list_keys("raw/binance/") if k.endswith(".bin")]
    assert len(payloads) == 2, "one landing per 1000-candle page"


# --- registry -------------------------------------------------------------


def test_every_source_has_a_resume_unit_registered():
    for source in sources_for(Session):
        assert source.name in SOURCES, (
            f"{source.name} is missing from the pipeline registry"
        )


def test_sources_for_returns_the_four_configured_sources():
    names = [s.name for s in sources_for(Session)]
    assert set(names) == {"sec_edgar", "sec_xbrl", "binance", "stooq"}


def test_sources_for_builds_one_source_per_ticker():
    names = [
        s.name
        for s in sources_for(
            Session,
            binance_tickers=("BTCUSDT", "ETHUSDT"),
            stooq_tickers=("aapl.us", "msft.us"),
            ciks=("0000320193", "0000789019"),
        )
    ]
    assert names.count("binance") == 2
    assert names.count("stooq") == 2
    assert names.count("sec_xbrl") == 1
    assert names.count("sec_edgar") == 1


def test_a_stooq_ticker_never_becomes_a_binance_source():
    # Regression: one combined ticker list produced a Binance source per Stooq
    # ticker, so `aapl.us` was requested from Binance on every run.
    built = sources_for(
        Session, binance_tickers=("BTCUSDT",), stooq_tickers=("aapl.us",)
    )
    assert [s.row_label for s in built if s.name == "binance"] == ["BTCUSDT"]


def test_a_wide_range_enumerates_a_day_per_unit():
    source = next(s for s in sources_for(Session) if s.name == "stooq")
    units = source.units(date(2026, 1, 1), date(2026, 1, 31))
    assert len(units) == 31
    assert units[0] == "2026-01-01" and units[-1] == "2026-01-31"


def test_xbrl_resumes_per_company_not_per_date():
    from de_lake.checkpoint import Checkpoint

    xbrl = next(s for s in sources_for(Session) if s.name == "sec_xbrl")
    assert xbrl.units(date(2026, 1, 1), date(2026, 1, 31)) == ["0000320193"]
    cp = Checkpoint(source="sec_xbrl", unit="xbrl")
    cp.mark_done("0000320193")
    assert pending_units(cp, xbrl, date(2026, 1, 1), date(2026, 1, 31)) == []
