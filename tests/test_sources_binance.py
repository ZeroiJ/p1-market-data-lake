"""Tests for the Binance OHLCV source.

One page per yielded tuple, so landing is incremental: a crash mid-day loses at
most the page in flight, never the whole unit.

The tests that matter most are the boundary ones. Pagination bugs do not fail
loudly — they silently duplicate a candle or skip one, and a market dataset with
a duplicated minute is quietly wrong forever. So `test_no_page_overlap_or_gap`
is the centre of this file.
"""

from __future__ import annotations

import json
from datetime import UTC, date

import pytest

from de_lake.sources.base import RetryableSourceError
from de_lake.sources.binance import BinanceSource

BASE = "https://api.binance.com/api/v3/klines"
MINUTE = 60_000
DAY_MINUTES = 1440
LIMIT = 1000


class RecordingSession:
    def __init__(self, days_with_data: int = 1, page_size: int = LIMIT) -> None:
        self.days_with_data = days_with_data
        self.page_size = page_size
        self.urls: list[str] = []
        self.params: list[dict] = []

    def get(self, url: str, headers=None, params=None, timeout: int = 30):
        self.urls.append(url)
        self.params.append(dict(params or {}))
        return _Response(200, json.dumps(_synthetic_day(params or {}, self.page_size)))


def _ms(iso: str) -> int:
    return (
        int(
            __import__("datetime")
            .datetime.fromisoformat(iso)
            .replace(tzinfo=UTC)
            .timestamp()
        )
        * 1000
    )


def _synthetic_day(params: dict, page_size: int) -> list[list]:
    start = int(params.get("startTime", _ms("2026-01-05T00:00")))
    limit = min(int(params.get("limit", page_size)), page_size)
    rows = []
    for i in range(limit):
        open_ms = start + i * MINUTE
        rows.append(
            [
                open_ms,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "12.5",
                open_ms + MINUTE - 1,
                "1000",
                10,
                "50.0",
                "5025.0",
                "0",
            ]
        )
    return rows


class _Response:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


class StatusSession:
    def __init__(self, status: int, body: str = "[]") -> None:
        self._status, self._body = status, body

    def get(self, url, headers=None, params=None, timeout: int = 30):
        return _Response(self._status, self._body)


class EmptySession:
    def get(self, url, headers=None, params=None, timeout: int = 30):
        return _Response(200, "[]")


# --- units ----------------------------------------------------------------


def test_units_enumerate_each_day_in_range():
    src = BinanceSource(session=RecordingSession())
    assert src.units(date(2026, 1, 5), date(2026, 1, 7)) == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
    ]


# --- pagination -----------------------------------------------------------


def test_pagination_covers_a_full_day_at_one_minute_resolution():
    src = BinanceSource(session=RecordingSession())
    pages = list(src.fetch("2026-01-05"))
    assert len(pages) == DAY_MINUTES // LIMIT + 1


def test_no_page_overlap_or_gap_across_the_boundary():
    # The whole point of per-page landing. If pages repeated or skipped a
    # minute, the mart would be silently wrong with no error anywhere.
    src = BinanceSource(session=RecordingSession())
    pages = [json.loads(p) for _, p in src.fetch("2026-01-05")]

    def opens(page):
        return [_ms(r["open_time"]) for r in page]

    assert opens(pages[0])[-1] + MINUTE == opens(pages[1])[0]

    seen = [ms for page in pages for ms in opens(page)]
    assert len(seen) == len(set(seen)), "a candle was landed twice"
    assert seen == sorted(seen), "candles are out of order"
    assert len(seen) == DAY_MINUTES, "expected exactly one candle per minute"


def test_pagination_stops_at_the_end_of_the_requested_day():
    src = BinanceSource(session=RecordingSession())
    pages = [json.loads(p) for _, p in src.fetch("2026-01-05")]
    last_open_ms = _ms(pages[-1][-1]["open_time"])
    assert last_open_ms < _ms("2026-01-06T00:00")


def test_each_page_carries_its_own_logical_name():
    src = BinanceSource(session=RecordingSession())
    names = [name for name, _ in src.fetch("2026-01-05")]
    assert all(n.startswith("BTCUSDT:") for n in names)
    assert len(set(names)) == len(names)


# --- row shape ------------------------------------------------------------


def test_page_rows_are_transposed_into_kline_column_order():
    src = BinanceSource(session=RecordingSession())
    _, payload = next(iter(src.fetch("2026-01-05")))
    row = json.loads(payload)[0]
    assert row["_row_key"].startswith("BTCUSDT:")
    assert row["ticker"] == "BTCUSDT"
    assert set(row) == {
        "_row_key",
        "ticker",
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }


def test_timestamps_are_iso8601_with_timezone():
    src = BinanceSource(session=RecordingSession())
    _, payload = next(iter(src.fetch("2026-01-05")))
    row = json.loads(payload)[0]
    assert row["open_time"].startswith("2026-01-05T00:00")
    assert row["open_time"].endswith("+00:00")


def test_row_keys_are_unique_across_the_whole_day():
    src = BinanceSource(session=RecordingSession())
    rows = [r for _, p in src.fetch("2026-01-05") for r in json.loads(p)]
    assert len({r["_row_key"] for r in rows}) == len(rows)


# --- error semantics ------------------------------------------------------


def test_empty_day_returns_no_pages():
    assert list(BinanceSource(session=EmptySession()).fetch("2026-01-05")) == []


def test_a_429_raises_retryable():
    with pytest.raises(RetryableSourceError):
        list(BinanceSource(session=StatusSession(429)).fetch("2026-01-05"))


def test_a_503_raises_retryable():
    with pytest.raises(RetryableSourceError):
        list(BinanceSource(session=StatusSession(503)).fetch("2026-01-05"))


def test_a_404_yields_nothing():
    assert list(BinanceSource(session=StatusSession(404)).fetch("2026-01-05")) == []


def test_a_ticker_binance_rejects_yields_nothing():
    # Binance answers an unknown symbol with 400. That is "no data for this
    # ticker", not a pipeline fault.
    assert (
        list(
            BinanceSource(session=StatusSession(400), ticker="NOPE").fetch("2026-01-05")
        )
        == []
    )


# --- configuration --------------------------------------------------------


def test_the_ticker_is_part_of_the_request():
    session = RecordingSession()
    list(BinanceSource(session=session, ticker="ETHUSDT").fetch("2026-01-05"))
    assert session.params
    assert all(p["symbol"] == "ETHUSDT" for p in session.params)


def test_every_page_requests_the_same_symbol_interval_and_limit(session=None):
    session = session or RecordingSession()
    list(BinanceSource(session=session, ticker="BTCUSDT").fetch("2026-01-05"))
    assert all(p["interval"] == "1m" and p["limit"] == LIMIT for p in session.params)
