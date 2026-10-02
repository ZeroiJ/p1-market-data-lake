"""Tests for the Stooq source — the deliberate non-JSON control case.

Every other source in this project speaks JSON. Stooq speaks flat CSV with a
header line and no envelope, which is what most real-world "just download the
file" upstreams actually look like. Keeping it in the pipeline means the design
is tested against a second wire format rather than against one assumption
repeated four times.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from de_lake.sources.base import RetryableSourceError
from de_lake.sources.stooq import StooqSource

CSV = b"Date,Open,High,Low,Close,Volume\n2026-01-05,1.0,2.0,0.5,1.5,100\n2026-01-06,1.5,2.5,1.0,2.0,120\n"


class _Response:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content
        self.text = content.decode("utf-8", "replace")

    def json(self):
        return json.loads(self.text)


class CsvSession:
    def __init__(self, body: bytes = CSV) -> None:
        self._body = body
        self.urls: list[str] = []

    def get(self, url: str, headers=None, timeout: int = 30):
        self.urls.append(url)
        return _Response(200, self._body)


class NoDataSession:
    """Stooq answers an exhausted ticker with the literal body 'No data'."""

    def get(self, url, headers=None, timeout: int = 30):
        return _Response(200, b"No data")


class StatusSession:
    def __init__(self, status: int, body: bytes = b"") -> None:
        self._status, self._body = status, body

    def get(self, url, headers=None, timeout: int = 30):
        return _Response(self._status, self._body)


class RaisingSession:
    def get(self, url, headers=None, timeout: int = 30):
        raise ConnectionError("reset by peer")


# --- units ----------------------------------------------------------------


def test_units_enumerate_each_day_in_range():
    src = StooqSource(session=CsvSession(), ticker="aapl.us")
    assert src.units(date(2026, 1, 5), date(2026, 1, 7)) == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
    ]


# --- verbatim pass-through ------------------------------------------------


def test_fetch_returns_the_csv_verbatim(tmp_path):
    # Stooq serves a whole history per ticker, not per date, so this source does
    # not parse. Curation does. That keeps the wire format honest: the lake stores
    # exactly what the upstream sent.
    src = StooqSource(session=CsvSession(), ticker="aapl.us")
    _, payload = next(iter(src.fetch("2026-01-05")))
    assert payload == CSV


def test_fetch_returns_the_csv_verbatim_with_no_leading_or_trailing_mangling():
    src = StooqSource(session=CsvSession(), ticker="aapl.us")
    _, payload = next(iter(src.fetch("2026-01-05")))
    assert payload.startswith(b"Date,Open,High,Low,Close,Volume")
    assert payload.endswith(b"\n")


def test_logical_name_names_the_ticker_and_unit():
    src = StooqSource(session=CsvSession(), ticker="aapl.us")
    name, _ = next(iter(src.fetch("2026-01-05")))
    assert name == "aapl.us:2026-01-05"


def test_the_ticker_reaches_the_request_url():
    session = CsvSession()
    list(StooqSource(session=session, ticker="msft.us").fetch("2026-01-05"))
    assert any("msft.us" in url for url in session.urls)


# --- empty and failure semantics ------------------------------------------


def test_a_stooq_exhausted_ticker_yields_no_rows_not_an_error():
    assert list(StooqSource(session=NoDataSession()).fetch("2026-01-05")) == []


def test_an_empty_body_yields_nothing():
    assert list(StooqSource(session=CsvSession(body=b"")).fetch("2026-01-05")) == []


def test_a_429_raises_retryable():
    with pytest.raises(RetryableSourceError):
        list(StooqSource(session=StatusSession(429)).fetch("2026-01-05"))


def test_a_503_raises_retryable():
    with pytest.raises(RetryableSourceError):
        list(StooqSource(session=StatusSession(503)).fetch("2026-01-05"))


def test_a_404_yields_nothing():
    assert list(StooqSource(session=StatusSession(404)).fetch("2026-01-05")) == []


def test_a_connection_reset_raises_retryable():
    with pytest.raises(RetryableSourceError):
        list(StooqSource(session=RaisingSession()).fetch("2026-01-05"))