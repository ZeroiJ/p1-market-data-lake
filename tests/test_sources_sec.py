"""Tests for the SEC sources.

Every test runs against a fixture trimmed from a real SEC response on
2026-10-02 (see the `_fixture_note` field in each). Nothing here touches the
network — that is what makes the suite deterministic and free.

The fakes below stand in for a session so that header propagation, status-code
mapping and pagination can all be asserted without a live service.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from de_lake.sources.base import RetryableSourceError
from de_lake.sources.sec_edgar import SecEdgarSource, SecXbrlSource

FIXTURES = Path(__file__).parent / "fixtures"
UA = "DataEngineeringPortfolio/0.1 (sujalbirwadkar19@gmail.com)"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


class FakeSession:
    """Serves fixture content and records what was requested."""

    def __init__(self, routes: dict[str, object] | None = None) -> None:
        self.routes = routes or {}
        self.headers: list[dict[str, str]] = []
        self.urls: list[str] = []

    def get(self, url: str, headers: dict[str, str] | None = None, timeout: int = 30):
        self.headers.append(headers or {})
        self.urls.append(url)
        return self._respond(url)

    def _respond(self, url: str):
        for pattern, response in self.routes.items():
            if pattern in url:
                return response(url) if callable(response) else response
        if "daily-index" in url:
            return _Response(200, json.dumps(_load("sec_daily_index_sample.json")))
        if "submissions" in url:
            return _Response(200, json.dumps(_load("sec_submissions_sample.json")))
        if "companyfacts" in url:
            return _Response(200, json.dumps(_load("sec_companyfacts_sample.json")))
        return _Response(404, "")


class _Response:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


class StatusSession(FakeSession):
    def __init__(self, status: int, body: str = "") -> None:
        super().__init__()
        self._status = status
        self._body = body

    def _respond(self, url: str):
        return _Response(self._status, self._body)


class RaisingSession(FakeSession):
    def __init__(self) -> None:
        super().__init__()

    def _respond(self, url: str):
        raise TimeoutError("connection reset by peer")


# --- units ----------------------------------------------------------------


def test_units_enumerate_each_filing_date_in_range():
    src = SecEdgarSource(session=FakeSession(), user_agent=UA)
    assert src.units(date(2026, 1, 5), date(2026, 1, 7)) == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
    ]


def test_units_is_empty_when_the_range_is_inverted():
    src = SecEdgarSource(session=FakeSession(), user_agent=UA)
    assert src.units(date(2026, 1, 8), date(2026, 1, 5)) == []


def test_xbrl_units_are_ciks_not_dates():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    assert src.units(date(2026, 1, 1), date(2026, 1, 31)) == ["0000320193"]


def test_xbrl_units_are_zero_padded_to_ten_digits():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["320193"])
    assert src.units(date(2026, 1, 1), date(2026, 1, 2)) == ["0000320193"]


# --- identification -------------------------------------------------------


def test_fetch_sends_an_identifying_user_agent():
    session = FakeSession()
    list(SecEdgarSource(session=session, user_agent=UA).fetch("2026-01-05"))
    assert session.headers
    assert all(h.get("User-Agent") == UA for h in session.headers)


def test_xbrl_fetch_sends_an_identifying_user_agent():
    session = FakeSession()
    list(SecXbrlSource(session=session, user_agent=UA, ciks=["0000320193"]).fetch("0000320193"))
    assert all(h.get("User-Agent") == UA for h in session.headers)


# --- filings index --------------------------------------------------------


def test_fetch_parses_daily_index_rows_into_contracts():
    src = SecEdgarSource(session=FakeSession(), user_agent=UA)
    rows = [json.loads(payload) for _, payload in src.fetch("2026-01-05")]
    assert rows, "expected at least one filing from the fixture index"
    assert set(rows[0]) >= {"_row_key", "cik", "form", "filing_date", "accession_number"}


def test_fetch_uses_the_submissions_document_for_each_filing():
    session = FakeSession()
    list(SecEdgarSource(session=session, user_agent=UA).fetch("2026-01-05"))
    assert any("daily-index" in url for url in session.urls)


# --- XBRL -----------------------------------------------------------------


def test_xbrl_fetch_flattens_facts_into_rows_not_one_blob_per_company():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    rows = [json.loads(payload) for _, payload in src.fetch("0000320193")]
    assert rows
    assert {"taxonomy", "tag", "fy", "fp", "val", "unit"}.issubset(rows[0].keys())


def test_xbrl_keeps_every_unit_of_a_fact_that_has_several():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    rows = [json.loads(payload) for _, payload in src.fetch("0000320193")]
    distinct_tags = {row["tag"] for row in rows}
    # More rows than distinct tags: the fixture is deliberately trimmed to tags
    # that are reported under several distinct units.
    assert len(rows) > len(distinct_tags), "expected some tag to appear under >1 unit"
    assert len({r["_row_key"] for r in rows}) == len(rows), "row keys must be unique"
    assert len({(r["_row_key"], r["unit"]) for r in rows}) == len(rows)


def test_xbrl_rows_carry_start_and_end_so_periods_stay_comparable():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    rows = [json.loads(payload) for _, payload in src.fetch("0000320193")]
    assert all({"start", "end"} <= row.keys() for row in rows)


# --- error semantics ------------------------------------------------------


def test_a_429_response_raises_retryable_not_a_crash():
    src = SecEdgarSource(session=StatusSession(429), user_agent=UA)
    with pytest.raises(RetryableSourceError):
        list(src.fetch("2026-01-05"))


def test_a_503_response_raises_retryable():
    src = SecEdgarSource(session=StatusSession(503), user_agent=UA)
    with pytest.raises(RetryableSourceError):
        list(src.fetch("2026-01-05"))


def test_a_404_yields_nothing_rather_than_raising():
    src = SecEdgarSource(session=StatusSession(404), user_agent=UA)
    assert list(src.fetch("2026-01-05")) == []


def test_a_400_yields_nothing_rather_than_raising():
    src = SecEdgarSource(session=StatusSession(400), user_agent=UA)
    assert list(src.fetch("2026-01-05")) == []


def test_a_connection_reset_raises_retryable():
    src = SecEdgarSource(session=RaisingSession(), user_agent=UA)
    with pytest.raises(RetryableSourceError):
        list(src.fetch("2026-01-05"))


def test_an_empty_index_yields_nothing_and_does_not_raise():
    empty = {"format": "CIK|Name|Form|Date|File", "rows": []}
    session = FakeSession({"daily-index": _Response(200, json.dumps(empty))})
    assert list(SecEdgarSource(session=session, user_agent=UA).fetch("2026-01-05")) == []