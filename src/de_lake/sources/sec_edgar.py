"""SEC EDGAR: the daily filing index, and XBRL company facts.

Two sources from one upstream, sharing a User-Agent and error semantics.

SEC requires every request to identify a contactable party and throttles
requests that do not, so the User-Agent is not optional. It is an email address,
not a secret.

Error mapping is the interesting part, and it is deliberately asymmetric:

- 429 and 5xx, and connection resets, raise `RetryableSourceError`. These are
  transient and a retry is the correct response — SEC rate-limits aggressively
  and a naive retry storm makes it worse.
- 400 and 404 yield nothing. For these endpoints a missing unit is a normal
  outcome (a date with no filings, a CIK with no XBRL facts), not a fault.
  Raising would turn "nothing to do" into "the DAG failed", which trains
  operators to ignore failures.

`SecXbrlSource` yields one row per `(taxonomy, tag, unit)` triple rather than one
blob per company. A fact reported under several units is several *distinct*
facts, not duplicates — "USD" and "USD/shares" are different quantities — and
flattening per unit means one malformed fact can be quarantined without
discarding its siblings. `start` and `end` are carried through because without
them a quarterly fact and an annual fact are indistinguishable downstream.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, timedelta

from de_lake.sources.base import RetryableSourceError

SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
COMPANYFACTS_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
DAILY_INDEX_BASE = "https://www.sec.gov/Archives/edgar/daily-index"

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_EMPTY_STATUS = {400, 404}


class _Empty(Exception):
    """Internal signal: the unit legitimately has no content."""


def _daterange(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


class _SecClient:
    """Shared HTTP behaviour: identification, retry classification, empty handling."""

    def __init__(self, session, user_agent: str) -> None:
        self._session = session
        self._user_agent = user_agent

    def get_json(self, url: str):
        try:
            response = self._session.get(url, headers={"User-Agent": self._user_agent}, timeout=30)
        except (TimeoutError, ConnectionError) as exc:
            raise RetryableSourceError(f"transient failure for {url}: {exc}") from exc

        status = getattr(response, "status_code", 200)
        if status in _RETRYABLE_STATUS:
            raise RetryableSourceError(f"HTTP {status} for {url}")
        if status in _EMPTY_STATUS:
            raise _Empty(url)

        text = getattr(response, "text", "") or ""
        if not text.strip():
            raise _Empty(url)
        return json.loads(text)


class SecEdgarSource:
    """Filings listed in SEC's daily index for a given calendar date."""

    name = "sec_edgar"

    def __init__(self, session, user_agent: str) -> None:
        self._client = _SecClient(session, user_agent)

    def units(self, start: date, end: date) -> list[str]:
        return [day.isoformat() for day in _daterange(start, end)]

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        day = date.fromisoformat(unit)
        quarter = (day.month - 1) // 3 + 1
        url = (
            f"{DAILY_INDEX_BASE}/{day.year}/QTR{quarter}/master.{day:%Y%m%d}.idx"
        )
        try:
            index = self._client.get_json(url)
        except _Empty:
            return

        for row in index.get("rows", []):
            fields = row.split("|")
            if len(fields) < 5:
                continue
            cik, _name, form, filed, _path = fields[:5]
            record = {
                "_row_key": f"{cik}:{fields[4]}",
                "cik": cik.zfill(10),
                "entity_name": fields[1].strip(),
                "form": form.strip(),
                "filing_date": filed.strip(),
                "accession_number": fields[4].rsplit("/", 1)[-1].removesuffix(".txt"),
            }
            yield record["_row_key"], json.dumps(record, sort_keys=True).encode()


class SecXbrlSource:
    """XBRL company facts, one row per (taxonomy, tag, unit).

    `units` returns CIKs rather than dates: companyfacts is a whole-history
    endpoint with no date range, so the natural unit of resumable work is one
    company.
    """

    name = "sec_xbrl"

    def __init__(self, session, user_agent: str, ciks: list[str]) -> None:
        self._client = _SecClient(session, user_agent)
        self._ciks = [str(cik).zfill(10) for cik in ciks]

    def units(self, start: date, end: date) -> list[str]:
        return list(self._ciks)

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        cik = unit.zfill(10)
        try:
            payload = self._client.get_json(f"{COMPANYFACTS_BASE}/CIK{cik}.json")
        except _Empty:
            return

        for taxonomy, tags in payload.get("facts", {}).items():
            for tag, node in tags.items():
                for unit_name, rows in node.get("units", {}).items():
                    for fact in rows:
                        # XBRL legitimately repeats the same (tag, unit, end, val):
                        # an amendment re-files it, and a quarterly and an annual
                        # fact can share an end date. The key must therefore carry
                        # every distinguishing field, or two distinct facts would
                        # collide and collapse into one downstream.
                        record = {
                            "_row_key": "|".join(
                                str(part)
                                for part in (
                                    cik,
                                    taxonomy,
                                    tag,
                                    unit_name,
                                    fact.get("start"),
                                    fact.get("end"),
                                    fact.get("val"),
                                    fact.get("form"),
                                    fact.get("filed"),
                                    fact.get("fy"),
                                    fact.get("fp"),
                                )
                            ),
                            "cik": cik,
                            "taxonomy": taxonomy,
                            "tag": tag,
                            "unit": unit_name,
                            "val": fact.get("val"),
                            "fy": fact.get("fy"),
                            "fp": fact.get("fp"),
                            "form": fact.get("form"),
                            "filed": fact.get("filed"),
                            "start": fact.get("start"),
                            "end": fact.get("end"),
                        }
                        yield record["_row_key"], json.dumps(record, sort_keys=True).encode()