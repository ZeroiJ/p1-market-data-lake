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

    def _response_text(self, url: str) -> str:
        """Fetch `url`, returning body text, or raising the right signal.

        Returns None-free: raises `_Empty` for a legitimately absent unit and
        `RetryableSourceError` for anything transient.
        """
        try:
            response = self._session.get(
                url, headers={"User-Agent": self._user_agent}, timeout=30
            )
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
        return text

    def get_json(self, url: str):
        return json.loads(self._response_text(url))

    def get_text(self, url: str) -> str:
        """Return raw body text for the non-JSON SEC endpoints."""
        return self._response_text(url)


_INDEX_HEADER = "CIK|Company Name|Form Type|Date Filed|File Name"


def parse_daily_index(body: str) -> list[dict]:
    """Parse an EDGAR ``master.<date>.idx`` file into contract-shaped records.

    The file is pipe-delimited text, not JSON, and it opens with a metadata
    preamble (Description / Last Data Received / FTP lines), a real header row
    and a dashed separator. All three have to be skipped, or they parse as
    filings. ``Date Filed`` is ``YYYYMMDD`` upstream and is normalised to ISO
    here, because ``curate`` partitions on the first 10 characters of this
    column and ``20260105`` would otherwise become a ``date=20260105``
    partition that sorts nowhere near its neighbours.
    """
    records: list[dict] = []
    started = False

    for raw in body.splitlines():
        line = raw.rstrip("\r")
        if not started:
            if line.startswith(_INDEX_HEADER):
                started = True
            continue

        if set(line) <= {"-"} or not line.strip():
            continue

        fields = line.split("|")
        if len(fields) < 5:
            continue

        cik, name, form, filed, path = (f.strip() for f in fields[:5])
        if not cik.isdigit():
            continue

        filed_iso = (
            f"{filed[:4]}-{filed[4:6]}-{filed[6:8]}"
            if filed.isdigit() and len(filed) == 8
            else filed
        )
        accession = path.rsplit("/", 1)[-1].removesuffix(".txt")
        records.append(
            {
                # One filing per (CIK, document path) — the natural key.
                "_row_key": f"{cik.zfill(10)}:{accession}",
                "cik": cik.zfill(10),
                "entity_name": name,
                "form": form,
                "filing_date": filed_iso,
                "accession_number": accession,
            }
        )
    return records


class SecEdgarSource:
    """Filings listed in SEC's daily index for a given calendar date."""

    name = "sec_edgar"
    row_label = None

    def __init__(self, session, user_agent: str) -> None:
        self._client = _SecClient(session, user_agent)

    def units(self, start: date, end: date) -> list[str]:
        return [day.isoformat() for day in _daterange(start, end)]

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        day = date.fromisoformat(unit)
        quarter = (day.month - 1) // 3 + 1
        url = f"{DAILY_INDEX_BASE}/{day.year}/QTR{quarter}/master.{day:%Y%m%d}.idx"
        try:
            body = self._client.get_text(url)
        except _Empty:
            return

        for record in parse_daily_index(body):
            yield record["_row_key"], json.dumps(record, sort_keys=True).encode()


class SecXbrlSource:
    """XBRL company facts, one row per (taxonomy, tag, unit).

    `units` returns CIKs rather than dates: companyfacts is a whole-history
    endpoint with no date range, so the natural unit of resumable work is one
    company.
    """

    name = "sec_xbrl"
    row_label = None

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
                        yield (
                            record["_row_key"],
                            json.dumps(record, sort_keys=True).encode(),
                        )
