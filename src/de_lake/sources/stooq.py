"""Stooq daily OHLCV, as CSV.

This source deliberately does no parsing. Stooq serves a ticker's entire history
as one flat CSV with a header line and no envelope, unlike the three JSON
sources, so `fetch` hands the bytes through untouched and lets `curate` deal
with them.

That is not laziness — it is the honest boundary. A source's job is to fetch
what the upstream sent, faithfully. Interpreting it is curation's job. Keeping
that line drawn is why a future fourth CSV-shaped upstream would be a one-line
adapter rather than a change to the pipeline.

The quirk worth knowing: Stooq answers an unknown or delisted ticker with HTTP
200 and the literal body `No data`. Treating that as a payload would land a
one-line file that later fails validation with a baffling message, so it is
detected here and treated as an empty unit.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta

from de_lake.sources.base import RetryableSourceError

DL_BASE = "https://stooq.com/q/d/l/"

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_EMPTY_STATUS = {400, 404}
NO_DATA_BODY = b"No data"


class _Empty(Exception):
    """Internal signal: this unit legitimately has no content."""


class StooqSource:
    name = "stooq"

    def __init__(self, session, ticker: str = "aapl.us") -> None:
        self._session = session
        self._ticker = ticker.lower()

    def units(self, start: date, end: date) -> list[str]:
        current = start
        units: list[str] = []
        while current <= end:
            units.append(current.isoformat())
            current += timedelta(days=1)
        return units

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        url = f"{DL_BASE}?s={self._ticker}&d1={unit}&d2={unit}&i=d"
        try:
            response = self._session.get(url, headers={}, timeout=30)
        except (TimeoutError, ConnectionError) as exc:
            raise RetryableSourceError(f"transient failure: {exc}") from exc

        status = getattr(response, "status_code", 200)
        if status in _RETRYABLE_STATUS:
            raise RetryableSourceError(f"HTTP {status} from Stooq")
        if status in _EMPTY_STATUS:
            return

        # Accept either `content` (requests) or `text` (anything else). Reading
        # only one of them made this source silently yield nothing against a
        # session that offered the other — a failure that looks like an empty day.
        body = getattr(response, "content", None)
        if body is None:
            text = getattr(response, "text", "") or ""
            body = text.encode() if isinstance(text, str) else bytes(text)
        if not body.strip() or body.strip() == NO_DATA_BODY:
            return

        yield f"{self._ticker}:{unit}", body