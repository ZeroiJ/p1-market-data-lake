"""The Source protocol.

Every source is an independent module that knows two things: how to enumerate
the units of work it must fetch (a date, a CIK, a page) and how to fetch one.
They know nothing about landing, curation, checkpoints or the lake layout.

That separation is what lets four very different upstream shapes — SEC JSON
envelopes, XBRL fact graphs, Binance's positional kline arrays, and Stooq's
flat CSV — share one pipeline without any of them leaking their quirks into it.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from typing import Protocol, runtime_checkable


class SourceError(Exception):
    """Base class for source failures."""


class RetryableSourceError(SourceError):
    """A transient failure: 429, 5xx, connection reset.

    Distinct from `SourceError` because these are the only failures worth
    retrying. A 404 is not one — for SEC and Stooq a 404 usually means "no data
    for that unit", which is a normal outcome, not a fault.
    """


@runtime_checkable
class Source(Protocol):
    """Fetches raw payloads for a named upstream."""

    name: str

    #: The symbol every row belongs to, for upstreams whose payloads do not
    #: carry one — `aapl.us`, `BTCUSDT`. `None` for upstreams that identify each
    #: row themselves (SEC filings carry a CIK). Declared here so downstream
    #: code never has to reach into a source's private attributes.
    row_label: str | None

    def units(self, start: date, end: date) -> list[str]:
        """Enumerate the units of work between `start` and `end`, inclusive."""
        ...

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        """Yield `(logical_name, raw_payload)` pairs for one unit.

        Yields rather than returns a single payload so that a source with
        internal pagination — Binance at 1000 rows per request — lands
        incrementally. A crash mid-unit then loses at most one page instead of
        the whole unit.
        """
        ...
