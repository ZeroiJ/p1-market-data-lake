"""Binance 1-minute OHLCV klines.

Binance returns a *positional* array per candle — thirteen unnamed fields whose
order is only documented in prose — so this module's real job is transposing
that into named columns the contract can validate. Get the order wrong and the
data still loads; it is just wrong, which is worse.

Pagination is per page rather than per day, deliberately. `fetch` yields one
tuple per 1000-candle page so that landing is incremental: a crash mid-day loses
at most the page in flight instead of the whole unit. The price is a source that
cannot be restarted mid-unit without re-requesting what it already landed —
which is exactly what content-addressed landing makes free.

The boundary between page N and page N+1 is where pagination bugs hide. They do
not raise; they silently duplicate or skip a candle, and a market dataset with a
duplicated minute is quietly wrong forever. `test_no_page_overlap_or_gap` exists
specifically to pin that down.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

from de_lake.sources.base import RetryableSourceError

KLINES_URL = "https://api.binance.com/api/v3/klines"
PAGE_LIMIT = 1000
MINUTE_MS = 60_000

# Binance's positional kline tuple, in the order the API returns it.
KLINE_FIELDS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_EMPTY_STATUS = {400, 404}


class _Empty(Exception):
    """Internal signal: this unit legitimately has no content."""


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


class BinanceSource:
    """One-minute OHLCV candles for a single spot symbol."""

    name = "binance"

    def __init__(self, session, ticker: str = "BTCUSDT") -> None:
        self._session = session
        self.ticker = ticker.upper()

    @property
    def row_label(self) -> str:
        return self.ticker

    def units(self, start: date, end: date) -> list[str]:
        current = start
        units: list[str] = []
        while current <= end:
            units.append(current.isoformat())
            current += timedelta(days=1)
        return units

    def _get_page(self, params: dict[str, int]) -> list:
        try:
            response = self._session.get(
                KLINES_URL, headers={}, params=params, timeout=30
            )
        except (TimeoutError, ConnectionError) as exc:
            raise RetryableSourceError(f"transient failure: {exc}") from exc

        status = getattr(response, "status_code", 200)
        if status in _RETRYABLE_STATUS:
            raise RetryableSourceError(f"HTTP {status} from Binance")
        if status in _EMPTY_STATUS:
            raise _Empty()
        return response.json()

    def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]:
        """Yield one `(logical_name, payload)` per page of candles for `unit`."""
        day = date.fromisoformat(unit)
        cursor_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000
        )
        end_ms = cursor_ms + 24 * 60 * MINUTE_MS
        page_index = 0

        while cursor_ms < end_ms:
            try:
                rows = self._get_page(
                    {
                        "symbol": self.ticker,
                        "interval": "1m",
                        "startTime": cursor_ms,
                        "limit": PAGE_LIMIT,
                    }
                )
            except _Empty:
                return

            if not rows:
                return

            # A short page means we reached the end of the day; a full page means
            # there may be more. Both are handled by the cursor advance below.
            records = [self._to_record(row) for row in rows if int(row[0]) < end_ms]
            if records:
                name = f"{self.ticker}:{unit}:p{page_index:04d}"
                yield name, json.dumps(records, sort_keys=True).encode()

            last_open_ms = int(rows[-1][0])
            if last_open_ms < cursor_ms:
                raise RetryableSourceError("Binance returned a non-advancing cursor")
            cursor_ms = last_open_ms + MINUTE_MS
            page_index += 1

            if len(rows) < PAGE_LIMIT:
                return

    def _to_record(self, row: list) -> dict:
        values = dict(zip(KLINE_FIELDS, row, strict=False))
        open_ms = int(values["open_time"])
        close_ms = int(values["close_time"])
        return {
            # One candle per (symbol, minute) — that pair is the natural key and
            # makes re-landing the same day a no-op.
            "_row_key": f"{self.ticker}:{open_ms}",
            "ticker": self.ticker,
            "open_time": _iso(open_ms),
            "close_time": _iso(close_ms),
            "open": float(values["open"]),
            "high": float(values["high"]),
            "low": float(values["low"]),
            "close": float(values["close"]),
            "volume": float(values["volume"]),
        }
