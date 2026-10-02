"""The Source protocol's public contract.

These tests exist because `pipeline._to_frame` needed the ticker a source was
built with, and the first fix reached into `source._ticker`. Reaching into a
private attribute from outside its class is the kind of coupling that makes a
module impossible to refactor later, so the protocol now declares the capability
explicitly as `row_label`.
"""

import pytest

from de_lake.sources.base import RetryableSourceError, Source, SourceError
from de_lake.sources.binance import BinanceSource
from de_lake.sources.sec_edgar import SecEdgarSource, SecXbrlSource
from de_lake.sources.stooq import StooqSource


class _Session:
    def get(self, url, headers=None, params=None, timeout=30):
        raise AssertionError("no network in this test")


def _sources():
    return [
        SecEdgarSource(session=_Session(), user_agent="UA"),
        SecXbrlSource(session=_Session(), user_agent="UA", ciks=["0000320193"]),
        BinanceSource(session=_Session(), ticker="BTCUSDT"),
        StooqSource(session=_Session(), ticker="aapl.us"),
    ]


@pytest.mark.parametrize("source", _sources(), ids=lambda s: s.name)
def test_every_source_satisfies_the_protocol(source):
    assert isinstance(source, Source)


@pytest.mark.parametrize("source", _sources(), ids=lambda s: s.name)
def test_every_source_exposes_row_label_without_private_access(source):
    assert "row_label" in dir(source)
    assert hasattr(source, "row_label")


def test_market_sources_report_their_ticker_as_row_label():
    binance, stooq = (
        BinanceSource(session=_Session(), ticker="BTCUSDT"),
        StooqSource(session=_Session(), ticker="aapl.us"),
    )
    assert binance.row_label == "BTCUSDT"
    assert stooq.row_label == "aapl.us"


def test_sec_sources_have_no_row_label():
    edgar = SecEdgarSource(session=_Session(), user_agent="UA")
    xbrl = SecXbrlSource(session=_Session(), user_agent="UA", ciks=["0000320193"])
    assert edgar.row_label is None
    assert xbrl.row_label is None


def test_row_label_is_normalised_to_upper_for_binance():
    assert BinanceSource(session=_Session(), ticker="btcusdt").row_label == "BTCUSDT"


def test_row_label_is_normalised_to_lower_for_stooq():
    assert StooqSource(session=_Session(), ticker="AAPL.US").row_label == "aapl.us"


def test_retryable_error_is_a_source_error():
    assert issubclass(RetryableSourceError, SourceError)
