from datetime import datetime, timezone

import polars as pl
import pytest

from de_lake.contracts import UnknownSource, contract_for, validate


def _ts(minute: int) -> datetime:
    return datetime(2026, 1, 5, 0, minute, tzinfo=timezone.utc)


def _binance_row(**overrides) -> dict:
    row = {
        "_row_key": "a",
        "ticker": "AAPL",
        "open_time": _ts(0),
        "close_time": _ts(1),
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100.0,
    }
    row.update(overrides)
    return row


def _binance_frame(*rows) -> pl.DataFrame:
    return pl.DataFrame(list(rows) or [_binance_row()])


# --- the happy path -------------------------------------------------------


def test_valid_rows_pass_through_untouched():
    df = _binance_frame(_binance_row(), _binance_row(_row_key="b", ticker="MSFT"))
    r = validate(df, "binance")
    assert r.ok.height == 2 and r.quarantine.height == 0
    assert r.reasons == {}


def test_valid_stooq_rows_pass_through():
    df = pl.DataFrame(
        {
            "_row_key": ["a"],
            "ticker": ["aapl.us"],
            "date": ["2026-01-05"],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [100.0],
        }
    )
    assert validate(df, "stooq").ok.height == 1


# --- quarantine routing ---------------------------------------------------


def test_row_missing_required_column_goes_to_quarantine_with_reason():
    df = _binance_frame(_binance_row(_row_key="a"), _binance_row(_row_key="b", ticker=None))
    r = validate(df, "binance")
    assert r.ok.height == 1
    assert r.quarantine.height == 1
    assert "ticker" in r.reasons["b"]


def test_non_positive_close_is_rejected_for_binance():
    assert validate(_binance_frame(_binance_row(close=0.0)), "binance").quarantine.height == 1


def test_negative_volume_is_rejected():
    assert validate(_binance_frame(_binance_row(volume=-1.0)), "binance").quarantine.height == 1


def test_high_below_low_is_rejected():
    assert validate(_binance_frame(_binance_row(high=0.1)), "binance").quarantine.height == 1


def test_a_null_partition_key_is_rejected_rather_than_lost():
    # open_time is what curate partitions on; a null here would be an
    # unpartitionable row, so it must be caught before it reaches the mart.
    df = _binance_frame(_binance_row(open_time=None))
    assert validate(df, "binance").quarantine.height == 1


def test_quarantined_rows_keep_their_original_values():
    df = _binance_frame(_binance_row(close=-5.0))
    q = validate(df, "binance").quarantine
    assert q["close"][0] == -5.0


def test_reason_names_the_offending_column():
    r = validate(_binance_frame(_binance_row(high=0.1)), "binance")
    assert "high" in r.reasons["a"]


# --- contract registry ----------------------------------------------------


def test_unknown_source_raises():
    with pytest.raises(UnknownSource):
        validate(pl.DataFrame({"_row_key": ["a"]}), "nope")


@pytest.mark.parametrize(
    "source", ["sec_edgar", "sec_xbrl", "binance", "stooq"]
)
def test_every_registered_contract_is_reachable(source):
    assert isinstance(contract_for(source), dict)


def test_contract_includes_the_row_key():
    assert "_row_key" in contract_for("binance")


# --- typing ---------------------------------------------------------------


def test_string_columns_are_coerced_to_their_contract_type():
    df = pl.DataFrame(
        {
            "_row_key": ["a"],
            "ticker": ["AAPL"],
            "open_time": [str(_ts(0))],
            "close_time": [str(_ts(1))],
            "open": ["1.0"],
            "high": ["2.0"],
            "low": ["0.5"],
            "close": ["1.5"],
            "volume": ["100"],
        }
    )
    r = validate(df, "binance")
    assert r.ok.height == 1
    assert r.ok.schema["open_time"] == pl.Datetime(time_unit="us", time_zone="UTC")