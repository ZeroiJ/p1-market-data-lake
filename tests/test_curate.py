import dataclasses
from datetime import UTC, datetime

import polars as pl
import pytest

from de_lake.curate import PARTITION_COLUMNS, CurateResult, curate
from de_lake.storage.filesystem import FilesystemStore


def _ts(minute: int) -> datetime:
    return datetime(2026, 1, 5, 0, minute, tzinfo=UTC)


def _row(**overrides) -> dict:
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


def _frame(*rows) -> pl.DataFrame:
    return pl.DataFrame(list(rows) or [_row()])


def _boom(monkeypatch, store, match: str = ""):
    real = store.atomic_write

    def explode(key, data):
        if match in key:
            raise OSError("disk full")
        return real(key, data)

    monkeypatch.setattr(store, "atomic_write", explode)


# --- partitioning ---------------------------------------------------------


def test_rows_are_partitioned_by_ingest_date(tmp_path):
    store = FilesystemStore(tmp_path)
    result = curate(store, "binance", _frame())
    assert result.ok_keys[0].startswith("curated/binance/date=2026-01-05/")


def test_rows_spanning_two_days_land_in_two_partitions(tmp_path):
    store = FilesystemStore(tmp_path)
    other = datetime(2026, 1, 6, 0, 0, tzinfo=UTC)
    result = curate(
        store,
        "binance",
        _frame(_row(), _row(_row_key="b", open_time=other, close_time=other)),
    )
    assert len(result.ok_keys) == 2
    assert sum("date=2026-01-05" in k for k in result.ok_keys) == 1
    assert sum("date=2026-01-06" in k for k in result.ok_keys) == 1


@pytest.mark.parametrize(
    ("source", "column"),
    list(PARTITION_COLUMNS.items()),
)
def test_every_source_declares_the_column_it_partitions_on(source, column):
    assert PARTITION_COLUMNS[source] == column


def test_stooq_partitions_on_its_date_column(tmp_path):
    store = FilesystemStore(tmp_path)
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
    assert (
        curate(store, "stooq", df)
        .ok_keys[0]
        .startswith("curated/stooq/date=2026-01-05/")
    )


# --- idempotency ----------------------------------------------------------


def test_identical_input_produces_identical_keys(tmp_path):
    df = _frame()
    a = curate(FilesystemStore(tmp_path / "a"), "binance", df)
    b = curate(FilesystemStore(tmp_path / "b"), "binance", df)
    assert a.ok_keys[0].split("/")[-1] == b.ok_keys[0].split("/")[-1]


def test_rerunning_over_unchanged_data_creates_nothing_new(tmp_path):
    store = FilesystemStore(tmp_path)
    curate(store, "binance", _frame())
    before = sorted(store.list_keys(""))
    assert sorted(store.list_keys("")) == before


def test_a_single_added_row_produces_exactly_one_new_file(tmp_path):
    # `high` is raised alongside `close`: an OHLC row with close > high is not a
    # real bar, so the contract would (correctly) quarantine it instead.
    store = FilesystemStore(tmp_path)
    curate(store, "binance", _frame())
    curate(store, "binance", _frame(_row(_row_key="b", close=9.0, high=10.0)))
    assert len(list(store.list_keys("curated/"))) == 2


def test_a_close_above_its_high_goes_to_quarantine_not_the_mart(tmp_path):
    store = FilesystemStore(tmp_path)
    result = curate(store, "binance", _frame(_row(close=9.0)))
    assert result.rows_ok == 0 and result.rows_quarantined == 1
    assert list(store.list_keys("curated/")) == []


# --- quarantine isolation -------------------------------------------------


def test_bad_rows_land_under_quarantine_not_the_mart(tmp_path):
    store = FilesystemStore(tmp_path)
    result = curate(store, "binance", _frame(_row(), _row(_row_key="b", ticker=None)))
    assert result.rows_ok == 1 and result.rows_quarantined == 1
    assert all(k.startswith("quarantine/binance/") for k in result.quarantine_keys)


def test_the_mart_contains_only_good_rows(tmp_path):
    store = FilesystemStore(tmp_path)
    curate(store, "binance", _frame(_row(), _row(_row_key="b", close=-1.0)))
    key = next(k for k in store.list_keys("curated/") if k.endswith(".parquet"))
    mart = pl.read_parquet(tmp_path / key)
    assert mart.height == 1
    assert mart["_row_key"].to_list() == ["a"]


def test_quarantine_records_why_each_row_failed(tmp_path):
    store = FilesystemStore(tmp_path)
    curate(store, "binance", _frame(_row(close=-1.0)))
    key = next(k for k in store.list_keys("quarantine/") if k.endswith(".parquet"))
    q = pl.read_parquet(tmp_path / key)
    assert "reason" in q.columns
    assert "close" in q["reason"][0]


def test_failed_mart_write_does_not_publish_quarantine(tmp_path, monkeypatch):
    # Quarantine is written only after every mart partition has landed, so a
    # failed curate retries cleanly instead of recording a partial outcome.
    store = FilesystemStore(tmp_path)
    _boom(monkeypatch, store, match="curated/")
    with pytest.raises(OSError):
        curate(store, "binance", _frame(_row(), _row(_row_key="b", ticker=None)))
    assert list(store.list_keys("quarantine/")) == []


def test_failed_quarantine_write_leaves_the_mart_intact(tmp_path, monkeypatch):
    store = FilesystemStore(tmp_path)
    _boom(monkeypatch, store, match="quarantine/")
    with pytest.raises(OSError):
        curate(store, "binance", _frame(_row(), _row(_row_key="b", ticker=None)))
    assert len(list(store.list_keys("curated/"))) == 1


# --- results --------------------------------------------------------------


def test_curate_returns_row_counts(tmp_path):
    store = FilesystemStore(tmp_path)
    result = curate(store, "binance", _frame(_row(), _row(_row_key="b", close=-1.0)))
    assert (result.rows_ok, result.rows_quarantined) == (1, 1)


def test_curating_an_empty_frame_writes_nothing(tmp_path):
    store = FilesystemStore(tmp_path)
    result = curate(store, "binance", pl.DataFrame(schema=_frame().schema))
    assert result.ok_keys == [] and result.quarantine_keys == []
    assert list(store.list_keys("")) == []


def test_curate_result_is_frozen(tmp_path):
    result = curate(FilesystemStore(tmp_path), "binance", _frame())
    assert isinstance(result, CurateResult)
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.rows_ok = 99
