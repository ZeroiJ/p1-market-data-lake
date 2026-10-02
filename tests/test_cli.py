import polars as pl
import pytest

from de_lake.cli import main
from de_lake.storage.filesystem import FilesystemStore


def _parquet_bytes(rows: int = 2) -> bytes:
    import io

    buffer = io.BytesIO()
    pl.DataFrame({"_row_key": [f"k{i}" for i in range(rows)], "ticker": ["AAPL"] * rows}).write_parquet(
        buffer
    )
    return buffer.getvalue()


@pytest.fixture
def lake(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("curated/binance/date=2026-01-05/part-a.parquet", _parquet_bytes())
    store.atomic_write("curated/binance/date=2026-01-06/part-b.parquet", _parquet_bytes())
    store.atomic_write("curated/stooq/date=2026-01-05/part-c.parquet", _parquet_bytes(3))
    store.atomic_write("raw/binance/ab/abcd.bin", b"payload")
    return tmp_path


# --- partitions -----------------------------------------------------------


def test_partitions_lists_key_count_and_bytes(lake, capsys):
    main(["partitions", "--store-root", str(lake)])
    out = capsys.readouterr().out
    assert "curated/binance/date=2026-01-05" in out
    assert "curated/binance/date=2026-01-06" in out
    assert "curated/stooq/date=2026-01-05" in out


def test_partitions_reports_each_partition_separately(lake, capsys):
    main(["partitions", "--store-root", str(lake)])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if "date=" in ln]
    assert len(lines) == 3


def test_partitions_can_be_scoped_to_a_prefix(lake, capsys):
    main(["partitions", "--store-root", str(lake), "--prefix", "curated/"])
    out = capsys.readouterr().out
    assert "date=" in out
    assert "raw/" not in out


def test_partitions_shows_a_byte_column(lake, capsys):
    main(["partitions", "--store-root", str(lake)])
    header = capsys.readouterr().out.splitlines()[0]
    assert "BYTES" in header.upper()


def test_partitions_on_an_empty_lake_says_so(tmp_path, capsys):
    main(["partitions", "--store-root", str(tmp_path)])
    assert "no" in capsys.readouterr().out.lower()


def test_partitions_exits_zero_on_an_empty_lake(tmp_path):
    assert main(["partitions", "--store-root", str(tmp_path)]) == 0


# --- show -----------------------------------------------------------------


def test_show_reports_schema_and_row_count(lake, capsys):
    key = "curated/stooq/date=2026-01-05/part-c.parquet"
    main(["show", key, "--store-root", str(lake)])
    out = capsys.readouterr().out
    assert "ticker" in out
    assert "3" in out


def test_show_reports_the_row_count_for_a_mart_partition(lake, capsys):
    key = "curated/binance/date=2026-01-05/part-a.parquet"
    main(["show", key, "--store-root", str(lake)])
    assert "2" in capsys.readouterr().out


def test_show_on_a_missing_key_exits_nonzero(lake, capsys):
    assert main(["show", "curated/nope.parquet", "--store-root", str(lake)]) != 0
    assert "not found" in capsys.readouterr().err.lower()


def test_show_on_a_non_parquet_key_reports_it_clearly(lake, capsys):
    assert main(["show", "raw/binance/ab/abcd.bin", "--store-root", str(lake)]) != 0
    assert "not parquet" in capsys.readouterr().err.lower()


# --- wiring ---------------------------------------------------------------


def test_no_arguments_is_a_usage_error(capsys):
    assert main([]) != 0


def test_unknown_subcommand_is_a_usage_error(capsys):
    assert main(["frobnicate"]) != 0

# --- scale ----------------------------------------------------------------


def _big_lake(tmp_path, raw_count: int = 300) -> None:
    store = FilesystemStore(tmp_path)
    for i in range(raw_count):
        digest = f"{i:064x}"
        store.atomic_write(f"raw/binance/{digest[:2]}/{digest}.bin", b"x")
        store.atomic_write(f"raw/binance/{digest[:2]}/{digest}.bin.meta", b"y")
    store.atomic_write("curated/binance/date=2026-01-05/part-a.parquet", _parquet_bytes())


def test_raw_objects_group_by_source_not_one_line_each(tmp_path, capsys):
    # Regression: every raw object became its own "partition", so a real lake
    # printed 37,000 lines instead of a handful.
    _big_lake(tmp_path)
    main(["partitions", "--store-root", str(tmp_path)])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("raw/")]
    assert len(lines) == 1
    assert lines[0].startswith("raw/binance")
    assert "600" in lines[0], "300 payloads + 300 sidecars, counted as keys"


def test_output_is_capped_and_says_how_many_were_hidden(tmp_path, capsys):
    _big_lake(tmp_path)
    for day in range(1, 120):
        FilesystemStore(tmp_path).atomic_write(
            f"curated/stooq/date=2026-01-{day:03d}/part-x.parquet", _parquet_bytes()
        )
    main(["partitions", "--store-root", str(tmp_path), "--limit", "10"])
    out = capsys.readouterr().out
    body = [ln for ln in out.splitlines() if ln.startswith(("curated/", "raw/", "quarantine/"))]
    assert len(body) == 10
    assert "more" in out.lower()


def test_limit_is_configurable(tmp_path, capsys):
    _big_lake(tmp_path)
    main(["partitions", "--store-root", str(tmp_path), "--limit", "1"])
    body = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(("curated/", "raw/"))]
    assert len(body) == 1
