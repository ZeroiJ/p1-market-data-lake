import pytest

from de_lake.storage.base import StoreKeyNotFound
from de_lake.storage.filesystem import FilesystemStore


def test_atomic_write_then_read_roundtrips(tmp_path):
    store = FilesystemStore(tmp_path)
    assert store.atomic_write("raw/a.parquet", b"hello").created is True
    assert store.read("raw/a.parquet") == b"hello"


def test_rewrite_identical_bytes_reports_not_created(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/a.parquet", b"hello")
    assert store.atomic_write("raw/a.parquet", b"hello").created is False


def test_no_tmp_file_is_ever_visible_in_a_listing(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/a.parquet", b"hello")
    assert not any(k.endswith(".tmp") for k in store.list_keys(""))


def test_list_keys_filters_by_prefix(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/x", b"1")
    store.atomic_write("curated/y", b"2")
    assert sorted(store.list_keys("raw/")) == ["raw/x"]


def test_read_missing_key_raises(tmp_path):
    with pytest.raises(StoreKeyNotFound):
        FilesystemStore(tmp_path).read("raw/nope")


def test_exists_reflects_what_was_written(tmp_path):
    store = FilesystemStore(tmp_path)
    assert store.exists("raw/a") is False
    store.atomic_write("raw/a", b"x")
    assert store.exists("raw/a") is True


def test_delete_removes_the_key(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/a", b"x")
    store.delete("raw/a")
    assert store.exists("raw/a") is False


def test_different_bytes_overwrite_and_report_created(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/a", b"one")
    assert store.atomic_write("raw/a", b"two").created is True
    assert store.read("raw/a") == b"two"


def test_purge_temp_files_removes_orphans_and_counts_them(tmp_path):
    store = FilesystemStore(tmp_path)
    (tmp_path / "curated").mkdir()
    orphan = tmp_path / "curated" / "part-x.parquet.abc123.tmp"
    orphan.write_bytes(b"half a file")
    store.atomic_write("curated/real.parquet", b"x")

    assert store.purge_temp_files() == 1
    assert not orphan.exists()
    assert store.read("curated/real.parquet") == b"x"


def test_purge_temp_files_is_a_no_op_on_a_clean_store(tmp_path):
    store = FilesystemStore(tmp_path)
    store.atomic_write("raw/a", b"x")
    assert store.purge_temp_files() == 0
    assert list(store.list_keys("raw/")) == ["raw/a"]


def test_a_purged_orphan_was_never_visible_to_list_keys(tmp_path):
    store = FilesystemStore(tmp_path)
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "junk.tmp").write_bytes(b"x")
    assert list(store.list_keys("raw/")) == []
