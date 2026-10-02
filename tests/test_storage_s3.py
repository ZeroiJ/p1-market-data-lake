from de_lake.storage.filesystem import FilesystemStore


def test_s3_backend_matches_filesystem_semantics(s3_store, tmp_path):
    fs = FilesystemStore(tmp_path)
    fs.atomic_write("raw/a.parquet", b"hello")
    s3_store.atomic_write("raw/a.parquet", b"hello")
    assert s3_store.read("raw/a.parquet") == fs.read("raw/a.parquet")
    assert s3_store.atomic_write("raw/a.parquet", b"hello").created is False
    assert sorted(s3_store.list_keys("raw/")) == ["raw/a.parquet"]


def test_s3_missing_key_raises_the_same_error_type(s3_store):
    from de_lake.storage.base import StoreKeyNotFound

    import pytest

    with pytest.raises(StoreKeyNotFound):
        s3_store.read("raw/nope")


def test_s3_delete_and_exists(s3_store):
    s3_store.atomic_write("raw/a", b"x")
    assert s3_store.exists("raw/a") is True
    s3_store.delete("raw/a")
    assert s3_store.exists("raw/a") is False


def test_s3_different_bytes_report_created(s3_store):
    s3_store.atomic_write("raw/a", b"one")
    assert s3_store.atomic_write("raw/a", b"two").created is True
    assert s3_store.read("raw/a") == b"two"