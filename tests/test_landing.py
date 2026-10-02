import hashlib
import json

import pytest

from de_lake.landing import land, landing_key
from de_lake.storage.filesystem import FilesystemStore


def test_landing_same_bytes_twice_creates_once(tmp_path):
    store = FilesystemStore(tmp_path)
    assert land(store, "stooq", "aapl.us", b"csv").created is True
    assert land(store, "stooq", "aapl.us", b"csv").created is False


def test_different_bytes_land_to_different_keys(tmp_path):
    store = FilesystemStore(tmp_path)
    a = land(store, "stooq", "aapl.us", b"csv1")
    b = land(store, "stooq", "aapl.us", b"csv2")
    assert a.key != b.key


def test_key_is_content_addressed_and_fanout_by_prefix(tmp_path):
    store = FilesystemStore(tmp_path)
    digest = hashlib.sha256(b"csv").hexdigest()
    result = land(store, "stooq", "aapl.us", b"csv")
    assert result.key == f"raw/stooq/{digest[:2]}/{digest}.bin"
    assert landing_key("stooq", "aapl.us", digest) == result.key


def test_sidecar_records_logical_name_and_sha(tmp_path):
    store = FilesystemStore(tmp_path)
    result = land(store, "stooq", "aapl.us", b"csv")
    sidecar = json.loads(store.read(result.key + ".meta"))
    assert sidecar["logical_name"] == "aapl.us"
    assert sidecar["sha256"] == hashlib.sha256(b"csv").hexdigest()
    assert sidecar["source"] == "stooq"


def test_interrupted_write_leaves_no_readable_part(tmp_path, monkeypatch):
    # Atomicity must survive a process death *during* the write, not merely a
    # failure afterwards. A half-written payload must never become visible.
    store = FilesystemStore(tmp_path)
    real_atomic_write = store.atomic_write

    def explode(key, data):
        if key.endswith(".bin"):
            raise KeyboardInterrupt
        return real_atomic_write(key, data)

    monkeypatch.setattr(store, "atomic_write", explode)
    with pytest.raises(KeyboardInterrupt):
        land(store, "stooq", "aapl.us", b"csv")
    assert list(store.list_keys("raw/")) == []


def test_payload_written_before_sidecar(tmp_path, monkeypatch):
    # A crash between the two writes must leave an unreadable-but-harmless
    # payload, never a sidecar pointing at content that is not there.
    store = FilesystemStore(tmp_path)
    real_atomic_write = store.atomic_write

    def explode(key, data):
        if key.endswith(".meta"):
            raise KeyboardInterrupt
        return real_atomic_write(key, data)

    monkeypatch.setattr(store, "atomic_write", explode)
    with pytest.raises(KeyboardInterrupt):
        land(store, "stooq", "aapl.us", b"csv")
    assert [k for k in store.list_keys("raw/") if k.endswith(".bin")]
    assert not [k for k in store.list_keys("raw/") if k.endswith(".meta")]


def test_landing_works_on_the_s3_backend_too(s3_store):
    first = land(s3_store, "binance", "BTCUSDT", b"[1,2,3]")
    second = land(s3_store, "binance", "BTCUSDT", b"[1,2,3]")
    assert first.created is True and second.created is False
    assert s3_store.read(first.key) == b"[1,2,3]"
    assert json.loads(s3_store.read(first.key + ".meta"))["logical_name"] == "BTCUSDT"


def test_landing_an_empty_payload_is_still_a_landing(tmp_path):
    # Sources legitimately return nothing; that must not be conflated with a crash.
    store = FilesystemStore(tmp_path)
    assert land(store, "binance", "BTCUSDT", b"").bytes_written == 0
