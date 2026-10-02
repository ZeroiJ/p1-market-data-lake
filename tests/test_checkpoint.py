import json

from de_lake.checkpoint import Checkpoint, checkpoint_key
from de_lake.storage.filesystem import FilesystemStore


def test_checkpoint_survives_a_reload(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("2026-01-05")
    cp.save(store)
    assert Checkpoint.load(store, "binance", "klines").is_done("2026-01-05")


def test_resume_skips_completed_units_and_does_the_rest(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("2026-01-05")
    cp.save(store)

    reloaded = Checkpoint.load(store, "binance", "klines")
    todo = [u for u in ["2026-01-05", "2026-01-06", "2026-01-07"] if not reloaded.is_done(u)]
    assert todo == ["2026-01-06", "2026-01-07"]


def test_marking_the_same_unit_twice_is_idempotent(tmp_path):
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("u")
    cp.mark_done("u")
    assert cp.completed == ["u"]


def test_a_truncated_checkpoint_file_does_not_corrupt_state(tmp_path):
    # A torn checkpoint costs re-work; a raised exception would cost the whole
    # DAG run. Losing progress is always cheaper than losing the pipeline.
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("u")
    cp.save(store)
    store.atomic_write(checkpoint_key("binance", "klines"), b'{"completed": [')
    assert Checkpoint.load(store, "binance", "klines").completed == []


def test_loading_a_checkpoint_that_never_existed_is_empty_not_an_error(tmp_path):
    store = FilesystemStore(tmp_path)
    assert Checkpoint.load(store, "stooq", "daily").completed == []


def test_different_sources_do_not_share_a_checkpoint(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("u")
    cp.save(store)
    assert Checkpoint.load(store, "stooq", "klines").completed == []


def test_units_are_kept_in_completion_order(tmp_path):
    cp = Checkpoint(source="binance", unit="klines")
    for unit in ["2026-01-07", "2026-01-05", "2026-01-06"]:
        cp.mark_done(unit)
    assert cp.completed == ["2026-01-07", "2026-01-05", "2026-01-06"]


def test_a_checkpoint_from_a_different_unit_name_is_ignored(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="sec_edgar", unit="filings")
    cp.mark_done("u")
    cp.save(store)
    assert Checkpoint.load(store, "sec_edgar", "xbrl").completed == []


def test_saved_checkpoint_is_readable_json(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("2026-01-05")
    cp.save(store)
    payload = json.loads(store.read(checkpoint_key("binance", "klines")))
    assert payload["source"] == "binance"
    assert payload["unit"] == "klines"
    assert payload["completed"] == ["2026-01-05"]


def test_partial_progress_survives_repeated_save_cycles(tmp_path):
    # The realistic kill scenario: save after each unit, die somewhere in the
    # middle, reload, and carry on from exactly where it stopped.
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    for unit in ["2026-01-05", "2026-01-06", "2026-01-07"]:
        cp.mark_done(unit)
        cp.save(store)

    resumed = Checkpoint.load(store, "binance", "klines")
    assert resumed.completed == ["2026-01-05", "2026-01-06", "2026-01-07"]

    remaining = [u for u in ["2026-01-08"] if not resumed.is_done(u)]
    resumed.mark_done(remaining[0])
    resumed.save(store)
    assert Checkpoint.load(store, "binance", "klines").is_done("2026-01-08")