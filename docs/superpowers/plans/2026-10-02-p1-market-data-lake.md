# P1 · Market Data Lake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A resumable, idempotent ingestion pipeline that lands SEC EDGAR filings, Binance OHLCV, and Stooq daily OHLCV into a partitioned Parquet lake, via a pluggable storage backend, with quarantine-not-drop handling and a browser for the result.

**Architecture:** Each source is an isolated module that yields raw bytes. A single `ObjectStore` protocol has two implementations — `FilesystemStore` (dev default) and `S3Store` (boto3; targets R2/MinIO/Garage). Landing is raw + immutable + content-addressed, which is what makes the pipeline idempotent and resumable. Normalisation applies a per-source schema contract, routing bad rows to quarantine instead of dropping them. Partitioning happens at curate time only.

**Tech Stack:** Python 3.14.7 · uv · Polars · PyArrow · boto3 · Apache Airflow 3.3.2 · pytest · moto 5.2.3

**Spec:** `../PORTFOLIO_MASTER_PLAN.md` §4 P1

---

## Global Constraints

- **Python 3.14.7 exactly.** Airflow 3.3.2 declares `requires_python >=3.10, !=3.15`, so the system interpreter is supported. Do not add a second Python.
- **No container runtime.** Docker Hub is unreachable from this machine. No Dockerfile, no MinIO container, no Airflow container.
- **No credentials in v1 except a User-Agent email.** No API keys, no tokens, no `.env` files with secrets. `SEC_USER_AGENT = "DataEngineeringPortfolio/0.1 (sujalbirwadkar19@gmail.com)"`.
- **FRED is out of scope for v1.** Deferred until a key exists.
- **Zero cost.** No paid API, no object store requiring a payment method. The local filesystem is the default sink.
- **Polars for all data work.** No pandas anywhere in the project.
- **Tests never hit the network.** Every source module is tested against a committed fixture. Live-network checks run only in an explicitly-marked integration test.
- **Every task ends with a commit.**

## File Structure

```
p1-market-data-lake/
  pyproject.toml
  uv.lock
  .env.example              # documents the User-Agent knob, holds no secret
  src/de_lake/
    __init__.py
    settings.py             # paths, SEC_USER_AGENT, lake root
    storage/
      __init__.py
      base.py               # ObjectStore Protocol + WriteResult
      filesystem.py         # FilesystemStore
      s3.py                 # S3Store (boto3, works against R2/MinIO/Garage)
    contracts.py            # canonical schema + validate() -> rows/ok/quarantine/reasons
    landing.py              # content-addressed landing, atomic
    curate.py               # contract validation + partitioned Parquet write
    checkpoint.py           # atomic JSON checkpoint
    manifest.py             # part inventory + dedupe-on-read
    sources/
      __init__.py
      base.py               # Source protocol
      sec_edgar.py
      binance.py
      stooq.py
    cli.py                  # lake browser
  tests/
    fixtures/               # recorded API responses
    conftest.py
    test_storage_filesystem.py
    test_storage_s3.py
    test_contracts.py
    test_landing.py
    test_curate.py
    test_checkpoint.py
    test_sources_*.py
    test_cli.py
  dags/
    ingest_market_data.py
  docs/
    superpowers/plans/
    superpowers/specs/
  README.md
```

## Review Focus

The spec is a vision document; these are the input classes it implies but no happy-path test exercises. Most likely to bite first.

1. **Re-running a completed date produces zero new rows.** Idempotency is the whole point of the project; a silent double-count is the worst possible bug.
2. **The process dies mid-write.** A partial Parquet file left in the lake is worse than no file — it is a file that fails to read later. Atomic write must be all-or-nothing.
3. **A row violates the schema contract.** It must reach quarantine *with a reason*. Dropping it is the cardinal sin.
4. **The source returns an empty body, a 429, or a truncated final page.** Empty is not an error and must not wipe existing data; 429 must back off, not crash the DAG.
5. **A partition directory would contain a single tiny file.** Partitioning by source+date only is deliberate — if a date range is chosen where partitions get tiny, the layout is wrong and that must be noticed.

---

## Task 1: Storage abstraction — ObjectStore protocol and both backends

**Files:**
- Create: `pyproject.toml`, `src/de_lake/__init__.py`, `src/de_lake/storage/{__init__.py,base.py,filesystem.py,s3.py}`, `tests/conftest.py`, `tests/test_storage_filesystem.py`, `tests/test_storage_s3.py`, `.env.example`

**Interfaces:**
- Produces:
  ```python
  # storage/base.py
  class ObjectStore(Protocol):
      def atomic_write(self, key: str, data: bytes) -> WriteResult: ...
      def read(self, key: str) -> bytes: ...
      def exists(self, key: str) -> bool: ...
      def list_keys(self, prefix: str) -> Iterator[str]: ...
      def delete(self, key: str) -> None: ...
  @dataclass(frozen=True) class WriteResult: key: str; bytes_written: int; created: bool
  def get_store() -> ObjectStore        # storage/__init__.py, reads settings
  ```
  `WriteResult.created` is `False` when the key already held identical bytes — this is how idempotency is observed.

  **`atomic_write`, not `write`.** Atomicity is a property of the backend, not of the caller. `FilesystemStore` implements it as write-`key.tmp`-fsync-`os.replace`. `S3Store` implements it as a single `put_object`, because S3 has no rename but a whole-object PUT is already atomic. Every later task — landing, curation, checkpointing — calls `atomic_write` and inherits correct behaviour on both backends instead of each one re-deriving it.

- [ ] **Step 1: Initialise the uv project and install dependencies**

```bash
cd /home/zeroij/de_portfolio/p1-market-data-lake
uv init --lib --python 3.14 --name de-lake
uv add polars pyarrow boto3
uv add --dev pytest moto
```

- [ ] **Step 2: Write the failing FilesystemStore tests**

```python
# tests/test_storage_filesystem.py
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
    store.atomic_write("raw/x", b"1"); store.atomic_write("curated/y", b"2")
    assert sorted(store.list_keys("raw/")) == ["raw/x"]

def test_read_missing_key_raises(tmp_path):
    with pytest.raises(StoreKeyNotFound):
        FilesystemStore(tmp_path).read("raw/nope")
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_storage_filesystem.py -v`
Expected: FAIL with `ImportError: cannot import name 'FilesystemStore'`

- [ ] **Step 4: Implement `base.py` and `filesystem.py`**

`ObjectStore` as a `typing.Protocol`. `FilesystemStore(root: Path)` maps `key` to `root / key`, creating parent directories on write. `write` computes the sha256 of `data`; if the target exists and its hash matches, return `WriteResult(created=False)` without rewriting. Raise `StoreKeyNotFound(KeyError)` from `read`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_storage_filesystem.py -v`
Expected: 5 passed

- [ ] **Step 6: Write the failing S3Store parity tests against moto**

```python
# tests/test_storage_s3.py
def test_s3_backend_matches_filesystem_semantics(s3_store, tmp_path):
    fs = FilesystemStore(tmp_path)
    fs.atomic_write("raw/a.parquet", b"hello")
    s3_store.atomic_write("raw/a.parquet", b"hello")
    assert s3_store.read("raw/a.parquet") == fs.read("raw/a.parquet")
    assert s3_store.atomic_write("raw/a.parquet", b"hello").created is False
    assert sorted(s3_store.list_keys("raw/")) == ["raw/a.parquet"]
```

The `s3_store` fixture starts `moto.server` on an ephemeral port and points boto3 at it with dummy credentials. Put that fixture in `conftest.py`.

- [ ] **Step 7: Run the S3 tests to verify they fail**

Run: `uv run pytest tests/test_storage_s3.py -v`
Expected: FAIL with `ImportError: cannot import name 'S3Store'`

- [ ] **Step 8: Implement `s3.py` and `get_store()`**

`S3Store(bucket: str, client)`. `atomic_write` is a single `put_object` — S3 has no rename, but a whole-object PUT is already atomic, so no `.tmp` dance is needed or wanted. Use `get_object` / `list_objects_v2` with `Pagination`; map botocore's `NoSuchKey`/`ClientError` 404 to `StoreKeyNotFound`. `get_store()` returns `FilesystemStore` unless `DE_LAKE_S3_BUCKET` is set, in which case `S3Store` from `DE_LAKE_S3_ENDPOINT`/`DE_LAKE_S3_ACCESS_KEY`/`DE_LAKE_S3_SECRET_KEY`. Document these four vars in `.env.example`.

- [ ] **Step 9: Run the full test suite**

Run: `uv run pytest -v`
Expected: 9 passed

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml uv.lock .env.example src tests
git commit -m "feat: ObjectStore protocol with filesystem and S3 backends"
```

---

## Task 2: Schema contracts and quarantine routing

**Files:**
- Create: `src/de_lake/contracts.py`, `tests/test_contracts.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  ```python
  # contracts.py
  @dataclass(frozen=True)
  class ContractResult:
      ok: pl.DataFrame
      quarantine: pl.DataFrame
      reasons: dict[str, str]      # row index -> reason
  def contract_for(source: str) -> pl.Schema
  def validate(df: pl.DataFrame, source: str) -> ContractResult
  ```
  Row keys in `reasons` are the values of the row's `_row_key` column, which callers must supply.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_contracts.py
def test_valid_rows_pass_through_untouched():
    df = pl.DataFrame({"_row_key": ["a", "b"], "ticker": ["AAPL", "MSFT"], "close": [1.0, 2.0]})
    r = validate(df, "binance")
    assert r.ok.height == 2 and r.quarantine.height == 0

def test_row_missing_required_column_goes_to_quarantine_with_reason():
    df = pl.DataFrame({"_row_key": ["a", "b"], "ticker": ["AAPL", None], "close": [1.0, 2.0]})
    r = validate(df, "binance")
    assert r.ok.height == 1
    assert r.quarantine.height == 1
    assert "ticker" in r.reasons["b"]

def test_unknown_source_raises():
    with pytest.raises(UnknownSource):
        validate(pl.DataFrame({"_row_key": ["a"]}), "nope")

def test_non_positive_close_is_rejected_for_binance():
    df = pl.DataFrame({"_row_key": ["a"], "ticker": ["AAPL"], "close": [0.0]})
    assert validate(df, "binance").quarantine.height == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_contracts.py -v`
Expected: FAIL with `ImportError: cannot import name 'validate'`

- [ ] **Step 3: Implement `contracts.py`**

A module-level `dict[str, pl.Schema]` named `_CONTRACTS`. Four contracts in v1: `sec_edgar` (`_row_key`, `cik: str`, `entity_name: str`, `form: str`, `filing_date: str`, `accession_number: str`), `sec_xbrl` (`_row_key`, `cik: str`, `taxonomy: str`, `tag: str`, `unit: str`, `val: float64`, `fy: int64 | None`, `fp: str | None`, `form: str`, `filed: str`, `start: str | None`, `end: str | None`), `binance` (`_row_key`, `ticker: str`, `open_time: datetime`, `open/high/low/close: float64`, `volume: float64`, `close_time: datetime`), `stooq` (`_row_key`, `ticker: str`, `date: str`, `open/high/low/close: float64`, `volume: float64`). `validate` casts to the target schema with `strict=False`, then applies the per-source null and range rules; any row failing becomes a quarantine row plus a reason string naming the offending column. Raise `UnknownSource` for an unregistered name.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_contracts.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/de_lake/contracts.py tests/test_contracts.py
git commit -m "feat: per-source schema contracts with quarantine routing"
```

---

## Task 3: Atomic, content-addressed landing

**Files:**
- Create: `src/de_lake/landing.py`, `tests/test_landing.py`
- Test: `tests/test_landing.py`

**Interfaces:**
- Consumes: `get_store()` from Task 1.
- Produces:
  ```python
  # landing.py
  def land(store: ObjectStore, source: str, logical_name: str, data: bytes) -> WriteResult
  def landing_key(source: str, logical_name: str, content_sha256: str) -> str
  ```
  Key layout: `raw/{source}/{sha256[:2]}/{sha256}.bin`. The logical name is recorded in the sidecar, not the key — that is what makes landing content-addressed and therefore idempotent.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_landing.py
def test_landing_same_bytes_twice_creates_once(tmp_path):
    store = FilesystemStore(tmp_path)
    assert land(store, "stooq", "aapl.us", b"csv").created is True
    assert land(store, "stooq", "aapl.us", b"csv").created is False

def test_different_bytes_land_to_different_keys(tmp_path):
    store = FilesystemStore(tmp_path)
    a = land(store, "stooq", "aapl.us", b"csv1")
    b = land(store, "stooq", "aapl.us", b"csv2")
    assert a.key != b.key

def test_interrupted_write_leaves_no_readable_part(tmp_path, monkeypatch):
    store = FilesystemStore(tmp_path)
    real_atomic_write = store.atomic_write
    def explode(key, data):
        if key.endswith(".tmp"): raise KeyboardInterrupt
        return real_atomic_write(key, data)
    monkeypatch.setattr(store, "atomic_write", explode)
    with pytest.raises(KeyboardInterrupt):
        land(store, "stooq", "aapl.us", b"csv")
    assert list(store.list_keys("raw/")) == []

def test_sidecar_records_logical_name_and_sha(tmp_path):
    store = FilesystemStore(tmp_path)
    r = land(store, "stooq", "aapl.us", b"csv")
    sidecar = json.loads(store.read(r.key + ".meta"))
    assert sidecar["logical_name"] == "aapl.us"
    assert sidecar["sha256"] == hashlib.sha256(b"csv").hexdigest()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_landing.py -v`
Expected: FAIL with `ImportError: cannot import name 'land'`

- [ ] **Step 3: Implement `landing.py`**

`land` calls `store.atomic_write` for the payload and then again for the `.meta` sidecar. It never touches `.tmp` itself — that is the backend's job. Order matters: payload first, sidecar second, so a crash between them leaves an unreadable-but-harmless payload rather than a sidecar pointing at nothing.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_landing.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/de_lake/landing.py tests/test_landing.py
git commit -m "feat: atomic content-addressed landing"
```

---

## Task 4: Curate — partition, and write Parquet only once all-or-nothing

**Files:**
- Create: `src/de_lake/curate.py`, `tests/test_curate.py`
- Test: `tests/test_curate.py`

**Interfaces:**
- Consumes: `ObjectStore` (T1), `validate` (T2).
- Produces:
  ```python
  # curate.py
  @dataclass(frozen=True)
  class CurateResult: ok_keys: list[str]; quarantine_keys: list[str]; rows_ok: int; rows_quarantined: int
  def curate(store: ObjectStore, source: str, frame: pl.DataFrame) -> CurateResult
  ```
  Partition layout: `curated/{source}/date={YYYY-MM-DD}/part-{sha256[:8]}.parquet`, where the hash is of the row content. Identical content on a re-run yields the identical key, so the write is naturally idempotent.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_curate.py
def test_rows_are_partitioned_by_ingest_date(tmp_path):
    store = FilesystemStore(tmp_path)
    df = pl.DataFrame({"_row_key": ["a"], "ticker": ["AAPL"], "close": [1.0],
                       "open_time": [datetime(2026, 1, 5, tzinfo=timezone.utc)],
                       "close_time": [datetime(2026, 1, 5, tzinfo=timezone.utc)]})
    res = curate(store, "binance", df)
    assert res.ok_keys[0].startswith("curated/binance/date=2026-01-05/")

def test_identical_input_produces_identical_keys(tmp_path):
    df = pl.DataFrame({"_row_key": ["a"], "ticker": ["AAPL"], "close": [1.0]})
    a = curate(FilesystemStore(tmp_path / "a"), "stooq", df)
    b = curate(FilesystemStore(tmp_path / "b"), "stooq", df)
    assert a.ok_keys[0].split("/")[-1] == b.ok_keys[0].split("/")[-1]

def test_bad_rows_land_under_quarantine_not_the_mart(tmp_path):
    store = FilesystemStore(tmp_path)
    df = pl.DataFrame({"_row_key": ["a", "bad"], "ticker": ["AAPL", None], "close": [1.0, 2.0]})
    res = curate(store, "binance", df)
    assert res.rows_ok == 1 and res.rows_quarantined == 1
    assert all(k.startswith("quarantine/binance/") for k in res.quarantine_keys)

def test_failed_mart_write_does_not_publish_quarantine(tmp_path, monkeypatch):
    # Quarantine is only written after the mart write succeeds, so a failed
    # curate retries cleanly instead of recording a partial outcome.
    store = FilesystemStore(tmp_path)
    df = pl.DataFrame({"_row_key": ["a", "bad"], "ticker": ["AAPL", None], "close": [1.0, 2.0]})
    monkeypatch.setattr(store, "atomic_write", lambda k, d: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        curate(store, "binance", df)
    assert list(store.list_keys("quarantine/")) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_curate.py -v`
Expected: FAIL with `ImportError: cannot import name 'curate'`

- [ ] **Step 3: Implement `curate.py`**

Call `validate`, then `partition_by` the date column (`open_time` for binance, `filing_date` for sec_edgar, `date` for stooq), serialise each partition to Parquet via `write_parquet`, and write it with `store.atomic_write`. Write mart partitions first; write quarantine only after every mart partition succeeded.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_curate.py -v`
Expected: 4 passed

- [ ] **Step 5: Run the whole suite and commit**

Run: `uv run pytest -v` — Expected: all passed

```bash
git add src/de_lake/curate.py tests/test_curate.py
git commit -m "feat: partitioned Parquet curation with quarantine isolation"
```

---

## Task 5: Checkpoint and resume — the hard part

**Files:**
- Create: `src/de_lake/checkpoint.py`, `tests/test_checkpoint.py`
- Test: `tests/test_checkpoint.py`

**Interfaces:**
- Consumes: `ObjectStore` (T1).
- Produces:
  ```python
  # checkpoint.py
  @dataclass
  class Checkpoint:
      source: str
      unit: str              # logical unit of work, e.g. "2026-01-05"
      completed: list[str]
      def is_done(self, unit: str) -> bool: ...
      def mark_done(self, unit: str) -> None: ...
      def save(self, store: ObjectStore) -> None: ...
      @classmethod
      def load(cls, store: ObjectStore, source: str) -> "Checkpoint": ...
  ```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_checkpoint.py
def test_checkpoint_survives_a_reload(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("2026-01-05")
    cp.save(store)
    assert Checkpoint.load(store, "binance").is_done("2026-01-05")

def test_resume_skips_completed_units_and_does_the_rest(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("2026-01-05")
    cp.save(store)

    reloaded = Checkpoint.load(store, "binance")
    todo = [u for u in ["2026-01-05", "2026-01-06", "2026-01-07"] if not reloaded.is_done(u)]
    assert todo == ["2026-01-06", "2026-01-07"]

def test_marking_the_same_unit_twice_is_idempotent(tmp_path):
    cp = Checkpoint(source="binance", unit="klines")
    cp.mark_done("u"); cp.mark_done("u")
    assert cp.completed == ["u"]

def test_a_truncated_checkpoint_file_does_not_corrupt_state(tmp_path):
    store = FilesystemStore(tmp_path)
    cp = Checkpoint(source="binance", unit="klines"); cp.mark_done("u"); cp.save(store)
    key = f"_checkpoints/binance/klines.json"
    store.atomic_write(key, b'{"completed": [')   # simulate a torn write
    assert Checkpoint.load(store, "binance").completed == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_checkpoint.py -v`
Expected: FAIL with `ImportError: cannot import name 'Checkpoint'`

- [ ] **Step 3: Implement `checkpoint.py`**

Store at `_checkpoints/{source}/{unit}.json`. `save` calls `store.atomic_write`, so a torn write cannot replace a good checkpoint. `load` catches `json.JSONDecodeError` and returns an empty checkpoint rather than raising — a lost checkpoint costs re-work, while a raised exception costs the whole DAG run.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_checkpoint.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/de_lake/checkpoint.py tests/test_checkpoint.py
git commit -m "feat: atomic checkpoints with safe resume"
```

---

## Task 6: SEC EDGAR source — filings index and XBRL company facts

**Files:**
- Create: `src/de_lake/sources/{__init__.py,base.py,sec_edgar.py}`, `tests/fixtures/{sec_submissions_sample.json,sec_companyfacts_sample.json}`, `tests/test_sources_sec.py`
- Test: `tests/test_sources_sec.py`

**Interfaces:**
- Consumes: `land` (T3), `Checkpoint` (T5).
- Produces:
  ```python
  # sources/base.py
  class Source(Protocol):
      name: str
      def units(self, start: date, end: date) -> list[str]: ...
      def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]: ...   # (logical_name, payload)

  # sources/sec_edgar.py
  class SecEdgarSource:
      name = "sec_edgar"
      def __init__(self, session, user_agent: str): ...
  class SecXbrlSource:
      name = "sec_xbrl"
      def __init__(self, session, user_agent: str, ciks: list[str]): ...
  ```
  Both are SEC sources and share the User-Agent, retry mapping and error semantics. `SecXbrlSource.units` returns one unit per CIK rather than per date, because `companyfacts` is a whole-history endpoint with no date range.

- [ ] **Step 1: Commit trimmed real API responses as fixtures**

Fetch one real `submissions/CIK0000320193.json` (Apple) and one real `api/xbrl/companyfacts/CIK0000320193.json`, trim each to the fields the contract needs, commit them as `tests/fixtures/sec_submissions_sample.json` and `tests/fixtures/sec_companyfacts_sample.json`, and record the fetch date inside each file.

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_sources_sec.py
def test_units_enumerate_each_filing_date_in_range():
    src = SecEdgarSource(session=FakeSession(), user_agent=UA)
    assert src.units(date(2026, 1, 5), date(2026, 1, 7)) == [
        "2026-01-05", "2026-01-06", "2026-01-07"]

def test_fetch_sends_an_identifying_user_agent():
    src = SecEdgarSource(session=RecordingSession(), user_agent=UA)
    list(src.fetch("2026-01-05"))
    assert RecordingSession.last_headers["User-Agent"] == UA

def test_fetch_parses_recent_filings_into_rows():
    src = SecEdgarSource(session=FakeSession(), user_agent=UA)
    rows = list(src.fetch("2026-01-05"))
    assert rows and rows[0][0].startswith("CIK0000320193")

def test_a_429_response_raises_retryable_not_a_crash():
    src = SecEdgarSource(session=RateLimitedSession(), user_agent=UA)
    with pytest.raises(RetryableSourceError):
        list(src.fetch("2026-01-05"))

def test_empty_body_yields_nothing_and_does_not_raise():
    src = SecEdgarSource(session=EmptySession(), user_agent=UA)
    assert list(src.fetch("2026-01-05")) == []

def test_xbrl_units_are_ciks_not_dates():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    assert src.units(date(2026, 1, 1), date(2026, 1, 31)) == ["0000320193"]

def test_xbrl_fetch_flattens_facts_into_rows_not_one_blob_per_company():
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    rows = [json.loads(p) for _, p in src.fetch("0000320193")]
    assert rows and {"taxonomy", "tag", "fy", "fp", "val"}.issubset(rows[0].keys())

def test_xbrl_keeps_every_unit_of_a_fact_that_has_several():
    # "USD", "USD/shares" and "pure" are distinct facts, not duplicates
    src = SecXbrlSource(session=FakeSession(), user_agent=UA, ciks=["0000320193"])
    units = {r["unit"] for r in (json.loads(p) for _, p in src.fetch("0000320193"))}
    assert len(units) > 1
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_sources_sec.py -v`
Expected: FAIL with `ImportError: cannot import name 'SecEdgarSource'`

- [ ] **Step 4: Implement `sec_edgar.py`**

`SecEdgarSource.units` yields one string per calendar date. `fetch` GETs the daily index for that date with the User-Agent header, then GETs each referenced submissions document, and yields `(accession_number, raw_json_bytes)`.

`SecXbrlSource.fetch` GETs `api/xbrl/companyfacts/CIK{cik}.json` and yields **one tuple per (taxonomy, tag, unit) triple** rather than one blob per company, so a single malformed fact lands in quarantine without discarding its siblings. Preserve `fy`, `fp`, `form`, `filed`, `start`, `end`, `val` and `unit` — dropping `start`/`end` makes period-over-period analysis impossible later in P2.

Both classes share a private `_get(url)` helper that attaches the User-Agent and maps HTTP 429 and 5xx to `RetryableSourceError`; a 404 maps to an empty iterator, since a date with no filings is normal.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_sources_sec.py -v`
Expected: 9 passed

- [ ] **Step 6: Commit**

```bash
git add src/de_lake/sources tests/fixtures tests/test_sources_sec.py
git commit -m "feat: SEC EDGAR filings and XBRL company facts sources"
```

---

## Task 7: Binance OHLCV source

**Files:**
- Create: `src/de_lake/sources/binance.py`, `tests/fixtures/binance_klines_page.json`, `tests/test_sources_binance.py`
- Test: `tests/test_sources_binance.py`

**Interfaces:**
- Consumes: `RetryableSourceError` from Task 6.
- Produces:
  ```python
  class BinanceSource:
      name = "binance"
      def __init__(self, session, ticker: str = "BTCUSDT"): ...
      def units(self, start: date, end: date) -> list[str]: ...
      def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]: ...   # one page tuple per 1000-row page
  ```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_sources_binance.py
def test_pagination_covers_a_full_day_at_one_minute_resolution():
    src = BinanceSource(session=FakeSession(), ticker="BTCUSDT")
    pages = list(src.fetch("2026-01-05"))
    assert len(pages) == 1440 // 1000 + 1

def test_page_rows_are_transposed_into_kline_column_order():
    src = BinanceSource(session=FakeSession(), ticker="BTCUSDT")
    name, payload = next(iter(src.fetch("2026-01-05")))
    row = json.loads(payload)[0]
    assert row[0] == "open_time" and row[1] == "BTCUSDT"

def test_no_page_overlap_or_gap_across_the_boundary():
    # consecutive pages must not repeat the final minute of the previous page
    src = BinanceSource(session=FakeSession(), ticker="BTCUSDT")
    pages = [json.loads(p) for _, p in src.fetch("2026-01-05")]
    assert pages[0][-1][2] != pages[1][0][2]

def test_empty_day_returns_no_pages():
    assert list(BinanceSource(session=EmptySession()).fetch("2026-01-05")) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_sources_binance.py -v`
Expected: FAIL with `ImportError: cannot import name 'BinanceSource'`

- [ ] **Step 3: Implement `binance.py`**

Paginate `/api/v3/klines` with `limit=1000`, walking forward from the unit's start millisecond until past its end. Emit one raw payload tuple per page so landing is per-page and a mid-day crash loses at most one page. Transpose Binance's positional array into labelled rows matching the `binance` contract from Task 2.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_sources_binance.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/de_lake/sources/binance.py tests/fixtures/binance_klines_page.json tests/test_sources_binance.py
git commit -m "feat: Binance OHLCV source with per-page landing"
```

---

## Task 8: Stooq CSV source

**Files:**
- Create: `src/de_lake/sources/stooq.py`, `tests/fixtures/stooq_daily.csv`, `tests/test_sources_stooq.py`
- Test: `tests/test_sources_stooq.py`

**Interfaces:**
- Consumes: `RetryableSourceError` (T6).
- Produces:
  ```python
  class StooqSource:
      name = "stooq"
      def __init__(self, session, ticker: str = "aapl.us"): ...
      def units(self, start: date, end: date) -> list[str]: ...
      def fetch(self, unit: str) -> Iterator[tuple[str, bytes]]: ...
  ```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_sources_stooq.py
def test_fetch_returns_the_csv_verbatim(tmp_path):
    src = StooqSource(session=FakeSession(), ticker="aapl.us")
    name, payload = next(iter(src.fetch("2026-01-05")))
    assert payload.startswith(b"Date,Open,High,Low,Close,Volume")

def test_a_stooq_exhausted_ticker_yields_no_rows_not_an_error():
    # Stooq answers an unknown ticker with the literal body "No data"
    assert list(StooqSource(session=NoDataSession()).fetch("2026-01-05")) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_sources_stooq.py -v`
Expected: FAIL with `ImportError: cannot import name 'StooqSource'`

- [ ] **Step 3: Implement `stooq.py`**

Stooq serves a whole history per ticker as CSV, so `fetch` yields the raw body unchanged and lets `curate` do the parsing — that keeps Stooq as the honest control case against the two JSON sources. Detect the literal `No data` body and yield nothing.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_sources_stooq.py -v`
Expected: 2 passed

- [ ] **Step 5: Run the whole suite and commit**

Run: `uv run pytest -v` — Expected: all passed

```bash
git add src/de_lake/sources/stooq.py tests/fixtures/stooq_daily.csv tests/test_sources_stooq.py
git commit -m "feat: Stooq CSV source as the non-JSON control case"
```

---

## Task 9: Airflow DAG and lake browser, then documentation

**Files:**
- Create: `dags/ingest_market_data.py`, `src/de_lake/cli.py`, `tests/test_cli.py`, `README.md`, `docs/superpowers/specs/2026-10-02-p1-storage-abstraction.md`, `docs/adr/0001-pluggable-storage-backends.md`, `docs/adr/0002-parquet-over-a-warehouse-for-the-lake.md`, `docs/adr/0003-partition-by-source-and-date.md`

**Interfaces:**
- Consumes: all of Tasks 3–8.
- Produces: `python -m de_lake.cli partitions` and `python -m de_lake.cli show <key>`; Airflow DAG `ingest_market_data` with one task per source.

- [ ] **Step 1: Write the failing CLI tests**

```python
# tests/test_cli.py
def test_partitions_lists_key_count_and_bytes(tmp_path, capsys):
    store = FilesystemStore(tmp_path)
    store.atomic_write("curated/binance/date=2026-01-05/part-a.parquet", b"x" * 100)
    main(["partitions", "--store-root", str(tmp_path)])
    assert "curated/binance/date=2026-01-05" in capsys.readouterr().out
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'de_lake.cli'`

- [ ] **Step 3: Implement `cli.py`**

`argparse` with `partitions` (group keys by partition prefix, print key count and total bytes) and `show <key>` (print schema and row count of a Parquet key). Add `[project.scripts] de-lake = "de_lake.cli:main"`.

- [ ] **Step 4: Run the CLI tests to verify they pass**

Run: `uv run pytest tests/test_cli.py -v`
Expected: 1 passed

- [ ] **Step 5: Write the Airflow DAG**

One `PythonOperator` per source. Each passes `start_date`/`end_date` from a DAG-level `catchup=False` schedule, and returns early when `Checkpoint.is_done(unit)` is true for every unit in range. Set `retries=2` with `retry_delay=timedelta(minutes=5)` so a 429 backs off rather than hammering SEC.

- [ ] **Step 6: Verify the DAG imports**

Run: `uv run python -c "import ast,sys; ast.parse(open('dags/ingest_market_data.py').read())"`
Expected: no output, exit 0

- [ ] **Step 7: Write the two ADRs**

`0001-pluggable-storage-backends.md` records that the lake targets an `ObjectStore` protocol with a filesystem default, and why: Docker Hub is unreachable on this machine and Cloudflare R2 requires a payment method, so a hard dependency on either would make the project unrunnable and the zero-cost claim untrue. `0002-parquet-over-a-warehouse-for-the-lake.md` records why the lake is partitioned Parquet and the warehouse arrives in P2.

`0003-partition-by-source-and-date.md` records the partition key choice and — this is the point of writing it — its failure mode. `curated/{source}/date={YYYY-MM-DD}/` is chosen because every source in v1 is time-series and a date range is the only predicate anyone actually queries. It breaks in two named ways: if the load is backfilled one source at a time across three years, each date partition holds a file small enough that a query fans out to thousands of tiny reads; and SEC filings for a given day vary wildly in size, so partition sizes are skewed. State the thresholds at which you would switch to month-level or ticker-level partitioning, and the compaction step that would come first.

- [ ] **Step 8: Write the README**

Sections: what it does, architecture diagram in mermaid, quickstart, the three sources with their real shapes, the Review Focus list as "known limits", and an explicit honest-limitations section.

- [ ] **Step 9: Run the full suite and commit**

Run: `uv run pytest -v` — Expected: all passed

```bash
git add dags src/de_lake/cli.py tests/test_cli.py README.md docs
git commit -m "feat: Airflow DAG, lake browser CLI, ADRs, and README"
```

---

## Post-Implementation Verification

Run after Task 9 and before claiming the project works:

```bash
cd /home/zeroij/de_portfolio/p1-market-data-lake
uv run pytest -v                                    # every test green
uv run de-lake partitions                           # lake browser responds
git log --oneline                                   # one commit per task, 9 total
git status --porcelain                              # empty
```

Then re-run the full suite a second time and confirm the landing keys are unchanged — that is the idempotency claim, proven rather than asserted.

Finally, exercise the resume path against the live sources, which is the project's headline claim and cannot be proven by unit tests alone:

1. Run the EDGAR task for a three-month window.
2. Kill the process mid-run with `SIGKILL` — not a graceful shutdown, so the checkpoint write is genuinely interrupted.
3. Re-run the same window. It must resume from the last completed unit and produce a row count identical to an uninterrupted run.

Record the observed row counts and the wall-clock time in the README. Do this once by hand; a number nobody reproduced is not evidence.