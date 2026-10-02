"""Local filesystem implementation of `ObjectStore`.

This is the default sink. It exists so the entire pipeline can be developed,
tested and demonstrated with no cloud account, no API key and no cost — which
is the entire point of the project's zero-cost constraint.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

from de_lake.storage.base import StoreKeyNotFound, WriteResult

_TMP_SUFFIX = ".tmp"


class FilesystemStore:
    """Stores objects as files under a single root directory.

    Keys are relative paths using forward slashes. There is no notion of a real
    bucket, so `key=""` is rejected rather than silently resolving to the root.
    """

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        if not key or key.startswith("/"):
            raise ValueError(f"invalid key: {key!r}")
        return self._root / key

    def atomic_write(self, key: str, data: bytes) -> WriteResult:
        target = self._path(key)

        if target.exists() and target.read_bytes() == data:
            return WriteResult(key=key, bytes_written=len(data), created=False)

        target.parent.mkdir(parents=True, exist_ok=True)

        # Unique temporary name so two writers racing on one key cannot
        # clobber each other's partial file.
        tmp = target.with_name(f"{target.name}.{uuid.uuid4().hex}{_TMP_SUFFIX}")
        try:
            with open(tmp, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                tmp.unlink()

        return WriteResult(key=key, bytes_written=len(data), created=True)

    def read(self, key: str) -> bytes:
        path = self._path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise StoreKeyNotFound(key) from exc

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list_keys(self, prefix: str) -> Iterator[str]:
        for path in sorted(self._root.rglob("*")):
            if not path.is_file() or path.name.endswith(_TMP_SUFFIX):
                continue
            key = path.relative_to(self._root).as_posix()
            if key.startswith(prefix):
                yield key

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def local_path(self, key: str) -> Path:
        """Filesystem path for a key.

        Only meaningful for this backend — the S3 backend has no local path.
        Exists so tests and local tooling can read a Parquet object directly
        without reaching into `_root`.
        """
        return self._path(key)

    def purge_temp_files(self) -> int:
        """Delete orphaned `*.tmp` files and return how many were removed.

        `atomic_write` renames rather than copies, so a hard kill between the
        temp write and the `os.replace` leaves a `.tmp` behind. `list_keys`
        already hides these, so the pipeline behaves correctly without this —
        they just occupy space forever. Repeated crashes therefore accumulate
        orphans, and on a 10 GB free-tier bucket that eventually matters.

        Safe because a live temp file belongs to an in-flight write on this same
        path; any temp file found at startup is by definition abandoned.

        S3Store has no equivalent: a single `put_object` creates no temp file.
        """
        removed = 0
        for path in self._root.rglob(f"*{_TMP_SUFFIX}"):
            if path.is_file():
                path.unlink(missing_ok=True)
                removed += 1
        return removed
