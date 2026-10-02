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