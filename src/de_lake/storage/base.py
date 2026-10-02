"""The storage abstraction the whole lake is written against.

Atomicity is a property of the *backend*, not of the caller. Every write in
this project goes through `ObjectStore.atomic_write`, which guarantees that a
reader either sees the previous content or the new content, never a partial
file. Two very different mechanisms satisfy that promise:

- `FilesystemStore` writes to a temporary name, fsyncs, then `os.replace`s.
- `S3Store` issues a single `put_object`. S3 has no rename at all, but a
  whole-object PUT is already atomic, so adding a temporary key there would
  buy nothing and cost an extra Class A operation.

Callers therefore never think about `.tmp` files, and never have to be
rewritten when the sink changes.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class StoreError(Exception):
    """Base class for storage failures."""


class StoreKeyNotFound(StoreError, KeyError):
    """Raised when a key is read that does not exist."""


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of a single atomic write.

    `created` is False when the key already held byte-identical content. That
    distinction is how idempotency is observed: re-running a pipeline over
    unchanged data reports `created=False` and leaves the lake untouched.
    """

    key: str
    bytes_written: int
    created: bool


@runtime_checkable
class ObjectStore(Protocol):
    """Key/value object storage with atomic whole-object writes."""

    def atomic_write(self, key: str, data: bytes) -> WriteResult:
        """Write `data` to `key` atomically. Returns whether anything changed."""
        ...

    def read(self, key: str) -> bytes:
        """Return the full contents of `key`. Raises `StoreKeyNotFound`."""
        ...

    def exists(self, key: str) -> bool:
        """Return whether `key` is present."""
        ...

    def list_keys(self, prefix: str) -> Iterator[str]:
        """Yield keys beginning with `prefix`, in sorted order."""
        ...

    def delete(self, key: str) -> None:
        """Remove `key`. Deleting a missing key is not an error."""
        ...
