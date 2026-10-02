"""S3-compatible implementation of `ObjectStore`.

Written against the S3 API rather than a vendor SDK, so the same code targets
Cloudflare R2, MinIO, Garage, and AWS S3. Swapping the lake's sink is a
configuration change, never a code change.

Why this exists even though v1 runs on the filesystem: the project's zero-cost
promise is only credible if the storage boundary is genuinely swappable, and
that is impossible to demonstrate unless both sides are implemented and tested
against the same protocol.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from de_lake.storage.base import StoreKeyNotFound, WriteResult


def _is_missing(exc: Exception) -> bool:
    """True when a botocore error means 'no such key' rather than a real fault."""
    if isinstance(exc, StoreKeyNotFound):
        return True
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"NoSuchKey", "404", "NoSuchBucket"}


class S3Store:
    def __init__(self, bucket: str, client: Any) -> None:
        self._bucket = bucket
        self._client = client

    def atomic_write(self, key: str, data: bytes) -> WriteResult:
        try:
            existing = self._client.get_object(Bucket=self._bucket, Key=key)[
                "Body"
            ].read()
        except Exception as exc:
            if not _is_missing(exc):
                raise
            existing = None

        if existing == data:
            return WriteResult(key=key, bytes_written=len(data), created=False)

        # A single PUT is atomic. No temporary key: that would cost an extra
        # Class A operation and buy no additional guarantee.
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        return WriteResult(key=key, bytes_written=len(data), created=True)

    def read(self, key: str) -> bytes:
        try:
            return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()
        except Exception as exc:
            if _is_missing(exc):
                raise StoreKeyNotFound(key) from exc
            raise

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:
            if _is_missing(exc):
                return False
            raise
        return True

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)
