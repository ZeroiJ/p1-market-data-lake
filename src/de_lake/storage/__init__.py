"""Storage backends for the lake."""

from __future__ import annotations

from de_lake.storage.base import (
    ObjectStore,
    StoreError,
    StoreKeyNotFound,
    WriteResult,
)
from de_lake.storage.filesystem import FilesystemStore
from de_lake.storage.s3 import S3Store


def get_store(bucket: str | None = None) -> ObjectStore:
    """Return the configured store.

    Filesystem by default. Setting `DE_LAKE_S3_BUCKET` switches to the S3
    backend, which is the only supported way to point the lake at Cloudflare R2
    or any other S3-compatible service.
    """
    from de_lake import settings

    bucket = bucket or settings.S3_BUCKET
    if not bucket:
        return FilesystemStore(settings.LAKE_ROOT)

    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
    )
    return S3Store(bucket, client)


__all__ = [
    "FilesystemStore",
    "ObjectStore",
    "S3Store",
    "StoreError",
    "StoreKeyNotFound",
    "WriteResult",
    "get_store",
]
