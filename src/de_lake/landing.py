"""Landing: immutable, content-addressed capture of exactly what a source returned.

Landing is the project's foundation for idempotency. Because a payload's key is
derived from its own content hash, landing the same bytes twice is a no-op and
landing different bytes never collides. Nothing downstream needs to know whether
it is running for the first time or the hundredth.

The logical name — which ticker, which CIK, which date — is deliberately kept
*out* of the key and recorded in a sidecar instead. That is what makes the key
content-addressed rather than name-addressed, and it is why re-ingesting a
corrected upstream file produces a new object instead of silently overwriting
the old one.

Key layout:

    raw/{source}/{sha256[:2]}/{sha256}.bin
    raw/{source}/{sha256[:2]}/{sha256}.bin.meta

The two-character fanout keeps any single prefix directory small, which matters
on S3-compatible stores where a flat bucket with millions of keys performs badly.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from de_lake.storage.base import ObjectStore, WriteResult

LANDING_PREFIX = "raw"


def landing_key(source: str, logical_name: str, content_sha256: str) -> str:
    """Return the key for a payload's content hash.

    `logical_name` is accepted for symmetry with `land()` but deliberately not
    part of the key: the key identifies *content*, not origin.
    """
    return f"{LANDING_PREFIX}/{source}/{content_sha256[:2]}/{content_sha256}.bin"


def land(store: ObjectStore, source: str, logical_name: str, data: bytes) -> WriteResult:
    """Land a raw payload exactly as received.

    Writes the payload first, then its sidecar. That ordering matters: a crash
    between the two leaves an unreadable-but-harmless payload, whereas the
    reverse would leave a sidecar pointing at content that does not exist.
    """
    digest = hashlib.sha256(data).hexdigest()
    key = landing_key(source, logical_name, digest)

    result = store.atomic_write(key, data)

    sidecar = json.dumps(
        {
            "source": source,
            "logical_name": logical_name,
            "sha256": digest,
            "bytes": len(data),
            "landed_at": datetime.now(UTC).isoformat(),
        },
        sort_keys=True,
    ).encode()
    store.atomic_write(f"{key}.meta", sidecar)

    return result


def read_sidecar(store: ObjectStore, key: str) -> dict[str, str]:
    """Return the parsed `.meta` sidecar for a landed payload."""
    from de_lake.storage.base import StoreKeyNotFound

    try:
        return json.loads(store.read(f"{key}.meta"))
    except StoreKeyNotFound as exc:
        raise StoreKeyNotFound(f"{key}.meta") from exc