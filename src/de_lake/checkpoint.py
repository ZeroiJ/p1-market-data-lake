"""Checkpointing: the mechanism that makes a long backfill resumable.

Restarting where you left off is not the same as restarting from the last
successful commit. The naive version — append to a file as you go — can leave a
half-written record after a hard kill, and the next run then trusts a corrupt
state and silently skips work that never happened.

Two decisions fix that:

1. `save` goes through `atomic_write`, so a checkpoint is replaced whole or not
   at all. There is no window in which a reader can observe a partial file.

2. `load` treats an unreadable checkpoint as *empty* rather than raising. This is
   deliberate and asymmetric on purpose: a lost checkpoint costs re-work, while
   a raised exception costs the entire DAG run. Losing progress is always the
   cheaper failure.

The checkpoint is stored in the same object store as the data, so it travels
with the lake rather than depending on local disk that a container restart can
destroy.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from de_lake.storage.base import ObjectStore, StoreKeyNotFound

CHECKPOINT_PREFIX = "_checkpoints"


def checkpoint_key(source: str, unit: str) -> str:
    return f"{CHECKPOINT_PREFIX}/{source}/{unit}.json"


@dataclass
class Checkpoint:
    """Which units of work for one source are already complete.

    Mutable and saved after every completed unit, so the on-disk state is
    always at most one unit stale.
    """

    source: str
    unit: str
    completed: list[str] = field(default_factory=list)

    def is_done(self, unit: str) -> bool:
        return unit in self.completed

    def mark_done(self, unit: str) -> None:
        """Record `unit` as complete. Marking twice is a no-op, not a duplicate."""
        if unit not in self.completed:
            self.completed.append(unit)

    def save(self, store: ObjectStore) -> None:
        payload = json.dumps(
            {"source": self.source, "unit": self.unit, "completed": self.completed},
            sort_keys=True,
        ).encode()
        store.atomic_write(checkpoint_key(self.source, self.unit), payload)

    @classmethod
    def load(cls, store: ObjectStore, source: str, unit: str) -> Checkpoint:
        """Return the stored checkpoint, or a fresh empty one.

        A missing or corrupt checkpoint yields an empty checkpoint rather than an
        exception — see the module docstring for why that asymmetry is correct.
        """
        try:
            payload = json.loads(store.read(checkpoint_key(source, unit)))
        except StoreKeyNotFound, json.JSONDecodeError, UnicodeDecodeError:
            return cls(source=source, unit=unit)
        return cls(
            source=payload.get("source", source),
            unit=payload.get("unit", unit),
            completed=list(payload.get("completed", [])),
        )
