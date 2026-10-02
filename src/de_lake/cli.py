"""`de-lake` — a browser for the lake.

Exists so the project can be demonstrated without a terminal. The portfolio's
whole premise is that a recruiter should be able to *see* the pipeline work
rather than read about it, and "clone it and run pytest" is not that.

Two subcommands:

    de-lake partitions      every partition, its key count, its bytes
    de-lake show <key>      schema and row count of one Parquet object

Both take `--store-root`, so the lake can be inspected without any cloud
account configured. The default comes from `settings.LAKE_ROOT`.
"""

from __future__ import annotations

import argparse
import io
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

from de_lake import settings
from de_lake.storage.base import StoreKeyNotFound
from de_lake.storage.filesystem import FilesystemStore


def _store(args: argparse.Namespace) -> FilesystemStore:
    return FilesystemStore(Path(args.store_root) if args.store_root else settings.LAKE_ROOT)


def _partition_of(key: str) -> str:
    """Everything up to and including the final `date=` component."""
    marker = "date="
    if marker not in key:
        return key
    tail = key.split(marker, 1)[1]
    return key[: key.rindex(marker)] + marker + tail.split("/", 1)[0]


def _cmd_partitions(args: argparse.Namespace) -> int:
    store = _store(args)
    groups: dict[str, list[tuple[str, int]]] = defaultdict(list)

    for key in store.list_keys(args.prefix):
        groups[_partition_of(key)].append((key, len(store.read(key))))

    if not groups:
        print("no objects in the lake (yet)")
        return 0

    width = max(len(p) for p in groups)
    print(f"{'PARTITION'.ljust(width)}  {'KEYS':>5}  {'BYTES':>12}")
    for partition in sorted(groups):
        entries = groups[partition]
        print(
            f"{partition.ljust(width)}  {len(entries):>5}  "
            f"{sum(size for _, size in entries):>12,}"
        )
    total = sum(size for entries in groups.values() for _, size in entries)
    print(f"\n{len(groups)} partitions, {sum(len(e) for e in groups.values())} keys, {total:,} bytes")
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        payload = store.read(args.key)
    except StoreKeyNotFound:
        print(f"key not found: {args.key}", file=sys.stderr)
        return 1

    if not args.key.endswith(".parquet"):
        print(f"{args.key} is not parquet ({len(payload):,} bytes)", file=sys.stderr)
        return 1

    frame = pl.read_parquet(io.BytesIO(payload))
    print(f"key    {args.key}")
    print(f"rows   {frame.height:,}")
    print(f"cols   {frame.width}")
    print("schema")
    for name, dtype in frame.schema.items():
        print(f"  {name:<16} {dtype}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="de-lake", description="Browse the market data lake.")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(sp):
        sp.add_argument("--store-root", default=None, help="lake root (default: $DE_LAKE_ROOT)")

    partitions = sub.add_parser("partitions", help="list partitions with key and byte counts")
    partitions.add_argument("--prefix", default="", help="only list keys under this prefix")
    add_common(partitions)
    partitions.set_defaults(func=_cmd_partitions)

    show = sub.add_parser("show", help="show schema and row count of one parquet key")
    show.add_argument("key")
    add_common(show)
    show.set_defaults(func=_cmd_show)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point. Always returns an exit code rather than raising SystemExit,
    so it is callable from tests and from other Python without a try/except."""
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())