# ADR-0003: Partition by source and date

**Status** Accepted · **Date** 2026-10-02

## Context

Partitioning is the single decision in a data lake that is cheap to get right
early and expensive to change later. Every read either touches a small number of
partitions or scans everything.

The layout chosen is:

```
curated/{source}/date={YYYY-MM-DD}/part-{content_hash}.parquet
```

## Decision

Partition by **source and date**. The partition file is named for the sha256 of
its own row content.

## Rationale

Every source in v1 is a time series, and the only predicate anyone actually
queries is a date range. Date partitioning turns "give me January" into a
directory listing instead of a full scan.

Content-hash filenames are the other half. Re-curating unchanged data rewrites
the *identical* key, and `atomic_write` reports `created=False`. Idempotency is
therefore a property of the naming rather than of bookkeeping — there is no
manifest to keep in sync, because there is nothing to get out of sync.

The two-character fanout from landing carries over to nothing here, since date
partitions are already few.

## Where this breaks — two named ways

**1. Tiny partitions during backfill.** Loading three years of one source, one
file at a time, produces up to ~1,100 date partitions. If the load is per-ticker
or per-CIK, each partition ends up holding a handful of rows, and a
three-month query fans out to dozens of small reads.

*Mitigation, in order:* compaction (rewrite a date range into one file) before
changing the partition key. Compaction is local to a partition and reversible.

**2. Skewed partition sizes.** SEC filing counts per day vary by orders of
magnitude — a quiet Tuesday versus a day with several 8-Ks. One date partition
can be a hundred times the size of its neighbours, so a query over a "normal"
week is dominated by whichever day happens to be large.

*Mitigation:* Hive-style bucketing inside each date partition by a stable key
(e.g. `ticker`), or sub-hour partitions for the specific dates that skew.

## Thresholds for revisiting

| Signal | Threshold | Move to |
| --- | --- | --- |
| Median rows per date partition | < 10,000 | month-level partitioning |
| Median rows per date partition | > 50 million | day + ticker bucketing |
| P99 / P50 partition size ratio | > 100 | sub-day partitions, or bucket |
| Query planning time dominates runtime | any | compaction pass |

These are heuristics, not laws. The right move is to measure against real
partitions — which is what `de-lake partitions` exists for.

## Alternatives considered

- **Partition by ticker.** Rejected as the primary key: OHLCV and filings are
  queried by date far more often than by ticker, and ticker cardinality creates
  many small partitions.
- **Partition by ingestion timestamp.** Rejected: it fragments the same business
  date across ingest days, so a query for "January data" touches every
  partition.
- **Hive-style `bucket=N`.** Deferred rather than rejected. It composes with the
  current layout and is the natural answer to the skew problem above, but it is
  not needed until the skew is measured.