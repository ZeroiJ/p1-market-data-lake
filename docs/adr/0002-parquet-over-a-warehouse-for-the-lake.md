# ADR-0002: The lake is partitioned Parquet, not a warehouse

**Status** Accepted · **Date** 2026-10-02

## Context

P1 needs somewhere to put raw payloads and typed, queryable tables. Two shapes
were available: a relational warehouse (Postgres), or files.

A warehouse gives transactions, a query language everyone knows, and a single
place where data lives. It also means the lake and the mart are the same thing,
so "raw immutable payloads" and "cleaned queryable rows" have to share a schema
and a lifecycle.

## Decision

The lake is **Apache Parquet files in an object store**, partitioned by source
and date. Postgres arrives in **P2**, not P1, as the warehouse the dbt models
materialise into.

## Rationale

The landing zone's job is to hold exactly what the upstream sent, immutably, so
that a bad transform can always be re-run against the original. Parquet over an
object store does that natively, and it is the shape every real lakehouse uses —
so the design is worth explaining in an interview on its own merits.

Deferring the warehouse also keeps P1 honest. A warehouse in P1 would mean P1
never demonstrates resumable file-level ingestion, which is the whole point.

## Alternatives considered

- **Postgres as the lake.** Rejected: overwriting raw payloads on re-ingest
  destroys the immutability guarantee that makes re-runs safe, and it conflates
  the landing zone with the mart.
- **SQLite.** Rejected: concurrent writers are a poor fit for parallel ingestion
  tasks, and DuckDB already covers the embedded-analytics case.
- **Polars' lazy scan without writing files.** Rejected: nothing durable lands,
  so resume has nothing to resume from.

## Consequences

- Landing and curation are both file operations, so both get atomicity from
  `os.replace` / `put_object` rather than from transaction semantics.
- Querying Parquet during development goes through DuckDB (P2) rather than a
  server. See ADR-0004 when that lands.
- Two storage technologies exist — files and, from P2, Postgres. The dbt
  project in P2 is where that boundary is made explicit and tested.