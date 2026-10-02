# ADR-0001: Pluggable storage backends

**Status** Accepted · **Date** 2026-10-02

## Context

The lake needs object storage. Three candidate sinks were considered: the local
filesystem, MinIO, and Cloudflare R2. Each turned out to be unavailable or
qualified on this machine:

- **Docker Hub is unreachable**, so `minio/minio` cannot be pulled as a
  container.
- **`dl.min.io` returns HTTP 410** for MinIO release binaries, so MinIO cannot
  be installed as a single binary either.
- **Cloudflare R2 requires a payment method to enable.** The activation dialog
  asks the user to authorise charges for usage above the free tier. R2 is a
  metered service with a free allowance, not a cardless free plan.

The last point matters most. The project's stated constraint is zero cost, and a
design that quietly requires a credit card is not zero cost — it is a cost that
has not been incurred yet.

Committing to any single sink would have made the project unrunnable on this
machine, or would have quietly weakened the zero-cost claim.

## Decision

Define an `ObjectStore` protocol and implement it twice:

| Backend | Atomic write mechanism |
| --- | --- |
| `FilesystemStore` | write `key.tmp` → `fsync` → `os.replace` |
| `S3Store` | single `put_object` |

`FilesystemStore` is the default. Setting `DE_LAKE_S3_BUCKET` switches to the
S3 backend, which works unchanged against R2, MinIO, Garage or AWS S3 because
all four speak the S3 API.

**Atomicity lives in the backend, not the caller.** Callers call
`atomic_write` and never handle `.tmp` files. This is the part that mattered:
the first draft of the plan had callers doing tmp-then-rename, which is
unimplementable on S3 because S3 has no rename.

## Alternatives considered

- **Target R2 directly, from the start.** Best demo story, but requires a card
  and makes the whole project unrunnable without one. Rejected.
- **Target MinIO via Docker.** Correct in principle, but Docker Hub is blocked
  here. Rejected as unverifiable today; the S3 backend still works if the user
  later gets MinIO running.
- **Write to local disk paths with no abstraction.** Simplest, but then the
  claim "swapping the sink is a configuration change" is untestable, and
  therefore unproven.
- **`moto` as the production sink.** Rejected: moto is a test double. Using it
  as the real backend would make the portfolio dishonest.

## Consequences

- The project runs end-to-end with **no account, no key, and no card**, which is
  what makes the zero-cost claim true rather than aspirational.
- Switching to R2 later is four environment variables and zero code changes.
- S3 integration is tested for real against `moto`'s in-process AWS
  interception, so the backend is not shipped untested.
- The interface is slightly larger than a filesystem-only design would need.
  That cost is paid deliberately.
- A live-network edge case that only a real S3 endpoint would surface stays
  untested. Accepted: the boundary is the same `boto3` API either way.