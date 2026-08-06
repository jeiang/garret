# Garret — Design Overview

Garret is a single-tenant Nix binary cache: attic's successor for this
infrastructure, designed for maximum push throughput, minimal protocol
round-trips, and provably bounded memory under load. It maintains no
compatibility with attic's protocol or tokens.

Every decision below was resolved on the wayfinder map
(`.scratch/spec/map.md`); each section links its ticket, which carries the
full rationale and rejected alternatives.

## Components

| Component | Role | Exposure |
|---|---|---|
| **Pusher** | Accepts NARs over the garret push protocol; owns all DB writes; runs GC | OIDC-protected (Pocket ID + GitHub Actions); not public |
| **Puller** | Standard Nix substituter (narinfo/NAR) + browse API | Public; browse routes require Pocket ID OIDC |
| **garret** (CLI) | `login`, `push`, `watch-store`, `list`, `tree` | Runs on build machines / anywhere |
| **garret-admin** (CLI) | Key management, resign, GC trigger, stats | Local to the server host |
| **garret-bench** | Load harness (see [09-benchmarks.md](09-benchmarks.md)) | Dev/ops tool |

## Topology

Pusher and Puller are separate processes **colocated on one NixOS host**,
sharing a SQLite database file (WAL mode) and one S3-compatible bucket
(Garage). The service split is about exposure and auth, not placement.
([map: charting decisions](../../.scratch/spec/map.md))

```
build machines ── push protocol (OIDC) ──▶ Pusher ──┐
                                                     ├── SQLite (one file, WAL)
any machine ──── substituter protocol ───▶ Puller ──┤
                       (anonymous)                   └── Garage (S3), internal
```

## Storage model

One zstd-compressed NAR per object, stored as a single S3 blob keyed by
store path hash. **No chunking, no content dedup, no refcounting.**
Dedup happens at path granularity via push negotiation only.
([ticket 05](../../.scratch/spec/issues/05-chunking-decision.md),
[ADR-0002](../adr/0002-whole-nar-storage.md))

## Stack

Rust; axum 0.8 on hyper 1.x with tower middleware. Throughput engineering
concentrates on HTTP/2 flow-control tuning and streaming discipline, not
framework choice. ([ticket 01](../../.scratch/spec/issues/01-http-framework.md),
[ADR-0004](../adr/0004-axum-on-hyper.md))

## Spec index

- [01-push-protocol.md](01-push-protocol.md) — negotiation, upload, backpressure
- [02-database.md](02-database.md) — schema, concurrency discipline, pragmas
- [03-storage.md](03-storage.md) — S3 layout, multipart, read path
- [04-auth.md](04-auth.md) — OIDC flows, claims policy, validation
- [05-gc.md](05-gc.md) — quota + LRU eviction, sweeps
- [06-client.md](06-client.md) — CLI, store watcher
- [07-browse-api.md](07-browse-api.md) — listing/search/tree endpoints
- [08-observability.md](08-observability.md) — metrics catalog, logging
- [09-benchmarks.md](09-benchmarks.md) — harness, scenarios, pass/fail
- [10-packaging.md](10-packaging.md) — crate layout, NixOS modules, admin CLI
- [../adr/](../adr/) — architecture decision records

## Non-goals (v1)

Multi-cache/multi-tenancy; attic compatibility or data migration;
container/k8s packaging; custom pull protocol; upload resume;
post-build-hook socket ingestion; presigned-redirect downloads; OTLP
tracing. (See the map's Out-of-scope section for rationale.)
