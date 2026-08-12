# Wayfinder map: garret design spec

Label: wayfinder:map

## Destination

A full design spec in this repo — every architectural decision locked and written
down (architecture docs, ADRs, glossary), ready to hand to implementation
sessions. Implementation itself is a separate effort, out of this map's scope.

**DESTINATION REACHED (2026-08-05).** The spec lives at `docs/spec/`
(index: `docs/spec/00-overview.md`) with ADRs in `docs/adr/`. All 17
tickets resolved; no open tickets remain.

Garret is a from-scratch, single-tenant Nix binary cache replacing attic for
Aidan's infrastructure: a **Pusher** service (OIDC-protected, accepts NARs via a
custom high-throughput protocol) and a **Puller** service (public, standard Nix
substituter protocol), colocated on one host sharing SQLite + S3.

## Notes

- Domain: Nix binary caching. Prior art lives at `/Users/aidanp/Projects/attic`
  — especially `OPTIMIZATIONS.md` (throughput levers), `OPTIMIZATION_PLAN.md`
  (OOM history and fixes, all landed), and `CONTEXT.md` (attic-era glossary).
  Consult these before resolving any performance-adjacent ticket.
- Skills every session should consult: `/grilling` + `/domain-modeling` for
  grilling tickets; `/research` for research tickets.
- Standing preferences: max throughput, parallel uploads, minimum protocol
  round-trips, bounded memory under load, extensive Prometheus metrics
  (VictoriaMetrics consumer).
- Research findings are written to `.scratch/spec/research/<slug>.md` and linked
  from their ticket (no branches — repo has no initial commit yet).
- The spec-assembly ticket mints the ADRs; charting decisions below that meet
  the ADR bar (SQLite+same-host topology, multi-issuer OIDC, chunking outcome)
  get ADRs there.

## Decisions so far

Charting-session decisions (no ticket — resolved while drawing the map):

- Destination — full design spec; implementation is a separate effort.
- Storage backend — S3-compatible (Garage/MinIO); no local-filesystem backend.
- Metadata store — SQLite in WAL mode.
- Language — Rust; HTTP framework deliberately reopened (research ticket).
- Topology — Pusher and Puller on the same host sharing the SQLite file and S3
  bucket; the service split is about exposure/auth, not placement.
- Auth — multi-issuer JWT validation at the Pusher: Pocket ID and GitHub
  Actions OIDC trusted directly via their JWKS; no token-exchange service.
- Features retained — store watcher, cache listing, configurable multi-threaded
  push, Prometheus metrics, garbage collection, admin CLI, NixOS modules +
  flake, dependency-tree browse.
- Benchmark target — N concurrent pushers with flat/bounded memory and no
  timeouts (graceful-under-load, not link-saturation).
- Client shape — Puller speaks the standard substituter protocol (server-side
  ed25519 narinfo signing, works from any nix.conf); Pusher gets a
  purpose-built garret CLI (push, watch-store, list).
- GC policy — storage quota + LRU eviction ("store everything" bounded by a
  size budget).
- Migration — none; garret starts empty, attic retires after garret is proven.
- Deployment — NixOS host, native systemd services via the NixOS module; no
  container packaging.

<!-- one line per closed ticket: title link + one-line gist -->

- [HTTP framework choice](issues/01-http-framework.md) — axum 0.8 on hyper 1.x
  (fallback: raw hyper + tower); throughput hinges on HTTP/2 flow-control
  tuning, not framework choice.
- [Chunk-dedup state of the art](issues/02-chunking-state-of-the-art.md) —
  chunking is a storage win (6.55× vs 2.69× at nixbuild.net), not a
  push-throughput win; leaning: whole-NAR baseline unless ticket 03's
  measurements justify the read-path and metadata costs.
- [Pocket ID + GitHub OIDC capabilities](issues/04-pocket-id-oidc.md) — all
  three caller types work: device flow (CLI), client_credentials (watcher
  daemon), GitHub OIDC with 5-min TTL (CI must re-mint or exchange);
  audience via RFC 8707; pin Pocket ID past CVE-2026-43983.
- [Store-path detection mechanisms](issues/12-store-watcher-mechanisms.md) —
  hybrid recommended: persisted-cursor poll of the Nix DB (ValidPaths.id is
  monotonic — complete, with free catch-up/backlog) plus inotify lock-file
  events as the latency wakeup.
- [Measure dedup ratio on real closures](issues/03-dedup-measurement.md) —
  20 real system generations: chunking cuts rebuild-churn storage
  1.6–1.9× vs whole-NAR zstd, but only ~8% end-to-end on the actual store
  (model weights dominate); whole-NAR-hash dedup is worthless (1.009×);
  if chunking, use ≥1 MiB average chunks, not attic's 64 KiB.
- [Chunking model decision](issues/05-chunking-decision.md) — whole-NAR:
  one zstd-compressed NAR per object in S3, keyed by store path hash; no
  chunking, no content dedup, no refcounting; presigned redirects always
  possible; path-level negotiation only.
- [Push protocol design](issues/06-push-protocol.md) — one batch
  missing-paths round-trip, then parallel self-contained PUTs (JSON
  preamble + zstd stream); client compresses, server trusts NarHash;
  429-based backpressure with global byte/upload caps; fully idempotent;
  no resume in v1.
- [SQLite schema and concurrency](issues/07-db-schema.md) — Pusher owns
  writes; Puller reads plus >24h-debounced async last-accessed bumps;
  upload state in memory only with the row-exists⇒blob-exists invariant
  (no pending rows); normalized refs table for dependency trees.
- [GC: quota + LRU design](issues/11-gc-design.md) — root-first
  closure-safe eviction (only unreferenced objects evictable, LRU order,
  loop until low watermark); 95%/85% watermarks on a maintained counter;
  runs inside the Pusher; row-then-blob deletes with a weekly orphan
  sweep.
- [Auth flows and claims policy](issues/09-auth-flows.md) — device flow
  (CLI), per-machine client_credentials (watcher), per-request re-mint
  (CI); Pocket ID authz = audience only (groups managed in Pocket ID);
  GitHub authz = owner_id match; browse requires OIDC, pull anonymous;
  no auth-disable flag.
- [S3 storage layout](issues/08-storage-layout.md) — Puller proxies bytes
  (Garage stays internal; 256 KiB buffers, Range passthrough); flat
  `nar/<hash>.nar.zst` keys; multipart above 100 MiB with 64 MiB parts,
  ≤4 in flight, permit-before-read; cleanup via GC sweeps, no lifecycle
  rules.
- [Narinfo signing and key management](issues/10-signing-keys.md) — sign
  on write (sigs stored; Puller does zero crypto); multi-key overlap
  rotation with `garret-admin resign`; nix-format keys as secret files,
  never in the nix store.
- [Garret client CLI design](issues/13-client-cli.md) — five commands
  (login/push/watch-store/list/tree) + separate garret-admin; watcher is
  a root service on a ValidPaths.id cursor with inotify wakeup, bare-path
  pushes, .drv+upstream-signed filtering, capped-retry skip-list, MAX(id)
  bootstrap.
- [Listing and browse API](issues/14-browse-api.md) — hosted on the
  Puller with OIDC on browse routes only; four JSON endpoints (search,
  detail, tree, referrers) with keyset pagination, all served by ticket
  07's indices.
- [Metrics and observability](issues/15-metrics-observability.md) —
  dedicated internal metrics ports (9091/9092); bounded labels only,
  never per-object; full per-subsystem catalog in the ticket; structured
  logs via tracing (JSON option), no OTLP in v1.
- [Benchmark harness](issues/16-benchmark-harness.md) — seeded synthetic
  corpus from the measured distribution; garret-bench reuses the client
  library; 20 pushers with cap-relative memory pass/fail + large-body and
  pull scenarios; nix-provisioned local Garage; JSON baselines, no CI
  gating.
- [Assemble the design spec](issues/17-assemble-spec.md) — spec written
  to docs/spec/ (11 documents) + 4 ADRs; assembly decided the crate
  layout, unix-socket admin API, and NixOS module surface. Destination
  reached.

## Not yet specified

(Emptied at spec assembly — admin CLI, NixOS modules, and crate layout
were decided in ticket 17. One item graduated out of this effort:)

- Rate limiting / abuse posture for the public Puller endpoint — never
  sharpened into a ticket; nothing in the spec precludes fronting the
  Puller with standard reverse-proxy rate limiting. Revisit during
  implementation or operation if abuse appears.

## Out of scope

- Multi-cache / multi-tenancy — garret is single-cache by charter.
- Attic protocol or token compatibility — clean break.
- Data migration from the attic deployment — decided: start empty.
- Container / k8s packaging — deployment is a native NixOS host.
- Custom pull-side client or protocol — the Puller stays a standard substituter.
- Implementing garret — beyond this map's destination (the spec).
- post-build-hook socket ingestion (cachix-daemon pattern) — ruled out of
  v1 by the client CLI ticket; the NixOS fleet's cursor watcher covers all
  ingestion. Revisit only if non-NixOS machines join the fleet.

## Post-v1 review (2026-08)

A codebase review after M5: features surveyed across attic, harmonia,
nix-serve-ng, ncps, cachix, FlakeHub/magic-nix-cache, tvix/snix,
bazel-remote and sccache; the spec-09 bench harness completed (all three
scenarios, micro-benches, baseline tracking) and a first baseline
measured. Findings and proposals:

- Research: [similar-projects-survey](research/similar-projects-survey.md),
  [benchmark-baseline-2026-08](research/benchmark-baseline-2026-08.md)
- Tickets: [19 daemon push](issues/19-daemon-push.md) (reopens the
  post-build-hook out-of-scope item above with cachix/MNC evidence),
  [20 sha256 acceleration](issues/20-sha256-hardware-acceleration.md)
  (measured 5.2x), [21 upstream filter](issues/21-upstream-filter.md),
  [22 closure pinning](issues/22-closure-pinning.md),
  [23 fsck](issues/23-fsck.md), [24 doctor](issues/24-doctor.md),
  [25 pull-path budgets](issues/25-pull-path-budgets.md)
- Non-goals otherwise revalidated: chunking/dedup, custom pull protocol,
  upload resume, OTLP, multi-tenancy, containers.
