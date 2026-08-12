# Similar-projects survey: features worth stealing, non-goals re-examined

Date: 2026-08-12. Scope agreed in review session: Nix ecosystem servers
(attic, harmonia, nix-serve-ng, ncps), hosted caches and stores (cachix,
FlakeHub Cache + magic-nix-cache, tvix/snix-store), plus two adjacent build
caches mined for technique (bazel-remote, sccache). Docs-first, source
skimmed where docs were thin.

Scoring axes (agreed): **build/substitution speed**, **ops simplicity for a
single-host deployment**, **client/developer UX**. Storage cost is
deliberately **not** an axis, so nothing here re-litigates
[ADR-0002](../../docs/adr/0002-whole-nar-storage.md) from the dedup angle.
The v1 non-goals were open for renegotiation with evidence.

## What the survey validated about garret's design

- **302-to-presigned-S3 reads** (ADR-0005): attic must proxy and reassemble
  every NAR byte; bazel-remote's hot lesson (issue #280) is that blob serving
  eats the server. garret sidesteps the whole class. Presigned S3 also gives
  clients native Range/resume support for free — harmonia documents that its
  transparent compression *breaks* resume.
- **Whole-NAR zstd, no chunking**: attic and ncps both implement FastCDC, and
  every stated benefit is storage cost (excluded axis). ncps documents CDC as
  ingest CPU overhead and ships it off by default; nix-serve-ng — the fastest
  server in the only published cross-server benchmark — has none. Non-goal
  kept.
- **Standard substituter protocol for pulls**: all surveyed projects speak
  it; nobody invented a pull protocol. snix's "flavoured" protocol is the
  right template *if* this ever reopens: a backwards-compatible narinfo
  extension with mandatory graceful fallback, benefit gated on a smart
  client. Filed, not planned.
- **zstd over xz**: cachix's migration data (xz capped pushes at ~3–5 Mbit/s
  per core; zstd ~100 Mbit/s, ≤10% storage growth, ~10x faster decompression)
  retroactively validates garret's choice; `--zstd-level` already exposes the
  trade.
- **Prometheus + /healthz on an internal listener**: harmonia and ncps treat
  this as the ops baseline; garret already ships it (spec 08). Only gap: no
  ready-made Grafana dashboard JSON (harmonia bundles one).
- **Retry-After-aware client backoff with jitter**: cachix arrived here in
  v1.11.1; garret's client already does both (push.rs).
- **OIDC/JWT federation for CI pushes**: FlakeHub's entire auth pitch is
  "short-lived JWTs from trusted identity providers, no static keys" — which
  is exactly garret's GitHub Actions issuer support (ADR-0003).

## Candidate features (scored, consolidated)

Accepted → ticket; the rest recorded here with reasons.

| Feature (evidence) | Speed | Ops | UX | Verdict |
|---|---|---|---|---|
| Post-build-hook / local daemon push (cachix v1.7 daemon; magic-nix-cache; both converged independently) | high | med | high | **Ticket 19** — reopens the non-goal; strongest evidence in the survey ("tens of minutes → seconds" for CI pushes that overlap the build) |
| SHA-256 hardware acceleration (measured here, not surveyed) | high | high | — | **Ticket 20** — 5.2x measured on the Pusher's only per-byte cost |
| Skip pushing paths an upstream cache already serves (attic `--upstream-cache-key-name`; cachix "configurable upstreams", Dec 2025) | med | low | high | **Ticket 21** — pure client-side filter, no server change |
| Closure pinning, GC-exempt (ncps pin API walks references; cachix pins with keep-days/revisions) | low | med | high | **Ticket 22** — small table + eviction filter on garret's existing GC |
| fsck: DB↔S3 consistency audit/repair (ncps fsck check/repair/dry-run; attic FAQ concedes it cannot detect missing blobs) | low | high | low | **Ticket 23** — the recovery tool a single-host cache actually needs |
| `garret doctor` (cachix doctor; sccache startup `check()` that probes the backend and names the failure) | low | high | high | **Ticket 24** — kills the "why doesn't it work" loop for one-person infra |
| Degrade-to-miss + bounded budgets on the pull path (sccache's 60s lookup budget and `MissType::{TimedOut, CacheReadError}` counters; MNC's "never fail the build" rule) | med | high | high | **Ticket 25** — a slow cache must read as a miss, never an outage |
| `watch-exec` — push only what one command built (cachix) | med | high | high | Folded into ticket 19: it falls out of the post-build-hook mechanism |
| Stepped-concurrency pull scenario, keep-alive on/off (bazel-remote issue #280: mean latency 123ms → 2122ms at c=300) | — | — | — | Bench-harness note, recorded in the baseline research doc |
| Read-only / maintenance mode (sccache `*_RW_MODE`) | low | med | low | Noted in ticket 23 as a companion flag; not its own ticket |
| Grafana dashboard JSON shipped in-repo (harmonia) | low | med | low | Cheap; noted here, no ticket — bundle when the metrics settle |
| `.ls` listings for nix-index; streaming build logs; `/serve/` website mode (harmonia) | low | low | med | Deferred: niche until nix-locate or `nix log` against garret is actually wanted |
| Standard `nix copy --to http://` PUT ingestion (ncps) | low | low | med | Deferred: the garret CLI is the supported pusher; revisit if a client-less machine ever needs to push |
| Time-based retention (attic "keep 1y") | low | med | low | Skipped: LRU+quota already covers the need with fewer knobs |
| Pull-through proxy of upstreams with failover/re-signing (ncps) | med | negative | med | Skipped: a second product surface; ops-simplicity negative for one host |
| Client-side parallel multipart of a single NAR / upload resume (cachix v1.3; resume falls out of tracked parts) | med (WAN only) | med | low | Deferred with evidence noted: garret's server already multiparts to S3; the client-side variant only pays on WAN single-giant-NAR pushes. Revisit when real S4 runs show the giant blob dominating push time |
| Chunking/dedup (attic, ncps, snix; Replit −90% storage) | low | low | low | Skipped: storage-cost benefit, excluded axis; see "validated" above |
| CDN / edge routing (cachix, FlakeHub) | med | negative | low | Skipped: multi-region problem garret doesn't have; conflicts with presigned redirects |
| Deploy agent (cachix Deploy) | low | low | med | Skipped: scope creep beyond a cache |
| OTLP tracing (ncps, snix) | low | med | low | Non-goal kept: justified by their multi-process/fleet architectures; garret is two processes on one host with Prometheus already |
| Verified streaming / BAO, castore protocols (snix) | med† | low | low | Filed as the reference design (†needs a smart client); non-goal kept |

## Non-goal renegotiation outcomes

| Non-goal | Outcome | Evidence |
|---|---|---|
| Post-build-hook socket ingestion | **Reopen — ticket 19** | cachix daemon v1.7 and magic-nix-cache independently converged on local-daemon + post-build-hook because watch-store is racy on multi-user stores and trailing pushes waste CI wall-clock |
| Upload resume | Keep closed; note | No surveyed server implements it; the only credible path (multipart part tracking) is the deferred client-side multipart item |
| Custom pull protocol | Keep closed | Universal standard-protocol adherence; snix's compatible-extension design filed as the template |
| OTLP tracing | Keep closed | Only fleet-scale projects carry it; Prometheus covers single-host |
| Multi-tenancy / attic compat | Keep closed | Drives most of attic's complexity; nothing in four other projects suggests a single-tenant cache wants it |
| Containers/k8s | Keep closed | Only ncps invests, in service of HA garret doesn't want |

## Per-project source notes

Full per-project write-ups (with URLs) from the three research passes are
condensed above; key sources: docs.attic.rs (FAQ, reference),
github.com/nix-community/harmonia (README, harmonia-bench),
github.com/aristanetworks/nix-serve-ng (README benchmark section),
docs.ncps.dev (concepts, pinning, fsck, operations), docs.cachix.org +
blog.cachix.org (v1.7 daemon, zstd migration, uploads, doctor, pins),
docs.determinate.systems (FlakeHub cache, magic-nix-cache-action),
snix.dev/docs (castore, store protocol, performance guide) + Replit
tvix-store case study, github.com/buchgr/bazel-remote (README, lru.go,
casblob.go, issue #280), github.com/mozilla/sccache (Architecture.md,
compiler.rs, cache.rs).
