# Observability

Source: [ticket 15](../../.scratch/spec/issues/15-metrics-observability.md).
Consumer: VictoriaMetrics.

## Exposure

Each service binds a dedicated internal metrics listener — defaults
**9091 (Pusher)** and **9092 (Puller)**, interface configurable — serving
`/metrics` and `/healthz`. The public listener never exposes metrics.

## Readiness

The Puller's public listener answers two readiness probes:

- `/ready`: 200 once the database is open, 503 before. No I/O.
- `/ready/deep`: `/ready`, plus one real read through the path a
  substituter follows. It presigns the blob of the most recently
  accessed object exactly as the NAR route does and fetches its first
  byte (`Range: bytes=0-0`). On failure it answers 503 naming the
  failing step, e.g. `presigned GET answered 403`.
  - Presigning is signature-only (spec
    [03](03-storage.md)), so revoked or rotated S3 credentials or an S4
    outage fail neither `/ready` nor the NAR redirect. Only a fetch
    shows them.
  - An empty cache has nothing to read and passes.
  - The fetch trusts the bundled webpki roots, like the JWKS client, not
    the system store, so an S4 endpoint with a private CA fails it.
  - The answer is cached for 30 s, and concurrent callers share one
    probe that finishes even if its caller disconnects. However often
    the public route is hit, it costs at most one one-byte S3 read per
    30 s. Each probe counts in `garret_s3_read_probes_total` by
    `outcome` (`ok`, `failed`).
  - Monitoring should probe `/ready/deep` rather than `/ready`, at an
    interval of 30 s or more, and alert on a few consecutive failures.

## Cardinality rules

Bounded labels only: route, status class, issuer, phase, outcome.
**Never per-object labels** (store path hashes as label values would
explode series); per-object insight is the logs' job. Histogram buckets:
bytes 64 KiB…8 GiB log-scale; latency 1 ms…60 s.

## Catalog

Prefix `garret_`; service distinguished by scrape job.

- **Common**: HTTP requests/duration by route + status class; in-flight
  gauge; SQLite busy/lock-wait histogram; DB query duration by statement
  family; process/runtime defaults; build info.
- **Pusher — uploads**: in-flight uploads and in-flight bytes gauges
  *versus their configured caps* (saturation visible before it hurts);
  upload size/duration histograms; accepted/failed/shed(429) counters;
  negotiation batch-size and missing-ratio histograms;
  `garret_upload_skipped_total` by `reason` (`exists`, `in-progress`,
  `quiescing`, and `deleting` for an upload turned away with `503`
  because GC, `delete` or prune is removing the path, spec 05).
- **Pusher — S3**: put/multipart-part counters, part duration, retries,
  aborted multiparts; blob deletes (`garret_s3_deletes_total`, only keys
  the response did not report as failed) and per-key delete failures
  (`garret_s3_delete_failures_total`: keys a `DeleteObjects` 200 reported
  as not deleted — each an orphan until a later sweep succeeds).
- **Pusher — auth** (also on the Puller, for browse tokens):
  `garret_auth_validations_total` by `issuer` (a configured issuer URL,
  or `unknown` when the token names none: never the token's own `iss`)
  and `outcome` (`accepted`, `malformed`, `untrusted_issuer`,
  `unknown_key`, `jwks_unavailable`, `invalid`, `unauthorized`);
  `garret_jwks_refreshes_total` (fetch attempts) and
  `garret_jwks_refresh_failures_total` by `issuer`. A rise in
  `unknown_key` without matching refreshes is either an unknown-kid flood
  the JWKS floor is absorbing (spec 04) or, if refresh failures rise too,
  a down issuer: during the floor after a failed fetch, unknown kids count
  as `unknown_key`, not `jwks_unavailable`.
- **Pusher — GC**: usage and quota gauges; evicted objects/bytes per
  pass; pass duration; orphans found; candidates-exhausted alarm
  counter; `garret_gc_failures_total` by `phase` (`pass`, `sweep`), for
  passes and orphan sweeps that errored, whoever triggered them;
  `garret_gc_last_success_timestamp`, set by every successful pass and
  every successful tick — including a quota check that finds nothing to
  evict — so it goes stale only when GC stops running, never merely
  because usage is low.
- **Pusher — fsck**: `garret_fsck_runs_total` counter; `garret_fsck_findings`
  gauge by `kind` (`dangling`, `orphan`, `size_mismatch`), set to the
  latest run's count per kind; `garret_fsck_rows_repaired_total` counter
  by `reason` (`dangling`, `size_mismatch`).
- **Puller**: narinfo hit/miss counters; NAR redirects issued, by
  hit/miss; presign duration histogram; `garret_degraded_total` by
  `reason` (`db_timeout`, `db_error`, `presign_timeout`,
  `presign_error`) — pull-path requests degraded to a 404 miss when a
  budget tripped or a read failed (spec
  [03-storage](03-storage.md#bounded-budgets-degrade-to-a-miss); a
  degraded narinfo request also counts as a miss); last-accessed bumps
  (spec [02](02-database.md#concurrency-discipline)):
  `garret_bump_queue_depth` gauge, `garret_bump_debounced_total`
  (hits on a fresh row, no write) and `garret_bump_failures_total`
  (failed flushes, batch dropped); browse requests by endpoint;
  browse auth failures; `garret_s3_read_probes_total` by `outcome`
  (deep readiness, [above](#readiness)). The Puller no longer sees NAR bytes
  ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)), so
  bytes-served, serve-duration, first-byte and Range counters are gone —
  served-byte volume is now S4's to report, not ours.

## Logs

`tracing` crate: human-readable by default, JSON via config
(journald-friendly); per-request spans with request ids. No
OTLP/distributed tracing in v1. The client is metrics-free: progress
output, logs, and the watcher skip-list.

A rejected token's reason is logged escaped and cut at 256 characters: it
can carry attacker-controlled token text (the `kid`, header fields echoed
by parse errors), which must not forge log lines or flood the journal.
