# Observability

Source: [ticket 15](../../.scratch/spec/issues/15-metrics-observability.md).
Consumer: VictoriaMetrics.

## Exposure

Each service binds a dedicated internal metrics listener — defaults
**9091 (Pusher)** and **9092 (Puller)**, interface configurable — serving
`/metrics` and `/healthz`. The public listener never exposes metrics.

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
  negotiation batch-size and missing-ratio histograms; idempotent-skip
  counters (exists / in-progress).
- **Pusher — S3**: put/multipart-part counters, part duration, retries,
  aborted multiparts.
- **Pusher — auth**: validations by issuer+outcome; JWKS refreshes and
  failures.
- **Pusher — GC**: usage and quota gauges; evicted objects/bytes per
  pass; pass duration; orphans found; candidates-exhausted alarm
  counter; last-successful-pass timestamp.
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
  degraded narinfo request also counts as a miss); last-accessed bump
  queue depth and debounce-skip counter; browse requests by endpoint;
  browse auth failures. The Puller no longer sees NAR bytes
  ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)), so
  bytes-served, serve-duration, first-byte and Range counters are gone —
  served-byte volume is now S4's to report, not ours.

## Logs

`tracing` crate: human-readable by default, JSON via config
(journald-friendly); per-request spans with request ids. No
OTLP/distributed tracing in v1. The client is metrics-free: progress
output, logs, and the watcher skip-list.
