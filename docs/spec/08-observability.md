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
- **Puller**: narinfo hit/miss counters; NAR serves and bytes served;
  serve duration and S3 first-byte histograms; Range request counter;
  last-accessed bump queue depth and debounce-skip counter; browse
  requests by endpoint; browse auth failures.

## Logs

`tracing` crate: human-readable by default, JSON via config
(journald-friendly); per-request spans with request ids. No
OTLP/distributed tracing in v1. The client is metrics-free: progress
output, logs, and the watcher skip-list.
