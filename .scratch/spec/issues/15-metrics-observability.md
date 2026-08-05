# Metrics and observability catalog

Type: grilling
Status: resolved
Blocked by: 06

## Question

Define the extensive Prometheus metrics catalog for both services, sized
for VictoriaMetrics: upload-path metrics (throughput, in-flight pushes,
negotiation hit rates, per-stage latencies, dedup savings if chunking won),
download-path metrics (narinfo/NAR request rates, latencies, S3 fetch
timings), resource metrics (memory, semaphore saturation, SQLite busy/lock
waits), GC metrics, and client/watcher metrics if any. Set label/cardinality
rules, the exposition endpoints (per-service /metrics), and the stance on
structured logging and tracing beyond metrics.

## Answer

**Exposure** — each service binds a dedicated internal metrics listener
(defaults 9091 Pusher / 9092 Puller, interface configurable) serving
`/metrics` + `/healthz`. The public listener never exposes metrics; no
auth on the LAN-bound port.

**Cardinality rules (locked)** — bounded labels only: route, status
class, issuer, phase, outcome. **Never per-object labels** (store path
hashes would explode VictoriaMetrics series); per-object insight is the
logs' job. Histograms use size-appropriate buckets: bytes 64 KiB…8 GiB
log-scale, latency 1 ms…60 s.

**Catalog** (prefix `garret_`, service distinguished by scrape job;
extensive but bounded):

- *Common*: HTTP requests/duration by route+status class, in-flight
  gauge, SQLite busy/lock-wait histogram, DB query duration by statement
  family, process/runtime defaults, build info.
- *Pusher — uploads*: in-flight uploads and in-flight bytes gauges vs
  their configured caps (semaphore saturation), upload size/duration
  histograms, accepted/failed/shed(429) counters, negotiation batch size
  and missing-ratio histograms, idempotent-skip counters
  (exists/in-progress).
- *Pusher — S3*: put/multipart-part counters, part duration, retries,
  aborted multiparts.
- *Pusher — auth*: validations by issuer+outcome, JWKS refreshes and
  failures.
- *Pusher — GC* (from ticket 11): usage and quota gauges, evicted
  objects/bytes per pass, pass duration, orphans found,
  candidates-exhausted alarm counter, last-successful-pass timestamp.
- *Puller*: narinfo hit/miss counters, NAR serves and bytes-served
  counter, serve duration and S3 first-byte histograms, Range request
  counter, last-accessed bump queue depth and debounce-skip counter,
  browse requests by endpoint, browse auth failures.

**Logs/tracing** — tracing crate; human-readable by default, JSON via
config (journald-friendly); per-request spans with request ids. No
OTLP/distributed tracing in v1. The client stays metrics-free (ticket
13): progress output, logs, and the watcher skip-list.
