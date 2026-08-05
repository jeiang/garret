# Metrics and observability catalog

Type: grilling
Status: open
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
