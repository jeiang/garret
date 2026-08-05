# HTTP framework choice

Type: research
Status: resolved

## Question

Which Rust HTTP stack should garret use for maximum streaming throughput?
Candidates: axum (attic's choice), actix-web, hyper used directly, or other
tokio-compatible options. Evaluate: streaming request/response body
performance and backpressure behavior, per-request overhead, large-body
upload ergonomics, middleware story (auth, metrics), maintenance health, and
any published benchmarks relevant to proxy/cache-shaped workloads (not JSON
microbenchmarks). Recommend one, with a fallback.

## Answer

**axum 0.8.x on hyper 1.x (via hyper-util's auto builder) with tower/tower-http
middleware; fallback is raw hyper + tower.** Full findings:
[research/http-framework.md](../research/http-framework.md).

- axum adds no body-path overhead: bodies are hyper's `http_body` frames with
  ~one dynamic dispatch per frame poll — negligible for few-huge-requests
  workloads. Framework overhead is per-request, not per-byte.
- The real streaming-throughput lever is HTTP/2 flow control, not framework
  choice: default 64 KiB windows cap every stack. hyper exposes
  `adaptive_window`, window sizes, and `max_send_buf_size` (the per-stream
  memory bound) — tuning these is mandatory for the push protocol.
- Backpressure is pull-based and equivalent across stacks; actix-web's
  10–15% plaintext-RPS edge is irrelevant here (and it shares the same `h2`).
- axum wins middleware (tower-http, axum-prometheus, OIDC extractors),
  maintenance (tokio-team, active 2026 releases), and precedent (attic is
  axum; harmonia proves actix viable).
- Gotcha: axum's `DefaultBodyLimit` does NOT apply to streamed bodies — the
  Pusher needs explicit body-limit handling.
- No credible large-body cross-framework benchmark exists; garret should
  build its own 1 MiB / 100 MiB / 2 GiB harness as a regression gate (feeds
  ticket 16). axum handlers are tower services, so dropping to raw hyper
  later is an incremental migration, not a rewrite.
