# HTTP framework for garret

Research for `.scratch/spec/issues/01-http-framework.md`. Date: 2026-08-05.

## Verdict

**Recommended: axum (0.8.x) on hyper 1.x, served via `hyper-util`'s auto builder, with tower / tower-http middleware.**
**Fallback: raw hyper 1.x + tower, no axum.**

Reasoning gist: for a byte-shuffling cache workload, the HTTP *body* path is what matters, and axum does not have one of its own — request/response bodies are hyper's `http_body::Body` frames passed through untouched. The framework layer (routing, extractors, middleware) costs on the order of microseconds *per request*, while a single NAR transfer costs milliseconds-to-minutes; the framework layer is therefore noise for garret's workload, and axum buys a materially better middleware/ecosystem story (tower-http, axum-prometheus, tokio-team maintenance, attic precedent) at that negligible cost. The performance-critical knobs all live in hyper (HTTP/2 flow-control windows, buffer sizes), which axum exposes unmodified. Because axum handlers *are* tower services, the fallback (dropping axum's router and driving hyper directly) is a cheap, incremental migration if profiling ever shows the router/extractor layer mattering — not a rewrite.

actix-web is a credible second place (harmonia, the other Rust Nix cache, uses it) but is a whole separate stack: its own HTTP implementation (actix-http), its own middleware trait (no tower), and a single-threaded-worker runtime model. It wins JSON/plaintext microbenchmarks by 10–15%, which is irrelevant to streaming GB bodies, and it costs garret the tower ecosystem for no benefit on this workload.

## The layering fact that frames everything

- **axum is a router + extractor layer over hyper.** It is "a relatively thin layer on top of hyper and adds very little overhead" (axum's own README). Bodies are `http_body::Body` implementations; axum's `axum::body::Body` is a boxed body that forwards `poll_frame` to hyper's body. Streaming a body through an axum handler adds one dynamic dispatch per frame poll (~ns each; a 1 GiB body in 16 KiB frames is ~65k polls, so total added CPU is measured in microseconds-to-low-milliseconds against seconds of transfer dominated by syscalls, memcpy, TLS, and compression).
- **actix-web is NOT on hyper.** It has its own HTTP/1 implementation (actix-http) and uses the hyperium `h2` crate for HTTP/2 — the same `h2` crate hyper uses. So even the "different stack" shares the HTTP/2 framing/flow-control core with hyper.
- **hyper is the shared core of most of the rest of the field**: warp, salvo, poem, tonic, linkerd2-proxy, and Vector all sit on hyper/tower. Choosing hyper's body model is choosing the most battle-tested streaming path in the Rust ecosystem (817M crates.io downloads vs actix-web's 76M as of 2026-08).

Consequence: the candidates are really (a) hyper's body path with some routing layer on top (axum / raw hyper / salvo / poem / warp), or (b) actix-http's body path. Framework-vs-framework throughput deltas measured on tiny-response microbenchmarks say almost nothing about which moves large bodies faster — both stacks move `Bytes` chunks through a pull-based poll loop.

## Evaluation against the criteria

### Streaming performance and backpressure

**Both stacks are pull-based and give backpressure for free; bounded memory is achievable in both.**

- hyper's `http_body::Body` is polled by the consumer (`poll_frame`). If the handler stalls (e.g., S3 multipart upload is slow), hyper stops reading the socket: for HTTP/1 that surfaces as TCP backpressure; for HTTP/2, no `WINDOW_UPDATE` is sent, so the client's send window closes. Memory in flight is bounded by hyper's read buffers plus the h2 window.
- actix-web's `web::Payload` is likewise a `Stream<Item = Result<Bytes>>` with pull semantics; equivalent behavior.
- **The real streaming-throughput lever is HTTP/2 flow control, not the framework.** The 64 KiB default window throttles large transfers in *every* HTTP/2 implementation (curl [#9571](https://github.com/curl/curl/issues/9571), .NET [runtime#43086](https://github.com/dotnet/runtime/issues/43086), Cloudflare's [upload-speed writeup](https://blog.cloudflare.com/delivering-http-2-upload-speed-improvements/)). hyper had exactly this class of problem historically ([hyper#1813](https://github.com/hyperium/hyper/issues/1813), request bodies slow over h2) and grew the fix as configuration: `hyper-util`'s [`Http2Builder`](https://docs.rs/hyper-util/latest/hyper_util/server/conn/auto/struct.Http2Builder.html) exposes `initial_stream_window_size`, `initial_connection_window_size`, **`adaptive_window`** (BDP-based auto-tuning), `max_frame_size`, and `max_send_buf_size` (~400 KiB default per stream — this is the per-stream memory bound knob). Garret should enable `adaptive_window` or set windows in the several-MiB range on the Pusher, and treat `max_send_buf_size * max_concurrent_streams` as its memory budget. actix-web exposes h2 window settings too (via actix-http builder), but hyper's adaptive window is the more polished implementation (ported from gRPC's BDP estimation).

### Per-request overhead

- axum's router is a radix-tree (`matchit`) lookup plus tower layer traversal — sub-microsecond. Microbenchmarks showing axum "25% behind raw hyper, 8x latency" ([axum discussion #2566](https://github.com/tokio-rs/axum/discussions/2566)) were TechEmpower-style plaintext runs; the maintainers' response: "do not read too much into those benchmarks... optimized in all sorts of unrealistic ways." A real but small CPU regression from the 0.6→0.7 (hyper 1.0) migration was tracked in [axum#2381](https://github.com/tokio-rs/axum/issues/2381) — visible at hundreds-of-thousands of small requests/sec, partially mitigated in 0.7.4+.
- **Why this doesn't matter for garret:** the workload is few, huge requests, not many tiny ones. At even 1k req/s (an extremely hot cache), 1–5 µs of routing overhead is 0.1–0.5% of one core. The Puller's hot path per request is: one narinfo lookup (small response — overhead-sensitive but trivially fast anyway) or one NAR stream (seconds long — overhead-invisible).
- actix-web's 10–15% req/s edge in plaintext/JSON microbenchmarks (e.g. [reintech 2026 comparison](https://reintech.io/blog/axum-vs-actix-web-vs-rocket-vs-rust-framework-comparison-2026), various Medium roundups) is real but measures exactly the regime garret is not in. No published benchmark of large-body streaming throughput across these frameworks was found (searched; the roundups all measure small-response RPS) — expect parity, dominated by shared `h2`/TCP behavior, and verify with garret's own harness.

### Large-body upload ergonomics

- **axum**: extract `Body` (or any `impl Body`), call `into_data_stream()`, get a `Stream<Item = Result<Bytes>>` — feed straight into an `AsyncWrite`/S3 multipart. Two gotchas, both documented: (1) [`DefaultBodyLimit`](https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html) is 2 MiB by default — but it only applies to buffering extractors (`Bytes`/`Json`/`String`); direct `poll_frame` streaming bypasses it, so the Pusher must add its own `tower_http::limit::RequestBodyLimit` if it wants a cap at all; (2) responses stream via `Body::from_stream`. Clean fit for both Pusher (custom protocol over streaming bodies) and Puller.
- **actix-web**: `web::Payload` stream in, `HttpResponse::streaming()` out — equally capable, slightly different idioms, `PayloadConfig` for limits.
- **raw hyper**: same body traits with zero sugar; you hand-write routing, error mapping, and per-route limits. Perfectly doable for garret's ~6 routes, but you re-implement exactly the thin layer axum gives away for free, and you lose extractor-level clarity for no throughput gain (the body path is identical).

### Middleware story (OIDC/JWT auth, Prometheus)

This is where the candidates genuinely differ:

- **axum/tower**: middleware are `tower::Layer`s, shareable between the Pusher and Puller binaries and with any other tower-based service. Off-the-shelf: `tower-http` (auth/`validate_request`, `limit`, `timeout`, `trace`, compression, `ServeDir`), `axum-prometheus` / `metrics` + `metrics-exporter-prometheus` for RED metrics, `jsonwebtoken`/`jwt-simple` + `openidconnect` for OIDC validation as a small custom layer or extractor (`FromRequestParts` makes "validated token as handler argument" idiomatic). Attic itself implements exactly this pattern (axum + JWT auth) — code to crib from.
- **actix-web**: its own `Transform`/middleware trait; equivalents exist (`actix-web-prom`, `actix-web-httpauth`) but the ecosystem is smaller and nothing is reusable with tower-based components (e.g., if garret ever embeds a tonic gRPC endpoint or reuses layers in a client stack).
- **raw hyper**: you can still use tower layers (wrap your `Service` manually) — the middleware story is actually fine; what you lose is only routing/extractor ergonomics.

### HTTP/2

- **hyper/axum**: full h1 + h2, including **h2c with prior-knowledge/auto-detection** via `hyper_util::server::conn::auto::Builder` (serves h1 and h2 on one port). Matters because Nix's downloader is libcurl, which speaks HTTP/2, and garret may sit behind a TLS-terminating proxy where h2c upstream is useful. Tuning knobs per above.
- **actix-web**: h2 over TLS via ALPN, and h2c via `bind_auto_h2c()` (added after [actix-web#1414](https://github.com/actix/actix-web/issues/1414); see [HttpServer docs](https://docs.rs/actix-web/latest/actix_web/struct.HttpServer.html)). Parity in practice.
- HTTP/3: nothing production-integrated in either (hyperium's `h3` crate exists but isn't wired into hyper's server API; salvo has experimental h3). Not a deciding factor; curl/Nix substitution over h3 is not a practical requirement in 2026.

### Maintenance health (verified via crates.io, 2026-08-05)

| Crate | Latest stable | Last publish | Downloads | Notes |
|---|---|---|---|---|
| hyper | 1.11.0 | 2026-07-20 | 817M | Tokio-org; the ecosystem substrate |
| axum | 0.8.9 | 2026-04-14 | 413M | Tokio-org, maintained by tokio team |
| actix-web | 4.14.0 | 2026-06-21 | 76M | Active; 4.13/4.14 in 2026, v5 discussed |
| warp | 0.4.3 | 2026-05-04 | 45M | Alive but slow-moving; superseded culturally by axum |
| salvo | 0.95.1 | 2026-07-29 | 12M | hyper-based, very active, small community |
| poem | 3.1.12 | 2025-07-28 | 5M | hyper-based, quieter |
| ntex | 3.12.1 | 2026-08-05 | 1M | actix fork, mostly one maintainer; benchmark-topping but small bus factor |

All primary candidates are healthy. axum's 0.x versioning means occasional breaking releases (0.7→0.8 was mildly disruptive), but attic has tracked them fine. actix-web 4.x has been API-stable since 2022 — a point in its favor for churn-aversion.

### Precedents in exactly this problem domain

- **attic** (garret's predecessor): axum — see `/Users/aidanp/Projects/attic/server/Cargo.toml` (axum 0.8.9). Its streaming upload/download paths are a direct reference implementation.
- **harmonia** (nix-community binary cache): actix-web ([repo](https://github.com/nix-community/harmonia)). Proof that actix-web also handles NAR streaming fine in production.
- **linkerd2-proxy** (proxy-shaped, throughput-critical): raw hyper + tower. Proof that hyper's body path is proxy-grade at scale.

## Caveats and unknowns

1. **No trustworthy published large-body streaming benchmark exists across these frameworks.** Every public comparison found (TechEmpower, reintech, Medium roundups, sharkbench) measures small-response RPS — explicitly flagged in the ticket as misleading, and cited here only to bound the *per-request* overhead question. Garret should build a 10-line criterion/wrk harness early (1 MiB / 100 MiB / 2 GiB bodies, h1 and h2, concurrency 1/32/256) and treat it as a regression gate; if axum ever measurably lags raw hyper there, the fallback migration is small.
2. **HTTP/2 window tuning is mandatory, not optional.** Defaults (64 KiB windows) will cap per-stream throughput on high-BDP links regardless of framework choice. Enable `adaptive_window` (or set multi-MiB windows) and size `max_send_buf_size` x `max_concurrent_streams` against the memory budget. This dwarfs any framework delta.
3. **axum's default 2 MiB body limit does not protect streaming routes** — direct body consumption bypasses `DefaultBodyLimit`, so the Pusher needs explicit `RequestBodyLimit`/auth-gated unlimited streaming, and the Puller should keep tight limits on request bodies (it should never receive one).
4. **axum 0.x churn**: expect a breaking release every ~1–1.5 years. Contained risk (attic absorbs them routinely) but real.
5. **The [axum#2381](https://github.com/tokio-rs/axum/issues/2381) CPU regression** (0.6→0.7) never got a definitive close; it manifests at very high small-request rates. Monitor if the Puller's narinfo endpoint ever sees extreme QPS; the raw-hyper fallback covers the tail risk.
6. **actix-web remains a defensible choice** (harmonia proves it); the decision here is ecosystem-shaped (tower, tokio-team alignment, attic code reuse), not performance-shaped. If garret's team already had actix expertise, that would flip the call without a throughput penalty.

## Sources

- axum repo/README (thin-layer claim, releases): https://github.com/tokio-rs/axum
- axum perf discussion: https://github.com/tokio-rs/axum/discussions/2566 ; regression: https://github.com/tokio-rs/axum/issues/2381
- axum DefaultBodyLimit: https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html
- hyper-util auto builder + Http2Builder (h2 tuning): https://docs.rs/hyper-util/latest/hyper_util/server/conn/auto/struct.Http2Builder.html
- hyper h2 body-throughput history: https://github.com/hyperium/hyper/issues/1813
- HTTP/2 flow-control window pathology (cross-ecosystem): https://github.com/curl/curl/issues/9571 , https://github.com/dotnet/runtime/issues/43086 , https://blog.cloudflare.com/delivering-http-2-upload-speed-improvements/
- actix-web h2c: https://github.com/actix/actix-web/issues/1414 , https://docs.rs/actix-web/latest/actix_web/struct.HttpServer.html , https://actix.rs/docs/http2/
- actix-web releases: https://github.com/actix/actix-web/releases (4.14.0 2026-06, 4.13.0 2026-02)
- harmonia (actix-web precedent): https://github.com/nix-community/harmonia
- attic (axum precedent): local checkout `/Users/aidanp/Projects/attic/server/Cargo.toml`
- Microbenchmark roundups (used only for per-request-overhead bounds, per ticket caveat): https://reintech.io/blog/axum-vs-actix-web-vs-rocket-vs-rust-framework-comparison-2026 , https://sharpskill.dev/en/blog/rust/rust-actix-web-vs-axum-comparison
- crates.io API (versions/dates/downloads, queried 2026-08-05)
