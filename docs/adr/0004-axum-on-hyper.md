# axum 0.8 on hyper 1.x; throughput engineering targets HTTP/2 flow control

Garret uses axum on hyper with tower middleware, with raw hyper + tower as
the sanctioned fallback (axum handlers are tower services, so dropping
down is an incremental migration, not a rewrite). Research found framework
choice is not the throughput lever for a few-huge-requests workload —
bodies pass through as hyper frames with negligible per-frame overhead —
while the real lever is HTTP/2 flow control: default 64 KiB windows cap
every stack, so garret must tune adaptive_window and max_send_buf_size
(which doubles as the per-stream memory bound). Gotcha recorded: axum's
DefaultBodyLimit does not apply to streamed bodies; the Pusher enforces
its own limits. Evidence: `.scratch/spec/research/http-framework.md`.
