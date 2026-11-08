//! Process plumbing shared by the Pusher and Puller: startup, logging, the
//! HTTP accept loop, and systemd readiness.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _};

use crate::config::ServiceConfig;
use crate::telemetry::Telemetry;

/// A client that has not sent complete request headers by then is dropped, so
/// idle connections cannot pin sockets or tasks. Bodies have their own idle
/// timeout (see `storage`).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// What [`start`] hands a service.
pub struct Started<C> {
    /// The loaded and validated config.
    pub config: C,
    /// The public listener, bound but not yet serving.
    pub listener: TcpListener,
    /// The span exporter, if `[telemetry]` enabled it. [`Telemetry::shutdown`]
    /// it once serving ends, or the last batch is lost.
    pub telemetry: Telemetry,
}

/// The startup both services share: the config named by the first argument
/// (loaded and validated), the telemetry exporter it asks for, logging, the
/// metrics recorder (installed first, so later startup work is recorded), then
/// both listeners bound, each failure naming the address. The metrics listener
/// serves from here on; the public one is the caller's to serve once the
/// service is ready.
pub async fn start<C: ServiceConfig>(service: &str) -> Result<Started<C>> {
    let path = std::env::args()
        .nth(1)
        .with_context(|| format!("usage: garret-{service} <config.toml>"))?;
    // The config decides whether there is a telemetry layer at all, so it is
    // loaded before the subscriber is installed. Failures still reach the log:
    // the fmt subscriber goes up first, then the error is logged and returned.
    let config: C = match crate::config::load(&path) {
        Ok(config) => config,
        Err(e) => {
            init_tracing(None);
            tracing::error!("{e:#}");
            return Err(e);
        }
    };
    let telemetry = match Telemetry::start(config.telemetry(), service) {
        Ok(telemetry) => telemetry,
        Err(e) => {
            init_tracing(None);
            tracing::error!("{e:#}");
            return Err(e);
        }
    };
    init_tracing(telemetry.tracer(service));
    let metrics = crate::metrics::install(service)?;
    let metrics_listener = bind(config.metrics_listen(), "metrics").await?;
    let listener = bind(config.listen(), service).await?;
    let metrics_app = crate::metrics::router(metrics);
    tokio::spawn(async move {
        if let Err(e) = serve(metrics_listener, metrics_app, std::future::pending()).await {
            tracing::error!("metrics listener failed: {e:#}");
        }
    });
    Ok(Started {
        config,
        listener,
        telemetry,
    })
}

async fn bind(addr: &str, name: &str) -> Result<TcpListener> {
    let addr: SocketAddr = addr
        .parse()
        .with_context(|| format!("invalid {name} listen address {addr:?}"))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {name} listener {addr}"))?;
    tracing::info!("{name} listening on {addr}");
    Ok(listener)
}

/// Logs to stdout at `RUST_LOG` (default `info`), with colour only on a
/// terminal so journald gets plain text. With a tracer, garret's own spans are
/// also bridged to OpenTelemetry.
fn init_tracing(tracer: Option<opentelemetry_sdk::trace::SdkTracer>) {
    use std::io::IsTerminal;
    use tracing_subscriber::filter::{LevelFilter, Targets};
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Per layer, not global: the operation spans are `debug` so that with no
    // exporter they are never created — a registry span per request is a
    // measurable share of a sub-millisecond narinfo — while the OTel layer
    // takes every garret span the moment a collector is configured.
    let fmt = tracing_subscriber::fmt::layer()
        .with_ansi(std::io::stdout().is_terminal())
        .with_filter(filter);
    let registry = tracing_subscriber::registry().with(fmt);
    match tracer {
        Some(tracer) => registry
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    // Source location, thread ids and busy/idle timings are
                    // noise in a request trace and cost work per span; the
                    // span's own fields carry what a trace needs.
                    .with_location(false)
                    .with_threads(false)
                    .with_tracked_inactivity(false)
                    // Only garret's spans are exported. The SDK, hyper,
                    // rustls and reqwest spans under `s3.request` are
                    // dependency internals: they multiply one S3 call into
                    // ~10 spans and say nothing `s3.request` does not.
                    // Dependency WARN/ERROR events still pass and attach to
                    // the active span; `RUST_LOG` keeps controlling the fmt
                    // layer only, so it cannot widen what is exported.
                    .with_filter(
                        Targets::new()
                            .with_target("garret_server", LevelFilter::TRACE)
                            .with_target("garret_pusher", LevelFilter::TRACE)
                            .with_target("garret_puller", LevelFilter::TRACE)
                            .with_default(LevelFilter::WARN),
                    ),
            )
            .init(),
        None => registry.init(),
    }
}

/// Serves `app` on `listener` until `shutdown` resolves, then waits for open
/// connections to finish. HTTP/1.1 only: every connection must send complete
/// request headers within [`HEADER_READ_TIMEOUT`] of being accepted (and of
/// each keep-alive request), so a silent client is dropped. The listener is
/// closed as soon as `shutdown` fires, so drain refuses new connections.
pub async fn serve<F>(listener: TcpListener, app: Router, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_header_timeout(listener, app, shutdown, HEADER_READ_TIMEOUT).await
}

async fn serve_with_header_timeout<F>(
    listener: TcpListener,
    app: Router,
    shutdown: F,
    header_timeout: Duration,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    // Small final responses (a narinfo, a redirect, an upload
                    // ack) must not wait on Nagle for an ACK that may never
                    // come. Failure is not fatal: the connection still works.
                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::debug!("could not disable Nagle: {e}");
                    }
                    stream
                }
                // EMFILE and friends: back off instead of spinning.
                Err(e) => {
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let service = TowerToHyperService::new(app.clone());
        let conn = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("connection ended with an error: {e}");
            }
        });
    }
    drop(listener);
    graceful.shutdown().await;
    Ok(())
}

/// Resolves on SIGTERM or SIGINT, the signals systemd and a terminal send.
/// Both services stop accepting on it and drain, then flush their spans.
pub async fn stop_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Tells systemd (`Type=notify`) the service is ready. A no-op outside
/// systemd.
pub fn notify_ready() {
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    if let Err(e) = send_notify(&path, b"READY=1") {
        tracing::warn!("sd_notify READY failed: {e}");
    }
}

fn send_notify(path: &std::ffi::OsStr, message: &[u8]) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixDatagram;
    let socket = UnixDatagram::unbound()?;
    let bytes = path.as_bytes();
    if let Some(name) = bytes.strip_prefix(b"@") {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
            socket.send_to_addr(message, &addr)?;
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = name;
            return Err(std::io::Error::other("abstract NOTIFY_SOCKET needs Linux"));
        }
    }
    socket.send_to(message, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn silent_connection_is_closed_after_header_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(super::serve_with_header_timeout(
            listener,
            axum::Router::new(),
            async move {
                let _ = stopped.await;
            },
            Duration::from_millis(200),
        ));
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await
            .expect("server did not drop the silent connection");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "expected EOF or reset, got {read:?}"
        );
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    }
}
