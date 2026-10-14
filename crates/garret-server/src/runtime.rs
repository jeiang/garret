//! Process plumbing shared by the Pusher and Puller: logging, the HTTP accept
//! loop, and systemd readiness.

use std::time::Duration;

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;

/// A client that has not sent complete request headers by then is dropped, so
/// idle connections cannot pin sockets or tasks. Bodies have their own idle
/// timeout (see `storage`).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Logs to stdout at `RUST_LOG` (default `info`), with colour only on a
/// terminal so journald gets plain text.
pub fn init_tracing() {
    use std::io::IsTerminal;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
}

/// Serves `app` on `listener` until `shutdown` resolves, then waits for open
/// connections to finish. HTTP/1 connections must send their headers within
/// [`HEADER_READ_TIMEOUT`].
pub async fn serve<F>(listener: TcpListener, app: Router, shutdown: F) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
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
        let conn = builder
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .into_owned();
        let conn = graceful.watch(conn);
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("connection ended with an error: {e}");
            }
        });
    }
    graceful.shutdown().await;
    Ok(())
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

#[cfg(unix)]
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

#[cfg(not(unix))]
fn send_notify(_: &std::ffi::OsStr, _: &[u8]) -> std::io::Result<()> {
    Ok(())
}
