//! Process plumbing shared by the Pusher and Puller: logging, the HTTP accept
//! loop, and systemd readiness.

use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
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
        // Closed (EOF) or reset; either way no hang.
        assert!(matches!(read, Ok(0) | Err(_)) || read.unwrap() > 0);
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    }
}
