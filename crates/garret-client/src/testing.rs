//! Test-only fixtures.

use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::push::Pusher;

/// An empty directory, removed with its contents on drop, so a failing
/// test leaves nothing behind.
pub struct ScratchDir(PathBuf);

impl Deref for ScratchDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Creates a fresh [`ScratchDir`]: the pid tells concurrent test binaries
/// apart, the counter calls within one.
pub fn scratch_dir() -> ScratchDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "garret-client-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    // A dead process with the same pid may have left one behind.
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("creating scratch dir");
    ScratchDir(path)
}

/// Serves `status` with `body` to every request, counting them. Each
/// request is read whole (headers, then a `Content-Length` body) before
/// the reply, so nothing hinges on how the client's writes are split.
pub async fn answering(status: u16, body: &'static str) -> (String, Arc<AtomicUsize>) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut content_length = 0;
            let mut line = String::new();
            loop {
                line.clear();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut request_body = vec![0; content_length];
            socket.read_exact(&mut request_body).await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            let reply = format!(
                "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.get_mut().write_all(reply.as_bytes()).await;
        }
    });
    (endpoint, hits)
}

/// A [`Pusher`] for `endpoint` that never retries, with a token source
/// counting its mints.
pub fn pusher(endpoint: String) -> (Pusher, Arc<AtomicUsize>) {
    let (tokens, mints) = crate::auth::TokenSource::counting();
    let pusher = Pusher {
        http: reqwest::Client::new(),
        endpoint,
        tokens,
        jobs: 1,
        zstd_level: 3,
        max_retries: 0,
        upload_idle: Duration::from_secs(5),
    };
    (pusher, mints)
}
