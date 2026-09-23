//! Server-side NarHash/NarSize verification (ADR-0010). The compressed NAR
//! is decompressed and hashed as it streams past on its way to S3 — never
//! buffered, never altered — so a push whose NAR does not match its
//! preamble is refused before anything is stored or signed.

use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use anyhow::anyhow;
use bytes::Bytes;
use futures::Stream;
use garret_server::nix_base32;
use sha2::{Digest, Sha256};
use zstd::stream::raw::{DParameter, Decoder, InBuffer, Operation, OutBuffer};

/// Largest zstd window accepted: 8 MiB, what level 19 (the highest level
/// short of `--ultra`) uses. The decoder holds one window per upload, so
/// this keeps that memory bounded (spec 01).
const WINDOW_LOG_MAX: u32 = 23;

/// Decompressed bytes produced per decoder call: zstd's recommended
/// streaming output size.
const SCRATCH: usize = 128 * 1024;

/// Passes the compressed NAR through untouched while decompressing and
/// hashing it. At end of stream it compares against the claimed NarHash and
/// NarSize and, on a mismatch, yields an error in place of EOF. Every store
/// path reads to EOF before it commits — the single `PutObject` is sent, or
/// the multipart completed, only after — so a mismatch fails the store and
/// `put_streaming` aborts it.
pub struct VerifiedNar<S> {
    inner: S,
    decoder: Decoder<'static>,
    scratch: Box<[u8]>,
    hasher: Sha256,
    nar_size: i64,
    /// The last decoder call ended exactly at a frame boundary.
    frame_done: bool,
    /// End of stream was reached or the NAR refused: never poll again.
    finished: bool,
    claimed_hash: String,
    claimed_size: i64,
    /// Why the NAR was refused, once it has been: the caller's 400.
    pub refused: Option<String>,
}

impl<S> VerifiedNar<S> {
    /// `claimed_hash` is the normalised `sha256:<nix-base32>` NarHash.
    pub fn new(inner: S, claimed_hash: &str, claimed_size: i64) -> std::io::Result<Self> {
        let mut decoder = Decoder::new()?;
        decoder.set_parameter(DParameter::WindowLogMax(WINDOW_LOG_MAX))?;
        Ok(Self {
            inner,
            decoder,
            scratch: vec![0; SCRATCH].into_boxed_slice(),
            hasher: Sha256::new(),
            nar_size: 0,
            frame_done: false,
            finished: false,
            claimed_hash: claimed_hash.to_owned(),
            claimed_size,
            refused: None,
        })
    }

    fn feed(&mut self, chunk: &[u8]) -> Result<(), String> {
        let mut src = InBuffer::around(chunk);
        loop {
            let mut dst = OutBuffer::around(&mut self.scratch[..]);
            let consumed = src.pos();
            let hint = self
                .decoder
                .run(&mut src, &mut dst)
                .map_err(|e| format!("NAR is not a valid zstd stream: {e}"))?;
            let out = dst.as_slice();
            self.nar_size += out.len() as i64;
            // Checked as it grows, so a decompression bomb stops at the claim.
            if self.nar_size > self.claimed_size {
                return Err(format!(
                    "NAR is larger than its claimed NarSize {}",
                    self.claimed_size
                ));
            }
            self.hasher.update(out);
            // Only a call that did something says where the frame stands: an
            // idle call at a frame boundary (a drain after a full buffer, an
            // empty chunk) reports the header of a next frame that never
            // comes, and would refuse an honest NAR.
            if src.pos() > consumed || !out.is_empty() {
                self.frame_done = hint == 0;
            }
            // Done once the chunk is consumed and the decoder had room to
            // spare, so it holds no more output.
            if src.pos() == chunk.len() && out.len() < SCRATCH {
                return Ok(());
            }
        }
    }

    fn finish(&mut self) -> Result<(), String> {
        if !self.frame_done {
            return Err("NAR stream ended mid-frame".into());
        }
        if self.nar_size != self.claimed_size {
            return Err(format!(
                "NarSize is {}, not the claimed {}",
                self.nar_size, self.claimed_size
            ));
        }
        let hash = format!(
            "sha256:{}",
            nix_base32::encode(&self.hasher.finalize_reset())
        );
        if hash != self.claimed_hash {
            return Err(format!(
                "NarHash is {hash}, not the claimed {}",
                self.claimed_hash
            ));
        }
        Ok(())
    }

    fn refuse(&mut self, reason: String) -> Poll<Option<anyhow::Result<Bytes>>> {
        let error = anyhow!("{reason}");
        self.refused = Some(reason);
        self.finished = true;
        Poll::Ready(Some(Err(error)))
    }
}

impl<S, E> Stream for VerifiedNar<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<anyhow::Error>,
{
    type Item = anyhow::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
            Some(Ok(chunk)) => match this.feed(&chunk) {
                Ok(()) => Poll::Ready(Some(Ok(chunk))),
                Err(reason) => this.refuse(reason),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => match this.finish() {
                Ok(()) => {
                    this.finished = true;
                    Poll::Ready(None)
                }
                Err(reason) => this.refuse(reason),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    /// A NAR-sized body (larger than one decoder call's output), compressed
    /// the way the client does, and the claim an honest client would make.
    fn nar() -> (Vec<u8>, Vec<u8>, String, i64) {
        let nar: Vec<u8> = (0..300_000u32)
            .flat_map(|i| (i % 251).to_le_bytes())
            .collect();
        let compressed = zstd::encode_all(nar.as_slice(), 3).unwrap();
        let hash = format!("sha256:{}", nix_base32::encode(&Sha256::digest(&nar)));
        (nar, compressed, hash, 300_000 * 4)
    }

    /// Streams `compressed` through the verifier in small chunks; returns
    /// the bytes that came out and whether the stream ended in an error.
    async fn run(compressed: &[u8], hash: &str, size: i64) -> (Vec<u8>, Option<String>) {
        let chunks: Vec<Result<Bytes, std::io::Error>> = compressed
            .chunks(1000)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let mut nar = VerifiedNar::new(futures::stream::iter(chunks), hash, size).unwrap();
        let mut passed = Vec::new();
        while let Some(item) = nar.next().await {
            match item {
                Ok(chunk) => passed.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }
        (passed, nar.refused)
    }

    #[tokio::test]
    async fn an_honest_nar_passes_through_untouched() {
        let (_, compressed, hash, size) = nar();
        let (passed, refused) = run(&compressed, &hash, size).await;
        assert_eq!(refused, None);
        assert_eq!(passed, compressed);
    }

    /// A NAR that ends exactly on a 128 KiB output buffer: the decoder is
    /// drained once more, idle, at the frame boundary. That must not read as
    /// "mid-frame" (about 1 in 16k real NARs is sized like this).
    #[tokio::test]
    async fn an_honest_nar_ending_on_a_buffer_boundary_passes() {
        let nar = vec![b'n'; 4 * SCRATCH];
        let compressed = zstd::encode_all(nar.as_slice(), 3).unwrap();
        let hash = format!("sha256:{}", nix_base32::encode(&Sha256::digest(&nar)));
        let (passed, refused) = run(&compressed, &hash, nar.len() as i64).await;
        assert_eq!(refused, None);
        assert_eq!(passed, compressed);
    }

    #[tokio::test]
    async fn a_wrong_nar_hash_is_refused_at_end_of_stream() {
        let (_, compressed, _, size) = nar();
        let other = format!("sha256:{}", nix_base32::encode(&Sha256::digest(b"other")));
        let (_, refused) = run(&compressed, &other, size).await;
        assert!(refused.is_some());
    }

    #[tokio::test]
    async fn a_wrong_nar_size_is_refused_either_way() {
        let (_, compressed, hash, size) = nar();
        // Claimed too small: refused as soon as the NAR outgrows it.
        let (passed, refused) = run(&compressed, &hash, size - 1).await;
        assert!(refused.is_some());
        assert!(passed.len() < compressed.len());
        // Claimed too large: refused at end of stream.
        assert!(run(&compressed, &hash, size + 1).await.1.is_some());
    }

    #[tokio::test]
    async fn a_truncated_or_corrupt_stream_is_refused() {
        let (_, compressed, hash, size) = nar();
        let truncated = &compressed[..compressed.len() - 10];
        assert!(run(truncated, &hash, size).await.1.is_some());
        let mut trailing = compressed.clone();
        trailing.extend_from_slice(b"not zstd");
        assert!(run(&trailing, &hash, size).await.1.is_some());
        assert!(run(b"", &hash, size).await.1.is_some());
    }

    #[tokio::test]
    async fn a_window_wider_than_the_budget_is_refused() {
        // 16 MiB: one step past the 8 MiB each upload's decoder may hold.
        let (nar, _, hash, size) = nar();
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.window_log(WINDOW_LOG_MAX + 1).unwrap();
        std::io::Write::write_all(&mut encoder, &nar).unwrap();
        let wide = encoder.finish().unwrap();
        assert!(run(&wide, &hash, size).await.1.is_some());
    }
}
