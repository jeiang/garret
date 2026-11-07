//! Server-side NarHash/NarSize verification (ADR-0010). The compressed NAR
//! is decompressed and hashed as it streams past on its way to S3 — never
//! buffered, never altered — so a push whose NAR does not match its
//! preamble is refused before anything is stored or signed.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use bytes::Bytes;
use futures::{FutureExt, Stream};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;
use zstd::stream::raw::{DParameter, Decoder, InBuffer, Operation, OutBuffer};

/// Largest zstd window accepted: 8 MiB, what level 19 (the highest level
/// short of `--ultra`) uses. The decoder holds one window per upload, so
/// this keeps that memory bounded (spec 01).
const WINDOW_LOG_MAX: u32 = 23;

/// Slack over zstd's worst-case compressed size for the claimed NarSize:
/// room for a few skippable or empty frames, never for a bulk padding stream
/// (ADR-0010).
const COMPRESSED_SLACK: usize = 64 * 1024;

/// The most compressed bytes an honest push of a `claimed_size`-byte NAR can
/// send: zstd's worst case plus [`COMPRESSED_SLACK`]. Beyond it the body is
/// refused, and it is the first-part size hint for upload buffering.
pub fn max_compressed_len(claimed_size: i64) -> usize {
    let size = usize::try_from(claimed_size.max(0)).unwrap_or(usize::MAX);
    zstd::zstd_safe::compress_bound(size).saturating_add(COMPRESSED_SLACK)
}

/// Decompressed bytes produced per decoder call: zstd's recommended
/// streaming output size.
const SCRATCH: usize = 128 * 1024;

/// Claimed NarSize from which the check runs on its own blocking thread,
/// pipelined with the upload, instead of inline on the request's task.
/// Below it a NAR is a few milliseconds of work, less than the thread
/// handoff saves.
const OFFLOAD_MIN: i64 = 4 * 1024 * 1024;

/// Compressed bytes handed to an offloaded check in one message. A wake and
/// a park per body chunk cost more than the check itself for small chunks,
/// so chunks are batched; the batch is also the read-ahead the request task
/// holds before passing bytes on.
const BATCH_BYTES: usize = 1024 * 1024;

/// Batches queued for an offloaded check: enough to keep its thread busy
/// while the request task reads, bounded so a slow check pushes back on the
/// body instead of buffering it. At most `OFFLOAD_BATCHES * BATCH_BYTES` is
/// queued, plus the one batch being filled and the one being drained.
const OFFLOAD_BATCHES: usize = 4;

/// The NarHash/NarSize check over a compressed NAR, fed chunk by chunk.
struct Checker {
    decoder: Decoder<'static>,
    scratch: Box<[u8]>,
    hasher: Sha256,
    nar_size: i64,
    /// Compressed bytes received so far, capped at `max_compressed`.
    compressed: usize,
    max_compressed: usize,
    /// The last decoder call ended exactly at a frame boundary.
    frame_done: bool,
    claimed_hash: String,
    claimed_size: i64,
}

impl Checker {
    fn new(claimed_hash: &str, claimed_size: i64) -> std::io::Result<Self> {
        let mut decoder = Decoder::new()?;
        decoder.set_parameter(DParameter::WindowLogMax(WINDOW_LOG_MAX))?;
        Ok(Self {
            decoder,
            scratch: vec![0; SCRATCH].into_boxed_slice(),
            hasher: Sha256::new(),
            nar_size: 0,
            compressed: 0,
            max_compressed: max_compressed_len(claimed_size),
            frame_done: false,
            claimed_hash: claimed_hash.to_owned(),
            claimed_size,
        })
    }

    fn feed(&mut self, chunk: &[u8]) -> Result<(), String> {
        // Counted before decoding: zero-output input (skippable frames) would
        // otherwise pass the NarSize check while still being stored.
        self.compressed = self.compressed.saturating_add(chunk.len());
        if self.compressed > self.max_compressed {
            return Err(format!(
                "compressed body exceeds {} bytes, the zstd bound for the claimed NarSize {}",
                self.max_compressed, self.claimed_size
            ));
        }
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
        let hash = garret_common::sha256_nix32(&self.hasher.finalize_reset());
        if hash != self.claimed_hash {
            return Err(format!(
                "NarHash is {hash}, not the claimed {}",
                self.claimed_hash
            ));
        }
        Ok(())
    }
}

/// Where the check runs: inline on the request's task, or on a blocking
/// thread the chunks are handed to.
enum Engine {
    Inline(Box<Checker>),
    Offload {
        /// Closed at end of stream, which tells the thread to finish.
        tx: Option<PollSender<Vec<Bytes>>>,
        verdict: oneshot::Receiver<Result<(), String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// End of stream, and the NAR matched its claim.
    Accepted,
    /// The NAR does not match its claim: the caller's 400.
    Refused(String),
    /// The check itself failed (its thread panicked or was dropped): a
    /// server fault, so the caller's 500 and a push it may retry.
    Failed(String),
}

/// Which wait a `Pending` belongs to, for the upload wait-time counters.
#[derive(Clone, Copy)]
enum Wait {
    /// The client's body stream had nothing to give.
    Client,
    /// The offloaded check was behind (its queue full) or finishing.
    Verify,
}

/// Passes the compressed NAR through untouched while decompressing and
/// hashing it. At end of stream it compares against the claimed NarHash and
/// NarSize and, on a mismatch, yields an error in place of EOF. Every store
/// path reads to EOF before it commits — the single `PutObject` is sent, or
/// the multipart completed, only after — so a mismatch fails the store and
/// `put_streaming` aborts it.
///
/// A large NAR is checked on its own thread (pipelined: a mismatch found
/// mid-stream surfaces a few chunks later, and always before EOF); a small
/// one inline.
pub struct VerifiedNar<S> {
    inner: S,
    engine: Engine,
    /// A chunk read from `inner`, waiting to join the batch.
    pending: Option<Bytes>,
    /// Chunks read from `inner` but not yet handed to the check.
    batch: Vec<Bytes>,
    /// Bytes in `batch`.
    batch_bytes: usize,
    /// Chunks the check has seen, waiting to be passed on.
    ready: VecDeque<Bytes>,
    /// `inner` has ended: its last batch is flushed, then the verdict.
    eof: bool,
    /// How the stream ended, once it has: never poll again. `None` while
    /// streaming, and after a body error from `inner`.
    outcome: Option<Outcome>,
    /// Where the stream last went `Pending`, and when. The wait is closed on
    /// the next poll, so it is timed once per wait, not once per chunk.
    waiting: Option<(Wait, Instant)>,
    /// Time the body spent waiting on the client, and on the check.
    client_wait: Duration,
    verify_wait: Duration,
}

impl<S> VerifiedNar<S> {
    /// `claimed_hash` is the normalised `sha256:<nix-base32>` NarHash.
    pub fn new(inner: S, claimed_hash: &str, claimed_size: i64) -> std::io::Result<Self> {
        Self::with_offload(
            inner,
            claimed_hash,
            claimed_size,
            claimed_size >= OFFLOAD_MIN,
        )
    }

    fn with_offload(
        inner: S,
        claimed_hash: &str,
        claimed_size: i64,
        offload: bool,
    ) -> std::io::Result<Self> {
        let checker = Box::new(Checker::new(claimed_hash, claimed_size)?);
        let engine = if offload {
            let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(OFFLOAD_BATCHES);
            let (verdict_tx, verdict) = oneshot::channel();
            tokio::task::spawn_blocking(move || {
                let mut checker = checker;
                let mut verdict = Ok(());
                'batches: while let Some(batch) = rx.blocking_recv() {
                    for chunk in batch {
                        if let Err(reason) = checker.feed(&chunk) {
                            verdict = Err(reason);
                            break 'batches;
                        }
                    }
                }
                // Dropping `rx` here closes the queue: the request task
                // stops reading and collects this verdict.
                drop(rx);
                let verdict = verdict.and_then(|()| checker.finish());
                let _ = verdict_tx.send(verdict);
            });
            Engine::Offload {
                tx: Some(PollSender::new(tx)),
                verdict,
            }
        } else {
            Engine::Inline(checker)
        };
        Ok(Self {
            inner,
            engine,
            pending: None,
            batch: Vec::new(),
            batch_bytes: 0,
            ready: VecDeque::new(),
            eof: false,
            outcome: None,
            waiting: None,
            client_wait: Duration::ZERO,
            verify_wait: Duration::ZERO,
        })
    }

    /// How the stream ended; `None` if it did not reach a verdict.
    pub fn into_outcome(self) -> Option<Outcome> {
        self.outcome
    }

    /// Time the body spent waiting on the client, and on the check (queue
    /// backpressure plus the final verdict). Read before `into_outcome`,
    /// which consumes the stream.
    pub fn waits(&self) -> (Duration, Duration) {
        (self.client_wait, self.verify_wait)
    }

    fn refuse(&mut self, reason: String) -> Poll<Option<anyhow::Result<Bytes>>> {
        let error = anyhow!("{reason}");
        self.outcome = Some(Outcome::Refused(reason));
        Poll::Ready(Some(Err(error)))
    }

    /// The offloaded check's verdict, once its thread has finished.
    fn poll_verdict(&mut self, cx: &mut Context<'_>) -> Poll<Option<anyhow::Result<Bytes>>> {
        let Engine::Offload { tx, verdict } = &mut self.engine else {
            unreachable!("only an offloaded check has a verdict to wait for");
        };
        *tx = None;
        let verdict = match verdict.poll_unpin(cx) {
            Poll::Ready(verdict) => verdict,
            Poll::Pending => {
                self.waiting = Some((Wait::Verify, Instant::now()));
                return Poll::Pending;
            }
        };
        match verdict {
            Ok(Ok(())) => {
                self.outcome = Some(Outcome::Accepted);
                Poll::Ready(None)
            }
            Ok(Err(reason)) => self.refuse(reason),
            Err(_) => {
                let reason = "NAR check thread failed".to_owned();
                self.outcome = Some(Outcome::Failed(reason.clone()));
                Poll::Ready(Some(Err(anyhow!(reason))))
            }
        }
    }
}

impl<S, E> VerifiedNar<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<anyhow::Error>,
{
    /// The inline path: one chunk in, one chunk out, no queue.
    fn poll_inline(&mut self, cx: &mut Context<'_>) -> Poll<Option<anyhow::Result<Bytes>>> {
        let next = match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(next) => next,
            Poll::Pending => {
                self.waiting = Some((Wait::Client, Instant::now()));
                return Poll::Pending;
            }
        };
        match next {
            Some(Ok(chunk)) => {
                let fed = match &mut self.engine {
                    Engine::Inline(checker) => checker.feed(&chunk),
                    Engine::Offload { .. } => unreachable!("inline poll on an offloaded check"),
                };
                match fed {
                    Ok(()) => Poll::Ready(Some(Ok(chunk))),
                    Err(reason) => self.refuse(reason),
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => {
                let finished = match &mut self.engine {
                    Engine::Inline(checker) => checker.finish(),
                    Engine::Offload { .. } => unreachable!("inline poll on an offloaded check"),
                };
                match finished {
                    Ok(()) => {
                        self.outcome = Some(Outcome::Accepted);
                        Poll::Ready(None)
                    }
                    Err(reason) => self.refuse(reason),
                }
            }
        }
    }

    /// The offloaded path: chunks are batched for the check, so its thread
    /// wakes once per batch rather than once per body chunk. Bytes are
    /// passed on only after the batch holding them is queued, so the check
    /// sees every byte before it is stored; a refusal therefore always
    /// surfaces before the bytes it covers reach the caller.
    fn poll_offloaded(&mut self, cx: &mut Context<'_>) -> Poll<Option<anyhow::Result<Bytes>>> {
        loop {
            if self.eof {
                // The last batch goes to the check before its verdict is
                // awaited, and its bytes are passed on before EOF.
                if let Some(chunk) = self.ready.pop_front() {
                    return Poll::Ready(Some(Ok(chunk)));
                }
                if !self.batch.is_empty() {
                    match self.send_batch(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(false) => return self.poll_verdict(cx),
                        Poll::Ready(true) => continue,
                    }
                }
                return self.poll_verdict(cx);
            }
            if matches!(&self.engine, Engine::Offload { tx: None, .. }) {
                // The check has finished: its refusal is the answer, and the
                // bytes still held are not stored.
                return self.poll_verdict(cx);
            }
            if let Some(chunk) = self.ready.pop_front() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            if let Some(chunk) = self.pending.take() {
                // A full batch goes first, so the queue holds at most one
                // chunk more than BATCH_BYTES.
                if self.batch_bytes > 0 && self.batch_bytes + chunk.len() > BATCH_BYTES {
                    match self.send_batch(cx) {
                        Poll::Pending => {
                            self.pending = Some(chunk);
                            return Poll::Pending;
                        }
                        Poll::Ready(false) => return self.poll_verdict(cx),
                        Poll::Ready(true) => {}
                    }
                    self.pending = Some(chunk);
                    continue;
                }
                self.batch_bytes += chunk.len();
                self.batch.push(chunk);
                if self.batch_bytes >= BATCH_BYTES {
                    match self.send_batch(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(false) => return self.poll_verdict(cx),
                        Poll::Ready(true) => {}
                    }
                }
                continue;
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => self.pending = Some(chunk),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(None) => self.eof = true,
                Poll::Pending => {
                    // Nothing is held back from the check while the client
                    // is quiet: the batch goes now.
                    if !self.batch.is_empty() {
                        match self.send_batch(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(false) => return self.poll_verdict(cx),
                            Poll::Ready(true) => {}
                        }
                        if let Some(chunk) = self.ready.pop_front() {
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                    }
                    self.waiting = Some((Wait::Client, Instant::now()));
                    return Poll::Pending;
                }
            }
        }
    }

    /// Hands the held batch to the check. `false` means the queue is closed:
    /// the check has finished, refused or died, and its verdict is ready.
    fn send_batch(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        let Engine::Offload { tx, .. } = &mut self.engine else {
            unreachable!("only an offloaded check has a batch to send");
        };
        let Some(tx) = tx else {
            return Poll::Ready(false);
        };
        match tx.poll_reserve(cx) {
            Poll::Pending => {
                self.waiting = Some((Wait::Verify, Instant::now()));
                Poll::Pending
            }
            Poll::Ready(Ok(())) => {
                let batch = std::mem::take(&mut self.batch);
                self.batch_bytes = 0;
                match tx.send_item(batch.clone()) {
                    Ok(()) => {
                        self.ready.extend(batch);
                        Poll::Ready(true)
                    }
                    Err(_) => Poll::Ready(false),
                }
            }
            Poll::Ready(Err(_)) => Poll::Ready(false),
        }
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
        // A wait ends when the wake that brought us back here arrives, so the
        // time is counted once per wait rather than once per chunk.
        if let Some((wait, since)) = this.waiting.take() {
            let waited = since.elapsed();
            match wait {
                Wait::Client => this.client_wait += waited,
                Wait::Verify => this.verify_wait += waited,
            }
        }
        if this.outcome.is_some() {
            return Poll::Ready(None);
        }
        if matches!(this.engine, Engine::Inline(_)) {
            this.poll_inline(cx)
        } else {
            this.poll_offloaded(cx)
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
        let hash = garret_common::sha256_nix32(&Sha256::digest(&nar));
        (nar, compressed, hash, 300_000 * 4)
    }

    /// Streams `compressed` through the verifier in small chunks; returns
    /// the bytes that came out and how the stream ended.
    ///
    /// Both engines run, and must agree on the verdict.
    async fn run(compressed: &[u8], hash: &str, size: i64) -> (Vec<u8>, Option<Outcome>) {
        let inline = run_on(compressed, hash, size, false).await;
        let offloaded = run_on(compressed, hash, size, true).await;
        assert_eq!(inline.1, offloaded.1, "the engines disagree");
        if inline.1 == Some(Outcome::Accepted) {
            assert_eq!(inline.0, offloaded.0);
        }
        inline
    }

    async fn run_on(
        compressed: &[u8],
        hash: &str,
        size: i64,
        offload: bool,
    ) -> (Vec<u8>, Option<Outcome>) {
        let chunks: Vec<Result<Bytes, std::io::Error>> = compressed
            .chunks(1000)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let mut nar =
            VerifiedNar::with_offload(futures::stream::iter(chunks), hash, size, offload).unwrap();
        let mut passed = Vec::new();
        while let Some(item) = nar.next().await {
            match item {
                Ok(chunk) => passed.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }
        (passed, nar.into_outcome())
    }

    /// Asserts the stream was refused, and by the check named in `why`: a
    /// refusal for some other reason must not satisfy a test.
    fn assert_refused(outcome: Option<Outcome>, why: &str) {
        let Some(Outcome::Refused(reason)) = outcome else {
            panic!("not refused: {outcome:?}");
        };
        assert!(reason.contains(why), "refused for {reason:?}, not {why:?}");
    }

    #[tokio::test]
    async fn an_honest_nar_passes_through_untouched() {
        let (_, compressed, hash, size) = nar();
        let (passed, outcome) = run(&compressed, &hash, size).await;
        assert_eq!(outcome, Some(Outcome::Accepted));
        assert_eq!(passed, compressed);
    }

    /// A NAR that ends exactly on a 128 KiB output buffer: the decoder is
    /// drained once more, idle, at the frame boundary. That must not read as
    /// "mid-frame" (about 1 in 16k real NARs is sized like this).
    #[tokio::test]
    async fn an_honest_nar_ending_on_a_buffer_boundary_passes() {
        let nar = vec![b'n'; 4 * SCRATCH];
        let compressed = zstd::encode_all(nar.as_slice(), 3).unwrap();
        let hash = garret_common::sha256_nix32(&Sha256::digest(&nar));
        let (passed, outcome) = run(&compressed, &hash, nar.len() as i64).await;
        assert_eq!(outcome, Some(Outcome::Accepted));
        assert_eq!(passed, compressed);
    }

    /// A NAR large enough to span several batches of tiny chunks and to fill
    /// the check's queue, so backpressure is exercised: the check still sees
    /// every byte, and a corrupted one is still refused.
    #[tokio::test]
    async fn a_nar_spanning_several_batches_still_verifies() {
        // Pseudo-random (splitmix64), so zstd cannot shrink it below a batch.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut nar = Vec::with_capacity(6 * 1024 * 1024);
        for _ in 0..6 * 1024 * 1024 {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            nar.push((z ^ (z >> 31)) as u8);
        }
        let compressed = zstd::encode_all(nar.as_slice(), 1).unwrap();
        assert!(
            compressed.len() > 2 * BATCH_BYTES,
            "{} bytes is not several batches",
            compressed.len()
        );
        let hash = garret_common::sha256_nix32(&Sha256::digest(&nar));
        let chunks = |bytes: &[u8]| -> Vec<Result<Bytes, std::io::Error>> {
            bytes
                .chunks(1000)
                .map(|c| Ok(Bytes::copy_from_slice(c)))
                .collect()
        };

        let mut verified = VerifiedNar::with_offload(
            futures::stream::iter(chunks(&compressed)),
            &hash,
            nar.len() as i64,
            true,
        )
        .unwrap();
        let mut passed = Vec::new();
        while let Some(item) = verified.next().await {
            passed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(verified.into_outcome(), Some(Outcome::Accepted));
        assert_eq!(passed, compressed);

        // One flipped byte in the NAR, with the original claim: refused.
        let mut corrupt = nar.clone();
        corrupt[nar.len() / 2] ^= 0xff;
        let corrupt = zstd::encode_all(corrupt.as_slice(), 1).unwrap();
        let mut verified = VerifiedNar::with_offload(
            futures::stream::iter(chunks(&corrupt)),
            &hash,
            nar.len() as i64,
            true,
        )
        .unwrap();
        while let Some(item) = verified.next().await {
            if item.is_err() {
                break;
            }
        }
        assert_refused(verified.into_outcome(), "NarHash is");
    }

    #[tokio::test]
    async fn a_wrong_nar_hash_is_refused_at_end_of_stream() {
        let (_, compressed, _, size) = nar();
        let other = garret_common::sha256_nix32(&Sha256::digest(b"other"));
        let (_, refused) = run(&compressed, &other, size).await;
        assert_refused(refused, "NarHash is");
    }

    #[tokio::test]
    async fn a_wrong_nar_size_is_refused_either_way() {
        let (_, compressed, hash, size) = nar();
        // Claimed too small: refused as soon as the NAR outgrows it.
        let (passed, refused) = run(&compressed, &hash, size - 1).await;
        assert_refused(refused, "larger than its claimed NarSize");
        assert!(passed.len() < compressed.len());
        // Claimed too large: refused at end of stream.
        assert_refused(run(&compressed, &hash, size + 1).await.1, "NarSize is");
    }

    #[tokio::test]
    async fn a_truncated_or_corrupt_stream_is_refused() {
        let (_, compressed, hash, size) = nar();
        let truncated = &compressed[..compressed.len() - 10];
        assert_refused(run(truncated, &hash, size).await.1, "mid-frame");
        let mut trailing = compressed.clone();
        trailing.extend_from_slice(b"not zstd");
        assert_refused(
            run(&trailing, &hash, size).await.1,
            "not a valid zstd stream",
        );
        assert_refused(run(b"", &hash, size).await.1, "mid-frame");
    }

    /// Skippable frames decode to nothing, so only the compressed-byte bound
    /// stops a body padded with them.
    #[tokio::test]
    async fn a_body_padded_beyond_the_compression_bound_is_refused() {
        let (_, compressed, hash, size) = nar();
        assert!(compressed.len() <= max_compressed_len(size));
        let mut padded = compressed.clone();
        // One skippable frame (magic 0x184D2A50) larger than the bound's slack over the NAR.
        padded.extend_from_slice(&0x184D_2A50u32.to_le_bytes());
        padded.extend_from_slice(&(2u32 * 1024 * 1024).to_le_bytes());
        padded.extend(std::iter::repeat_n(0u8, 2 * 1024 * 1024));
        let (_, refused) = run(&padded, &hash, size).await;
        assert_refused(refused, "zstd bound");
    }

    #[tokio::test]
    async fn a_window_wider_than_the_budget_is_refused() {
        // 16 MiB: one step past the 8 MiB each upload's decoder may hold.
        let (nar, _, hash, size) = nar();
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.window_log(WINDOW_LOG_MAX + 1).unwrap();
        std::io::Write::write_all(&mut encoder, &nar).unwrap();
        let wide = encoder.finish().unwrap();
        assert_refused(run(&wide, &hash, size).await.1, "not a valid zstd stream");
    }

    /// A check thread that dies (panic, runtime shutdown) is the server's
    /// fault: the push must fail with a 500 the client retries, not a 400
    /// it treats as final.
    #[tokio::test]
    async fn a_failed_check_thread_is_a_server_fault_not_a_refusal() {
        let (tx, rx) = mpsc::channel::<Vec<Bytes>>(1);
        let (verdict_tx, verdict) = oneshot::channel();
        drop((rx, verdict_tx));
        let chunks = vec![Ok::<_, std::io::Error>(Bytes::from_static(b"nar"))];
        let mut nar = VerifiedNar {
            inner: futures::stream::iter(chunks),
            engine: Engine::Offload {
                tx: Some(PollSender::new(tx)),
                verdict,
            },
            pending: None,
            batch: Vec::new(),
            batch_bytes: 0,
            ready: VecDeque::new(),
            eof: false,
            outcome: None,
            waiting: None,
            client_wait: Duration::ZERO,
            verify_wait: Duration::ZERO,
        };
        assert!(nar.next().await.unwrap().is_err());
        assert!(
            nar.next().await.is_none(),
            "the stream ends after the failure"
        );
        assert!(
            matches!(nar.into_outcome(), Some(Outcome::Failed(_))),
            "a server fault, not a refusal"
        );
    }

    /// A body that goes quiet is the client's wait, not the check's: the
    /// split is what the upload wait-time counters report.
    #[tokio::test]
    async fn a_stalled_body_reports_its_client_wait() {
        const STALL: Duration = Duration::from_millis(100);
        let (_, compressed, hash, size) = nar();
        let half = compressed.len() / 2;
        let (first, second) = (compressed[..half].to_vec(), compressed[half..].to_vec());
        let body =
            futures::stream::once(async move { Ok::<_, std::io::Error>(Bytes::from(first)) })
                .chain(futures::stream::once(async move {
                    tokio::time::sleep(STALL).await;
                    Ok(Bytes::from(second))
                }));
        let mut nar = VerifiedNar::with_offload(Box::pin(body), &hash, size, false).unwrap();
        while let Some(item) = nar.next().await {
            item.unwrap();
        }
        let (client_wait, verify_wait) = nar.waits();
        assert_eq!(nar.into_outcome(), Some(Outcome::Accepted));
        assert!(
            client_wait >= STALL / 2,
            "client wait {client_wait:?} is under half the stall"
        );
        assert!(
            client_wait < STALL * 20,
            "client wait {client_wait:?} is implausible"
        );
        assert_eq!(verify_wait, Duration::ZERO, "an inline check never waits");
    }

    /// A body that breaks off with a client error reached no verdict: it is
    /// neither accepted nor refused, and the store fails on the error itself.
    #[tokio::test]
    async fn a_client_body_error_leaves_no_outcome() {
        let (_, compressed, hash, size) = nar();
        for offload in [false, true] {
            let chunks = vec![
                Ok(Bytes::copy_from_slice(&compressed[..compressed.len() / 2])),
                Err(std::io::Error::other("client went away")),
            ];
            let mut nar =
                VerifiedNar::with_offload(futures::stream::iter(chunks), &hash, size, offload)
                    .unwrap();
            // Whatever was read before the break may or may not be passed on
            // (the offloaded path still holds it in a batch); the error is
            // what the caller must see, and it leaves no verdict.
            let error = loop {
                match nar.next().await {
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => break e,
                    None => panic!("the stream ended without the client error"),
                }
            };
            assert!(error.to_string().contains("client went away"), "{error}");
            assert_eq!(nar.into_outcome(), None, "offload: {offload}");
        }
    }
}
