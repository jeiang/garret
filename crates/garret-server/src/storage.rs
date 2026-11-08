//! S3 blob storage (MEGA S4 in production, versitygw locally). Spec 03-storage.

use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context as _, Result, anyhow, bail};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    error::SdkError,
    presigning::PresigningConfig,
    primitives::{ByteStream, SdkBody},
    types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier},
};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tracing::Instrument as _;

use crate::config::S3Config;

/// An S3 client bound to the cache's bucket. Cheap to clone — the inner
/// client is already shared.
#[derive(Clone)]
pub struct Storage {
    client: Client,
    bucket: String,
    sign_payload: bool,
}

/// Bounds every upload's memory. The shared semaphore counts bytes in
/// fixed-size units, and a buffer holds the units covering its capacity, so
/// the sum of all upload buffers never exceeds `max_in_flight_bytes` while a
/// small NAR costs a few units instead of a whole part (spec 03).
pub struct UploadLimits {
    /// Bytes per part; also the single-`PutObject` threshold (spec 03).
    pub part_size: usize,
    /// Concurrent part uploads per NAR.
    pub max_parts_in_flight: usize,
    /// How long a body may send nothing before the upload is abandoned.
    pub body_idle: Duration,
    /// One permit per `unit` bytes of buffer, shared process-wide.
    part_slots: Arc<Semaphore>,
    /// Bytes one permit stands for: the power of two at or below
    /// `part_size / 256`, at most 64 KiB.
    unit: usize,
    /// Permits making up one part.
    part_units: usize,
    /// Permits in the whole budget.
    total_units: usize,
}

impl UploadLimits {
    /// `max_in_flight_bytes` becomes a whole number of units, never fewer
    /// than one part's worth so a cap below one part cannot deadlock every
    /// upload.
    pub fn new(
        part_size: usize,
        max_parts_in_flight: usize,
        max_in_flight_bytes: u64,
        body_idle: Duration,
    ) -> Self {
        let (unit, part_units, total_units) = budget(part_size, max_in_flight_bytes);
        Self {
            part_size,
            max_parts_in_flight,
            body_idle,
            part_slots: Arc::new(Semaphore::new(total_units)),
            unit,
            part_units,
            total_units,
        }
    }

    /// How many part-sized buffers fit in the budget at once, process-wide.
    pub fn total_slots(&self) -> usize {
        self.total_units / self.part_units
    }

    /// Bytes of buffer currently reserved, process-wide.
    #[cfg(test)]
    fn in_flight_bytes(&self) -> u64 {
        (self.total_units - self.part_slots.available_permits()) as u64 * self.unit as u64
    }

    async fn acquire(&self, bytes: usize) -> Result<Reservation> {
        let units = units_for(bytes, self.unit, self.part_units);
        let started = std::time::Instant::now();
        let permit = self
            .part_slots
            .clone()
            .acquire_many_owned(units as u32)
            .await
            .map_err(|_| anyhow!("upload capacity semaphore closed"))?;
        metrics::histogram!("garret_upload_buffer_wait_seconds").record(started.elapsed());
        let bytes = (units * self.unit) as f64;
        metrics::gauge!("garret_in_flight_bytes").increment(bytes);
        Ok(Reservation {
            _permit: permit,
            bytes,
        })
    }
}

/// Buffer capacity taken from [`UploadLimits`], counted in
/// `garret_in_flight_bytes` until it is dropped.
struct Reservation {
    _permit: OwnedSemaphorePermit,
    bytes: f64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        metrics::gauge!("garret_in_flight_bytes").decrement(self.bytes);
    }
}

/// The byte budget in permits: `(unit, part_units, total_units)`. The unit
/// is the power of two at or below `part_size / 256`, at most 64 KiB, so
/// every count below is a shift: the Kani proofs then cover every part size
/// S3 accepts, which they cannot through a general division.
fn budget(part_size: usize, max_in_flight_bytes: u64) -> (usize, usize, usize) {
    let shift = (part_size >> 8).clamp(1, 64 * 1024).ilog2();
    let unit = 1 << shift;
    let part_units = units_ceil(part_size, shift);
    let total_units = ((max_in_flight_bytes >> shift) as usize).max(part_units);
    (unit, part_units, total_units)
}

fn units_ceil(bytes: usize, shift: u32) -> usize {
    (bytes >> shift) + usize::from(bytes & ((1 << shift) - 1) != 0)
}

/// Permits covering `bytes` of buffer: at least one, at most a part.
fn units_for(bytes: usize, unit: usize, part_units: usize) -> usize {
    units_ceil(bytes, unit.trailing_zeros()).clamp(1, part_units)
}

/// Times one S3 request and, on failure, counts it by operation and error
/// kind, so a timeout, a refused connection and a 5xx tell apart.
async fn timed<T, E>(
    op: &'static str,
    fut: impl Future<Output = Result<T, SdkError<E>>>,
) -> Result<T, SdkError<E>> {
    let started = Instant::now();
    // One client span per S3 request, named for the operation in
    // `s3.operation`; the span is the S3 wait a trace otherwise hides.
    let span = tracing::debug_span!(
        "s3.request",
        otel.kind = "client",
        s3.operation = op,
        otel.status_code = tracing::field::Empty,
    );
    let result = fut.instrument(span.clone()).await;
    metrics::histogram!("garret_s3_request_duration_seconds", "operation" => op)
        .record(started.elapsed());
    if let Err(e) = &result {
        span.record("otel.status_code", "error");
        metrics::counter!("garret_s3_errors_total", "operation" => op, "kind" => error_kind(e))
            .increment(1);
    }
    result
}

fn error_kind<E>(e: &SdkError<E>) -> &'static str {
    match e {
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(_) => "dispatch",
        SdkError::ResponseError(_) => "response",
        SdkError::ServiceError(_) => "service",
        _ => "other",
    }
}

/// The blob key for an object: flat `nar/<hash>.nar.zst`, derivable from the
/// DB row and vice versa (spec 03).
pub fn key_for(store_path_hash: &str) -> String {
    format!("nar/{store_path_hash}.nar.zst")
}

/// The store-path hash a blob key encodes — the inverse of [`key_for`].
/// `None` for anything outside the flat `nar/<hash>.nar.zst` layout.
pub fn hash_for(key: &str) -> Option<&str> {
    key.strip_prefix("nar/")
        .and_then(|k| k.strip_suffix(".nar.zst"))
        .filter(|h| !h.is_empty())
}

/// One blob from [`BlobLister::next_page`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// The store-path hash its key encodes ([`hash_for`]).
    pub hash: String,
    /// Time since S3 last modified it.
    pub age: Duration,
    /// Its size in bytes.
    pub size: i64,
}

/// A blob with no row is an orphan candidate, unless an upload owns the path
/// or the blob is younger than `grace`: a row lands only after its blob
/// completes, so a rowless blob may just be mid-push. Shared by the GC orphan
/// sweep and `garret-admin fsck`'s read-only mirror-image check.
pub fn is_orphan(blob: &Blob, in_flight: &crate::inflight::InFlight, grace: Duration) -> bool {
    !in_flight.contains(&blob.hash) && blob.age >= grace
}

/// A window over the object rows, pulled in `store_path_hash` order as the
/// bucket listing advances. The sweep never materializes the table and never
/// holds the connection across an S3 await: the caller fetches one page
/// ([`crate::db::objects_after`]) whenever [`RowWindow::needs_through`] says
/// the window does not reach the listing yet.
#[derive(Default)]
pub struct RowWindow {
    rows: std::collections::VecDeque<crate::db::ObjectBrief>,
    /// The last key fetched; `None` before the first page.
    last: Option<String>,
    /// The last page was short, so the table has no more rows.
    done: bool,
}

impl RowWindow {
    /// An empty window: no rows fetched yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The cursor for the next fetch: rows after this key.
    pub fn cursor(&self) -> Option<&str> {
        self.last.as_deref()
    }

    /// True when rows up to and including `hash` may not all be in the window
    /// yet, so the caller must fetch another page.
    pub fn needs_through(&self, hash: &str) -> bool {
        !self.done && self.last.as_deref().is_none_or(|last| last < hash)
    }

    /// True when the table may have rows that were never fetched.
    pub fn more(&self) -> bool {
        !self.done
    }

    /// Appends a fetched page; a short page means the table is exhausted.
    pub fn extend(&mut self, page: Vec<crate::db::ObjectBrief>) {
        let more = page.len() == crate::db::ROW_PAGE;
        if let Some(last) = page.last() {
            self.last = Some(last.store_path_hash.clone());
        }
        self.rows.extend(page);
        self.done = !more;
    }

    /// Pops the next row when it sorts before `hash`: a row with no blob.
    pub fn pop_before(&mut self, hash: &str) -> Option<crate::db::ObjectBrief> {
        match self.rows.front() {
            Some(row) if row.store_path_hash.as_str() < hash => self.rows.pop_front(),
            _ => None,
        }
    }

    /// Pops the row whose key is `hash`, if the window holds it.
    pub fn pop_equal(&mut self, hash: &str) -> Option<crate::db::ObjectBrief> {
        match self.rows.front() {
            Some(row) if row.store_path_hash == hash => self.rows.pop_front(),
            _ => None,
        }
    }

    /// Pops every remaining row, in order.
    pub fn drain(&mut self) -> impl Iterator<Item = crate::db::ObjectBrief> + '_ {
        self.rows.drain(..)
    }
}

/// One page of the sorted merge: every blob classified against the rows that
/// sort with it.
#[derive(Debug, Default)]
pub struct Merged<'a> {
    /// Blobs with no row; [`is_orphan`] applies the remaining guards.
    pub orphans: Vec<&'a Blob>,
    /// Blobs with their row.
    pub pairs: Vec<(&'a Blob, crate::db::ObjectBrief)>,
    /// Rows with no blob.
    pub dangling: Vec<crate::db::ObjectBrief>,
}

/// Merges one page of blobs against the row window, advancing it. Rows before
/// the page's first blob are dangling too; rows past its last blob stay for
/// the next page. The caller must have fetched rows through the page's last
/// key ([`RowWindow::needs_through`]).
pub fn merge_page<'a>(page: &'a [Blob], rows: &mut RowWindow) -> Merged<'a> {
    let mut merged = Merged::default();
    for blob in page {
        while let Some(row) = rows.pop_before(&blob.hash) {
            merged.dangling.push(row);
        }
        match rows.pop_equal(&blob.hash) {
            Some(row) => merged.pairs.push((blob, row)),
            None => merged.orphans.push(blob),
        }
    }
    merged
}

/// Pages through the bucket's `nar/` listing in key order.
///
/// The merge against the object rows assumes `ListObjectsV2` returns keys in
/// UTF-8 byte order. S3 does, and `nar/<hash>.nar.zst` keys preserve hash
/// order, so a page that breaks it — an implementation that does not sort —
/// fails the walk instead of misclassifying every key after it.
pub struct BlobLister<'a> {
    storage: &'a Storage,
    token: Option<String>,
    last_key: Option<String>,
    done: bool,
}

impl BlobLister<'_> {
    /// The next page of blobs, or `None` once the bucket is listed. Keys
    /// outside the `nar/<hash>.nar.zst` layout are skipped, but still checked
    /// for order.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Blob>>> {
        if self.done {
            return Ok(None);
        }
        let page = timed(
            "list",
            self.storage
                .client
                .list_objects_v2()
                .bucket(&self.storage.bucket)
                .prefix("nar/")
                .set_continuation_token(self.token.take())
                .send(),
        )
        .await
        .context("listing blobs")?;

        let mut blobs = Vec::new();
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            if let Some(last) = &self.last_key
                && key <= last.as_str()
            {
                bail!(
                    "bucket listing is not sorted: {key} came after {last}; the orphan \
                     sweep and fsck merge the listing against the database and need \
                     ListObjectsV2 to return keys in lexicographic order"
                );
            }
            self.last_key = Some(key.to_owned());
            let Some(hash) = hash_for(key) else { continue };
            let size = object
                .size()
                .with_context(|| format!("listing blobs: no size for {key}"))?;
            blobs.push(Blob {
                hash: hash.to_owned(),
                age: age(object.last_modified()),
                size,
            });
        }

        self.token = page.next_continuation_token().map(str::to_owned);
        if self.token.is_none() {
            // A truncated page with no token would silently end the listing,
            // and every row past it would read as dangling.
            if page.is_truncated() == Some(true) {
                bail!("listing blobs: truncated page without a continuation token");
            }
            self.done = true;
        }
        Ok(Some(blobs))
    }
}

/// Time since an S3 timestamp; zero when it is missing or in the future.
fn age(at: Option<&aws_sdk_s3::primitives::DateTime>) -> Duration {
    at.and_then(|t| SystemTime::try_from(*t).ok())
        .and_then(|t| SystemTime::now().duration_since(t).ok())
        .unwrap_or(Duration::ZERO)
}

/// The client's request body stalled or broke mid-upload: a failure on the
/// sender's side, not the server's. Handlers downcast to it to log such
/// failures as warnings rather than server errors.
#[derive(Debug)]
pub struct ClientBodyError(pub String);

impl std::fmt::Display for ClientBodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ClientBodyError {}

/// The body is larger than the caller allows for its claimed size: a client
/// error that retrying cannot fix, unlike [`ClientBodyError`].
#[derive(Debug)]
pub struct BodyOverLimit(pub String);

impl std::fmt::Display for BodyOverLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BodyOverLimit {}

/// An S3 request on the upload path failed. Attached as context over the
/// SDK error, whose type is generic and cannot itself be downcast.
#[derive(Debug)]
pub struct S3Failure(pub String);

impl std::fmt::Display for S3Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where an upload body stood when it went quiet, for the stall error.
#[derive(Clone, Copy)]
pub struct Stall {
    /// Body bytes received before the read being waited on.
    pub received: u64,
    /// When the upload began.
    pub started: Instant,
    /// `preamble`, `first_part` or `multipart`.
    pub phase: &'static str,
}

/// The body's next chunk, or a [`ClientBodyError`] once it has sent nothing
/// for `idle`.
pub async fn next_chunk<S: Stream + Unpin>(
    body: &mut S,
    idle: Duration,
    at: Stall,
) -> Result<Option<S::Item>> {
    tokio::time::timeout(idle, body.next()).await.map_err(|_| {
        anyhow::Error::new(ClientBodyError(format!(
            "upload body stalled: nothing received for {}s after {} bytes in {}s ({})",
            idle.as_secs(),
            at.received,
            at.started.elapsed().as_secs(),
            at.phase
        )))
    })
}

/// One part's bytes: the body chunks that make it up, in order, and their
/// exact total. A chunk is a `Bytes` slice of the request body, so a part is
/// never copied into a contiguous buffer — the SDK streams the chunks as they
/// are. `len` is what S3's uniform-part-size rule and `Content-Length` need.
#[derive(Debug, Default)]
struct Part {
    chunks: Vec<Bytes>,
    len: usize,
}

impl Part {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The part as one buffer. Only the single-`PutObject` path needs this
    /// (a body below one part), and it is free when hyper delivered the body
    /// in one chunk.
    fn into_bytes(mut self) -> Bytes {
        match self.chunks.len() {
            0 => Bytes::new(),
            1 => self.chunks.pop().expect("one chunk"),
            _ => {
                let mut buf = Vec::with_capacity(self.len);
                for chunk in &self.chunks {
                    buf.extend_from_slice(chunk);
                }
                Bytes::from(buf)
            }
        }
    }
}

/// A part's chunks as an `http_body::Body`, so the SDK reads them straight
/// out of the request buffers. `remaining` is the exact size hint the SDK
/// turns into `Content-Length`.
struct Chunks {
    chunks: std::vec::IntoIter<Bytes>,
    remaining: u64,
}

impl http_body::Body for Chunks {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        Poll::Ready(this.chunks.next().map(|chunk| {
            this.remaining -= chunk.len() as u64;
            Ok(http_body::Frame::data(chunk))
        }))
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// The part as a retryable streaming SDK body. Rebuilding it only clones the
/// chunk list (refcount bumps), so an SDK retry re-sends the same bytes — the
/// guarantee `ByteStream::from(Bytes)` gives for the contiguous paths.
fn part_body(chunks: Vec<Bytes>, len: usize) -> SdkBody {
    SdkBody::retryable(move || {
        SdkBody::from_body_1_x(Chunks {
            chunks: chunks.clone().into_iter(),
            remaining: len as u64,
        })
    })
}

/// CRC32 of a part, base64-encoded the way S3's `x-amz-checksum-crc32`
/// header wants it. Computing it here, rather than letting the SDK checksum a
/// streaming body, keeps the request from switching to aws-chunked encoding
/// with a trailer.
fn checksum_crc32(chunks: &[Bytes]) -> String {
    let mut hasher = crc32fast::Hasher::new();
    for chunk in chunks {
        hasher.update(chunk);
    }
    B64.encode(hasher.finalize().to_be_bytes())
}

/// The upload body as consumed so far. It owns the read cursor (an overshoot
/// `carry` left by a split chunk), the running hash and the byte count, so
/// FileHash and FileSize (spec 03) always describe the same bytes: exactly
/// those appended to a part.
struct BodyReader<S> {
    body: S,
    carry: Option<Bytes>,
    hasher: Sha256,
    /// Bytes appended to parts so far.
    received: u64,
    started: Instant,
    idle: Duration,
}

impl<S, E> BodyReader<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    fn new(body: S, idle: Duration) -> Self {
        Self {
            body,
            carry: None,
            hasher: Sha256::new(),
            received: 0,
            started: Instant::now(),
            idle,
        }
    }

    /// The next chunk not yet appended: the carry first, so leftover bytes
    /// are never skipped, then the body. Neither hashed nor counted here.
    async fn next(&mut self, phase: &'static str) -> Result<Option<Bytes>> {
        if let Some(left) = self.carry.take() {
            return Ok(Some(left));
        }
        let at = Stall {
            received: self.received,
            started: self.started,
            phase,
        };
        match next_chunk(&mut self.body, self.idle, at).await? {
            None => Ok(None),
            Some(chunk) => chunk.map(Some).map_err(|e| {
                anyhow::Error::new(ClientBodyError(format!("reading request body: {e}")))
            }),
        }
    }

    /// Hashes and counts `bytes` as they go into `part`, while still
    /// cache-hot. The chunk is moved, not copied: it is a slice of hyper's
    /// read buffer and stays one.
    fn append(&mut self, part: &mut Part, bytes: Bytes) {
        self.hasher.update(&bytes);
        self.received += bytes.len() as u64;
        part.len += bytes.len();
        part.chunks.push(bytes);
    }

    /// Appends body chunks to `part` until it holds `cap` bytes or the body
    /// ends, keeping any overshoot as the carry. `part` must have been
    /// reserved against a permit: a part slot accounts for the whole part.
    async fn fill(&mut self, part: &mut Part, cap: usize, phase: &'static str) -> Result<()> {
        while part.len < cap {
            let Some(mut chunk) = self.next(phase).await? else {
                break;
            };
            // Empty chunks carry nothing and would only add a frame to the
            // SDK body; the old buffer copy skipped them implicitly.
            if chunk.is_empty() {
                continue;
            }
            // A chunk can span several parts, so it is split at the part
            // boundary: a part over part_size would abort the upload. The
            // split is a `Bytes` slice, so neither half is copied.
            let room = cap - part.len;
            if chunk.len() > room {
                let head = chunk.split_to(room);
                self.append(part, head);
                self.carry = Some(chunk);
                break;
            }
            self.append(part, chunk);
        }
        Ok(())
    }

    /// Whether the body holds another byte, without consuming it: a
    /// non-empty chunk read here becomes the carry. Empty chunks are skipped.
    async fn has_more(&mut self, phase: &'static str) -> Result<bool> {
        while let Some(chunk) = self.next(phase).await? {
            if !chunk.is_empty() {
                self.carry = Some(chunk);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn finish(self) -> (Vec<u8>, i64) {
        (self.hasher.finalize().to_vec(), self.received as i64)
    }
}

/// Takes the next finished part, adding the wait for it to `waited`: S3 part
/// backpressure and the final drain both show up here.
async fn collect(
    in_flight: &mut JoinSet<Result<CompletedPart>>,
    waited: &mut Duration,
) -> Result<CompletedPart> {
    let started = Instant::now();
    let joined = in_flight.join_next().await;
    *waited += started.elapsed();
    joined
        .context("no part upload was in flight")?
        .context("part upload task failed")?
}

impl Storage {
    /// Builds a client from config: explicit credentials when set, otherwise
    /// the AWS default chain (env vars via EnvironmentFile in production).
    pub async fn new(cfg: &S3Config) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(
            aws_sdk_s3::config::Region::new(
                cfg.region.clone().unwrap_or_else(|| "us-east-1".into()),
            ),
        );
        if let (Some(id), Some(secret)) = (&cfg.access_key_id, &cfg.secret_access_key) {
            loader = loader.credentials_provider(Credentials::new(
                id.clone(),
                secret.clone(),
                None,
                None,
                "garret-config",
            ));
        }
        let mut builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        if let Some(url) = &cfg.endpoint_url {
            builder = builder.endpoint_url(url.clone());
        }
        // S4 and versitygw both accept path-style; it avoids DNS games for dotted buckets.
        builder = builder.force_path_style(cfg.path_style);
        // The overall deadline covers the SDK's retries; the optional attempt
        // deadline bounds each try, so a hung attempt leaves time for a retry
        // inside the overall one. UploadPart is never retried by the SDK (see
        // `upload_part`): re-uploading a part is forbidden on S4 (spec 03), so
        // it fails once and the caller aborts the multipart.
        let mut timeouts = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
            .operation_timeout(Duration::from_secs(cfg.operation_timeout_secs));
        if let Some(secs) = cfg.operation_attempt_timeout_secs {
            timeouts = timeouts.operation_attempt_timeout(Duration::from_secs(secs));
        }
        builder = builder.timeout_config(timeouts.build());
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
            sign_payload: cfg.sign_payload,
        })
    }

    /// Single-request `PutObject` for bodies already in memory (at most one
    /// part; larger uploads go through [`Storage::put_streaming`]).
    #[tracing::instrument(name = "s3.put", level = "debug", skip_all, fields(s3.key = key, blob.bytes = body.len()))]
    pub async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        let bytes = body.len() as u64;
        let put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .customize();
        let put = if self.sign_payload {
            put
        } else {
            put.disable_payload_signing()
        };
        timed("put", put.send())
            .await
            .inspect_err(|_| metrics::counter!("garret_s3_put_failures_total").increment(1))
            .with_context(|| S3Failure(format!("uploading {key}")))?;
        metrics::counter!("garret_s3_puts_total").increment(1);
        metrics::counter!("garret_s3_bytes_uploaded_total").increment(bytes);
        Ok(())
    }

    /// Streams a body to S3, hashing the bytes as they pass (spec 03-storage).
    /// Returns the sha256 of exactly what was stored, and its length.
    ///
    /// One part is held at a time — as the request body's own chunks, never a
    /// copy of them — and a global permit is taken **before** each read, so
    /// the reader can never race ahead of S3 and server memory stays bounded
    /// by configuration rather than by client behaviour. Parts upload on their
    /// own tasks while the next one is read.
    ///
    /// `first_part_hint` is the most bytes the body is expected to hold. The
    /// first buffer reserves only that much (capped at a part), so a small NAR
    /// neither allocates nor holds a whole part slot. A body shorter than a
    /// part that outgrows the hint is refused: the buffer never exceeds the
    /// bytes charged to the permit.
    #[tracing::instrument(name = "s3.put_streaming", level = "debug", skip_all, fields(s3.key = key, blob.bytes = tracing::field::Empty))]
    pub async fn put_streaming<S, E>(
        &self,
        key: &str,
        body: S,
        limits: &UploadLimits,
        first_part_hint: usize,
    ) -> Result<(Vec<u8>, i64)>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut part_wait = Duration::ZERO;
        let stored = self
            .put_streaming_inner(key, body, limits, first_part_hint, &mut part_wait)
            .await;
        // Once per upload, failed ones included: the wait is what tells a
        // slow store apart from a slow client. Milliseconds, not seconds:
        // the metrics crate's counters take integers only, and a sub-second
        // upload would otherwise record nothing.
        metrics::counter!("garret_upload_s3_part_wait_milliseconds_total")
            .increment(part_wait.as_millis() as u64);
        if let Ok((_, size)) = &stored {
            tracing::Span::current().record("blob.bytes", *size);
        }
        stored
    }

    async fn put_streaming_inner<S, E>(
        &self,
        key: &str,
        body: S,
        limits: &UploadLimits,
        first_part_hint: usize,
        part_wait: &mut Duration,
    ) -> Result<(Vec<u8>, i64)>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut reader = BodyReader::new(body, limits.body_idle);

        // First part decides the shape: if the whole body fits, one PutObject.
        let reserve = first_part_hint.clamp(1, limits.part_size);
        let permit = limits.acquire(reserve).await?;
        let mut first = Part::default();
        reader.fill(&mut first, reserve, "first_part").await?;
        // The hint is a hard cap for a body that is not a whole part: more
        // data means the body exceeds what the caller will accept, so fail
        // rather than wait for more memory while holding a partial permit.
        if first.len == reserve
            && reserve < limits.part_size
            && reader.has_more("first_part").await?
        {
            return Err(anyhow::Error::new(BodyOverLimit(format!(
                "upload body exceeds the {reserve}-byte limit for its size"
            ))));
        }
        if first.len < limits.part_size {
            self.put(key, first.into_bytes()).await?;
            drop(permit);
            return Ok(reader.finish());
        }

        let upload_id = timed(
            "create_mpu",
            self.client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .send(),
        )
        .await
        .with_context(|| S3Failure(format!("starting multipart upload for {key}")))?
        .upload_id
        .context(S3Failure("S3 returned no upload id".into()))?;
        metrics::counter!("garret_s3_multipart_started_total").increment(1);

        // Any failure past this point must abort, or the parts linger and bill.
        let mut parts = JoinSet::new();
        parts.spawn(self.upload_part(key, &upload_id, 1, first, permit));
        match self
            .multipart_body(key, &upload_id, &mut parts, &mut reader, limits, part_wait)
            .await
        {
            Ok(()) => Ok(reader.finish()),
            Err(e) => {
                // Let the parts already sent finish first: one landing after
                // the abort would re-create what the abort freed. Cancelling
                // the task is not enough, because S3 keeps processing a request
                // whose body it has. Each part is bounded by the operation
                // timeout, so this wait is too.
                while parts.join_next().await.is_some() {}
                metrics::counter!("garret_s3_multipart_aborted_total").increment(1);
                if let Err(abort) = timed(
                    "abort_mpu",
                    self.client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send(),
                )
                .await
                {
                    tracing::error!("aborting multipart for {key} also failed: {abort}");
                }
                Err(e)
            }
        }
    }

    /// Reads and uploads parts 2..N; part 1 is already in `in_flight`.
    ///
    /// Each part uploads on its own task, so it finishes, and frees its
    /// permit, whether or not this loop is waiting. A permit is only ever
    /// held by a part being read or uploaded, never by a waiter, so uploads
    /// sharing the budget cannot wedge each other — whatever the slot count.
    async fn multipart_body<S, E>(
        &self,
        key: &str,
        upload_id: &str,
        in_flight: &mut JoinSet<Result<CompletedPart>>,
        reader: &mut BodyReader<S>,
        limits: &UploadLimits,
        part_wait: &mut Duration,
    ) -> Result<()>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut completed: Vec<CompletedPart> = Vec::new();
        let mut part_number = 1;

        loop {
            // Room, then permit, then read: a part is never buffered before it
            // can be sent, so one NAR holds at most max_parts_in_flight slots.
            while in_flight.len() >= limits.max_parts_in_flight {
                completed.push(collect(in_flight, part_wait).await?);
            }
            let permit = limits.acquire(limits.part_size).await?;
            let mut part = Part::default();
            reader
                .fill(&mut part, limits.part_size, "multipart")
                .await?;
            if part.is_empty() {
                drop(permit);
                break;
            }
            let is_final = part.len < limits.part_size;

            part_number += 1;
            in_flight.spawn(self.upload_part(key, upload_id, part_number, part, permit));
            if is_final {
                break;
            }
        }

        while !in_flight.is_empty() {
            completed.push(collect(in_flight, part_wait).await?);
        }

        completed.sort_by_key(|p| p.part_number());

        timed(
            "complete_mpu",
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed))
                        .build(),
                )
                .send(),
        )
        .await
        .with_context(|| S3Failure(format!("completing multipart upload for {key}")))?;
        metrics::counter!("garret_s3_multipart_completed_total").increment(1);
        Ok(())
    }

    fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Part,
        permit: Reservation,
    ) -> impl Future<Output = Result<CompletedPart>> + Send + 'static {
        // The part uploads on its own task, where the caller's span is not
        // current: carry it over so the part is a child of the Blob upload.
        let span = tracing::Span::current();
        let (client, bucket, key, upload_id, sign_payload) = (
            self.client.clone(),
            self.bucket.clone(),
            key.to_owned(),
            upload_id.to_owned(),
            self.sign_payload,
        );
        async move {
            let len = body.len;
            let started = Instant::now();
            // The SDK would otherwise checksum a streaming body in a trailer,
            // which switches the request to aws-chunked encoding; a checksum
            // header is what the store expects and what spec 03 promises.
            let crc32 = checksum_crc32(&body.chunks);
            // SigV4 payload signing hashes the whole body, so that mode keeps
            // the contiguous copy; the default path streams the chunks as
            // they arrived.
            let payload = if sign_payload {
                ByteStream::from(body.into_bytes())
            } else {
                ByteStream::from(part_body(body.chunks, len))
            };
            let part = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(payload)
                .content_length(len as i64)
                .checksum_crc32(crc32)
                .customize()
                // S4 forbids re-uploading a part, so the SDK must not retry
                // one: the caller aborts the whole multipart (spec 03-storage).
                .config_override(aws_sdk_s3::config::Config::builder().retry_config(
                    aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                ));
            let part = if sign_payload {
                part
            } else {
                part.disable_payload_signing()
            };
            let output = timed("upload_part", part.send())
                .await
                .with_context(|| S3Failure(format!("uploading part {part_number} of {key}")))?;
            drop(permit);

            metrics::counter!("garret_s3_parts_total").increment(1);
            metrics::counter!("garret_s3_bytes_uploaded_total").increment(len as u64);
            metrics::histogram!("garret_s3_part_duration_seconds").record(started.elapsed());
            Ok(CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(output.e_tag)
                .build())
        }
        .instrument(span)
    }

    /// Every blob under `nar/`, a page at a time, in the bucket's key order.
    /// Paginated — the bucket outgrows one response long before the cache is
    /// interesting — and order-checked, because the sweep and fsck merge the
    /// listing against the object rows instead of materializing either side.
    pub fn list_blobs(&self) -> BlobLister<'_> {
        BlobLister {
            storage: self,
            token: None,
            last_key: None,
            done: false,
        }
    }

    /// Batched at 1,000 keys — the S3 limit for a single `DeleteObjects`.
    ///
    /// A 200 can still report per-key failures. Each one is logged and
    /// counted, and once every batch has been tried the call fails: the rows
    /// are already gone, so a blob that stayed must not pass for a deleted
    /// one (spec 05). A key reported as already absent (`NoSuchKey`, which
    /// some S3 implementations list under `Errors` where S3 lists it as
    /// deleted) is not a failure — absent is what the caller asked for.
    #[tracing::instrument(name = "s3.delete", level = "debug", skip_all, fields(blobs = keys.len()))]
    pub async fn delete_objects(&self, keys: &[String]) -> Result<()> {
        let (mut failed, mut first_failure) = (0, None);
        for batch in keys.chunks(1000) {
            let objects = batch
                .iter()
                .map(|k| ObjectIdentifier::builder().key(k).build())
                .collect::<Result<Vec<_>, _>>()?;
            let requested = objects.len();
            let output = timed(
                "delete",
                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(Delete::builder().set_objects(Some(objects)).build()?)
                    .send(),
            )
            .await
            .context("deleting blobs")?;
            let before = failed;
            for e in output.errors() {
                if e.code() == Some("NoSuchKey") {
                    continue;
                }
                let failure = format!(
                    "{} ({}: {})",
                    e.key().unwrap_or("?"),
                    e.code().unwrap_or("unknown"),
                    e.message().unwrap_or_default()
                );
                tracing::error!("blob not deleted: {failure}");
                failed += 1;
                first_failure.get_or_insert(failure);
            }
            let batch_failed = failed - before;
            metrics::counter!("garret_s3_deletes_total")
                .increment(requested.saturating_sub(batch_failed) as u64);
            metrics::counter!("garret_s3_delete_failures_total").increment(batch_failed as u64);
        }
        if let Some(first) = first_failure {
            bail!("{failed} blob(s) not deleted, first: {first}");
        }
        Ok(())
    }

    /// Aborts multipart uploads older than `grace` that no live upload owns.
    /// Consulting the in-flight set is why GC lives inside the Pusher (spec 05).
    /// Paginated, like [`Storage::list_blobs`]: one listing returns at most
    /// 1,000 uploads.
    #[tracing::instrument(name = "s3.abort_stale_multiparts", level = "debug", skip_all)]
    pub async fn abort_stale_multiparts(
        &self,
        grace: Duration,
        in_flight: &crate::inflight::InFlight,
    ) -> Result<usize> {
        let mut aborted = 0;
        let (mut key_marker, mut upload_id_marker) = (None, None);
        loop {
            let page = timed(
                "list",
                self.client
                    .list_multipart_uploads()
                    .bucket(&self.bucket)
                    .set_key_marker(key_marker.take())
                    .set_upload_id_marker(upload_id_marker.take())
                    .send(),
            )
            .await
            .context("listing multipart uploads")?;

            for upload in page.uploads() {
                let (Some(key), Some(upload_id)) = (upload.key(), upload.upload_id()) else {
                    continue;
                };
                let Some(hash) = hash_for(key) else { continue };
                if in_flight.contains(hash) || age(upload.initiated()) < grace {
                    continue;
                }
                timed(
                    "abort_mpu",
                    self.client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .send(),
                )
                .await
                .with_context(|| format!("aborting stale multipart for {key}"))?;
                aborted += 1;
            }

            if page.is_truncated() != Some(true) {
                break;
            }
            key_marker = page.next_key_marker().map(str::to_owned);
            upload_id_marker = page.next_upload_id_marker().map(str::to_owned);
            // A truncated page without a marker to continue from would list
            // the first page forever.
            if key_marker.is_none() {
                bail!("listing multipart uploads: truncated page without a next marker");
            }
        }
        Ok(aborted)
    }

    /// The Puller redirects here instead of proxying bytes (ADR-0005).
    #[tracing::instrument(name = "s3.presign", level = "debug", skip_all, fields(s3.key = key))]
    pub async fn presigned_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(ttl)?)
            .await
            .with_context(|| format!("presigning {key}"))?;
        Ok(req.uri().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDLE: Duration = Duration::from_secs(60);

    fn blob(hash: &str, age_secs: u64) -> Blob {
        Blob {
            hash: hash.into(),
            age: Duration::from_secs(age_secs),
            size: 1,
        }
    }

    fn row(hash: &str) -> crate::db::ObjectBrief {
        crate::db::ObjectBrief {
            store_path_hash: hash.into(),
            name: "thing".into(),
            created_at: 1,
            file_size: 1,
        }
    }

    #[test]
    fn is_orphan_excludes_in_flight_and_young_blobs() {
        let grace = Duration::from_secs(86400);
        let in_flight = crate::inflight::InFlight::new();
        let _claim = in_flight.claim("uploading").unwrap();

        assert!(!is_orphan(&blob("uploading", 90000), &in_flight, grace));
        assert!(!is_orphan(&blob("young", 10), &in_flight, grace));
        assert!(is_orphan(&blob("stale", 90000), &in_flight, grace));
    }

    /// The walk pages both sides: every key is classified once, across page
    /// boundaries on either side, and the row window only ever holds what the
    /// listing has reached.
    #[test]
    fn the_merge_walk_pages_both_sides_across_boundaries() {
        let key = |i: usize| format!("{i:032x}");
        // A blob with no row every 7th key, a row with no blob every 11th.
        let has_blob = |i: usize| !i.is_multiple_of(11);
        let has_row = |i: usize| !i.is_multiple_of(7);
        let total = 3000;

        let rows: Vec<_> = (0..total)
            .filter(|i| has_row(*i))
            .map(|i| row(&key(i)))
            .collect();
        let blobs: Vec<_> = (0..total)
            .filter(|i| has_blob(*i))
            .map(|i| blob(&key(i), 90000))
            .collect();

        let mut window = RowWindow::new();
        let mut fetched = 0;
        let mut merged = Merged::default();
        for page in blobs.chunks(64) {
            let last = page.last().unwrap();
            while window.needs_through(&last.hash) {
                let end = (fetched + crate::db::ROW_PAGE).min(rows.len());
                window.extend(rows[fetched..end].to_vec());
                fetched = end;
            }
            let page = merge_page(page, &mut window);
            merged.orphans.extend(page.orphans);
            merged.pairs.extend(page.pairs);
            merged.dangling.extend(page.dangling);
        }
        while window.more() {
            let end = (fetched + crate::db::ROW_PAGE).min(rows.len());
            window.extend(rows[fetched..end].to_vec());
            fetched = end;
        }
        merged.dangling.extend(window.drain());

        let expected = |kept: &dyn Fn(usize) -> bool| -> Vec<String> {
            (0..total).filter(|i| kept(*i)).map(key).collect()
        };
        assert_eq!(
            merged
                .orphans
                .iter()
                .map(|b| b.hash.clone())
                .collect::<Vec<_>>(),
            expected(&|i| has_blob(i) && !has_row(i))
        );
        assert_eq!(
            merged
                .dangling
                .iter()
                .map(|r| r.store_path_hash.clone())
                .collect::<Vec<_>>(),
            expected(&|i| has_row(i) && !has_blob(i))
        );
        assert_eq!(
            merged
                .pairs
                .iter()
                .map(|(_, r)| r.store_path_hash.clone())
                .collect::<Vec<_>>(),
            expected(&|i| has_row(i) && has_blob(i))
        );
    }

    #[test]
    fn key_layout_is_flat_and_derivable() {
        assert_eq!(key_for("abc"), "nar/abc.nar.zst");
        assert_eq!(hash_for(&key_for("abc")), Some("abc"));
        assert_eq!(hash_for("other/abc.nar.zst"), None);
    }

    /// One `fill` of a fresh `cap`-sized part, as `multipart_body` does it.
    async fn read_part<S, E>(reader: &mut BodyReader<S>, cap: usize) -> Result<Part>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut part = Part::default();
        reader.fill(&mut part, cap, "multipart").await?;
        Ok(part)
    }

    fn reader_of(
        chunks: Vec<&'static [u8]>,
    ) -> BodyReader<impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin> {
        let body = futures::stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from_static(c))));
        BodyReader::new(body, IDLE)
    }

    /// Feeds `chunks` through part fills and returns the part sizes produced.
    /// S4 requires parts 1..N-1 to be identical in size, so this is the check
    /// that matters most: only the final part may be short.
    /// Also checks that the hash and length cover every byte exactly once.
    fn parts_of(chunks: Vec<&'static [u8]>, part_size: usize) -> Vec<usize> {
        let whole: Vec<u8> = chunks.concat();
        // Tokio, not a bare executor: reads carry the body idle timeout.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut reader = reader_of(chunks);
            let mut sizes = Vec::new();
            loop {
                let part = read_part(&mut reader, part_size).await.unwrap();
                if part.is_empty() {
                    break;
                }
                let short = part.len < part_size;
                sizes.push(part.len);
                if short {
                    break;
                }
            }
            assert_eq!(
                reader.finish(),
                (Sha256::digest(&whole).to_vec(), whole.len() as i64)
            );
            sizes
        })
    }

    #[test]
    fn parts_are_uniform_except_the_last() {
        // Chunk boundaries that do not line up with part boundaries.
        assert_eq!(
            parts_of(vec![b"abcde", b"fghij", b"klm"], 4),
            vec![4, 4, 4, 1]
        );
        // A single chunk larger than several parts.
        assert_eq!(parts_of(vec![b"abcdefghijklm"], 4), vec![4, 4, 4, 1]);
        // Many small chunks.
        assert_eq!(
            parts_of(vec![b"a", b"b", b"c", b"d", b"e"], 2),
            vec![2, 2, 1]
        );
    }

    #[test]
    fn an_exact_multiple_ends_without_a_short_part() {
        // 8 bytes at part size 4: two full parts, then the stream is empty.
        assert_eq!(parts_of(vec![b"abcdefgh"], 4), vec![4, 4]);
    }

    #[test]
    fn an_empty_body_produces_no_parts() {
        assert_eq!(parts_of(vec![], 4), Vec::<usize>::new());
        assert_eq!(parts_of(vec![b""], 4), Vec::<usize>::new());
    }

    #[test]
    fn byte_caps_become_whole_part_slots() {
        let limits = UploadLimits::new(64 * 1024 * 1024, 4, 1024 * 1024 * 1024, IDLE);
        assert_eq!(limits.total_slots(), 16);
        // A cap smaller than one part still leaves a usable slot rather than
        // deadlocking every upload.
        let tiny = UploadLimits::new(64 * 1024 * 1024, 4, 1024, IDLE);
        assert_eq!(tiny.total_slots(), 1);
    }

    /// A sender that goes quiet mid-part must fail the read rather than hold
    /// its slots forever. Paused clock: the idle bound passes at once, and a
    /// read that ignores it trips the outer bound instead of hanging.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_body_fails_the_read() {
        let body = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))])
            .chain(futures::stream::pending());
        let mut reader = BodyReader::new(body, IDLE);
        let read = tokio::time::timeout(IDLE * 2, read_part(&mut reader, 1024)).await;
        assert!(read.expect("a stalled body hung the read").is_err());
    }

    /// The stall error names how much arrived, so an operator can tell a
    /// sender that never started from one that died mid-NAR. The count spans
    /// parts: here 10 bytes filled an earlier part.
    #[tokio::test(start_paused = true)]
    async fn the_stall_error_carries_the_byte_count() {
        let body = futures::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"0123456789ab")),
            Ok(Bytes::from_static(b"c")),
        ])
        .chain(futures::stream::pending());
        let mut reader = BodyReader::new(body, Duration::from_millis(10));
        assert_eq!(read_part(&mut reader, 10).await.unwrap().len, 10);
        let err = read_part(&mut reader, 1024).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("after 13 bytes"), "{msg}");
        assert!(msg.contains("(multipart)"), "{msg}");
    }

    /// The bound is on silence, not on total time: a sender that keeps
    /// trickling bytes is never cut off, however long the part takes.
    #[tokio::test(start_paused = true)]
    async fn a_slow_body_that_keeps_moving_is_not_cut_off() {
        let body = Box::pin(futures::stream::iter(0..4).then(|_| async {
            tokio::time::sleep(IDLE / 2).await;
            Ok::<_, std::io::Error>(Bytes::from_static(b"ab"))
        }));
        let mut reader = BodyReader::new(body, IDLE);
        assert_eq!(read_part(&mut reader, 1024).await.unwrap().len, 8);
    }

    /// Just enough S3 on a loopback port. Every call succeeds, except that
    /// `DeleteObjects` answers inside a 200 the way S3 does: keys containing
    /// `denied` fail, and keys containing `gone` come back as a `NoSuchKey`
    /// error — the shape some implementations use for an already-absent key,
    /// which the caller must tolerate. For tests where the question is our
    /// side, not S3 semantics.
    async fn fake_s3() -> Storage {
        use axum::{
            extract::Request,
            http::{Method, StatusCode},
            response::{IntoResponse, Response},
        };

        async fn handle(req: Request) -> Response {
            let (parts, body) = req.into_parts();
            let body = axum::body::to_bytes(body, usize::MAX)
                .await
                .unwrap_or_default();
            let query = parts.uri.query().unwrap_or_default();
            let has = |name: &str| query.split('&').any(|p| p.split('=').next() == Some(name));
            let xml = |body: String| ([("content-type", "application/xml")], body);
            match parts.method {
                Method::POST if has("uploads") => {
                    xml("<InitiateMultipartUploadResult><UploadId>u</UploadId>\
                     </InitiateMultipartUploadResult>"
                        .into())
                    .into_response()
                }
                Method::POST if has("delete") => {
                    let request = String::from_utf8_lossy(&body);
                    let results: String = request
                        .split("<Key>")
                        .skip(1)
                        .filter_map(|s| s.split("</Key>").next())
                        .map(|key| {
                            let code = match key {
                                k if k.contains("denied") => "AccessDenied",
                                k if k.contains("gone") => "NoSuchKey",
                                _ => return format!("<Deleted><Key>{key}</Key></Deleted>"),
                            };
                            format!("<Error><Key>{key}</Key><Code>{code}</Code></Error>")
                        })
                        .collect();
                    xml(format!("<DeleteResult>{results}</DeleteResult>")).into_response()
                }
                Method::POST => xml("<CompleteMultipartUploadResult/>".into()).into_response(),
                Method::PUT => [("etag", "\"e\"")].into_response(),
                _ => StatusCode::NO_CONTENT.into_response(),
            }
        }

        crate::testing::fake_s3(axum::Router::new().fallback(handle)).await
    }

    /// What the fake S3 saw of one `UploadPart` request.
    #[derive(Clone, Debug)]
    struct SeenPart {
        number: i32,
        content_length: Option<String>,
        crc32: Option<String>,
        /// The request was aws-chunked: a trailer checksum instead of a
        /// header, and a body wrapped in chunk framing.
        chunked: bool,
        body: Vec<u8>,
    }

    /// A fake S3 that records every `UploadPart`, so a test can check the
    /// bytes, the length and the checksum that actually went on the wire.
    async fn recording_s3(seen: Arc<std::sync::Mutex<Vec<SeenPart>>>) -> Storage {
        use axum::{
            extract::Request,
            http::{Method, StatusCode},
            response::IntoResponse,
        };

        let app = axum::Router::new().fallback(move |req: Request| {
            let seen = seen.clone();
            async move {
                let (parts, body) = req.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX)
                    .await
                    .unwrap_or_default();
                let query = parts.uri.query().unwrap_or_default().to_owned();
                let param = |name: &str| {
                    query
                        .split('&')
                        .find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
                        .map(str::to_owned)
                };
                let xml = |body: String| ([("content-type", "application/xml")], body);
                match parts.method {
                    Method::POST
                        if query
                            .split('&')
                            .any(|p| p.split('=').next() == Some("uploads")) =>
                    {
                        xml("<InitiateMultipartUploadResult><UploadId>u</UploadId>\
                             </InitiateMultipartUploadResult>"
                            .into())
                        .into_response()
                    }
                    Method::POST => xml("<CompleteMultipartUploadResult/>".into()).into_response(),
                    Method::PUT => {
                        if let Some(number) = param("partNumber") {
                            let header = |name: &str| {
                                parts
                                    .headers
                                    .get(name)
                                    .and_then(|v| v.to_str().ok())
                                    .map(str::to_owned)
                            };
                            seen.lock().expect("seen lock").push(SeenPart {
                                number: number.parse().expect("part number"),
                                content_length: header("content-length"),
                                crc32: header("x-amz-checksum-crc32"),
                                chunked: header("content-encoding")
                                    .is_some_and(|v| v.contains("aws-chunked"))
                                    || header("x-amz-decoded-content-length").is_some(),
                                body: body.to_vec(),
                            });
                        }
                        [("etag", "\"e\"")].into_response()
                    }
                    _ => StatusCode::NO_CONTENT.into_response(),
                }
            }
        });
        crate::testing::fake_s3(app).await
    }

    /// A part is sent as the bytes that arrived, not a copy of them: chunk
    /// boundaries that straddle part boundaries must still produce the exact
    /// part bytes, the exact `Content-Length`, and a CRC32 header S3 can
    /// verify over those bytes. A streaming body without that header would
    /// make the SDK choose aws-chunked encoding with a trailer instead.
    #[tokio::test]
    async fn parts_are_sent_whole_with_their_crc32() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let storage = recording_s3(seen.clone()).await;
        let part_size = 1024;
        let limits = UploadLimits::new(part_size, 2, 4 * part_size as u64, IDLE);
        let body: Vec<u8> = (0..2 * part_size + 300).map(|i| (i % 251) as u8).collect();
        // Chunk boundaries at 700, 800, 1600, 2100: two fall inside a part.
        let mut at = 0;
        let chunks: Vec<_> = [700, 100, 800, 500, 248]
            .iter()
            .map(|&size| {
                let chunk = Bytes::copy_from_slice(&body[at..at + size]);
                at += size;
                Ok::<_, std::io::Error>(chunk)
            })
            .collect();
        assert_eq!(at, body.len());

        let stored = storage
            .put_streaming(
                &key_for("parts"),
                futures::stream::iter(chunks),
                &limits,
                part_size,
            )
            .await
            .unwrap();
        assert_eq!(stored, (Sha256::digest(&body).to_vec(), body.len() as i64));

        let mut parts = seen.lock().expect("seen lock").clone();
        parts.sort_by_key(|p| p.number);
        assert_eq!(parts.len(), 3, "two whole parts and the tail");
        let mut sent = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            let start = index * part_size;
            let end = (start + part_size).min(body.len());
            assert_eq!(part.body, body[start..end], "part {} bytes", part.number);
            let expected_len = (end - start).to_string();
            assert_eq!(
                part.content_length.as_deref(),
                Some(expected_len.as_str()),
                "part {} length",
                part.number
            );
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&part.body);
            let expected_crc = B64.encode(hasher.finalize().to_be_bytes());
            assert_eq!(
                part.crc32.as_deref(),
                Some(expected_crc.as_str()),
                "part {} checksum",
                part.number
            );
            assert!(!part.chunked, "part {} used aws-chunked", part.number);
            sent.extend_from_slice(&part.body);
        }
        assert_eq!(sent, body, "the parts reassemble the body");
    }

    /// An upload is charged its hint (rounded up to a unit) while it waits on
    /// the body, and gives it back when dropped mid-read.
    #[tokio::test]
    async fn in_flight_bytes_track_a_live_upload() {
        let storage = fake_s3().await;
        let limits = UploadLimits::new(1024, 2, 4096, IDLE);
        assert_eq!(limits.in_flight_bytes(), 0);
        let body = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(vec![1u8; 100]))])
            .chain(futures::stream::pending());
        let key = key_for("slow");
        let mut upload = Box::pin(storage.put_streaming(&key, body, &limits, 200));
        assert!(futures::poll!(&mut upload).is_pending());
        assert_eq!(limits.in_flight_bytes(), 200);
        drop(upload);
        assert_eq!(limits.in_flight_bytes(), 0);
    }

    /// A failed upload aborts its multipart only after the parts already
    /// sent have finished: S3 completes a part request it is processing, and
    /// one that lands after the abort re-creates the upload.
    #[tokio::test]
    async fn a_failed_multipart_aborts_after_its_parts_land() {
        use axum::{
            extract::Request,
            http::{Method, StatusCode},
            response::IntoResponse,
        };
        let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log = events.clone();
        let app = axum::Router::new().fallback(move |req: Request| {
            let log = log.clone();
            async move {
                let query = req.uri().query().unwrap_or_default().to_owned();
                let method = req.method().clone();
                let _ = axum::body::to_bytes(req.into_body(), usize::MAX).await;
                match method {
                    Method::POST if query.contains("uploads") => (
                        [("content-type", "application/xml")],
                        "<InitiateMultipartUploadResult><UploadId>u</UploadId>\
                         </InitiateMultipartUploadResult>",
                    )
                        .into_response(),
                    Method::PUT => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        log.lock().unwrap().push("part".into());
                        [("etag", "\"e\"")].into_response()
                    }
                    Method::DELETE => {
                        log.lock().unwrap().push("abort".into());
                        StatusCode::NO_CONTENT.into_response()
                    }
                    _ => StatusCode::NO_CONTENT.into_response(),
                }
            }
        });
        let storage = crate::testing::fake_s3(app).await;
        let limits = UploadLimits::new(1024, 4, 8192, IDLE);
        // Two full parts, then the client body fails.
        let body = futures::stream::iter([
            Ok(Bytes::from(vec![1u8; 2048])),
            Err(std::io::Error::other("client went away")),
        ]);
        let stored = storage
            .put_streaming(&key_for("torn"), body, &limits, 4096)
            .await;
        assert!(stored.is_err());
        assert_eq!(*events.lock().unwrap(), ["part", "part", "abort"]);
    }

    /// Uploads that together want more part slots than exist must still all
    /// finish; the timeout turns a wedge on the shared budget into a failure.
    #[tokio::test]
    async fn concurrent_multiparts_finish_on_fewer_slots_than_they_want() {
        let storage = fake_s3().await;
        // Two slots, two parts in flight per upload, three 5-part uploads:
        // one upload alone already wants every slot.
        let limits = UploadLimits::new(1024, 2, 2048, IDLE);
        let body: Vec<u8> = (0..4 * 1024 + 100).map(|i| i as u8).collect();
        let keys: Vec<String> = (0..3).map(|i| key_for(&i.to_string())).collect();
        let uploads = keys.iter().map(|key| {
            let chunks: Vec<_> = body
                .chunks(300)
                .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
                .collect();
            storage.put_streaming(key, futures::stream::iter(chunks), &limits, 1024)
        });

        let stored = tokio::time::timeout(
            Duration::from_secs(10),
            futures::future::try_join_all(uploads),
        )
        .await
        .expect("uploads wedged on the shared part budget")
        .unwrap();

        let digest = Sha256::digest(&body).to_vec();
        for (hash, len) in stored {
            assert_eq!((hash, len), (digest.clone(), body.len() as i64));
        }
    }

    /// A small body reserves only its hint: with the budget holding barely
    /// more than one part and most of it already taken, it still uploads at
    /// once. Under whole-part accounting it would wait on the other holder.
    #[tokio::test]
    async fn a_small_upload_does_not_need_a_whole_part_slot() {
        let storage = fake_s3().await;
        let limits = UploadLimits::new(1024 * 1024, 4, 1024 * 1024, IDLE);
        // Another upload holds all but two units of the one-part budget.
        let spare = limits.part_slots.available_permits() - 2;
        let held = limits
            .part_slots
            .clone()
            .acquire_many_owned(spare as u32)
            .await
            .unwrap();
        let body = vec![7u8; 1000];
        let stored = tokio::time::timeout(
            Duration::from_secs(5),
            storage.put_streaming(
                &key_for("small"),
                futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(body.clone()))]),
                &limits,
                2000,
            ),
        )
        .await
        .expect("a small upload waited for a whole part")
        .unwrap();
        assert_eq!(stored, (Sha256::digest(&body).to_vec(), 1000));
        drop(held);
        assert_eq!(limits.in_flight_bytes(), 0);
    }

    /// A body that outgrows its hint fails instead of growing past its
    /// reservation, and returns its permit.
    #[tokio::test]
    async fn a_body_past_its_hint_is_refused() {
        let storage = fake_s3().await;
        let limits = UploadLimits::new(1024, 2, 4096, IDLE);
        let body: Vec<u8> = (0..500).map(|i| i as u8).collect();
        let chunks: Vec<_> = body
            .chunks(300)
            .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
            .collect();
        let result = storage
            .put_streaming(
                &key_for("grown"),
                futures::stream::iter(chunks),
                &limits,
                100,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(limits.in_flight_bytes(), 0);
    }

    /// FileHash and FileSize (spec 03) describe exactly the stored bytes when
    /// chunks straddle the first part, later part boundaries and the hint,
    /// and when one chunk spans several parts.
    #[tokio::test]
    async fn hash_and_length_cover_the_stored_bytes_across_boundaries() {
        let storage = fake_s3().await;
        let limits = UploadLimits::new(1024, 2, 8192, IDLE);
        let body: Vec<u8> = (0..5000u32).map(|i| (i * 7) as u8).collect();
        let chunked = |body: &[u8], sizes: &[usize]| {
            let mut at = 0;
            let chunks: Vec<_> = sizes
                .iter()
                .map(|&s| {
                    let chunk = Bytes::copy_from_slice(&body[at..at + s]);
                    at += s;
                    Ok::<_, std::io::Error>(chunk)
                })
                .collect();
            futures::stream::iter(chunks)
        };
        for sizes in [&[700, 700, 3600][..], &[1000, 2500, 1500]] {
            let stored = storage
                .put_streaming(&key_for("straddle"), chunked(&body, sizes), &limits, 1024)
                .await
                .unwrap();
            assert_eq!(stored, (Sha256::digest(&body).to_vec(), 5000));
        }
        // A body ending exactly at its hint, then an empty chunk: the probe
        // past the hint skips it and the body is accepted whole.
        let small = &body[..500];
        let stored = storage
            .put_streaming(
                &key_for("at-hint"),
                chunked(small, &[300, 200, 0]),
                &limits,
                500,
            )
            .await
            .unwrap();
        assert_eq!(stored, (Sha256::digest(small).to_vec(), 500));
    }

    /// A body that fills its hint and then goes quiet stalls in the probe
    /// for more; the error counts every byte received before it.
    #[tokio::test]
    async fn a_stall_probing_past_the_hint_reports_the_bytes_received() {
        let storage = fake_s3().await;
        let limits = UploadLimits::new(1024, 2, 4096, Duration::from_millis(50));
        let body = futures::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from(vec![1u8; 60])),
            Ok(Bytes::from(vec![2u8; 40])),
        ])
        .chain(futures::stream::pending());
        let err = storage
            .put_streaming(&key_for("probe"), body, &limits, 100)
            .await
            .unwrap_err();
        assert!(err.is::<ClientBodyError>(), "{err:#}");
        let msg = err.to_string();
        assert!(msg.contains("after 100 bytes"), "{msg}");
        assert!(msg.contains("(first_part)"), "{msg}");
        assert_eq!(limits.in_flight_bytes(), 0);
    }

    /// `DeleteObjects` answers 200 even when some keys fail, listing them in
    /// the body. Those blobs are still there, so the call must fail and name
    /// them rather than report the whole batch deleted. A key that was
    /// already absent is not one of them: absent is the goal (a dangling row
    /// evicted by GC, say).
    #[tokio::test]
    async fn per_key_delete_failures_fail_the_call() {
        let storage = fake_s3().await;
        let (ok, gone, denied) = (key_for("ok"), key_for("gone"), key_for("denied"));
        storage
            .delete_objects(&[ok.clone(), gone.clone()])
            .await
            .unwrap();

        let err = storage
            .delete_objects(&[ok.clone(), gone.clone(), denied.clone()])
            .await
            .expect_err("a per-key failure must fail the delete");
        let err = format!("{err:#}");
        assert!(err.contains(&denied), "{err}");
    }

    /// A listing returns at most 1,000 uploads: the abort follows the
    /// markers of a truncated page to the next one.
    #[tokio::test]
    async fn stale_multiparts_on_a_later_page_are_aborted_too() {
        use axum::{extract::Request, http::Method};

        let aborted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let storage = crate::testing::fake_s3(axum::Router::new().fallback({
            let aborted = aborted.clone();
            move |req: Request| {
                let aborted = aborted.clone();
                async move {
                    let query = req.uri().query().unwrap_or_default().to_owned();
                    if req.method() == Method::DELETE {
                        aborted.lock().unwrap().push(req.uri().path().to_owned());
                        return String::new();
                    }
                    let upload = |key: &str| {
                        format!(
                            "<Upload><Key>{key}</Key><UploadId>u-{key}</UploadId>\
                             <Initiated>2000-01-01T00:00:00.000Z</Initiated></Upload>"
                        )
                    };
                    let page = if query.contains("key-marker=") {
                        format!("<IsTruncated>false</IsTruncated>{}", upload("nar/b.nar.zst"))
                    } else {
                        format!(
                            "<IsTruncated>true</IsTruncated><NextKeyMarker>nar/a.nar.zst\
                             </NextKeyMarker><NextUploadIdMarker>u-nar/a.nar.zst\
                             </NextUploadIdMarker>{}",
                            upload("nar/a.nar.zst")
                        )
                    };
                    format!(
                        r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test</Bucket>{page}</ListMultipartUploadsResult>"#
                    )
                }
            }
        }))
        .await;

        let count = storage
            .abort_stale_multiparts(Duration::ZERO, &crate::inflight::InFlight::new())
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            *aborted.lock().unwrap(),
            ["/test/nar/a.nar.zst", "/test/nar/b.nar.zst"]
        );
    }
}

/// Kani proofs (`just kani`) of the upload byte budget (spec 03): for every
/// part size the config accepts and every in-flight cap.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// S3's part-size range, which `PusherConfig::validate` enforces.
    fn any_part_size() -> usize {
        kani::any_where(|p: &usize| {
            (crate::config::MIN_PART_SIZE..=crate::config::MAX_PART_SIZE).contains(p)
        })
    }

    /// A buffer of up to one part is always fully paid for by its permits,
    /// one request never asks for more permits than exist (so it cannot
    /// wait forever), and the count fits the `u32` the semaphore takes.
    #[kani::proof]
    fn every_buffer_is_covered_by_its_permits() {
        let part_size = any_part_size();
        let max_in_flight: u64 = kani::any_where(|m| *m >= part_size as u64);
        let (unit, part_units, total_units) = budget(part_size, max_in_flight);
        let bytes: usize = kani::any_where(|b| *b <= part_size);
        let units = units_for(bytes, unit, part_units);
        assert!(units >= 1 && units * unit >= bytes);
        assert!(units <= part_units && part_units <= total_units);
        assert!(u32::try_from(part_units).is_ok());
        assert!(part_units * unit >= part_size);
        // tokio's Semaphore::new panics above usize::MAX >> 3 permits.
        assert!(total_units <= usize::MAX >> 3);
    }

    /// The whole budget never exceeds the configured cap by more than the
    /// rounding of one part to whole units: what RSS is measured against.
    #[kani::proof]
    fn the_budget_stays_within_the_cap() {
        let part_size = any_part_size();
        let max_in_flight: u64 = kani::any_where(|m| *m >= part_size as u64);
        let (unit, _, total_units) = budget(part_size, max_in_flight);
        let budget_bytes = total_units as u128 * unit as u128;
        assert!(budget_bytes <= max_in_flight as u128 + unit as u128);
        assert!(budget_bytes >= part_size as u128);
    }
}
