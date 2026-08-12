//! S3 blob storage (MEGA S4 in production, Garage locally). Spec 03-storage.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier},
};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::S3Config;

/// An S3 client bound to the cache's bucket. Cheap to clone — the inner
/// client is already shared.
#[derive(Clone)]
pub struct Storage {
    client: Client,
    bucket: String,
}

/// Bounds every upload's memory: `part_size` bytes per in-flight part, with
/// the total across all concurrent uploads capped by a shared semaphore.
pub struct UploadLimits {
    /// Bytes per part; also the single-`PutObject` threshold (spec 03).
    pub part_size: usize,
    /// Concurrent part uploads per NAR.
    pub max_parts_in_flight: usize,
    /// One permit per part-sized buffer, shared process-wide.
    pub part_slots: Arc<Semaphore>,
}

impl UploadLimits {
    /// `max_in_flight_bytes` becomes a whole number of part-sized slots, so
    /// the byte cap is enforced by counting buffers instead of bytes.
    pub fn new(part_size: usize, max_parts_in_flight: usize, max_in_flight_bytes: u64) -> Self {
        let slots = (max_in_flight_bytes / part_size as u64).max(1) as usize;
        Self {
            part_size,
            max_parts_in_flight,
            part_slots: Arc::new(Semaphore::new(slots)),
        }
    }

    /// How many part-sized buffers may exist at once, process-wide.
    pub fn total_slots(&self) -> usize {
        self.part_slots.available_permits()
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        self.part_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("upload capacity semaphore closed"))
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

/// Pulls exactly `part_size` bytes (fewer only at end of stream), keeping any
/// overshoot in `carry` for the next part.
async fn read_part<S, E>(
    body: &mut S,
    carry: &mut Option<Bytes>,
    part_size: usize,
) -> Result<Vec<u8>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut part = BytesMut::with_capacity(part_size);
    if let Some(left) = carry.take() {
        // The carry can exceed a whole part when one chunk spans several, so
        // it is split too — a part over part_size would abort the upload.
        if left.len() >= part_size {
            part.extend_from_slice(&left[..part_size]);
            if left.len() > part_size {
                *carry = Some(left.slice(part_size..));
            }
            return Ok(part.to_vec());
        }
        part.extend_from_slice(&left);
    }
    while part.len() < part_size {
        let Some(chunk) = body.next().await else {
            break;
        };
        let chunk = chunk.map_err(|e| anyhow!("reading request body: {e}"))?;
        let room = part_size - part.len();
        if chunk.len() > room {
            part.extend_from_slice(&chunk[..room]);
            *carry = Some(chunk.slice(room..));
            break;
        }
        part.extend_from_slice(&chunk);
    }
    Ok(part.to_vec())
}

async fn collect(
    in_flight: &mut FuturesUnordered<impl Future<Output = Result<CompletedPart>>>,
) -> Result<CompletedPart> {
    in_flight
        .next()
        .await
        .context("no part upload was in flight")?
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
        // S4 and Garage both accept path-style; it avoids DNS games for dotted buckets.
        builder = builder.force_path_style(cfg.path_style);
        // Overall deadline per call, not per attempt: a timed-out attempt
        // would be retried, and re-uploading a part is forbidden on S4
        // (spec 03) — fail once and let the caller abort the multipart.
        builder = builder.timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .operation_timeout(std::time::Duration::from_secs(cfg.operation_timeout_secs))
                .build(),
        );
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
        })
    }

    /// Single-request `PutObject` for bodies already in memory (at most one
    /// part; larger uploads go through [`Storage::put_streaming`]).
    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<()> {
        let bytes = body.len() as u64;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .inspect_err(|_| metrics::counter!("garret_s3_put_failures_total").increment(1))
            .with_context(|| format!("uploading {key}"))?;
        metrics::counter!("garret_s3_puts_total").increment(1);
        metrics::counter!("garret_s3_bytes_uploaded_total").increment(bytes);
        Ok(())
    }

    /// Streams a body to S3, hashing the bytes as they pass (spec 03-storage).
    /// Returns the sha256 of exactly what was stored, and its length.
    ///
    /// One part is buffered at a time and a global permit is taken **before**
    /// each read, so the reader can never race ahead of S3 and server memory
    /// stays bounded by configuration rather than by client behaviour.
    pub async fn put_streaming<S, E>(
        &self,
        key: &str,
        mut body: S,
        limits: &UploadLimits,
    ) -> Result<(Vec<u8>, i64)>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut hasher = Sha256::new();
        let mut total: i64 = 0;
        let mut carry: Option<Bytes> = None;

        // First part decides the shape: if the whole body fits, one PutObject.
        let permit = limits.acquire().await?;
        let first = read_part(&mut body, &mut carry, limits.part_size).await?;
        hasher.update(&first);
        total += first.len() as i64;
        if first.len() < limits.part_size {
            self.put(key, first).await?;
            drop(permit);
            return Ok((hasher.finalize().to_vec(), total));
        }

        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("starting multipart upload for {key}"))?
            .upload_id
            .context("S3 returned no upload id")?;
        metrics::counter!("garret_s3_multipart_started_total").increment(1);

        // Any failure past this point must abort, or the parts linger and bill.
        match self
            .multipart_body(
                key,
                &upload_id,
                first,
                permit,
                &mut body,
                &mut carry,
                limits,
                &mut hasher,
                &mut total,
            )
            .await
        {
            Ok(()) => Ok((hasher.finalize().to_vec(), total)),
            Err(e) => {
                metrics::counter!("garret_s3_multipart_aborted_total").increment(1);
                if let Err(abort) = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await
                {
                    tracing::error!("aborting multipart for {key} also failed: {abort}");
                }
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn multipart_body<S, E>(
        &self,
        key: &str,
        upload_id: &str,
        first: Vec<u8>,
        first_permit: OwnedSemaphorePermit,
        body: &mut S,
        carry: &mut Option<Bytes>,
        limits: &UploadLimits,
        hasher: &mut Sha256,
        total: &mut i64,
    ) -> Result<()>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut in_flight = FuturesUnordered::new();
        let mut completed: Vec<CompletedPart> = Vec::new();
        let mut part_number = 1;

        in_flight.push(self.upload_part(key, upload_id, part_number, first, first_permit));

        loop {
            // Permit first, then read: never buffer a part we have no room for.
            let permit = limits.acquire().await?;
            let part = read_part(body, carry, limits.part_size).await?;
            if part.is_empty() {
                drop(permit);
                break;
            }
            hasher.update(&part);
            *total += part.len() as i64;
            let is_final = part.len() < limits.part_size;

            while in_flight.len() >= limits.max_parts_in_flight {
                completed.push(collect(&mut in_flight).await?);
            }
            part_number += 1;
            in_flight.push(self.upload_part(key, upload_id, part_number, part, permit));
            if is_final {
                break;
            }
        }

        while !in_flight.is_empty() {
            completed.push(collect(&mut in_flight).await?);
        }
        completed.sort_by_key(|p| p.part_number());

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
            .send()
            .await
            .with_context(|| format!("completing multipart upload for {key}"))?;
        metrics::counter!("garret_s3_multipart_completed_total").increment(1);
        Ok(())
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Vec<u8>,
        permit: OwnedSemaphorePermit,
    ) -> Result<CompletedPart> {
        let (client, bucket, key, upload_id) = (
            self.client.clone(),
            self.bucket.clone(),
            key.to_owned(),
            upload_id.to_owned(),
        );
        let bytes = body.len() as u64;
        let started = Instant::now();
        let output = client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            // S4 forbids re-uploading a part, so there is no retry here: the
            // caller aborts the whole multipart (spec 03-storage).
            .with_context(|| format!("uploading part {part_number} of {key}"))?;
        drop(permit);

        metrics::counter!("garret_s3_parts_total").increment(1);
        metrics::counter!("garret_s3_bytes_uploaded_total").increment(bytes);
        metrics::histogram!("garret_s3_part_duration_seconds").record(started.elapsed());
        Ok(CompletedPart::builder()
            .part_number(part_number)
            .set_e_tag(output.e_tag)
            .build())
    }

    /// Every blob under `nar/`, with its age and size. Paginated — the
    /// bucket outgrows one response long before the cache is interesting.
    pub async fn list_blobs(&self) -> Result<Vec<(String, Duration, i64)>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("nar/")
                .set_continuation_token(token.take())
                .send()
                .await
                .context("listing blobs")?;

            for object in page.contents() {
                let Some(key) = object.key() else { continue };
                let age = object
                    .last_modified()
                    .and_then(|t| SystemTime::try_from(*t).ok())
                    .and_then(|t| SystemTime::now().duration_since(t).ok())
                    .unwrap_or(Duration::ZERO);
                out.push((key.to_owned(), age, object.size().unwrap_or(0)));
            }

            token = page.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// Batched at 1,000 keys — the S3 limit for a single `DeleteObjects`.
    pub async fn delete_objects(&self, keys: &[String]) -> Result<()> {
        for batch in keys.chunks(1000) {
            let objects: Vec<ObjectIdentifier> = batch
                .iter()
                .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
                .collect();
            if objects.is_empty() {
                continue;
            }
            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(Delete::builder().set_objects(Some(objects)).build()?)
                .send()
                .await
                .context("deleting blobs")?;
            metrics::counter!("garret_s3_deletes_total").increment(batch.len() as u64);
        }
        Ok(())
    }

    /// Aborts multipart uploads older than `grace` that no live upload owns.
    /// Consulting the in-flight set is why GC lives inside the Pusher (spec 05).
    pub async fn abort_stale_multiparts(
        &self,
        grace: Duration,
        in_flight: &crate::inflight::InFlight,
    ) -> Result<usize> {
        let uploads = self
            .client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .send()
            .await
            .context("listing multipart uploads")?;

        let mut aborted = 0;
        for upload in uploads.uploads() {
            let (Some(key), Some(upload_id)) = (upload.key(), upload.upload_id()) else {
                continue;
            };
            let hash = hash_for(key).unwrap_or_default();
            if in_flight.contains(hash) {
                continue;
            }
            let age = upload
                .initiated()
                .and_then(|t| SystemTime::try_from(*t).ok())
                .and_then(|t| SystemTime::now().duration_since(t).ok())
                .unwrap_or(Duration::ZERO);
            if age < grace {
                continue;
            }
            self.client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .with_context(|| format!("aborting stale multipart for {key}"))?;
            aborted += 1;
        }
        Ok(aborted)
    }

    /// The Puller redirects here instead of proxying bytes (ADR-0005).
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

    #[test]
    fn key_layout_is_flat_and_derivable() {
        assert_eq!(key_for("abc"), "nar/abc.nar.zst");
        assert_eq!(hash_for("nar/abc.nar.zst"), Some("abc"));
        assert_eq!(hash_for("nar/.nar.zst"), None, "empty hash is not a match");
        assert_eq!(hash_for("other/abc.nar.zst"), None);
        assert_eq!(hash_for("nar/abc.zst"), None);
    }

    /// Feeds `chunks` through `read_part` and returns the part sizes produced.
    /// S4 requires parts 1..N-1 to be identical in size, so this is the check
    /// that matters most: only the final part may be short.
    fn parts_of(chunks: Vec<&'static [u8]>, part_size: usize) -> Vec<usize> {
        futures::executor::block_on(async {
            let mut body = futures::stream::iter(
                chunks
                    .into_iter()
                    .map(|c| Ok::<_, std::io::Error>(Bytes::from_static(c))),
            );
            let mut carry = None;
            let mut sizes = Vec::new();
            loop {
                let part = read_part(&mut body, &mut carry, part_size).await.unwrap();
                if part.is_empty() {
                    break;
                }
                let short = part.len() < part_size;
                sizes.push(part.len());
                if short {
                    break;
                }
            }
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
        let limits = UploadLimits::new(64 * 1024 * 1024, 4, 1024 * 1024 * 1024);
        assert_eq!(limits.total_slots(), 16);
        // A cap smaller than one part still leaves a usable slot rather than
        // deadlocking every upload.
        let tiny = UploadLimits::new(64 * 1024 * 1024, 4, 1024);
        assert_eq!(tiny.total_slots(), 1);
    }
}
