# Operations

Day-to-day work on a running deployment: keeping the cache inside its budget,
removing what should not be there, backing it up, and watching it.

## Quota and garbage collection

Garbage collection is optional. The `[gc]` section is what turns it on: without
it there is no quota, no eviction and no orphan sweep, the Pusher logs that the
cache is unbounded and will never evict, and both `garret-admin gc run` and
`garret-admin fsck` fail with `no [gc] section is configured`.

| Key | Default |
|---|---|
| `gc.quota_bytes` | required |
| `gc.high_watermark` | 0.95 |
| `gc.low_watermark` | 0.85 |
| `gc.interval_secs` | 300 |
| `gc.orphan_grace_secs` | 86400 |

Eviction starts when usage reaches the high watermark — 95% of the quota by
default — and stops at the low watermark. Below the high watermark nothing is
deleted.

An object is removed only when no other object in the cache references it, no
live pin holds it, and no push used it within the 24-hour push grace. Candidates
go in last-accessed order, so old roots go first and deleting them frees their
dependencies for the same pass. If everything left is referenced, pinned or
inside the grace, the pass stops and raises an alarm metric rather than breaking
a closure.

The push grace exists because a client that was told a path is present will not
upload it, and will upload its referrers afterwards. Until those land, the path
looks like an unreferenced root.

## Pins

A pin is a GC-exempt root that protects its whole closure.

```
garret-admin pin <name> <hash> [--expires <duration>]
garret-admin unpin <name>
```

Pinning a hash the cache does not hold is a hard error. An expired pin simply
stops matching; nothing sweeps it. Pinned bytes still count against the quota,
so a quota made entirely of live pins shows up as the alarm above rather than
silent thrashing.

## Prune

```
garret-admin prune --before <YYYY-MM-DD|age> [--apply]
```

Deletes every closure last pushed before the cutoff, regardless of quota. It
keeps everything pushed at or after the cutoff, every live pin, and each one's
whole reference closure. The cutoff must be at least 24 hours in the past.

Age is the time of the last push, not the time the object was first stored: CI
re-pushing an unchanged closure uploads nothing, but its negotiation marks every
path it names as pushed now. Without that, a system closure unchanged for months
would look old while still in daily use.

Without `--apply` it lists what it would delete and takes no write lock.

## Delete

```
garret-admin delete <hash>... [--apply]
garret-admin delete --pushed-by <subject> [--since <YYYY-MM-DD|age>] [--apply]
```

Removes objects unconditionally, row then blob, whatever still references them.
Without `--apply` it lists what would go.

`--pushed-by` is the incident-response form: it takes a subject as the browse
API shows it (`<issuer>#<sub>`) and selects every object that subject first
pushed at or after the cutoff.

Wherever a command takes a `<hash>`, it also accepts the store path
(`/nix/store/<hash>-<name>` or `<hash>-<name>`). Any other argument is rejected
before the request is sent, so a typo fails loudly instead of coming back as
"not in the cache".

## fsck

```
garret-admin fsck [--repair] [--verify-sizes] [--quiesce] [--json]
```

Audits the database against the bucket. It reports dangling rows (a row with no
blob) and orphan blobs (a blob with no row); `--verify-sizes` adds a third
category, rows whose recorded size disagrees with the stored object.

The default is a dry run. `--repair` deletes dangling and size-mismatched rows;
it never deletes an S3 object, so a size-mismatched row's blob is left for the
orphan sweep. Only the row the listing saw is deleted: a row that changed since
is skipped and not counted as repaired.

`--quiesce` shuts the upload gate, answers new uploads with 503 and
`Retry-After: 1`, and waits for in-flight uploads to drain before repairing. The
drain is bounded at a fixed 300 seconds — there is no flag or config key for it
— and a timed-out drain repairs nothing and reports `quiesce_drained: false`.

`--json` emits the report plus `ok`, and exits 1 when the run is not ok.

## Status

```
garret-admin status
```

Prints the object count, usage against the quota, and the number of in-flight
uploads.

## Backup and restore

```
garret-admin backup <path>
```

Takes an online copy of the database while both services keep serving. The copy
is a consistent snapshot including the uncheckpointed write-ahead log, is
created mode 0600, and never overwrites an existing path. Rotation is your job:
remove or rename the previous copy first.

The path is resolved by `garret-admin` and written by the Pusher, so it must be
writable by the Pusher and visible outside it. Under the NixOS module, put it in
the directory holding `dbPath`.

To restore a copy that is older than the bucket:

1. Stop both services. Put the copy in place of `dbPath`, owned by `garret`, and
   delete any `-wal` and `-shm` files beside it.
2. Start **only the Pusher**, with its push endpoint unreachable to clients.
3. `garret-admin fsck --repair --verify-sizes --quiesce` deletes the rows whose
   blobs were evicted after the backup, and rows whose blob a later delete and
   re-push replaced with one of a different size.
4. Start the Puller.
5. Once every restored row is older than `orphan_grace_secs`, run step 3 again,
   then reopen the push endpoint.
6. Re-apply any pins set or removed since the backup.

Closures stay complete because eviction and prune delete an object only once
nothing surviving references it, roots first. Anything deleted after the backup
was therefore unreferenced by every object still present. Blobs uploaded after
the backup have no row; clients re-push them through normal negotiation, and the
orphan sweep removes the rest. That costs cache misses, never correctness.

The ordering matters: negotiation answers from rows alone, so while a restored
row is unchecked it can tell a client that a path whose blob is gone is present.
Keeping pushes closed until fsck has covered every row is what prevents that.

## Last-accessed bumps

The Puller records last-accessed times for eviction ordering, but only when the
stored value is stale. The threshold is `bump_debounce_secs`, default 86400. A
burst of hits on one object is one write, flushed in a batch every few seconds.

## Monitoring

The Pusher exposes metrics on port 9091 and the Puller on 9092, each with
`/metrics`, `/healthz`, `/ready` and `/ready/deep`. The public listener never
exposes metrics.

- `/ready` answers 200 once the database is open and 503 before.
- `/ready/deep` also performs one real read through the path a substituter
  follows: it presigns the most recently accessed object's blob and fetches its
  first byte. Probe this one rather than `/ready`, at 30-second intervals or
  slower, and alert on a few consecutive failures. The answer is cached for 30
  seconds, so it costs at most one small read per half minute.

Alert rules and a Grafana dashboard ship in `contrib/monitoring/`. The rules
assume each service is scraped under a job name starting with `garret`; if yours
differ, edit the `GarretDown` selector.

Uploads also report where their time went, as three unlabelled counters in
milliseconds, each recorded once per upload (failed ones included):

- `garret_upload_client_wait_milliseconds_total` — waiting on the client's
  body.
- `garret_upload_verify_wait_milliseconds_total` — waiting for the NAR check
  to keep up, including the wait for its verdict.
- `garret_upload_s3_part_wait_milliseconds_total` — waiting for S3 part
  uploads to drain.

Divide each by the number of uploads over the same window for a per-upload
average. A client wait that dominates points at the network, a verify wait at
CPU, and a part wait at S3.

The Puller makes no S3 API calls. Its only S3 traffic is the `/ready/deep` read
of a presigned URL, which is why revoked or rotated credentials fail that probe
while narinfo and NAR redirects keep working.

## Tracing

Distributed tracing is optional and off by default. The `[telemetry]` section
turns it on:

```toml
[telemetry]
enabled = true
endpoint = "http://127.0.0.1:4318"   # OTLP/HTTP base; spans go to /v1/traces
service_name = "garret-pusher"        # default: the service's own name
sample_ratio = 1.0                    # 0.0..=1.0, default 1.0
```

| Key | Default |
|---|---|
| `telemetry.enabled` | `false` |
| `telemetry.endpoint` | `http://127.0.0.1:4318` |
| `telemetry.service_name` | `garret-pusher` / `garret-puller` |
| `telemetry.sample_ratio` | `1.0` |

Spans are exported over OTLP/HTTP (protobuf) to any collector. With `enabled`
unset or `false` there is no exporter, no background task and no propagator:
the process behaves exactly as it does without the section. Sampling is
`ParentBased(TraceIdRatioBased)`, so a trace is sampled or not as a whole and a
caller's decision is never overridden halfway down.

A request carrying W3C `traceparent`/`tracestate` headers joins the caller's
trace, so a push or a pull can be followed from the client into the cache.

What a trace contains, one span per request or operation — never per chunk or
byte:

- `{method} {route}` — one server span per HTTP request in both services, with
  `http.request.method`, `http.route`, `url.path` and
  `http.response.status_code`.
- `negotiation` — the Pusher's missing-paths call, with the batch size and how
  many paths were missing.
- `upload` and `blob.store` — the Pusher's push phases; `blob.bytes` is
  recorded once the Blob is stored.
- `narinfo.serve` and `nar.redirect` — the Puller's two public paths.
- `browse.query` — the Puller's authenticated browse queries, with the
  endpoint they hit.
- `db.call` and the `db.*` query spans — SQLite, with `db.lock_wait_ms`
  separating the wait for the connection from the query itself.
- `s3.request` (per S3 call, `s3.operation` names it) under `s3.put`,
  `s3.put_streaming`, `s3.delete` and the multipart calls
  (`create_mpu`, `upload_part`, `complete_mpu`, `abort_mpu`) and
  `s3.abort_stale_multiparts`. `s3.presign` is a local signature, not an S3
  request, so it has no `s3.request` under it.
- `auth.verify` — token verification, with the issuer; the token itself is
  never recorded.
- `gc.evict` and `gc.sweep` — eviction and orphan-sweep cycles, the eviction
  candidate loop under one `gc.evict.candidates` span.

The Pusher's `negotiation` and `upload` spans carry the caller's OIDC subject
as `issuer#sub` — the same identity the browse API and `garret-admin
--pushed-by` show. It is exported to the collector, so treat the collector as
holding deployment identities.

Export failures — a collector that is down or unreachable — are logged and the
spans are dropped. They never block a request: the batch processor exports on
its own thread, and shutdown flushes what it holds for at most five seconds.
