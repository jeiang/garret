# Observability

Source: [ticket 15](../../.scratch/spec/issues/15-metrics-observability.md).
Consumer: VictoriaMetrics.

## Exposure

Each service binds a dedicated internal metrics listener — defaults
**9091 (Pusher)** and **9092 (Puller)**, interface configurable — serving
`/metrics` and `/healthz`. The public listener never exposes metrics.

## Readiness

The Puller's public listener answers two readiness probes:

- `/ready`: 200 once the database is open, 503 before. No I/O.
- `/ready/deep`: `/ready`, plus one real read through the path a
  substituter follows. It presigns the blob of the most recently
  accessed object exactly as the NAR route does and fetches its first
  byte (`Range: bytes=0-0`). On failure it answers 503 with the fixed
  body `S3 read path failing`: the public route never echoes upstream
  detail. The WARN log keeps it (the failing step, and the S3 error code
  and `x-amz-request-id` when S4 sent them; never the presigned URL).
  - Presigning is signature-only (spec
    [03](03-storage.md)), so revoked or rotated S3 credentials or an S4
    outage fail neither `/ready` nor the NAR redirect. Only a fetch
    shows them.
  - An empty cache has nothing to read and passes.
  - The fetch trusts the bundled webpki roots, like the JWKS client, not
    the system store, so an S4 endpoint with a private CA fails it.
  - The answer is cached for 30 s, and concurrent callers share one
    probe that finishes even if its caller disconnects. However often
    the public route is hit, it costs at most one one-byte S3 read per
    30 s. Each probe counts in `garret_s3_read_probes_total` by
    `outcome` (`ok`, `failed`) and is timed in the
    `garret_s3_read_probe_duration_seconds` histogram; cached answers
    are neither counted nor timed.
  - Monitoring should probe `/ready/deep` rather than `/ready`, at an
    interval of 30 s or more, and alert on a few consecutive failures.

## Cardinality rules

Bounded labels only: route, method, status class, issuer, phase, outcome.
The `method` label is one of `GET`, `HEAD`, `POST`, `PUT`, `DELETE` or
`OTHER`: any other method a client sends collapses to `OTHER`, so request
headers cannot mint series.
**Never per-object labels** (store path hashes as label values would
explode series); per-object insight is the logs' job. Histogram buckets:
bytes 64 KiB…8 GiB log-scale; latency 100 us…15 min.

## Catalog

Prefix `garret_`; service distinguished by scrape job.

- **Common**: HTTP requests/duration by route + method + status class;
  in-flight gauge; `garret_db_lock_wait_seconds` histogram by `conn`
  (`pusher`, `pull`, `browse`, `bump`): time spent waiting for a
  connection's lock, which is where SQLite contention shows up;
  `garret_db_query_duration_seconds` by `query` (statement family:
  `missing`, `insert_object`, `get_object`, `exists`, `bump_flush`,
  `evictable`, `evict_object`); standard `process_*` metrics
  (`process_cpu_seconds_total`, `process_resident_memory_bytes`,
  `process_open_fds`, `process_max_fds`, `process_start_time_seconds`),
  read from `/proc` on each scrape (Linux only, no background task; the
  CPU series is exposed as a gauge of cumulative seconds), except
  `process_start_time_seconds`, which is the wall-clock time recorded when
  the metrics recorder is installed at startup (so it needs no `/proc/stat`
  and works under `ProcSubset=pid`, on every platform);
  `garret_build_info` with `service`, `version` and `revision` (the
  commit the binary was built from, `unknown` outside the Nix build).
- **Pusher — uploads**: in-flight uploads and `garret_in_flight_bytes` gauges
  *versus their configured caps* (saturation visible before it hurts;
  `garret_in_flight_bytes` is read from the upload budget on each scrape);
  `garret_upload_buffer_wait_seconds`, the time an upload waited for buffer
  room in that budget;
  upload size histogram; `garret_upload_duration_seconds` by `outcome`
  (`accepted`, `failed`), so slow failures stay visible;
  accepted/shed(429) counters; `garret_uploads_failed_total` by `reason`
  (`client_body`: the sender stalled or dropped; `refused`: the body or
  preamble was rejected with `400`; `storage`: an S3 failure; `internal`:
  anything else);
  `garret_upload_slot_milliseconds_total`, the time upload slots were held
  (from the slot acquire to the end of the upload, dropped handlers
  included), so `rate(garret_upload_slot_milliseconds_total[5m]) / 1000 /
  uploads_limit` is slot utilization: alert on it near 1, since the
  in-flight gauge alone undersamples short bursts;
  negotiation batch-size (1…10000) and missing-ratio (0…1) histograms with
  their own buckets, plus `garret_negotiation_missing_paths_total` (a
  weighted ratio is its rate over the batch-size `_sum` rate);
  `garret_upload_skipped_total` by `reason` (`exists`, `in-progress`,
  `quiescing`, and `deleting` for an upload turned away with `503`
  because GC, `delete` or prune is removing the path, spec 05).
- **Pusher — S3**: put/multipart-part counters, part duration,
  aborted multiparts; every S3 request's latency
  (`garret_s3_request_duration_seconds` by `operation`: `put`,
  `upload_part`, `create_mpu`, `complete_mpu`, `abort_mpu`, `delete`,
  `list`) and failures (`garret_s3_errors_total` by `operation` and
  `kind`: `timeout`, `dispatch`, `response`, `service`, `other`), kept
  alongside `garret_s3_put_failures_total` and
  `garret_s3_part_duration_seconds`; blob deletes (`garret_s3_deletes_total`, only keys
  the response did not report as failed) and per-key delete failures
  (`garret_s3_delete_failures_total`: keys a `DeleteObjects` 200 reported
  as not deleted — each an orphan until a later sweep succeeds).
- **Pusher — auth** (also on the Puller, for browse tokens):
  `garret_auth_validations_total` by `issuer` (a configured issuer URL,
  or `unknown` when the token names none: never the token's own `iss`)
  and `outcome` (`accepted`, `malformed`, `untrusted_issuer`,
  `unknown_key`, `jwks_unavailable`, `invalid`, `unauthorized`);
  `garret_auth_unauthorized_total` by `issuer` and `claim` (the failed
  gate: `owner`, `ref`, `ref_protected`, `repository_id`, `event_name`,
  `job_workflow_ref`, `group`; claim values never become labels);
  `garret_jwks_refreshes_total` (fetch attempts) and
  `garret_jwks_refresh_failures_total` by `issuer`. A rise in
  `unknown_key` without matching refreshes is either an unknown-kid flood
  the JWKS floor is absorbing (spec 04) or, if refresh failures rise too,
  a down issuer: during the floor after a failed fetch, unknown kids count
  as `unknown_key`, not `jwks_unavailable`.
- **Pusher — GC**: usage and quota gauges; `garret_gc_high_watermark_bytes`
  and `garret_gc_low_watermark_bytes` gauges (the eviction trigger and
  target); evicted objects/bytes per
  pass; pass duration; `garret_gc_orphans_deleted_total`; `garret_gc_last_sweep_timestamp_seconds`,
  set by every successful orphan sweep, even one that deletes nothing;
  candidates-exhausted alarm
  counter; `garret_gc_failures_total` by `phase` (`pass`, `sweep`), for
  passes and orphan sweeps that errored, whoever triggered them;
  `garret_gc_last_success_timestamp`, set by every successful pass and
  every successful tick — including a quota check that finds nothing to
  evict — so it goes stale only when GC stops running, never merely
  because usage is low.
- **Pusher — fsck**: `garret_fsck_runs_total` counter; `garret_fsck_findings`
  gauge by `kind` (`dangling`, `orphan`, `size_mismatch`), set to the
  latest run's count per kind; `garret_fsck_rows_repaired_total` counter
  by `reason` (`dangling`, `size_mismatch`).
- **Puller**: `garret_narinfo_requests_total` and
  `garret_nar_requests_total` by `outcome`, counting every request to
  their route whose file name is a store hash's (`<hash>.narinfo`,
  `<hash>.nar.zst`): `hit` is a narinfo served (200) or a NAR redirect
  issued; `miss` is an unknown hash, a 503 before the database exists, or
  a degraded request. Any other name is a plain 404 before any database
  read and is not counted, so junk requests cannot inflate the miss rate
  (`garret_nar_requests_total` replaces the former
  `garret_nar_redirects_total`); presign duration histogram;
  `garret_degraded_total` by
  `reason` (`db_timeout`, `db_error`, `presign_timeout`,
  `presign_error`) — pull-path requests degraded to a 404 miss when a
  budget tripped or a read failed (spec
  [03-storage](03-storage.md#bounded-budgets-degrade-to-a-miss); each is
  also a miss on its route's counter); last-accessed bumps
  (spec [02](02-database.md#concurrency-discipline)):
  `garret_bump_queue_depth` gauge, `garret_bump_debounced_total`
  (hits on a fresh row, no write) and `garret_bump_failures_total`
  (failed flushes, batch dropped); browse requests by endpoint;
  browse auth failures (`garret_browse_auth_failures_total` by `reason`:
  `missing_bearer`, `token_rejected`; unmatched paths are 404 and not counted); `garret_s3_read_probes_total` by `outcome`
  (deep readiness, [above](#readiness)). The Puller no longer sees NAR bytes
  ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)), so
  bytes-served, serve-duration, first-byte and Range counters are gone —
  served-byte volume is now S4's to report, not ours.

## Shipped rules and dashboard

`contrib/monitoring/` holds Prometheus-format recording and alert rules
(`garret.rules.yml`, for Prometheus or vmalert) and a Grafana dashboard. The
alerts cover scrape loss, failing deep readiness, refused tokens, upload
failures and shedding, S3 errors, degraded pulls, stale or failing GC, quota
near the high watermark and fsck findings. `nix flake check` runs `promtool
check rules` and `jq empty` on them. The deep-readiness alert only fires when
something probes `/ready/deep`.

## Logs

`tracing` crate, human-readable, on stdout. Verbosity follows `RUST_LOG`
(default `info`). Output is plain, with no ANSI colour codes, whenever stdout is
not a terminal, so journald and files stay clean; there is no JSON
logging. No OTLP/distributed tracing
in v1. The client is metrics-free: progress output, logs, and the
watcher's failed list.

A rejected token's reason is logged escaped and cut at 256 characters: it
can carry attacker-controlled token text (the `kid`, header fields echoed
by parse errors), which must not forge log lines or flood the journal.
An `unauthorized` reason names the refused claim value (`ref "refs/heads/wip"
is not authorized`) near the front (after a `refused by policy` prefix),
so the 256-character cut keeps it. An `unauthorized` outcome is answered
`403`; every other outcome is a `401` (spec 04). The Pusher logs the refusal at warn. The Puller logs a rejected browse token at
debug only, since that surface is public. The Pusher logs each Negotiation
at debug with the subject, batch size and missing count, to attribute
wasted negotiation to a token.
