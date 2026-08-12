# 27 — S3 operation timeout on the Pusher's store path

Status: implemented (2026-08; spec 03). Evidence:
[baseline](../research/benchmark-baseline-2026-08.md) — the 1-CPU noise
investigation that observed the hang live.

## Problem

The Pusher's S3 client is built with aws-sdk defaults, which set **no
operation timeout**: a store call that stops making progress waits
forever. Observed live during the 2026-08 benchmark investigation:
Garage stopped reading a part upload mid-body (7.7 MB unread in its
socket receive queue for over an hour, every process idle) and the PUT
above it hung indefinitely — holding its upload-concurrency permit and
its in-flight byte reservation the whole time. Against real S4 over WAN
the same shape is a stalled TCP connection; enough of them exhaust
`max_concurrent_uploads` / `max_in_flight_bytes` and the Pusher stops
accepting work while looking healthy.

## Shape

One knob: `[s3] operation_timeout_secs` (default **60**), applied as the
SDK's **overall operation timeout** — the deadline covers connect,
transfer, and any internal SDK retry attempts for one logical call.

Deliberately *not* the per-attempt timeout: a timed-out attempt would be
retried by the SDK, and for `UploadPart` a retry re-uploads the part,
which S4 forbids (spec 03: no part re-upload; the multipart aborts
instead). The overall deadline fails the call once; the existing error
path aborts the multipart, frees the parts, and the push fails loudly —
the client's contract already covers retrying the whole NAR.

Sizing: parts are 16 MiB (64 MiB spec default); at even 5 Mbps a part
moves in well under 60 s. Deployments on slower uplinks or with larger
parts raise the knob.

## Non-goals

- Per-attempt timeouts / SDK retry tuning — existing retry behavior is
  untouched.
- The Puller: it never talks to S3 on the serve path (presigned
  redirects), and its pull-path budgets are ticket 25.
