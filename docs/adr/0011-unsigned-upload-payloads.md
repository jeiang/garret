# Uploads to S3 send UNSIGNED-PAYLOAD

The AWS SDK signs an in-memory request body with SigV4 by computing the
SHA-256 of every byte (`x-amz-content-sha256`). For `PutObject` and
`UploadPart` that is a second SHA-256 pass over every stored byte: the
Pusher already hashes the same bytes for the FileHash (spec 03). SHA-256
is the dominant per-byte cost after the NarHash check (ADR-0010), and the
production host has 2 vCPUs, so the Pusher now sends `UNSIGNED-PAYLOAD`
for both operations (`disable_payload_signing`). The request headers,
including the SDK's CRC32 checksum header, stay signed.

Measured with `just bench-local` on a 4-vCPU Xeon (SHA-NI), 3 interleaved
runs per side against the same binaries otherwise:
- Pusher CPU for the full scenario set: 11.7 s before, 9.5 s after (-19%).
- 20-way push of the corpus: 271 to 286 MiB/s of NAR (+5%).
- Single-stream 100 MiB: 177 to 237 MiB/s (+34%); 2 GiB: 149 to 172 MiB/s
  (+15%). Garage shares the 4 vCPUs, so these understate the gain on a
  host where S3 is remote.

What integrity remains:
- In transit to S3: TLS on every production endpoint. The SDK still sends
  a CRC32 of the body (`request_checksum_calculation` stays at its
  default), which S3 checks, so a corrupted body is refused.
- At rest and on download: the FileHash the Pusher computes over exactly
  the bytes it sent is signed into the narinfo, and nix checks it, then
  the NarHash, on every substitution. `garret-admin fsck --verify-sizes`
  compares sizes.
- Lost: on a plain-HTTP endpoint (local Garage), an on-path attacker can
  now change a body and fix up its CRC32 without breaking the signature.
  The result is a blob nix refuses on download, not a poisoned path.

Rejected alternatives:
- Pass the FileHash as `x-amz-checksum-sha256` so S3 verifies it: one
  hash would serve both, but only for single-part uploads (multipart
  checksums are per part), and S4 support for SHA-256 checksums is not
  verified.
- Sign with a per-part SHA-256 computed alongside the FileHash: still two
  SHA-256 passes per byte, since SHA-256 does not compose.

`[s3] sign_payload = true` restores signed payloads for a store that
refuses `UNSIGNED-PAYLOAD`. MEGA S4's acceptance of `UNSIGNED-PAYLOAD` on
uploads is not verified here [UNVERIFIED]: check the first push after
deploying, and set the switch if it fails with a signature error.
