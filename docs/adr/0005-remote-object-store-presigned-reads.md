# Remote object store (MEGA S4); the Puller redirects instead of proxying

The deployment target for blob storage is MEGA S4, a hosted S3-compatible
service reached over WAN, not a colocated Garage instance as the spec
originally assumed. That single change invalidates the read path's premise:
proxying was cheap when the bucket was a LAN service, but with a remote
store every served byte crosses the host's uplink twice (down from S4, back
out to the client). So the Puller now answers NAR requests with a `302` to a
presigned S4 URL (default 1 h TTL) and leaves S4 to serve the bytes and any
Range requests; it still serves narinfo, since it owns the signatures. This
is the option [ADR-0002](0002-whole-nar-storage.md) deliberately preserved
by storing one blob per NAR. Consequences: the Puller leaves the byte path
entirely, so its flat memory is structural rather than engineered, and the
256 KiB-buffer/Range-passthrough/connection-reuse machinery is never
written; the S4 endpoint and bucket become publicly visible in redirect
URLs (contents are already public, credentials are not exposed); pulls gain
one round-trip before first byte. S4 verified to support presigned URLs,
multipart with abort/list, `DeleteObjects`, Range GET, and `ListObjectsV2`
continuation — but *not* part re-upload (a failed part aborts the whole
multipart) and parts 1..N-1 must be identical in size. Garage remains the
local dev and benchmark stand-in. Revisit if a CDN fronts the cache, or if
leaking the backing store ever becomes unacceptable — reinstating the proxy
is additive.
