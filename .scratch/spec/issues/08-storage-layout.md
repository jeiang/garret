# S3 storage layout and interaction strategy

Type: grilling
Status: open
Blocked by: 05

## Question

Design the S3 side: object key naming scheme (ticket 05: one zstd-NAR blob
per object, keyed by store path hash), when to use multipart vs single PUT
(multi-GB model blobs are routine here), bounded part concurrency (attic's
OOM vector — see `OPTIMIZATION_PLAN.md` step 2), read-path streaming,
Garage-specific behavior worth designing around, failure/cleanup of partial
uploads, and whether the Puller redirects clients to presigned S3 URLs or
proxies bytes itself (ticket 05 guarantees one object per NAR, so redirects
are always possible).
