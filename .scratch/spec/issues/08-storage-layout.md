# S3 storage layout and interaction strategy

Type: grilling
Status: open
Blocked by: 05

## Question

Design the S3 side: object key naming scheme, when to use multipart vs
single PUT, bounded part concurrency (attic's OOM vector — see
`OPTIMIZATION_PLAN.md` step 2), read-path streaming (prefetch depth for
chunk reassembly if chunking won — `OPTIMIZATIONS.md` item 7; range
requests), Garage-specific behavior worth designing around, failure/cleanup
of partial uploads, and whether the Puller redirects clients to presigned
S3 URLs or proxies bytes itself.
