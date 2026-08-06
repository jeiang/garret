# Whole-NAR storage: no chunking, no content dedup, trusted NarHash

Each object is stored as a single client-compressed zstd NAR in S3, keyed
by store path hash, with no chunking, no content-level dedup, and no
refcounting; the server hashes only the compressed bytes it stores and
trusts the authenticated client's claimed NarHash. This was decided on
measurement, not taste: across 20 real system generations, FastCDC
chunking (any parameter set) cut rebuild-churn storage 1.6–1.9× but only
~8% end-to-end (model weights dominate and are dedup/compression-inert),
identical-NAR dedup measured 1.009× (worthless), and chunking's costs land
exactly where garret optimizes — hot-path CPU, protocol round-trips,
GC complexity, and multi-object reads. Consequences: presigned redirects
are always possible, GC is row⇒blob trivial, and push negotiation is
path-level only. Revisit only if rebuild-churn storage becomes the binding
cost — and then with ≥1 MiB average chunks, never attic's 64 KiB.
Evidence: `.scratch/spec/research/dedup-measurement.md` and
`chunking-state-of-the-art.md`.
