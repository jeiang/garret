# Measure dedup ratio on real closures

Type: task
Status: resolved

## Question

What dedup ratio would chunking actually achieve on this infrastructure's
real build outputs? Run FastCDC chunking (attic's actual defaults:
16/64/256 KiB min/avg/max, 64 KiB threshold — implementation in
`/Users/aidanp/Projects/attic/attic/src/chunking/`; ticket 02 corrected the
earlier 64/256/512 figure) over representative closures from the local
`/nix/store` (e.g. a NixOS system closure and a few large dev shells, plus
two successive rebuilds of the same configuration if available). Report:

- Unique-chunk ratio within one closure, and cross-closure dedup between
  successive rebuilds — compare against the whole-NAR-dedup baseline
  (identical NAR hashes), since that's the alternative garret would ship
  (ticket 02's leaning).
- A chunk-size sweep (e.g. 64 KiB / 256 KiB / 1 MiB / 2 MiB averages) —
  ticket 02 found backup tools converged on much larger chunks than attic.
- Chunk-count-per-NAR distribution and projected SQLite row counts at each
  size.
- If cheap to add: how much of the corpus is internally-compressed payloads
  (which dedup ~0% under CDC).

This is the empirical half of the chunking decision; ticket 02 is the
literature half.

## Answer

Measured on artemis: 20 successive system generations from cornn-flaek,
5,134 unique paths, 74.57 GB NAR bytes (49% of which is four GGUF model
files). Full report: [research/dedup-measurement.md](../research/dedup-measurement.md);
tool + raw outputs in `research/dedup-measurement-tool/`.

- **Incremental CI proxy** (19 rebuilds, compressed, non-GGUF): chunking
  stores **1.59×–1.93× less** than whole-NAR zstd-3 — 3.74 GB at attic's
  16/64/256 KiB vs 7.21 GB baseline; still 4.53 GB at borg-like
  512/2048/8192 KiB.
- The win concentrates in the one nixpkgs-bump generation (55.8% of its
  17.6 GB of new paths deduped against prior chunks). Config tweaks dedup
  93–95% but are tiny (~8–30 MB); genuinely new content dedups ~0%.
- **Whole-NAR-hash dedup is worthless**: 1.009× over path identity
  corpus-wide, 3.3% incremental — rebuilt NARs are almost never
  bit-identical. The shared baseline both designs get is path negotiation
  (~99% of each generation's paths unchanged).
- Whole-corpus compressed advantage of chunking: 1.34× on ordinary
  content but **only ~1.08× on the real mixed store**, because model
  weights (incompressible, dedup-inert) dominate; ~63% of unique corpus
  bytes are CDC/zstd-inert.
- The sweep flattens after compression: coarse 512/2048/8192 keeps 77%
  (256/1024/4096 keeps 85%) of the finest setting's savings with 3.8% of
  the rows (29k vs 759k chunks), p90 of 3 chunks/NAR vs 90 — most NARs
  stay single-chunk, preserving presigned-redirect reads.
- **Implication for ticket 05**: chunking is defensible only if
  rebuild-churn storage is the binding cost; if adopted, use ≥1 MiB
  average chunks, not attic's 64 KiB. Whole-NAR remains the simpler
  default given ~8% end-to-end saving on this store's actual contents.
