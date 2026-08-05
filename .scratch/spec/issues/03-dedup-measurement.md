# Measure dedup ratio on real closures

Type: task
Status: open

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
