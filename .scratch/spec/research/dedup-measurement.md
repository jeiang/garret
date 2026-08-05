# Dedup measurement: FastCDC vs whole-NAR on real build outputs

Empirical half of the chunking decision (companion to `chunking-state-of-the-art.md`;
resolves ticket 03, feeds ticket 05). Measured 2026-08-05 on artemis
(NixOS, Determinate Nix 3.21.9, x86_64, 8 cores), against the closures of **20 successive
NixOS system generations** (116–135) of the user's own config (`~/Projects/cornn-flaek`),
including one nixpkgs bump (gen 126→127, `20260606.a799d3e` → `20260718.61b7c44`) and many
small config-tweak rebuilds.

Corpus: **5,134 unique store paths, 74.57 GB** of uncompressed NAR bytes.
Composition matters: **49.3% of corpus bytes (36.79 GB) are four GGUF model-weight files**
(quantized LLMs served by llama-swap) — high-entropy blobs that neither dedup (0.03%
self/cross dedup even at the finest chunking) nor compress (zstd-3 = 1.037x, measured on a
256 MiB slab). The remaining 5,130 paths (37.78 GB) are ordinary system/package content.

## Headline verdict

**Incremental cross-rebuild storage/upload (the CI proxy): after seeding generation 116,
the 19 subsequent rebuilds, non-GGUF, zstd-3-compressed stored bytes:**

| Scheme | Stored (compressed) | vs whole-NAR baseline |
|---|---|---|
| Whole-NAR zstd-3 + NAR-hash dedup (baseline) | 7.21 GB | 1.00x |
| FastCDC 16/64/256 KiB (attic default) | 3.74 GB | **1.93x** |
| FastCDC 64/256/1024 KiB | 3.95 GB | 1.82x |
| FastCDC 256/1024/4096 KiB | 4.26 GB | 1.69x |
| FastCDC 512/2048/8192 KiB (borg-like) | 4.53 GB | 1.59x |

**Whole-corpus dedup (raw bytes, what a long-retention cache stores):**

| Scheme | Corpus incl GGUF (74.57 GB) | Non-GGUF only (37.78 GB) | Incremental raw, non-GGUF (19.87 GB new) |
|---|---|---|---|
| Store-path identity (both designs get this) | 1.000x | 1.000x | baseline |
| + whole-NAR-hash dedup | 1.009x | 1.018x | −3.3% |
| 16/64/256 KiB | 1.200x | 1.490x | −51.6% |
| 64/256/1024 KiB | 1.165x | 1.386x | −45.0% |
| 256/1024/4096 KiB | 1.125x | 1.281x | −36.9% |
| 512/2048/8192 KiB | 1.104x | 1.229x | −32.1% |

**Whole-corpus compressed** (zstd-3, non-GGUF): whole-NAR+narHash-dedup stores 14.27 GB;
chunked stores 10.68 / 10.84 / 11.10 / 11.36 GB (finest→coarsest) = **1.34x / 1.32x /
1.29x / 1.26x** better. Folding the GGUFs back in (≈35.5 GB stored either way), the
**total real-corpus advantage of chunking collapses to ≈1.08x**.

Interpretation for ticket 05:

1. **Whole-NAR-hash dedup is nearly worthless as a dedup strategy** (0.9–1.8% over path
   identity). Its value is that it's free. The real baseline dedup is get-missing-paths
   negotiation, which both designs get: ~99% of each successive generation's paths are
   shared and never re-uploaded (2,151–2,400 paths per gen; typically only 16–120 new).
2. **Chunking's genuine win is on mass-rebuild deltas**: the nixpkgs-bump generation
   uploaded 17.61 GB of new paths of which 55.8% deduped against the previous
   generation's chunks (at 16/64/256; 34.9% even at borg-size). Config-tweak rebuilds
   dedup 93–95% but are only ~8–30 MB each. Genuinely new content (new packages, model
   weights) dedups ~0–12%.
3. **After compression the parameter sweep flattens**: coarse 512/2048/8192 keeps 77% of
   the finest setting's incremental savings (85% at 256/1024/4096) with **3.8% of the
   chunk rows** and a p90 of 3 chunks/NAR instead of 90. Fine attic-style chunks buy
   ~0.5–0.8 GB extra over 19 rebuilds while costing 26x the metadata and 30x the
   read-path fan-out. If garret chunks at all, it should chunk coarse (≥1 MiB average).
4. On this infrastructure's actual store contents — where incompressible model weights
   are half the bytes and disk is local and 34% full — chunking's end-to-end storage
   saving is ~8%, against real costs (proxy reassembly, refcount GC, 10⁵–10⁷ extra rows).
   For the rebuild-delta subset it is a solid ~1.6–1.9x. The decision hinges on whether
   storage of *rebuild churn* is the binding cost; these numbers say nothing else moves.

## Methodology

- **Corpus**: union of `nix path-info -r --json` closures of
  `/nix/var/nix/profiles/system-{116..135}-link`. All paths already realized in the
  store; nothing was built. devShells were skipped (devenv-based, impure eval fails;
  the system closures already contain the flake's own packages — caddy, attic-server,
  netbird, llama-cpp, hath-rust, etc.).
- **Tool**: `dedup-measurement-tool/` (Rust; sources preserved next to this file).
  Streams each NAR once via `nix-store --dump`, fans the byte stream out to four
  concurrent FastCDC chunkers (`fastcdc` crate 3.x, **v2020 StreamCDC**) — one per
  parameter set — hashing each chunk with blake3 (128-bit ids). Never buffers a NAR in
  memory or on disk. 74.57 GB chunked 4 ways in 82 s wall (jobs=3, ≤8 threads).
- **Attic-mirroring threshold**: NARs < 64 KiB are not chunked; they are recorded as a
  single whole-NAR chunk under every parameter set (attic's `nar-size-threshold`
  behavior). 1,915 of 5,134 paths were below threshold (37% of paths, <0.2% of bytes).
- **Parameter sets** (min/avg/max): 16/64/256 KiB (attic ships this), 64/256/1024 KiB,
  256/1024/4096 KiB, 512/2048/8192 KiB (borg's defaults).
- **Compression pass**: second run with `--zstd` over the 5,130 non-GGUF paths recording
  per-chunk `zstd::bulk::compress(level 3)` sizes plus a whole-NAR streaming zstd-3 size
  per path. GGUFs excluded and handled analytically (measured 1.037x on a slab).
- **Incremental simulation**: generations replayed in order 116→135 against a cumulative
  cache. Per generation, "upload" = bytes of paths not yet in the cache, under three
  schemes: path identity (bytes of all new paths), narHash dedup (skip new paths whose
  NAR hash was seen), chunk dedup (only chunks never seen, dedup applied within the
  batch too). This models a CI cache with infinite retention over the series.
- **Baselines**: `narHash`/`narSize` from `nix path-info --json` (no chunking needed).
  Sanity check: tool-observed NAR sizes matched path-info narSize for all 5,134 paths.

## Per-workload detail (raw bytes, incremental against cumulative cache)

| Rebuild type | Example gens | New-path bytes | narHash saves | Chunk dedup saves (fine → coarse) |
|---|---|---|---|---|
| Config tweak (no pkg changes) | 117, 120–126, 129, 132–134 | 8–31 MB each | ~0% | **94% → 47%** (e.g. gen 134: 7.97 MB → 0.44 MB at 16/64/256, 4.2 MB at 512/2048) |
| Package additions to config | 118, 119, 123, 130, 131 | 81–711 MB each | ~0% | 2–20% (mostly genuinely new bytes) |
| **nixpkgs bump (mass rebuild)** | **127** | **17.61 GB** | 3.7% | **55.8% → 34.9%** |
| New large content | 128 (llama-cpp + 7.36 GB GGUF), 135 (three GGUFs, 29.43 GB) | 7.63 / 29.43 GB | ~0% | 0.07–0.16% |

The nixpkgs bump is the workload chunking exists for: thousands of same-package-new-hash
paths whose contents are half-identical to their predecessors. Note narHash dedup catches
almost none of it (0.65 GB of 17.61 GB) — rebuilt NARs are rarely bit-identical.

Chunks-per-NAR distribution (whole corpus):

| Set | median | p90 | max | mean |
|---|---|---|---|---|
| 16/64/256 | 3 | 90 | 126,708 (10.1 GB GGUF) | 176 |
| 64/256/1024 | 1 | 23 | 31,708 | 44 |
| 256/1024/4096 | 1 | 6 | 7,850 | 12 |
| 512/2048/8192 | 1 | 3 | 3,931 | 6 |

At ≥256 KiB-min settings, the majority of NARs are single-chunk — meaning the
presigned-redirect read path survives for most objects and proxy reassembly is confined
to the large-NAR tail (which is also where it hurts most: a 10 GB NAR is 3.9k–127k GETs).

## Row-count projections

Measured on this 69.4 GiB (74.57 GB) corpus, and scaled per TiB of unique NAR bytes:

| Set | chunk rows | chunkref rows | rows/GiB (chunk+ref) | per 1 TiB unique data |
|---|---|---|---|---|
| 16/64/256 | 759,055 | 905,903 | ~24.0k | ~11.2M chunks + 13.4M refs |
| 64/256/1024 | 196,415 | 227,805 | ~6.1k | ~2.9M + 3.4M |
| 256/1024/4096 | 53,295 | 60,018 | ~1.6k | ~0.8M + 0.9M |
| 512/2048/8192 | 29,053 | 32,232 | ~0.9k | ~0.4M + 0.5M |
| whole-NAR | 5,134 objects | — | 74 | ~76k |

(Object rows: 5,134 either way; non-GGUF-only per-GiB rates are within ~25% of these, so
the projection is not GGUF-skewed.) SQLite handles all of these, but the
GC orphan-sweep join and per-delete fan-out scale with the first two columns.

## Compression interplay (non-GGUF subset, zstd-3)

- Whole-NAR zstd-3: 37.78 GB → 14.64 GB (2.58x), 14.27 GB after narHash dedup.
- **Per-chunk compression window loss** (same bytes, no dedup, vs whole-NAR): 1.101x at
  16/64/256, 1.057x, 1.020x, 1.006x at 512/2048/8192. Smaller than feared in the
  literature review — but real at attic's 64 KiB average: it gives back ~10 points of
  the ~49 points chunking gains, before dedup.
- **Incompressible share of unique chunk bytes** (chunk zstd-3 ratio > 0.95): 10.6% /
  9.5% / 8.3% / 7.9% (fine → coarse), non-GGUF. Including model weights, **~63% of the
  full corpus's unique bytes are effectively incompressible and dedup-inert**.

## Caveats

- Single machine, single config lineage, 20 generations (nixpkgs June→August 2026). No
  multi-project CI diversity; a cache holding many unrelated projects would look more
  like the "corpus-wide" column and less like the incremental one.
- The GGUF share (49%) is idiosyncratic to this host (LLM serving) but is a fair warning:
  garret's real store will contain whatever the user actually builds, and one imported
  model outweighs a year of config rebuilds.
- Chunk identity is blake3-128, not attic's SHA-256; irrelevant to ratios.
- Dedup simulated with infinite retention; GC/retention interacts with chunking's
  refcount costs and is not modeled.
- Compressed incremental figures exclude GGUFs from both sides (they'd add ≈35.5 GB to
  both, since they neither dedup nor compress).
- Per-chunk zstd used independent frames (attic's model), no dictionary.

## Appendix: reproduction

Tool sources: `dedup-measurement-tool/` (this directory) — `src/main.rs` (chunker),
`analysis.py` (main metrics), `addendum.py` (non-GGUF splits, sanity checks); raw outputs
in `analysis-out.json` / `addendum-out.json`. All remote work under `/tmp/garret-dedup/`
on artemis (192.168.100.219), removed afterwards.

```bash
# on artemis
mkdir -p /tmp/garret-dedup && cd /tmp/garret-dedup
for g in $(seq 116 135); do
  nix path-info -r --json /nix/var/nix/profiles/system-$g-link > gen$g.json
done
jq -rs 'add | to_entries[] | "\(.value.narSize) \(.key)"' gen*.json \
  | sort -u -k2 | sort -rn > union-paths.txt          # 5134 paths
# build & run chunker (fastcdc v2020, blake3, 64 KiB attic threshold baked in)
nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#gcc \
  --command cargo build --release                      # in chunkmeter/
./chunkmeter/target/release/chunkmeter union-paths.txt corpus.jsonl --jobs 3
grep -v '\.gguf' union-paths.txt > zstd-paths.txt
./chunkmeter/target/release/chunkmeter zstd-paths.txt corpus-zstd.jsonl --jobs 3 --zstd
# GGUF compressibility spot check
head -c 268435456 /nix/store/k3500y7…-….gguf | zstd -3 -c | wc -c   # → 258783420 (1.037x)
# metrics
nix shell nixpkgs#python3 --command python3 analysis.py > analysis-out.json
nix shell nixpkgs#python3 --command python3 addendum.py > addendum-out.json
```
