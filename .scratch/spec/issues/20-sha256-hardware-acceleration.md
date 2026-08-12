# 20 — Hardware-accelerated SHA-256 in the Pusher

Status: implemented (2026-08; commit 319bf70). Evidence:
[baseline](../research/benchmark-baseline-2026-08.md).

## Problem

`put_streaming` hashes every stored byte (the system's only integrity
check since ADR-0005). `sha2 = "0.10"` with default features uses the
portable software implementation and ignores the SHA extensions on both
Apple Silicon and modern x86.

## Measurement

`cargo bench -p garret-bench -- sha256`, 4 MiB body:

| | throughput |
|---|---|
| aarch64 (M-series), sha2 0.10 default | 315 MiB/s |
| aarch64, sha2 0.10 `features = ["asm"]` | **1.61 GiB/s (5.2×)** |
| x86_64 (artemis), sha2 0.10 default | **2.21 GiB/s** |

**Scope correction (2026-08-12, after the NixOS runs):** sha2 0.10 already
runtime-detects SHA-NI on x86_64, so stock builds are fast there — the
software-fallback gap is **aarch64-only** (the `asm` feature gates the ARM
sha2 intrinsics). On the aarch64 dev machine, 315 MiB/s single-core
shadows the measured 200 MiB/s single-stream push; x86 deployments are
unaffected.

## Fix

Enable the feature for ARM targets only, so the proven x86 SHA-NI dispatch
stays untouched — in `crates/garret-server/Cargo.toml`:

```toml
[target.'cfg(target_arch = "aarch64")'.dependencies]
sha2 = { version = "0.10", features = ["asm"] }
```

Mirror it in garret-bench's dev-dependency so the micro-bench keeps
measuring what the server ships. `sha2` 0.11 does runtime detection on
aarch64 by default and removes the flag when the tree upgrades.

## Score

Speed **high** (measured) · Ops **high** (none) · UX — (invisible).
