# 20 — Hardware-accelerated SHA-256 in the Pusher

Status: proposed, measured (2026-08 review). Evidence:
[baseline](../research/benchmark-baseline-2026-08.md).

## Problem

`put_streaming` hashes every stored byte (the system's only integrity
check since ADR-0005). `sha2 = "0.10"` with default features uses the
portable software implementation and ignores the SHA extensions on both
Apple Silicon and modern x86.

## Measurement

`cargo bench -p garret-bench -- sha256`, 4 MiB body, aarch64 (M-series):

| | throughput |
|---|---|
| sha2 0.10 default | 315 MiB/s |
| sha2 0.10 `features = ["asm"]` | **1.61 GiB/s (5.2×)** |

315 MiB/s single-core already shadows the measured 200 MiB/s single-stream
push and is the first wall a cpu-limited sandbox will hit.

## Fix

One line in `crates/garret-server/Cargo.toml`:
`sha2 = { version = "0.10", features = ["asm"] }` — and mirror it in
garret-bench's dev-dependency so the micro-bench keeps measuring what the
server ships. Verify the feature builds on x86_64-linux (it uses
`sha2-asm`/cc) before the sandbox runs; `sha2` 0.11 does runtime detection
by default and removes the flag when the tree upgrades.

## Score

Speed **high** (measured) · Ops **high** (none) · UX — (invisible).
