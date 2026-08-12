//! Micro-benchmarks for the per-byte costs on the push hot path
//! (spec 09-benchmarks): client-side zstd at the corpus's compressibility
//! mix, the server's SHA-256 over the stored stream, and preamble framing.
//!
//! These run in-process with no server, so they are the "maximum speed this
//! CPU can reach" probes — the first numbers to compare when the
//! memory/cpu-limited sandbox looks slow.

// The corpus generator is part of the bin crate, not a library; include it
// directly rather than growing a lib target for one bench. Only Entry::body
// is used here, so the rest of the module is dead code in this target.
#[allow(dead_code)]
#[path = "../src/corpus.rs"]
mod corpus;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use garret_common::Preamble;
use sha2::{Digest, Sha256};
use std::hint::black_box;

const BODY_SIZE: usize = 4 * 1024 * 1024;

fn body(compressibility: u8) -> Vec<u8> {
    corpus::Entry {
        hash: "b".repeat(32),
        name: "bench".into(),
        size: BODY_SIZE,
        compressibility,
    }
    .body()
}

/// zstd over the corpus's three compressibility classes at the default level,
/// plus the level ladder on the mixed class — the knob `garret push
/// --zstd-level` exposes.
fn compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_encode");
    group.throughput(Throughput::Bytes(BODY_SIZE as u64));
    for compressibility in [0u8, 70, 100] {
        let input = body(compressibility);
        group.bench_function(format!("level3/compressibility{compressibility}"), |b| {
            b.iter(|| zstd::encode_all(black_box(input.as_slice()), 3).unwrap())
        });
    }
    let mixed = body(70);
    for level in [1, 9, 19] {
        group.bench_function(format!("level{level}/compressibility70"), |b| {
            b.iter(|| zstd::encode_all(black_box(mixed.as_slice()), level).unwrap())
        });
    }
    group.finish();
}

/// The Pusher's only per-byte CPU cost: SHA-256 over exactly the bytes stored
/// (storage.rs `put_streaming`).
fn hashing(c: &mut Criterion) {
    let input = body(0);
    let mut group = c.benchmark_group("sha256");
    group.throughput(Throughput::Bytes(BODY_SIZE as u64));
    group.bench_function("digest", |b| {
        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(black_box(input.as_slice()));
            hasher.finalize()
        })
    });
    group.finish();
}

/// Preamble encode + decode round-trip — per-request, not per-byte, so this
/// only matters if it ever shows up at thousands of requests per second.
fn framing(c: &mut Criterion) {
    let preamble = Preamble {
        store_path: format!("/nix/store/{}-something-realistic-1.2.3", "a".repeat(32)),
        nar_hash: format!("sha256:{}", "0".repeat(52)),
        nar_size: 123_456_789,
        references: (0..16)
            .map(|i| format!("/nix/store/{}-dep-{i}", "c".repeat(32)))
            .collect(),
        deriver: Some(format!("/nix/store/{}-x.drv", "d".repeat(32))),
        ca: None,
    };
    c.bench_function("preamble/frame+parse", |b| {
        b.iter(|| {
            let framed = black_box(&preamble).to_framed().unwrap();
            let len = u32::from_le_bytes(framed[..4].try_into().unwrap()) as usize;
            let back: Preamble = serde_json::from_slice(&framed[4..4 + len]).unwrap();
            back
        })
    });
}

criterion_group!(benches, compression, hashing, framing);
criterion_main!(benches);
