//! A seeded synthetic corpus reproducing the measured NAR-size distribution
//! (spec 09-benchmarks): many KB–MB paths, a fat tail, one multi-GB blob, and
//! a compressibility mix including an incompressible "model weights" slice.
//!
//! Seeded so the corpus is identical on any machine — results are comparable
//! across runs and hosts, and no real store contents live in the repo.

/// xorshift64*: a few lines, no dependency, and identical everywhere. Bench
/// input needs reproducibility, not statistical quality.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// Uniform in `[0, n)`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub hash: String,
    pub name: String,
    pub size: usize,
    /// 0 = incompressible (model weights), 100 = trivially compressible.
    pub compressibility: u8,
}

impl Entry {
    pub fn store_path(&self) -> String {
        format!("/nix/store/{}-{}", self.hash, self.name)
    }

    /// Deterministic body: the same entry always produces the same bytes, so
    /// a rerun pushes an identical corpus.
    pub fn body(&self) -> Vec<u8> {
        let mut rng = Rng::new(u64::from_str_radix(&self.hash[..16], 32).unwrap_or(1));
        let mut out = Vec::with_capacity(self.size);
        // A run of repeats compresses; random bytes do not. Mixing the two in
        // proportion gives a body with roughly the intended compressibility.
        while out.len() < self.size {
            if rng.below(100) < self.compressibility as u64 {
                let run = 64.min(self.size - out.len());
                out.extend(std::iter::repeat_n(b'a', run));
            } else {
                let chunk = rng.next_u64().to_le_bytes();
                let take = chunk.len().min(self.size - out.len());
                out.extend_from_slice(&chunk[..take]);
            }
        }
        out
    }
}

const NIX_BASE32: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";

/// `count` entries plus, when `with_giant` is set, one multi-GB blob — the fat
/// tail is the interesting part of the real distribution, not the median.
pub fn generate(seed: u64, count: usize, with_giant: bool) -> Vec<Entry> {
    let mut rng = Rng::new(seed);
    let mut entries = Vec::with_capacity(count + 1);
    for i in 0..count {
        let hash: String = (0..32)
            .map(|_| NIX_BASE32[rng.below(32) as usize] as char)
            .collect();
        // Log-ish spread from ~4 KiB to ~64 MiB, weighted small.
        let magnitude = rng.below(5);
        let size = (4096u64 << (magnitude * 3)) + rng.below(4096);
        entries.push(Entry {
            hash,
            name: format!("bench-{i}"),
            size: size as usize,
            // One in five is dedup- and compression-inert, as measured.
            compressibility: if rng.below(5) == 0 { 0 } else { 70 },
        });
    }
    if with_giant {
        let hash: String = (0..32)
            .map(|_| NIX_BASE32[rng.below(32) as usize] as char)
            .collect();
        entries.push(Entry {
            hash,
            name: "bench-giant".into(),
            size: 2 * 1024 * 1024 * 1024,
            compressibility: 0,
        });
    }
    entries
}

/// The same corpus under different keys: identical sizes and compressibility,
/// so a serial run over it measures exactly the work the load run does. A
/// baseline drawn from a *different* size distribution would compare latency
/// against unrelated latency.
pub fn rekey(entries: &[Entry]) -> Vec<Entry> {
    entries
        .iter()
        .map(|entry| Entry {
            // Advance the first character rather than fixing it: a constant
            // prefix collides with every entry that already starts with it,
            // and a collision is silently skipped as already-present.
            hash: format!("{}{}", next_char(&entry.hash), &entry.hash[1..]),
            name: format!("{}-baseline", entry.name),
            ..entry.clone()
        })
        .collect()
}

fn next_char(hash: &str) -> char {
    let first = hash.bytes().next().unwrap_or(b'0');
    let index = NIX_BASE32.iter().position(|c| *c == first).unwrap_or(0);
    NIX_BASE32[(index + 1) % NIX_BASE32.len()] as char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_gives_the_same_corpus() {
        assert_eq!(generate(42, 20, false), generate(42, 20, false));
        assert_ne!(generate(42, 20, false), generate(43, 20, false));
    }

    #[test]
    fn bodies_are_deterministic_and_the_right_length() {
        let entry = &generate(7, 3, false)[0];
        assert_eq!(entry.body(), entry.body());
        assert_eq!(entry.body().len(), entry.size);
    }

    #[test]
    fn compressibility_actually_varies() {
        let squishy = Entry {
            hash: "a".repeat(32),
            name: "x".into(),
            size: 256 * 1024,
            compressibility: 100,
        };
        let solid = Entry {
            compressibility: 0,
            ..squishy.clone()
        };
        let ratio = |e: &Entry| zstd::encode_all(e.body().as_slice(), 3).unwrap().len();
        // The incompressible slice must not quietly compress away, or the
        // benchmark measures the wrong thing entirely.
        assert!(ratio(&squishy) < e_len(&squishy) / 10);
        assert!(ratio(&solid) > e_len(&solid) / 2);
    }

    fn e_len(entry: &Entry) -> usize {
        entry.size
    }

    #[test]
    fn rekeying_moves_every_first_character() {
        // Including the wrap at the end of the alphabet.
        for (input, expected) in [("0aa", '1'), ("zaa", '0'), ("baa", 'c')] {
            assert_eq!(next_char(input), expected);
        }
    }

    #[test]
    fn the_baseline_corpus_matches_the_load_corpus_in_shape() {
        let load = generate(3, 10, false);
        let baseline = rekey(&load);
        // Same work, different keys — otherwise the comparison is meaningless.
        assert_eq!(
            load.iter().map(|e| e.size).collect::<Vec<_>>(),
            baseline.iter().map(|e| e.size).collect::<Vec<_>>()
        );
        assert_eq!(
            load.iter().map(|e| e.compressibility).collect::<Vec<_>>(),
            baseline
                .iter()
                .map(|e| e.compressibility)
                .collect::<Vec<_>>()
        );
        for (a, b) in load.iter().zip(&baseline) {
            assert_ne!(
                a.hash, b.hash,
                "baseline must not collide with the load run"
            );
        }
    }

    #[test]
    fn the_fat_tail_is_present_when_asked_for() {
        let with = generate(1, 5, true);
        assert_eq!(with.len(), 6);
        assert!(with.last().unwrap().size >= 2 * 1024 * 1024 * 1024);
        assert_eq!(generate(1, 5, false).len(), 5);
    }
}
