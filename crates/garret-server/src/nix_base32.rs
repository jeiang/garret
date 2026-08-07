//! Nix's base32 alphabet — narinfo hashes are printed in it, nothing else uses it.

const ALPHABET: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";

/// Encode bytes least-significant-group first, as `nix hash convert --to nix32` does.
pub fn encode(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let len = (bytes.len() * 8 - 1) / 5 + 1;
    (0..len)
        .rev()
        .map(|n| {
            let b = n * 5;
            let (i, j) = (b / 8, b % 8);
            let lo = bytes[i] >> j;
            // j == 0 means the group is byte-aligned: no carry, and `<< 8` would overflow.
            let hi = match (j, bytes.get(i + 1)) {
                (0, _) | (_, None) => 0,
                (_, Some(next)) => next << (8 - j),
            };
            ALPHABET[usize::from((lo | hi) & 0x1f)] as char
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::encode;
    use sha2::{Digest, Sha256};

    /// Vectors produced by `nix hash convert --to nix32 --hash-algo sha256`.
    #[test]
    fn matches_nix() {
        assert_eq!(
            encode(&Sha256::digest(b"")),
            "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
        );
        assert_eq!(
            encode(&Sha256::digest(b"garret")),
            "1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52"
        );
        assert_eq!(encode(&[]), "");
    }
}
