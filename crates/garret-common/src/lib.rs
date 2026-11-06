//! Wire types and store-path helpers shared by the client and the server.

#![warn(missing_docs)]

pub mod admin;
pub mod wire;

use serde::{Deserialize, Serialize};

/// The JSON metadata that prefixes an uploaded NAR (spec 01-push-protocol).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preamble {
    /// Full `/nix/store/<hash>-<name>` path of the object being pushed.
    pub store_path: String,
    /// `sha256:` hash of the uncompressed NAR, as `nix path-info` reports it.
    pub nar_hash: String,
    /// Uncompressed NAR size in bytes.
    pub nar_size: i64,
    /// Full store paths — narinfo prints reference names and the signed
    /// fingerprint uses full paths, neither recoverable from a hash.
    pub references: Vec<String>,
    /// Store path of the deriving `.drv`, when the local store knows it.
    pub deriver: Option<String>,
    /// Content-address string for CA paths, passed through to the narinfo.
    pub ca: Option<String>,
}

impl Preamble {
    /// Serialises to the 4-byte little-endian length prefix plus JSON.
    pub fn to_framed(&self) -> serde_json::Result<Vec<u8>> {
        let json = serde_json::to_vec(self)?;
        let mut out = Vec::with_capacity(4 + json.len());
        out.extend_from_slice(&(json.len() as u32).to_le_bytes());
        out.extend_from_slice(&json);
        Ok(out)
    }
}

/// Where a framed preamble ends in `buf`: the 4-byte little-endian length
/// plus that many bytes of JSON. `Ok(None)` until all of it has arrived;
/// `Err(len)` for a declared length over `max`, refused before any wait.
pub fn preamble_end(buf: &[u8], max: usize) -> Result<Option<usize>, usize> {
    let Some(&[a, b, c, d]) = buf.first_chunk::<4>() else {
        return Ok(None);
    };
    let len = u32::from_le_bytes([a, b, c, d]) as usize;
    if len > max {
        return Err(len);
    }
    Ok((buf.len() - 4 >= len).then_some(4 + len))
}

/// `<hash>-<name>` → hash, the object key everything else is keyed by.
pub fn hash_of_store_path(path: &str) -> &str {
    let base = path.rsplit('/').next().unwrap_or(path);
    // `get` rather than slicing: a multibyte character straddling byte 32
    // would make `&base[..32]` panic.
    base.get(..32).unwrap_or(base)
}

/// Nix's base32 alphabet (no `e`, `o`, `u`, `t`), as used by store path
/// hashes and narinfo hashes.
pub const NIX_BASE32_ALPHABET: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";

/// Whether every byte of `s` is in [`NIX_BASE32_ALPHABET`].
pub fn is_nix_base32(s: &str) -> bool {
    s.bytes().all(|b| NIX_BASE32_ALPHABET.contains(&b))
}

/// Length of the Nix base32 spelling of `bytes` bytes.
pub const fn nix_base32_len(bytes: usize) -> usize {
    if bytes == 0 {
        0
    } else {
        (bytes * 8 - 1) / 5 + 1
    }
}

/// Nix's base32 encoding, least-significant group first, as
/// `nix hash convert --to nix32` does.
pub fn nix_base32_encode(bytes: &[u8]) -> String {
    (0..nix_base32_len(bytes.len()))
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
            NIX_BASE32_ALPHABET[usize::from((lo | hi) & 0x1f)] as char
        })
        .collect()
}

/// A sha256 digest as Nix spells it in narinfo: `sha256:<nix-base32>`.
pub fn sha256_nix32(digest: &[u8]) -> String {
    format!("sha256:{}", nix_base32_encode(digest))
}

/// Whether `s` is a store path hash: exactly 32 characters of Nix's base32
/// alphabet.
pub fn is_store_hash(s: &str) -> bool {
    s.len() == 32 && is_nix_base32(s)
}

/// `{store_dir}/{hash}-{name}` → `{hash}-{name}`, if nix would accept it as a
/// store path: an [`is_store_hash`] hash, and a name of 1–211 characters from
/// `[A-Za-z0-9+-._?=]`.
pub fn store_basename<'a>(path: &'a str, store_dir: &str) -> Option<&'a str> {
    let base = path.strip_prefix(store_dir)?.strip_prefix('/')?;
    let (hash, name) = base.split_once('-')?;
    let name_ok = (1..=211).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+-._?=".contains(&b));
    (is_store_hash(hash) && name_ok).then_some(base)
}

/// A store-path hash, given bare or as a store path (`/nix/store/<hash>-<name>`
/// or `<hash>-<name>`, trailing `/` allowed). `None` unless the result is a
/// valid [`is_store_hash`], so a typo is refused up front.
pub fn parse_store_hash(text: &str) -> Option<&str> {
    let base = text
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(text);
    let hash = base.split('-').next().unwrap_or(base);
    is_store_hash(hash).then_some(hash)
}

/// Bytes as a short human-readable size (`B`, `KiB`, `MiB`, `GiB`, `TiB`).
pub fn human_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_round_trips() {
        let p = Preamble {
            store_path: "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x".into(),
            nar_hash: "sha256:x".into(),
            nar_size: 1,
            references: vec![],
            deriver: None,
            ca: None,
        };
        let framed = p.to_framed().unwrap();
        let len = u32::from_le_bytes(framed[..4].try_into().unwrap()) as usize;
        assert_eq!(framed.len(), 4 + len);
        let back: Preamble = serde_json::from_slice(&framed[4..]).unwrap();
        assert_eq!(back.store_path, p.store_path);
    }

    #[test]
    fn hash_comes_off_the_basename() {
        assert_eq!(
            hash_of_store_path("/nix/store/v2xb3fmd3qd34s7q7sngajj8rjl5k25j-payload"),
            "v2xb3fmd3qd34s7q7sngajj8rjl5k25j"
        );
    }

    #[test]
    fn hash_of_multibyte_input_does_not_panic() {
        // 31 ASCII bytes then a 2-byte char: byte 32 is mid-character.
        let name = format!("{}é-x", "a".repeat(31));
        assert_eq!(hash_of_store_path(&name), name);
        assert_eq!(hash_of_store_path("short"), "short");
    }

    #[test]
    fn store_paths_and_bare_hashes_name_the_same_object() {
        let hash = "9anpmlwx6772cpf1782ilhwbjr1x0y47";
        for arg in [
            hash.to_owned(),
            format!("{hash}-garret-deploy-check.txt"),
            format!("/nix/store/{hash}-garret-deploy-check.txt"),
            format!("/nix/store/{hash}-dir/"),
        ] {
            assert_eq!(parse_store_hash(&arg), Some(hash), "{arg:?}");
        }
        for bad in [
            "",
            "/nix/store/",
            "9anpmlwx6772cpf1782ilhwbjr1x0y4",
            "9anpmlwx6772cpf1782ilhwbjr1x0y47x",
            "eanpmlwx6772cpf1782ilhwbjr1x0y47",
            "/nix/store/9anpmlwx",
        ] {
            assert_eq!(parse_store_hash(bad), None, "{bad:?} should be rejected");
        }
    }

    /// Vectors produced by `nix hash convert --to nix32 --hash-algo sha256`
    /// for the sha256 of `""` and of `"garret"`.
    #[test]
    fn nix_base32_matches_nix() {
        let hex = |s: &str| -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        };
        assert_eq!(
            nix_base32_encode(&hex(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )),
            "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
        );
        assert_eq!(
            nix_base32_encode(&hex(
                "a2dc98e1f6875dfd2c63d0c8a79616a8848728ff96660326b75bd0d3dae0dfe0"
            )),
            "1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52"
        );
        assert_eq!(nix_base32_encode(&[]), "");
    }

    #[test]
    fn sizes_are_humanised() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(3 << 30), "3.0 GiB");
    }
}

/// Kani proofs (`just kani`): exhaustive over every input up to the bounds.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// The framing never panics, and a frame it reports is exactly the
    /// declared length, within `max`, wholly inside the buffer.
    #[kani::proof]
    #[kani::unwind(10)]
    fn preamble_end_is_exact() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|l| *l <= 8);
        let max: usize = kani::any();
        let buf = &bytes[..len];
        match preamble_end(buf, max) {
            Ok(Some(end)) => {
                let declared = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
                assert!(end == 4 + declared && end <= buf.len() && declared <= max);
            }
            Ok(None) => {
                assert!(
                    buf.len() < 4
                        || buf.len() - 4
                            < u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize
                );
            }
            Err(declared) => assert!(declared > max),
        }
    }

    /// What `to_framed` writes, the Pusher reads back: the frame ends after
    /// the declared length whatever NAR bytes follow it.
    #[kani::proof]
    #[kani::unwind(10)]
    fn preamble_end_round_trips_the_header() {
        let json_len: u32 = kani::any_where(|l| *l <= 3);
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&json_len.to_le_bytes());
        let total: usize = kani::any_where(|t| *t >= 4 + json_len as usize && *t <= 8);
        assert_eq!(
            preamble_end(&buf[..total], 3),
            Ok(Some(4 + json_len as usize))
        );
    }
}
