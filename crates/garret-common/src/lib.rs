//! Wire types shared by the client and the server.

#![warn(missing_docs)]

pub mod admin;

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

/// Whether `s` is a store path hash: exactly 32 characters of Nix's base32
/// alphabet.
pub fn is_store_hash(s: &str) -> bool {
    s.len() == 32 && is_nix_base32(s)
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

    #[test]
    fn sizes_are_humanised() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(3 << 30), "3.0 GiB");
    }
}
