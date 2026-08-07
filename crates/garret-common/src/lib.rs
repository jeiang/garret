//! Wire types shared by the client and the server.

pub mod admin;

use serde::{Deserialize, Serialize};

/// The JSON metadata that prefixes an uploaded NAR (spec 01-push-protocol).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preamble {
    pub store_path: String,
    pub nar_hash: String,
    pub nar_size: i64,
    /// Full store paths — narinfo prints reference names and the signed
    /// fingerprint uses full paths, neither recoverable from a hash.
    pub references: Vec<String>,
    pub deriver: Option<String>,
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
    &base[..base.len().min(32)]
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
}
