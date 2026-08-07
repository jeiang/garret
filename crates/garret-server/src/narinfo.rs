//! Narinfo rendering and ed25519 signing. Signatures are computed on write
//! (spec 02-database: `sigs` is stored); the Puller does no crypto.

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ed25519_dalek::{Signer, SigningKey};

use crate::db::Object;

pub struct SigningKeyFile {
    pub name: String,
    key: SigningKey,
}

impl SigningKeyFile {
    /// Parses nix's secret-key format: `<name>:<base64 of 64-byte keypair>`.
    pub fn parse(contents: &str) -> Result<Self> {
        let (name, b64) = contents
            .trim()
            .split_once(':')
            .context("malformed signing key: expected `name:base64`")?;
        let bytes = B64.decode(b64).context("signing key is not valid base64")?;
        let seed: [u8; 32] = bytes
            .get(..32)
            .and_then(|s| s.try_into().ok())
            .context("signing key is too short")?;
        Ok(Self {
            name: name.to_owned(),
            key: SigningKey::from_bytes(&seed),
        })
    }

    pub fn public_key(&self) -> String {
        format!(
            "{}:{}",
            self.name,
            B64.encode(self.key.verifying_key().to_bytes())
        )
    }

    fn sign(&self, fingerprint: &str) -> String {
        format!(
            "{}:{}",
            self.name,
            B64.encode(self.key.sign(fingerprint.as_bytes()).to_bytes())
        )
    }
}

/// Nix's signed fingerprint. References are full store paths, sorted — nix
/// holds them in a sorted set, so any other order verifies as a bad signature.
fn fingerprint(obj: &Object, store_dir: &str) -> String {
    let refs = obj
        .references
        .iter()
        .map(|r| format!("{store_dir}/{r}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "1;{};{};{};{}",
        obj.store_path, obj.nar_hash, obj.nar_size, refs
    )
}

/// Signs with every configured key (multi-key overlap rotation, spec 10-packaging).
pub fn sign(obj: &Object, store_dir: &str, keys: &[SigningKeyFile]) -> Result<Vec<String>> {
    if keys.is_empty() {
        bail!("no signing keys configured");
    }
    let fp = fingerprint(obj, store_dir);
    Ok(keys.iter().map(|k| k.sign(&fp)).collect())
}

/// Renders the narinfo body served by the Puller.
pub fn render(obj: &Object) -> String {
    let mut out = String::new();
    out.push_str(&format!("StorePath: {}\n", obj.store_path));
    out.push_str(&format!("URL: nar/{}.nar.zst\n", obj.store_path_hash));
    out.push_str("Compression: zstd\n");
    out.push_str(&format!("FileHash: {}\n", obj.file_hash));
    out.push_str(&format!("FileSize: {}\n", obj.file_size));
    out.push_str(&format!("NarHash: {}\n", obj.nar_hash));
    out.push_str(&format!("NarSize: {}\n", obj.nar_size));
    out.push_str(&format!("References: {}\n", obj.references.join(" ")));
    if let Some(d) = &obj.deriver {
        out.push_str(&format!("Deriver: {d}\n"));
    }
    if let Some(ca) = &obj.ca {
        out.push_str(&format!("CA: {ca}\n"));
    }
    for sig in &obj.sigs {
        out.push_str(&format!("Sig: {sig}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier, VerifyingKey};

    fn object() -> Object {
        Object {
            store_path_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            store_path: "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello-1.0".into(),
            name: "hello-1.0".into(),
            nar_hash: "sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73".into(),
            nar_size: 4096,
            file_hash: "sha256:1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52".into(),
            file_size: 1024,
            deriver: None,
            ca: None,
            // deliberately unsorted: sign() must not depend on caller order
            references: vec!["cccccccccccccccccccccccccccccccc-b".into()],
            sigs: vec![],
            pushed_by: None,
        }
    }

    #[test]
    fn signature_verifies_over_the_fingerprint() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let keypair: Vec<u8> = key
            .to_bytes()
            .iter()
            .chain(key.verifying_key().to_bytes().iter())
            .copied()
            .collect();
        let file = SigningKeyFile::parse(&format!("test-1:{}", B64.encode(&keypair))).unwrap();

        let obj = object();
        let sigs = sign(&obj, "/nix/store", std::slice::from_ref(&file)).unwrap();
        let (name, b64) = sigs[0].split_once(':').unwrap();
        assert_eq!(name, "test-1");

        let sig = ed25519_dalek::Signature::from_slice(&B64.decode(b64).unwrap()).unwrap();
        let public = VerifyingKey::from_bytes(
            &B64.decode(file.public_key().split_once(':').unwrap().1)
                .unwrap()[..]
                .try_into()
                .unwrap(),
        )
        .unwrap();
        assert!(
            public
                .verify(fingerprint(&obj, "/nix/store").as_bytes(), &sig)
                .is_ok()
        );
    }

    #[test]
    fn fingerprint_uses_full_store_paths() {
        assert_eq!(
            fingerprint(&object(), "/nix/store"),
            "1;/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello-1.0;\
             sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73;4096;\
             /nix/store/cccccccccccccccccccccccccccccccc-b"
        );
    }

    #[test]
    fn unsigned_object_is_refused() {
        assert!(sign(&object(), "/nix/store", &[]).is_err());
    }

    #[test]
    fn narinfo_has_the_fields_nix_requires() {
        let mut obj = object();
        obj.sigs = vec!["test-1:abc".into()];
        let text = render(&obj);
        for field in [
            "StorePath:",
            "URL: nar/",
            "Compression: zstd",
            "FileHash:",
            "FileSize:",
            "NarHash:",
            "NarSize:",
            "References:",
            "Sig:",
        ] {
            assert!(text.contains(field), "narinfo missing {field}:\n{text}");
        }
        assert!(!text.contains("Deriver:"));
    }
}
