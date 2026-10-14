//! Narinfo rendering and ed25519 signing. Signatures are computed on write
//! (spec 02-database: `sigs` is stored); the Puller does no crypto.

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use std::fmt::Write as _;

use crate::db::Object;

/// A named ed25519 signing key, in nix's secret-key format.
pub struct SigningKeyFile {
    /// The key name signatures are prefixed with (`cache.example.org-1`).
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

    /// The public half as `<name>:<base64>` — what goes in `trusted-public-keys`.
    pub fn public_key(&self) -> String {
        format!(
            "{}:{}",
            self.name,
            B64.encode(self.key.verifying_key().to_bytes())
        )
    }

    /// Whether `sig` (`<name>:<base64>`) is this key's signature over
    /// `fingerprint`. A different key name, malformed base64 or a bad
    /// signature all answer false.
    fn verifies(&self, sig: &str, fingerprint: &str) -> bool {
        let Some((name, b64)) = sig.split_once(':') else {
            return false;
        };
        if name != self.name {
            return false;
        }
        let Ok(bytes) = B64.decode(b64) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(&bytes) else {
            return false;
        };
        self.key
            .verifying_key()
            .verify(fingerprint.as_bytes(), &sig)
            .is_ok()
    }

    fn sign(&self, fingerprint: &str) -> String {
        format!(
            "{}:{}",
            self.name,
            B64.encode(self.key.sign(fingerprint.as_bytes()).to_bytes())
        )
    }
}

/// Nix prints hashes as `sha256:<nix-base32>` in narinfo and in the signed
/// fingerprint, but `nix path-info` reports SRI (`sha256-<base64>`). Both name
/// the same bytes, and a signature over the wrong spelling verifies as forged —
/// so everything is normalised here, on the way in.
///
/// Content-addressed paths hide this: nix trusts their hash directly and never
/// checks the signature. Only input-addressed paths fail, which is why this
/// survived until the e2e pushed a built derivation.
pub fn normalize_hash(hash: &str) -> Result<String> {
    if let Some(rest) = hash.strip_prefix("sha256:") {
        return Ok(format!("sha256:{}", normalize_digest(rest)?));
    }
    if let Some(rest) = hash.strip_prefix("sha256-") {
        return Ok(format!("sha256:{}", normalize_digest(rest)?));
    }
    bail!("unsupported hash format: {hash}")
}

fn normalize_digest(digest: &str) -> Result<String> {
    // 52 base32 characters is an already-canonical sha256.
    if digest.len() == 52 && garret_common::is_nix_base32(digest) {
        return Ok(digest.to_owned());
    }
    let bytes = B64
        .decode(digest)
        .with_context(|| format!("hash is neither nix-base32 nor base64: {digest}"))?;
    if bytes.len() != 32 {
        bail!("expected a 32-byte sha256, got {} bytes", bytes.len());
    }
    Ok(crate::nix_base32::encode(&bytes))
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

/// Whether at least one of the object's stored signatures verifies against
/// its fingerprint under one of `keys` (their public halves). Used before
/// re-signing: signing a row whose metadata was altered would turn the
/// alteration into a valid signature.
pub fn verify(obj: &Object, store_dir: &str, keys: &[SigningKeyFile]) -> bool {
    let fp = fingerprint(obj, store_dir);
    obj.sigs
        .iter()
        .any(|sig| keys.iter().any(|k| k.verifies(sig, &fp)))
}

/// Renders the narinfo body served by the Puller.
pub fn render(obj: &Object) -> String {
    let mut out = String::with_capacity(
        256 + obj.references.iter().map(|r| r.len() + 1).sum::<usize>()
            + obj.sigs.iter().map(|s| s.len() + 6).sum::<usize>(),
    );
    // Writing to a String cannot fail.
    let _ = writeln!(out, "StorePath: {}", obj.store_path);
    let _ = writeln!(out, "URL: nar/{}.nar.zst", obj.store_path_hash);
    out.push_str("Compression: zstd\n");
    let _ = writeln!(out, "FileHash: {}", obj.file_hash);
    let _ = writeln!(out, "FileSize: {}", obj.file_size);
    let _ = writeln!(out, "NarHash: {}", obj.nar_hash);
    let _ = writeln!(out, "NarSize: {}", obj.nar_size);
    out.push_str("References: ");
    for (i, r) in obj.references.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(r);
    }
    out.push('\n');
    if let Some(d) = &obj.deriver {
        let _ = writeln!(out, "Deriver: {d}");
    }
    if let Some(ca) = &obj.ca {
        let _ = writeln!(out, "CA: {ca}");
    }
    for sig in &obj.sigs {
        let _ = writeln!(out, "Sig: {sig}");
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
    fn hashes_normalise_to_the_form_nix_signs_over() {
        // Same 32 bytes (sha256 of ""), spelled three ways.
        let base32 = "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73";
        let sri = "sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        assert_eq!(normalize_hash(sri).unwrap(), format!("sha256:{base32}"));
        assert_eq!(
            normalize_hash(&format!("sha256:{base32}")).unwrap(),
            format!("sha256:{base32}")
        );
        assert_eq!(
            normalize_hash("sha256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=").unwrap(),
            format!("sha256:{base32}")
        );

        assert!(normalize_hash("md5:abc").is_err());
        assert!(normalize_hash("sha256-not-base64!!").is_err());
        // Right encoding, wrong length: must not silently produce a short hash.
        assert!(normalize_hash("sha256-YWJj").is_err());
    }

    fn test_key() -> SigningKeyFile {
        let key = SigningKey::from_bytes(&[9u8; 32]);
        let keypair: Vec<u8> = key
            .to_bytes()
            .iter()
            .chain(key.verifying_key().to_bytes().iter())
            .copied()
            .collect();
        SigningKeyFile::parse(&format!("test-1:{}", B64.encode(&keypair))).unwrap()
    }

    #[test]
    fn verify_accepts_own_signature_and_rejects_tampering() {
        let keys = [test_key()];
        let mut obj = object();
        obj.sigs = sign(&obj, "/nix/store", &keys).unwrap();
        assert!(verify(&obj, "/nix/store", &keys));

        obj.nar_hash = "sha256:1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52".into();
        assert!(!verify(&obj, "/nix/store", &keys));

        obj.sigs.clear();
        assert!(!verify(&obj, "/nix/store", &keys));
        assert!(!verify(&obj, "/nix/store", &[]));
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
