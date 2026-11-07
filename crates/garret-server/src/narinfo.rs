//! Narinfo rendering and ed25519 signing. Signatures are computed on write
//! (spec 02-database: `sigs` is stored); the Puller does no crypto.

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use garret_common::wire;
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
/// checks the signature, so only input-addressed paths fail.
pub fn normalize_hash(hash: &str) -> Result<String> {
    const SHA256_BYTES: usize = 32;
    let Some(digest) = hash
        .strip_prefix("sha256:")
        .or_else(|| hash.strip_prefix("sha256-"))
    else {
        bail!("unsupported hash format: {hash}")
    };
    if digest.len() == garret_common::nix_base32_len(SHA256_BYTES)
        && garret_common::is_nix_base32(digest)
    {
        return Ok(format!("sha256:{digest}"));
    }
    let bytes = B64
        .decode(digest)
        .with_context(|| format!("hash is neither nix-base32 nor base64: {digest}"))?;
    if bytes.len() != SHA256_BYTES {
        bail!("expected a 32-byte sha256, got {} bytes", bytes.len());
    }
    Ok(garret_common::sha256_nix32(&bytes))
}

/// Nix's signed fingerprint. References are full store paths, sorted — nix
/// holds them in a sorted set, so any other order verifies as a bad signature.
fn fingerprint(obj: &Object, store_dir: &str) -> String {
    let mut refs: Vec<String> = obj
        .references
        .iter()
        .map(|r| format!("{store_dir}/{r}"))
        .collect();
    refs.sort_unstable();
    let refs = refs.join(",");
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

/// Appends `n` in decimal to `out`, without `write!`'s formatting machinery:
/// two sizes per narinfo, on the hot path.
fn push_u64(out: &mut String, mut n: u64) {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    // ASCII digits by construction.
    out.push_str(std::str::from_utf8(&digits[start..]).expect("ascii digits"));
}

/// Renders the narinfo body served by the Puller.
pub fn render(obj: &Object) -> String {
    let mut out = String::with_capacity(
        256 + obj.references.iter().map(|r| r.len() + 1).sum::<usize>()
            + obj.sigs.iter().map(|s| s.len() + 6).sum::<usize>(),
    );
    // Writing to a String cannot fail.
    let _ = writeln!(out, "StorePath: {}", obj.store_path);
    // The URL is `nar/<hash>.nar.zst`, written straight into the buffer: this
    // is the hot path, and a temporary String here is one allocation and copy
    // per narinfo.
    out.push_str("URL: nar/");
    out.push_str(&obj.store_path_hash);
    out.push_str(wire::NAR_SUFFIX);
    out.push('\n');
    out.push_str("Compression: zstd\n");
    let _ = writeln!(out, "FileHash: {}", obj.file_hash);
    // Sizes are non-negative: the Pusher checks a claimed NarSize against the
    // NAR it received, and FileSize is the blob it wrote.
    out.push_str("FileSize: ");
    push_u64(&mut out, obj.file_size as u64);
    out.push('\n');
    let _ = writeln!(out, "NarHash: {}", obj.nar_hash);
    out.push_str("NarSize: ");
    push_u64(&mut out, obj.nar_size as u64);
    out.push('\n');
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
            nar_hash: "sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73".into(),
            nar_size: 4096,
            file_hash: "sha256:1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52".into(),
            file_size: 1024,
            ..crate::testing::object("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .named("hello-1.0")
                // deliberately unsorted: sign() must not depend on caller order
                .referencing(&[
                    "cccccccccccccccccccccccccccccccc-b",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-a",
                ])
        }
    }

    /// What nix verifies a signature of `object()` against: full store paths,
    /// references sorted.
    const FINGERPRINT: &str = "1;/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello-1.0;\
        sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73;4096;\
        /nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-a,\
        /nix/store/cccccccccccccccccccccccccccccccc-b";

    #[test]
    fn signature_verifies_over_the_nix_fingerprint() {
        let file = test_key();
        let sigs = sign(&object(), "/nix/store", std::slice::from_ref(&file)).unwrap();
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
        assert!(public.verify(FINGERPRINT.as_bytes(), &sig).is_ok());
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
    fn decimal_sizes_match_the_formatting_machinery() {
        for n in [0u64, 1, 9, 10, 99, 1024, 4_000_000_000, u64::MAX] {
            let mut out = String::new();
            push_u64(&mut out, n);
            assert_eq!(out, n.to_string(), "{n}");
        }
    }

    #[test]
    fn narinfo_renders_the_fields_nix_reads() {
        let mut obj = object();
        obj.deriver = Some("dddddddddddddddddddddddddddddddd-hello-1.0.drv".into());
        obj.sigs = sign(&obj, "/nix/store", &[test_key()]).unwrap();
        assert_eq!(
            render(&obj),
            format!(
                "StorePath: /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello-1.0\n\
                 URL: nar/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nar.zst\n\
                 Compression: zstd\n\
                 FileHash: sha256:1q6zw3dd7l2vnwk06rlnzwl8g1582sbagj6hccngspc7yvhrip52\n\
                 FileSize: 1024\n\
                 NarHash: sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73\n\
                 NarSize: 4096\n\
                 References: cccccccccccccccccccccccccccccccc-b bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-a\n\
                 Deriver: dddddddddddddddddddddddddddddddd-hello-1.0.drv\n\
                 Sig: {}\n",
                obj.sigs[0]
            )
        );
    }
}
