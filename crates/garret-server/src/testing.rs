//! Test-only fixtures (feature `test-support`): an RSA key generated once
//! per test process, its JWKS, a token signer, and an issuer trusting it, so
//! router tests can send a verified token (no private key is committed);
//! plus Object rows, S3 stand-ins, a GC config, and self-removing scratch
//! directories shared by every crate's tests.

use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeyPair, KeySize};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};

use crate::config::{GcConfig, IssuerConfig, S3Config};
use crate::db::Object;
use crate::storage::Storage;

/// The `kid` of the fixture key.
pub const KID: &str = "test-key";

/// The issuer URL of [`dev_issuer`].
pub const DEV_ISSUER: &str = "https://issuer.example";

/// An issuer at [`DEV_ISSUER`] for audience `garret` with no policy gates,
/// reading its keys from `jwks_url` (see [`jwks_file`]).
pub fn dev_issuer(jwks_url: &str) -> IssuerConfig {
    IssuerConfig {
        issuer: DEV_ISSUER.into(),
        audience: "garret".into(),
        extra_audiences: vec![],
        client_id: None,
        jwks_url: Some(jwks_url.into()),
        github_owner_id: None,
        ref_patterns: vec![],
        ref_protected: None,
        repository_ids: vec![],
        event_names: vec![],
        job_workflow_refs: vec![],
        allowed_groups: vec![],
    }
}

/// A JWKS file publishing the fixture key, removed on drop.
pub struct JwksFile(PathBuf);

impl JwksFile {
    /// The file's path, usable as a `jwks_url`.
    pub fn path(&self) -> &str {
        self.0.to_str().expect("temp path is UTF-8")
    }
}

impl Drop for JwksFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Writes the fixture key's JWKS to a fresh temp file.
pub fn jwks_file() -> JwksFile {
    let path = unique_temp_path("jwks");
    std::fs::write(&path, jwks()).expect("writing JWKS file");
    JwksFile(path)
}

/// A temp path no other call in any process has used: the pid tells
/// concurrent test binaries apart, the counter calls within one.
fn unique_temp_path(kind: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "garret-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// An empty directory, removed with its contents on drop, so a failing
/// test leaves nothing behind.
pub struct ScratchDir(PathBuf);

impl Deref for ScratchDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Creates a fresh [`ScratchDir`].
pub fn scratch_dir() -> ScratchDir {
    let path = unique_temp_path("scratch");
    // A dead process with the same pid may have left one behind.
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("creating scratch dir");
    ScratchDir(path)
}

/// An Object `<hash>-thing` holding a 10-byte NAR in a 5-byte blob, with no
/// references, signatures or pusher. Vary it with [`Object::named`],
/// [`Object::referencing`], [`Object::sized`], or struct update.
pub fn object(hash: &str) -> Object {
    Object {
        store_path_hash: hash.into(),
        store_path: format!("/nix/store/{hash}-thing"),
        name: "thing".into(),
        nar_hash: "sha256:x".into(),
        nar_size: 10,
        file_hash: "sha256:y".into(),
        file_size: 5,
        deriver: None,
        ca: None,
        references: vec![],
        sigs: vec![],
        pushed_by: None,
    }
}

impl Object {
    /// Renames the fixture, store path included.
    pub fn named(mut self, name: &str) -> Self {
        self.store_path = format!("/nix/store/{}-{name}", self.store_path_hash);
        self.name = name.into();
        self
    }

    /// Sets the reference basenames (`<hash>-<name>`).
    pub fn referencing(mut self, references: &[&str]) -> Self {
        self.references = references.iter().map(|r| (*r).to_owned()).collect();
        self
    }

    /// Sets both the NAR and the blob size.
    pub fn sized(mut self, bytes: i64) -> Self {
        self.nar_size = bytes;
        self.file_size = bytes;
        self
    }
}

/// Quota `quota_bytes` with the default watermarks, interval and orphan
/// grace.
pub fn gc_config(quota_bytes: u64) -> GcConfig {
    toml::from_str(&format!("quota_bytes = {quota_bytes}")).expect("gc config")
}

/// What S3 answers a `DeleteObjects` that deleted every key it named.
pub const S3_DELETED: &str = r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#;

fn s3_config(endpoint: &str) -> S3Config {
    S3Config {
        bucket: "test".into(),
        endpoint_url: Some(endpoint.into()),
        region: Some("us-east-1".into()),
        path_style: true,
        access_key_id: Some("x".into()),
        secret_access_key: Some("x".into()),
        operation_timeout_secs: 5,
        operation_attempt_timeout_secs: None,
        sign_payload: false,
    }
}

/// A `Storage` for bucket `test` at the S3 stand-in `endpoint`.
pub async fn storage_at(endpoint: &str) -> Storage {
    Storage::new(&s3_config(endpoint)).await.expect("storage")
}

/// A `Storage` pointed at an address nothing listens on. `Storage::new` only
/// builds a client, so this never touches a real S3 endpoint; a storage call
/// fails with connection refused within a second.
pub async fn unreachable_storage() -> Storage {
    Storage::new(&S3Config {
        operation_timeout_secs: 1,
        ..s3_config("http://127.0.0.1:1")
    })
    .await
    .expect("storage")
}

/// Serves `app` as an S3 stand-in on a loopback port and returns a
/// `Storage` for it.
pub async fn fake_s3(app: axum::Router) -> Storage {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding fake S3");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    storage_at(&endpoint).await
}

/// The fixture key as PKCS#1 `RSAPrivateKey` DER, plus its public `n` and `e`.
struct Fixture {
    pkcs1: Vec<u8>,
    n: Vec<u8>,
    e: Vec<u8>,
}

fn fixture() -> &'static Fixture {
    static KEY: OnceLock<Fixture> = OnceLock::new();
    KEY.get_or_init(|| {
        let pair = KeyPair::generate(KeySize::Rsa2048).expect("RSA keygen");
        let pkcs8 = AsDer::<aws_lc_rs::encoding::Pkcs8V1Der>::as_der(&pair).expect("PKCS#8");
        // PrivateKeyInfo ::= SEQUENCE { version, algorithm, privateKey OCTET STRING }
        let (_, info, _) = tlv(pkcs8.as_ref());
        let (_, _, rest) = tlv(info);
        let (_, _, rest) = tlv(rest);
        let (_, pkcs1, _) = tlv(rest);
        // RSAPrivateKey ::= SEQUENCE { version, modulus, publicExponent, ... }
        let (_, body, _) = tlv(pkcs1);
        let (_, _, rest) = tlv(body);
        let (_, n, rest) = tlv(rest);
        let (_, e, _) = tlv(rest);
        let unsigned = |i: &[u8]| i.strip_prefix(&[0]).unwrap_or(i).to_vec();
        Fixture {
            pkcs1: pkcs1.to_vec(),
            n: unsigned(n),
            e: unsigned(e),
        }
    })
}

/// Splits one DER TLV off `b`: (tag, contents, rest).
fn tlv(b: &[u8]) -> (u8, &[u8], &[u8]) {
    let (len, hdr) = match b[1] {
        l if l < 0x80 => (l as usize, 2),
        l => {
            let n = (l & 0x7f) as usize;
            (
                b[2..2 + n].iter().fold(0, |a, &x| a << 8 | x as usize),
                2 + n,
            )
        }
    };
    (b[0], &b[hdr..hdr + len], &b[hdr + len..])
}

/// The JWKS an issuer would publish for the fixture key.
fn jwks() -> String {
    let f = fixture();
    format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"{KID}","use":"sig","alg":"RS256","n":"{}","e":"{}"}}]}}"#,
        URL_SAFE_NO_PAD.encode(&f.n),
        URL_SAFE_NO_PAD.encode(&f.e),
    )
}

/// An RS256 token signed by the fixture key carrying `claims`, a JSON object.
pub fn sign(claims: &str) -> String {
    let claims: serde_json::Value = serde_json::from_str(claims).expect("claims JSON");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KID.into());
    let key = EncodingKey::from_rsa_der(&fixture().pkcs1);
    jsonwebtoken::encode(&header, &claims, &key).expect("signing")
}
