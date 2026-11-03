//! Test-only signing fixtures (feature `test-support`): an RSA key generated
//! once per test process, its JWKS, and a token signer, so router tests can
//! send a verified token. No private key is committed.

use std::sync::OnceLock;

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeyPair, KeySize};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};

/// The `kid` of the fixture key.
pub const KID: &str = "test-key";

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
pub fn jwks() -> String {
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
