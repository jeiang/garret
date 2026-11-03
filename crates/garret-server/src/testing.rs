//! Test-only signing fixtures (feature `test-support`): a throwaway RSA key,
//! its JWKS, and a token signer, so router tests can send a verified token.

use jsonwebtoken::{Algorithm, EncodingKey, Header};

/// The `kid` of the fixture key.
pub const KID: &str = "test-key";

const PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEArZrZcEq7vUnfTy04rK3Trc3WOqnEak7Ku2Q+D9Q1RGnOCojG
4EnzATGaEW+kHxnwrXXj9+irIplaRunlCgmaBHVgQ0QIEKJbY2YulakiwLGuy+kl
ZM5tYZvS7tM/4PlCeoBxz+WlBo7bAVbyUQems3Tp6zEe5Fq3eeRZEMFPSf3tD8AF
fQZ/0F/bIYyRfRICbp0D1c97gvUmLE9dUhw6rlSGijyjsjvIH7iT1dapOeYalIYx
XDMZsO/mXz8Ug3XrCEwiEAUngaFbfyDRrnv3yuhYrmK0qX25Y7Tv/kDVxn1xWzhS
azrhfdBcBcg4fD0Ilugk8cbrs4jk5P9qIgbyOwIDAQABAoIBAAwvBxAot66Xdcd8
v9c1dP9j8RECntSQSw2GAcUHMVOMZNMplRk+TjlbOT6O4PU90xkrqzJqSmYgXjq8
/Bsx8nSjXMWSndv/SP4SlBE99PeSN6CNpyB4AyrnF5F+t6M/f6bjkD3QqKu0lTIt
us4j4He7LDr165EaBeeMEe1E0YxM2xyF4ghbCsRqb67D4mW5gdjFNhvJW/PA6YsG
+FV2DbLzqrRzNl9/VQNS+6H8xKvlQDgSW+TlN+NrNLEwp1M7+rwqjKhoCN0yixED
6QeL23SbwViVM/WixC0c+5Xq7vqrWmAp7jKaj6RjSl1X37d/YCzwTN0Vdcz92ahO
98rtHqECgYEA2vQQxoUmph+nv+99zE/ecBF0Fpd11Cj3VfOS+UFn2Vdpy0Vk6O2J
/16YRoyd1mzLXqUjh1O5gPIvQgJ10ecnZf24cEF00Ba/hVyNKMFyZf0t8paQg0a7
bpgWe69Rlh52OfqefqK3lZreYrC4jUd8dSJMknWBEo+XeO5ADKiDPxUCgYEAyvqF
HvFr+CBR+WpIFQNyUMAQFe4y7fXypCSbQO2mTjNQgudS+gFY9KggpW2Fa9/boFF0
060BRAtTgyLDH/sf+XjWcImCDciU2mvptNHLagVuXvtPF/FRL/2FOcjMkBkPrpQL
bmPvNrL6kTLXaPWCq2Mm5+aYcdwVLq6sh+SNQA8CgYA6qAnadQ9Oj8pr0+f6lSb9
FN/psXfCd5sTLqagGdKNWiqgZx3KfOBsRlY7CC68o/azi0ACX/6hOcBJSclwiyF1
5EvF3vNBT+bM/mdelLxl8Daf95ANaySJUzsqtWQA+HYpwCc3u7W84XlULQ2z44PH
CYJD9jC23TryDbmg6oAGpQKBgGHji4uutnlFbQMyBG1N6jWTSq6o35rvQAN1PAKG
GewgxzU6RkQXnkTvSD8/oFaz8QFSB1uGVdPw+meFvkCQJY10f0ZqqnPnRhJIdL8l
ZkpNMDtSS4SqdpMEWtnRQwZDb9Krxw+H1ne0vS6FLGmL7WamPL8d8GsIzpZi2wX2
bUB1AoGAR/Xq5VRtnpmfpFzzTw1JCFoo2cSw/AWEuGKeVGU4+l31b6YcvduxkxVZ
cSukqJF+mzVoKan+KVQ5hJUNopxsGUwSmXhUxPEM0DXo97f5ah4dNhNr5cuuunt3
8lzbG7d/Yp9MZxHAAjmVCQyD/AF/dtePDWZMKMrgNy1usSJyjUA=
-----END RSA PRIVATE KEY-----
";

/// The JWKS an issuer would publish for the fixture key.
pub fn jwks() -> String {
    format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"{KID}","use":"sig","alg":"RS256","n":"rZrZcEq7vUnfTy04rK3Trc3WOqnEak7Ku2Q-D9Q1RGnOCojG4EnzATGaEW-kHxnwrXXj9-irIplaRunlCgmaBHVgQ0QIEKJbY2YulakiwLGuy-klZM5tYZvS7tM_4PlCeoBxz-WlBo7bAVbyUQems3Tp6zEe5Fq3eeRZEMFPSf3tD8AFfQZ_0F_bIYyRfRICbp0D1c97gvUmLE9dUhw6rlSGijyjsjvIH7iT1dapOeYalIYxXDMZsO_mXz8Ug3XrCEwiEAUngaFbfyDRrnv3yuhYrmK0qX25Y7Tv_kDVxn1xWzhSazrhfdBcBcg4fD0Ilugk8cbrs4jk5P9qIgbyOw","e":"AQAB"}}]}}"#
    )
}

/// An RS256 token signed by the fixture key carrying `claims`, a JSON object.
pub fn sign(claims: &str) -> String {
    let claims: serde_json::Value = serde_json::from_str(claims).expect("claims JSON");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KID.into());
    let key = EncodingKey::from_rsa_pem(PRIVATE_KEY.as_bytes()).expect("fixture key");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing")
}
