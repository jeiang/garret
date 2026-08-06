# Direct multi-issuer OIDC validation; no token issuance by garret

The Pusher validates JWTs from two issuers directly against their JWKS —
Pocket ID (humans via device flow; machines via per-machine
client_credentials) and GitHub Actions (CI, authorized by immutable
owner_id) — and garret itself never issues tokens. GitHub's hard 5-minute
token TTL is handled client-side by re-minting per request rather than by
a token-exchange endpoint, keeping the Pusher a pure validator with no
issuance surface, no session state, and no revocation story to maintain.
Authorization lives at the issuer where possible (Pocket ID group
membership), not in duplicated garret allowlists. There is deliberately
no auth-disable flag; local dev uses a dev-issuer JWKS override.
