# Pocket ID + GitHub OIDC capabilities

Type: research
Status: resolved

## Question

What token flows can the two trusted issuers actually provide, and how should
a resource server validate them? For Pocket ID: supported grant types (device
flow? client credentials for headless machines?), token/refresh lifetimes,
audience configuration, JWKS endpoint behavior, and anything relevant to a
long-running daemon (the store watcher) holding credentials. For GitHub
Actions OIDC: token claim set (repo, ref, workflow, environment), audience
customization, token TTL, and best practice for validating these tokens at a
non-cloud resource server. Note any gotchas for multi-issuer JWT validation
in Rust (crate options, JWKS caching/rotation).

## Answer

Full findings: [research/pocket-id-oidc.md](../research/pocket-id-oidc.md)
(verified against Pocket ID source at commit 22e3909c). All three caller
types have a workable flow; six open questions are queued in the findings
for ticket 09.

- **Pocket ID** (fosite-based since v2.10.0): authorization code + enforced
  PKCE, refresh_token, device flow, and client_credentials for confidential
  clients — the latter is its de-facto service account (`sub =
  client-<uuid>`, no identity scopes). Passkey-centric means no headless
  primary auth: device flow is the sanctioned CLI bootstrap.
- Lifetimes: access 60 min; refresh 30 days with rotation granting a fresh
  window each refresh — an active daemon can hold access indefinitely.
  Per-client lifetimes only exist on main (post-v2.12.0). **Pin ≥
  late-April-2026 release** for CVE-2026-43983 (refresh flow bypassed
  revocation/disable/group checks).
- Audience via RFC 8707 "APIs" (v2.10.0): register a garret API audience
  and require it. JWKS at `/.well-known/jwks.json`, RS256 RFC 9068 JWTs.
- **GitHub Actions OIDC**: rich claims, custom `aud` per request, but hard
  **5-minute TTL** — a push must re-mint per request or exchange for a
  garret session token; authorize on `repository_id`/`owner_id` (immutable
  sub format since 2026-07-15).
- **Rust**: `jwt-authorizer` (stacked per-issuer authorizers, built-in JWKS
  refresh) or `jsonwebtoken` + hand-rolled JWKS cache; `openidconnect` for
  the client/daemon side. Gotchas: per-issuer key pools, alg pinning, aud
  string-or-array, refresh-on-unknown-kid.
