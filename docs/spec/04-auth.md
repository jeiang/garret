# Authentication & Authorization

Sources: [ticket 09](../../.scratch/spec/issues/09-auth-flows.md),
[ticket 04 research](../../.scratch/spec/research/pocket-id-oidc.md),
[ADR-0003](../adr/0003-multi-issuer-oidc.md).

The Pusher validates bearer JWTs from two issuers directly via their
JWKS. There is no token-exchange service and no garret-issued token.

## Caller flows

| Caller | Flow |
|---|---|
| Human at a CLI | OIDC **device flow** against Pocket ID (passkey approval in browser). Refresh token (30-day rolling window, rotating) stored in XDG config, mode 0600. |
| Store watcher daemon | **client_credentials** with a per-machine confidential client in Pocket ID (`sub = client-<uuid>`). Secret in a root-owned file wired by the NixOS module. |
| GitHub Actions | **Re-mint per request**: the client fetches a fresh runner OIDC token whenever the cached one is >4 min old (the 5-minute TTL never bites; validation is at request start, so long streaming PUTs are unaffected). |

## Authorization policy

- **Pocket ID issuer**: valid JWT + garret's RFC 8707 audience. Access
  control lives in Pocket ID (restrict the garret client to the right
  user group there). Optional `allowed-groups` config exists as
  defense-in-depth, default off.
- **GitHub issuer**: match the immutable `repository_owner_id` claim
  (owner-wide — new repos work without config changes; never match
  renameable names). Optional `ref_patterns` constraints limit the triggering
  ref. An optional `ref_protected` boolean requires the token claim to be
  present and equal; setting it to `true` alongside
  `ref_patterns = ["refs/heads/main"]` restricts pushes to protected main.

## Surface summary

- Pusher: every endpoint requires OIDC (either issuer) **except
  `GET /api/v1/discovery`**, which is anonymous.
- Puller: narinfo/NAR anonymous; **browse routes require Pocket ID**.

`/api/v1/discovery` returns the Puller URL, the signing keys' public halves,
and the OIDC issuer, audience and `client_id` — everything `garret login` needs
to write a config, and nothing that is secret: a public key is public by
definition, and OIDC client metadata already travels in the clear in every
device-flow request. It reveals no cache contents, no subjects and no
credentials. It is anonymous by *placement* — registered after the
`require_oidc` layer, which wraps only the routes above it — so there is no
auth-bypass branch to get wrong, and the e2e asserts the anonymous 200 so a
router reordering cannot silently re-authenticate it. See
[ADR-0006](../adr/0006-server-served-client-discovery.md).

## Validation mechanics

Stacked per-issuer authorizers (jwt-authorizer-style): RS256 pinned,
audience required for both issuers, `exp` required and `nbf` checked when
present, ~60 s clock skew.

Each issuer's JWKS is cached in memory and refetched when a token names an
unknown kid (rotation) or the cached set is over an hour old, so a key the
issuer has removed stops being trusted without a restart. Fetches are
bounded so that neither an unknown-kid flood (anyone can send one) nor a
slow or down issuer turns into load on it or stalls requests:

- one fetch per issuer at a time; concurrent callers wait for its result;
- at most one fetch attempt per issuer every 10 s, failed or not;
- 5 s connect and 10 s overall timeout per fetch;
- a failed refresh of an expired set keeps the cached keys, since
  refusing every token while the issuer blips is worse and a removal
  needs the issuer up to be published anyway. An unknown kid with a
  failed fetch is refused.

Operational requirements: register a garret API/audience in Pocket ID;
**pin Pocket ID ≥ the late-April-2026 release** (CVE-2026-43983 — the
refresh flow previously bypassed revocation/group checks).

## Local development

A dev-issuer config override points at a local static JWKS (test keys in
the test tree). There is deliberately **no auth-disable flag**.
