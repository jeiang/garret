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
  user group). Optional `allowed-groups` config exists as
  defense-in-depth, default off.
- **GitHub issuer**: match the immutable `owner_id` claim (owner-wide —
  new repos work without config changes; never match renameable names).
  Optional ref constraints (e.g. default-branch only).

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

Stacked per-issuer authorizers (jwt-authorizer-style): per-issuer JWKS
cache with refresh-on-unknown-kid, RS256 pinned, audience required for
both issuers, ~60 s clock skew.

Operational requirements: register a garret API/audience in Pocket ID;
**pin Pocket ID ≥ the late-April-2026 release** (CVE-2026-43983 — the
refresh flow previously bypassed revocation/group checks).

## Local development

A dev-issuer config override points at a local static JWKS (test keys in
the test tree). There is deliberately **no auth-disable flag**.
