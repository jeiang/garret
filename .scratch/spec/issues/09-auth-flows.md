# Auth flows and claims policy

Type: grilling
Status: resolved
Blocked by: 04

## Question

Given ticket 04's findings on Pocket ID and GitHub OIDC capabilities, design
the concrete auth story: audience and claims policy per issuer (which GitHub
repos/refs may push; which Pocket ID users/groups), how the garret CLI
acquires tokens interactively, how the long-running store watcher holds and
refreshes credentials unattended, token lifetime and clock-skew handling,
JWKS caching/rotation at the Pusher, what (if anything) on the Puller is
authed (it is public for pulls — but is browse/listing public too? cross-ref
ticket 14), and the local-dev/testing auth story.

## Answer

**Per-caller flows**

- *Interactive human (CLI)*: OIDC device flow against Pocket ID — CLI shows
  the code/URL, passkey approval happens in a browser. The refresh token
  (30-day rolling window with rotation) is stored in the user's XDG config
  dir, mode 0600; an active CLI never re-prompts.
- *Store watcher daemon*: client_credentials with a per-machine
  confidential client in Pocket ID (`sub = client-<uuid>` — Pocket ID's
  de-facto service account). Client id+secret live in a root-owned file
  referenced by the NixOS module; the daemon re-mints access tokens as
  needed, no refresh token involved.
- *GitHub Actions*: **re-mint per request** — the client detects the
  Actions environment and fetches a fresh OIDC token from the runner's
  token endpoint whenever the cached one is >4 min old (minting is local
  and cheap). Validation is at request start, so PUTs streaming past the
  5-minute mark are unaffected. The Pusher stays a pure validator: no
  token issuance, no sessions, no revocation surface.

**Authorization policy**

- *Pocket ID issuer*: valid JWT + garret's RFC 8707 audience — that's it.
  Access control lives in Pocket ID (restrict the garret client to your
  user group there); one control point, no allowlist drift. An optional
  `allowed-groups` config exists as later defense-in-depth, default off.
- *GitHub issuer*: **owner-wide** — match the immutable `owner_id` claim
  (never renameable names), with optional ref constraints (e.g.
  default-branch only) in config. New repos push without config changes.

**Browse** — pull stays anonymous; listing/search/dependency-tree
endpoints require a valid Pocket ID token (enumeration of what you build
stays private even though known hashes are fetchable). Which service
hosts browse is ticket 14's call.

**Validation mechanics** — multi-issuer validation (jwt-authorizer-style
stacked authorizers): per-issuer JWKS cache with refresh-on-unknown-kid,
RS256 pinned, audience required for both issuers, ~60 s clock skew.
Operational notes: register a garret API/audience in Pocket ID; pin
Pocket ID ≥ the late-April-2026 release (CVE-2026-43983).

**Local dev/testing** — a dev-issuer config override pointing at a local
static JWKS (test keys checked into the test tree). There is deliberately
no auth-disable flag.
