# Pocket ID + GitHub Actions OIDC capabilities

Research for issue 04 (`.scratch/spec/issues/04-pocket-id-oidc.md`).
Date: 2026-08-05. Pocket ID facts verified against source at commit
`22e3909c` (main, 2026-08-05) and released docs; latest release at time of
writing is v2.12.0.

## Summary: what each caller type can actually do

| Caller | Flow | Token shape | Lifetime story |
|---|---|---|---|
| Interactive human (CLI on a machine with a browser) | Authorization code + PKCE (PKCE enforced for public clients) | RFC 9068 JWT access token, `aud` = client ID or a configured API audience via RFC 8707 `resource` | Access token default 60 min; refresh token default 30 days, rotated on every refresh with a fresh 30-day expiry |
| Headless / SSH machine (human present once) | **Device authorization flow** (supported since PR #270, Apr 2025). CLI polls `/api/oidc/token`; human opens `<issuer>/device` on any browser and approves with a passkey | Same as above, refresh token included | Device+user code valid 15 min, 5 s poll interval; afterwards same refresh-token treadmill |
| Unattended daemon (store watcher) | Two viable options: (a) **client_credentials** as a confidential OIDC client — Pocket ID's de-facto service account; (b) bootstrap once via device flow and hold a rotating refresh token | (a) `sub = "client-<client-uuid>"`, identity scopes stripped, `aud` = client ID or API audience; (b) normal user token | (a) mint a fresh access token whenever needed, no refresh token for this grant; (b) refresh token rotates with a full new 30-day window on each use → lives indefinitely **as long as it is used at least every 30 days** |
| CI job (GitHub Actions) | GitHub Actions OIDC ID token, requested in-job with `id-token: write` | JWT from `https://token.actions.githubusercontent.com`, rich claim set, customizable `aud` | **exp − iat = 5 minutes, not configurable.** A push job must either re-mint per request or exchange the token for something longer-lived at push start |

Headline conclusions for the auth design:

- Pocket ID has no "API key for third-party resource servers" concept. Its API
  keys authenticate only Pocket ID's own admin REST API. The service-account
  construct is a **confidential OIDC client using the client_credentials
  grant** (available for non-public clients).
- A refresh token **can** live indefinitely: rotation issues a fresh
  refresh token with a full new lifetime each time (this is also called out in
  security advisories). Default 30 days between refreshes; per-client
  configurable up to 365 days on current main (see below).
- Passkey-centricity means no headless *primary* authentication with user
  identity — there is no password grant. Device flow is the designed answer:
  the machine never touches the passkey; the human approves in a browser.
- Audience is real and configurable: since v2.10.0 Pocket ID has "APIs" (RFC
  8707 resource indicators) — an admin defines an API with an audience URI and
  permission scopes, clients request `resource=<audience-uri>`, and the token's
  `aud` becomes that URI with granted scopes limited to the client's allowed
  permission keys. Without `resource`, `aud` = the requesting client's UUID.
  Garret should define an API resource (e.g. `https://cache.example.com`) and
  validate `aud` against it for Pocket ID tokens, mirroring the custom `aud`
  it requires from GitHub tokens.

---

## Pocket ID (v2.10+ / main, Aug 2026)

Since v2.10.0 the OAuth 2.0 core is a fork of ory/fosite
(`backend/internal/oidc/provider.go`), which sharply improved spec coverage.

### Grant types

From `backend/internal/oidc/client.go` `GetGrantTypes()`:

- `authorization_code` — always. Response type `code` only; response modes
  query/fragment/form_post. PKCE **enforced for public clients**
  (`EnforcePKCEForPublicClients: true`; plain challenge method also enabled).
- `refresh_token` — always. `RefreshTokenScopes: []` in the fosite config
  means a refresh token is issued without requiring `offline_access`
  (the scope exists and is accepted; it isn't the gate).
- `urn:ietf:params:oauth:grant-type:device_code` — always. Implemented Apr
  2025 ([PR #270](https://github.com/pocket-id/pocket-id/pull/270), closing
  [issue #112](https://github.com/pocket-id/pocket-id/issues/112)).
  `device_authorization_endpoint` = `<issuer>/api/oidc/device/authorize`,
  verification page `<issuer>/device`, device/user code lifespan 15 min,
  poll interval 5 s.
- `client_credentials` — **confidential clients only** (appended only when
  `!c.IsPublic()`). Token handler (`token_handler.go`): synthetic subject
  `"client-" + clientID`, identity scopes (openid/profile/email/groups/
  offline_access) are stripped so machine tokens can't hit userinfo, RFC 8707
  `resource` resolved against the client's *client-subject* API grants. No
  refresh token for this grant (per-grant lifespan table in `client.go`).

Not supported: implicit, password, JWT-bearer *user* grant. Token endpoint auth
methods: `client_secret_basic`, `client_secret_post`, `none`; plus **federated
client credentials** (v1.3.0+): a client may authenticate to the token
endpoint with `client_assertion` (JWT from a third-party IdP — Kubernetes,
Azure, GitLab CI... and in principle GitHub Actions) instead of a stored
secret ([docs](https://pocket-id.org/docs/guides/oidc-client-authentication)).

### Token formats, lifetimes, configurability

- Access tokens are **JWTs (RFC 9068 strategy, `at+jwt`)** signed with the
  instance key — **RS256/RSA-2048 by default** (`jwt_service.go`). ID tokens
  same signer.
- JWKS: `GET <issuer>/.well-known/jwks.json`; discovery:
  `<issuer>/.well-known/openid-configuration`. Token endpoint:
  `<issuer>/api/oidc/token`. Introspection: `<issuer>/api/oidc/introspect`
  (client-authenticated) — an alternative to local JWT validation if garret
  ever wants real-time revocation checks.
- Lifetimes (source of truth: `backend/internal/model/oidc.go` and
  `provider.go`):
  - Access token: **per-client `AccessTokenDurationMinutes`, default 60**.
  - Refresh token: **per-client `RefreshTokenDurationMinutes`, default 43200
    (30 days)**; fosite fallback also 30 days.
  - Configurable range per client: **1 minute to 365 days**
    (`MinTokenDurationMinutes=1`, `MaxTokenDurationMinutes=525600`).
  - Caveat: the per-client duration columns come from migration
    `20260802120000_add_oidc_client_token_lifetimes` dated **2026-08-02, i.e.
    merged to main after v2.12.0** — on released versions up to v2.12.0
    lifetimes are fixed (1 h access / 30 d refresh; cf.
    [issue #792](https://github.com/pocket-id/pocket-id/issues/792), closed
    "not planned" before this landed). Expect it in the next release; verify
    before depending on it.
  - Device/user codes 15 min; PAR context 90 s; ID token follows access-token
    handling.
- **Refresh rotation**: each refresh rotates the token and grants a fresh
  full-lifetime expiry — perpetual access for an active daemon
  ([GitLab advisory for CVE-2026-43983](https://advisories.gitlab.com/golang/github.com/pocket-id/pocket-id/backend/CVE-2026-43983/)
  describes the 30-day rotate-forever behavior). Refresh tokens are hashed at
  rest (PR #379) and deleted on end-session/RP-initiated logout (v2.8/2.9).

### Audience / resource configuration

- Default: `aud` = requesting client's UUID (`Client.GetAudience()`, and
  `resolveResource()` in `api_resource.go`: "a plain login token is audienced
  to the requesting client").
- v2.10.0 added "OAuth APIs with scoped permissions"
  ([PR #1542](https://github.com/pocket-id/pocket-id/pull/1542)): admin
  defines an **API** (name + audience URI + permission keys), grants specific
  permissions to clients per subject type (user-delegated vs client). A client
  passes RFC 8707 `resource=<audience-uri>` at authorize/device/token time;
  the issued token gets `aud=<audience-uri>` and only the allowed permission
  scopes. Resource must be a valid resource-indicator URI. Audience matching
  is exact. API permissions are re-checked on refresh (v2.10.0 fix).

### Passkey-centricity and headless implications

- No passwords, no password grant. Primary auth = WebAuthn passkey in a
  browser; alternatives are admin-issued one-time login codes, email login
  codes, and (v2.12.0) QR-code sign-in — all still browser-based.
- Therefore a CLI/daemon can never "log in" by itself as a user. The device
  flow is the supported pattern (human approves once with a passkey on any
  browser); after that the machine lives on the rotating refresh token.
- For a fully unattended service with no human bootstrap, use a confidential
  client + client_credentials. Note the token then has no user identity or
  groups — garret's authz must recognize `sub = client-<uuid>` subjects.

### Security notes / version pinning

- **CVE-2026-43983** (HIGH 7.3): before the fix (commit dated 2026-04-19),
  the refresh flow skipped re-checks of authorization revocation, disabled
  accounts, and group restrictions — indefinite access via rotation. Run a
  release from late April 2026 or newer.
- GHSA-hp74-gm6m-2qm5: re-authentication bypass via one-time access token
  login (fixed; another reason to stay current).
- v2.12.0 hardened device-code redemption (atomic) and user verification for
  login assertions.

## GitHub Actions OIDC

- Issuer: `https://token.actions.githubusercontent.com` (different on GHES).
  JWKS via the issuer's `/.well-known/openid-configuration` → `jwks_uri`.
  Keys rotate; fetch by `kid`.
- Claims ([docs](https://docs.github.com/en/actions/concepts/security/openid-connect),
  [claims reference](https://docs.github.com/enterprise-cloud@latest/actions/reference/security/oidc)):
  `sub` (customizable format), `repository`, `repository_owner`,
  `repository_id`, `repository_owner_id`, `repository_visibility`, `ref`,
  `ref_type`, `sha`, `head_ref`, `base_ref`, `workflow`, `workflow_ref`,
  `job_workflow_ref`, `environment`, `event_name`, `run_id`, `run_number`,
  `run_attempt`, `actor`, `actor_id`, `runner_environment`, plus
  `repo_property_*` custom repository properties.
  - **New in 2026: repositories created after 2026-07-15 use an immutable
    ID-based default `sub` format** (embeds owner/repo IDs). Best practice:
    authorize on `repository_id` + `repository_owner_id` (rename/recreate
    safe), not name strings or exact `sub` matching.
- Audience: default `aud` = `https://github.com/<owner>`; customizable per
  token request (`audience` parameter to the runner's ID-token endpoint /
  `core.getIDToken(aud)`). Garret should require a custom audience, e.g. the
  cache's canonical URL.
- **TTL: 5 minutes** (`exp - iat = 300 s`), fixed; GitHub has declined
  requests to lengthen it (actions/toolkit #2048, google-github-actions/auth
  #485). Implication for garret: a multi-GB NAR push can outlive the token.
  Options: (a) validate the bearer per-request and have the CI client re-mint
  a token per request (cheap — the runner endpoint mints on demand); (b)
  validate once at push-session start and issue a garret-scoped short-lived
  session token; (c) validate at connection establishment only. Decide in the
  auth-design ticket.
- Validation best practice at a self-hosted resource server: standard JWT
  validation — signature via cached JWKS (`kid` lookup), `iss` exact, `aud`
  exact (the custom value), `exp`/`iat` with small leeway — then
  **authorization against pinned claims** (`repository_id`, `ref`,
  `environment`, optionally `runner_environment == "github-hosted"`). Treat
  `sub` as informational unless you pin its exact configured format.

## Rust implementation notes

Candidates for multi-issuer validation with JWKS fetch/cache/rotation:

- **`jsonwebtoken`** (Keats): the de-facto validator; ships a `jwk` module
  that parses JWKS into `DecodingKey`s. You own fetching/caching: cache keys
  by `kid` per issuer, refresh on unknown `kid` with a minimum refresh
  interval (rate-limit ~10 s), plus periodic background refresh. Simple and
  transparent — a good fit for exactly two issuers.
- **`jwt-authorizer`**: tower/axum layer over `jsonwebtoken` with OIDC
  discovery, built-in JWKS refresh (reloads on unknown `kid`, default 10 s
  min interval, configurable), and support for **stacking multiple
  authorizers → native multi-issuer**. Fastest path if garret's Pusher is
  axum/tonic.
- **`axum-jwt-auth`**, **`axum-jwks`**, **`jsonwebtoken-jwks-cache`** (cache
  honoring HTTP caching semantics): smaller alternatives in the same space.
- **`openidconnect`**: full RP library — wrong tool for resource-server
  validation, but the right tool for the **CLI/daemon side** (it implements
  the device flow and token refresh as a client).

Gotchas:

1. **Dispatch on unverified `iss`, trust only after verification.** Decode the
   header/claims without verification to pick the issuer config, but bind the
   JWKS pool per issuer; never search one merged key pool (kid collision
   across issuers = cross-issuer confusion).
2. **Pin algorithms per issuer** (both issuers here are RS256 in practice;
   Pocket ID generates RSA-2048/RS256 by default). Never accept the token's
   own `alg` from an open set.
3. `aud` may be string or array — `jsonwebtoken`'s `Validation::set_audience`
   handles both; hand-rolled checks often miss the array case.
4. Pocket ID access tokens follow RFC 9068 (`typ: at+jwt`) and fosite emits
   scopes as both `scp` (array) and `scope` (space-delimited string)
   (`JWTScopeFieldBoth`). Don't assume one form; verify `typ` empirically
   before enforcing it strictly.
5. JWKS rotation: refresh-on-unknown-`kid` with rate limiting is the pattern
   that survives Pocket ID key regeneration and GitHub's rolling keys; also
   keep serving cached keys on refresh failure (don't hard-fail all auth on a
   transient JWKS fetch error).
6. Clock skew: allow small leeway (jsonwebtoken default 60 s) — significant
   for GitHub's 5-minute tokens.
7. GitHub `sub` formats changed mid-2026; prefer claim-by-claim authorization
   over `sub` string equality.

## Open questions for the auth-design ticket

1. Store watcher identity: confidential client + client_credentials
   (`sub=client-<uuid>`, no groups — authz must model client subjects), or a
   device-flow-bootstrapped user refresh token (rotating storage on disk,
   dies if unused >30 days or on Pocket ID restore-from-backup)?
2. Minimum Pocket ID version to require: ≥ v2.10.0 for API
   audiences/RFC 8707 and fosite core; per-client token lifetimes are
   main-only as of 2026-08-05 — confirm which release ships migration
   `20260802120000` before relying on non-default lifetimes.
3. GitHub 5-minute TTL vs long uploads: per-request re-mint, validate-at-start,
   or a garret-issued session token exchanged at push start?
4. Define one garret API audience in Pocket ID (e.g. cache base URL) and
   require it — and require the same value as custom `aud` from GitHub
   tokens? (Symmetric validation simplifies the multi-issuer code path.)
5. Use Pocket ID's permission keys (API scopes) to model push vs pull rights,
   or keep authz entirely in garret config keyed on `sub`?
6. Should garret also call Pocket ID's introspection endpoint for high-value
   operations (real-time revocation), accepting the availability coupling?

## Sources

- Pocket ID source (verified at commit `22e3909c`, 2026-08-05):
  `backend/internal/oidc/{client,provider,token_handler,api_resource}.go`,
  `backend/internal/model/oidc.go`,
  `backend/internal/controller/well_known_controller.go`,
  `backend/internal/service/jwt_service.go`, `CHANGELOG.md`,
  migration `20260802120000_add_oidc_client_token_lifetimes`
- https://pocket-id.org/docs/guides/oidc-client-authentication
- https://pocket-id.org/docs/configuration/environment-variables
- https://github.com/pocket-id/pocket-id/pull/270 (device flow),
  https://github.com/pocket-id/pocket-id/issues/112,
  https://github.com/pocket-id/pocket-id/issues/792 (lifetimes history),
  https://github.com/pocket-id/pocket-id/pull/1542 (OAuth APIs)
- https://advisories.gitlab.com/golang/github.com/pocket-id/pocket-id/backend/CVE-2026-43983/
- https://docs.github.com/en/actions/concepts/security/openid-connect and
  https://docs.github.com/enterprise-cloud@latest/actions/reference/security/oidc
- https://github.com/actions/toolkit/issues/2048,
  https://github.com/google-github-actions/auth/issues/485 (5-minute TTL)
- https://docs.rs/jwt-authorizer, https://docs.rs/axum-jwt-auth,
  https://crates.io/crates/jsonwebtoken-jwks-cache
