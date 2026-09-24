# Authentication & Authorization

Sources: [ticket 09](../../.scratch/spec/issues/09-auth-flows.md),
[ticket 04 research](../../.scratch/spec/research/pocket-id-oidc.md),
[ADR-0003](../adr/0003-multi-issuer-oidc.md).

The Pusher validates bearer JWTs from two issuers directly via their
JWKS. There is no token-exchange service and no garret-issued token.

## Caller flows

| Caller | Flow |
|---|---|
| Human at a CLI | OIDC **device flow** against Pocket ID (passkey approval in browser). Refresh token (30-day rolling window, rotating) stored in XDG config: the directory created mode 0700, the file mode 0600 from creation — written to a new sibling and renamed over the old one, so no reader ever sees it unrestricted or half-written, and a symlink at the path is replaced, not followed. |
| Store watcher daemon | **client_credentials** with a per-machine confidential client in Pocket ID (`sub = client-<uuid>`). Secret in a root-owned file wired by the NixOS module. |
| GitHub Actions | **Re-mint per request**: the client fetches a fresh runner OIDC token whenever the cached one is >4 min old (the 5-minute TTL never bites; validation is at request start, so long streaming PUTs are unaffected). |

## Authorization policy

- **Pocket ID issuer**: valid JWT + garret's RFC 8707 audience. Access
  control lives in Pocket ID (restrict the garret client to the right
  user group there). Optional `allowed-groups` config exists as
  defense-in-depth, default off.
- **GitHub issuer**: match the immutable `repository_owner_id` claim
  (never match renameable names). Owner-wide on its own: every workflow in
  every repository of that owner can mint a push token, so narrow it with
  the optional constraints below. Each is off when unset; once set, a token
  missing the claim is refused (fail closed).
  - `ref_patterns`: allowed `ref`s (trailing-`*` globs).
  - `ref_protected`: the claim must be present and equal; `true` requires a
    protected ref.
  - `repository_ids`: allowed immutable numeric `repository_id`s.
  - `event_names`: allowed `event_name`s. `pull_request_target`,
    `workflow_run` and `issue_comment` run with `ref` = the default branch
    even when a stranger's pull request set them off, so `ref_patterns`
    alone admits them.
  - `job_workflow_refs`: allowed `job_workflow_ref`s
    (`owner/repo/.github/workflows/file.yml@ref`, trailing-`*` globs). This
    names the file the job actually runs: for a reusable workflow it is the
    called file, so a third-party reusable workflow called from an allowed
    repository — which inherits the caller's other claims — is refused.
    It spells repositories by name, so a rename can only refuse tokens
    (never grant one, given the id checks): update it alongside the rename.

  Recommended policy: pin the repositories and the workflow that pushes, on
  protected main, for push and manual runs only:

  ```toml
  github_owner_id = "31970261"
  ref_patterns = ["refs/heads/main"]
  ref_protected = true
  repository_ids = ["553667153", "1324491067"] # jeiang/.dotfiles (cornn-flaek), jeiang/garret
  event_names = ["push", "workflow_dispatch"]
  job_workflow_refs = [
    "jeiang/.dotfiles/.github/workflows/ci.yml@refs/heads/main",
    "jeiang/garret/.github/workflows/ci.yml@refs/heads/main",
  ]
  ```

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

- at most one fetch attempt per issuer at a time. It runs as its own
  task, so a caller hanging up can't abort it, and every caller that needs
  it shares its result;
- the next attempt starts no sooner than 10 s after the last one ended,
  failed or not;
- 5 s connect and 10 s overall timeout per HTTP request (an attempt
  without a configured `jwks_url` makes two: discovery, then the JWKS);
- an unknown kid waits for the attempt and is refused if it fails or the
  kid is still unknown. A known kid in an expired set is validated against
  the cached keys while the refetch runs in the background, and a failed
  refetch keeps them: refusing every token while the issuer blips is
  worse, and a removal needs the issuer up to be published anyway.

Operational requirements: register a garret API/audience in Pocket ID;
**pin Pocket ID ≥ the late-April-2026 release** (CVE-2026-43983 — the
refresh flow previously bypassed revocation/group checks).

## Local development

A dev-issuer config override points at a local static JWKS (test keys in
the test tree). There is deliberately **no auth-disable flag**.
