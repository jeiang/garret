# The Pusher serves client configuration anonymously

The Pusher exposes an unauthenticated `GET /api/v1/discovery` returning the
Puller URL, the signing keys' public halves, and the OIDC issuer, audience and
client id, so that `garret login <pusher-url>` writes a complete client config
from a single argument the human already had to know. The alternative was
prompting for five values and having `garret use` take a pasted public key —
more client code, and a paste step that gets pasted wrong.

It lives on the Pusher, not the Puller, because the Pusher already holds
everything the document contains: the signing keys it signs every object with,
and the issuers it validates tokens against. Serving it from the Puller would
mean duplicating the audience and the public key into `PullerConfig`, where
they could silently drift from what the Pusher actually enforces. Only
`puller_endpoint` had to be added, since a reverse-proxied Pusher cannot infer
its sibling's public URL — and for the same reason the document deliberately
carries no `pusher_endpoint`: the client dialled it to ask.

The route is anonymous by *placement* — `Router::layer` wraps only the routes
registered before it, so `/api/v1/discovery` is registered after the
`require_oidc` layer and has no auth-bypass logic to get wrong. Nothing it
returns is secret: a public key is public by definition, and OIDC client
metadata travels in the clear in every device-flow request. The e2e asserts an
anonymous 200 precisely because a future reordering of that router would
silently re-authenticate it and break `login` for anyone not already logged in.

The `client_id` sits on `IssuerConfig` rather than in a top-level `[discovery]`
section, so it cannot name an issuer the Pusher does not trust, and so it
self-selects: only the human device-flow issuer has one, never the GitHub
Actions issuer, and discovery advertises the first issuer that sets it.

Consequence: client rollout now depends on a server deploy. A client newer than
its Pusher gets a 404, which the client turns into "this Pusher predates
`garret login <url>`" rather than a bare HTTP error. Revisit if garret ever
becomes multi-tenant, where one document per cache would no longer be static
and the anonymous route would start leaking which caches exist.
