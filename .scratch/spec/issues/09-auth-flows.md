# Auth flows and claims policy

Type: grilling
Status: open
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
