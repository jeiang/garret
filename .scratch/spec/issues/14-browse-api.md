# Listing and browse API

Type: grilling
Status: open
Blocked by: 07

## Question

Design the cache listing/browse surface: endpoints for listing objects,
searching by name, and the dependency-tree view (semantics carried from the
attic-era fork — see `/Users/aidanp/Projects/attic/CONTEXT.md`); pagination
and response shapes; which service serves it (Puller? Pusher? both?) and
whether it is public or OIDC-gated given the Puller is public for pulls
(cross-ref ticket 09); and what indices the schema needs to keep these
queries cheap (cross-ref ticket 07).
