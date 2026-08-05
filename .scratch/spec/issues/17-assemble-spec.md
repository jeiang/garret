# Assemble the design spec

Type: task
Status: open
Blocked by: 05, 06, 07, 08, 09, 10, 11, 13, 14, 15, 16

## Question

Write the destination artifact: the garret design spec in this repo,
synthesized from every resolved ticket. Includes: architecture overview
(Pusher/Puller split, topology), the push protocol spec, DB schema, storage
layout, auth design, GC design, client/watcher design, browse API, metrics
catalog, benchmark plan, repo/workspace crate layout (decide here), NixOS
module option surface (decide here), admin CLI command surface (decide
here), ADRs for the hard-to-reverse calls (SQLite + same-host topology,
multi-issuer OIDC, the chunking outcome, HTTP framework), and CONTEXT.md
updates for any terms that crystallized late.
