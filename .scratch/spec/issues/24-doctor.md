# 24 — `garret doctor`: one-command client/deployment diagnosis

Status: resolved (implemented 2026-08; spec in
[06-client.md](../../../docs/spec/06-client.md)). Evidence:
[survey](../research/similar-projects-survey.md).

## Problem

When a push or substitution misbehaves, diagnosis today is manual: is the
config present, is the token expired, is the Pusher reachable, is the
signing key the one nix.conf trusts, is a given path actually in the
cache? Each is one curl — but only if you remember them all.

## Evidence

- cachix v1.10 `cachix doctor`: validates config, tokens, daemon
  liveness, cache connectivity/auth, signing keys, and path-exists.
- sccache's startup `check()` probes the backend and *names* the failure
  (rate-limited, read-only, credentials) instead of 500ing later.

## Proposed shape

`garret doctor [path]` in the client, reusing what already exists:
discovery fetch (server reachable, config drift vs `garret login`), token
acquisition + `whoami` (auth), puller `/nix-cache-info` + a narinfo probe
(pull path), signing-key match against configured `public_keys`, and with
a path argument a negotiation round asking "is this cached". Each check
prints pass/fail with the failing layer named.

## Score

Speed **low** · Ops **high** · UX **high** (kills the recurring support
loop with one command).
