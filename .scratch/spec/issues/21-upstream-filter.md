# 21 — Skip pushing paths an upstream cache already serves

Status: proposed (2026-08 review). Evidence:
[survey](../research/similar-projects-survey.md).

## Problem

`garret push` uploads every path the negotiation reports missing —
including toolchains and stdenv paths cache.nixos.org already serves
signed. That wastes push bandwidth and fills the quota with paths eviction
will happily reclaim but never needed to hold.

## Evidence

- attic: `--upstream-cache-key-name` (default `cache.nixos.org-1`) skips
  paths already carrying an upstream signature — a pure client-side filter
  on `nix path-info` sig data.
- cachix "configurable upstream caches" (Dec 2025) ships the same idea as
  a headline feature: paths present upstream are never uploaded.

## Proposed shape

Client-side only. During closure assembly, drop paths whose narinfo `Sig:`
carries a configured upstream key name (config: `upstream_keys = [...]`,
default cache.nixos.org's; `--no-upstream-filter` to bypass). No server
change; negotiation batch shrinks for free.

## Score

Speed **med** (smaller pushes, smaller GC working set) · Ops **low** ·
UX **high** (the cache holds what was actually built here).
