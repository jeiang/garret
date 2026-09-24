# The Pusher verifies NarHash and NarSize instead of trusting them

This supersedes the "trusted NarHash" part of
[ADR-0002](0002-whole-nar-storage.md), which had the server hash only the
compressed bytes it stores and sign whatever NarHash the authenticated
client claimed. That made the Pusher a signing oracle. Anyone holding a
push token could get the cache key over any NarHash for any
input-addressed path, including paths nobody has built yet, since their
hashes follow from public flakes. First-writer-wins and LRU then keep a
poisoned entry indefinitely (security review GARRET-SIGN-002). Now the
Pusher decompresses the zstd stream as it relays it to S3, never
buffering the NAR or changing the stored bytes, and sha256es the result.
At end of stream it compares against the preamble. Every store path
reads to EOF before it commits, so on a mismatch the single `PutObject`
is never sent or the multipart is aborted, the reply is `400`, and
nothing is inserted or signed. The wrapper sits around the body stream
in the Pusher; storage is untouched. The decoder window is capped at
8 MiB, what level 19 uses, so each upload's extra memory is bounded
(2 MiB at the client's default level 3). Clients configured for the
`--ultra` levels 20–22 are refused.

Cost, measured on an Apple M3 Pro:
- A real 379 MiB NAR (llvm 21 `lib`, 97.8 MiB at zstd-3) decompresses
  and hashes at about 650 MiB/s of NAR, or 1.57 CPU-s per GiB. Hashing
  only the compressed bytes costs 0.15 CPU-s per GiB.
- `just microbench` `nar_verify` on the synthetic corpus runs at
  1.26 GiB/s, against 1.65 GiB/s for plain `sha256`.
- The production host has 2 x86_64 vCPUs. Expect the same order of
  magnitude there, a few hundred MiB/s of NAR per core. That is on the
  order of its uplink, so a saturating push may now be CPU-bound rather
  than network-bound [INFERENCE].

The check runs inline on the Tokio worker per chunk, as the existing
FileHash does.

What it does not buy:
- A matching NarHash proves the bytes are the NAR the preamble
  describes, not that they are the genuine build output. A token holder
  can still sign a self-consistent malicious NAR, so token scope stays
  the primary control.
- A re-push claiming a different NarHash for a present path is not
  detected. `exists` is answered before the preamble is read, on
  purpose, so `Expect: 100-continue` clients skip the transfer.
