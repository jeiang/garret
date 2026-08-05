#!/usr/bin/env python3
import json
from collections import defaultdict

DIR = "/tmp/garret-dedup"
SETS = ["16-64-256", "64-256-1024", "256-1024-4096", "512-2048-8192"]
GENS = list(range(116, 136))

gen_paths = {}
for g in GENS:
    with open(f"{DIR}/gen{g}.json") as f:
        d = json.load(f)
    gen_paths[g] = {p: (v["narSize"], v["narHash"]) for p, v in d.items()}
union_meta = {}
for g in GENS:
    union_meta.update(gen_paths[g])

chunks = {s: {} for s in SETS}
sizes_from_tool = {}
with open(f"{DIR}/corpus.jsonl") as f:
    for line in f:
        r = json.loads(line)
        p, s = r["path"], r["set"]
        sizes_from_tool.setdefault(p, r["nar_size"])
        cl = [(c[0], c[1]) for c in r["chunks"]]
        if s == "whole":
            for ss in SETS:
                chunks[ss][p] = cl
        else:
            chunks[s][p] = cl

out = {}

# sanity: tool nar_size vs path-info narSize
mismatch = [p for p in union_meta if sizes_from_tool.get(p) != union_meta[p][0]]
out["size_mismatches"] = len(mismatch)
if mismatch:
    out["size_mismatch_examples"] = [
        (p, union_meta[p][0], sizes_from_tool.get(p)) for p in mismatch[:5]
    ]

gguf = [p for p in union_meta if p.endswith(".gguf")]
gguf_bytes = sum(union_meta[p][0] for p in gguf)
out["gguf"] = {"paths": gguf, "total_bytes": gguf_bytes}

# GGUF cross/self dedup per set
out["gguf_dedup"] = {}
for s in SETS:
    uniq = {}
    tot = 0
    for p in gguf:
        for h, sz in chunks[s][p]:
            uniq[h] = sz
            tot += sz
    out["gguf_dedup"][s] = {"total": tot, "unique": sum(uniq.values())}

# non-GGUF raw corpus dedup + incremental sim
non = [p for p in union_meta if not p.endswith(".gguf")]
non_total = sum(union_meta[p][0] for p in non)
out["non_gguf_corpus"] = {"paths": len(non), "bytes": non_total, "per_set": {}}
for s in SETS:
    uniq = {}
    for p in non:
        for h, sz in chunks[s][p]:
            uniq[h] = sz
    out["non_gguf_corpus"]["per_set"][s] = sum(uniq.values())

seen_p, seen_h = set(), set()
seen_c = {s: set() for s in SETS}
inc = {"path_identity": 0, "narhash": 0, "chunked": {s: 0 for s in SETS}}
for gi, g in enumerate(GENS):
    new = [p for p in gen_paths[g] if p not in seen_p and not p.endswith(".gguf")]
    seen_p.update(gen_paths[g].keys())
    bp = sum(gen_paths[g][p][0] for p in new)
    bn = 0
    for p in new:
        sz, nh = gen_paths[g][p]
        if nh not in seen_h:
            seen_h.add(nh)
            bn += sz
    bc = {}
    for s in SETS:
        acc = 0
        for p in new:
            for h, sz in chunks[s][p]:
                if h not in seen_c[s]:
                    seen_c[s].add(h)
                    acc += sz
        bc[s] = acc
    if gi > 0:
        inc["path_identity"] += bp
        inc["narhash"] += bn
        for s in SETS:
            inc["chunked"][s] += bc[s]
out["non_gguf_incremental_after_gen116"] = inc

# gen128 big new paths
prev = set()
for g in range(116, 128):
    prev.update(gen_paths[g].keys())
big = sorted(
    ((gen_paths[128][p][0], p) for p in gen_paths[128] if p not in prev),
    reverse=True,
)[:8]
out["gen128_new_big"] = big

# gen127 (nixpkgs bump) non-GGUF detail per set: new bytes vs chunk-dedup bytes
print(json.dumps(out, indent=1))
