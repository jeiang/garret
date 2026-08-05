#!/usr/bin/env python3
"""Dedup analysis over chunkmeter output + nix path-info generation dumps."""
import json, sys, statistics
from collections import defaultdict

DIR = "/tmp/garret-dedup"
SETS = ["16-64-256", "64-256-1024", "256-1024-4096", "512-2048-8192"]
GENS = list(range(116, 136))

GiB = 1024 ** 3


def fmt(b):
    return f"{b / GiB:.3f}"


# ---- load generation metadata ----
gen_paths = {}   # gen -> {path: (narSize, narHash)}
for g in GENS:
    with open(f"{DIR}/gen{g}.json") as f:
        d = json.load(f)
    gen_paths[g] = {p: (v["narSize"], v["narHash"]) for p, v in d.items()}

union_meta = {}
for g in GENS:
    union_meta.update(gen_paths[g])

# ---- load chunk records ----
# per set: path -> list[(hash, size)]
chunks = {s: {} for s in SETS}
whole_paths = set()
with open(f"{DIR}/corpus.jsonl") as f:
    for line in f:
        r = json.loads(line)
        p, s = r["path"], r["set"]
        cl = [(c[0], c[1]) for c in r["chunks"]]
        if s == "whole":
            whole_paths.add(p)
            for ss in SETS:
                chunks[ss][p] = cl
        else:
            chunks[s][p] = cl

# ---- load zstd records (non-GGUF subset) ----
# per set: path -> list[(hash, size, zsize)]; plus nar-zstd3: path -> (nar_size, zsize)
zchunks = {s: {} for s in SETS}
znar = {}
try:
    with open(f"{DIR}/corpus-zstd.jsonl") as f:
        for line in f:
            r = json.loads(line)
            p, s = r["path"], r["set"]
            if s == "nar-zstd3":
                znar[p] = (r["nar_size"], r["zsize"])
            elif s == "whole":
                cl = [(c[0], c[1], r["zsize"]) for c in r["chunks"]]
                znar[p] = (r["nar_size"], r["zsize"])
                for ss in SETS:
                    zchunks[ss][p] = cl
            else:
                zchunks[s][p] = [(c[0], c[1], c[2]) for c in r["chunks"]]
except FileNotFoundError:
    pass

out = {}

# ---- sanity ----
total_bytes = sum(sz for sz, _ in union_meta.values())
out["corpus"] = {"unique_store_paths": len(union_meta), "unique_path_bytes": total_bytes}
for s in SETS:
    missing = [p for p in union_meta if p not in chunks[s]]
    if missing:
        print(f"WARN: {len(missing)} paths missing from set {s}", file=sys.stderr)

# ---- A. corpus-wide dedup per set ----
out["corpus_dedup"] = {}
for s in SETS:
    uniq = {}
    total_refs = 0
    counts = []
    for p, cl in chunks[s].items():
        total_refs += len(cl)
        counts.append(len(cl))
        for h, sz in cl:
            uniq[h] = sz
    ub = sum(uniq.values())
    counts.sort()
    out["corpus_dedup"][s] = {
        "unique_chunk_bytes": ub,
        "dedup_ratio_vs_path_identity": total_bytes / ub,
        "chunk_rows": len(uniq),
        "chunkref_rows": total_refs,
        "object_rows": len(union_meta),
        "chunks_per_nar_median": counts[len(counts) // 2],
        "chunks_per_nar_p90": counts[int(len(counts) * 0.9)],
        "chunks_per_nar_max": counts[-1],
        "chunks_per_nar_mean": total_refs / len(counts),
    }

# ---- B. whole-NAR-hash baseline ----
narhash_bytes = {}
for p, (sz, nh) in union_meta.items():
    narhash_bytes[nh] = sz
out["whole_nar_baseline"] = {
    "unique_narhash_count": len(narhash_bytes),
    "unique_narhash_bytes": sum(narhash_bytes.values()),
    "dedup_ratio_vs_path_identity": total_bytes / sum(narhash_bytes.values()),
}

# ---- C. incremental generation-ordered simulation ----
sim = {"per_gen": [], "totals": {}}
seen_paths = set()
seen_hashes = set()
seen_chunks = {s: set() for s in SETS}
tot_path, tot_nar, tot_chunk = 0, 0, {s: 0 for s in SETS}
inc_path, inc_nar, inc_chunk = 0, 0, {s: 0 for s in SETS}  # excluding first gen
for gi, g in enumerate(GENS):
    new = [p for p in gen_paths[g] if p not in seen_paths]
    b_path = sum(gen_paths[g][p][0] for p in new)
    b_nar = 0
    for p in new:
        sz, nh = gen_paths[g][p]
        if nh not in seen_hashes:
            b_nar += sz
            seen_hashes.add(nh)
    b_chunk = {}
    for s in SETS:
        acc = 0
        for p in new:
            for h, sz in chunks[s].get(p, []):
                if h not in seen_chunks[s]:
                    acc += sz
                    seen_chunks[s].add(h)
        b_chunk[s] = acc
    seen_paths.update(new)
    row = {"gen": g, "new_paths": len(new), "new_path_bytes": b_path,
           "narhash_dedup_bytes": b_nar,
           "chunk_dedup_bytes": {s: b_chunk[s] for s in SETS}}
    sim["per_gen"].append(row)
    tot_path += b_path
    tot_nar += b_nar
    for s in SETS:
        tot_chunk[s] += b_chunk[s]
    if gi > 0:
        inc_path += b_path
        inc_nar += b_nar
        for s in SETS:
            inc_chunk[s] += b_chunk[s]
sim["totals"] = {
    "stored_path_identity": tot_path,
    "stored_narhash": tot_nar,
    "stored_chunked": tot_chunk,
    "incremental_after_gen116_path_identity": inc_path,
    "incremental_after_gen116_narhash": inc_nar,
    "incremental_after_gen116_chunked": inc_chunk,
}
out["incremental_sim"] = sim

# ---- D/E. compression analysis (non-GGUF subset) ----
if znar:
    zpaths = set()
    for s in SETS:
        zpaths.update(zchunks[s].keys())
    zpaths &= set(znar.keys())
    sub_total = sum(znar[p][0] for p in zpaths)
    # whole-NAR zstd with narHash dedup
    seen_nh, wnz, wnz_nodedup = set(), 0, 0
    for p in zpaths:
        sz, zs = znar[p]
        wnz_nodedup += zs
        nh = union_meta[p][1]
        if nh not in seen_nh:
            seen_nh.add(nh)
            wnz += zs
    comp = {"subset_paths": len(zpaths), "subset_bytes": sub_total,
            "whole_nar_zstd3_bytes_nodedup": wnz_nodedup,
            "whole_nar_zstd3_bytes_narhash_dedup": wnz, "per_set": {}}
    for s in SETS:
        uniqz = {}
        all_inst_z = 0
        incompressible = 0  # unique chunk bytes with ratio > 0.95
        uniq_raw = {}
        for p in zpaths:
            for h, sz, zs in zchunks[s].get(p, []):
                all_inst_z += zs
                uniqz[h] = zs
                uniq_raw[h] = sz
        for h, zs in uniqz.items():
            if zs / uniq_raw[h] > 0.95:
                incompressible += uniq_raw[h]
        comp["per_set"][s] = {
            "unique_chunk_zstd3_bytes": sum(uniqz.values()),
            "all_instances_zstd3_bytes": all_inst_z,
            "window_loss_vs_whole_nar": all_inst_z / wnz_nodedup,
            "incompressible_unique_chunk_bytes": incompressible,
            "incompressible_share_of_unique": incompressible / sum(uniq_raw.values()),
        }
    # incremental sim on compressed sizes
    zmap = {s: {} for s in SETS}
    for s in SETS:
        for p in zpaths:
            for h, sz, zs in zchunks[s].get(p, []):
                zmap[s][h] = zs
    seen_paths2, seen_nh2 = set(), set()
    seen_c2 = {s: set() for s in SETS}
    z_inc_nar, z_inc_chunk = 0, {s: 0 for s in SETS}
    z_tot_nar, z_tot_chunk = 0, {s: 0 for s in SETS}
    for gi, g in enumerate(GENS):
        new = [p for p in gen_paths[g] if p not in seen_paths2 and p in zpaths]
        seen_paths2.update(gen_paths[g].keys())
        bn = 0
        for p in new:
            nh = union_meta[p][1]
            if nh not in seen_nh2:
                seen_nh2.add(nh)
                bn += znar[p][1]
        z_tot_nar += bn
        if gi > 0:
            z_inc_nar += bn
        for s in SETS:
            acc = 0
            for p in new:
                for h, sz, zs in zchunks[s].get(p, []):
                    if h not in seen_c2[s]:
                        seen_c2[s].add(h)
                        acc += zs
            z_tot_chunk[s] += acc
            if gi > 0:
                z_inc_chunk[s] += acc
    comp["stored_compressed_narhash_dedup"] = z_tot_nar
    comp["stored_compressed_chunked"] = z_tot_chunk
    comp["incremental_compressed_narhash"] = z_inc_nar
    comp["incremental_compressed_chunked"] = z_inc_chunk
    out["compression"] = comp

print(json.dumps(out, indent=1))
