#!/usr/bin/env python3
"""Compare a garret-bench results file against the checked-in baseline
(spec 09). Reports deltas for every numeric metric; judgement stays with the
human — the only hard failures are failed requests in the new run or an
environment mismatch, because a laptop number diffed against a sandbox
number is noise dressed up as a regression."""

import json
import sys


def flatten(value, prefix=""):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from flatten(v, f"{prefix}{k}.")
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from flatten(v, f"{prefix}{i}.")
    elif isinstance(value, bool):
        pass
    elif isinstance(value, (int, float)):
        yield prefix.rstrip("."), value


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} <baseline.json> <current.json>")
    baseline_path, current_path = sys.argv[1], sys.argv[2]
    baseline = json.load(open(baseline_path))
    current = json.load(open(current_path))

    labels = (baseline.get("meta", {}).get("label"), current.get("meta", {}).get("label"))
    if labels[0] != labels[1]:
        sys.exit(
            f"environment mismatch: baseline is '{labels[0]}', current is '{labels[1]}'\n"
            "these runs are not comparable; record a baseline for this environment instead"
        )

    base = dict(flatten(baseline))
    curr = dict(flatten(current))
    skip = ("meta.seed", "meta.timestamp_unix", "meta.cpus")

    print(f"{'metric':<38} {'baseline':>14} {'current':>14} {'delta':>9}")
    for key in sorted(set(base) | set(curr)):
        if key.startswith(skip):
            continue
        b, c = base.get(key), curr.get(key)
        if b is None or c is None:
            print(f"{key:<38} {b if b is not None else '—':>14} "
                  f"{c if c is not None else '—':>14} {'new' if b is None else 'gone':>9}")
            continue
        delta = f"{(c - b) / b:+8.1%}" if b else "     n/a"
        fmt = lambda v: f"{v:>14.2f}" if isinstance(v, float) else f"{v:>14}"
        print(f"{key:<38} {fmt(b)} {fmt(c)} {delta}")

    failures = sum(v for k, v in curr.items() if k.endswith(".failed"))
    if failures:
        sys.exit(f"\nFAIL: the current run has {int(failures)} failed request(s)")
    print("\nzero failures; latency/throughput judgement is yours")


if __name__ == "__main__":
    main()
