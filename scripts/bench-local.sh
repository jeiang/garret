#!/usr/bin/env bash
# Self-provisioned benchmark run (spec 09): throwaway Garage + both services
# from the flake, all three garret-bench scenarios in release mode, and the
# Pusher's RSS sampled throughout — the one criterion the harness cannot see
# from the outside. Leaves bench-results.json behind for `just bench-compare`.
set -euo pipefail

root=$(cd "$(mktemp -d)" && pwd -P)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$root"' EXIT

bin=${GARRET_BIN:-./target/release}
out=${GARRET_BENCH_JSON:-bench-results.json}

source "$(dirname "$0")/provision.sh"

require_free_ports "$s3_port" "$garage_rpc_port" "$pusher_port" "$puller_port" "$pusher_metrics_port" "$puller_metrics_port"
# Room for the corpus (~750 MiB), its serial baseline, and the 2 GiB blob.
start_garage 20G
make_signing_key
mint_dev_tokens

# The caps the RSS criterion is measured against (spec 09 scenario 1: peak
# RSS < 2x max_in_flight_bytes). Modest on purpose: with the 2 GiB default
# cap the check could never fail on a corpus this size. Overridable so a
# memory-limited sandbox can size the Pusher to its box, exactly as a real
# deployment would. No [gc]: eviction mid-benchmark would measure the
# sweep, not the push path.
in_flight_cap=${GARRET_IN_FLIGHT_CAP:-268435456} # 256 MiB
write_configs "[limits]
part_size = 16777216
max_parts_in_flight = 4
max_concurrent_uploads = 32
max_in_flight_bytes = $in_flight_cap"
build_binaries release garret-pusher garret-puller garret-bench
start_services

say "sampling pusher RSS"
rss_log="$root/rss.log"
(
  while kill -0 "$pusher_pid" 2>/dev/null; do
    ps -o rss= -p "$pusher_pid" 2>/dev/null || true
    sleep 0.2
  done > "$rss_log"
) &

say "running all scenarios"
GARRET_TOKEN=$(cat "$root/token")
export GARRET_TOKEN
# The bench client stays outside GARRET_WRAP: it stands in for the remote
# machines pushing to the cache, which don't share the sandbox's limits.
"$bin"/garret-bench all \
  --endpoint "$pusher_url" \
  --puller-endpoint "$puller_url" \
  --json "$out" \
  ${GARRET_BENCH_ARGS:-}

say "memory criterion (spec 09 scenario 1)"
# ps reports RSS in KiB on both macOS and Linux.
peak_kib=$(sort -n "$rss_log" | tail -1)
final_kib=$(tail -1 "$rss_log")
budget_kib=$((2 * in_flight_cap / 1024))
echo "  peak RSS ${peak_kib:-0} KiB, final ${final_kib:-0} KiB, budget $budget_kib KiB (2x in-flight cap)"
python3 - "$out" "${peak_kib:-0}" "${final_kib:-0}" <<'PY'
import json, sys
path, peak_kib, final_kib = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
r = json.load(open(path))
r["pusher_rss"] = {"peak_kib": peak_kib, "final_kib": final_kib}
json.dump(r, open(path, "w"), indent=2)
PY
[ "${peak_kib:-0}" -lt "$budget_kib" ] || {
  echo "FAIL: pusher RSS exceeded 2x the in-flight byte cap"; exit 1; }

echo
echo "results written to $out — compare with: just bench-compare"
