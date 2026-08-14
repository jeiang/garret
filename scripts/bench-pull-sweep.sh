#!/usr/bin/env bash
# Stepped-concurrency pull sweep (spec 09 / ticket 28): throwaway Garage +
# both services, corpus pushed once, then the pull scenario at c=20/100/300
# with keep-alive on and off. Reported-only — no baseline, no latency gate;
# the curve is what a human reads before reopening ticket 26. Heavier than
# bench-local's headline pull, which is why it is its own recipe.
set -euo pipefail

root=$(cd "$(mktemp -d)" && pwd -P)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$root"' EXIT

bin=${GARRET_BIN:-./target/release}

source "$(dirname "$0")/provision.sh"

require_free_ports "$s3_port" "$garage_rpc_port" "$pusher_port" "$puller_port" "$pusher_metrics_port" "$puller_metrics_port"
# Corpus (~750 MiB) plus its serial baseline; no 2 GiB streaming blob here.
start_garage 5G
make_signing_key
mint_dev_tokens

write_configs "[limits]
part_size = 16777216
max_parts_in_flight = 4
max_concurrent_uploads = 32
max_in_flight_bytes = 268435456"
build_binaries release garret-pusher garret-puller garret-bench
start_services

say "seeding the corpus"
GARRET_TOKEN=$(cat "$root/token")
export GARRET_TOKEN
"$bin"/garret-bench push --endpoint "$pusher_url"

steps=${GARRET_SWEEP_STEPS:-20,100,300}
say "pull sweep c=$steps, keep-alive on"
"$bin"/garret-bench pull \
  --puller-endpoint "$puller_url" \
  --pull-concurrency "$steps" \
  --json bench-pull-sweep.json

say "pull sweep c=$steps, keep-alive off"
"$bin"/garret-bench pull \
  --puller-endpoint "$puller_url" \
  --pull-concurrency "$steps" \
  --no-pull-keepalive \
  --json bench-pull-sweep-nokeepalive.json

echo
echo "results in bench-pull-sweep.json and bench-pull-sweep-nokeepalive.json"
