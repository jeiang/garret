#!/usr/bin/env bash
# M1 gate: push a real store path through the Pusher, then substitute it back
# out of the Puller with `nix copy` — signature-checked, no --no-check-sigs.
# Provisions a throwaway Garage; leaves nothing behind.
set -euo pipefail

# pwd -P: nix refuses a store whose parent is a symlink, and macOS /tmp is one.
root=$(cd "$(mktemp -d)" && pwd -P)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$root"' EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# A leftover Garage on 3901 answers with a different RPC secret and the failure
# looks like a handshake bug rather than a stale process. Fail clearly instead.
for port in 3900 3901 18080 18081; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is already in use — kill the leftover process and retry" >&2
    exit 1
  fi
done

say "starting garage"
mkdir -p "$root"/garage/{meta,data}
cat > "$root/garage.toml" <<EOF
metadata_dir = "$root/garage/meta"
data_dir = "$root/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(openssl rand -hex 32)"

[s3_api]
s3_region = "garage"
api_bind_addr = "127.0.0.1:3900"
EOF
garage -c "$root/garage.toml" server &
until garage -c "$root/garage.toml" status >/dev/null 2>&1; do sleep 0.3; done

node=$(garage -c "$root/garage.toml" node id -q | cut -d@ -f1)
garage -c "$root/garage.toml" layout assign -z dc1 -c 1G "$node" >/dev/null
garage -c "$root/garage.toml" layout apply --version 1 >/dev/null
garage -c "$root/garage.toml" bucket create garret >/dev/null
keyinfo=$(garage -c "$root/garage.toml" key create garret-key)
key_id=$(grep -o 'GK[0-9a-f]*' <<<"$keyinfo" | head -1)
key_secret=$(awk '/Secret key:/ {print $3}' <<<"$keyinfo")
garage -c "$root/garage.toml" bucket allow --read --write garret --key garret-key >/dev/null

say "generating signing key"
nix key generate-secret --key-name garret-e2e-1 > "$root/signing.key"
pubkey=$(nix key convert-secret-to-public < "$root/signing.key")
echo "public key: $pubkey"

s3_block="
[s3]
bucket = \"garret\"
endpoint_url = \"http://127.0.0.1:3900\"
region = \"garage\"
path_style = true
access_key_id = \"$key_id\"
secret_access_key = \"$key_secret\"
"
cat > "$root/pusher.toml" <<EOF
listen = "127.0.0.1:18080"
db_path = "$root/garret.db"
signing_key_files = ["$root/signing.key"]
$s3_block
EOF
cat > "$root/puller.toml" <<EOF
listen = "127.0.0.1:18081"
db_path = "$root/garret.db"
$s3_block
EOF

say "starting garret"
cargo build --quiet -p garret-pusher -p garret-puller

# Bounded: a service that dies on startup should fail the run, not hang it.
wait_for() {
  local what=$1 url=$2 i
  for i in $(seq 100); do
    curl -s -o /dev/null "$url" && return 0
    sleep 0.3
  done
  echo "$what never came up ($url)" >&2
  exit 1
}

# The Pusher owns the schema, so it must exist before the Puller opens it.
./target/debug/garret-pusher "$root/pusher.toml" &
wait_for pusher "http://127.0.0.1:18080/api/v1/missing-paths"
./target/debug/garret-puller "$root/puller.toml" &
wait_for puller "http://127.0.0.1:18081/nix-cache-info"

say "pushing a store path"
echo "garret e2e $(date)" > "$root/payload"
path=$(nix store add-path "$root/payload" --name garret-e2e-payload)
hash=$(basename "$path" | cut -c1-32)
echo "$path"

info=$(nix path-info --json "$path")
python3 - "$path" "$info" "$root" <<'PY'
import json, subprocess, sys, urllib.request
path, info, root = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
meta = info[0] if isinstance(info, list) else info[path]
nar = subprocess.run(["nix", "nar", "dump-path", path], capture_output=True, check=True).stdout
comp = subprocess.run(["zstd", "-3", "-c"], input=nar, capture_output=True, check=True).stdout
preamble = json.dumps({
    "store_path": path,
    "nar_hash": meta["narHash"],
    "nar_size": meta["narSize"],
    "references": meta.get("references", []),
    "deriver": meta.get("deriver"),
    "ca": meta.get("ca"),
}).encode()
body = len(preamble).to_bytes(4, "little") + preamble + comp
h = path.split("/")[-1][:32]

missing = urllib.request.urlopen(urllib.request.Request(
    "http://127.0.0.1:18080/api/v1/missing-paths", data=json.dumps([h]).encode(),
    headers={"Content-Type": "application/json"}, method="POST")).read()
print("missing-paths ->", missing.decode())
assert json.loads(missing) == [h], "negotiation should report the path missing"

for expected in ("created", "exists"):  # second PUT must be idempotent
    r = urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:18080/api/v1/nar/{h}", data=body, method="PUT"))
    got = json.loads(r.read())["status"]
    print(f"PUT -> {r.status} {got}")
    assert got == expected, f"expected {expected}, got {got}"
PY

say "narinfo"
curl -sf "http://127.0.0.1:18081/$hash.narinfo" | tee "$root/narinfo"
grep -q "^Sig: garret-e2e-1:" "$root/narinfo"

say "NAR request redirects (ADR-0005)"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:18081/nar/$hash.nar.zst")
location=$(curl -s -o /dev/null -w '%{redirect_url}' "http://127.0.0.1:18081/nar/$hash.nar.zst")
echo "$code -> ${location%%\?*}?<presigned>"
[ "$code" = "307" ] || { echo "expected a redirect, got $code"; exit 1; }

say "nix copy out of the puller (signature-checked)"
dest="$root/dest"
nix copy --from "http://127.0.0.1:18081" --to "$dest" "$path" \
  --option trusted-public-keys "$pubkey" \
  --option require-sigs true \
  --option substitute false --refresh
diff <(cat "$dest/$path") "$root/payload"

say "PASS — pushed, signed, redirected, and substituted back with signature verification"
