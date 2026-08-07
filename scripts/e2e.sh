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
for port in 3900 3901 18080 18081 19091 19092; do
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
say "minting dev-issuer keys and a token"
# The dev issuer is a static JWKS on disk (spec 04-auth) — the sanctioned local
# override. There is no auth-disable flag to reach for instead.
GARRET_E2E_ROOT="$root" python3 - <<'PY'
import json, os, time
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

root = os.environ["GARRET_E2E_ROOT"]
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
jwk.update({"kid": "dev-1", "use": "sig", "alg": "RS256"})
with open(f"{root}/jwks.json", "w") as f:
    json.dump({"keys": [jwk]}, f)

def mint(name, **overrides):
    claims = {
        "iss": "https://dev.garret.test",
        "aud": "garret",
        "sub": "dev-user",
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    token = jwt.encode(claims, key, algorithm="RS256", headers={"kid": "dev-1"})
    with open(f"{root}/{name}", "w") as f:
        f.write(token)

mint("token")
mint("token-wrong-audience", aud="somebody-else")
mint("token-expired", exp=int(time.time()) - 60)
PY

cat > "$root/pusher.toml" <<EOF
listen = "127.0.0.1:18080"
metrics_listen = "127.0.0.1:19091"
db_path = "$root/garret.db"
signing_key_files = ["$root/signing.key"]

[limits]
# 5 MiB is S4's minimum part size; small here so the multipart path is
# actually exercised without moving 64 MiB.
part_size = 5242880
max_parts_in_flight = 2
max_concurrent_uploads = 4

[[oidc]]
issuer = "https://dev.garret.test"
audience = "garret"
jwks_url = "$root/jwks.json"
$s3_block
EOF

cat > "$root/client.toml" <<EOF
endpoint = "http://127.0.0.1:18080"

[oidc]
issuer = "https://dev.garret.test"
client_id = "garret-cli"
audience = "garret"
EOF
cat > "$root/puller.toml" <<EOF
listen = "127.0.0.1:18081"
metrics_listen = "127.0.0.1:19092"
db_path = "$root/garret.db"
$s3_block
EOF

say "starting garret"
cargo build --quiet -p garret-pusher -p garret-puller -p garret-client

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

say "unauthenticated and bad tokens are refused"
check_401() {
  local what=$1 code
  shift
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' -d '[]' "$@" \
    "http://127.0.0.1:18080/api/v1/missing-paths")
  echo "  $what -> $code"
  [ "$code" = "401" ] || { echo "expected 401 for $what, got $code"; exit 1; }
}
check_401 "no token"
check_401 "garbage token"   -H "Authorization: Bearer not-a-jwt"
check_401 "wrong audience"  -H "Authorization: Bearer $(cat "$root/token-wrong-audience")"
check_401 "expired token"   -H "Authorization: Bearer $(cat "$root/token-expired")"

say "building a two-path closure"
# A real reference, not a lone path: this is what exercises closure discovery,
# the narinfo References line, and the signature computed over it. `nix store
# add-path` will not do — it does not scan for references.
stamp=$(date +%s)
path=$(nix build --impure --no-link --print-out-paths --expr "
let
  leaf = derivation {
    name = \"garret-e2e-leaf\"; system = builtins.currentSystem;
    builder = \"/bin/sh\"; args = [ \"-c\" \"echo leaf $stamp > \$out\" ];
  };
in derivation {
  name = \"garret-e2e-root\"; system = builtins.currentSystem;
  builder = \"/bin/sh\"; args = [ \"-c\" \"echo \${leaf} > \$out\" ];
}")
hash=$(basename "$path" | cut -c1-32)
leaf=$(nix path-info --recursive "$path" | grep -- '-garret-e2e-leaf$')
echo "root: $path"
echo "leaf: $leaf"

say "pushing the closure with the garret client"
export GARRET_TOKEN
GARRET_TOKEN=$(cat "$root/token")
garret() { ./target/debug/garret --config "$root/client.toml" "$@"; }

garret push "$path" | tee "$root/push.out"
grep -q "2 path(s) in closure, 2 missing" "$root/push.out"
grep -q "done: 2 path(s) uploaded" "$root/push.out"

# Re-pushing must be a no-op: idempotency is normal operation, not an error.
garret push "$path" | tee "$root/push2.out"
grep -q "2 path(s) in closure, 0 missing" "$root/push2.out"

say "multipart: a body larger than several parts"
# Incompressible, so zstd cannot shrink it back under the part size.
head -c 12000000 /dev/urandom > "$root/big.bin"
big=$(nix store add-path "$root/big.bin" --name garret-e2e-big)
garret push "$big" | tee "$root/push-big.out"
grep -q "done: 1 path(s) uploaded" "$root/push-big.out"

metric() { curl -sf "http://127.0.0.1:$1/metrics" | awk -v k="$2" '$1 == k {print $2}'; }
parts=$(metric 19091 garret_s3_parts_total)
completed=$(metric 19091 garret_s3_multipart_completed_total)
aborted=$(metric 19091 garret_s3_multipart_aborted_total)
echo "  parts=$parts completed=$completed aborted=${aborted:-0}"
[ "${completed:-0}" = "1" ] || { echo "expected one completed multipart"; exit 1; }
[ "${parts:-0}" -ge 3 ] || { echo "expected 3+ parts for a 12 MB body"; exit 1; }
[ -z "$aborted" ] || [ "$aborted" = "0" ] || { echo "multipart was aborted"; exit 1; }

# The big path must come back intact — proof the parts were reassembled in order.
nix copy --from "http://127.0.0.1:18081" --to "$root/dest-big" "$big" \
  --option trusted-public-keys "$pubkey" --option require-sigs true \
  --option substitute false --refresh
cmp "$root/dest-big/$big" "$root/big.bin"

say "metrics and health"
curl -sf "http://127.0.0.1:19092/healthz" >/dev/null
accepted=$(metric 19091 garret_uploads_accepted_total)   # 2 closure paths + 1 big
echo "  uploads accepted: $accepted"
[ "$accepted" = "3" ] || { echo "expected 3 accepted uploads, got $accepted"; exit 1; }
[ "$(metric 19091 garret_uploads_limit)" = "4" ] || { echo "cap not exported"; exit 1; }
if curl -sf "http://127.0.0.1:19091/metrics" | grep -q '^garret_narinfo_requests_total'; then
  echo "pusher must not expose puller metrics"; exit 1
fi
# The public listener must never serve metrics — they are internal-only.
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/metrics")
echo "  public /metrics -> $code"
[ "$code" != "200" ] || { echo "metrics leaked onto the public listener"; exit 1; }

say "narinfo"
curl -sf "http://127.0.0.1:18081/$hash.narinfo" | tee "$root/narinfo"
grep -q "^Sig: garret-e2e-1:" "$root/narinfo"
# The reference must appear by *name*, which a bare hash could not reconstruct.
grep -q "^References: $(basename "$leaf")\$" "$root/narinfo"

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
# nix walks the closure itself, so both paths must have arrived and verified.
diff <(cat "$dest/$path") <(cat "$path")
diff <(cat "$dest/$leaf") <(cat "$leaf")

say "PASS — authenticated push via the client, signed, redirected, and substituted back verified"
