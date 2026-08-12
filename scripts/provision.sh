# Shared provisioning for the e2e gate and the bench harness: a throwaway
# Garage, a dev OIDC issuer, signing keys, service configs, and both services
# running. Sourced, not executed; the caller owns $root, $bin and the cleanup
# trap.
#
# Callers:
#   scripts/e2e.sh         — the M1 end-to-end gate
#   scripts/bench-local.sh — the self-provisioned benchmark run (spec 09)
# shellcheck shell=bash

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# A leftover Garage on 3901 answers with a different RPC secret and the failure
# looks like a handshake bug rather than a stale process. Fail clearly instead.
require_free_ports() {
  local port
  for port in "$@"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "port $port is already in use — kill the leftover process and retry" >&2
      exit 1
    fi
  done
}

# Starts Garage, creates the bucket and key, and leaves the credentials in
# $key_id/$key_secret and a ready-to-embed TOML fragment in $s3_block.
# $1: bucket capacity for the layout (the e2e corpus fits in 1G; bench needs
# room for the 2 GiB streaming blob and the corpus).
start_garage() {
  local capacity=$1
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

  local node keyinfo
  node=$(garage -c "$root/garage.toml" node id -q | cut -d@ -f1)
  garage -c "$root/garage.toml" layout assign -z dc1 -c "$capacity" "$node" >/dev/null
  garage -c "$root/garage.toml" layout apply --version 1 >/dev/null
  garage -c "$root/garage.toml" bucket create garret >/dev/null
  keyinfo=$(garage -c "$root/garage.toml" key create garret-key)
  key_id=$(grep -o 'GK[0-9a-f]*' <<<"$keyinfo" | head -1)
  key_secret=$(awk '/Secret key:/ {print $3}' <<<"$keyinfo")
  garage -c "$root/garage.toml" bucket allow --read --write garret --key garret-key >/dev/null

  s3_block="
[s3]
bucket = \"garret\"
endpoint_url = \"http://127.0.0.1:3900\"
region = \"garage\"
path_style = true
access_key_id = \"$key_id\"
secret_access_key = \"$key_secret\"
"
}

# Leaves the public key in $pubkey and the secret at $root/signing.key.
make_signing_key() {
  say "generating signing key"
  nix key generate-secret --key-name garret-e2e-1 > "$root/signing.key"
  pubkey=$(nix key convert-secret-to-public < "$root/signing.key")
  echo "public key: $pubkey"
}

mint_dev_tokens() {
  say "minting dev-issuer keys and a token"
  # The dev issuer is a static JWKS on disk (spec 04-auth) — the sanctioned
  # local override. There is no auth-disable flag to reach for instead.
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
# Well past the server's 60s skew allowance: expired by exactly the leeway is
# a coin flip on sub-second timing, not a test.
mint("token-expired", exp=int(time.time()) - 3600)
PY
}

# $1: the pusher's [limits]/[gc] TOML fragment — the one knob the two callers
# disagree on (e2e exercises tiny multiparts and GC; bench wants caps the RSS
# criterion is measured against).
write_configs() {
  local pusher_extras=$1
  cat > "$root/pusher.toml" <<EOF
listen = "127.0.0.1:18080"
metrics_listen = "127.0.0.1:19091"
db_path = "$root/garret.db"
signing_key_files = ["$root/signing.key"]
admin_socket = "$root/admin.sock"
puller_endpoint = "http://127.0.0.1:18081"

$pusher_extras

[[oidc]]
issuer = "https://dev.garret.test"
audience = "garret"
client_id = "garret-cli"
jwks_url = "$root/jwks.json"
$s3_block
EOF

  cat > "$root/client.toml" <<EOF
endpoint = "http://127.0.0.1:18080"
puller_endpoint = "http://127.0.0.1:18081"
public_keys = ["$pubkey"]

[oidc]
issuer = "https://dev.garret.test"
client_id = "garret-cli"
audience = "garret"

[watch]
nix_db = "/nix/var/nix/db/db.sqlite"
cursor_path = "$root/watcher-cursor"
poll_interval_secs = 1
EOF

  cat > "$root/puller.toml" <<EOF
listen = "127.0.0.1:18081"
metrics_listen = "127.0.0.1:19092"
db_path = "$root/garret.db"
bump_debounce_secs = 0

[browse_oidc]
issuer = "https://dev.garret.test"
audience = "garret"
jwks_url = "$root/jwks.json"
$s3_block
EOF
}

# $1: debug|release (must match the caller's $bin); rest: the binaries this
# caller actually runs.
build_binaries() {
  local profile=$1 exe
  shift
  say "starting garret"
  if [ -z "${GARRET_BIN:-}" ]; then
    local crates=()
    for exe in "$@"; do
      case "$exe" in
        garret) crates+=(-p garret-client) ;;
        *) crates+=(-p "$exe") ;;
      esac
    done
    if [ "$profile" = release ]; then
      cargo build --quiet --release "${crates[@]}"
    else
      cargo build --quiet "${crates[@]}"
    fi
  fi
  for exe in "$@"; do
    [ -x "$bin/$exe" ] || { echo "missing $bin/$exe" >&2; exit 1; }
  done
}

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

# Leaves the Pusher's PID in $pusher_pid (the e2e GC stage restarts it, and
# the bench RSS sampler watches it).
start_services() {
  # Deliberately backwards: the Pusher owns the schema, and the Puller must
  # wait for it rather than die, serving 503 until /ready says otherwise.
  "$bin"/garret-puller "$root/puller.toml" &
  "$bin"/garret-pusher "$root/pusher.toml" &
  pusher_pid=$!
  wait_for pusher "http://127.0.0.1:18080/api/v1/missing-paths"
  local i
  for i in $(seq 100); do
    curl -sf -o /dev/null "http://127.0.0.1:18081/ready" && break
    [ "$i" = 100 ] && { echo "puller never became ready" >&2; exit 1; }
    sleep 0.3
  done
}
