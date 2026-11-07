# Shared provisioning for the e2e gate and the bench harness: a throwaway
# versitygw, a dev OIDC issuer, signing keys, service configs, and both services
# running. Sourced, not executed; the caller owns $root and the cleanup trap.
# build_binaries sets $bin; everything that runs a garret binary comes after it.
#
# Callers:
#   scripts/e2e.sh              — the end-to-end gate
#   scripts/bench-local.sh      — the self-provisioned benchmark run (spec 09)
#   scripts/bench-pull-sweep.sh — the stepped-concurrency pull sweep (spec 09)
# shellcheck shell=bash

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# The ports one run uses: $port_base and the next port_count - 1, assigned
# below. Parallel runs (git worktrees, agents sharing a machine) must not
# collide: each run gets a random free block unless the caller pins
# GARRET_E2E_PORT_BASE.
port_count=5

port_in_use() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

pick_port_base() {
  local base p
  while :; do
    base=$(( (RANDOM % 40000) + 20000 ))
    for p in $(seq "$base" $((base + port_count - 1))); do
      port_in_use "$p" && continue 2
    done
    echo "$base"
    return
  done
}

port_base=${GARRET_E2E_PORT_BASE:-$(pick_port_base)}
s3_port=$port_base
pusher_port=$((port_base + 1))
puller_port=$((port_base + 2))
pusher_metrics_port=$((port_base + 3))
puller_metrics_port=$((port_base + 4))
pusher_url="http://127.0.0.1:$pusher_port"
puller_url="http://127.0.0.1:$puller_port"

# A leftover process on one of these ports answers with someone else's data,
# and the failure looks like a bug in garret rather than a stale listener. Fail
# clearly instead. (With random ports this mostly guards a pinned
# GARRET_E2E_PORT_BASE.)
require_free_ports() {
  local port
  for port in $(seq "$port_base" $((port_base + port_count - 1))); do
    if port_in_use "$port"; then
      echo "port $port is already in use — kill the leftover process and retry" >&2
      exit 1
    fi
  done
}

# Starts versitygw on the POSIX backend, creates the bucket, and leaves the
# credentials in $key_id/$key_secret and a ready-to-embed TOML fragment in
# $s3_block. The key is static: versitygw's root account comes from the
# environment, so there is nothing to import.
#
# versitygw is a background job of the sourcing shell, so the caller's
# `kill $(jobs -p)` trap takes it down with everything else.
start_s3() {
  say "starting versitygw"
  mkdir -p "$root/vgw"
  key_id=garretkey
  key_secret=garretsecret
  ROOT_ACCESS_KEY=$key_id ROOT_SECRET_KEY=$key_secret \
    versitygw --port "127.0.0.1:$s3_port" --keep-alive posix "$root/vgw" \
    >"$root/versitygw.log" 2>&1 &

  s3_block="
[s3]
bucket = \"garret\"
endpoint_url = \"http://127.0.0.1:$s3_port\"
region = \"us-east-1\"
path_style = true
access_key_id = \"$key_id\"
secret_access_key = \"$key_secret\"
"

  # The bucket is also the readiness probe: versitygw answers only once it is
  # listening, and a PUT is idempotent enough to retry.
  local i
  for i in $(seq 100); do
    if curl -sf -o /dev/null --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$key_id:$key_secret" -X PUT "http://127.0.0.1:$s3_port/garret"; then
      return 0
    fi
    sleep 0.3
  done
  echo "versitygw never came up (127.0.0.1:$s3_port); see $root/versitygw.log" >&2
  exit 1
}

# Leaves the public key in $pubkey and the secret at $root/signing.key.
make_signing_key() {
  say "generating signing key"
  (umask 077; nix key generate-secret --key-name garret-e2e-1 > "$root/signing.key")
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
mint("token-not-yet-valid", nbf=int(time.time()) + 3600)
PY
}

# $1: the pusher's [limits]/[gc] TOML fragment — the one knob the two callers
# disagree on (e2e exercises tiny multiparts and GC; bench wants caps the RSS
# criterion is measured against).
write_configs() {
  local pusher_extras=$1
  cat > "$root/pusher.toml" <<EOF
listen = "127.0.0.1:$pusher_port"
metrics_listen = "127.0.0.1:$pusher_metrics_port"
db_path = "$root/garret.db"
signing_key_files = ["$root/signing.key"]
admin_socket = "$root/admin.sock"
puller_endpoint = "$puller_url"

$pusher_extras

[[oidc]]
issuer = "https://dev.garret.test"
audience = "garret"
client_id = "garret-cli"
jwks_url = "$root/jwks.json"
$s3_block
EOF

  cat > "$root/client.toml" <<EOF
endpoint = "$pusher_url"
puller_endpoint = "$puller_url"
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
listen = "127.0.0.1:$puller_port"
metrics_listen = "127.0.0.1:$puller_metrics_port"
db_path = "$root/garret.db"
bump_debounce_secs = 0

[browse_oidc]
issuer = "https://dev.garret.test"
audience = "garret"
jwks_url = "$root/jwks.json"
$s3_block
EOF
}

# Sets $bin to the binaries' directory and checks each is there. GARRET_BIN
# points at already-built binaries (CI passes the nix build's result/bin), so
# nothing is compiled; unset, cargo builds them into its target dir.
# $1: debug|release; rest: the binaries this caller actually runs.
build_binaries() {
  local profile=$1 exe
  shift
  bin=${GARRET_BIN:-${CARGO_TARGET_DIR:-./target}/$profile}
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
#
# GARRET_WRAP is a command prefix (e.g. "taskset -c 0" or a systemd-run
# scope) applied to the garret services and the bench client — but not to
# versitygw or the builds: in real usage the S3 service is upstream of the
# sandbox, so resource-limited benchmarks constrain garret alone.
start_services() {
  # Deliberately backwards: the Pusher owns the schema, and the Puller must
  # wait for it rather than die, serving 503 until /ready says otherwise.
  ${GARRET_WRAP:-} "$bin"/garret-puller "$root/puller.toml" &
  ${GARRET_WRAP:-} "$bin"/garret-pusher "$root/pusher.toml" &
  pusher_pid=$!
  wait_for pusher "$pusher_url/api/v1/missing-paths"
  # A wrapper like systemd-run forks, making $! the wrapper's PID; resolve
  # the PID the RSS sampler and GC restart actually need.
  [ -n "${GARRET_WRAP:-}" ] && pusher_pid=$(pgrep -nf "garret-pusher $root/pusher.toml")
  local i
  for i in $(seq 100); do
    curl -sf -o /dev/null "$puller_url/ready" && break
    [ "$i" = 100 ] && { echo "puller never became ready" >&2; exit 1; }
    sleep 0.3
  done
}
