#!/usr/bin/env bash
# M1 gate: push a real store path through the Pusher, then substitute it back
# out of the Puller with `nix copy` — signature-checked, no --no-check-sigs.
# Provisions a throwaway Garage; leaves nothing behind.
set -euo pipefail

# pwd -P: nix refuses a store whose parent is a symlink, and macOS /tmp is one.
root=$(cd "$(mktemp -d)" && pwd -P)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$root"' EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# GARRET_BIN points at a directory of already-built binaries (CI passes the nix
# build's result/bin), so the run does not compile the workspace a second time.
# Unset means the usual local flow: build the workspace in debug below.
bin=${GARRET_BIN:-./target/debug}

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
# Well past the server's 60s skew allowance: expired by exactly the leeway is
# a coin flip on sub-second timing, not a test.
mint("token-expired", exp=int(time.time()) - 3600)
PY

cat > "$root/pusher.toml" <<EOF
listen = "127.0.0.1:18080"
metrics_listen = "127.0.0.1:19091"
db_path = "$root/garret.db"
signing_key_files = ["$root/signing.key"]
admin_socket = "$root/admin.sock"
puller_endpoint = "http://127.0.0.1:18081"

[limits]
# 5 MiB is S4's minimum part size; small here so the multipart path is
# actually exercised without moving 64 MiB.
part_size = 5242880
max_parts_in_flight = 2
max_concurrent_uploads = 4

[gc]
# Roomy: GC must not evict the corpus out from under the earlier stages.
# The GC stage below restarts the Pusher with a deliberately tiny quota.
quota_bytes = 10737418240
interval_secs = 1
orphan_grace_secs = 0

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

say "starting garret"
if [ -z "${GARRET_BIN:-}" ]; then
  cargo build --quiet -p garret-pusher -p garret-puller -p garret-client \
    -p garret-admin -p garret-bench
fi
for exe in garret garret-pusher garret-puller garret-admin garret-bench; do
  [ -x "$bin/$exe" ] || { echo "missing $bin/$exe" >&2; exit 1; }
done

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

# Deliberately backwards: the Pusher owns the schema, and the Puller must wait
# for it rather than die, serving 503 until /ready says otherwise.
"$bin"/garret-puller "$root/puller.toml" &
"$bin"/garret-pusher "$root/pusher.toml" &
pusher_pid=$!
wait_for pusher "http://127.0.0.1:18080/api/v1/missing-paths"
for i in $(seq 100); do
  curl -sf -o /dev/null "http://127.0.0.1:18081/ready" && break
  [ "$i" = 100 ] && { echo "puller never became ready" >&2; exit 1; }
  sleep 0.3
done

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
# See scripts/e2e-fixture.nix for why these are runCommand derivations rather
# than a bare `derivation` with a bash builder.
export GARRET_E2E_FLAKE="$PWD"
export GARRET_E2E_STAMP
GARRET_E2E_STAMP=$(date +%s)
fixture() {
  nix build --impure --no-link --print-out-paths -f scripts/e2e-fixture.nix "$1"
}
path=$(fixture closure)
hash=$(basename "$path" | cut -c1-32)
leaf=$(nix path-info --recursive "$path" | grep -- '-garret-e2e-leaf$')
echo "root: $path"
echo "leaf: $leaf"

say "pushing the closure with the garret client"
export GARRET_TOKEN
GARRET_TOKEN=$(cat "$root/token")
garret() { "$bin"/garret --config "$root/client.toml" "$@"; }

# --dry-run first, on a closure nothing has uploaded yet: it must report the
# work and then not do it. If it uploaded, the real push below would report
# `deduped` instead of `pushed` and this stage would fail.
garret push --dry-run "$path" | tee "$root/dry.out"
grep -q "2 path(s) in closure, 2 missing" "$root/dry.out"
[ "$(grep -c 'would-push' "$root/dry.out")" = "2" ]

garret push "$path" | tee "$root/push.out"
grep -q "2 path(s) in closure, 2 missing" "$root/push.out"
grep -q "done: 2 pushed, 0 deduped, 0 failed" "$root/push.out"

# Re-pushing must be a no-op: idempotency is normal operation, not an error.
garret push "$path" | tee "$root/push2.out"
grep -q "2 path(s) in closure, 0 missing" "$root/push2.out"

say "multipart: a body larger than several parts"
# Incompressible, so zstd cannot shrink it back under the part size.
head -c 12000000 /dev/urandom > "$root/big.bin"
big=$(nix store add-path "$root/big.bin" --name garret-e2e-big)
garret push "$big" | tee "$root/push-big.out"
grep -q "done: 1 pushed, 0 deduped, 0 failed" "$root/push-big.out"

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
# References must appear by *name*, which a bare hash could not reconstruct --
# and must list both the leaf and the root's own path. The self-reference is
# part of what the signature covers, so rendering it is not cosmetic: omit it
# and nix rebuilds a different fingerprint and rejects the path.
grep -q "^References: .*$(basename "$leaf")" "$root/narinfo"
grep -q "^References: .*$(basename "$path")" "$root/narinfo"

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

say "browse API"
# Anonymous pull must keep working while browse demands a token (spec 04).
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:18081/api/v1/objects")
echo "  browse without a token -> $code"
[ "$code" = "401" ] || { echo "browse must require OIDC, got $code"; exit 1; }

authed() { curl -sf -H "Authorization: Bearer $GARRET_TOKEN" "$@"; }
authed "http://127.0.0.1:18081/api/v1/objects?limit=50" > "$root/objects.json"
python3 -c "
import json, sys
page = json.load(open('$root/objects.json'))
names = sorted(o['name'] for o in page['objects'])
print('  objects:', names)
assert 'garret-e2e-root' in names and 'garret-e2e-leaf' in names, names
"
authed "http://127.0.0.1:18081/api/v1/objects?q=leaf" | grep -q garret-e2e-leaf
authed "http://127.0.0.1:18081/api/v1/objects/$hash" | grep -q '"pushed_by"'

leaf_hash=$(basename "$leaf" | cut -c1-32)
garret tree "$hash" | tee "$root/tree.out"
grep -q "garret-e2e-leaf" "$root/tree.out"
# The leaf's referrer is the root — the reverse index, end to end.
authed "http://127.0.0.1:18081/api/v1/objects/$leaf_hash/referrers" | grep -q "garret-e2e-root"
garret list leaf | grep -q garret-e2e-leaf

say "client UX: discovery, whoami, use, json"
# Discovery must stay anonymous. It is registered after the auth layer, and
# that placement is the only thing keeping it public — if someone reorders the
# router, `garret login` silently breaks for anyone not already logged in.
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/v1/discovery")
echo "  discovery without a token -> $code"
[ "$code" = "200" ] || { echo "discovery must be anonymous, got $code"; exit 1; }
curl -sf "http://127.0.0.1:18080/api/v1/discovery" > "$root/discovery.json"
python3 -c "
import json
d = json.load(open('$root/discovery.json'))
assert d['puller_endpoint'] == 'http://127.0.0.1:18081', d
assert d['oidc']['client_id'] == 'garret-cli', d
assert d['oidc']['audience'] == 'garret', d
assert d['public_keys'] == ['$pubkey'], d
print('  discovery:', d['public_keys'])
"

# whoami proves the token is not merely present but accepted.
garret whoami | tee "$root/whoami.out"
grep -q "authenticated" "$root/whoami.out"
grep -q "http://127.0.0.1:18080" "$root/whoami.out"
garret --json whoami | python3 -c "
import json, sys
w = json.load(sys.stdin)
assert w['audience'] == 'garret', w
assert w['puller_endpoint'] == 'http://127.0.0.1:18081', w
"

# --print must name the Puller (never the Pusher) and the real signing key.
garret use --print | tee "$root/use.out"
grep -q "extra-substituters = http://127.0.0.1:18081" "$root/use.out"
grep -q "extra-trusted-public-keys = $pubkey" "$root/use.out"
grep -q "nix.settings" "$root/use.out"
# Writing is never exercised here: it would edit the invoking user's nix.conf.

say "client UX: push --json emits a well-formed NDJSON stream"
head -c 4096 /dev/urandom > "$root/json.bin"
jsonpath=$(nix store add-path "$root/json.bin" --name garret-e2e-json)
garret --json push "$jsonpath" > "$root/push.ndjson"
python3 -c "
import json
events = [json.loads(l) for l in open('$root/push.ndjson') if l.strip()]
assert events[0]['event'] == 'negotiated', events[0]
assert events[-1]['event'] == 'done', events[-1]
paths = [e for e in events if e['event'] == 'path']
# The terminating summary must agree with the per-path events, or a consumer
# that trusts only one of the two gets a different answer.
assert events[-1]['pushed'] == sum(p['status'] == 'pushed' for p in paths), events
assert events[-1]['failed'] == 0, events
assert events[0]['missing'] == len(paths), (events[0], paths)
print('  events:', [e['event'] for e in events])
"
garret --json list leaf | python3 -c "
import json, sys
# Pass-through: the browse API's own schema, not a client re-shaping of it.
assert 'objects' in json.load(sys.stdin)
"
garret --json tree "$hash" | python3 -c "
import json, sys
assert json.load(sys.stdin)['children'], 'tree must carry the leaf'
"

say "client UX: completions need no config, other commands say how to make one"
# Guards the load-then-dispatch ordering: `login` and `completions` must run on
# a machine that has no config at all, which is the state they exist to fix.
"$bin"/garret --config "$root/absent.toml" completions fish > "$root/garret.fish"
[ -s "$root/garret.fish" ] || { echo "completions must work without a config"; exit 1; }
grep -q "garret" "$root/garret.fish"
if "$bin"/garret --config "$root/absent.toml" list 2>"$root/noconfig.err"; then
  echo "list must fail without a config"; exit 1
fi
grep -q "garret login" "$root/noconfig.err"

say "store watcher"
# The cursor bootstraps at MAX(id), so only paths built *after* this starts
# are pushed — that is the whole point of the bootstrap rule.
"$bin"/garret --config "$root/client.toml" watch-store > "$root/watch.log" 2>&1 &
watch_pid=$!
sleep 2
watched=$(fixture watched)
watched_hash=$(basename "$watched" | cut -c1-32)
echo "  built $watched"
for _ in $(seq 40); do
  curl -sf "http://127.0.0.1:18081/$watched_hash.narinfo" >/dev/null 2>&1 && break
  sleep 0.5
done
kill $watch_pid 2>/dev/null || true
curl -sf "http://127.0.0.1:18081/$watched_hash.narinfo" >/dev/null || {
  echo "watcher never pushed the new path"; sed -n '1,40p' "$root/watch.log"; exit 1; }
echo "  watcher pushed it without being asked"
# .drv paths must never be pushed.
drv_hash=$(basename "$(nix path-info --derivation "$watched" 2>/dev/null || echo x-x)" | cut -c1-32)
if curl -sf "http://127.0.0.1:18081/$drv_hash.narinfo" >/dev/null 2>&1; then
  echo "watcher pushed a .drv, which the filter must exclude"; exit 1
fi

say "admin socket"
admin() { "$bin"/garret-admin --socket "$root/admin.sock" "$@"; }
admin status | tee "$root/status.out"
grep -q "^objects:" "$root/status.out"

# Key management is offline: it must work with no Pusher involved at all.
admin key generate garret-rotate-2 "$root/rotate.key" > "$root/keygen.out"
generated=$(awk '/public key:/ {print $3}' "$root/keygen.out")
shown=$(admin key show "$root/rotate.key")
[ "$generated" = "$shown" ] || { echo "generate and show disagree: $generated vs $shown"; exit 1; }
echo "  offline keygen agrees with key show"
# It must refuse to clobber an existing key rather than destroying signatures.
if admin key generate garret-rotate-2 "$root/rotate.key" 2>/dev/null; then
  echo "key generate overwrote an existing key"; exit 1
fi
echo "  refuses to overwrite an existing key"

# Delete: the operator escape hatch for an object GC would not reach. Uses the
# leaf, whose removal is observable without disturbing the root the GC section
# below still needs.
leaf_hash=$(basename "$leaf" | cut -c1-32)
curl -sf "http://127.0.0.1:18081/$leaf_hash.narinfo" >/dev/null \
  || { echo "leaf should be present before delete"; exit 1; }
admin delete "$leaf_hash" | tee "$root/delete.out"
grep -q "deleted 1 object" "$root/delete.out"
if curl -sf "http://127.0.0.1:18081/$leaf_hash.narinfo" >/dev/null 2>&1; then
  echo "narinfo still served after delete"; exit 1
fi
echo "  deleted object is gone from the Puller"

# A hash that was never there must be reported, not silently counted as a
# successful delete -- otherwise a typo looks like success.
admin delete 00000000000000000000000000000000 | tee "$root/delete-missing.out"
grep -q "deleted 0 object" "$root/delete-missing.out"
grep -q "not in the cache: 00000000000000000000000000000000" "$root/delete-missing.out"
echo "  unknown hash is reported rather than swallowed"

# Re-push it so the GC section still has the closure it expects.
garret push "$path" >/dev/null

say "benchmark harness"
"$bin"/garret-bench --endpoint "http://127.0.0.1:18080" \
  --concurrency 4 --count 12 --json "$root/bench.json" | tail -20
python3 -c "
import json
r = json.load(open('$root/bench.json'))
print('  pushed', r['pushed'], 'failed', r['failed'], 'shed-retries', r['shed_retries'])
assert r['zero_failures'], r
assert r['pushed'] == r['corpus_entries'], r
assert r['uncontended_median_ms'] > 0, 'baseline must actually be measured'
assert r['p99_slowdown'] > 0, 'slowdown must be measured even though it is not a gate'
"
# The same seed must produce the same corpus on any machine.
"$bin"/garret-bench --endpoint "http://127.0.0.1:18080" \
  --concurrency 2 --count 12 --json "$root/bench2.json" >/dev/null
python3 -c "
import json
a, b = json.load(open('$root/bench.json')), json.load(open('$root/bench2.json'))
assert a['corpus_bytes'] == b['corpus_bytes'], (a['corpus_bytes'], b['corpus_bytes'])
print('  same seed, same corpus:', a['corpus_bytes'], 'bytes')
"

say "garbage collection"
# Restart the Pusher under a quota the corpus already exceeds. Only one
# process ever writes the DB, so the old one goes first.
kill $pusher_pid 2>/dev/null || true
wait $pusher_pid 2>/dev/null || true
# 1000 bytes: the 12 MB path must go, the small closure need not — so there
# are survivors whose references can be checked.
sed 's/^quota_bytes = .*/quota_bytes = 1000/; s/^interval_secs = .*/interval_secs = 1\nlow_watermark = 0.5/' \
  "$root/pusher.toml" > "$root/pusher-gc.toml"
grep -q "quota_bytes = 1000" "$root/pusher-gc.toml"
"$bin"/garret-pusher "$root/pusher-gc.toml" &
wait_for pusher "http://127.0.0.1:18080/api/v1/missing-paths"

# GC on demand through the admin socket, rather than waiting on the timer.
admin gc run | tee "$root/gcrun.out"
grep -q "evicted" "$root/gcrun.out"

before=$(metric 19091 garret_gc_usage_bytes)
echo "  usage before: $before (quota 1000)"
for _ in $(seq 30); do
  evicted=$(metric 19091 garret_gc_evicted_objects_total)
  [ "${evicted:-0}" != "0" ] && break
  sleep 1
done
echo "  evicted objects: ${evicted:-0}"
[ "${evicted:-0}" != "0" ] || { echo "GC never evicted despite exceeding quota"; exit 1; }
# The invariant, checked over whatever actually survived: no surviving object
# may reference an object GC removed. Which objects go is GC's business; broken
# closures are not.
authed "http://127.0.0.1:18081/api/v1/objects?limit=500" > "$root/after-gc.json"
pushed="$hash $leaf_hash $watched_hash"
GARRET_PUSHED="$pushed" python3 -c "
import json, os, urllib.request
token = os.environ['GARRET_TOKEN']
survivors = {o['hash'] for o in json.load(open('$root/after-gc.json'))['objects']}
pushed = set(os.environ['GARRET_PUSHED'].split())
print('  survivors:', len(survivors), 'of', len(pushed) + 1, 'pushed')
assert survivors, 'GC evicted everything, including unreferenced roots it should have stopped at'
for h in survivors:
    req = urllib.request.Request(
        f'http://127.0.0.1:18081/api/v1/objects/{h}',
        headers={'Authorization': f'Bearer {token}'})
    obj = json.load(urllib.request.urlopen(req))
    for ref in obj['references']:
        ref_hash = ref[:32]
        if ref_hash in pushed and ref_hash not in survivors:
            raise SystemExit(
                f'closure broken: {obj[\"name\"]} survives but its reference {ref} was evicted')
print('  every surviving object still has its full closure')
"

say "PASS — authenticated push via the client, signed, redirected, and substituted back verified"
