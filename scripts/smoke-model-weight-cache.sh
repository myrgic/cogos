#!/usr/bin/env bash
# smoke-model-weight-cache.sh — Two-node model-weight block cache smoke test.
#
# Proves the cross-node model-weight cache end to end against TWO real cogos
# daemons (no httptest, no loopback in-process fakes): node A is the
# authoritative source (the authoritative role), node B is the cache (the
# caching role). Exercises every behavior from the design review:
#
#   1. Build binary + init two fresh workspaces with blob stores
#   2. Start two daemons on alternate ports
#   3. Generate a model manifest via `cogos blobs manifest`
#   4. PUT every shard into node A over the streaming /v1/blocks/{hash} API
#   5. COLD hydrate: node B pulls all shards from node A, materializes a dir
#   6. Integrity: materialized files are byte-identical to the originals
#   7. WARM hydrate: re-run is all cache hits, zero bytes transferred
#   8. DRIFT: re-publish one shard with new content; only that shard re-pulls
#   9. PROMOTE: --promote defers when remote reachable, claims local
#      authority (with split-brain warning) when remote is unreachable
#  10. INTEGRITY REJECTION: a PUT whose bytes do not match the claimed hash
#      is refused with HTTP 409 (rolling SHA-256 on the streaming PUT path)
#  11. THROUGHPUT: a 512MB blob (larger than the retired 500MB PUT cap)
#      streams through PUT+GET byte-identical
#  12. Shutdown and verify clean exit
#
# Prerequisites:
#   - Go toolchain installed
#   - curl, shasum, python3 on PATH
#
# Usage:
#   bash scripts/smoke-model-weight-cache.sh
#   SMOKE_PORT_A=7940 SMOKE_PORT_B=7941 bash scripts/smoke-model-weight-cache.sh
#   SMOKE_BIG_MB=0 bash scripts/smoke-model-weight-cache.sh   # skip 512MB throughput

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

COGOS_BIN="${COGOS_BIN:-/tmp/cogos-smoke-mwc}"
WS_A="${SMOKE_WS_A:-/tmp/smoke-mwc-nodeA}"
WS_B="${SMOKE_WS_B:-/tmp/smoke-mwc-nodeB}"
PORT_A="${SMOKE_PORT_A:-6932}"
PORT_B="${SMOKE_PORT_B:-6933}"
MODELDIR="${SMOKE_MODELDIR:-/tmp/smoke-mwc-model}"
DEST="${SMOKE_DEST:-/tmp/smoke-mwc-dest}"
BIG_MB="${SMOKE_BIG_MB:-512}"
MODEL_ID="${SMOKE_MODEL_ID:-google/gemma-smoke-test}"
BASE_A="http://127.0.0.1:$PORT_A"
BASE_B="http://127.0.0.1:$PORT_B"

pass=0
fail=0
PIDS=()

note()  { printf '\n=== %s ===\n' "$1"; }
ok()    { printf '  PASS: %s\n' "$1"; pass=$((pass+1)); }
bad()   { printf '  FAIL: %s\n' "$1"; fail=$((fail+1)); }

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    rm -rf "$WS_A" "$WS_B" "$MODELDIR" "$DEST" "$DEST"2 "$DEST"3 \
           /tmp/smoke-mwc-manifest.json /tmp/smoke-mwc-manifest-v2.json \
           /tmp/smoke-mwc-big.bin /tmp/smoke-mwc-big-back.bin 2>/dev/null || true
}
trap cleanup EXIT

sha() { shasum -a 256 "$1" | awk '{print $1}'; }

# ---------------------------------------------------------------------------
note "1. Build binary"
go build -tags fts5 -o "$COGOS_BIN" ./cmd/cogos
[ -x "$COGOS_BIN" ] && ok "built $COGOS_BIN" || { bad "build"; exit 1; }

note "2. Init two workspaces + blob stores"
rm -rf "$WS_A" "$WS_B"; mkdir -p "$WS_A" "$WS_B"
"$COGOS_BIN" init --workspace "$WS_A" >/dev/null 2>&1
"$COGOS_BIN" init --workspace "$WS_B" >/dev/null 2>&1
( cd "$WS_A" && "$COGOS_BIN" blobs init >/dev/null 2>&1 )
( cd "$WS_B" && "$COGOS_BIN" blobs init >/dev/null 2>&1 )
ok "workspaces initialized"

note "3. Start two daemons"
( cd "$WS_A" && "$COGOS_BIN" serve --workspace "$WS_A" --port "$PORT_A" --bind 127.0.0.1 >/tmp/smoke-mwc-nodeA.log 2>&1 ) &
PIDS+=($!)
( cd "$WS_B" && "$COGOS_BIN" serve --workspace "$WS_B" --port "$PORT_B" --bind 127.0.0.1 >/tmp/smoke-mwc-nodeB.log 2>&1 ) &
PIDS+=($!)
sleep 4
curl -s --max-time 5 "$BASE_A/health" | grep -q '"status":"ok"' && ok "node A up ($BASE_A)" || bad "node A health"
curl -s --max-time 5 "$BASE_B/health" | grep -q '"status":"ok"' && ok "node B up ($BASE_B)" || bad "node B health"

note "4. Build a model directory + manifest"
rm -rf "$MODELDIR"; mkdir -p "$MODELDIR"
head -c 8388608 /dev/urandom > "$MODELDIR/model-00001-of-00002.safetensors"
head -c 6291456 /dev/urandom > "$MODELDIR/model-00002-of-00002.safetensors"
echo '{"architectures":["GemmaForCausalLM"]}' > "$MODELDIR/config.json"
echo '{"version":"1.0"}' > "$MODELDIR/tokenizer.json"
( cd "$WS_A" && "$COGOS_BIN" blobs manifest "$MODELDIR" --model-id "$MODEL_ID" ) > /tmp/smoke-mwc-manifest.json
python3 -m json.tool /tmp/smoke-mwc-manifest.json >/dev/null && ok "manifest is valid JSON" || bad "manifest JSON"

note "5. PUT every shard into node A (streaming /v1/blocks/{hash})"
put_ok=1
for f in config.json model-00001-of-00002.safetensors model-00002-of-00002.safetensors tokenizer.json; do
    h=$(sha "$MODELDIR/$f")
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 30 -X PUT \
        --data-binary @"$MODELDIR/$f" -H "Content-Type: application/octet-stream" \
        "$BASE_A/v1/blocks/$h")
    [ "$code" = "201" ] || put_ok=0
done
[ "$put_ok" = "1" ] && ok "all shards stored on node A (HTTP 201)" || bad "shard PUT"

note "6. COLD hydrate node B from node A + materialize"
rm -rf "$DEST"
rep=$( cd "$WS_B" && "$COGOS_BIN" blobs remote-hydrate /tmp/smoke-mwc-manifest.json --from "$BASE_A" --target "$DEST" )
echo "$rep" | grep -qE "pulled:[[:space:]]*4" && ok "COLD: 4 shards pulled" || bad "COLD pulled count"

note "7. Integrity: materialized == originals"
mism=0
for f in config.json model-00001-of-00002.safetensors model-00002-of-00002.safetensors tokenizer.json; do
    [ "$(sha "$MODELDIR/$f")" = "$(sha "$DEST/$f")" ] || mism=1
done
[ "$mism" = "0" ] && ok "all 4 materialized files byte-identical" || bad "integrity mismatch"

note "8. WARM hydrate (cache hits, zero transfer)"
rep=$( cd "$WS_B" && "$COGOS_BIN" blobs remote-hydrate /tmp/smoke-mwc-manifest.json --from "$BASE_A" --target "$DEST"2 )
echo "$rep" | grep -qE "already local:[[:space:]]*4" && \
echo "$rep" | grep -qE "pulled:[[:space:]]*0" && ok "WARM: 4 cache hits, 0 pulled" || bad "WARM cache hits"

note "9. DRIFT: re-publish one shard, only it re-pulls"
head -c 6291456 /dev/urandom > "$MODELDIR/model-00002-of-00002.safetensors"
( cd "$WS_A" && "$COGOS_BIN" blobs manifest "$MODELDIR" --model-id "$MODEL_ID" ) > /tmp/smoke-mwc-manifest-v2.json
newh=$(sha "$MODELDIR/model-00002-of-00002.safetensors")
curl -s -o /dev/null -X PUT --data-binary @"$MODELDIR/model-00002-of-00002.safetensors" \
    -H "Content-Type: application/octet-stream" "$BASE_A/v1/blocks/$newh"
rep=$( cd "$WS_B" && "$COGOS_BIN" blobs remote-hydrate /tmp/smoke-mwc-manifest-v2.json --from "$BASE_A" --target "$DEST" )
echo "$rep" | grep -qE "already local:[[:space:]]*3" && \
echo "$rep" | grep -qE "pulled:[[:space:]]*1" && ok "DRIFT: 3 hits, 1 re-pulled" || bad "DRIFT delta"

note "10. PROMOTE reachability gate"
rep=$( cd "$WS_B" && "$COGOS_BIN" blobs remote-hydrate /tmp/smoke-mwc-manifest.json --from "$BASE_A" --target "$DEST"3 --promote 2>&1 )
echo "$rep" | grep -qiE "not promoted|reachable" && ok "PROMOTE: defers when remote reachable" || bad "PROMOTE reachable"
rep=$( cd "$WS_B" && "$COGOS_BIN" blobs remote-hydrate /tmp/smoke-mwc-manifest.json --from "http://127.0.0.1:1" --target "$DEST"3 --promote 2>&1 )
echo "$rep" | grep -qiE "self-promoted|local authority" && \
echo "$rep" | grep -qiE "reconcile|split-brain" && ok "PROMOTE: claims authority + split-brain warning when unreachable" || bad "PROMOTE unreachable"

note "11. INTEGRITY REJECTION on PUT"
fakehash=$(echo -n "claimed identity" | shasum -a 256 | awk '{print $1}')
code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT --data-binary "bytes that do not match the hash" \
    -H "Content-Type: application/octet-stream" "$BASE_A/v1/blocks/$fakehash")
[ "$code" = "409" ] && ok "corrupt PUT refused (HTTP 409)" || bad "integrity rejection (got HTTP $code)"

note "12. THROUGHPUT: ${BIG_MB}MB blob (> retired 500MB cap)"
if [ "$BIG_MB" -gt 0 ]; then
    bytes=$((BIG_MB * 1024 * 1024))
    head -c "$bytes" /dev/urandom > /tmp/smoke-mwc-big.bin
    bigh=$(sha /tmp/smoke-mwc-big.bin)
    pcode=$(curl -s -o /dev/null -w "%{http_code}" -X PUT --data-binary @/tmp/smoke-mwc-big.bin \
        -H "Content-Type: application/octet-stream" "$BASE_A/v1/blocks/$bigh")
    curl -s -o /tmp/smoke-mwc-big-back.bin "$BASE_A/v1/blocks/$bigh"
    backh=$(sha /tmp/smoke-mwc-big-back.bin)
    [ "$pcode" = "201" ] && [ "$backh" = "$bigh" ] && ok "${BIG_MB}MB PUT+GET byte-identical (no 500MB cap)" || bad "${BIG_MB}MB round-trip"
else
    echo "  (skipped: SMOKE_BIG_MB=0)"
fi

# ---------------------------------------------------------------------------
note "RESULT"
printf '  %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = "0" ] || exit 1
