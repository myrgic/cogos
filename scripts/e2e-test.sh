#!/usr/bin/env bash
# e2e-test.sh — End-to-end test for CogOS cold-start flow.
#
# Tests the full lifecycle a new user would experience:
#   1. cogos init      — scaffold a workspace
#   2. cogos serve     — start the daemon
#   3. health check    — verify the daemon is running
#   4. context query   — verify the API returns valid data
#   5. shutdown        — clean exit
#
# Exit 0 on success, 1 on any failure.
# Designed to run inside a container (see Dockerfile) or locally.

set -euo pipefail

COGOS="${COGOS_BIN:-cogos}"
WORKSPACE="${E2E_WORKSPACE:-/tmp/e2e-workspace}"
PORT="${E2E_PORT:-5299}"
TIMEOUT="${E2E_TIMEOUT:-10}"

pass=0
fail=0

check() {
    local name="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo "  PASS  $name"
        pass=$((pass + 1))
    else
        echo "  FAIL  $name"
        fail=$((fail + 1))
    fi
}

check_output() {
    local name="$1"
    local expected="$2"
    local url="$3"
    local body
    body=$(curl -sf "$url" 2>/dev/null || echo "CURL_FAILED")
    if echo "$body" | grep -q "$expected"; then
        echo "  PASS  $name"
        pass=$((pass + 1))
    else
        echo "  FAIL  $name (expected '$expected', got: $body)"
        fail=$((fail + 1))
    fi
}

cleanup() {
    if [ -n "${DAEMON_PID:-}" ]; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKSPACE"
}
trap cleanup EXIT

echo "=== CogOS E2E Test ==="
echo "Binary:    $COGOS"
echo "Workspace: $WORKSPACE"
echo "Port:      $PORT"
echo ""

# ── Phase 1: Init ─────────────────────────────────────────────────────────────

echo "Phase 1: Init"
$COGOS init --workspace "$WORKSPACE" 2>&1 | sed 's/^/  /'

check "workspace dir exists"      test -d "$WORKSPACE/.cog"
check "config dir exists"         test -d "$WORKSPACE/.cog/config"
check "memory dirs exist"         test -d "$WORKSPACE/.cog/mem/semantic"
check "identity card exists"      test -f "$WORKSPACE/.cog/agents/identities/identity_cogos.md"
check "kernel.yaml exists"        test -f "$WORKSPACE/.cog/config/kernel.yaml"
check "providers.yaml exists"     test -f "$WORKSPACE/.cog/config/providers.yaml"
check "VERSION exists"            test -f "$WORKSPACE/.cog/VERSION"

# Idempotency: run init again — should not fail or overwrite.
INIT2_OUT=$($COGOS init --workspace "$WORKSPACE" 2>&1)
if echo "$INIT2_OUT" | grep -q "already existed"; then
    echo "  PASS  init is idempotent"
    pass=$((pass + 1))
else
    echo "  FAIL  init is idempotent"
    fail=$((fail + 1))
fi

echo ""

# ── Phase 2: Serve ────────────────────────────────────────────────────────────

echo "Phase 2: Serve"
$COGOS serve --workspace "$WORKSPACE" --port "$PORT" &
DAEMON_PID=$!

# Wait for the daemon to be ready.
ready=false
for i in $(seq 1 "$TIMEOUT"); do
    if curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done

if [ "$ready" = "true" ]; then
    echo "  PASS  daemon started (${i}s)"
    ((pass++))
else
    echo "  FAIL  daemon did not start within ${TIMEOUT}s"
    ((fail++))
    echo ""
    echo "=== RESULT: $pass passed, $fail failed ==="
    exit 1
fi

echo ""

# ── Phase 3: API Checks ──────────────────────────────────────────────────────

echo "Phase 3: API"
check_output "health returns ok"         '"status":"ok"'       "http://localhost:$PORT/health"
check_output "health has identity"       '"identity":"CogOS"'  "http://localhost:$PORT/health"
check_output "context returns state"     '"state":"receptive"' "http://localhost:$PORT/v1/context"
check_output "context has nucleus"       '"nucleus":"CogOS"'   "http://localhost:$PORT/v1/context"

# ── Phase 3b: MCP Transport Probe ────────────────────────────────────────────
# Exercises the wire protocol the daemon actually serves to agents:
# initialize → tools/list → one tools/call. Catches dead wiring (a tool
# registered in source but absent from the live daemon) and transport
# regressions that direct-method unit tests structurally cannot see.
#
# /mcp is gated on every method by the write-route grant-auth middleware
# (serve_grant_auth.go) — the daemon mints/recovers its own node-root
# identity grant at boot (ensureNodeRootGrant, boot_node_root_grant.go) as
# the zero-paste bootstrap credential for exactly this case. This probe does
# the REAL bootstrap a live consumer (Claude Code, THESEUS, the dashboard)
# would do: read the node-root token from the 0600 vault file the daemon
# writes at boot (~/.cog/vault/node-root-grant) and attach it as
# X-Cogos-Grant on every /mcp call below. It is a file read rather than an
# HTTP GET because /v1/identity/* is now gated on every method (ledger L03) —
# a caller holding no grant cannot fetch one over loopback, which is the
# point; the vault file is the designated bootstrap store for that case.
# Mirrors internal/testkernel/testkernel.go's NodeRootGrantToken/ListTools —
# keep the two consistent if either changes.

echo "Phase 3b: MCP transport"
MCP_URL="http://localhost:$PORT/mcp"
MCP_H_CT='Content-Type: application/json'
MCP_H_ACC='Accept: application/json, text/event-stream'

GRANT_VAULT_FILE="$HOME/.cog/vault/node-root-grant"
GRANT_TOKEN=$(cat "$GRANT_VAULT_FILE" 2>/dev/null | tr -d '[:space:]')
if [ -n "${GRANT_TOKEN:-}" ]; then
    echo "  PASS  node-root grant token acquired"
    pass=$((pass + 1))
else
    # Deliberately does NOT echo any token material on failure — only the
    # path that was consulted.
    echo "  FAIL  node-root grant token acquired (no readable token at $GRANT_VAULT_FILE)"
    fail=$((fail + 1))
fi
GRANT_HEADER_ARG=""
if [ -n "${GRANT_TOKEN:-}" ]; then
    GRANT_HEADER_ARG="X-Cogos-Grant: ${GRANT_TOKEN}"
fi

MCP_SID=$(curl -s -D - -o /dev/null -m 10 -X POST "$MCP_URL" -H "$MCP_H_CT" -H "$MCP_H_ACC" ${GRANT_HEADER_ARG:+-H "$GRANT_HEADER_ARG"}     -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"e2e","version":"1"}}}'     | grep -i '^mcp-session-id:' | tr -d '\r' | awk '{print $2}')
if [ -n "${MCP_SID:-}" ]; then
    echo "  PASS  mcp initialize (session $MCP_SID)"
    pass=$((pass + 1))
else
    echo "  FAIL  mcp initialize (no session id)"
    fail=$((fail + 1))
fi

mcp_post() {
    curl -s -m 15 -X POST "$MCP_URL" -H "$MCP_H_CT" -H "$MCP_H_ACC" \
        ${MCP_SID:+-H "Mcp-Session-Id: ${MCP_SID}"} \
        ${GRANT_HEADER_ARG:+-H "$GRANT_HEADER_ARG"} -d "$1"
}
mcp_post '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null 2>&1 || true

TOOLS_OUT=$(mcp_post '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
# Golden tool set: one per wired subsystem, so a dropped registration chain
# (providers_wire.go / z_*_wire.go) fails loudly here.
for tool in cog_get_state cog_search_memory cog_search_conversations cog_list_sessions cog_read_cogdoc; do
    if echo "$TOOLS_OUT" | grep -q "\"$tool\""; then
        echo "  PASS  tools/list has $tool"
        pass=$((pass + 1))
    else
        echo "  FAIL  tools/list has $tool"
        fail=$((fail + 1))
    fi
done

# Registration is not function. tools/list saw a REGISTERED cog_search_memory
# throughout ledger L01, while every non-Makefile build shipped without the
# fts5 tag and multi-word queries silently returned 0 rows via the substring
# grep fallback. So call the tool — but call it in a way that can actually
# tell the two apart.
#
# Three mechanics matter here; each one was verified against a live daemon,
# and getting any of them wrong makes this check vacuous:
#
#  1. Only *.cog.md files are indexed. mem_watcher (internal/engine/
#     mem_watcher.go) indexes on a Create/Write event whose target matches
#     that suffix. A plain .md file is invisible to search.
#  2. The file must be written AFTER the daemon is up, because indexing is
#     event-driven. Seeding before boot indexes nothing: the "index built
#     docs=N" line at startup is a DIFFERENT, in-memory CogDoc index, not the
#     constellation DB that cog_search_memory reads.
#  3. The query terms must be SCATTERED, never a contiguous phrase. FTS5
#     evaluates multi-word queries as an AND over terms and matches
#     regardless of position; the grep fallback looks for the literal phrase,
#     finds nothing, and returns a well-formed EMPTY result. A check that
#     only asserts "no JSON-RPC error" passes straight through the
#     degradation it is meant to catch.
#
# Negative control, run against a live daemon on both builds:
#   -tags fts5 -> {"count":1,...}   (matches the probe doc)
#   untagged   -> {"count":0,"results":null}, no error
cat > "$WORKSPACE/.cog/mem/semantic/e2e-fts5-probe.cog.md" <<'SEED'
---
type: insight
status: active
confidence: high
description: e2e fts5 probe document
---

# e2e fts5 probe

zarquon appears here.
Some intervening prose so the terms are not adjacent.
frobnicate appears here, well away from the first term.
More filler.
bletcherous appears here, at the end.
SEED

# Let the watcher pick it up and index it.
sleep 3

SEARCH_OUT=$(mcp_post '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"cog_search_memory","arguments":{"query":"zarquon frobnicate bletcherous","limit":5}}}')
if echo "$SEARCH_OUT" | grep -q '"error"'; then
    echo "  FAIL  cog_search_memory returned an error: $(echo "$SEARCH_OUT" | head -c 200)"
    fail=$((fail + 1))
elif echo "$SEARCH_OUT" | grep -q 'e2e-fts5-probe'; then
    echo "  PASS  cog_search_memory matched a scattered-term query (FTS5 live)"
    pass=$((pass + 1))
else
    echo "  FAIL  cog_search_memory found nothing for a multi-word query whose terms all"
    echo "        exist in one indexed document. This is the ledger L01 signature: the"
    echo "        FTS path failed and SearchMemory fell back to an unranked substring"
    echo "        grep. Check this binary was built with -tags fts5 AND CGO_ENABLED=1."
    echo "        response: $(echo "$SEARCH_OUT" | head -c 200)"
    fail=$((fail + 1))
fi

CALL_OUT=$(mcp_post '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"cog_get_state","arguments":{}}}')
if echo "$CALL_OUT" | grep -q '"result"' && ! echo "$CALL_OUT" | grep -q '"isError":true'; then
    echo "  PASS  tools/call cog_get_state"
    pass=$((pass + 1))
else
    echo "  FAIL  tools/call cog_get_state (got: $(echo "$CALL_OUT" | head -c 200))"
    fail=$((fail + 1))
fi

# Version endpoint.
VERSION_OUT=$($COGOS version 2>&1)
if echo "$VERSION_OUT" | grep -q "cogos.*build="; then
    echo "  PASS  version command works"
    pass=$((pass + 1))
else
    echo "  FAIL  version command works (got: $VERSION_OUT)"
    fail=$((fail + 1))
fi

echo ""

# ── Phase 4: Shutdown ─────────────────────────────────────────────────────────

echo "Phase 4: Shutdown"
kill "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null || true
DAEMON_PID=""

# Verify it's actually down.
sleep 1
if curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1; then
    echo "  FAIL  daemon still running after kill"
    ((fail++))
else
    echo "  PASS  daemon stopped cleanly"
    ((pass++))
fi

echo ""
echo "=== RESULT: $pass passed, $fail failed ==="

if [ "$fail" -gt 0 ]; then
    exit 1
fi
