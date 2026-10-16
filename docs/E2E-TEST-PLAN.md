# CogOS E2E Integration Test Plan

This document defines end-to-end integration test specifications for Wave 0-2 capabilities in the `cogos` kernel. It is a **test plan/spec**, not test implementation code.

## Global Conventions

- Kernel binary: `./cogos`
- Default test port examples: `5299` (override per test to avoid collisions)
- Test workspace root: `/tmp/cogos-e2e-*`
- Ledger path pattern: `.cog/ledger/<session_id>/events.jsonl`
- Proprioceptive log path: `.cog/run/proprioceptive.jsonl`
- Sync inbox path: `.cog/sync/inbox/`
- Unless stated otherwise, all API calls target `http://localhost:$PORT`

---

## 1) Standalone Kernel Test

Validates baseline kernel lifecycle and API behavior (current `scripts/e2e-test.sh` coverage).

### Prerequisites

- `cogos` binary built and executable
- `curl` available
- No process currently bound to the selected test port

### Setup Commands

```bash
export COGOS_BIN=./cogos
export E2E_WORKSPACE=/tmp/cogos-e2e-standalone
export E2E_PORT=5299
rm -rf "$E2E_WORKSPACE"
"$COGOS_BIN" init --workspace "$E2E_WORKSPACE"
"$COGOS_BIN" serve --workspace "$E2E_WORKSPACE" --port "$E2E_PORT" &
KERNEL_PID=$!
```

### Test Steps

1. Poll `GET /health` until ready.
2. Verify `/health` includes:
   - `status: "ok"`
   - `identity` (expected default: `CogOS`)
   - `state` (expected initial: `receptive`)
3. Send `POST /v1/chat/completions` with a minimal user message.
4. Verify the request created a ledger event in `.cog/ledger/*/events.jsonl` (event type `cogblock.ingest`).
5. Verify context assembly includes `nucleus`:
   - `GET /v1/context` returns `nucleus` field
   - `nucleus` equals identity (`CogOS` in default scaffold)
6. Stop kernel (`kill $KERNEL_PID`) and confirm `/health` is no longer reachable.

### Expected Outcomes

- Kernel starts from clean workspace without fatal errors.
- Health endpoint returns identity and runtime state.
- Chat completion endpoint accepts OpenAI-compatible request.
- Ledger records interaction as append-only hash-chained event.
- Context endpoint includes `nucleus` identity.
- Shutdown is graceful (process exits; port is released).

### Cleanup

```bash
kill "$KERNEL_PID" 2>/dev/null || true
wait "$KERNEL_PID" 2>/dev/null || true
rm -rf "$E2E_WORKSPACE"
```

---

## 2) Gemma 4 Integration Test (Ollama Required)

Validates local-provider routing to `gemma4:e4b` and tool-call hallucination gate behavior.

### Prerequisites

- Ollama daemon running at `http://localhost:11434`
- `gemma4:e4b` model available in Ollama
- `cogos` binary built and executable

### Setup Commands

```bash
export COGOS_BIN=./cogos
export E2E_WORKSPACE=/tmp/cogos-e2e-gemma
export E2E_PORT=5300
rm -rf "$E2E_WORKSPACE"
"$COGOS_BIN" init --workspace "$E2E_WORKSPACE"
"$COGOS_BIN" serve --workspace "$E2E_WORKSPACE" --port "$E2E_PORT" &
KERNEL_PID=$!
```

### Test Steps

1. Confirm provider readiness through successful `POST /v1/chat/completions` using a simple prompt (e.g. “Reply with the word READY”).
2. Verify returned completion content is non-empty and attributed to local model routing.
3. Send a tool-enabled request designed to provoke an invalid/unsupported tool call (hallucinated function name or malformed arguments).
4. Verify hallucination gate activates by checking one of:
   - Provider is re-called with rejection feedback and tool call is not executed.
   - Result avoids unsafe tool execution and returns normal content or corrected output.
5. Verify proprioceptive logging captured validation failure in `.cog/run/proprioceptive.jsonl` with event `tool_call_rejected` and reason details.

### Expected Outcomes

- Kernel uses Gemma 4 E4B as default local inference path.
- Normal prompt returns successful completion.
- Invalid model-emitted tool call is blocked by validation gate.
- Proprioceptive log includes rejection telemetry (`provider`, `tool_name`, `reason`).

### Cleanup

```bash
kill "$KERNEL_PID" 2>/dev/null || true
wait "$KERNEL_PID" 2>/dev/null || true
rm -rf "$E2E_WORKSPACE"
```

---

## 3) Digestion Pipeline Test

Validates StreamTailer-based ingestion from JSONL and process-state gating.

### Prerequisites

- `cogos` binary built and executable
- Writable test JSONL file path
- Adapter format chosen to match configured tailer (`claudecode`)

### Setup Commands

```bash
export COGOS_BIN=./cogos
export E2E_WORKSPACE=/tmp/cogos-e2e-digest
export E2E_PORT=5301
export DIGEST_FILE=/tmp/cogos-e2e-digest-input.jsonl
rm -rf "$E2E_WORKSPACE" "$DIGEST_FILE"
"$COGOS_BIN" init --workspace "$E2E_WORKSPACE"
touch "$DIGEST_FILE"
```

Update `"$E2E_WORKSPACE/.cog/config/kernel.yaml"` to include:

```yaml
digest_paths:
  claudecode: /tmp/cogos-e2e-digest-input.jsonl
```

Then start kernel:

```bash
"$COGOS_BIN" serve --workspace "$E2E_WORKSPACE" --port "$E2E_PORT" &
KERNEL_PID=$!
```

### Test Steps

1. Append valid JSONL lines representing digest input events to `$DIGEST_FILE`.
2. Wait for poll interval and ingestion cycle.
3. Verify CogBlock ingestion records appear in ledger (`cogblock.ingest`) with provenance fields reflecting adapter/source channel.
4. Force kernel into/observe non-ingesting state (Consolidating or Dormant), append additional lines, and verify these are not ingested until state returns to Receptive/Active.
5. Return to Receptive/Active, append another line, and verify ingestion resumes.

### Expected Outcomes

- Tailer reads new lines and emits normalized blocks.
- Ledger contains ingestion events with correct provenance metadata.
- Ingestion is state-gated: accepted in `receptive`/`active`, dropped or deferred outside those states.
- No duplicate ingestion for unchanged lines.

### Cleanup

```bash
kill "$KERNEL_PID" 2>/dev/null || true
wait "$KERNEL_PID" 2>/dev/null || true
rm -rf "$E2E_WORKSPACE" "$DIGEST_FILE"
```

---

## 4) Constellation Bridge Test (Constellation Binary Required)

Validates heartbeat bridge payload and SyncWatcher envelope handling.

### Prerequisites

- `cogos` binary built and executable
- Constellation binary installed and runnable
- Constellation test configuration available for local node

### Setup Commands

```bash
export COGOS_BIN=./cogos
export E2E_WORKSPACE=/tmp/cogos-e2e-constellation
export E2E_PORT=5302
rm -rf "$E2E_WORKSPACE"
"$COGOS_BIN" init --workspace "$E2E_WORKSPACE"
mkdir -p "$E2E_WORKSPACE/.cog/sync/inbox"
```

Start constellation node using test config, then start kernel bound to that constellation integration.

### Test Steps

1. Wait for dormant heartbeat cycle.
2. Verify bridge heartbeat payload includes `KernelHeartbeatPayload` fields:
   - `process_state`
   - `field_size`
   - `coherence_fingerprint`
   - `nucleus_fingerprint`
   - `timestamp`
3. Create a valid `SyncEnvelope` JSON file in `.cog/sync/inbox/`.
4. Verify `SyncWatcher` detects envelope and performs structural validation.
5. Verify validation result marks envelope as valid (or invalid with explicit reason if malformed variant is tested).

### Expected Outcomes

- Kernel emits bridge heartbeat during dormant cadence.
- Heartbeat payload conforms to expected schema.
- Sync inbox drop is discovered by watcher.
- Envelope validation behavior matches schema rules (version, IDs, hash format, RFC3339 timestamp, kind, signature).

### Cleanup

```bash
kill "$KERNEL_PID" 2>/dev/null || true
wait "$KERNEL_PID" 2>/dev/null || true
# stop constellation process(es)
rm -rf "$E2E_WORKSPACE"
```

---

## 5) Full Stack Test (Kernel + mod3 + Constellation)

Validates cross-repo/system integration and attention signaling through full runtime topology.

### Prerequisites

- `cogos` binary built and executable
- `mod3` process/binary available and configured
- Constellation binary running with compatible node identity/trust config
- Shared test workspace and interoperable endpoint configuration

### Setup Commands

1. Initialize shared workspace for stack test.
2. Start constellation service(s).
3. Start `mod3` with test speech input/output adapters enabled.
4. Start kernel (`cogos serve`) with matching integration config and dedicated test port.

### Test Steps

1. Confirm all three processes are healthy.
2. Send a controlled event through one subsystem (e.g. mod3 speech utterance).
3. Verify event propagates across stack:
   - mod3 emits speech event
   - kernel receives/records corresponding perturbation or attention event
   - constellation-facing state remains coherent/available
4. Verify kernel produces attention updates (state transition or salience/context impact) attributable to mod3 speech trigger.
5. Verify ledger captures cross-system interaction trail for traceability.

### Expected Outcomes

- Kernel, mod3, and constellation communicate without deadlock or protocol mismatch.
- Speech-originated perturbations reach kernel and trigger attention behavior.
- Observable cross-system telemetry/ledger evidence exists for the same interaction window.

### Cleanup

1. Stop kernel.
2. Stop mod3.
3. Stop constellation services.
4. Remove test workspace and temporary runtime artifacts.

---

## Exit Criteria

The E2E plan is considered passing when all five scenarios complete with expected outcomes and no unresolved critical failures in:

- API availability and correctness
- ledger/provenance integrity
- tool-call safety gate behavior
- digestion state gating
- constellation bridge/sync envelope handling
- cross-system signaling in full-stack runtime
