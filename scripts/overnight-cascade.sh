#!/usr/bin/env bash
# overnight-cascade.sh — Run inference cascades overnight using only local models.
#
# Supervisor: Codex (gpt-5.3-codex, low cost) OR gemma4:26b (free)
# Agents: gemma4:26b / gemma4:e4b / qwen3.5:9b via Ollama (free)
# Claude credits used: ZERO
#
# The supervisor designs experiments, spawns agents, analyzes chains.
# Agents explore the workspace read-only and append to chain files.
# Everything is recorded. Run until interrupted.
#
# Usage:
#   bash scripts/overnight-cascade.sh                          # local supervisor (free)
#   SUPERVISOR=codex bash scripts/overnight-cascade.sh         # Codex supervisor (low cost)
#   MAX_CYCLES=10 bash scripts/overnight-cascade.sh            # limit cycles
#   QUESTION="custom question" bash scripts/overnight-cascade.sh

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────

SUPERVISOR="${SUPERVISOR:-local}"  # "local" (gemma4:26b) or "codex"
WORKSPACE="${CASCADE_WORKSPACE:-$HOME/workspaces/cog}"
COGOS_WORKSPACE="${CASCADE_COGOS:-${MYRGIC_REPOS_ROOT:-$HOME/workspaces/myrgic}/cogos}"
OLLAMA="${OLLAMA_HOST:-http://localhost:11434}"
MAX_CYCLES="${MAX_CYCLES:-999}"
CYCLE_PAUSE="${CYCLE_PAUSE:-60}"  # seconds between cycles
AGENT_TURNS="${AGENT_TURNS:-8}"
CHAIN_DIR="${CHAIN_DIR:-/tmp/cascade-overnight}"
MODELS="${MODELS:-gemma4:26b gemma4:e4b qwen3.5:9b}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Default research questions (supervisor can override)
DEFAULT_QUESTIONS=(
  "How does the foveated context engine decide which documents to include? Trace the scoring pipeline."
  "What are the key differences between the v2 and v3 kernel architectures? Find specific code changes."
  "How does the TRM scoring work? Find the Mamba SSM implementation and trace its data flow."
  "What is the tool-call hallucination gate? How does it validate tool calls before execution?"
  "How does the router decide which provider to use? Trace the sovereignty gradient implementation."
  "What consolidation happens during Dormant state? Find the consolidation action code."
  "How does the digestion pipeline work? Trace from JSONL tailer to CogBlock normalization."
  "What is the ConstellationBridge interface? How does the kernel communicate with constellation?"
)

mkdir -p "$CHAIN_DIR/chains" "$CHAIN_DIR/hypotheses" "$CHAIN_DIR/syntheses"

# ── Helpers ──────────────────────────────────────────────────────────────────

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$CHAIN_DIR/supervisor.log"; }

ollama_generate() {
    local model="$1" prompt="$2" temp="${3:-0.7}"
    curl -sf "$OLLAMA/api/generate" \
        -d "{\"model\":\"$model\",\"prompt\":$(echo "$prompt" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'),\"stream\":false,\"options\":{\"temperature\":$temp}}" \
        2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('response',''))" 2>/dev/null
}

codex_generate() {
    local prompt="$1"
    codex exec -m "gpt-5.6-terra" \
        --config model_reasoning_effort="low" \
        --sandbox read-only --skip-git-repo-check \
        -C "$COGOS_WORKSPACE" \
        "$prompt" 2>/dev/null
}

supervisor_think() {
    local prompt="$1"
    if [ "$SUPERVISOR" = "codex" ]; then
        codex_generate "$prompt"
    else
        ollama_generate "gemma4:26b" "$prompt" 0.4
    fi
}

run_ralph() {
    local model="$1" question="$2" chain_file="$3" temp="${4:-0.7}"
    python3 "$COGOS_WORKSPACE/scripts/sandbox-agent.py" \
        --model "$model" \
        --sandbox read-only \
        --max-turns "$AGENT_TURNS" \
        --workspace "$WORKSPACE" \
        "$question" > "$chain_file" 2>&1
}

# ── Main Loop ────────────────────────────────────────────────────────────────

log "╔══════════════════════════════════════════════════╗"
log "║  Overnight Inference Cascade                    ║"
log "╠══════════════════════════════════════════════════╣"
log "║  Supervisor:  $SUPERVISOR"
log "║  Models:      $MODELS"
log "║  Workspace:   $WORKSPACE"
log "║  Chain dir:   $CHAIN_DIR"
log "║  Max cycles:  $MAX_CYCLES"
log "║  Agent turns: $AGENT_TURNS"
log "╚══════════════════════════════════════════════════╝"
log ""

# Load or initialize hypotheses
HYPOTHESES_FILE="$CHAIN_DIR/hypotheses/current.md"
if [ ! -f "$HYPOTHESES_FILE" ]; then
    cat > "$HYPOTHESES_FILE" << 'EOF'
# Active Hypotheses

## Confirmed
(none yet)

## Investigating
(none yet)

## Refuted
(none yet)
EOF
fi

for cycle in $(seq 1 "$MAX_CYCLES"); do
    log "═══════════════════════════════════════════════"
    log "  CYCLE $cycle / $MAX_CYCLES"
    log "═══════════════════════════════════════════════"

    CYCLE_DIR="$CHAIN_DIR/chains/cycle-$cycle"
    mkdir -p "$CYCLE_DIR"

    # ── Step 1: Pick a question ──────────────────────────────────────────

    if [ -n "${QUESTION:-}" ]; then
        CURRENT_Q="$QUESTION"
    else
        # Rotate through default questions, or let supervisor pick
        Q_IDX=$(( (cycle - 1) % ${#DEFAULT_QUESTIONS[@]} ))
        CURRENT_Q="${DEFAULT_QUESTIONS[$Q_IDX]}"
    fi

    log "Question: $CURRENT_Q"

    # ── Step 2: Run agents in parallel ───────────────────────────────────

    PIDS=()
    for model in $MODELS; do
        safe_name=$(echo "$model" | tr ':.' '-')
        chain_file="$CYCLE_DIR/$safe_name.log"
        log "Spawning Ralph: $model"
        run_ralph "$model" "$CURRENT_Q" "$chain_file" &
        PIDS+=($!)
    done

    # Wait for all agents
    log "Waiting for ${#PIDS[@]} agents..."
    FAILED=0
    for pid in "${PIDS[@]}"; do
        if ! wait "$pid" 2>/dev/null; then
            FAILED=$((FAILED + 1))
        fi
    done
    log "Agents complete ($FAILED failed)"

    # ── Step 3: Collect observations ─────────────────────────────────────

    OBSERVATIONS=""
    for model in $MODELS; do
        safe_name=$(echo "$model" | tr ':.' '-')
        chain_file="$CYCLE_DIR/$safe_name.log"
        if [ -f "$chain_file" ]; then
            TOOLS_USED=$(grep -c "^.*Tool:" "$chain_file" 2>/dev/null || echo 0)
            SUMMARY=$(grep "Summary:\|Agent Complete\|Model:" "$chain_file" 2>/dev/null | tail -2)
            OBSERVATIONS="$OBSERVATIONS
--- $model ($TOOLS_USED tool calls) ---
$SUMMARY
"
        fi
    done

    # ── Step 4: Supervisor analysis ──────────────────────────────────────

    ANALYSIS_PROMPT="You are the supervisor of an inference cascade. Three agents explored a workspace with the question: '$CURRENT_Q'

Here are their observations:
$OBSERVATIONS

Previous hypotheses:
$(cat "$HYPOTHESES_FILE")

Analyze:
1. Where do agents agree? (signal)
2. Where do they disagree? (investigate)
3. What did none of them find? (blind spot)
4. Update the hypotheses.

Be concise. Output updated hypotheses in the same markdown format."

    log "Supervisor analyzing..."
    ANALYSIS=$(supervisor_think "$ANALYSIS_PROMPT" 2>/dev/null || echo "(supervisor failed)")

    # Save synthesis
    echo "# Cycle $cycle — $(date -u +%Y-%m-%dT%H:%M:%SZ)
Question: $CURRENT_Q

## Agent Observations
$OBSERVATIONS

## Supervisor Analysis
$ANALYSIS
" > "$CHAIN_DIR/syntheses/cycle-$cycle.md"

    # Update hypotheses if supervisor produced output
    if [ -n "$ANALYSIS" ] && [ "$ANALYSIS" != "(supervisor failed)" ]; then
        echo "$ANALYSIS" > "$HYPOTHESES_FILE"
    fi

    log "Cycle $cycle complete. Synthesis saved."
    log ""

    # ── Step 5: Pause ────────────────────────────────────────────────────

    if [ "$cycle" -lt "$MAX_CYCLES" ]; then
        log "Pausing ${CYCLE_PAUSE}s before next cycle..."
        sleep "$CYCLE_PAUSE"
    fi
done

log ""
log "═══ Overnight cascade complete: $MAX_CYCLES cycles ═══"
log "Results in: $CHAIN_DIR"
