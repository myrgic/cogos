# CogOS

A cognitive daemon for AI agents. Written in Go. Runs locally.

```sh
make build && ./cogos serve --workspace ~/my-project
# http://localhost:6931/health
```

Or install a pre-built binary. Each block below refuses to overwrite a
binary a running `cogos` daemon is executing, fetching the same shared
guard `make install` uses (`scripts/lib/refuse-if-running.sh` /
`scripts/lib/refuse-if-running.ps1`) rather than inlining its own copy, and
runs in a subshell/script block so a refusal exits only the install, not
your terminal:

```sh
# macOS Apple Silicon
(
  set -eu
  guard="$(mktemp)"; trap 'rm -f "$guard"' EXIT
  curl -fsSL https://raw.githubusercontent.com/myrgic/cogos/main/scripts/lib/refuse-if-running.sh -o "$guard"
  . "$guard"
  mkdir -p ~/.cog/bin
  refuse_if_running ~/.cog/bin/cogos || exit 1
  curl -L https://github.com/myrgic/cogos/releases/latest/download/cogos-darwin-arm64 -o cogos
  chmod +x cogos && mv cogos ~/.cog/bin/cogos
)
```

```sh
# Linux amd64
(
  set -eu
  guard="$(mktemp)"; trap 'rm -f "$guard"' EXIT
  curl -fsSL https://raw.githubusercontent.com/myrgic/cogos/main/scripts/lib/refuse-if-running.sh -o "$guard"
  . "$guard"
  mkdir -p ~/.cog/bin
  refuse_if_running ~/.cog/bin/cogos || exit 1
  curl -L https://github.com/myrgic/cogos/releases/latest/download/cogos-linux-amd64 -o cogos
  chmod +x cogos && mv cogos ~/.cog/bin/cogos
)
```

```powershell
# Windows amd64 (PowerShell)
$dest = "$HOME\.cog\bin"
New-Item -ItemType Directory -Force -Path $dest | Out-Null
Invoke-WebRequest -Uri https://raw.githubusercontent.com/myrgic/cogos/main/scripts/lib/refuse-if-running.ps1 -OutFile "$env:TEMP\refuse-if-running.ps1"
. "$env:TEMP\refuse-if-running.ps1"
Assert-CogosNotRunning -Target "$dest\cogos.exe"
Invoke-WebRequest -Uri https://github.com/myrgic/cogos/releases/latest/download/cogos-windows-amd64.exe `
    -OutFile "$dest\cogos.exe"
Unblock-File "$dest\cogos.exe"
# Add $dest to your PATH if it isn't already
```

Set `ALLOW_RUNNING_INSTALL=1` (bash) / `$env:ALLOW_RUNNING_INSTALL = '1'`
(PowerShell) to override, if you mean it. See
`scripts/lib/refuse-if-running.sh` for what the guard checks and why it
fails closed rather than assuming "not running" when it can't tell.

Other architectures (linux/arm64) are available on the [Releases page](https://github.com/myrgic/cogos/releases/latest). Intel Macs (darwin/amd64) are not published as a release asset; cross-compile locally with `make darwin-amd64`.

---

## What this is

`cogos` is a Go daemon that runs locally and gives AI tools persistent workspace memory, scored context per prompt, and cross-session continuity. Claude Code, Cursor, and any tool that can call a local endpoint or run a hook can plug into it.

The kernel owns workspace state. It intercepts each prompt before the model sees it, scores all workspace documents by relevance, and injects a focused context window. Externalized attention: the substrate decides what's relevant, not the model. It routes inference through local or cloud providers. It keeps a hash-chained ledger of every decision. It runs reconcilers that maintain workspace invariants (plan, apply, drift detection, topological ordering), the same shape as Kubernetes-era control loops, applied to AI workspace state.

Your codebase or project directory sits untouched. CogOS adds a `.cog/` overlay alongside `.git/`, or in a directory by itself. Everything runs on your machine. Nothing leaves unless you choose.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Your AI tools                                          │
│  Claude Code · Cursor · custom agents · Ollama · ...    │
└────────────────────┬────────────────────────────────────┘
                     │ hooks · MCP · HTTP
                     ▼
┌─────────────────────────────────────────────────────────┐
│  CogOS kernel  (local Go daemon)                        │
│  Owns workspace state. Hosts subsystems. Exposes        │
│  protocol surfaces for whatever AI tool plugs in.       │
└────────────────────┬────────────────────────────────────┘
                     │ reads & writes
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Workspace  (any directory)                             │
│                                                         │
│    your-project/                                        │
│    ├─ src/  docs/  ...    ← your stuff, untouched       │
│    ├─ .git/               ← code history (optional)     │
│    └─ .cog/               ← cognitive overlay           │
│       ├─ mem/    cogdocs (memory)                       │
│       ├─ run/    bus events, traces                     │
│       └─ ledger/ hash-chained record                    │
└─────────────────────────────────────────────────────────┘
```

Three pieces:

- **Your AI tools** speak to the kernel through hooks, MCP, and HTTP. Anything that can call a local endpoint or run a hook can plug in.
- **The CogOS kernel** is one local Go daemon. It owns workspace state, hosts subsystems (context assembly, inference routing, reconcilers, event bus, ledger), and exposes the protocol surfaces.
- **The workspace** is any directory you point the kernel at. CogOS adds a `.cog/` overlay alongside whatever else is there. Same shape regardless of what's in the directory.

### How the kernel is organized internally

The kernel has three internal layers:

```
┌──────────────────────────────────────────────────────────────┐
│  API Layer         HTTP API · MCP Server · Provider Router   │
│                    Event Broker (SSE) · Config API           │
├──────────────────────────────────────────────────────────────┤
│  Workspace         Context Engine · Memory · Ledger          │
│                    Salience Scorer · Blob Store · Traces     │
│                    Conversation Sidecars · Kernel Slog       │
├──────────────────────────────────────────────────────────────┤
│  Process Core      Process Loop · Identity · State FSM       │
│                    Agent Harness · CogBus · Tool-call Gate   │
└──────────────────────────────────────────────────────────────┘
```

**API Layer** is the kernel's HTTP and MCP surface. Serves OpenAI and Anthropic-compatible chat endpoints, the always-on MCP Streamable HTTP server, the Anthropic Messages API proxy, the event broker with SSE streaming, and the config mutation API. Routes inference requests to local or cloud providers. Binds to `127.0.0.1:6931` by default; `--bind` or `bind_addr` in YAML relaxes CORS when set to a non-loopback interface.

**Workspace** is where state lives. The context engine scores documents and arranges them into stability zones optimized for KV cache reuse. The ledger is append-only and hash-chained. Traces capture attention, proprioceptive state, and internal request metabolites. Conversation sidecars persist full turn text. Memory persists across sessions.

**Process Core** is the kernel's control loop. Runs continuously through four states (Active, Receptive, Consolidating, Dormant). Manages identity, consolidation, workspace lifecycle, the homeostatic agent harness, the tool-call hallucination gate, and emits to the kernel slog (stderr tee plus `.cog/run/kernel.log.jsonl`).

---

## How foveated context works

When you submit a prompt in Claude Code, the `UserPromptSubmit` hook fires and calls the CogOS daemon. The term "foveated" is borrowed from the eye, where the fovea is the high-resolution center: the context engine places what matters most in front of the model and lets the rest recede.

The context engine:

1. Scores all workspace documents using a ~2.3M-parameter Mamba SSM trained as a context retrieval model, combined with git-derived salience
2. Ranks by a composite signal (edit recency, semantic match, structural importance)
3. Assembles a context window organized into stability zones:

| Zone | Contents | Behavior |
|------|----------|----------|
| 0: Core | Identity, system config | Always present, never evicted |
| 1: Knowledge | Workspace docs, indexed memory | Shifts slowly, high cache hit rate |
| 2: History | Conversation turns | Scored by relevance, evictable |
| 3: Current | The current message | Always present |

4. Injects the assembled context into the prompt before it reaches the model

The model sees a pre-focused window instead of everything-or-nothing. Zone ordering is tuned for KV cache reuse: stable content stays at the front of the window across prompts, reducing cache misses. See [docs/EVALUATION.md](docs/EVALUATION.md) for the retrieval methodology.

---

## Feature summary

### Context and memory

- **Externalized attention.** The kernel intercepts each prompt, scores workspace documents by relevance, and injects a focused context window before the model sees it. Relevance scoring is done by the substrate, not the model.
- **Foveated context assembly.** A live `UserPromptSubmit` hook fires on every prompt. Documents are ranked and arranged into stability zones optimized for KV cache reuse.
- **Workspace memory.** Hierarchical memory with salience scoring and temporal attention. Your workspace remembers across sessions, models, and tools. Switch from Claude Code to Cursor and back. Same memory, same context.
- **Conversation persistence.** `turn.completed` ledger events plus a per-session sidecar at `.cog/run/turns/<sessionID>.jsonl` preserve full prompt and response text.

### Inference and routing

- **Multi-provider routing.** OpenAI-compatible and Anthropic Messages-compatible HTTP API. Works with Ollama, LM Studio, Claude, and any OpenAI-compatible endpoint. Local models preferred by default.
- **Anthropic Messages API proxy.** Transparent proxy at `POST /v1/messages` that forwards to the Anthropic API with streaming SSE passthrough. Enables `cog claude` to route Claude Code through the kernel via `ANTHROPIC_BASE_URL`.

### Observability

- **Hash-chained ledger.** CogBlock protocol for content-addressed, hash-chained records. Every routing decision, context assembly, state transition, turn completion, tool call, and config mutation is recorded in an append-only ledger (SHA-256, RFC 8785) with optional chain verification.
- **Three observability lanes.** The kernel exposes ledger (durable hash-chained events), traces (client metabolites + attention + proprioceptive state), and kernel slog (structured runtime logs) as non-overlapping surfaces. Each has a dedicated MCP tool, HTTP endpoint, and on-disk format.
- **Live event bus.** `AppendEvent` fans into an in-process broker with SSE streaming at `/v1/bus/:id/events/stream`. Subscribers see writes in real time; offline writers go straight to JSONL.

### Coordination

- **Reconcilers.** A generic plan/apply control loop (in `pkg/reconcile`) runs 27 registered providers: 11 core kernel functions (agent, discord, eval, identity, mcp-tools, openclaw-agents, openclaw-cron, openclaw-gateway, pin, self-update, service), 8 workspace and integration providers (component, conversations, site, margin-bridge, lms-model-state, mlx-inference, vitals-retention, and one worktree reconciler per managed repo), and 8 corpus-observatory reconcilers (projection-compiler plus 7 lineage-projection kinds). Run `cogos reconcile --help` for the current live list. Each provider implements `Reconcilable` (seven methods: Type, LoadConfig, FetchLive, ComputePlan, ApplyPlan, BuildState, Health). The orchestrator handles plan, apply, drift detection, topological ordering (Kahn's sort), and three-axis status (Sync, Health, Operation) for all providers.
- **Kernel-native session management.** `SessionRegistry` + `HandoffRegistry` with atomic-claim semantics: first-wins enforced at the bus boundary, not just in the in-memory cache. Bus stays ground truth; the registries are derived views rebuilt from seq-sorted replay on startup.
- **Native agent harness.** A homeostatic assessment loop runs as a goroutine inside the kernel. Calls a local model via Ollama with six kernel-native tools. Adaptive interval (5m-30m) based on assessment urgency, with panic recovery.
- **MCP Streamable HTTP.** Full MCP transport at `POST /mcp` with JSON-RPC 2.0, session management, and 30-minute expiry. Always-on (no build tag). 54 tools spanning observability, agent control, config, memory, sessions, handoffs, voice, and conversation search.
- **Config mutation API.** `cog_read_config` / `cog_write_config` / `cog_rollback_config` MCP tools and matching REST surface. RFC 7396 merge-patch semantics with atomic writes and rotating backups.

For endpoint and tool counts, see the HTTP API and MCP tools tables below.

---

## Exposure surfaces

Three non-overlapping observability lanes, each with an MCP tool, HTTP endpoint, and on-disk artifact:

| Lane | What it captures | MCP tool | HTTP | On-disk |
|------|------------------|----------|------|---------|
| **Ledger** | Durable hash-chained events (turns, config mutations, tool calls, state changes) | `cog_read_ledger` | `GET /v1/ledger` (optional `?verify_chain=true`) | Append-only CogBlock chain |
| **Traces** | Client metabolites, attention, proprioceptive state, internal requests | `cog_search_traces` | `GET /v1/traces` (legacy `/v1/proprioceptive` preserved) | Trace files |
| **Kernel slog** | Structured runtime logs via `teeHandler` | `cog_tail_kernel_log` | `GET /v1/kernel-log` | stderr + `.cog/run/kernel.log.jsonl` |

The live event bus is a fourth surface for real-time subscribers: SSE at `/v1/bus/:id/events/stream`, plus `cog_tail_events` and `cog_read_events` MCP tools. All `AppendEvent` writes fan through the broker.

---

## Library packages (pkg/)

Eight importable Go packages extracted into a `go.work` multi-module workspace. Each has its own `go.mod` and can be imported independently of the kernel. Six are stdlib-only; `pkg/bep` requires `google.golang.org/protobuf`.

| Package | What it provides |
|---------|-----------------|
| `pkg/cogblock` | Content-addressed block format, CogBlockKind enum, provenance/trust types, EventEnvelope, ledger (RFC 8785 canonicalization, hash chain, verify) |
| `pkg/coordination` | Claim/Handoff/Broadcast types, 13 coordination functions, AgentID |
| `pkg/bep` | BEP wire protocol types, TLS/DeviceID, index/version vectors, events, Engine/SyncProvider interfaces |
| `pkg/reconcile` | Reconcilable interface (7 methods), State/Plan/Action types, registry, Kahn's topological sort, meta-orchestrator |
| `pkg/modality` | Module interface, Bus, wire protocol (D2), events, salience tracker, channels, ProcessSupervisor |
| `pkg/cogfield` | Node/Edge/Graph types, Block, BlockAdapter interface, conditions, signals, sessions, documents |
| `pkg/uri` | URI struct, Parse/Format, 35 namespaces, ExtractInlineRefs, error types |
| `pkg/substrate` | Umbrella re-export module for substrate-shaped packages (uri, cogfield, bep); ADR-100 extraction target |

---

## Agent harness

The native Go agent harness runs a homeostatic assessment loop inside the kernel process:

- Calls a local model via Ollama's native `/api/chat` (with `think: false`)
- Adaptive interval: 5m idle, scales to 30m when assessment urgency is low
- Panic recovery: a crash in the agent goroutine doesn't take down the kernel
- State and loop control over MCP (`cog_list_agents`, `cog_get_agent_state`, `cog_trigger_agent_loop`) and REST (`/v1/agents[/...]`). The singular `/v1/agent/{status,traces,trigger}` routes are preserved byte-for-byte for the embedded dashboard.

Six kernel-native tools are available to the agent itself:

| Tool | Description |
|------|-------------|
| `memory_search` | Search CogDocs by query |
| `memory_read` | Read a specific memory document |
| `memory_write` | Write or update a memory document |
| `coherence_check` | Run drift detection on the workspace |
| `bus_emit` | Emit an event to the CogBus |
| `workspace_status` | Get workspace health and metrics |

---

## HTTP API

| Endpoint | Description |
|----------|-------------|
| `POST /v1/chat/completions` | OpenAI-compatible chat (streaming + non-streaming) |
| `POST /v1/messages` | Anthropic Messages API proxy (streaming SSE passthrough) |
| `POST /v1/context/foveated` | Foveated context assembly |
| `POST /v1/context/build` | Context engine without inference step |
| `GET /v1/context` | Current attentional field |
| `GET /v1/ledger` | Read hash-chained ledger; `?verify_chain=true` walks the chain |
| `GET /v1/traces` | Client metabolites, attention, proprioceptive, internal requests |
| `GET /v1/proprioceptive` | Legacy byte-compatible trace subset |
| `GET /v1/kernel-log` | Structured kernel slog tail |
| `GET /v1/conversation` | Turn history with full prompt/response text |
| `GET /v1/tool-calls` | Tool-call records and correlation state |
| `GET /v1/config` · `PATCH /v1/config` | Read or RFC 7396 merge-patch configuration |
| `POST /v1/config/rollback` | Roll back to a previous atomic backup |
| `GET /v1/agents` · `GET /v1/agents/:id/state` · `POST /v1/agents/:id/trigger` | Plural agent control surface |
| `GET /v1/agent/status` · `GET /v1/agent/traces` · `POST /v1/agent/trigger` | Singular agent routes (preserved for dashboard byte-compat) |
| `POST /v1/sessions/register` | Register a session on `bus_sessions` (kernel validates id + mints in-memory state) |
| `POST /v1/sessions/{id}/heartbeat` · `POST /v1/sessions/{id}/end` | Lifecycle (409 on ended-session heartbeat; no side effects on rejection) |
| `GET /v1/sessions/presence` | Aggregated roster with active-within-window flag (in-memory derived view) |
| `POST /v1/handoffs/offer` | Mint a handoff offer with kernel-side id; payload validated, TTL enforced |
| `POST /v1/handoffs/{id}/claim` | Atomic first-wins claim under registry lock; bus append before in-memory commit |
| `POST /v1/handoffs/{id}/complete` | Complete a claimed handoff; optional `next_handoff_id` links recursive relays |
| `GET /v1/handoffs` | List handoffs; filter by `state` (open, claimed, complete) and `for_session` |
| `GET /v1/hud/state` | Claude Code HUD state snapshot (identity, session, context pressure) |
| `GET /v1/claude-code/projects` · `GET /v1/claude-code/projects/{project}/sessions` · `POST /v1/claude-code/spawn` | ACP-client surface: enumerate Claude Code projects, list sessions in a project, spawn or resume a Claude Code subprocess wired into mod3 via a generated temp `.mcp.json` |
| `GET /v1/bus/:id/events/stream` | SSE stream of broker events |
| `GET /health` | Liveness probe (identity, state, trust) |
| `GET /dashboard` | Embedded web dashboard |
| `POST /mcp` · `DELETE /mcp` | MCP Streamable HTTP (JSON-RPC 2.0, session lifecycle) |

All endpoints serve on port **6931** by default. `--bind <addr>` (or `bind_addr` in YAML) overrides; CORS is strict on loopback and relaxed on non-loopback binds.

### MCP tools (54 total)

The always-on MCP server groups tools by surface. The `mcpserver` build tag was removed in #9. MCP ships in every binary.

| Category | Tool | Purpose |
|----------|------|---------|
| **Observability** | `cog_read_ledger` | Read hash-chained ledger events (optional chain verify) |
| | `cog_search_traces` | Query client metabolites + attention + proprioceptive + internal-request traces |
| | `cog_tail_kernel_log` | Stream the structured kernel slog |
| | `cog_tail_events` | Tail the live event bus |
| | `cog_read_events` | Filter event bus records by predicate |
| **Conversation / tool calls** | `cog_read_conversation` | Read turn sidecars with full prompt/response text |
| | `cog_read_tool_calls` | Read tool-call records + correlation state |
| | `cog_tail_tool_calls` | Live stream of tool-call activity |
| **Agent control** | `cog_list_agents` | Enumerate running agents |
| | `cog_get_agent_state` | State snapshot for an agent |
| | `cog_trigger_agent_loop` | Manually trigger an assessment cycle |
| | `cog_dispatch_to_harness` | Dispatch a prompt directly to the agent harness |
| **Config** | `cog_read_config` | Read the live config |
| | `cog_write_config` | RFC 7396 merge-patch (atomic write, rotating backups, `requires_restart=true` in v1) |
| | `cog_rollback_config` | Restore a prior atomic backup |
| **Sessions** | `cog_register_session` | Register a session on `bus_sessions` (kernel validates id, mints in-memory state) |
| | `cog_heartbeat_session` | Emit a heartbeat; rejected with 409 on ended sessions (no side effects) |
| | `cog_end_session` | Graceful shutdown marker; optional `handoff_id` links the chain |
| | `cog_list_sessions` | Aggregated roster with active-within-window flag |
| | `cog_fork_session` | Substrate-native session fork (RFC-0005); vLLM PagedAttention-aware KV hand-off |
| **Handoffs** | `cog_offer_handoff` | Mint an offer with kernel-side id; payload validated, TTL enforced |
| | `cog_claim_handoff` | Atomic first-wins claim under registry lock; bus append before in-memory commit |
| | `cog_complete_handoff` | Complete a claimed handoff; `next_handoff_id` links recursive relays |
| | `cog_list_handoffs` | List handoffs by `state` (open, claimed, complete) and `for_session` |
| **Memory** | `cog_search_memory` | Search CogDocs by query |
| | `cog_read_cogdoc` | Read a memory document |
| | `cog_write_cogdoc` | Write or update a memory document |
| | `cog_patch_frontmatter` | Patch frontmatter fields on an existing cogdoc |
| | `cog_check_coherence` | Drift detection across the workspace |
| | `cog_memory_toc` | Table of contents for workspace memory |
| | `cog_memory_index` | Index or re-index workspace memory |
| **Context and ingestion** | `cog_assemble_context` | Assemble a foveated context window |
| | `cog_ingest` | Ingest external material (URLs, documents, conversations) |
| | `cog_query_field` | Query the cogfield graph |
| | `cog_get_state` | Get the current kernel state snapshot |
| | `cog_emit_event` | Emit an event to the CogBus |
| | `cog_read_file` | Read a file from the workspace |
| | `cog_grep_files` | Grep files in the workspace |
| | `cog_render_peer_awareness_packet` | Render a peer-awareness context packet |
| **Conversations Observatory** | `cog_search_conversations` | Full-text search across indexed conversation turns |
| | `cog_get_conversation_turn` | Retrieve a specific turn from a conversation |
| | `cog_list_conversations` | List indexed conversation sessions |
| **Eval / experiments** | `cog_run_experiment` | Run a registered evaluation experiment |
| | `cog_list_experiments` | List available experiments |
| | `cog_get_experiment_status` | Get the status of a running experiment |
| | `cog_pin_baseline` | Pin the current workspace state as a baseline for future comparison |
| **Voice (Mod3 bridge)** | `mod3_speak` · `mod3_stop` · `mod3_voices` · `mod3_status` | TTS and voice channel control |
| | `mod3_tail_logs` | Tail chat-flow events from the mod3 ring buffer |
| | `mod3_register_session` · `mod3_deregister_session` · `mod3_list_sessions` | Mod3 channel session management |

Sessions are created on `initialize` and expire after 30 minutes of inactivity.

### Providers

Ships with adapters for Anthropic, Ollama, Claude Code, Codex, and vLLM (PagedAttention scaffolding). New providers implement [six methods](docs/writing-a-provider.md). Providers support `default_options` in config for per-provider request shaping (e.g. foveal vs peripheral inference profiles).

---

## CLI

`cog` wraps the daemon with subcommands for the common lifecycle plus the event bus:

```sh
cog serve               # Start the daemon (--bind <addr> to expose beyond loopback)
cog claude              # Launch Claude Code with ANTHROPIC_BASE_URL set to the kernel
cog emit ...            # Write an event through the engine (no more silent drops)
cog bus watch|tail|list # Read the event bus
cog bus send ...        # Write to the bus. Direct JSONL by default; --http for SSE broadcast
```

`cog emit` was migrated to the engine in #23 (Track 5 Phase 1): the root `cmdEmit` is retained for compat but the engine path fixes the silent-drop bug where hooks returned success without writing to the ledger.

---

## Getting started

### Requirements

- Go 1.25+
- macOS, Linux, or Windows

### Build and run

```sh
git clone https://github.com/myrgic/cogos.git
cd cogos
make build

# Initialize a workspace
./cogos init --workspace ~/my-project

# Start the daemon
./cogos serve --workspace ~/my-project

# Verify it's running
curl -s http://localhost:6931/health | jq .
```

### Cross-compile

```sh
make build-linux-amd64
make build-linux-arm64    # Unblocked in #31 (syscall.Dup2 -> unix.Dup2)
make build-darwin-arm64
make build-windows-amd64  # Added in #15; see docs for install steps
```

### Route Claude Code through the kernel

```sh
cog claude
# Sets ANTHROPIC_BASE_URL to http://localhost:6931 and starts Claude Code
```

### Developer setup

```sh
./scripts/setup-dev.sh    # Build, install to ~/.cog/bin, configure PATH
```

### Docker

```sh
make image        # Build production image
make run          # Run with workspace volume mount
make e2e          # Build + run full cold-start test in a container
```

A Docker Compose topology with `bridge-{primary,secondary}` and `tailscale-{primary,secondary}` siblings landed in #19.

### Workspace base image

`Dockerfile.workspace` is the declared minimal starting condition of a CogOS
workspace: the exact directory tree and default config `cogos init` produces,
built from a checksum- and signature-verified published release, nothing
more. Validate a workspace with `cog doctor`; materialize one with this
image. Distributions (a product, a fleet node, a demo) layer their own
skills and config on top of this base via its `SKILLS_OVERLAY` /
`CONFIG_OVERLAY` build args — they don't redefine what "minimal" means.

```sh
docker build -f Dockerfile.workspace -t cogos-workspace:dev .
docker compose -f docker-compose.workspace.yml up --build
```

---

## Logs and troubleshooting

The kernel writes structured logs (JSON, one record per line) to:

```
<workspace>/.cog/run/kernel.log.jsonl
```

Quick tail:

```sh
# Unix
tail -f /your/workspace/.cog/run/kernel.log.jsonl | jq -c .

# Windows (PowerShell)
Get-Content "$HOME\my-project\.cog\run\kernel.log.jsonl" -Wait | ForEach-Object { $_ | ConvertFrom-Json }
```

The same log is exposed over MCP (`cog_tail_kernel_log`) and HTTP (`GET /v1/kernel-log`) so any connected tool can read it without touching the file directly.

---

## Testing

```sh
make test         # Unit tests (with -race)
make e2e-local    # Full cold-start lifecycle test
make e2e          # Containerized e2e (Docker)
```

Ledger and sync-watcher tests are stable under `-count>=2` (#30).

---

## Project layout

```
cmd/cogos/              Entry point (thin; delegates to internal/engine)
internal/engine/        Kernel sources and tests
pkg/                    Importable library packages (go.work multi-module)
  cogblock/             Content-addressed blocks and ledger
  coordination/         Agent coordination primitives
  bep/                  BEP wire protocol types
  reconcile/            Reconciliation framework
  modality/             Modality bus and channel types
  cogfield/             Field graph types
  uri/                  URI parsing and namespaces
  substrate/            Umbrella re-export module (ADR-100)
sdk/                    Go SDK for CogOS clients
docs/                   Specs, architecture docs, provider guide
scripts/                Setup, CLI wrapper, e2e tests, experiment harnesses
```

---

## Status

**v3 kernel.** Ground-up rewrite after a year of daily use across Claude Code, Cursor, and custom agent harnesses.

### Working

- Continuous process daemon with four-state FSM
- Foveated context assembly with a ~2.3M-parameter Mamba SSM context retrieval model
- Hash-chained append-only ledger with optional chain verification
- Three-lane observability: ledger, traces, kernel slog
- Live event bus with in-process broker and SSE streaming
- Conversation persistence (turn sidecars + ledger `turn.completed` events)
- Tool-call observability with pending-call correlation cache (1024 / 10min)
- Config mutation API (MCP + REST, merge-patch, atomic write, rollback)
- Agent state and loop control over MCP and REST (singular routes preserved)
- Multi-provider routing (Ollama, Anthropic, Claude Code, Codex)
- Always-on MCP Streamable HTTP server (54 tools, sessions, JSON-RPC 2.0)
- Kernel-native session management with atomic handoff claim (bus-level first-wins via append-before-apply; seq-sorted replay on startup)
- Anthropic Messages API proxy with streaming SSE
- Native Go agent harness with adaptive interval and 6 kernel tools
- Embedded web dashboard with agent status, cycle history, and decomposition panel
- Library extraction: 8 packages in pkg/ (ADR-100; pkg/substrate added)
- Content-addressed blob store
- Git-derived salience scoring
- Tool-call hallucination gate (activated by `NormalizeMCPRequest` path in #25)
- Digestion pipeline (Claude Code + OpenClaw adapters) wired into the process loop
- Memory consolidation
- OpenAI and Anthropic API compatibility
- `cog bus send` subcommand for write-side symmetry (direct JSONL or opt-in SSE)
- `--bind` flag with non-loopback CORS relaxation
- Windows cross-compile (`make build-windows-amd64`) and linux/arm64
- End-to-end test suite
- OpenTelemetry instrumentation
- Port consolidation on 6931

### Next

- Direct import of the `myrgic/constellation` L1 trust-node protocol via the `ConstellationBridge` seam (the kernel already embeds `sdk/constellation/` for the Constellation memory graph of cogdocs; the external L1 peer protocol is the piece not yet wired)
- Multi-agent process management (the agent controller API is forward-compatible; only the `primary` instance is registered today)
- Further agent state surface (beyond the v1 trigger/state/list)

---

## Ecosystem

CogOS is one piece of a larger system. Each component is its own repo with independent releases:

| Repo | Purpose | Status |
|------|---------|--------|
| **[cogos](https://github.com/myrgic/cogos)** | The daemon (this repo) | Active |
| [constellation](https://github.com/myrgic/constellation) | L1 trust-node protocol for the Constellation substrate. Git-backed hash-chained ledger, ECDSA P-256 identity, EMA-weighted peer trust. Consumed via the kernel's `ConstellationBridge` seam. | Active |
| [mod3](https://github.com/myrgic/mod3) | Modality bus: voice I/O, TTS, channel multiplexing | Active |
| [plugins](https://github.com/myrgic/plugins) | Agent skill and plugin library (Claude Code compatible) | Active |
| [charts](https://github.com/myrgic/charts) | Helm charts and Docker Compose for deployment | Active |

---

## Releases & Changelog

Per-release summaries: the [Releases page](https://github.com/myrgic/cogos/releases) (auto-generated from PR titles since the previous tag). See [CHANGELOG.md](CHANGELOG.md) for the policy and the historical entries (pre-v0.4.0).

---

## Design documents

- [System Specification](docs/SYSTEM-SPEC.md): Multi-level spec from ontology to deployment
- [Architectural Principles](docs/architecture/principles.md): Core engineering constraints
- [Writing a Provider](docs/writing-a-provider.md): How to add a new inference provider
- [MCP Specification](docs/MCP-SPEC.md): MCP server contract
- [Provider Specification](docs/PROVIDER-SPEC.md): Provider interface contract
- [Architecture Diagrams](docs/architecture-diagram-source.md): Kernel layer diagrams, topology views
- [Cognitive GitOps](docs/architecture/cognitive-gitops.md): Substrate-coordinated repo model
- [E2E Test Plan](docs/E2E-TEST-PLAN.md): End-to-end test strategy

---

## License

[MIT](LICENSE). Copyright (c) 2025-2026 Chaz Dinkle.
