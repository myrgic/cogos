# CogOS

**A reconciler for the environment around your AI tools.**

You declare the state you want: which model is loaded on which machine, at what
context length; which agent worktrees should exist; what version the daemon
runs; what a site should serve; which derived views of your notes should be
current. CogOS is one local Go daemon that keeps pulling reality back to that
declaration, and says plainly when it can't.

It is the Kubernetes controller pattern (declare, observe, diff, apply, report
health) pointed at an AI workstation instead of a cluster. No cluster, no VM,
no etcd: one binary, and the declared state lives in files in your workspace.

```
declared (.cog/config, workspace files)
        │
        ▼
  LoadConfig ─► FetchLive ─► ComputePlan ─► ApplyPlan ─► Health
                   ▲            (pure diff,     │
                   │             no inference)  │
                   └────────── every 30s, plus event triggers
```

## See it converge

Declare that a model should stay loaded in LM Studio
(`.cog/config/providers.local.yaml`):

```yaml
providers:
  lmstudio-local:
    type: openai
    endpoint: http://localhost:1234
    options:
      model_state:
        manage: true
        model: qwen3-30b-a3b
        context_length: 262144
        parallel: 1
```

Then take it away by hand and watch it come back:

```sh
lms unload qwen3-30b-a3b
sleep 45 && lms ps          # loaded again, same context length
curl -s localhost:6931/v1/reconcile/convergence | jq '.providers[] | select(.provider | startswith("lms-model-state"))'
```

On the author's machine the model is back within one 30-second cycle. To take it
out on purpose you change the declaration (`manage: false`), not the machine.
That is the whole idea: the file is the source of truth, and the machine is
brought into line with it.

## What it reconciles today

Every row implements one seven-method contract
([`pkg/substrate/reconcile`](pkg/substrate/reconcile/types.go)) and is driven by
one daemon loop ([daemon reconcile loop driver](docs/adrs/095-daemon-reconcile-loop-driver.md)).
A live kernel on the author's node runs 28 instances. They split honestly into
two kinds.

**Converging** (15 instances): the plan is applied, so reality changes.

| Declared state | Reconciler | Design doc |
|---|---|---|
| Which model is loaded on which LM Studio backend, at what context length (local or over LAN) | `lms-model-state/<backend>` | [lms-model-state reconciler](docs/adrs/104-lms-model-state-reconciler.md) |
| The kernel's own version, from GitHub releases (SHA-256 checked) | `self-update` | `internal/providers/selfupdate` |
| Static sites: content-hash drift, then deploy (runs [myrgic.com](https://myrgic.com)) | `site` | `internal/providers/site` |
| Agent git worktrees: alive, orphaned, or reclaimable | `worktree-reconciler/<repo>` | [worktree reconciler](docs/adrs/096-worktree-reconciler.md) |
| The archive of agent conversations, from every harness that writes them | `conversations` | `internal/conversations` |
| Derived views of the notes corpus (lineage, decision graph, open questions) | `projection-compiler`, `lineage-projection-*` | [lineage observatory](docs/adrs/094-lineage-observatory.md) |
| Signals from a GitHub repo turned into wake events for an agent | `margin-bridge` | `internal/providers/marginbridge` |

A supervised `mlx_lm.server` (`mlx-inference/<name>`) and a Discord server
layout (`discord`) are full reconcilers too; neither is active on the author's
node right now.

**Observed** (13 instances): health is probed every cycle and reported, but
nothing is applied yet. Agents, identity, MCP tools, services, external
gateways, evaluation, workspace components (drift is reported, not fixed),
workspace pins, and node health history (which records and compacts on its own
schedule, outside plan/apply), plus the aggregate `lms-model-state`
and `mlx-inference` entries that summarize their per-backend reconcilers.

Promoting an observed row to a converging one means writing its plan and apply
steps; the loop, backoff, and reporting come for free. Memory and context
assembly are not reconciled; they are services the kernel runs.

## How it fails

A control loop is only as good as its behaviour when reality won't cooperate.
These are the decisions that took the most iterations to get right:

- **Five health states, not two.** `Healthy`, `Degraded`, `Progressing`,
  `Missing`, `Suspended`. Self-heal acts on `Degraded`, `Missing`, and out-of-sync. A backend
  that is simply switched off or off the LAN is `Suspended`, not `Degraded`, so
  the daemon doesn't spend the night trying to load a model onto a laptop that's
  asleep ([lms-model-state reconciler, §2](docs/adrs/104-lms-model-state-reconciler.md)).
- **One provider can't take down the others.** Each cycle is isolated; a panic
  or error in one provider is logged and counted, and the loop keeps ticking.
- **Backoff, then quarantine, then automatic release.** Consecutive failures
  back off exponentially with jitter (up to 32 ticks, ~16 minutes). After 12 in a
  row, roughly two hours, the daemon stops *acting* on that provider but keeps
  observing it. Quarantine lifts on its own when the provider's config
  fingerprint changes, i.e. when someone has actually changed what was failing.
- **A persistent condition is one anomaly, not one per tick.** Anomalies are
  tracked as episodes that open and close, so a stuck provider shows up once in
  the log instead of 700 times ([one condition, one anomaly](https://github.com/myrgic/cogos/pull/524)).
- **Convergence, not a metronome.** A reconciler that "fixes" something every
  cycle is itself a bug; the conversations reconciler did exactly that until
  [it was made to converge](https://github.com/myrgic/cogos/pull/480).
- **Observable.** `GET /v1/reconcile/convergence` reports per-provider cycle
  time, over-budget cycles, degraded cycles, open anomaly episodes, and
  quarantine.

**Known gaps, stated rather than hidden** (as of this writing):

- There are no leases yet. An experiment that needs the GPU must flip the
  declaration off and restart the kernel; a plain `lms unload` is undone within
  a cycle.
- A remote LM Studio backend with a bad credential fails every load. After
  12 failed cycles it is quarantined, as designed: the loop is working and the
  credential is not.
- The aggregate `lms-model-state` health entry reports `Degraded` while the
  per-backend entries are healthy: a reporting bug, not a serving one.
- Two reconcilers (`conversations`, one worktree instance) regularly exceed
  their cycle-time budget.

## Why this shape

The author spent a decade operating production infrastructure, most of it on
Kubernetes. The mapping is deliberate:

| Kubernetes ecosystem | CogOS |
|---|---|
| Controller / operator | `Reconcilable` provider |
| Flux / Argo CD (watch declared state, apply, detect drift) | The reconcile daemon |
| Helm charts | Skills (packaged procedures an agent loads) |
| Resource status and conditions | Health + operation phase, `/v1/reconcile/convergence` |

The one real difference: Kubernetes keeps its view of the world in etcd, a store
that can itself drift from reality. CogOS's declared state is plain files in the
workspace, usually committed to git, so the source of truth and the history of
every change to it can be the same thing. (Node-local settings with credentials,
like `providers.local.yaml`, stay out of git by design.)

---

## Install

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

## Architecture (the rest of the kernel)

```
┌─────────────────────────────────────────────────────────┐
│  Your AI tools                                          │
│  Claude Code · Codex · Cursor · custom agents · ...     │
└────────────────────┬────────────────────────────────────┘
                     │ hooks · MCP · HTTP
                     ▼
┌─────────────────────────────────────────────────────────┐
│  CogOS kernel  (local Go daemon)                        │
│  Owns workspace state. Runs the reconcile loop.         │
│  Exposes protocol surfaces for whatever plugs in.       │
└────────────────────┬────────────────────────────────────┘
                     │ reads & writes
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Workspace  (any directory)                             │
│                                                         │
│    your-project/                                        │
│    ├─ src/  docs/  ...    ← your stuff, untouched       │
│    ├─ .git/               ← code history (optional)     │
│    └─ .cog/               ← CogOS overlay               │
│       ├─ config/ declared state                         │
│       ├─ mem/    memory documents                       │
│       ├─ run/    bus events, traces, logs               │
│       └─ ledger/ hash-chained record                    │
└─────────────────────────────────────────────────────────┘
```

- **Your AI tools** talk to the kernel through hooks, MCP, and HTTP.
- **The kernel** is one local Go daemon. It owns workspace state, runs the
  reconcile loop, and hosts context assembly, inference routing, the event bus,
  and the ledger.
- **The workspace** is any directory you point the kernel at. CogOS adds a
  `.cog/` overlay alongside whatever else is there.

### How the kernel is organized internally

```
┌──────────────────────────────────────────────────────────────┐
│  API Layer         HTTP API · MCP Server · Inference Router  │
│                    Anthropic proxy · Event Broker (SSE)      │
├──────────────────────────────────────────────────────────────┤
│  Workspace         Context Assembly · Memory · Ledger        │
│                    Salience · Blob Store · Traces            │
│                    Conversation Sidecars · Kernel Log        │
├──────────────────────────────────────────────────────────────┤
│  Process Core      Process Loop · Identity · Reconcile Loop  │
│                    Maintenance Agent · Event Bus · Tool Gate │
└──────────────────────────────────────────────────────────────┘
```

**API Layer** is the kernel's HTTP and MCP surface: OpenAI- and
Anthropic-compatible chat endpoints, the MCP Streamable HTTP server, an
Anthropic Messages proxy, the event broker, and the config API. It routes each
inference request to a local or cloud provider. Binds to `127.0.0.1:6931` by
default.

**Workspace** is where state lives: memory documents, the append-only
hash-chained ledger, per-session conversation sidecars, traces, and the blob
store. Context assembly reads from here.

**Process Core** keeps the kernel running between requests: a continuous loop
through four states (Active, Receptive, Consolidating, Dormant), node identity,
the reconcile loop, and a small maintenance agent that only wakes when
something is unhealthy (see below).

---

## Context assembly

When you submit a prompt in Claude Code, the `UserPromptSubmit` hook calls the
kernel, which:

1. Ranks workspace documents by keyword relevance to the prompt, combined
   with git-derived salience (how recently and how often a file has been
   edited). An optional learned ranker can be loaded; it is off unless
   configured.
2. Assembles a context window in a fixed order, arranged so the parts that
   change least come first and the prompt cache stays warm:

| Order | Contents | Behavior |
|-------|----------|----------|
| 1 | Identity card | Always present, most stable |
| 2 | The client's own system prompt | Stable for the session |
| 3 | Conversation history | Scored by recency and relevance; evictable |
| 4 | Selected workspace documents | Re-ranked every turn, so placed late |
| 5 | The current message | Always present |

3. Injects the result before the prompt reaches the model.

The model sees a focused window instead of everything or nothing.

---

## Feature summary

### Context and memory

- **Context assembly** on every prompt, via a Claude Code hook or the HTTP API.
- **Workspace memory.** Markdown memory documents with frontmatter, full-text
  search (SQLite FTS5), and salience ranking. Memory lives in the workspace, so
  it is the same across sessions, models, and tools.
- **Conversation persistence.** `turn.completed` ledger events plus a
  per-session sidecar at `.cog/run/turns/<sessionID>.jsonl` keep full prompt and
  response text.

### Inference and routing

- **Multi-provider routing.** OpenAI- and Anthropic-compatible endpoints in
  front of LM Studio, MLX, Anthropic, Claude Code, Codex, and any
  OpenAI-compatible server. Local models are preferred when available.
- **Anthropic Messages proxy** at `POST /v1/messages`, with streaming, so
  Anthropic-API clients can run through the kernel.

### Observability

- **Hash-chained ledger.** Routing decisions, context assemblies, state
  transitions, turns, tool calls, and config changes are appended to a
  content-addressed ledger (SHA-256, RFC 8785 canonical JSON) with optional
  chain verification.
- **Three separate lanes:** the ledger (durable events), traces (attention and
  tool-call activity, internal requests), and the kernel log (structured runtime
  logs). Each has an MCP tool, an HTTP endpoint, and an on-disk file.
- **Live event bus** with SSE streaming at `/v1/bus/:id/events/stream`.
- **Reconcile health** at `/v1/reconcile/convergence` (see the top of this file).

### Coordination

- **Reconcilers.** See [What it reconciles today](#what-it-reconciles-today).
  `cogos reconcile --help` lists what your build registers.
- **Sessions and handoffs.** A session registry and a handoff registry with
  atomic first-wins claims, enforced at the bus. The bus is the source of truth;
  the registries are rebuilt from it on startup.
- **MCP Streamable HTTP** at `POST /mcp` (JSON-RPC 2.0, sessions with a
  30-minute idle expiry). Frequently used tools are listed directly; the rest
  are discoverable through `cog_tool_search` and callable through
  `cog_tool_invoke`.
- **Config API.** Read, merge-patch (RFC 7396), and roll back config over MCP
  or REST, with atomic writes and rotating backups.

---

## Exposure surfaces

| Lane | What it captures | MCP tool | HTTP | On disk |
|------|------------------|----------|------|---------|
| **Ledger** | Durable hash-chained events (turns, config changes, tool calls, state changes) | `cog_read_ledger` | `GET /v1/ledger` (`?verify_chain=true`) | Append-only chain |
| **Traces** | Attention events, tool-call activity, internal requests | `cog_search_traces` | `GET /v1/traces` | `.cog/run/*.jsonl` |
| **Kernel log** | Structured runtime logs | `cog_tail_kernel_log` | `GET /v1/kernel-log` | `.cog/run/kernel.log.jsonl` |

The event bus is a fourth surface for real-time subscribers: SSE at
`/v1/bus/:id/events/stream`, plus the `cog_tail_events` and `cog_read_events`
MCP tools.

---

## Library packages (pkg/)

Importable Go packages in a `go.work` multi-module workspace. The ones most
useful outside the kernel:

| Package | What it provides |
|---------|-----------------|
| `pkg/reconcile` | The `Reconcilable` interface, plan/action types, registry, topological ordering |
| `pkg/cogblock` | Content-addressed block format and the hash-chained ledger (canonicalization, chain verify) |
| `pkg/coordination` | Claim, handoff, and broadcast primitives |
| `pkg/bep` | Block Exchange Protocol types for node-to-node sync |
| `pkg/modality` | Module interface, bus, and channels for voice and other media |
| `pkg/cogfield` | Graph types over workspace documents |
| `pkg/uri` | `cog:` URI parsing and namespaces |
| `pkg/skills` | Skill discovery and frontmatter parsing |
| `pkg/substrate` | Umbrella module re-exporting the substrate-shaped packages |

Smaller utilities: `pkg/alias`, `pkg/filelock`, `pkg/pathsafe`,
`pkg/cogdoc_review`.

---

## Maintenance agent

Most of the time nothing intelligent happens. Each tick, the kernel probes every
reconciler and runs deterministic self-heal on the unhealthy ones (plans are
pure diffs, no model involved). Only if something stays unhealthy, or a trigger
is pending, does it escalate to a small agent running inside the process:

- It calls the local model named by `harness_provider` in config (for example
  an LM Studio backend).
- It assesses the situation and picks one of `sleep`, `observe`, `consolidate`,
  `repair`, `propose`, or `escalate`, then may act using the kernel's own tools
  (memory search and read/write, URI resolution, coherence check, event emit,
  file read and grep, state and field queries).
- A crash in the agent goroutine doesn't take down the kernel.
- It is controllable over MCP (`cog_list_agents`, `cog_get_agent_state`,
  `cog_trigger_agent_loop`, `cog_dispatch_to_harness`) and REST
  (`/v1/agents[/...]`).

---

## HTTP API

Inference, MCP, and most write routes require a grant. The kernel mints one at
first boot and writes it to `~/.cog/vault/node-root-grant` (mode 0600). Send it
as `X-Cogos-Grant`, `Authorization: Bearer`, or `x-api-key`.

| Endpoint | Description |
|----------|-------------|
| `POST /v1/chat/completions` | OpenAI-compatible chat (streaming and non-streaming) |
| `POST /v1/messages` | Anthropic Messages proxy (streaming passthrough) |
| `POST /v1/context/foveated` | Context assembly (the route name predates the current vocabulary) |
| `GET /v1/context` | Current context state |
| `GET /v1/reconcile/convergence` | Per-reconciler cycle time, anomalies, quarantine |
| `GET /v1/reconcile/coherence` | Reconcile-loop coherence summary |
| `POST /v1/reconcile/{type}/resume` | Lift a quarantine by hand |
| `GET /v1/ledger` | Read the ledger; `?verify_chain=true` walks the chain |
| `GET /v1/traces` | Search traces |
| `GET /v1/proprioceptive` | Legacy trace view, kept byte-compatible for the dashboard |
| `GET /v1/kernel-log` | Kernel log tail |
| `GET /v1/vitals` | Node health history |
| `GET /v1/conversation` | Turn history with full prompt and response text |
| `GET /v1/tool-calls` | Tool-call records and correlation state |
| `GET /v1/config` · `PATCH /v1/config` | Read or merge-patch configuration |
| `POST /v1/config/rollback` | Restore a previous backup |
| `GET /v1/agents` · `GET /v1/agents/:id/state` · `POST /v1/agents/:id/trigger` | Agent control |
| `GET /v1/dispatch-jobs/{id}` | Poll a dispatched job |
| `POST /v1/sessions/register` · `POST /v1/sessions/{id}/heartbeat` · `POST /v1/sessions/{id}/end` | Session lifecycle |
| `GET /v1/sessions/presence` | Active-session roster |
| `POST /v1/handoffs/offer` · `POST /v1/handoffs/{id}/claim` · `POST /v1/handoffs/{id}/complete` · `GET /v1/handoffs` | Handoffs between sessions (first claim wins) |
| `GET /v1/claude-code/projects` · `POST /v1/claude-code/spawn` | List Claude Code projects and sessions; spawn or resume one |
| `GET /v1/bus/:id/events/stream` | SSE stream of bus events |
| `GET /health` | Liveness (identity, state, trust); no grant needed |
| `GET /` | Embedded dashboard |
| `POST /mcp` · `DELETE /mcp` | MCP Streamable HTTP |

### MCP tools

| Group | Tools |
|-------|-------|
| **Observability** | `cog_read_ledger`, `cog_search_traces`, `cog_tail_kernel_log`, `cog_tail_events`, `cog_read_events`, `cog_vitals_window` |
| **Conversations and tool calls** | `cog_read_conversation`, `cog_read_tool_calls`, `cog_tail_tool_calls`, `cog_search_conversations`, `cog_get_conversation_turn`, `cog_list_conversations` |
| **Agents and dispatch** | `cog_list_agents`, `cog_get_agent_state`, `cog_trigger_agent_loop`, `cog_dispatch_to_harness`, `cog_poll_dispatch` |
| **Config** | `cog_read_config`, `cog_write_config`, `cog_rollback_config` |
| **Sessions and handoffs** | `cog_register_session`, `cog_heartbeat_session`, `cog_end_session`, `cog_list_sessions`, `cog_fork_session`, `cog_offer_handoff`, `cog_claim_handoff`, `cog_complete_handoff`, `cog_list_handoffs` |
| **Memory** | `cog_search_memory`, `cog_read_cogdoc`, `cog_write_cogdoc`, `cog_patch_frontmatter`, `cog_check_coherence`, `cog_memory_toc`, `cog_memory_index`, `cog_resolve_uri` |
| **Architecture docs** (decision records, found by name) | `cog_architecture_search`, `cog_architecture_list`, `cog_architecture_read`, `cog_architecture_resolve`, `cog_architecture_propose`, `cog_architecture_write`, `cog_architecture_audit`, `cog_architecture_project` |
| **Context and workspace** | `cog_assemble_context`, `cog_ingest`, `cog_query_field`, `cog_get_state`, `cog_emit_event`, `cog_read_file`, `cog_grep_files`, `cog_render_peer_awareness_packet` |
| **Experiments** | `cog_run_experiment`, `cog_list_experiments`, `cog_get_experiment_status`, `cog_pin_baseline` |
| **Catalog** | `cog_tool_search`, `cog_tool_invoke` |
| **Voice (Mod³ bridge)** | `mod3_speak`, `mod3_stop`, `mod3_voices`, `mod3_status`, `mod3_tail_logs`, `mod3_register_session`, `mod3_deregister_session`, `mod3_list_sessions` |

### Providers

Adapters for OpenAI-compatible servers (LM Studio and others), a supervised MLX
server, Anthropic, Claude via local OAuth, Claude Code, Codex, and pi, plus
vLLM scaffolding. New providers implement
[a small interface](docs/writing-a-provider.md); `default_options` in config
shapes requests per provider.

---

## CLI

```sh
cogos init --workspace ~/my-project   # create the .cog/ overlay
cogos serve                            # run the daemon in the foreground
cogos start | stop | restart | status  # manage the background daemon
cogos health | doctor                  # liveness, and a full diagnostic
cogos reconcile <type> [--dry-run]     # run or preview one reconciler
cogos self-update                      # update from GitHub releases
cogos emit ...                         # write an event through the kernel
cogos logs | version | mcp | agents    # and more: cogos help
```

`scripts/cog` is a thin wrapper that finds the workspace from your current
directory and forwards to `cogos`.

---

## Getting started

### Requirements

- Go 1.25+
- macOS or Linux. Windows binaries build and run, but Windows is not a
  supported daemon target (no FTS5 search in that build).

### Build and run

```sh
git clone https://github.com/myrgic/cogos.git
cd cogos
make build

./cogos init --workspace ~/my-project
./cogos serve --workspace ~/my-project

curl -s http://localhost:6931/health | jq .
```

### Cross-compile

```sh
make linux-amd64
make linux-arm64
make darwin-arm64
make darwin-amd64
make windows-amd64
```

### Route Anthropic-API clients through the kernel

```sh
export ANTHROPIC_BASE_URL=http://localhost:6931
export ANTHROPIC_API_KEY="$(cat ~/.cog/vault/node-root-grant)"
claude
```

### Developer setup

```sh
./scripts/setup-dev.sh    # build, install to ~/.cog/bin, configure PATH
make hooks                # install the repo's git hooks
```

### Docker

```sh
make image        # build the production image
make run          # run with a workspace volume mount
make e2e          # build and run the full cold-start test in a container
```

---

## Logs and troubleshooting

The kernel writes structured logs (JSON, one record per line) to
`<workspace>/.cog/run/kernel.log.jsonl`:

```sh
tail -f /your/workspace/.cog/run/kernel.log.jsonl | jq -c .
```

The same log is available over MCP (`cog_tail_kernel_log`) and HTTP
(`GET /v1/kernel-log`). `cogos doctor` checks build tags, providers, endpoints,
and credentials in one pass.

---

## Testing

```sh
make test         # unit tests (with -race)
make e2e-local    # full cold-start lifecycle test
make e2e          # containerized e2e (Docker)
```

---

## Project layout

```
cmd/cogos/              Entry point (thin; delegates to internal/engine)
internal/engine/        Kernel: API, context assembly, reconcile daemon, agent
internal/providers/     Reconcilers (site, self-update, pin, vitals, ...)
internal/conversations/ Conversation archive and its reconciler
pkg/                    Importable library packages (go.work multi-module)
sdk/                    Go SDK for CogOS clients
docs/                   Specs, design records, provider guide
scripts/                Setup, CLI wrapper, e2e tests
```

---

## Status

**v3 kernel**, in daily use on the author's machines across Claude Code, Codex,
Hermes, and voice.

### Working

- Reconcile daemon with backoff, quarantine, anomaly episodes, and a
  convergence endpoint; the reconcilers listed at the top of this file
- Self-update from GitHub releases, driven by its own reconciler
- Continuous process loop with four states
- Context assembly on every prompt
- Hash-chained ledger with chain verification
- Three observability lanes plus a live event bus
- Conversation persistence and a searchable conversation archive
- Multi-provider inference routing with local-first preference
- MCP Streamable HTTP server, with a searchable tool catalog
- Sessions and handoffs with atomic claims
- Anthropic Messages proxy
- In-process maintenance agent, escalated to only when something is unhealthy
- Grant-based auth on inference and MCP routes
- Embedded dashboard
- OpenTelemetry instrumentation
- End-to-end test suite

### Next

- Leases, so experiments can borrow a managed resource without editing config
- Wiring the `myrgic/constellation` trust protocol through the kernel's
  `ConstellationBridge` seam (the seam exists; the external peer protocol is not
  yet connected)
- Multi-agent process management (the controller API is ready; only the
  `primary` agent is registered today)
- A human-readable view and revert over the ledger

---

## Ecosystem

| Repo | Purpose | Status |
|------|---------|--------|
| **[cogos](https://github.com/myrgic/cogos)** | The daemon (this repo) | Active |
| [constellation](https://github.com/myrgic/constellation) | Trust protocol between nodes: git-backed hash-chained ledger, ECDSA P-256 identity, signed heartbeats | Active |
| [mod3](https://github.com/myrgic/mod3) | Voice for agents: speech in and out, multiple TTS engines, turn-taking | Active |
| [plugins](https://github.com/myrgic/plugins) | Agent skills and plugins (Claude Code compatible) | Active |
| [charts](https://github.com/myrgic/charts) | Helm charts and Docker Compose for deployment | Active |

---

## Releases & Changelog

Per-release summaries are on the [Releases page](https://github.com/myrgic/cogos/releases),
generated from PR titles. [CHANGELOG.md](CHANGELOG.md) holds the policy and
entries before v0.4.0.

---

## Design documents

- [System Specification](docs/SYSTEM-SPEC.md): the whole system, from concepts to deployment
- [Architectural Principles](docs/architecture/principles.md): core engineering constraints
- [Design records](docs/adrs/): one file per decision, found by name
- [Writing a Provider](docs/writing-a-provider.md): adding an inference provider
- [MCP Specification](docs/MCP-SPEC.md): the MCP server contract
- [Provider Specification](docs/PROVIDER-SPEC.md): the provider interface
- [Architecture Diagrams](docs/architecture-diagram-source.md): kernel layers and topology
- [Cognitive GitOps](docs/architecture/cognitive-gitops.md): how repos coordinate through the workspace
- [E2E Test Plan](docs/E2E-TEST-PLAN.md): end-to-end test strategy

---

## License

[MIT](LICENSE). Copyright (c) 2025-2026 Chaz Dinkle.
