# Channels, Buses, and Tool Surfaces

Three I/O primitives sit on top of the content-addressed substrate. They are not interchangeable. Every ingress and egress in CogOS is one of the three, and the shape of the primitive determines its life-cycle, its governance, and its reconciler.

This doc refines principle 5 ("substrate is the bus"). That statement holds at the macro level. At the mechanism level, the **substrate is git primitives** and the **bus is the event/topic layer** resting on them.

## The Triad

| Kind | Shape | Peer | Life-cycle | Governance |
|------|-------|------|-----------|-----------|
| **Bus** | Event/topic (fact-shaped) | None — it *is* the medium | Always-on; infrastructural | System owns schema, ordering, delivery |
| **Channel** | Session (conversation-shaped) | Counterparty (human, remote, or internal subsystem) | Opens on attendance; closes when last attendant leaves | Medium contract partly owned by peer |
| **Tool surface** | Request/response (call-shaped) | Peer with a protocol | Reachable while up; no session | Peer owns protocol |

Axes:
- **Channel vs bus** — channel has a peer; bus does not.
- **Channel vs tool surface** — channel has a session and attendance; tool surface does not.
- **Bus vs tool surface** — bus is interior; tool surface is exterior.

### Edge case worth naming: tool surfaces with conversational character

Claude Code, reached through CogOS as an inference harness, is a **tool surface** by primitive — request/response, peer protocol, no CogOS-side session — but it *attends its own channel* (the one between Claude Code and Anthropic's LLM) while serving our tool call. That makes it a tool surface with conversational character: request/response from our side, session-like continuity on the other. The triad holds; the category boundary is just fuzzy in exactly this shape. Expect more of these as inference harnesses proliferate.

## Live Worked Example: The Gateway

As of April 2026, the primary production instance of the triad is the kernel process at port 6931 (MCP) plus the OpenAI-compatible API gateway.

An external OpenAI-compatible client (any SDK, IDE, or CLI) opens a session to the gateway. CogOS attends that session with its identity + capability layer. When the session needs inference, CogOS invokes Claude Code (or the Anthropic / OpenAI endpoints directly) as a tool surface. The gateway mediates — logs, gates, routes — without generating.

Mapped to the triad:
- **Channel** — the client's OpenAI-compatible session to the gateway. Terminates at CogOS, not at the inference backend. Client sustains one side; CogOS sustains the other.
- **Bus** — CogOS's internal mediation / routing / logging event stream during the session. Attention signals, tool-call traces, capability checks, ledger events.
- **Tool surface** — Claude Code (conversational-character tool surface), MCP-routed tools, direct Anthropic / OpenAI endpoints.

This shape matters: **the channel is not nested inside Claude Code's channel to its LLM**. The client ↔ CogOS channel is one channel; CogOS invoking Claude Code is a tool call from inside that channel. The identity + capability layer sits at the gate between them.

Net effect: a Claude Code subscription becomes portable to any OpenAI-compatible client through CogOS's gateway, and CogOS's identity + capability + tool-routing layer rides along.

## Channels Require Sustenance

A channel exists only while at least one entity attends it. Attendance is the scarce resource. Drop below one attendant on either side and the channel collapses — the ref still exists, the history is still addressable in the object store, but the live session is gone.

Two observable states:
- **Attended** — live session, high-bandwidth; events flowing into and out of the bus at human or agent cadence.
- **Dormant** — ref still exists, content still local (or a fetch away), but no events flow. Cheap to resume.

Re-opening a dormant channel is fast-forwarding a ref. No data is lost.

A channel can be opened *onto the bus itself*: a user spins up a synchronous interface point (a TUI, a dashboard, a REPL) and the far side is an internal observer that subscribes to bus topics and projects them into the channel's medium. The channel didn't exist before; the bus did. The user's presence sustains the projection.

## Git as Substrate

Git primitives apply to blobs in any storage or cache medium. Every I/O kind lands here:

| Concept | Git analog |
|---------|-----------|
| Content-addressed blob | `.git/objects/<sha>` |
| Bus event | commit |
| Bus topic | named subtree / tag namespace |
| Channel | ref (branch) — named pointer, attended or not |
| Channel history | DAG reachable from the ref |
| Tool surface | remote — addressable, fetch/push, no local presence |
| Consumer view | worktree — materialized layout for one tool's expectations |
| Storage tier | object store backend — where blobs physically live, addressed by hash |

Consequences:
- **Storage-medium independence.** A blob with hash `abc…` is the same blob on SSD, SD card, or remote. Moving it between tiers invalidates no ref.
- **Loose coupling of temporal streams.** Each agent loop is a branch. Shared ancestors are synchronization points.
- **Dormant channels are near-free.** A ref is a name and a hash. Unattended channels cost bytes, not cycles.
- **Attention is where energy goes.** Sample rate tracks attended channels; dormant channels sample on heartbeat (principle 3).

## Scale Invariance

Principle 6 makes resource life-cycle fractal (Fork/Merge/Die at every scale). The I/O triad is fractal too:

| Scale | Bus | Channel | Tool surface |
|------|-----|---------|-------------|
| Kernel | CogOS event bus | Gateway client sessions, REPL, Discord chats | Claude Code, Ollama, MCP peers |
| Repo | Substrate under `.cog/` | PR / review discussion | CI runners |
| Agent | Agent's own reasoning stream | Attended user channel | Tool calls the agent makes |
| CogBlock | Block-as-event | The channel that produced it | The source it was derived from |

Two fractal axes — #6 (resource life-cycle) and this doc (I/O kind). Any resource has both.

## Providers per Kind

Reconciliation providers specialize by I/O kind. Status as of April 2026:

**Implemented today (production):**
- **Tool providers** — `OllamaProvider`, `AnthropicProvider`, `OpenAIProvider` at `internal/engine/provider_*.go`. Stateless peer clients, exact tool-surface shape. Concrete-in-code reference implementations.

**Partially implemented:**
- **Bus** — modality-specific bus (`pkg/modality/bus.go`) for encoders/decoders; ledger-event bus (`internal/engine/bus_stream.go`) that publishes to SSE subscribers. Not yet a unified topic/pub-sub layer.

**Forward-looking (design target for the Go harness):**
- **Channel providers** — `discord_provider`, `repl_provider`, `gateway_provider`, `watch_tui_provider`. Would maintain refs to their channels; fast-forward as events arrive. Ingress adapters commit bus events; egress adapters consume bus events and emit to the peer.
- **Additional tool providers** — `hf_provider`, `mcp_provider`, inference backends (local Ollama, MLX) for when the Go harness handles generation internally rather than routing to Claude Code.

Bus is not a reconciled resource. It is a service — depended upon, not declared. Topics are schema; the bus itself is the substrate's always-on face.

## Temporal Streams

Agents with sustained asynchronous loops synchronize via the bus. Anticipatable sync points are named bus events — shared refs that multiple loops fast-forward to:

- `reconcile.tick` — scheduler heartbeat
- `user.message` on an attended channel — user-driven sync point
- `deadline.reached` — scheduled wake-up
- `consolidation.window` — memory reconciliation (see `identity-cycle.md` and ADR-081's `Receptive → Consolidating` transition)

A loop's position relative to others is its last observed offset on shared refs. Divergence is tolerated; rejoin is merge. Three-way merge handles clean cases; what doesn't merge cleanly escalates to attention-mediated resolution (which is itself a channel event).

## Symmetric Primitives

Users, LLMs, Discord peers, orchestrator subagents — all the same kind of thing at the substrate level: external cognitive observers that attend channels, emit events, read the bus. The *relationship* between them (authoritative / deferential / supervising / peer) is declarative — a role on the bus, not a type in the primitives.

Consequences:
- **The LLM is a peer, not a backend.** When inference routes through CogOS, it's opening a tool-surface path to another cognitive observer whose identity lives on someone else's infrastructure. The substrate treats it with the same primitives it treats any other observer: attention, transcription, ledger.
- **Multi-agent teams aren't a separate architectural concern.** The fourth agent is just another attendant.
- **CogOS is the continuity layer.** The LLM is stateless across requests; the user is stateful but temporally bound; peers have their own horizons. None individually provide continuity — the substrate does. Refs persist, blobs are content-addressed, conversations have identity across time in a way no single attendant does.

## Sharp Test Cases

| Surface | Kind | Why |
|--------|------|-----|
| OpenAI-compatible client ↔ CogOS gateway | Channel | External client sustains one side; CogOS sustains the other; identity/capability layer is the gate |
| Clog Code REPL | Channel | User attends; TTY is the session; close TTY, channel collapses |
| `watch` TUI (any reconciler) | Channel | User-opened projection onto the bus; closes with the user's session |
| Memory-write event | Bus | Fact; no peer; many subscribers |
| `agent.heartbeat` | Bus | Sync primitive for sibling loops |
| Claude Code as inference harness | Tool surface, conversational character | Request-response from CogOS's side; its own session on the other side |
| Ollama HTTP API | Tool surface | Request-response; peer protocol; no session |
| MCP server over stdio | Tool surface | Same |
| Agent-to-agent DM | Channel if session + turn-taking; bus if fire-and-forget | Same pair can use both simultaneously |

## Related Principles and ADRs

- **Principle 1 (Information in the delta)** — the bus is where deltas are named as events; each event commits a distinction.
- **Principle 3 (Adaptive sampling)** — sample rate tracks attendance on channels and volatility on bus topics.
- **Principle 5 (Stigmergic coordination)** — "substrate is the bus" holds macro; this doc refines at the mechanism layer.
- **Principle 6 (Scale invariance)** — the I/O triad is scale-invariant in the same way Fork/Merge/Die is.
- **Principle 8 (Identity changes by being read)** — every channel attendance is a transcription event that modifies the attendant's identity.
- **ADR-011 (Kernel as Cognitive DNA)** — canonical identity as the shared substrate every embodiment reads from; anchor for the identity cycle story.
- **ADR-062 (Recursive Node Architecture)** — scale at which the triad repeats per node.
- **ADR-074 (Nested Sovereignty)** — scope boundaries that channels must respect.
- **ADR-081 (Homeostatic Kernel Loop)** — running process that attends, routes, and consolidates; `Receptive → Consolidating` transition is where the reverse-transcription pathway lives.

For the four-cycle identity framing (replication / transcription / translation / reverse transcription) that extends ADR-011 and ADR-081, see `identity-cycle.md`.
