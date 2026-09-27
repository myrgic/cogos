// mcp_server.go — MCP Streamable HTTP server for CogOS v3
//
// Embeds the MCP server into the existing HTTP server at /mcp.
// Registers 11 MCP tools and 3 MCP resources. Four former tools
// (resolve_uri, get_trust, get_nucleus, get_index) are no longer
// registered as MCP tools but their implementations remain — used
// by the internal tool loop (tool_loop.go).
//
// Resources (read-only addressable data):
//   - cogos://state   — kernel process state
//   - cogos://nucleus — identity context
//   - cogos://field   — attentional field (top-20)
//
// Transport: Streamable HTTP (MCP spec 2025-03-26)
// Endpoint: POST/GET /mcp
package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/myrgic/cogos/pkg/pathsafe"
	"gopkg.in/yaml.v3"
)

// MCPServer wraps the MCP server and its dependencies.
type MCPServer struct {
	server          *mcp.Server
	handler         http.Handler
	cfg             *Config
	nucleus         *Nucleus
	process         *Process
	cogdocSvc       *CogDocService
	agentController AgentController // optional; nil when the kernel has no live agent

	// Session management backends (see SetSessionsBackend). Optional —
	// NewMCPServer without a live HTTP server skips wiring and the
	// session tools return a graceful "not configured" error.
	busSessions     *BusSessionManager
	sessionRegistry *SessionRegistry
	handoffRegistry *HandoffRegistry

	// mod3Proxy backs the mod3_* MCP tools (Wave 3 of the mod3-kernel
	// integration). Lazily initialised — tests can pre-seed it with a
	// custom HTTP client + stub player to avoid real network + audio.
	mod3Proxy *modalityProxy

	// channelSessionBackend is the kernel Server whose
	// RegisterChannelSession / DeregisterChannelSession /
	// ListChannelSessions methods own session-ID authority (ADR-082
	// Wave 2). Wave 3.5 routes the three mod3 session-family MCP tools
	// through these shared methods so minting happens in exactly one
	// place. Nil when the MCP server is built outside a live kernel —
	// the tools return a clean "not configured" error in that case.
	channelSessionBackend channelSessionBackend

	// toolMeta is the manifest-introspection registry for MCP tools.
	// Populated by trackTool/trackToolDeferred at registration time (see
	// serve_manifest.go); read by handleManifest. Frozen after registerTools
	// returns, so lock-free reads are safe.
	toolMeta []mcpToolMeta

	// deferredHandlers is the plumbing catalog (porcelain/plumbing tool
	// surface, workflow wkweyu50g): tools registered via trackToolDeferred
	// instead of mcp.AddTool, keyed by tool name. These never appear in
	// tools/list; they are reachable only through cog_tool_search (discover)
	// and cog_tool_invoke (dispatch). Populated during registerTools() and
	// frozen afterwards — lock-free reads are safe post-construction, same
	// contract as toolMeta.
	deferredHandlers map[string]deferredToolHandler

	// toolDefs is the cached snapshot of MCP tools as kernel-side
	// [ToolDefinition] values, suitable for direct injection onto a
	// [CompletionRequest.Tools] slice. Populated at construction by
	// snapshotToolDefinitions (which does an in-process ListTools) and
	// re-populated by ApplyExtensions once extension tools (conversations,
	// eval) are registered — read by handleChat when the kernel-agent path
	// needs to advertise its own tools, and by IsInternalTool/CallTool to
	// decide whether a tool call executes in-process. Read-only after
	// ApplyExtensions returns (or after construction, for servers that never
	// call ApplyExtensions, e.g. extension-free tests).
	// See mcp_tool_defs.go and myrgic/cogos#89.
	toolDefs []ToolDefinition

	// forkRegistry is the derived in-memory view of fork relationships.
	// Wired by SetForkRegistry. Nil-safe — fork operations work without it
	// but the lineage projection will be empty until it is wired.
	forkRegistry *ForkRegistry

	// kvBlockHashProvider is optionally implemented by inference providers
	// that expose content-addressed KV blocks (RFC-0006 vLLM provider).
	// Nil-safe — fork handler degrades to cold start when nil.
	kvBlockHashProvider KVBlockHashProvider

	// harnessBackend is the RBAC layer that creates/resolves HarnessBindingCRDs
	// when cog_register_session receives an optional "subject" field. Nil-safe:
	// when not wired, session register proceeds without creating any binding
	// (naked-by-default contract). Set via SetHarnessBackend (mcp_sessions_identity.go).
	harnessBackend HarnessAttacher

	// correlation is the transport↔harness session correlation store (G2 PART A).
	// Populated by toolRegisterSession: transport_session_id → {harness_session_id,
	// subject}. Read by withToolObserver and toolIngest for per-session attribution.
	// sync.Map zero-value is ready-to-use; no initialisation needed.
	correlation transportCorrelationStore

	// capResolver gates tool calls against the bound identity's capability
	// envelope when IdentityNakedDefault is true (G2 PART C). Nil when not
	// wired — gates are skipped (permit-by-default). Set via SetCapabilityResolver.
	capResolver capabilityGater

	// clusterRouter is the Phase 2 S4 BEP dispatch router. Non-nil only when
	// cluster.enabled=true and the BEPEngine started successfully. When set,
	// cog_dispatch_to_harness with target_node routed through here instead of
	// running locally. Set via SetClusterRouter.
	clusterRouter RemoteDispatchRouter

	// kernelProber is the per-instance TTL cache for the cog://kernel/status
	// resource (RFC Phase 1). Each MCPServer has its own prober so that test
	// instances don't share probe state. See mcp_kernel_status.go.
	kernelProber kernelStatusProber

	// constellationIndexer is the optional eager-upsert handle wired from the
	// root package at construction time via SetConstellationIndexer.  The
	// concrete type is *constellation.Constellation; the interface lives in
	// cogdoc_service.go so internal/engine does not import sdk/constellation
	// directly (package-boundary guard).  When non-nil, also stored in
	// pkgFTSRepairIndexer so the free-function lazy-repair path can reach it.
	constellationIndexer ConstellationIndexer

	// dispatchJobs backs the async cog_dispatch_to_harness path (Async=true
	// on dispatchToHarnessInput) and GET /v1/dispatch-jobs/{id}. Constructed
	// unconditionally in NewMCPServerWithAgentController — never nil — so
	// toolDispatchToHarness and the HTTP poll handler don't need a nil check
	// before every use. See dispatch_jobs.go.
	dispatchJobs *DispatchJobRegistry
}

// channelSessionBackend is the narrow surface the mod3 session-family MCP
// tools need to forward through the kernel's shared minting/forwarding
// logic. Interface rather than concrete *Server so tests can inject a fake
// without building a whole Server.
type channelSessionBackend interface {
	RegisterChannelSession(ctx context.Context, req channelSessionRegisterRequest) (*channelSessionResponse, *channelSessionForwardError)
	DeregisterChannelSession(ctx context.Context, sessionID string) (json.RawMessage, int, *channelSessionForwardError)
	ListChannelSessions(ctx context.Context) (*channelSessionListResponse, int, *channelSessionForwardError)
}

// NewMCPServer creates and configures the MCP server with all stage-1 tools.
// The returned server has no AgentController attached. Call SetAgentController
// to enable cog_list_agents / cog_get_agent_state / cog_trigger_agent_loop.
func NewMCPServer(cfg *Config, nucleus *Nucleus, process *Process) *MCPServer {
	return NewMCPServerWithAgentController(cfg, nucleus, process, nil)
}

// NewMCPServerWithAgentController creates the MCP server and attaches an
// AgentController for the agent-state tools. The controller may be nil;
// the tools remain registered and return a "not configured" response in
// that case so clients get a consistent error shape.
func NewMCPServerWithAgentController(cfg *Config, nucleus *Nucleus, process *Process, ctrl AgentController) *MCPServer {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "cogos",
		Version: BuildTime,
	}, nil)

	m := &MCPServer{
		server:           server,
		cfg:              cfg,
		nucleus:          nucleus,
		process:          process,
		cogdocSvc:        NewCogDocService(cfg, process),
		agentController:  ctrl,
		deferredHandlers: make(map[string]deferredToolHandler),
		dispatchJobs:     NewDispatchJobRegistry(),
	}

	m.registerTools()
	m.registerResources()

	// Backfill InputSchema onto eager toolMeta entries (see trackTool's doc
	// comment: mcp.AddTool's schema inference never writes back onto the
	// original *mcp.Tool pointer, so this queries the live server the same
	// way snapshotToolDefinitions does, immediately below).
	m.backfillEagerSchemas()

	// Snapshot the registered tool list as kernel-side ToolDefinitions so the
	// chat path can auto-advertise the kernel's MCP tool surface to the
	// inference provider when the request targets the kernel-agent route
	// without supplying its own tools (myrgic/cogos#89). Best-effort: a
	// snapshot failure logs a warning and leaves m.toolDefs nil — chat falls
	// back to the previous behavior (no tools advertised) without breaking.
	m.toolDefs = snapshotToolDefinitions(server)

	m.handler = mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server { return server },
		nil,
	)

	return m
}

// SetAgentController wires a live AgentController into an already-built
// MCPServer. Safe to call after construction; the tool registration is
// unchanged because the tools resolve the current controller on each call.
func (m *MCPServer) SetAgentController(ctrl AgentController) {
	m.agentController = ctrl
}

// SetClusterRouter wires the Phase 2 S4 BEP dispatch router so that
// cog_dispatch_to_harness calls with a non-empty target_node are forwarded to
// the named peer over the authenticated BEP channel. Nil is the default (no
// cluster transport); the tool returns a clear error when target_node is set
// but no router is wired.
func (m *MCPServer) SetClusterRouter(r RemoteDispatchRouter) {
	m.clusterRouter = r
}

// SetChannelSessionBackend wires the kernel-owned channel-session minting
// logic into the MCP server so the mod3_register_session / _deregister /
// _list tools call through the same shared methods the HTTP surface uses
// (ADR-082 Wave 3.5). Safe to pass nil; the tools surface a clean "not
// configured" error in that case.
func (m *MCPServer) SetChannelSessionBackend(b channelSessionBackend) {
	m.channelSessionBackend = b
}

// SetConstellationIndexer wires a live ConstellationIndexer into the MCP
// server so that CogDocService.WriteAndSync / PatchAndSync trigger an eager
// per-file FTS upsert, and so that the lazy drift-repair path in
// searchMemoryFTSDriftRepair can call IndexFile without importing
// sdk/constellation (package-boundary guard #2 in cogdoc_service.go:22).
// Also propagates to the package-level pkgFTSRepairIndexer used by the
// free-function search path.  Safe to call with nil (disables eager upsert
// and drift repair; correct degraded mode for tests and CLI paths).
func (m *MCPServer) SetConstellationIndexer(c ConstellationIndexer) {
	m.constellationIndexer = c
	m.cogdocSvc.WithConstellationIndexer(c)
	// Expose to the free-function lazy-repair path (searchMemoryFTSDriftRepair).
	pkgFTSRepairIndexer = c
}

// Handler returns the http.Handler for mounting at /mcp.
func (m *MCPServer) Handler() http.Handler {
	return m.handler
}

// Server returns the underlying *mcp.Server for use by extension hooks
// (e.g. eval.RegisterEvalTools) that call mcp.AddTool directly.
// Extensions must be registered before Handler() is first called — the
// intended call site is RegisterMCPExtensions, which fires inside
// registerMCPRoutes before h := mcpSrv.Handler().
func (m *MCPServer) Server() *mcp.Server {
	return m.server
}

// registerTools registers MCP tools.
// Design: tools are actions with side effects or non-trivial computation.
// Read-only state queries will migrate to MCP Resources in Phase 2.
//
// Porcelain/plumbing tool surface (workflow wkweyu50g, tier-then-trim
// mechanism doc cog:mem/semantic/architecture/mcp-tool-surface-tier-then-trim
// #Mechanism, RESOLVED): only the ~14-tool porcelain set below is registered
// via mcp.AddTool (trackTool) — the set that appears in tools/list and is
// eagerly visible to every harness. Everything else (the plumbing) routes
// through trackToolDeferred into m.deferredHandlers instead, reachable only
// via cog_tool_search (discover) and cog_tool_invoke (dispatch). This is a
// server-autonomous mechanism: the kernel cannot ask Claude Code (or any
// other harness) to defer-load specific MCP tools, so it shrinks its own
// advertised schema surface instead of relying on client cooperation.
//
// Every eager handler is wrapped with withToolObserver so an invocation
// emits a paired tool.call + tool.result event to the hash-chained ledger.
// This closes Agent F gap #6 and activates the gate.go:94 recognizer that
// has been waiting for a producer. Deferred (plumbing) handlers keep their
// original withToolObserver wrapping too — trackToolDeferred's In type
// parameter is inferred from the wrapped handler's signature, so converting
// a registration site from trackTool/mcp.AddTool to trackToolDeferred does
// not change what runs, only how it's reached.
func (m *MCPServer) registerTools() {
	// ── Porcelain (eager, mcp.AddTool) ──────────────────────────────────────

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_search_memory",
		Description: "Full-text and semantic search over the CogDoc memory corpus. Returns ranked results with salience scores. Fallback: ./scripts/cog memory search \"query\"",
	}), withToolObserver(m, "cog_search_memory", m.toolSearchMemory))

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_read_cogdoc",
		Description: "Read a CogDoc by URI or path. Resolves cog: URIs automatically. Returns full content with parsed frontmatter and optional section extraction via #fragment. Fallback: ./scripts/cog memory read <path>",
	}), withToolObserver(m, "cog_read_cogdoc", m.toolReadCogdoc))

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_write_cogdoc",
		Description: "Write or update a CogDoc at the specified memory path. Creates the file with proper frontmatter if it doesn't exist. Fallback: ./scripts/cog memory write <path> \"Title\"",
	}), withToolObserver(m, "cog_write_cogdoc", m.toolWriteCogdoc))

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_check_coherence",
		Description: "Run coherence validation against the workspace. Checks URI resolution, frontmatter validity, and reference integrity. Fallback: ./scripts/cog coherence check",
	}), withToolObserver(m, "cog_check_coherence", m.toolCheckCoherence))

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_get_state",
		Description: "Get kernel state: process status, uptime, trust, node health (sibling services), field size, and heartbeat info. Includes identity and coherence metadata. Fallback: kernel API endpoint /health (local port 6931)",
	}), withToolObserver(m, "cog_get_state", m.toolGetState))

	mcp.AddTool(m.server, m.trackTool(&mcp.Tool{
		Name:        "cog_dispatch_to_harness",
		Description: "Phase 2 transport: dispatch a task into the kernel-interior agent harness with structured return, optional concurrency (n=1..4), named tool scope, per-call tool narrowing, optional system-prompt override, and pluggable model routing (default: the resolved provider's configured default model — provider resolution follows harness_provider config and process-state routing; explicit provider/model params override). Synchronous by default: blocks until every slot completes, errors, or hits its per-slot timeout (default 240s; max = the kernel's dispatch timeout cap, dispatch_timeout_cap_seconds in kernel.yaml, default 600s — over-cap requests are rejected before any slot runs). Returns one DispatchResult per slot with content (the agent's final respond text), tool-call digests, duration, and turn count. Set async=true to instead get an immediate {job_id, status:\"pending\"} receipt and poll via cog_poll_dispatch or GET /v1/dispatch-jobs/{id} — use this when the dispatch may outlive the caller's own request window. Use this for the foveal->peripheral handoff — offload validation, rewriting, modality matching to the resident swarm instead of paying with Anthropic tokens. Named scopes (RFC-016): \"consolidation\" (default, 11 substrate tools: memory/field/coherence/identity; respond excluded from base), \"consolidation_with_respond\" (+ respond), \"consolidation_no_respond\" (no respond), \"audit\" (consolidation + cog_read_file + cog_grep_files), \"search\" (cog_resolve_uri/cog_search_memory/cog_query_field), \"observe\" (consolidation reads, no emit), \"emit\" (cog_emit_event only). Unknown scope names error immediately. Backward-compat note: cog_trigger_agent_loop still wakes the singleton with no payload; this tool is the new payload-bearing path.",
	}), m.toolDispatchToHarness)

	// Kernel-native session/handoff tools (mcp_sessions.go). The porcelain
	// subset (register/list/heartbeat/end/list_handoffs/claim_handoff) is
	// registered eagerly inside registerSessionTools; the rest
	// (offer_handoff, complete_handoff, fork_session) is deferred there too.
	m.registerSessionTools()

	// ── Plumbing (deferred, trackToolDeferred) ──────────────────────────────

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_patch_frontmatter",
		Description: "Merge description, tags, or type patches into a CogDoc frontmatter block.",
	}, withToolObserver(m, "cog_patch_frontmatter", m.toolPatchFrontmatter))

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_query_field",
		Description: "Query the attentional field — the salience-scored map of all tracked CogDocs. Returns top-N items, optionally filtered by sector. Shows what the kernel considers most relevant right now.",
	}, withToolObserver(m, "cog_query_field", m.toolQueryField))

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_assemble_context",
		Description: "Build a context package for a given token budget with an explicit focus topic. Use this for intentional context assembly (subtasks, specific investigations). The automatic foveated-context hook handles ambient context on every prompt.",
	}, withToolObserver(m, "cog_assemble_context", m.toolAssembleContext))

	trackToolDeferred(m, &mcp.Tool{
		Name: "cog_emit_event",
		Description: "Emit a typed event to the workspace ledger. " +
			"Events: attention.boost (uri + weight), session.marker (label), " +
			"insight.captured (summary), decision.made (decision + rationale), " +
			"peer.utterance (from + to + content + turn; both sessions must be registered). " +
			"Optional from_session: records the emitting session as event source; required for peer.utterance and must match payload.from. " +
			"Fallback: events are JSONL in .cog/ledger/",
	}, withToolObserver(m, "cog_emit_event", m.toolEmitEvent))

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_read_ledger",
		Description: "Read the hash-chained event ledger. Filter by session_id, event_type (exact or 'prefix.*' wildcard), after_seq (requires session_id), since_timestamp (RFC3339), or limit (default 100, max 1000). Set verify_chain=true to recompute hashes and validate prior_hash links. Fallback: cat .cog/ledger/<session_id>/events.jsonl",
	}, m.toolReadLedger)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_read_events",
		Description: "Query recent kernel events for observability. Returns historical events from the ledger without chain verification (use cog_read_ledger for audit). Filters: session_id, event_type (exact or 'attention.*' wildcard), source ('kernel-v3' / 'mcp-client'), since/until (RFC3339 or duration shorthand like '5m'), limit (default 100, max 1000), order ('desc' newest-first default, 'asc' oldest-first). Fallback: ls .cog/ledger/ && cat .cog/ledger/*/events.jsonl",
	}, m.toolReadEvents)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_tail_events",
		Description: "Tail kernel events as they are appended to the ledger, like 'tail -f'. Blocks until max_events or max_duration reached. Same filters as cog_read_events plus since= for replay before going live. Bounded by max_events (default 100, max 1000) and max_duration (default 60s, max 10m). Fallback: kernel API streaming endpoint /v1/events/stream (local port 6931)",
	}, m.toolTailEvents)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_vitals_window",
		Description: "Query retained node-vitals history (RFC-040 S2): host gauges (disk_free_bytes, mem_free_bytes, load1, uptime_seconds) and provider-health counters (providers_healthy/degraded/missing/suspended, anomalies, anomalies_total) sampled once per autonomic tick. Exactly one query shape — window(metric, since, resolution) — no DSL, no cross-metric joins. Required: metric (see the list above), since (RFC3339 timestamp or duration shorthand like '24h', subtracted from now; capped at 2 years back), resolution ('raw' for tick-resolution points, '5m' or '1h' for downsampled aggregates with min/max/count). A metric/window with no recorded data returns an empty points array, not an error — this is a pull-only history store (RFC-040 N1: nothing here is pushed into agent context). Fallback: GET /v1/vitals?metric=&since=&resolution= (local port 6931)",
	}, m.toolVitalsWindow)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_ingest",
		Description: "Ingest external material into CogOS knowledge. Deterministic decomposition — no LLM calls. Supports URLs, conversations, documents. Applies membrane policy (accept/quarantine/defer/discard).",
	}, withToolObserver(m, "cog_ingest", m.toolIngest))

	// Tool-call observability — reads the paired tool.call/tool.result events
	// the wrapper above emits. Self-reflective: these two tools also go
	// through withToolObserver and end up in their own query results.
	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_read_tool_calls",
		Description: "Query recent tool invocations and their outcomes. Returns call+result pairs from the ledger, filterable by tool_name, status (pending/success/error/rejected/timeout), source, ownership, call_id, or time window. Default limit 100, max 500. Arguments and output are opt-in via include_args/include_output. Fallback: grep '\"type\":\"tool\\.' .cog/ledger/<sid>/events.jsonl",
	}, withToolObserver(m, "cog_read_tool_calls", m.toolReadToolCalls))

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_tail_tool_calls",
		Description: "Tail tool-call events live. Replays recent tool.call / tool.result events (up to max_events, default 50), applying the same filters as cog_read_tool_calls. When Agent N's event bus lands, this will stream new events live; until then it returns a snapshot of the latest matching rows. Fallback: tail -f .cog/ledger/<sid>/events.jsonl | grep '\"type\":\"tool\\.'",
	}, withToolObserver(m, "cog_tail_tool_calls", m.toolTailToolCalls))

	// Agent state / loop control — closes Agent F gap #8 per Agent T's design.
	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_list_agents",
		Description: "Enumerate active agent harness instances inside the kernel. Each entry summarises identity, state, and recent activity. Today returns one element (\"primary\") reflecting the ServeAgent singleton; forward-compatible for future multi-agent deployment. Fallback: kernel API endpoint /v1/agents (local port 6931)",
	}, m.toolListAgents)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_get_agent_state",
		Description: "Full state snapshot of one agent instance — status summary, activity awareness, rolling cycle memory, pending proposals, inbox queue, and optionally the most recent cycle traces. Matches the shape of GET /v1/agents/{id}. Fallback: kernel API endpoint /v1/agents/primary (local port 6931)",
	}, m.toolGetAgentState)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_trigger_agent_loop",
		Description: "Manually invoke one homeostatic cycle of the specified agent, outside the regular ticker. Equivalent to POST /v1/agents/{id}/tick. Returns immediately with a trigger receipt; cycle runs async unless wait=true. Refuses if a cycle is already in flight (overlap guard). Fallback: kernel API POST /v1/agents/primary/tick (local port 6931)",
	}, m.toolTriggerAgentLoop)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_poll_dispatch",
		Description: "Poll the status of an async cog_dispatch_to_harness job (one issued with async=true). Returns {job_id, status: pending|running|done|failed, cycle_id, result, error, created_at, updated_at}. result/error populate only once status is terminal (done/failed). Jobs are retained for a bounded window after completion (~30m) then lazily reclaimed — an unknown job_id after that window returns a clear not-found error. Convenience wrapper; the primary poll surface is GET /v1/dispatch-jobs/{id} (local port 6931).",
	}, m.toolPollDispatch)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_tail_kernel_log",
		Description: "Read recent entries from the kernel's own diagnostic log (slog JSON at .cog/run/kernel.log.jsonl). Returns newest-first, optionally filtered by level, substring, and time range. This is the OPERATOR/DEBUG surface — for hash-chained event history use cog_read_ledger (when available); for client metabolites (turn metrics, attention, proprioceptive) use cog_search_traces. Fallback: tail -n 100 .cog/run/kernel.log.jsonl | jq -c .",
	}, m.toolTailKernelLog)

	trackToolDeferred(m, &mcp.Tool{
		Name: "cog_read_conversation",
		Description: "Read conversation turns (prompt + response pairs) from a session's chat history. " +
			"Each turn is a complete user-to-assistant exchange; kernel tool calls are inlined when include_tools=true. " +
			"Backed by the turn.completed ledger event + per-session sidecar (.cog/run/turns/<sid>.jsonl). " +
			"Use after_turn / before_turn for pagination. Default: current process session, 20 turns, ascending. " +
			"Fallback (kernel unavailable): jq -c . .cog/run/turns/<sid>.jsonl",
	}, m.toolReadConversation)

	// Config mutation API (Agent O design — closes Agent F gaps #5 + #19).
	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_read_config",
		Description: "Read the kernel config (.cog/config/kernel.yaml). Returns the effective resolved config (defaults + file overrides). Optional include_raw_yaml returns the raw file bytes; include_defaults also returns the hardcoded defaults for diffing. kernel.yaml only — sibling configs (providers.yaml, secrets.yaml) are out of scope. Gated by Config.EnableConfigMutation (default false, parity with the REST /v1/config surface); returns a disabled error when the gate is off.",
	}, m.toolReadConfig)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_write_config",
		Description: "Merge a patch into the kernel config (.cog/config/kernel.yaml) using RFC 7396 JSON merge-patch semantics: fields omitted from the patch are left unchanged; explicit null removes a field and restores the default on next boot. Validated before persisting — returns violations without writing on failure. Atomic write + rotating .bak-<timestamp> backups (keeps 10). Takes effect on next daemon restart (requires_restart: true in response). Fallback: edit .cog/config/kernel.yaml and run `./scripts/cog restart`. Gated by Config.EnableConfigMutation (default false); set enable_config_mutation: true in kernel.yaml to opt in. No further authentication beyond the gate — the kernel assumes a trusted local caller once enabled.",
	}, m.toolWriteConfig)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_rollback_config",
		Description: "Restore kernel.yaml from a prior .bak-<timestamp> backup. Pass list_only=true to enumerate available backups without restoring. If backup is empty, the most recent backup is used. Atomic restore; response carries updated backup list. Gated by Config.EnableConfigMutation (default false), same as cog_read_config/cog_write_config.",
	}, m.toolRollbackConfig)

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_search_traces",
		Description: "Search kernel trace JSONL streams in .cog/run/ (attention, proprioceptive, internal_requests). Filter by source, session_id, level, case-insensitive substring, and time range (since/until accept RFC3339 or duration like 5m/1h). Returns unified chronological results with per-source scan diagnostics. Fallback: ls .cog/run/*.jsonl && jq -c . .cog/run/<name>.jsonl | head",
	}, m.toolSearchTraces)

	// Memory section-index ops (RFC-017 Phase B). Port of the legacy
	// `cog memory toc` / `cog memory index` verbs from the 2.5.0 monolith
	// (cog-workspace/.cog/memory.go:670,718). Before these tools existed,
	// the v3 kernel could only surface section-aware reads by shelling
	// out to scripts/cog — so agents reaching the kernel via MCP had no
	// path to generate or inspect the `sections:` frontmatter block.
	// Without that block, every cog_read_cogdoc degrades to a full-doc
	// read. See Phase 24 MCP API Coverage Audit for the gap analysis.
	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_memory_toc",
		Description: "List a CogDoc's sections with line ranges and byte sizes. Default output is a pretty-printed text table showing total lines/bytes plus per-section nesting, line range, and size; pass as_yaml=true to get the `sections:` frontmatter YAML block that cog_memory_index injects. Useful for orienting to a long document before extracting a specific section via cog_read_cogdoc with #fragment. Fallback: ./scripts/cog memory toc <path> [--yaml]",
	}, withToolObserver(m, "cog_memory_toc", m.toolMemoryTOC))

	trackToolDeferred(m, &mcp.Tool{
		Name:        "cog_memory_index",
		Description: "Generate the `sections:` frontmatter block for a CogDoc and inject it into the file's frontmatter (replacing any prior sections field). Required for cheap section-addressed reads: without `sections:` every cog_read_cogdoc call with a section selector falls back to whole-document reads. Pass dry_run=true to preview the block without writing. Requires existing frontmatter; indexing a plain markdown file is a no-op by design (run cog_write_cogdoc first). Writes are atomic. Fallback: ./scripts/cog memory index <path> [--dry-run]",
	}, withToolObserver(m, "cog_memory_index", m.toolMemoryIndex))

	// Wave 3: mod3 proxy tools (mcp_modality_proxy.go). The kernel becomes
	// the MCP front door for mod3 — HTTP-forwards synthesis/stop/voices/
	// status and plays the returned audio/wav locally. Supersedes the
	// installed binary's OpenClaw gateway which silently drops audio bytes.
	// All mod3_* tools are plumbing (registerMod3Tools uses trackToolDeferred).
	m.registerMod3Tools()

	// Architecture skill plugin tools (mcp_architecture.go). 8 cog_architecture_*
	// tools that subprocess-exec the Python implementations at
	// {WorkspaceRoot}/.cog/skills/architecture/tools/architecture_*.py per the
	// architecture-memory-canonical-form-and-projection ADR. All plumbing
	// (registerArchitectureTools uses trackToolDeferred).
	m.registerArchitectureTools()

	// Audit-scope read-only filesystem tools. Deferred/plumbing — reachable
	// via cog_tool_search + cog_tool_invoke, or directly when a harness
	// dispatch narrows scope="audit" (mcp_server.go's toolDispatchToHarness
	// path resolves these by name, not via mcp.AddTool registration).
	trackToolDeferred(m, &mcp.Tool{
		Name: "cog_read_file",
		Description: "Read an arbitrary file within the workspace root. " +
			"res=abstract (default, ADR pointer-first): returns a pointer envelope — workspace-relative path, file size, " +
			"and a short excerpt — without dumping content into the agent context. " +
			"res=full: returns line-numbered content with optional offset/limit (default 500 lines). " +
			"Rejects paths outside the workspace root. Use for source inspection and substrate-introspection tasks. " +
			"Fallback: cat -n <path>",
	}, withToolObserver(m, "cog_read_file", m.toolReadFile))

	trackToolDeferred(m, &mcp.Tool{
		Name: "cog_grep_files",
		Description: "Regex search over files within the workspace root (uses ripgrep when available, falls back to pure-Go walk). " +
			"res=abstract (default, ADR pointer-first): returns match count + unique matched file paths — no line text. " +
			"res=full: returns per-match path/line/text entries. " +
			"Rejects search paths outside the workspace root. Fallback: rg --no-heading -n <pattern> <path>",
	}, withToolObserver(m, "cog_grep_files", m.toolGrepFiles))

	// Phase 1B: peer-awareness packet (READ side of the 4E ambient-
	// awareness loop; Phase 1A publishes channel.<sid>.activity).
	trackToolDeferred(m, &mcp.Tool{
		Name: "cog_render_peer_awareness_packet",
		Description: "Render a token-budgeted peer-awareness packet for a " +
			"session. Combines the session's recent tailer activity, open " +
			"handoffs, peer sessions whose attention overlaps, and recent " +
			"coord/impl chatter on bus_broadcast. Defaults: budget=500 " +
			"tokens, window=15m, include_peers=true. Returns {packet, " +
			"token_count, sources[]} — designed to be prepended verbatim to " +
			"a UserPromptSubmit preamble. Fallback: kernel API endpoint " +
			"/v1/peer-awareness?sid=<sid> (local port 6931)",
	}, withToolObserver(m, "cog_render_peer_awareness_packet", m.toolRenderPeerAwarenessPacket))

	// The porcelain/plumbing catalog itself: cog_tool_search (discover) and
	// cog_tool_invoke (dispatch) are the two new eager tools that front the
	// deferred plumbing set registered above.
	m.registerToolCatalogTools()
}

// registerResources registers MCP Resources — read-only addressable data.
// Unlike tools (actions with side effects), resources expose live kernel state
// that clients can read without triggering mutations.
func (m *MCPServer) registerResources() {
	m.server.AddResource(&mcp.Resource{
		URI:         "cogos://state",
		Name:        "Kernel State",
		Description: "Process state, uptime, trust, field size, and node health",
		MIMEType:    "application/json",
	}, m.resourceState)

	m.server.AddResource(&mcp.Resource{
		URI:         "cogos://nucleus",
		Name:        "Identity",
		Description: "Kernel identity context — name, role, summary",
		MIMEType:    "application/json",
	}, m.resourceNucleus)

	m.server.AddResource(&mcp.Resource{
		URI:         "cogos://field",
		Name:        "Attentional Field",
		Description: "Top-20 salience-scored CogDocs with cog:// URIs",
		MIMEType:    "application/json",
	}, m.resourceField)

	m.server.AddResource(&mcp.Resource{
		URI:         "cogos://config",
		Name:        "Kernel Config",
		Description: "Effective kernel configuration (kernel.yaml resolved against defaults). Gated by Config.EnableConfigMutation (default false), same as cog_read_config/cog_write_config/cog_rollback_config; returns a disabled error when the gate is off.",
		MIMEType:    "application/json",
	}, m.resourceConfig)

	m.server.AddResource(&mcp.Resource{
		URI:  "cog://kernel/status",
		Name: "Kernel Status",
		Description: "Local kernel reachability probe. Reads GET /health on the " +
			"configured port (default 6931) with a 2 s timeout; result is cached " +
			"for ~3 s. Shape: {reachable, endpoint, version, identity, node_id, " +
			"checked_at, latency_ms} when up; {reachable:false, endpoint, " +
			"checked_at, error} when down.",
		MIMEType: "application/json",
	}, m.resourceKernelStatus)
}

// ── Resource Handlers ───────────────────────────────────────────────────────

func (m *MCPServer) resourceState(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if m.process == nil {
		return nil, fmt.Errorf("process not initialized")
	}

	queue := ReadIngestionQueueState(m.cfg.WorkspaceRoot)
	trust := m.process.TrustSnapshot()
	lastHeartbeat := ""
	if !trust.LastHeartbeatAt.IsZero() {
		lastHeartbeat = trust.LastHeartbeatAt.Format(time.RFC3339)
	}

	identity := ""
	if m.nucleus != nil {
		identity = m.nucleus.Name
	}

	result := map[string]any{
		"state":             m.process.State().String(),
		"identity":          identity,
		"session_id":        m.process.SessionID(),
		"node_id":           m.process.NodeID,
		"uptime_seconds":    int(time.Since(m.process.StartedAt()).Seconds()),
		"field_size":        m.process.Field().Len(),
		"trust_score":       trust.LocalScore,
		"fingerprint":       m.process.Fingerprint(),
		"last_heartbeat":    lastHeartbeat,
		"coherence_state":   trust.CoherenceFingerprint,
		"quarantined_count": queue.Quarantined,
		"deferred_count":    queue.Deferred,
	}

	if nh := m.process.NodeHealth(); nh != nil {
		if summary := nh.Summary(); len(summary) > 0 {
			result["node"] = summary
		}
	}

	b, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(b),
		}},
	}, nil
}

func (m *MCPServer) resourceNucleus(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if m.nucleus == nil {
		return nil, fmt.Errorf("nucleus not loaded")
	}

	result := map[string]any{
		"name":      m.nucleus.Name,
		"role":      m.nucleus.Role,
		"summary":   m.nucleus.Summary(),
		"workspace": m.cfg.WorkspaceRoot,
		"port":      m.cfg.Port,
		"build":     BuildTime,
	}

	b, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal nucleus: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(b),
		}},
	}, nil
}

func (m *MCPServer) resourceField(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if m.process == nil || m.process.field == nil {
		return nil, fmt.Errorf("attentional field not initialized")
	}

	const limit = 20
	scores := m.process.field.AllScores()

	type entry struct {
		URI      string  `json:"uri"`
		Salience float64 `json:"salience"`
	}
	var entries []entry
	for absPath, score := range scores {
		uri := FieldKeyToURI(m.cfg.WorkspaceRoot, absPath)
		entries = append(entries, entry{URI: uri, Salience: score})
	}
	// Sort by salience descending.
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Salience > entries[i].Salience {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	if len(entries) > limit {
		entries = entries[:limit]
	}

	result := map[string]any{
		"count":   len(entries),
		"entries": entries,
	}

	b, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal field: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(b),
		}},
	}, nil
}

// ── Tool Inputs ──────────────────────────────────────────────────────────────

// resolveURIInput — no longer an MCP tool; used by the internal tool loop (tool_loop.go).
type resolveURIInput struct {
	URI string `json:"uri" jsonschema:"A cog: URI to resolve. Examples: cog:mem/semantic/architecture/x or cog://cog-workspace/adr/059"`
}

type queryFieldInput struct {
	Sector string `json:"sector,omitempty" jsonschema:"Filter by memory sector (semantic/episodic/procedural/reflective). Empty for all."`
	Limit  int    `json:"limit,omitempty" jsonschema:"Maximum number of results (default 20)"`
}

type assembleContextInput struct {
	Budget int    `json:"budget" jsonschema:"Token budget for the assembled context"`
	Focus  string `json:"focus,omitempty" jsonschema:"Optional focus topic to bias context selection"`
}

type checkCoherenceInput struct {
	Scope string `json:"scope,omitempty" jsonschema:"Scope of coherence check: structural (default)/navigational/canonical"`
}

type getStateInput struct {
	Verbose bool `json:"verbose,omitempty" jsonschema:"Include detailed field and process info"`
}

// getTrustInput — no longer an MCP tool; used by the internal tool loop (tool_loop.go).
type getTrustInput struct{}

type searchMemoryInput struct {
	Query  string `json:"query" jsonschema:"Search query string"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Maximum results (default 10)"`
	Sector string `json:"sector,omitempty" jsonschema:"Filter by memory sector"`
}

// getNucleusInput — no longer an MCP tool; used by the internal tool loop (tool_loop.go).
// Logic retained for C1 migration to MCP Resource.
type getNucleusInput struct {
	IncludeConfig bool `json:"include_config,omitempty" jsonschema:"Include workspace configuration details"`
}

type readCogdocInput struct {
	URI     string `json:"uri" jsonschema:"A cog: URI pointing to the CogDoc"`
	Section string `json:"section,omitempty" jsonschema:"Optional section name to extract (from #fragment)"`
}

type cogdocFrontmatterPatch struct {
	Description string   `json:"description,omitempty" jsonschema:"One-line summary for the CogDoc" yaml:"description,omitempty"`
	Tags        []string `json:"tags,omitempty" jsonschema:"Classification tags" yaml:"tags,omitempty"`
	Type        string   `json:"type,omitempty" jsonschema:"CogDoc type" yaml:"type,omitempty"`
}

type patchFrontmatterInput struct {
	URI     string                 `json:"uri" jsonschema:"A cog: URI pointing to the CogDoc"`
	Patches cogdocFrontmatterPatch `json:"patches" jsonschema:"Frontmatter fields to merge into the CogDoc"`
}

type writeCogdocInput struct {
	Path    string   `json:"path" jsonschema:"Memory-relative path (e.g. semantic/insights/topic.md)"`
	Title   string   `json:"title" jsonschema:"Document title for frontmatter"`
	Content string   `json:"content" jsonschema:"Markdown content to write"`
	Tags    []string `json:"tags,omitempty" jsonschema:"Optional tags for classification"`
	Status  string   `json:"status,omitempty" jsonschema:"Document status (active/raw/enriched/integrated)"`
	DocType string   `json:"type,omitempty" jsonschema:"Document type (insight/link/conversation/architecture/guide)"`
}

type readCogdocResult struct {
	URI              string            `json:"uri"`
	Path             string            `json:"path"`
	Fragment         string            `json:"fragment,omitempty"`
	Frontmatter      cogdocFrontmatter `json:"frontmatter,omitempty"`
	Content          string            `json:"content"`
	SchemaIssues     []string          `json:"schema_issues,omitempty"`
	PatchFrontmatter map[string]any    `json:"patch_frontmatter,omitempty"`
	SchemaHint       string            `json:"schema_hint,omitempty"`
}

// memoryTOCInput matches the legacy `cog memory toc <path> [--yaml]` CLI.
// Path resolution accepts absolute paths, workspace-relative paths, and
// memory-relative paths (semantic/insights/foo.cog.md) — handled by the
// underlying MemoryTOC function.
type memoryTOCInput struct {
	Path   string `json:"path" jsonschema:"Path to the CogDoc: absolute, workspace-relative, or memory-relative (e.g. semantic/insights/topic.cog.md)"`
	AsYAML bool   `json:"as_yaml,omitempty" jsonschema:"Return the raw sections: YAML block (matches cog memory toc --yaml) instead of the text table"`
}

// memoryIndexInput matches the legacy `cog memory index <path>
// [--dry-run]` CLI. Unlike the legacy --all mode (which walked every
// cogdoc under .cog/mem/), this tool indexes a single document per
// invocation. Bulk indexing is deferred to a follow-up tool so the MCP
// surface stays clearly scoped per call.
type memoryIndexInput struct {
	Path   string `json:"path" jsonschema:"Path to the CogDoc: absolute, workspace-relative, or memory-relative (e.g. semantic/insights/topic.cog.md)"`
	DryRun bool   `json:"dry_run,omitempty" jsonschema:"Preview the generated sections: block without writing it to the file"`
}

// memoryTOCResult is the structured response returned by cog_memory_toc.
// The text/yaml field carries the rendered output exactly matching the
// legacy CLI; path echoes the resolved input for clients that want to
// confirm what was read.
type memoryTOCResult struct {
	Path     string `json:"path"`
	AsYAML   bool   `json:"as_yaml"`
	Rendered string `json:"rendered"`
	NumLines int    `json:"num_lines,omitempty"`
	NumBytes int    `json:"num_bytes,omitempty"`
	NumSecs  int    `json:"num_sections,omitempty"`
}

// memoryIndexResult is the structured response returned by cog_memory_index.
// When DryRun is true SectionsBlock carries the preview payload; when
// DryRun is false Message carries the "Indexed <path> (N sections)"
// confirmation string and SectionsBlock is empty.
type memoryIndexResult struct {
	Path          string `json:"path"`
	DryRun        bool   `json:"dry_run"`
	NumSections   int    `json:"num_sections"`
	SectionsBlock string `json:"sections_block,omitempty"`
	Message       string `json:"message,omitempty"`
}

type emitEventInput struct {
	Type        string         `json:"type" jsonschema:"Event type: attention.boost, session.marker, insight.captured, decision.made, peer.utterance"`
	Payload     map[string]any `json:"payload,omitempty" jsonschema:"Event payload. attention.boost: {uri, weight}. session.marker: {label}. insight.captured: {summary, tags}. decision.made: {decision, rationale}. peer.utterance: {from, to, content, turn, in_reply_to (optional)}."`
	FromSession string         `json:"from_session,omitempty" jsonschema:"Optional sender session_id. If provided, must be a registered session; recorded as event source. Required for peer.utterance events and must match payload.from."`
}

// UnmarshalJSON coerces a string-encoded JSON object in the "payload" field
// into a proper map before the rest of emitEventInput unmarshals normally.
//
// Some local-model tool-call plumbing (observed on LM Studio-served models,
// see issue #492) stringifies nested object arguments even when the tool
// schema declares an object type — the model's intent is a payload object,
// but the transport hands it over JSON-encoded twice. Rather than reject
// those calls outright, attempt a second-pass parse of the string; only
// error if that second pass also fails to produce a JSON object.
//
// This coercion is intentionally narrow: it applies only to the documented
// "payload" field, and only when it arrives as a JSON string. A payload that
// arrives as a JSON array, number, bool, or null (whether directly or nested
// inside the string) is rejected with a clear error — it is never coerced
// into a map "per the issue's spec" boundary.
func (e *emitEventInput) UnmarshalJSON(data []byte) error {
	// Alias to sidestep infinite recursion into this same UnmarshalJSON, and
	// swap Payload's type to json.RawMessage so we can inspect its raw form
	// before deciding how to decode it.
	type alias emitEventInput
	aux := struct {
		Payload json.RawMessage `json:"payload,omitempty"`
		*alias
	}{
		alias: (*alias)(e),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	raw := bytes.TrimSpace(aux.Payload)
	if len(raw) == 0 || string(raw) == "null" {
		e.Payload = nil
		return nil
	}

	payload, err := coerceEmitEventPayload(raw)
	if err != nil {
		return err
	}
	e.Payload = payload
	return nil
}

// coerceEmitEventPayload decodes a raw JSON value into the map[string]any
// shape emit_event's payload field requires. Object payloads decode
// directly. String payloads are given one additional parse pass — if the
// string itself contains a JSON object, that object is used; otherwise the
// failure is reported clearly. Any other JSON kind (array, number, bool) is
// rejected outright; it is never coerced.
func coerceEmitEventPayload(raw []byte) (map[string]any, error) {
	switch raw[0] {
	case '{':
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("payload: %w", err)
		}
		return obj, nil
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("payload: invalid JSON string: %w", err)
		}
		trimmed := strings.TrimSpace(s)
		if trimmed == "" || trimmed[0] != '{' {
			return nil, fmt.Errorf("payload: string value must contain a JSON object, got %q", s)
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			return nil, fmt.Errorf("payload: string value is not valid JSON: %w", err)
		}
		return obj, nil
	default:
		return nil, fmt.Errorf("payload: must be a JSON object (or a JSON-encoded string of one), got %s", describeJSONKind(raw))
	}
}

// describeJSONKind gives a short human-readable name for the JSON value kind
// starting at raw, for use in error messages.
func describeJSONKind(raw []byte) string {
	switch raw[0] {
	case '[':
		return "an array"
	case 't', 'f':
		return "a boolean"
	case 'n':
		return "null"
	default:
		if raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9') {
			return "a number"
		}
		return "an unrecognized value"
	}
}

type readLedgerInput struct {
	SessionID      string `json:"session_id,omitempty" jsonschema:"Filter to a single session; empty reads across all non-genesis sessions"`
	EventType      string `json:"event_type,omitempty" jsonschema:"Exact event type, or a prefix wildcard like 'attention.*'"`
	AfterSeq       int64  `json:"after_seq,omitempty" jsonschema:"Return events with seq greater than this. Requires session_id (seq is not monotonic across sessions)."`
	SinceTimestamp string `json:"since_timestamp,omitempty" jsonschema:"RFC3339 timestamp; return events with timestamp >= this"`
	Limit          int    `json:"limit,omitempty" jsonschema:"Maximum events to return. Default 100, capped at 1000."`
	VerifyChain    bool   `json:"verify_chain,omitempty" jsonschema:"Recompute hashes and validate prior_hash links on returned events. Off by default (chain walk is O(N))."`
}

type readEventsInput struct {
	SessionID string `json:"session_id,omitempty" jsonschema:"Filter to a single session; empty reads across all"`
	EventType string `json:"event_type,omitempty" jsonschema:"Exact event type, or a prefix wildcard like 'attention.*'"`
	Source    string `json:"source,omitempty" jsonschema:"Filter by source (e.g. 'kernel-v3', 'mcp-client')"`
	Since     string `json:"since,omitempty" jsonschema:"RFC3339 timestamp or duration shorthand ('5m', '1h')"`
	Until     string `json:"until,omitempty" jsonschema:"RFC3339 timestamp or duration shorthand (upper bound)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum events to return. Default 100, capped at 1000."`
	Order     string `json:"order,omitempty" jsonschema:"'desc' (default, newest first) or 'asc'"`
}

type tailEventsInput struct {
	SessionID   string `json:"session_id,omitempty" jsonschema:"Filter to a single session; empty reads across all"`
	EventType   string `json:"event_type,omitempty" jsonschema:"Exact event type, or a prefix wildcard like 'attention.*'"`
	Source      string `json:"source,omitempty" jsonschema:"Filter by source (e.g. 'kernel-v3', 'mcp-client')"`
	Since       string `json:"since,omitempty" jsonschema:"RFC3339 timestamp or duration shorthand; replay before going live"`
	MaxEvents   int    `json:"max_events,omitempty" jsonschema:"Stop after receiving this many events. Default 100, capped at 1000."`
	MaxDuration string `json:"max_duration,omitempty" jsonschema:"Stop after this duration ('30s', '5m'). Default 60s, capped at 10m."`
}

// getIndexInput — no longer an MCP tool; used by the internal tool loop (tool_loop.go).
type getIndexInput struct {
	Sector string `json:"sector,omitempty" jsonschema:"Filter by memory sector"`
}

type ingestInput struct {
	Source   string            `json:"source" jsonschema:"Data source: discord, chatgpt, claude, gemini, url, file"`
	Format   string            `json:"format" jsonschema:"Input format: url, conversation, message, document"`
	Data     string            `json:"data" jsonschema:"Raw material to ingest (URL, text, JSON)"`
	Metadata map[string]string `json:"metadata,omitempty" jsonschema:"Optional context (discord_message_id, channel, etc.)"`
}

type tailKernelLogInput struct {
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum entries to return (default 100, max 1000)"`
	Level     string `json:"level,omitempty" jsonschema:"Filter by exact level (case-insensitive): debug|info|warn|error"`
	Substring string `json:"substring,omitempty" jsonschema:"Case-insensitive substring filter applied to the raw JSON line. Max 1024 chars."`
	Since     string `json:"since,omitempty" jsonschema:"Lower time bound. RFC3339 OR duration like '5m', '2h', '24h'."`
	Until     string `json:"until,omitempty" jsonschema:"Upper time bound. RFC3339 OR duration."`
}

type listAgentsInput struct {
	IncludeStopped bool `json:"include_stopped,omitempty" jsonschema:"Include agents that have stopped (default false). Reserved for future multi-agent pool managers."`
}

type getAgentStateInput struct {
	AgentID      string `json:"agent_id,omitempty" jsonschema:"Which agent to inspect. Default \"primary\" (the ServeAgent singleton)."`
	IncludeTrace bool   `json:"include_trace,omitempty" jsonschema:"Attach up to trace_limit most-recent full cycle traces (observation + result)."`
	TraceLimit   int    `json:"trace_limit,omitempty" jsonschema:"If include_trace, how many recent traces to include. Range [1, 20]. Default 1."`
}

type triggerAgentLoopInput struct {
	AgentID string `json:"agent_id,omitempty" jsonschema:"Which agent to trigger. Default \"primary\"."`
	Reason  string `json:"reason,omitempty" jsonschema:"Free-text tag stored on a synthetic agent.wake event for audit (optional)."`
	Wait    bool   `json:"wait,omitempty" jsonschema:"If true, block until the cycle completes (up to 90s) and return the outcome. Default false (fire-and-forget)."`
}

// dispatchToHarnessInput is the wire shape for cog_dispatch_to_harness — the
// Phase 2 task-parameterized transport from the foveal Claude session into
// the resident peripheral swarm. See engine/agent_dispatch.go for the
// underlying DispatchRequest contract.
type dispatchToHarnessInput struct {
	AgentID      string                 `json:"agent_id,omitempty" jsonschema:"Which harness instance to dispatch into. Default \"primary\"."`
	Task         string                 `json:"task" jsonschema:"Required. The user-role prompt the harness's Execute loop will receive."`
	Scope        string                 `json:"scope,omitempty" jsonschema:"Named tool scope for this dispatch (RFC-016). \"consolidation\" (default, 11 substrate tools: memory/field/coherence/identity — respond excluded from base scope), \"consolidation_with_respond\" (consolidation + respond tool), \"consolidation_no_respond\" (consolidation without respond), \"audit\" (consolidation + cog_read_file + cog_grep_files), \"search\" (cog_resolve_uri + cog_search_memory + cog_query_field), \"observe\" (consolidation read tools minus cog_emit_event), \"emit\" (cog_emit_event only). Unknown scope names error immediately. The tools parameter, if set, narrows further within the chosen scope."`
	Tools        []string               `json:"tools,omitempty" jsonschema:"Optional tool allowlist (subset of the chosen scope's tools). nil/empty uses the full scope set. Unknown names error in the per-slot result rather than silently dropping."`
	Model        string                 `json:"model,omitempty" jsonschema:"Inference backend hint or explicit model id. Legacy values \"e4b\" and \"26b\" route through the local-LLM probe unchanged. Any other value is treated as an explicit model-id request: it wins over the resolved provider's configured default model and is sent to the provider verbatim. If the provider cannot serve it, the dispatch fails loudly (provider error surfaced in the slot's error field) rather than silently substituting. Per issue #430."`
	Provider     string                 `json:"provider,omitempty" jsonschema:"Named provider override (e.g. \"lmstudio-local\", \"anthropic\"). Must match a provider declared in providers.yaml or providers.local.yaml; unknown names error before any slot runs. Selects which backend serves the request (per RFC-0007) — model selection within that backend is independent: an explicit model (see model field) still overrides the provider's configured default."`
	Timeout      int                    `json:"timeout_seconds,omitempty" jsonschema:"Per-slot wall-clock budget in seconds. Default 240. Max = the dispatch timeout cap from kernel config (dispatch_timeout_cap_seconds in kernel.yaml, default 600); values above the cap are rejected with invalid_input before any slot runs. When a slot exceeds its budget at runtime, that slot returns success=false error=\"timeout\"; sibling slots continue."`
	N            int                    `json:"n,omitempty" jsonschema:"Parallel fan-out, [1,4]. Default 1. Each slot gets its own context, its own deadline, and its own result entry; failures don't abort siblings."`
	SystemPrompt string                 `json:"system_prompt,omitempty" jsonschema:"Optional system-prompt override for this dispatch only. Empty keeps the harness default. Used by output-alignment roles (validator/rewriter/modality-matcher)."`
	Thinking     *bool                  `json:"thinking,omitempty" jsonschema:"Optional override of the model's think flag. nil keeps the harness default."`
	Iss          string                 `json:"iss,omitempty" jsonschema:"OIDC-shaped identity claim: issuer (e.g. \"anthropic.claude-code\"). Forwarded as trace metadata; full CRD binding waits for Wave 6b."`
	Sub          string                 `json:"sub,omitempty" jsonschema:"OIDC-shaped identity claim: subject (e.g. session id, user handle)."`
	Aud          string                 `json:"aud,omitempty" jsonschema:"OIDC-shaped identity claim: audience (e.g. \"cogos.kernel\")."`
	Claims       map[string]interface{} `json:"claims,omitempty" jsonschema:"Free-form OIDC claim bag forwarded to the dispatch's trace metadata."`
	TargetNode   string                 `json:"target_node,omitempty" jsonschema:"Phase 2 S4: when set, forward this dispatch to the named peer node over the authenticated BEP cluster channel. Requires cluster.enabled=true and the peer to be connected. Empty (default) runs on the local harness."`
	Async        bool                   `json:"async,omitempty" jsonschema:"When true, return a job handle immediately ({job_id, status:\"pending\"}) instead of blocking until the dispatch completes. Poll the job via GET /v1/dispatch-jobs/{id} or the cog_poll_dispatch tool. Default false (synchronous, unchanged behavior)."`
	Ambient      bool                   `json:"ambient,omitempty" jsonschema:"When true, prepend an ambient-state-of-self block (cogdoc 16) to the system prompt of every fan-out slot in this batch — live kernel health (provider status, anomaly counts), workspace, identity, process state, and fovea top-5, computed once per batch. Default false (unchanged behavior); intended for looped kernel-interior callers (ralph runners, RFC-036 seats) that dispatch repeatedly and benefit from knowing what the kernel's body is doing between runs."`
}

// dispatchJobReceipt is the immediate response for an async dispatch — the
// job handle a caller polls until it reaches a terminal state.
type dispatchJobReceipt struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	CycleID string `json:"cycle_id,omitempty"`
}

// dispatchJobStatusResponse is the poll response shape for both the MCP
// cog_poll_dispatch tool and GET /v1/dispatch-jobs/{id}. Result/Error are
// populated only once Status is terminal (done/failed).
type dispatchJobStatusResponse struct {
	JobID     string               `json:"job_id"`
	Status    string               `json:"status"`
	CycleID   string               `json:"cycle_id,omitempty"`
	Result    *DispatchBatchResult `json:"result,omitempty"`
	Error     string               `json:"error,omitempty"`
	CreatedAt string               `json:"created_at,omitempty"`
	UpdatedAt string               `json:"updated_at,omitempty"`
}

// pollDispatchInput is the wire shape for cog_poll_dispatch.
type pollDispatchInput struct {
	JobID string `json:"job_id" jsonschema:"Required. The job_id returned by an async (Async=true) cog_dispatch_to_harness call."`
}

// readToolCallsInput mirrors ToolCallQuery for the MCP surface. Time bounds
// accept RFC3339 strings; relative shorthand ("5m", "1h", "24h") is supported.
type readToolCallsInput struct {
	SessionID     string `json:"session_id,omitempty" jsonschema:"Filter to a session; empty = all sessions"`
	ToolName      string `json:"tool_name,omitempty" jsonschema:"Exact match or wildcard (e.g. cog_read_*)"`
	Status        string `json:"status,omitempty" jsonschema:"pending | success | error | rejected | timeout"`
	Source        string `json:"source,omitempty" jsonschema:"mcp | openai-chat | anthropic-messages | kernel-loop"`
	Ownership     string `json:"ownership,omitempty" jsonschema:"kernel | client"`
	CallID        string `json:"call_id,omitempty" jsonschema:"Exact single-call lookup"`
	Since         string `json:"since,omitempty" jsonschema:"Lower bound — RFC3339 timestamp or relative duration (e.g. 5m, 1h)"`
	Until         string `json:"until,omitempty" jsonschema:"Upper bound — RFC3339 timestamp"`
	Limit         int    `json:"limit,omitempty" jsonschema:"Default 100, max 500"`
	Order         string `json:"order,omitempty" jsonschema:"desc (default) | asc"`
	IncludeArgs   bool   `json:"include_args,omitempty" jsonschema:"Include arguments payload (default false — PII control)"`
	IncludeOutput bool   `json:"include_output,omitempty" jsonschema:"Include output summary (default false — PII control)"`
}

// tailToolCallsInput is a snapshot-mode version of the live tail. Inherits
// all of readToolCallsInput's filters; defaults include_args/include_output
// to true (callers tailing are actively observing) and caps the returned set
// with max_events + max_duration.
type tailToolCallsInput struct {
	SessionID   string `json:"session_id,omitempty" jsonschema:"Filter to a session; empty = all sessions"`
	ToolName    string `json:"tool_name,omitempty" jsonschema:"Exact match or wildcard"`
	Status      string `json:"status,omitempty" jsonschema:"pending | success | error | rejected | timeout"`
	Source      string `json:"source,omitempty" jsonschema:"Source taxonomy filter"`
	Ownership   string `json:"ownership,omitempty" jsonschema:"kernel | client"`
	CallID      string `json:"call_id,omitempty" jsonschema:"Exact single-call lookup"`
	Since       string `json:"since,omitempty" jsonschema:"Lower bound — RFC3339 or relative"`
	MaxEvents   int    `json:"max_events,omitempty" jsonschema:"Stop after N matching events (default 50, max 500)"`
	MaxDuration string `json:"max_duration,omitempty" jsonschema:"Hard cap on wall-clock (default 60s, max 10m)"`
}

// readConversationInput drives cog_read_conversation — see Agent R §5.2.
type readConversationInput struct {
	SessionID    string `json:"session_id,omitempty" jsonschema:"Session to read. Empty = current process session."`
	AfterTurn    int    `json:"after_turn,omitempty" jsonschema:"Pagination: return turns with turn_index > this."`
	BeforeTurn   int    `json:"before_turn,omitempty" jsonschema:"Reverse pagination: turn_index < this."`
	Since        string `json:"since,omitempty" jsonschema:"RFC3339 lower bound on turn timestamp."`
	Limit        int    `json:"limit,omitempty" jsonschema:"Max turns (default 20, max 200)."`
	IncludeFull  *bool  `json:"include_full,omitempty" jsonschema:"Hydrate prompt/response from the sidecar (default true)."`
	IncludeTools *bool  `json:"include_tools,omitempty" jsonschema:"Include kernel tool-call transcript (default true)."`
	Order        string `json:"order,omitempty" jsonschema:"asc (default, natural reading order) or desc."`
}

type searchTracesInput struct {
	Source    string `json:"source,omitempty" jsonschema:"Trace source filter. One of: attention, proprioceptive, internal_requests, all (default)."`
	Level     string `json:"level,omitempty" jsonschema:"Level-like filter (exact match, case-insensitive). For proprioceptive source this matches the event field."`
	SessionID string `json:"session_id,omitempty" jsonschema:"Filter to rows whose session_id matches. Only meaningful for sources that carry a session_id (internal_requests)."`
	Substring string `json:"substring,omitempty" jsonschema:"Case-insensitive substring match against the raw JSONL line. Capped at 1024 characters."`
	Since     string `json:"since,omitempty" jsonschema:"Lower time bound. RFC3339 timestamp or Go duration (e.g. 5m, 1h, 24h for 'since N ago')."`
	Until     string `json:"until,omitempty" jsonschema:"Upper time bound. RFC3339 timestamp or Go duration."`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum results (default 100, max 1000)."`
	Order     string `json:"order,omitempty" jsonschema:"'desc' (default, newest first) or 'asc'."`
}

// readFileInput is the input type for cog_read_file (audit scope).
type readFileInput struct {
	Path   string `json:"path" jsonschema:"Absolute path within workspace root"`
	Offset int    `json:"offset,omitempty" jsonschema:"Line offset (0-based). Default 0."`
	Limit  int    `json:"limit,omitempty" jsonschema:"Maximum lines to return. Default 500."`
	// Res controls the response fidelity (ADR pointer-first-mcp-responses).
	// "abstract" (default) returns a pointer envelope: workspace-relative path,
	// file size, and a short excerpt — sufficient to decide whether to fetch
	// the full content. "full" returns the line-numbered file content (existing
	// behaviour). Default is "abstract" to prevent bulk content blowout in
	// audit-scope harness dispatches; callers that need content pass res="full".
	//
	// NOTE: a cog:file/<rel-path> URI projection (dereferenceable without
	// cog_read_file) is deferred pending resolver work (see uri.go projections).
	// Until then res=abstract emits a workspace-relative path as the pointer.
	Res string `json:"res,omitempty" jsonschema:"Response fidelity: \"abstract\" (default, pointer envelope) or \"full\" (line-numbered content). ADR pointer-first-mcp-responses."`
}

// grepFilesInput is the input type for cog_grep_files (audit scope).
type grepFilesInput struct {
	Pattern    string `json:"pattern" jsonschema:"Regular expression pattern to search for"`
	Path       string `json:"path,omitempty" jsonschema:"Directory or file to search (defaults to workspace root)"`
	MaxResults int    `json:"max_results,omitempty" jsonschema:"Maximum matches to return. Default 50."`
	// Res controls the response fidelity (ADR pointer-first-mcp-responses).
	// "abstract" (default) returns a summary envelope: match count, unique
	// matched file paths, and truncation flag — no line text. "full" returns
	// the per-match path/line/text entries (existing behaviour). Default is
	// "abstract" to prevent bulk match-text blowout in audit-scope dispatches.
	Res string `json:"res,omitempty" jsonschema:"Response fidelity: \"abstract\" (default, match summary + file list) or \"full\" (per-match path/line/text). ADR pointer-first-mcp-responses."`
}

// ── Tool Implementations ─────────────────────────────────────────────────────

// toolResolveURI — no longer registered as an MCP tool; used by the internal tool loop (tool_loop.go).
//
// Delegates entirely to ResolveURI, which now handles the full fallback chain:
//  1. Projection lookup (local fast path)
//  2. URIRegistry delegation (cross-workspace aliases + workspace registry)
//  3. ErrUnknownAuthority if neither can resolve
//
// The prior double-dispatch (registry first, then ResolveURI) is removed; the
// registry delegation now lives inside ResolveURI itself so every canonical
// path (cogdoc patching, content service, etc.) benefits automatically.
func (m *MCPServer) toolResolveURI(ctx context.Context, req *mcp.CallToolRequest, input resolveURIInput) (*mcp.CallToolResult, any, error) {
	res, err := ResolveURI(m.cfg.WorkspaceRoot, input.URI)
	if err != nil {
		return marshalResult(map[string]any{
			"uri":      input.URI,
			"resolved": false,
			"error":    err.Error(),
		})
	}
	_, statErr := os.Stat(res.Path)
	return marshalResult(map[string]any{
		"uri":      input.URI,
		"resolved": true,
		"path":     res.Path,
		"fragment": res.Fragment,
		"exists":   statErr == nil,
	})
}

func (m *MCPServer) toolQueryField(ctx context.Context, req *mcp.CallToolRequest, input queryFieldInput) (*mcp.CallToolResult, any, error) {
	if m.process == nil || m.process.field == nil {
		return textResult("attentional field not initialized")
	}

	limit := input.Limit
	if limit <= 0 {
		limit = 20
	}

	scores := m.process.field.AllScores()

	type entry struct {
		URI      string  `json:"uri"`
		Salience float64 `json:"salience"`
	}
	var entries []entry
	for absPath, score := range scores {
		if input.Sector != "" && !strings.Contains(absPath, input.Sector) {
			continue
		}
		// Project field key (abs path) to canonical URI.
		uri := FieldKeyToURI(m.cfg.WorkspaceRoot, absPath)
		entries = append(entries, entry{URI: uri, Salience: score})
	}
	// Sort by salience descending
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Salience > entries[i].Salience {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return marshalResult(map[string]any{
		"count":   len(entries),
		"entries": entries,
	})
}

func (m *MCPServer) toolAssembleContext(ctx context.Context, req *mcp.CallToolRequest, input assembleContextInput) (*mcp.CallToolResult, any, error) {
	if m.process == nil {
		return textResult("process not initialized")
	}

	budget := input.Budget
	if budget <= 0 {
		budget = 50000
	}

	// Use the existing context assembly pipeline
	assembled, err := m.process.AssembleContext(input.Focus, nil, budget, WithManifestMode(true))
	if err != nil {
		return textResult(fmt.Sprintf("context assembly failed: %v", err))
	}

	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}
	return capMarshalResult(assembled, maxBytes)
}

func (m *MCPServer) toolCheckCoherence(ctx context.Context, req *mcp.CallToolRequest, input checkCoherenceInput) (*mcp.CallToolResult, any, error) {
	report, err := CheckCoherenceMCP(m.cfg, m.nucleus)
	if err != nil {
		return fallbackResult(fmt.Sprintf("coherence check failed: %v", err),
			"./scripts/cog coherence check")
	}
	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}
	return capMarshalResult(report, maxBytes)
}

func (m *MCPServer) toolGetState(ctx context.Context, req *mcp.CallToolRequest, input getStateInput) (*mcp.CallToolResult, any, error) {
	if m.process == nil {
		return fallbackResult("process not initialized", "curl http://localhost:6931/health")
	}
	queue := ReadIngestionQueueState(m.cfg.WorkspaceRoot)
	trust := m.process.TrustSnapshot()
	lastHeartbeat := ""
	if !trust.LastHeartbeatAt.IsZero() {
		lastHeartbeat = trust.LastHeartbeatAt.Format(time.RFC3339)
	}

	// Identity (nucleus)
	identity := ""
	if m.nucleus != nil {
		identity = m.nucleus.Name
	}

	result := map[string]any{
		"state":             m.process.State().String(),
		"identity":          identity,
		"session_id":        m.process.SessionID(),
		"node_id":           m.process.NodeID,
		"uptime_seconds":    int(time.Since(m.process.StartedAt()).Seconds()),
		"field_size":        m.process.Field().Len(),
		"trust_score":       trust.LocalScore,
		"fingerprint":       m.process.Fingerprint(),
		"last_heartbeat":    lastHeartbeat,
		"coherence_state":   trust.CoherenceFingerprint,
		"quarantined_count": queue.Quarantined,
		"deferred_count":    queue.Deferred,
	}

	// Node health — sibling services probed on heartbeat.
	if nh := m.process.NodeHealth(); nh != nil {
		if summary := nh.Summary(); len(summary) > 0 {
			result["node"] = summary
		}
	}

	if input.Verbose {
		result["workspace"] = m.cfg.WorkspaceRoot
		result["started_at"] = m.process.StartedAt().Format(time.RFC3339)
		result["last_heartbeat_hash"] = trust.LastHeartbeatHash
		result["last_quarantine"] = queue.LastQuarantineRFC3339
	}
	return marshalResult(result)
}

// toolGetTrust — no longer registered as an MCP tool; used by the internal tool loop (tool_loop.go).
func (m *MCPServer) toolGetTrust(ctx context.Context, req *mcp.CallToolRequest, input getTrustInput) (*mcp.CallToolResult, any, error) {
	if m.process == nil {
		return textResult("process not initialized")
	}
	trust := m.process.TrustSnapshot()
	lastHeartbeat := ""
	if !trust.LastHeartbeatAt.IsZero() {
		lastHeartbeat = trust.LastHeartbeatAt.Format(time.RFC3339)
	}
	return marshalResult(map[string]any{
		"node_id":         m.process.NodeID,
		"trust_score":     trust.LocalScore,
		"fingerprint":     m.process.Fingerprint(),
		"last_heartbeat":  lastHeartbeat,
		"coherence_state": trust.CoherenceFingerprint,
	})
}

func (m *MCPServer) toolSearchMemory(ctx context.Context, req *mcp.CallToolRequest, input searchMemoryInput) (*mcp.CallToolResult, any, error) {
	limit := input.Limit
	if limit <= 0 {
		limit = 10
	}

	results, err := SearchMemory(m.cfg.WorkspaceRoot, input.Query, limit, input.Sector)
	if err != nil {
		return fallbackResult(fmt.Sprintf("search failed: %v", err),
			fmt.Sprintf("./scripts/cog memory search %q", input.Query))
	}
	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}
	return capMarshalResult(results, maxBytes)
}

// toolGetNucleus — no longer registered as an MCP tool; used by the internal tool loop (tool_loop.go).
// Logic retained for C1 migration to MCP Resource.
func (m *MCPServer) toolGetNucleus(ctx context.Context, req *mcp.CallToolRequest, input getNucleusInput) (*mcp.CallToolResult, any, error) {
	if m.nucleus == nil {
		return textResult("nucleus not loaded")
	}
	return marshalResult(map[string]any{
		"name":      m.nucleus.Name,
		"role":      m.nucleus.Role,
		"summary":   m.nucleus.Summary(),
		"workspace": m.cfg.WorkspaceRoot,
		"port":      m.cfg.Port,
		"build":     BuildTime,
	})
}

func (m *MCPServer) toolReadCogdoc(ctx context.Context, req *mcp.CallToolRequest, input readCogdocInput) (*mcp.CallToolResult, any, error) {
	uri := input.URI
	if input.Section != "" && !strings.Contains(uri, "#") {
		uri += "#" + input.Section
	}

	res, err := ResolveURI(m.cfg.WorkspaceRoot, uri)
	if err != nil {
		return textResult(fmt.Sprintf("resolve failed: %v", err))
	}

	data, err := os.ReadFile(res.Path)
	if err != nil {
		return textResult(fmt.Sprintf("read failed: %v", err))
	}

	content := string(data)
	fm, _ := parseCogdocFrontmatter(content)
	issues := missingSchemaIssues(content)
	patchTemplate := patchTemplateForIssues(issues)
	result := readCogdocResult{
		URI:              uri,
		Path:             res.Path,
		Frontmatter:      fm,
		Content:          content,
		SchemaIssues:     issues,
		PatchFrontmatter: patchTemplate,
	}
	if hasSchemaIssue(issues, "missing_description") {
		result.SchemaHint = fmt.Sprintf("This CogDoc is missing a description field. If you can summarize it in one sentence, include it in your next response as: COGDOC_PATCH: %s | description: your summary here", uri)
	}

	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}

	// If fragment specified, extract section
	if res.Fragment != "" {
		section := extractSection(content, res.Fragment)
		if section != "" {
			result.Fragment = res.Fragment
			result.Content = section
			return capMarshalResult(result, maxBytes)
		}
	}

	return capMarshalResult(result, maxBytes)
}

func (m *MCPServer) toolPatchFrontmatter(ctx context.Context, req *mcp.CallToolRequest, input patchFrontmatterInput) (*mcp.CallToolResult, any, error) {
	if input.URI == "" {
		return textResult("uri is required")
	}
	if input.Patches.empty() {
		return textResult("at least one frontmatter patch is required")
	}

	result, err := m.cogdocSvc.PatchAndSync(input.URI, input.Patches)
	if err != nil {
		return textResult(fmt.Sprintf("patch failed: %v", err))
	}

	// Read back the patched frontmatter for the response.
	var fm cogdocFrontmatter
	if data, readErr := os.ReadFile(result.Path); readErr == nil {
		fm, _ = parseCogdocFrontmatter(string(data))
	}

	return marshalResult(map[string]any{
		"updated":     true,
		"uri":         result.URI,
		"path":        result.Path,
		"frontmatter": fm,
	})
}

func (m *MCPServer) toolWriteCogdoc(ctx context.Context, req *mcp.CallToolRequest, input writeCogdocInput) (*mcp.CallToolResult, any, error) {
	if input.Path == "" || input.Content == "" {
		return textResult("path and content are required")
	}

	opts := CogDocWriteOpts{
		Title:   input.Title,
		Content: input.Content,
		Tags:    input.Tags,
		Status:  input.Status,
		DocType: input.DocType,
	}

	result, err := m.cogdocSvc.WriteAndSync(input.Path, opts)
	if err != nil {
		return textResult(fmt.Sprintf("write failed: %v", err))
	}

	return marshalResult(map[string]any{
		"written": true,
		"path":    result.Path,
		"uri":     result.URI,
	})
}

// toolMemoryTOC exposes the MemoryTOC port as cog_memory_toc. The handler
// forwards to MemoryTOC in memory_sections.go and wraps the rendered
// output in a structured result so clients that want metadata (section
// count, total bytes) can read it without re-parsing the text table.
func (m *MCPServer) toolMemoryTOC(ctx context.Context, req *mcp.CallToolRequest, input memoryTOCInput) (*mcp.CallToolResult, any, error) {
	if input.Path == "" {
		return textResult("path is required")
	}

	rendered, err := MemoryTOC(m.cfg.WorkspaceRoot, input.Path, input.AsYAML)
	if err != nil {
		return fallbackResult(fmt.Sprintf("toc failed: %v", err),
			fmt.Sprintf("./scripts/cog memory toc %q", input.Path))
	}

	// Best-effort metadata. We re-parse to surface counts in the JSON
	// response; this is a millisecond-scale double-read that matches the
	// legacy CLI behaviour (legacy renders its own summary line too).
	result := memoryTOCResult{
		Path:     input.Path,
		AsYAML:   input.AsYAML,
		Rendered: rendered,
	}
	if raw, readErr := readCogDocContentV3(m.cfg.WorkspaceRoot, input.Path); readErr == nil {
		body := raw
		if _, b, ok := splitFrontmatter(raw); ok {
			body = b
		}
		result.NumLines = len(strings.Split(raw, "\n"))
		result.NumBytes = len(raw)
		result.NumSecs = len(ParseMemorySections(body))
	}
	return marshalResult(result)
}

// toolMemoryIndex exposes the MemoryIndex port as cog_memory_index. On
// dry-run the generated sections: block is returned but the file is
// untouched. On live run the frontmatter is rewritten atomically and a
// confirmation message is returned; the caller receives the final section
// count so observability of the write is first-class.
func (m *MCPServer) toolMemoryIndex(ctx context.Context, req *mcp.CallToolRequest, input memoryIndexInput) (*mcp.CallToolResult, any, error) {
	if input.Path == "" {
		return textResult("path is required")
	}

	out, err := MemoryIndex(m.cfg.WorkspaceRoot, input.Path, input.DryRun)
	if err != nil {
		suffix := ""
		if input.DryRun {
			suffix = " --dry-run"
		}
		return fallbackResult(fmt.Sprintf("index failed: %v", err),
			fmt.Sprintf("./scripts/cog memory index %q%s", input.Path, suffix))
	}

	result := memoryIndexResult{
		Path:   input.Path,
		DryRun: input.DryRun,
	}

	// Count sections for the JSON response. On both dry-run and live
	// paths the body we just parsed produced the sections we care about;
	// re-reading is cheaper than threading a return value through the
	// pure-Go core.
	if raw, readErr := readCogDocContentV3(m.cfg.WorkspaceRoot, input.Path); readErr == nil {
		body := raw
		if _, b, ok := splitFrontmatter(raw); ok {
			body = b
		}
		// Only count level ≥ 2 — matches what made it into the sections:
		// block. The full ParseMemorySections return includes the doc
		// title (level 1) which we filter out of the frontmatter.
		count := 0
		for _, s := range ParseMemorySections(body) {
			if s.Level >= 2 {
				count++
			}
		}
		result.NumSections = count
	}

	if input.DryRun {
		result.SectionsBlock = out
	} else {
		result.Message = out
		// Refresh the CogDoc index so the freshly-indexed document
		// reflects in the kernel's in-memory view on the next query.
		// Parity with CogDocService.refreshIndex but local: MemoryIndex
		// is not a go through CogDocService because it operates on raw
		// frontmatter bytes rather than the structured opts shape.
		m.refreshIndexAfterWrite()
	}

	return marshalResult(result)
}

// refreshIndexAfterWrite rebuilds the CogDoc index after a non-WriteAndSync
// write path mutates disk. Kept small to avoid pulling the whole
// CogDocService into the MemoryIndex path (which does not need field
// boosts or ledger events — it's a metadata-only injection).
func (m *MCPServer) refreshIndexAfterWrite() {
	if m.process == nil {
		return
	}
	idx, err := BuildIndex(m.cfg.WorkspaceRoot)
	if err != nil {
		slog.Warn("cog_memory_index: index refresh failed", "err", err)
		return
	}
	m.process.indexMu.Lock()
	m.process.index = idx
	m.process.indexMu.Unlock()
}

// CogDocWriteOpts holds options for writing a CogDoc via the internal API.
type CogDocWriteOpts struct {
	Title    string
	Content  string
	Tags     []string
	Status   string            // default "active"
	DocType  string            // e.g. "link", "conversation", "insight"
	Source   string            // e.g. "discord", "chatgpt"
	URL      string            // optional URL field
	SourceID string            // dedup key
	Extra    map[string]string // additional frontmatter fields
}

// detectSector extracts the memory sector from a memory-relative path.
// e.g. "semantic/insights/foo.md" -> "semantic"
func detectSector(path string) string {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 {
		return "semantic"
	}
	switch parts[0] {
	case "semantic", "episodic", "procedural", "reflective":
		return parts[0]
	default:
		return "semantic"
	}
}

// slugFromPath generates a slug-based id from a memory-relative path.
// e.g. "semantic/insights/my-topic.cog.md" -> "my-topic"
func slugFromPath(path string) string {
	base := filepath.Base(path)
	// Strip known extensions
	base = strings.TrimSuffix(base, ".cog.md")
	base = strings.TrimSuffix(base, ".md")
	// Slugify: lowercase, replace non-alnum with hyphens, collapse
	slug := strings.ToLower(base)
	re := regexp.MustCompile(`[^a-z0-9]+`)
	slug = re.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	return slug
}

// WriteCogDoc writes a CogDoc to the memory filesystem with proper frontmatter.
// This is the internal API used by the ingestion pipeline.
func WriteCogDoc(workspaceRoot string, path string, opts CogDocWriteOpts) (string, error) {
	fullPath, perr := containedJoin(filepath.Join(workspaceRoot, ".cog", "mem"), path)
	if perr != nil {
		return "", fmt.Errorf("write cogdoc: %w", perr)
	}

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("mkdir failed: %w", err)
	}

	sector := detectSector(path)
	docID := slugFromPath(path)
	now := time.Now().UTC().Format(time.RFC3339)

	status := opts.Status
	if status == "" {
		status = "active"
	}

	// Build YAML frontmatter
	var sb strings.Builder
	sb.WriteString("---\n")
	if docID != "" {
		sb.WriteString(fmt.Sprintf("id: %s\n", docID))
	}
	sb.WriteString(fmt.Sprintf("title: %q\n", opts.Title))
	sb.WriteString(fmt.Sprintf("created: %q\n", now))
	sb.WriteString(fmt.Sprintf("memory_sector: %s\n", sector))
	sb.WriteString(fmt.Sprintf("status: %s\n", status))

	if opts.DocType != "" {
		sb.WriteString(fmt.Sprintf("type: %s\n", opts.DocType))
	}
	if opts.Source != "" {
		sb.WriteString(fmt.Sprintf("source: %s\n", opts.Source))
	}
	if opts.URL != "" {
		sb.WriteString(fmt.Sprintf("url: %q\n", opts.URL))
	}
	if opts.SourceID != "" {
		sb.WriteString(fmt.Sprintf("source_id: %q\n", opts.SourceID))
	}

	if len(opts.Tags) > 0 {
		sb.WriteString("tags:\n")
		for _, tag := range opts.Tags {
			sb.WriteString(fmt.Sprintf("  - %s\n", tag))
		}
	}

	// Write any extra frontmatter fields
	for k, v := range opts.Extra {
		sb.WriteString(fmt.Sprintf("%s: %q\n", k, v))
	}

	sb.WriteString("---\n\n")
	sb.WriteString(opts.Content)

	if err := os.WriteFile(fullPath, []byte(sb.String()), 0644); err != nil {
		return "", fmt.Errorf("write failed: %w", err)
	}

	uri := "cog:mem/" + path
	return uri, nil
}

// allowedEventTypes is the closed set of event types accepted by toolEmitEvent.
// Extend this list when new peer or substrate event types are ratified.
var allowedEventTypes = map[string]bool{
	"attention.boost":  true,
	"session.marker":   true,
	"insight.captured": true,
	"decision.made":    true,
	"peer.utterance":   true,
}

func (m *MCPServer) toolEmitEvent(ctx context.Context, req *mcp.CallToolRequest, input emitEventInput) (*mcp.CallToolResult, any, error) {
	if input.Type == "" {
		return textResult("event type is required. Valid types: attention.boost, session.marker, insight.captured, decision.made, peer.utterance")
	}
	if !allowedEventTypes[input.Type] {
		return textResult(fmt.Sprintf("unknown event type %q. Valid types: attention.boost, session.marker, insight.captured, decision.made, peer.utterance", input.Type))
	}
	if m.process == nil {
		return fallbackResult("process not initialized", "echo '{\"type\":\"...\"}' >> .cog/ledger/<session_id>/events.jsonl")
	}

	// Validate from_session if provided: must be a registered session.
	if input.FromSession != "" {
		if err := ValidateSessionID(input.FromSession); err != nil {
			return textResult("from_session: " + err.Error())
		}
		if m.sessionRegistry != nil {
			if _, ok := m.sessionRegistry.Get(input.FromSession); !ok {
				return textResult(fmt.Sprintf("from_session %q is not a registered session", input.FromSession))
			}
		}
	}

	// Validate peer.utterance payload fields.
	if input.Type == "peer.utterance" {
		if input.FromSession == "" {
			return textResult("peer.utterance requires from_session (must match payload.from)")
		}
		fromPayload, _ := input.Payload["from"].(string)
		if fromPayload == "" {
			return textResult("peer.utterance payload requires 'from' field")
		}
		if fromPayload != input.FromSession {
			return textResult(fmt.Sprintf("peer.utterance: from_session %q must match payload.from %q", input.FromSession, fromPayload))
		}
		toPayload, _ := input.Payload["to"].(string)
		if toPayload == "" {
			return textResult("peer.utterance payload requires 'to' field")
		}
		if m.sessionRegistry != nil {
			if _, ok := m.sessionRegistry.Get(toPayload); !ok {
				return textResult(fmt.Sprintf("peer.utterance payload.to %q is not a registered session", toPayload))
			}
		}
		contentPayload, _ := input.Payload["content"].(string)
		if contentPayload == "" {
			return textResult("peer.utterance payload requires non-empty 'content' field")
		}
		turnPayload, hasTurn := input.Payload["turn"]
		if !hasTurn {
			return textResult("peer.utterance payload requires 'turn' field (positive integer, 1-indexed)")
		}
		turnNum, ok := turnPayload.(float64) // JSON numbers unmarshal as float64.
		if !ok || turnNum < 1 || turnNum != float64(int(turnNum)) {
			return textResult("peer.utterance payload.turn must be a positive integer")
		}
	}

	// Build the event data. Legacy shape exposed payload under a separate
	// "payload" key; the on-disk envelope shape uses "data". We flatten so
	// subscribers see a uniform envelope and the historical payload is still
	// reachable under data.payload.
	data := map[string]any{}
	if input.Payload != nil {
		data["payload"] = input.Payload
	}

	// Record from_session as the attributed source in event data so
	// downstream consumers see consistent sender attribution.
	if input.FromSession != "" {
		data["from_session"] = input.FromSession
	}

	// Handle attention.boost: resolve URI to field key, then boost. Side
	// effects are recorded in the event data so downstream consumers know
	// the field mutated.
	if input.Type == "attention.boost" {
		if uri, ok := input.Payload["uri"].(string); ok && uri != "" {
			fieldKey := ResolveToFieldKey(m.cfg.WorkspaceRoot, uri)
			weight := 1.0
			if w, ok := input.Payload["weight"].(float64); ok && w > 0 {
				weight = w
			}
			m.process.Field().Boost(fieldKey, weight)
			data["field_boosted"] = true
			data["resolved_key"] = fieldKey
		}
	}

	// Route every emission through AppendEvent (hash chain + broker fan-out).
	// Fixes the cogos#10 orphan-file bug: pre-refactor writes landed in a
	// flat .cog/ledger/events.jsonl bypassing both the chain and the bus.
	source := "mcp-client"
	if input.FromSession != "" {
		source = "mcp-client:" + input.FromSession
	}
	if err := m.process.EmitEvent(input.Type, data, source); err != nil {
		return fallbackResult(fmt.Sprintf("emit failed: %v", err),
			"echo '{\"type\":\"...\"}' >> .cog/ledger/<session_id>/events.jsonl")
	}

	result := map[string]any{
		"emitted":    true,
		"type":       input.Type,
		"session_id": m.process.SessionID(),
	}
	if input.FromSession != "" {
		result["from_session"] = input.FromSession
	}
	return marshalResult(result)
}

func (m *MCPServer) toolReadLedger(ctx context.Context, req *mcp.CallToolRequest, input readLedgerInput) (*mcp.CallToolResult, any, error) {
	q := LedgerQuery{
		SessionID:      input.SessionID,
		EventType:      input.EventType,
		AfterSeq:       input.AfterSeq,
		SinceTimestamp: input.SinceTimestamp,
		Limit:          input.Limit,
		VerifyChain:    input.VerifyChain,
	}
	result, err := QueryLedger(m.cfg.WorkspaceRoot, q)
	if err != nil {
		return fallbackResult(fmt.Sprintf("read ledger failed: %v", err),
			"ls .cog/ledger/ && cat .cog/ledger/<session_id>/events.jsonl")
	}
	return m.cappedMarshal(result)
}

func (m *MCPServer) toolReadEvents(ctx context.Context, req *mcp.CallToolRequest, input readEventsInput) (*mcp.CallToolResult, any, error) {
	now := time.Now().UTC()

	sinceTime, err := ParseSinceDuration(input.Since, now)
	if err != nil {
		return fallbackResult(fmt.Sprintf("bad since: %v", err),
			"check duration format (RFC3339 or '5m'/'1h')")
	}
	untilTime, err := ParseSinceDuration(input.Until, now)
	if err != nil {
		return fallbackResult(fmt.Sprintf("bad until: %v", err),
			"check duration format (RFC3339 or '5m'/'1h')")
	}

	q := EventQuery{
		SessionID:        input.SessionID,
		EventTypePattern: input.EventType,
		Source:           input.Source,
		Since:            sinceTime,
		Until:            untilTime,
		Limit:            input.Limit,
		Order:            input.Order,
	}
	result, err := QueryEvents(m.cfg.WorkspaceRoot, q)
	if err != nil {
		return fallbackResult(fmt.Sprintf("read events failed: %v", err),
			"ls .cog/ledger/ && cat .cog/ledger/*/events.jsonl")
	}
	return m.cappedMarshal(result)
}

func (m *MCPServer) toolTailEvents(ctx context.Context, req *mcp.CallToolRequest, input tailEventsInput) (*mcp.CallToolResult, any, error) {
	if m.process == nil {
		return fallbackResult("process not initialised",
			"curl -N http://localhost:6931/v1/events/stream")
	}
	broker := m.process.Broker()
	if broker == nil {
		return fallbackResult("event broker not available",
			"curl -N http://localhost:6931/v1/events/stream")
	}

	// Parse bounds.
	const (
		defaultTailMaxEvents = 100
		maxTailMaxEvents     = 1000
		defaultTailDuration  = 60 * time.Second
		maxTailDuration      = 10 * time.Minute
	)
	maxEvents := input.MaxEvents
	if maxEvents <= 0 {
		maxEvents = defaultTailMaxEvents
	}
	if maxEvents > maxTailMaxEvents {
		maxEvents = maxTailMaxEvents
	}
	maxDur := defaultTailDuration
	if input.MaxDuration != "" {
		d, err := time.ParseDuration(input.MaxDuration)
		if err != nil || d <= 0 {
			return fallbackResult(fmt.Sprintf("bad max_duration: %q", input.MaxDuration),
				"use a Go duration string like '30s' or '5m'")
		}
		maxDur = d
	}
	if maxDur > maxTailDuration {
		maxDur = maxTailDuration
	}

	// Parse `since` for replay.
	now := time.Now().UTC()
	sinceTime, err := ParseSinceDuration(input.Since, now)
	if err != nil {
		return fallbackResult(fmt.Sprintf("bad since: %v", err),
			"check duration format (RFC3339 or '5m'/'1h')")
	}

	filter := EventFilter{
		SessionID:        input.SessionID,
		EventTypePattern: input.EventType,
		Source:           input.Source,
	}

	// Apply request context if available, plus our own max-duration cap.
	tailCtx, cancel := context.WithTimeout(ctx, maxDur)
	defer cancel()

	sub, err := broker.Subscribe(tailCtx, filter)
	if err != nil {
		return fallbackResult(fmt.Sprintf("subscribe failed: %v", err),
			"curl -N http://localhost:6931/v1/events/stream")
	}
	defer sub.Cancel()

	// Replay matching ring entries first so reconnecting clients catch up.
	replay := broker.RingReplay(filter, sinceTime)
	events := make([]LedgerEvent, 0, maxEvents)
	for _, env := range replay {
		events = append(events, envelopeToLedgerEvent(env))
		if len(events) >= maxEvents {
			return m.cappedMarshal(map[string]any{
				"count":          len(events),
				"events":         events,
				"stopped_reason": "max_events",
			})
		}
	}

	// Live loop.
	stopped := "max_duration"
tailLoop:
	for len(events) < maxEvents {
		select {
		case env, ok := <-sub.Events:
			if !ok {
				stopped = "session_end"
				break tailLoop
			}
			events = append(events, envelopeToLedgerEvent(env))
			if len(events) >= maxEvents {
				stopped = "max_events"
				break tailLoop
			}
		case <-tailCtx.Done():
			// Either ctx cancelled by caller or maxDur expired. ctx.Err
			// distinguishes: deadline → max_duration, canceled → client_cancel.
			if tailCtx.Err() == context.DeadlineExceeded {
				stopped = "max_duration"
			} else {
				stopped = "client_cancel"
			}
			break tailLoop
		}
	}
	if len(events) >= maxEvents {
		stopped = "max_events"
	}

	return m.cappedMarshal(map[string]any{
		"count":          len(events),
		"events":         events,
		"stopped_reason": stopped,
	})
}

// toolGetIndex — no longer registered as an MCP tool; used by the internal tool loop (tool_loop.go).
// Output is capped per ADR-066 pointer-discipline: the index can cover thousands of CogDocs,
// so we apply the configured byte cap to prevent context blowout. Callers needing deeper
// inspection should dereference specific cog: URIs via cog_read_cogdoc.
func (m *MCPServer) toolGetIndex(ctx context.Context, req *mcp.CallToolRequest, input getIndexInput) (*mcp.CallToolResult, any, error) {
	index, err := BuildMemoryIndex(m.cfg.WorkspaceRoot, input.Sector)
	if err != nil {
		return textResult(fmt.Sprintf("index build failed: %v", err))
	}
	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}
	return capMarshalResult(index, maxBytes)
}

func (m *MCPServer) toolIngest(ctx context.Context, req *mcp.CallToolRequest, input ingestInput) (*mcp.CallToolResult, any, error) {
	if input.Source == "" || input.Format == "" || input.Data == "" {
		return textResult("source, format, and data are required")
	}

	// Build the pipeline fresh (stateless except for workspace root).
	pipeline := NewIngestPipeline(m.cfg.WorkspaceRoot)
	pipeline.Register(NewURLDecomposer(m.cfg.WorkspaceRoot))

	// Build the IngestRequest from input.
	ingestReq := &IngestRequest{
		Source:   IngestSource(input.Source),
		Format:   IngestFormat(input.Format),
		Data:     input.Data,
		Metadata: input.Metadata,
	}

	// Derive a source ID for dedup. For URLs, it's the URL itself.
	// For other formats, use data as the key (or metadata source_id if provided).
	sourceID := input.Data
	if id, ok := input.Metadata["source_id"]; ok && id != "" {
		sourceID = id
	}

	// Check for duplicates.
	if pipeline.CheckDuplicate(sourceID) {
		return marshalResult(map[string]any{
			"ingested":  false,
			"reason":    "duplicate",
			"source_id": sourceID,
		})
	}

	// Run decomposition.
	result, err := pipeline.Ingest(ctx, ingestReq)
	if err != nil {
		return textResult(fmt.Sprintf("ingest failed: %v", err))
	}

	// Ensure source ID is set on the result.
	if result.SourceID == "" {
		result.SourceID = sourceID
	}
	block := NormalizeIngestBlock(ingestReq, result)
	block.WorkspaceID = filepath.Base(m.cfg.WorkspaceRoot)
	// G2 PART B: resolve the bound subject for this transport session.
	// m.resolveTransportSession looks up the correlation recorded at
	// cog_register_session time. When found, the subject is the correct
	// attribution. When not found (no register call, in-process test path
	// where req is nil or Session.ID() is empty), fall back to nucleus.Name
	// — same behaviour as pre-G2, so flag-off regression is impossible.
	ingestTransportID := ""
	if req != nil && req.Session != nil {
		ingestTransportID = req.Session.ID()
	}
	if entry, ok := m.resolveTransportSession(ingestTransportID); ok && entry.Subject != "" {
		block.TargetIdentity = entry.Subject
	} else if m.nucleus != nil {
		block.TargetIdentity = m.nucleus.Name
	}
	if m.process != nil {
		block.SessionID = m.process.SessionID()
		block.SourceIdentity = m.process.NodeID
		m.process.RecordBlock(block)
	}

	// Determine inbox subdirectory based on content type.
	var subdir string
	switch {
	case result.ContentType == ContentArticle || result.ContentType == ContentPaper ||
		result.ContentType == ContentRepo || result.ContentType == ContentVideo ||
		result.ContentType == ContentTool || result.URL != "":
		subdir = "links"
	case input.Format == string(FormatConversation) || input.Format == string(FormatMessage):
		subdir = "conversations"
	default:
		subdir = "documents"
	}

	// Generate filename: {source}-{date}-{slug}.cog.md
	//
	// input.Source is a caller-supplied cog_ingest MCP tool argument, never
	// validated against the IngestSource enum before this point (that
	// switch has no default/rejection case), and embedded here without
	// sanitization (myrgic/cogos#489 round 4 — same class as the ingest
	// pipeline's quarantine.go sibling). A source of "http:cog" produced
	// the on-disk filename "http:cog-<date>-<slug>.cog.md", the same
	// NTFS-illegal colon #489 targets. containedJoin below (via
	// WriteCogDoc) already defeats a "/"-bearing source used for
	// directory-escape, so this is a portability, not an escape, fix.
	date := time.Now().UTC().Format("2006-01-02")
	slug := slugify(result.Title)
	if slug == "" {
		slug = "untitled"
	}
	filename := fmt.Sprintf("%s-%s-%s.cog.md", pathsafe.SanitizeComponent(input.Source), date, slug)
	memPath := filepath.Join("semantic", "inbox", subdir, filename)

	// Write the CogDoc.
	opts := CogDocWriteOpts{
		Title:    result.Title,
		Content:  buildIngestContent(result),
		Tags:     result.Tags,
		Status:   string(StatusRaw),
		DocType:  string(result.ContentType),
		Source:   string(result.Source),
		URL:      result.URL,
		SourceID: result.SourceID,
	}

	decision := DefaultMembranePolicy{}.Evaluate(block)
	memPath, opts, shouldWrite := ApplyMembraneDecision(memPath, opts, decision)
	if !shouldWrite {
		slog.Info("ingest: discarded by membrane policy", "reason", decision.Reason)
		return marshalResult(map[string]any{
			"ingested":  false,
			"decision":  string(decision.Decision),
			"reason":    decision.Reason,
			"source_id": result.SourceID,
		})
	}
	if decision.Decision == Quarantine {
		slog.Warn("ingest: quarantined by membrane policy", "reason", decision.QuarantineReason, "path", memPath)
	}
	if decision.Decision == Defer {
		slog.Info("ingest: deferred by membrane policy", "reason", decision.Reason, "path", memPath)
	}

	writeResult, err := m.cogdocSvc.WriteAndSync(memPath, opts)
	if err != nil {
		return textResult(fmt.Sprintf("write cogdoc failed: %v", err))
	}

	return marshalResult(map[string]any{
		"ingested":     true,
		"decision":     string(decision.Decision),
		"reason":       decision.Reason,
		"path":         memPath,
		"uri":          writeResult.URI,
		"title":        result.Title,
		"content_type": string(result.ContentType),
	})
}

// toolTailKernelLog reads the kernel slog JSONL sink at
// <workspace>/.cog/run/kernel.log.jsonl and returns the most recent entries
// that match the filters in input. Mirror of GET /v1/kernel-log; shares the
// same QueryKernelLog backend. See Agent U's kernel-slog-api design for
// rationale (this is the surface half; log_capture.go is the capture half).
func (m *MCPServer) toolTailKernelLog(ctx context.Context, req *mcp.CallToolRequest, input tailKernelLogInput) (*mcp.CallToolResult, any, error) {
	q, err := BuildKernelLogQueryFromValues(
		intToStr(input.Limit),
		input.Level,
		input.Substring,
		input.Since,
		input.Until,
		time.Now(),
	)
	if err != nil {
		return textResult(fmt.Sprintf("invalid kernel-log query: %v", err))
	}

	path := kernelLogPathFor(m.cfg)
	result, err := QueryKernelLog(path, q)
	if err != nil {
		return fallbackResult(
			fmt.Sprintf("kernel log query failed: %v", err),
			fmt.Sprintf("tail -n 100 %s | jq -c .", path),
		)
	}
	return m.cappedMarshal(result)
}

// intToStr renders an int as a string for BuildKernelLogQueryFromValues.
// Zero is returned as "" so callers get the default limit rather than 0.
func intToStr(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d", n)
}

// --- Agent state / loop control tools -------------------------------------

// toolListAgents implements cog_list_agents — returns the set of active
// agents. Today always a one-element list ("primary"). See agent-T-agent-
// state-design §4.1.
func (m *MCPServer) toolListAgents(ctx context.Context, req *mcp.CallToolRequest, input listAgentsInput) (*mcp.CallToolResult, any, error) {
	resp, err := QueryListAgents(ctx, m.agentController, ListAgentsRequest{IncludeStopped: input.IncludeStopped})
	if err != nil {
		return agentErrorResult(err, "curl http://localhost:6931/v1/agents")
	}
	return marshalResult(resp)
}

// toolGetAgentState implements cog_get_agent_state — full snapshot of
// one agent. See agent-T-agent-state-design §4.1.
func (m *MCPServer) toolGetAgentState(ctx context.Context, req *mcp.CallToolRequest, input getAgentStateInput) (*mcp.CallToolResult, any, error) {
	snap, err := QueryGetAgent(ctx, m.agentController, GetAgentRequest{
		AgentID:      input.AgentID,
		IncludeTrace: input.IncludeTrace,
		TraceLimit:   input.TraceLimit,
	})
	if err != nil {
		return agentErrorResult(err, "curl http://localhost:6931/v1/agents/primary")
	}
	return m.cappedMarshal(snap)
}

// toolTriggerAgentLoop implements cog_trigger_agent_loop — manually
// invoke one cycle. See agent-T-agent-state-design §4.1.
func (m *MCPServer) toolTriggerAgentLoop(ctx context.Context, req *mcp.CallToolRequest, input triggerAgentLoopInput) (*mcp.CallToolResult, any, error) {
	result, err := QueryTriggerAgent(ctx, m.agentController, TriggerAgentRequest{
		AgentID: input.AgentID,
		Reason:  input.Reason,
		Wait:    input.Wait,
	})
	if err != nil {
		return agentErrorResult(err, "curl -X POST http://localhost:6931/v1/agents/primary/tick")
	}
	return marshalResult(result)
}

// toolDispatchToHarness implements cog_dispatch_to_harness — Phase 2
// task-parameterized transport. See engine/agent_dispatch.go for the
// underlying contract and the project_cogos_foveal_and_peripheral.md memory
// note for the architectural framing.
//
// Async=true (dispatchToHarnessInput.Async) switches this from a blocking
// call to a job-handle receipt: see dispatch_jobs.go and toolPollDispatch.
func (m *MCPServer) toolDispatchToHarness(ctx context.Context, req *mcp.CallToolRequest, input dispatchToHarnessInput) (*mcp.CallToolResult, any, error) {
	dr := DispatchRequest{
		AgentID:        input.AgentID,
		Task:           input.Task,
		Scope:          input.Scope,
		Tools:          input.Tools,
		Model:          DispatchModel(input.Model),
		Provider:       input.Provider,
		TimeoutSeconds: input.Timeout,
		// Kernel policy, not a caller parameter: the cap always comes from
		// this node's config (dispatch_timeout_cap_seconds, default 600).
		TimeoutCapSeconds: m.cfg.DispatchTimeoutCap(),
		N:                 input.N,
		SystemPrompt:      input.SystemPrompt,
		Thinking:          input.Thinking,
		Identity: DispatchIdentity{
			Iss:    input.Iss,
			Sub:    input.Sub,
			Aud:    input.Aud,
			Claims: input.Claims,
		},
		TargetNode: input.TargetNode,
		Ambient:    input.Ambient,
	}

	if input.Async {
		return m.dispatchToHarnessAsync(ctx, dr)
	}

	result, err := QueryDispatchToHarnessRouted(ctx, m.agentController, m.clusterRouter, dr)
	if err != nil {
		return agentErrorResult(err, "curl -X POST http://localhost:6931/v1/agents/primary/dispatch -d @body.json")
	}
	return m.cappedMarshal(result)
}

// dispatchToHarnessAsync is the MCP-tool-facing wrapper around
// startAsyncDispatch — see that function for the mechanism.
func (m *MCPServer) dispatchToHarnessAsync(ctx context.Context, dr DispatchRequest) (*mcp.CallToolResult, any, error) {
	receipt := m.startAsyncDispatch(ctx, dr)
	return marshalResult(receipt)
}

// startAsyncDispatch registers a job, spawns the real dispatch under a
// context detached from the caller's request (see dispatch_jobs.go doc
// comment — #432/e4e6aca: the request context dies the instant the calling
// handler returns, and the goroutine must not inherit that cancellation),
// and returns the job handle immediately. Shared by both transports that
// accept async=true: the MCP tool (dispatchToHarnessAsync) and the HTTP
// handler (Server.handleAgentDispatch, serve_agents.go).
//
// The job's CycleID is a registry-minted correlation id, not the harness-
// internal cycle id LocalHarnessController.DispatchToHarness mints for its
// own ledger events (local_agent_harness.go:1580) — that id is generated
// inside the dispatcher and isn't visible at this layer before the call
// starts. Both ids end up in the ledger (harness.dispatch.job.issued carries
// this job's CycleID; harness.dispatch.start/end carry the dispatcher's) so
// either can be used to locate the other via time-window correlation.
// dispatchOutcome is the batch-derived verdict for an async dispatch. It
// exists because a nil error from DispatchToHarness says only that the batch
// ran — every per-slot failure, including a timeout, is reported inside the
// batch (DispatchResult.Success / .Error), never as a returned error.
type dispatchOutcome struct {
	ok       bool
	timedOut bool
	failed   int
	total    int
	errMsg   string
}

// summarizeDispatchOutcome derives the terminal verdict from batch contents.
// A nil or empty batch is NOT treated as success: a dispatch that produced no
// slot produced no work, and silently recording it as done is the same class
// of bug as recording a timeout as done.
func summarizeDispatchOutcome(batch *DispatchBatchResult) dispatchOutcome {
	if batch == nil {
		return dispatchOutcome{errMsg: "dispatch returned no batch"}
	}
	out := dispatchOutcome{total: len(batch.Results)}
	if out.total == 0 {
		out.errMsg = "dispatch produced no slots"
		return out
	}
	var reasons []string
	for _, r := range batch.Results {
		if r.Success {
			continue
		}
		out.failed++
		reason := r.Error
		if reason == "" {
			reason = "unknown error"
		}
		if reason == "timeout" {
			out.timedOut = true
		}
		reasons = append(reasons, fmt.Sprintf("slot %d: %s", r.Index, reason))
	}
	if out.failed == 0 {
		out.ok = true
		return out
	}
	out.errMsg = fmt.Sprintf("%d/%d slots failed (%s)",
		out.failed, out.total, strings.Join(reasons, "; "))
	return out
}

func (m *MCPServer) startAsyncDispatch(ctx context.Context, dr DispatchRequest) dispatchJobReceipt {
	// Mint the registry's own correlation id up front. This is NOT the same
	// id as the harness-internal cycleID DispatchToHarness mints later
	// (local_agent_harness.go:1604) — that one doesn't exist yet at this
	// layer, before the dispatch has even started. This id just needs to be
	// non-empty and stable across the receipt + the job.issued ledger event
	// so a caller (or a bus consumer correlating harness.dispatch.job.issued
	// against harness.dispatch.start/end) has something to key on; see the
	// package doc above this function for how the two ids relate.
	cycleID := uuid.NewString()
	jobID := m.dispatchJobs.Create(cycleID)
	rec, _ := m.dispatchJobs.Get(jobID)

	_ = EmitLedgerEvent(m.cfg, map[string]any{
		"type":   "harness.dispatch.job.issued",
		"source": "mcp-dispatch-async",
		"payload": map[string]any{
			"job_id":   jobID,
			"cycle_id": cycleID,
			"task":     truncateDigest(dr.Task),
		},
	})

	detached, cancel := detachedDispatchContext(ctx, dr.TimeoutSeconds)
	agentController := m.agentController
	clusterRouter := m.clusterRouter
	registry := m.dispatchJobs
	cfg := m.cfg

	go func() {
		defer cancel()
		registry.MarkRunning(jobID)
		result, err := QueryDispatchToHarnessRouted(detached, agentController, clusterRouter, dr)
		if err != nil {
			registry.Fail(jobID, err.Error())
			_ = EmitLedgerEvent(cfg, map[string]any{
				"type":   "harness.dispatch.job.completed",
				"source": "mcp-dispatch-async",
				"payload": map[string]any{
					"job_id": jobID,
					"status": "failed",
					"error":  err.Error(),
				},
			})
			return
		}
		// A nil error means the BATCH ran, not that the work succeeded.
		// DispatchToHarness's contract is explicit: it "returns once every
		// slot has either completed, errored, or timed out" and the batch is
		// non-nil whenever err is nil. Per-slot failures — including a
		// deadline-exceeded slot, which sets Error="timeout" and leaves
		// Success=false (local_agent_harness.go, dispatchSlot) — live in the
		// batch, never in err. Reading only err reported a fully-timed-out
		// dispatch as status:"done" in both the ledger and the job registry.
		if summary := summarizeDispatchOutcome(result); !summary.ok {
			// FailWithResult, not Fail: a partially-failed batch still holds
			// the output of the slots that succeeded, and a poller must be
			// able to retrieve it. Fail() records only the error string.
			registry.FailWithResult(jobID, summary.errMsg, result)
			_ = EmitLedgerEvent(cfg, map[string]any{
				"type":   "harness.dispatch.job.completed",
				"source": "mcp-dispatch-async",
				"payload": map[string]any{
					"job_id":       jobID,
					"status":       "failed",
					"error":        summary.errMsg,
					"timed_out":    summary.timedOut,
					"failed_slots": summary.failed,
					"total_slots":  summary.total,
				},
			})
			return
		}
		registry.Complete(jobID, result)
		_ = EmitLedgerEvent(cfg, map[string]any{
			"type":   "harness.dispatch.job.completed",
			"source": "mcp-dispatch-async",
			"payload": map[string]any{
				"job_id":      jobID,
				"status":      "done",
				"total_slots": summarizeDispatchOutcome(result).total,
			},
		})
	}()

	receipt := dispatchJobReceipt{JobID: jobID, Status: string(DispatchJobPending)}
	if rec != nil {
		receipt.CycleID = rec.CycleID
	}
	return receipt
}

// toolPollDispatch implements cog_poll_dispatch — the MCP convenience poll
// surface for an async cog_dispatch_to_harness job. The primary poll surface
// is GET /v1/dispatch-jobs/{id} (serve_agents.go); this tool exists so a
// caller that only has MCP available doesn't need the HTTP port.
func (m *MCPServer) toolPollDispatch(ctx context.Context, req *mcp.CallToolRequest, input pollDispatchInput) (*mcp.CallToolResult, any, error) {
	jobID := strings.TrimSpace(input.JobID)
	if jobID == "" {
		return fallbackResult("job_id is required", "curl http://localhost:6931/v1/dispatch-jobs/<job_id>")
	}
	rec, ok := m.dispatchJobs.Get(jobID)
	if !ok {
		return fallbackResult(fmt.Sprintf("no such dispatch job %q (unknown, or expired past the %s retention window)", jobID, dispatchJobTTL),
			"curl http://localhost:6931/v1/dispatch-jobs/"+jobID)
	}
	return m.cappedMarshal(dispatchJobStatusFromRecord(rec))
}

// dispatchJobStatusFromRecord converts a DispatchJobRecord into the wire
// response shape shared by the MCP poll tool and the HTTP poll route.
func dispatchJobStatusFromRecord(rec *DispatchJobRecord) dispatchJobStatusResponse {
	return dispatchJobStatusResponse{
		JobID:     rec.JobID,
		Status:    string(rec.State),
		CycleID:   rec.CycleID,
		Result:    rec.Result,
		Error:     rec.Err,
		CreatedAt: rec.CreatedAt.Format(time.RFC3339),
		UpdatedAt: rec.UpdatedAt.Format(time.RFC3339),
	}
}

// agentErrorResult translates AgentControllerError into the MCP fallback
// format used by the rest of this file. Unknown errors are surfaced as
// internal-error text responses.
func agentErrorResult(err error, fallback string) (*mcp.CallToolResult, any, error) {
	msg := err.Error()
	if ace, ok := err.(*AgentControllerError); ok && ace != nil {
		msg = ace.Message
	}
	return fallbackResult(msg, fallback)
}

// --- Peer-awareness tool (Phase 1B of the 4E ambient-awareness loop) --

// peerAwarenessInput mirrors the HTTP query parameters: sid (required),
// budget (default 500), window (default 15m), include_peers (default true).
type peerAwarenessInput struct {
	Sid          string `json:"sid" jsonschema:"Session id to render the packet from. Required. Must match the lowercase/hyphen sid shape."`
	Budget       int    `json:"budget,omitempty" jsonschema:"Token budget for the whole packet. Default 500, hard cap 4000."`
	Window       string `json:"window,omitempty" jsonschema:"How far back to look for events. Go duration string (e.g. '15m', '1h'). Default '15m'."`
	IncludePeers *bool  `json:"include_peers,omitempty" jsonschema:"When true (default), include the peer-overlap section. When false, only MY ACTIVITY / HANDOFFS / COORD render."`
}

// toolRenderPeerAwarenessPacket is the MCP surface for
// cog_render_peer_awareness_packet. Wraps RenderPeerAwarenessPacket using
// deps from the MCPServer's wired dependencies (bus manager, handoff
// registry, attention log via the Server adapter if available).
//
// The MCP server today doesn't carry a Server handle — it holds the
// BusSessionManager, SessionRegistry, HandoffRegistry directly — so we
// build the deps bundle inline here and fall back to an attention log
// reader over the workspace file when needed.
func (m *MCPServer) toolRenderPeerAwarenessPacket(ctx context.Context, req *mcp.CallToolRequest, input peerAwarenessInput) (*mcp.CallToolResult, any, error) {
	sid := strings.TrimSpace(input.Sid)
	if sid == "" {
		return fallbackResult("sid is required",
			"curl 'http://localhost:6931/v1/peer-awareness?sid=<sid>'")
	}
	if err := ValidateSid(sid); err != nil {
		return fallbackResult(fmt.Sprintf("invalid sid: %v", err),
			"curl 'http://localhost:6931/v1/peer-awareness?sid=<sid>'")
	}

	paReq := PeerAwarenessRequest{
		Sid:          sid,
		Budget:       input.Budget,
		IncludePeers: true,
	}
	if input.IncludePeers != nil {
		paReq.IncludePeers = *input.IncludePeers
	}
	if input.Window != "" {
		d, err := time.ParseDuration(input.Window)
		if err != nil {
			return fallbackResult(fmt.Sprintf("invalid window: %v", err),
				"curl 'http://localhost:6931/v1/peer-awareness?sid=<sid>&window=15m'")
		}
		paReq.Window = d
	}

	deps := peerAwarenessDeps{}
	if m.busSessions != nil {
		deps.bus = m.busSessions
		deps.renderer = m.busSessions
	}
	if m.handoffRegistry != nil {
		deps.handoffs = m.handoffRegistry
	}
	// Attention log — the MCP surface doesn't hold a pointer, so fall
	// back to reading the workspace file directly if it exists.
	if m.cfg != nil && m.cfg.WorkspaceRoot != "" {
		path := filepath.Join(m.cfg.WorkspaceRoot, ".cog", "run", "attention.jsonl")
		if _, err := os.Stat(path); err == nil {
			deps.attn = fileAttentionReader{path: path}
		}
	}

	result, err := RenderPeerAwarenessPacket(deps, paReq)
	if err != nil {
		return fallbackResult(fmt.Sprintf("render failed: %v", err),
			"curl 'http://localhost:6931/v1/peer-awareness?sid=<sid>'")
	}
	return m.cappedMarshal(result)
}

// toolReadToolCalls is the MCP handler for cog_read_tool_calls. It parses the
// input filters, invokes QueryToolCalls, and returns the stitched result.
func (m *MCPServer) toolReadToolCalls(ctx context.Context, req *mcp.CallToolRequest, input readToolCallsInput) (*mcp.CallToolResult, any, error) {
	q := ToolCallQuery{
		SessionID:     input.SessionID,
		ToolName:      input.ToolName,
		Status:        input.Status,
		Source:        input.Source,
		Ownership:     input.Ownership,
		CallID:        input.CallID,
		Limit:         input.Limit,
		Order:         input.Order,
		IncludeArgs:   input.IncludeArgs,
		IncludeOutput: input.IncludeOutput,
	}
	if input.Since != "" {
		ts, err := parseTimeOrDuration(input.Since)
		if err != nil {
			return textResult(fmt.Sprintf("parse since: %v", err))
		}
		q.Since = ts
	}
	if input.Until != "" {
		ts, err := parseTimeOrDuration(input.Until)
		if err != nil {
			return textResult(fmt.Sprintf("parse until: %v", err))
		}
		q.Until = ts
	}
	result, err := QueryToolCalls(m.cfg.WorkspaceRoot, q)
	if err != nil {
		return fallbackResult(fmt.Sprintf("query failed: %v", err),
			"grep '\"type\":\"tool\\.' .cog/ledger/*/events.jsonl")
	}
	return m.cappedMarshal(result)
}

// toolTailToolCalls returns a snapshot of the most recent tool-call rows for
// the filter set. When Agent N's event bus lands this will become a proper
// SSE-style stream; the snapshot behavior is a safe stand-in (same data, same
// shape) that does not rely on a broker.
func (m *MCPServer) toolTailToolCalls(ctx context.Context, req *mcp.CallToolRequest, input tailToolCallsInput) (*mcp.CallToolResult, any, error) {
	limit := input.MaxEvents
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	q := ToolCallQuery{
		SessionID: input.SessionID,
		ToolName:  input.ToolName,
		Status:    input.Status,
		Source:    input.Source,
		Ownership: input.Ownership,
		CallID:    input.CallID,
		Limit:     limit,
		Order:     "desc",
		// Tails default to showing full rows — callers here are actively
		// observing so the PII opt-out is moot.
		IncludeArgs:   true,
		IncludeOutput: true,
	}
	if input.Since != "" {
		ts, err := parseTimeOrDuration(input.Since)
		if err != nil {
			return textResult(fmt.Sprintf("parse since: %v", err))
		}
		q.Since = ts
	}
	// MaxDuration is accepted for forward compatibility with a real stream.
	// The snapshot path is instantaneous; no wait is needed.
	_ = input.MaxDuration

	result, err := QueryToolCalls(m.cfg.WorkspaceRoot, q)
	if err != nil {
		return fallbackResult(fmt.Sprintf("tail failed: %v", err),
			"tail -f .cog/ledger/*/events.jsonl | grep '\"type\":\"tool\\.'")
	}
	stopped := "snapshot"
	return m.cappedMarshal(map[string]any{
		"count":          result.Count,
		"events":         result.Calls,
		"stopped_reason": stopped,
		"truncated":      result.Truncated,
	})
}

// parseTimeOrDuration accepts either an RFC3339 timestamp ("2026-04-21T…") or
// a relative duration ("5m", "1h", "24h"). Durations subtract from "now".
func parseTimeOrDuration(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().UTC().Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("not RFC3339 and not a Go duration: %q", s)
}

// toolReadConversation is the MCP handler for cog_read_conversation.
// Thin wrapper over QueryConversation — same shape the HTTP surface returns.
func (m *MCPServer) toolReadConversation(ctx context.Context, req *mcp.CallToolRequest, input readConversationInput) (*mcp.CallToolResult, any, error) {
	sessionID := input.SessionID
	if sessionID == "" && m.process != nil {
		sessionID = m.process.SessionID()
	}
	includeFull := true
	if input.IncludeFull != nil {
		includeFull = *input.IncludeFull
	}
	includeTools := true
	if input.IncludeTools != nil {
		includeTools = *input.IncludeTools
	}
	q := ConversationQuery{
		SessionID:    sessionID,
		AfterTurn:    input.AfterTurn,
		BeforeTurn:   input.BeforeTurn,
		Limit:        input.Limit,
		IncludeFull:  includeFull,
		IncludeTools: includeTools,
		Order:        input.Order,
	}
	if input.Since != "" {
		t, err := time.Parse(time.RFC3339, input.Since)
		if err != nil {
			return textResult(fmt.Sprintf("invalid since (want RFC3339): %v", err))
		}
		q.Since = t
	}
	res, err := QueryConversation(m.cfg.WorkspaceRoot, q)
	if err != nil {
		return fallbackResult(fmt.Sprintf("query failed: %v", err),
			fmt.Sprintf("jq -c . .cog/run/turns/%s.jsonl", sessionID))
	}
	return m.cappedMarshal(res)
}

// ── Config Mutation API ──────────────────────────────────────────────────────

type readConfigInput struct {
	IncludeRawYAML  bool `json:"include_raw_yaml,omitempty" jsonschema:"Also return the raw kernel.yaml bytes"`
	IncludeDefaults bool `json:"include_defaults,omitempty" jsonschema:"Also return the hardcoded defaults for comparison"`
}

type writeConfigInput struct {
	Patch  map[string]any `json:"patch" jsonschema:"RFC 7396 merge-patch object. Fields: port, consolidation_interval, heartbeat_interval, salience_days_window, output_reserve, trm_weights_path, trm_embeddings_path, trm_chunks_path, tool_call_validation_enabled, local_model, digest_paths. Explicit null deletes a key; missing keys preserved."`
	Scope  string         `json:"scope,omitempty" jsonschema:"Target section: 'top' (default) or 'v3'"`
	DryRun bool           `json:"dry_run,omitempty" jsonschema:"If true, validate + return diff without writing"`
}

type rollbackConfigInput struct {
	Backup   string `json:"backup,omitempty" jsonschema:"Backup filename (e.g. kernel.yaml.bak-2026-04-21T16-30-00Z). Empty = most recent."`
	ListOnly bool   `json:"list_only,omitempty" jsonschema:"If true, return the list of backups without restoring"`
}

// requireConfigMutationMCP checks the EnableConfigMutation gate for the MCP
// transport. Returns a non-nil (*mcp.CallToolResult, any, error) triple when
// the gate is closed — callers should return it immediately — and a nil
// triple when the gate is open and the caller should proceed. Mirrors the
// HTTP gate's error shape (requireConfigMutation in serve_config.go) so a
// caller sees the same {"error":"disabled","detail":"..."} body regardless
// of transport; unlike the HTTP path this is surfaced as an MCP tool error
// (IsError: true) rather than a 403 status code.
func (m *MCPServer) requireConfigMutationMCP() (*mcp.CallToolResult, any, error) {
	if m.cfg != nil && m.cfg.EnableConfigMutation {
		return nil, nil, nil
	}
	b, _ := json.Marshal(map[string]string{
		"error":  "disabled",
		"detail": "config access via MCP is disabled; set enable_config_mutation: true in kernel.yaml",
	})
	res, _, _ := textResult(string(b))
	res.IsError = true
	return res, nil, nil
}

func (m *MCPServer) toolReadConfig(ctx context.Context, req *mcp.CallToolRequest, input readConfigInput) (*mcp.CallToolResult, any, error) {
	if res, data, err := m.requireConfigMutationMCP(); res != nil {
		return res, data, err
	}
	snapshot, err := ReadConfigSnapshot(m.cfg.WorkspaceRoot, input.IncludeRawYAML, input.IncludeDefaults)
	if err != nil {
		// Parse error — still surface whatever we could read but tag the error.
		return marshalResult(map[string]any{
			"effective_config": snapshot.EffectiveConfig,
			"path":             snapshot.Path,
			"exists":           snapshot.Exists,
			"raw_yaml":         snapshot.RawYAML,
			"defaults":         snapshot.Defaults,
			"parse_error":      err.Error(),
		})
	}
	return marshalResult(snapshot)
}

func (m *MCPServer) toolWriteConfig(ctx context.Context, req *mcp.CallToolRequest, input writeConfigInput) (*mcp.CallToolResult, any, error) {
	if res, data, err := m.requireConfigMutationMCP(); res != nil {
		return res, data, err
	}
	result, err := WriteConfigPatch(m.cfg.WorkspaceRoot, input.Patch, WriteConfigOptions{
		Scope:  input.Scope,
		DryRun: input.DryRun,
	})
	if err != nil {
		return fallbackResult(fmt.Sprintf("write config failed: %v", err), "edit .cog/config/kernel.yaml and run './scripts/cog restart'")
	}
	return marshalResult(result)
}

func (m *MCPServer) toolRollbackConfig(ctx context.Context, req *mcp.CallToolRequest, input rollbackConfigInput) (*mcp.CallToolResult, any, error) {
	if res, data, err := m.requireConfigMutationMCP(); res != nil {
		return res, data, err
	}
	result, err := RollbackConfig(m.cfg.WorkspaceRoot, RollbackOptions{
		Backup:   input.Backup,
		ListOnly: input.ListOnly,
	})
	if err != nil {
		return fallbackResult(fmt.Sprintf("rollback failed: %v", err), "mv .cog/config/kernel.yaml.bak-<timestamp> .cog/config/kernel.yaml")
	}
	return marshalResult(result)
}

// resourceConfig serves the cogos://config MCP resource. Gated by
// Config.EnableConfigMutation, same as the three cog_*_config tools — a
// cog-review re-review round (PR #460) found this resource read the exact
// same ReadConfigSnapshot data via the untouched Resources API, bypassing
// the gate the tools had just been given. MCP resources have no IsError /
// structured-error-body convention (unlike CallToolResult); the established
// pattern in this file (see resourceState above) is to return a plain Go
// error, which the MCP SDK surfaces as a protocol-level error to the
// caller — so this uses the same "disabled" wording as the tool/HTTP gates
// rather than a JSON body.
func (m *MCPServer) resourceConfig(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if m.cfg == nil || !m.cfg.EnableConfigMutation {
		return nil, fmt.Errorf("disabled: config access via MCP is disabled; set enable_config_mutation: true in kernel.yaml")
	}
	snapshot, _ := ReadConfigSnapshot(m.cfg.WorkspaceRoot, false, true)
	b, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(b),
		}},
	}, nil
}

// toolSearchTraces serves the MCP-side entry for `cog_search_traces`.
// Mirrors the HTTP /v1/traces handler: validates inputs, delegates to
// QueryTraces, returns a JSON-encoded TraceQueryResult.
func (m *MCPServer) toolSearchTraces(ctx context.Context, req *mcp.CallToolRequest, input searchTracesInput) (*mcp.CallToolResult, any, error) {
	tq, err := buildTraceQueryFromInput(input)
	if err != nil {
		return textResult(fmt.Sprintf("invalid trace query: %v", err))
	}
	res, err := QueryTraces(m.cfg.WorkspaceRoot, tq)
	if err != nil {
		return fallbackResult(
			fmt.Sprintf("trace search failed: %v", err),
			"ls .cog/run/*.jsonl && jq -c . .cog/run/<name>.jsonl | head",
		)
	}
	return m.cappedMarshal(res)
}

// buildTraceQueryFromInput validates the MCP input shape and normalizes it
// into a TraceQuery. Shares semantics with parseTraceQueryFromRequest so that
// the HTTP and MCP surfaces agree on defaults and bounds.
func buildTraceQueryFromInput(in searchTracesInput) (TraceQuery, error) {
	q := TraceQuery{
		Source:    TraceSource(strings.TrimSpace(in.Source)),
		Level:     strings.TrimSpace(in.Level),
		SessionID: strings.TrimSpace(in.SessionID),
		Substring: in.Substring,
		Limit:     in.Limit,
		Order:     strings.TrimSpace(in.Order),
	}
	if q.Source == "" {
		q.Source = SourceAll
	}
	if _, err := resolveSources(q.Source); err != nil {
		return TraceQuery{}, err
	}

	now := time.Now()
	if in.Since != "" {
		t, err := ParseTraceDurationOrTime(in.Since, now)
		if err != nil {
			return TraceQuery{}, fmt.Errorf("since: %w", err)
		}
		q.Since = t
	}
	if in.Until != "" {
		t, err := ParseTraceDurationOrTime(in.Until, now)
		if err != nil {
			return TraceQuery{}, fmt.Errorf("until: %w", err)
		}
		q.Until = t
	}
	if q.Limit < 0 {
		return TraceQuery{}, fmt.Errorf("limit: expected non-negative integer, got %d", q.Limit)
	}
	if q.Limit > maxTracesLimit {
		return TraceQuery{}, fmt.Errorf("limit: %d exceeds max %d", q.Limit, maxTracesLimit)
	}
	return q, nil
}

// slugify converts a string to a URL-friendly slug.
func slugify(s string) string {
	s = strings.ToLower(s)
	re := regexp.MustCompile(`[^a-z0-9]+`)
	s = re.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 50 {
		// Truncate at a hyphen boundary if possible.
		s = s[:50]
		if idx := strings.LastIndex(s, "-"); idx > 20 {
			s = s[:idx]
		}
	}
	return s
}

// buildIngestContent generates markdown body from an IngestResult.
func buildIngestContent(r *IngestResult) string {
	var sb strings.Builder
	sb.WriteString("# " + r.Title + "\n\n")
	if r.URL != "" {
		sb.WriteString("**URL:** " + r.URL + "\n\n")
	}
	if r.Domain != "" {
		sb.WriteString("**Domain:** " + r.Domain + "\n\n")
	}
	if r.Summary != "" {
		sb.WriteString("## Summary\n\n" + r.Summary + "\n\n")
	}
	if len(r.Fields) > 0 {
		sb.WriteString("## Metadata\n\n")
		for k, v := range r.Fields {
			sb.WriteString(fmt.Sprintf("- **%s:** %s\n", k, v))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (p cogdocFrontmatterPatch) empty() bool {
	return strings.TrimSpace(p.Description) == "" && len(p.Tags) == 0 && strings.TrimSpace(p.Type) == ""
}

func patchTemplateForIssues(issues []string) map[string]any {
	if len(issues) == 0 {
		return nil
	}
	template := map[string]any{}
	for _, issue := range issues {
		switch issue {
		case "missing_description":
			template["description"] = ""
		case "missing_tags":
			template["tags"] = []string{}
		case "missing_type":
			template["type"] = ""
		}
	}
	if len(template) == 0 {
		return nil
	}
	return template
}

func hasSchemaIssue(issues []string, want string) bool {
	for _, issue := range issues {
		if issue == want {
			return true
		}
	}
	return false
}

func applyFrontmatterPatch(content string, patch cogdocFrontmatterPatch) (string, cogdocFrontmatter, error) {
	var (
		raw  map[string]any
		body string
	)

	yamlBlock, extractedBody, ok := extractFrontmatterYAML(content)
	if ok {
		if err := yaml.Unmarshal([]byte(yamlBlock), &raw); err != nil {
			return "", cogdocFrontmatter{}, fmt.Errorf("parse frontmatter: %w", err)
		}
		body = extractedBody
	} else {
		raw = map[string]any{}
		body = strings.TrimLeft(content, "\r\n")
	}
	if raw == nil {
		raw = map[string]any{}
	}

	if strings.TrimSpace(patch.Description) != "" {
		raw["description"] = strings.TrimSpace(patch.Description)
	}
	if len(patch.Tags) > 0 {
		raw["tags"] = patch.Tags
	}
	if strings.TrimSpace(patch.Type) != "" {
		raw["type"] = strings.TrimSpace(patch.Type)
	}

	marshaled, err := yaml.Marshal(raw)
	if err != nil {
		return "", cogdocFrontmatter{}, fmt.Errorf("marshal frontmatter: %w", err)
	}

	updated := fmt.Sprintf("---\n%s---\n", marshaled)
	if body != "" {
		updated += "\n" + body
	}

	fm, _ := parseCogdocFrontmatter(updated)
	return updated, fm, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func marshalResult(data any) (*mcp.CallToolResult, any, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return textResult(fmt.Sprintf("marshal error: %v", err))
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(b)},
		},
	}, nil, nil
}

func textResult(text string) (*mcp.CallToolResult, any, error) {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}, nil, nil
}

// fallbackResult returns an error message with a CLI fallback command.
// This is the graceful degradation path — when the kernel is unavailable,
// the agent can fall back to shell commands that work without it.
func fallbackResult(errMsg, fallbackCmd string) (*mcp.CallToolResult, any, error) {
	text := fmt.Sprintf("%s\n\nFallback (kernel unavailable): %s", errMsg, fallbackCmd)
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
		IsError: true,
	}, nil, nil
}

// ── Audit-scope tool implementations ─────────────────────────────────────────

// toolReadFile implements cog_read_file. Reads an arbitrary file path that
// must be within cfg.WorkspaceRoot. Returns line-numbered content with
// optional offset and limit for large files.
//
// res=abstract (default, ADR pointer-first-mcp-responses): returns a pointer
// envelope with the workspace-relative path, file size, and a short excerpt
// without loading the full content into the agent context.
// res=full: returns the full line-numbered content (legacy behaviour).
//
// NOTE: a cog:file/<rel-path> projection that would make the pointer
// dereferenceable purely via URI is deferred (not in uri.go projections).
// Until that lands, res=abstract emits a workspace-relative path that can be
// re-fetched via cog_read_file with res=full.
//
// Example output (res=abstract):
//
//	{"path":"internal/engine/foo.go","file_size_bytes":4096,"excerpt":"...","res":"abstract"}
//
// Example output (res=full):
//
//	{"content":"   1\tpackage engine\n   2\t\n", "lines":2, "truncated":false}
func (m *MCPServer) toolReadFile(ctx context.Context, req *mcp.CallToolRequest, input readFileInput) (*mcp.CallToolResult, any, error) {
	if input.Path == "" {
		return textResult("path is required")
	}

	workspaceRoot := ""
	if m.cfg != nil {
		workspaceRoot = m.cfg.WorkspaceRoot
	}

	// Bare relative paths resolve against the workspace root, not the kernel
	// process CWD — "blob.json" means "<workspace>/blob.json" to a caller.
	inputPath := input.Path
	if !filepath.IsAbs(inputPath) && workspaceRoot != "" {
		inputPath = filepath.Join(workspaceRoot, inputPath)
	}

	// Workspace jail: reject paths that are not under the workspace root.
	// We resolve both to absolute paths so symlink traversal can't escape.
	abs, err := filepath.Abs(inputPath)
	if err != nil {
		return textResult(fmt.Sprintf("invalid path: %v", err))
	}
	if workspaceRoot != "" {
		rootAbs, err2 := filepath.Abs(workspaceRoot)
		if err2 != nil {
			return textResult(fmt.Sprintf("workspace root resolution failed: %v", err2))
		}
		// EvalSymlinks to catch symlink escapes.
		absReal, err3 := filepath.EvalSymlinks(abs)
		if err3 != nil {
			// If the file doesn't exist EvalSymlinks fails; use abs for the check.
			absReal = abs
		}
		rootReal, _ := filepath.EvalSymlinks(rootAbs)
		if rootReal == "" {
			rootReal = rootAbs
		}
		if !strings.HasPrefix(absReal, rootReal+string(filepath.Separator)) && absReal != rootReal {
			return textResult(fmt.Sprintf("path %q is outside workspace root %q", input.Path, workspaceRoot))
		}
	}

	// res=abstract (default, ADR pointer-first-mcp-responses): emit a pointer
	// envelope without dumping file content into the agent context. The pointer
	// is the workspace-relative path (re-fetchable via cog_read_file?res=full).
	// A cog:file/<rel-path> URI projection that would make this dereferenceable
	// purely by URI is deferred pending resolver work (uri.go projections).
	if input.Res == "" || input.Res == "abstract" {
		fi, statErr := os.Stat(abs)
		if statErr != nil {
			return textResult(fmt.Sprintf("stat failed: %v", statErr))
		}
		// Compute workspace-relative pointer path.
		ptrPath := abs
		if workspaceRoot != "" {
			if rel, relErr := filepath.Rel(workspaceRoot, abs); relErr == nil {
				ptrPath = rel
			}
		}
		// Read a short excerpt (first ~512 bytes) so the caller can decide
		// without fetching the full file.
		const excerptBytes = 512
		excerpt := ""
		if ef, efErr := os.Open(abs); efErr == nil {
			var eb [excerptBytes]byte
			n, _ := ef.Read(eb[:])
			ef.Close()
			if n > 0 {
				excerpt = string(eb[:n])
				if n == excerptBytes && fi.Size() > excerptBytes {
					excerpt += "…"
				}
			}
		}
		return marshalResult(map[string]any{
			"res":             "abstract",
			"path":            ptrPath,
			"file_size_bytes": fi.Size(),
			"excerpt":         excerpt,
			"note":            "pointer-first (ADR pointer-first-mcp-responses); pass res=full to retrieve line-numbered content",
		})
	}

	f, err := os.Open(abs)
	if err != nil {
		return textResult(fmt.Sprintf("open failed: %v", err))
	}
	defer f.Close()

	const defaultLimit = 500
	limit := input.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	offset := input.Offset
	if offset < 0 {
		offset = 0
	}

	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}

	// Chunked line reading via readLineCapped — unlike bufio.Scanner this can
	// NEVER fail with "token too long" on arbitrarily long lines (the
	// minified-one-line-blob case from the 2026-06-04 diagnosis). It also
	// never reads more than ~maxBytes+ε from disk: once the output budget is
	// exhausted mid-line, the rest of the file is left unread.
	reader := bufio.NewReaderSize(f, 64*1024)
	var buf bytes.Buffer
	lineNo := 0
	linesWritten := 0
	truncated := false
	overflowed := false // a single line exceeded the remaining byte budget

readLoop:
	for {
		// Skip offset lines — must be fully drained to find line boundaries,
		// but nothing is retained.
		if lineNo < offset {
			_, _, rerr := readLineCapped(reader, 0, true)
			if rerr == io.EOF {
				break readLoop
			}
			if rerr != nil {
				return textResult(fmt.Sprintf("read error: %v", rerr))
			}
			lineNo++
			continue
		}
		if linesWritten >= limit || buf.Len() >= maxBytes {
			// Bounds hit — peek one byte to detect remaining content.
			if _, rerr := reader.ReadByte(); rerr == nil {
				truncated = true
			}
			break readLoop
		}
		remaining := maxBytes - buf.Len()
		line, overflow, rerr := readLineCapped(reader, remaining, false)
		if rerr == io.EOF {
			break readLoop
		}
		if rerr != nil {
			return textResult(fmt.Sprintf("read error: %v", rerr))
		}
		lineNo++
		fmt.Fprintf(&buf, "%4d\t%s\n", lineNo, line)
		if overflow {
			// The line was cut at the byte budget and its tail was left
			// unread — stop here; line numbering beyond this point is unknown.
			truncated = true
			overflowed = true
			break readLoop
		}
		linesWritten++
	}

	// Use the true file size as the marker denominator: the reader above is
	// bounded, so len(buf) understates a large file by design.
	totalBytes := int64(buf.Len())
	var fileSize int64 = -1
	if fi, statErr := f.Stat(); statErr == nil {
		fileSize = fi.Size()
		if fi.Size() > totalBytes {
			totalBytes = fi.Size()
		}
	}
	content, byteTruncated := capToolOutputWithTotal(buf.String(), maxBytes, totalBytes)
	if byteTruncated {
		truncated = true
	}
	resp := map[string]any{
		"content":   content,
		"lines":     linesWritten,
		"truncated": truncated,
	}
	if fileSize >= 0 {
		resp["file_size_bytes"] = fileSize
	}
	if overflowed {
		resp["note"] = fmt.Sprintf(
			"line %d exceeded the %d-byte output cap and was cut mid-line; the rest of the file was not scanned, so line numbering beyond this point is unknown — use offset/limit to page",
			lineNo, maxBytes,
		)
	}
	return marshalResult(resp)
}

// toolGrepFiles implements cog_grep_files. Runs ripgrep (rg) when available,
// otherwise falls back to a pure-Go regexp walk. The search path must be
// within cfg.WorkspaceRoot.
//
// Example match entry: {"path":"engine/foo.go","line":42,"text":"func Foo() {"}
func (m *MCPServer) toolGrepFiles(ctx context.Context, req *mcp.CallToolRequest, input grepFilesInput) (*mcp.CallToolResult, any, error) {
	if input.Pattern == "" {
		return textResult("pattern is required")
	}

	workspaceRoot := ""
	if m.cfg != nil {
		workspaceRoot = m.cfg.WorkspaceRoot
	}

	// Resolve search path. Bare relative paths resolve against the workspace
	// root, not the kernel process CWD.
	searchPath := input.Path
	if searchPath == "" {
		searchPath = workspaceRoot
	}
	if searchPath == "" {
		searchPath = "."
	}
	if !filepath.IsAbs(searchPath) && workspaceRoot != "" {
		searchPath = filepath.Join(workspaceRoot, searchPath)
	}

	absSearch, err := filepath.Abs(searchPath)
	if err != nil {
		return textResult(fmt.Sprintf("invalid search path: %v", err))
	}

	// Workspace jail check.
	if workspaceRoot != "" {
		rootAbs, err2 := filepath.Abs(workspaceRoot)
		if err2 != nil {
			return textResult(fmt.Sprintf("workspace root resolution failed: %v", err2))
		}
		rootReal, _ := filepath.EvalSymlinks(rootAbs)
		if rootReal == "" {
			rootReal = rootAbs
		}
		absReal, err3 := filepath.EvalSymlinks(absSearch)
		if err3 != nil {
			absReal = absSearch
		}
		if !strings.HasPrefix(absReal, rootReal+string(filepath.Separator)) && absReal != rootReal {
			return textResult(fmt.Sprintf("search path %q is outside workspace root %q", searchPath, workspaceRoot))
		}
	}

	maxResults := input.MaxResults
	if maxResults <= 0 {
		maxResults = 50
	}

	type matchEntry struct {
		Path string `json:"path"`
		Line int    `json:"line"`
		Text string `json:"text"`
	}

	var matches []matchEntry
	truncated := false

	// Try ripgrep first (much faster on large trees).
	if rgPath, rgErr := exec.LookPath("rg"); rgErr == nil {
		// rg --no-heading --line-number --max-count N pattern path
		args := []string{
			"--no-heading",
			"--line-number",
			"--color=never",
			"--max-count", strconv.Itoa(maxResults + 1), // +1 to detect truncation
			input.Pattern,
			absSearch,
		}
		cmd := exec.CommandContext(ctx, rgPath, args...)
		out, _ := cmd.Output() // exit 1 means no match, not an error we care about
		// readLineCapped instead of bufio.Scanner: a single match on a
		// minified multi-megabyte line must not abort (or silently stop)
		// output parsing. Over-long lines are retained up to grepLineKeep
		// and drained; the response-level byte cap bounds the final JSON.
		reader := bufio.NewReaderSize(bytes.NewReader(out), 64*1024)
		for {
			lineBytes, _, rerr := readLineCapped(reader, grepLineKeep, true)
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return textResult(fmt.Sprintf("read rg output: %v", rerr))
			}
			line := string(lineBytes)
			// rg --no-heading format: path:linenum:text
			parts := strings.SplitN(line, ":", 3)
			if len(parts) < 3 {
				continue
			}
			lineNum, err2 := strconv.Atoi(parts[1])
			if err2 != nil {
				continue
			}
			if len(matches) >= maxResults {
				truncated = true
				break
			}
			relPath := parts[0]
			if workspaceRoot != "" {
				if rel, err3 := filepath.Rel(workspaceRoot, relPath); err3 == nil {
					relPath = rel
				}
			}
			matches = append(matches, matchEntry{
				Path: relPath,
				Line: lineNum,
				Text: parts[2],
			})
		}
	} else {
		// Pure-Go fallback: walk the tree and grep with regexp.
		slog.Debug("cog_grep_files: rg not found, using Go fallback", "path", absSearch)
		re, err2 := regexp.Compile(input.Pattern)
		if err2 != nil {
			return textResult(fmt.Sprintf("invalid pattern: %v", err2))
		}
		err = filepath.Walk(absSearch, func(p string, info os.FileInfo, walkErr error) error {
			if walkErr != nil || info.IsDir() {
				return nil
			}
			if len(matches) >= maxResults {
				truncated = true
				return io.EOF // stop walk
			}
			f, ferr := os.Open(p)
			if ferr != nil {
				return nil
			}
			defer f.Close()
			// readLineCapped instead of bufio.Scanner: a >64 KiB line used
			// to silently stop the scan of the rest of the file. Over-long
			// lines are matched against their first grepLineKeep bytes and
			// fully drained so subsequent lines are still scanned.
			reader := bufio.NewReaderSize(f, 64*1024)
			lineNo := 0
			for {
				lineBytes, _, rerr := readLineCapped(reader, grepLineKeep, true)
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					return nil // unreadable file — skip, like open errors
				}
				lineNo++
				if re.Match(lineBytes) {
					if len(matches) >= maxResults {
						truncated = true
						return io.EOF
					}
					relPath := p
					if workspaceRoot != "" {
						if rel, err3 := filepath.Rel(workspaceRoot, p); err3 == nil {
							relPath = rel
						}
					}
					matches = append(matches, matchEntry{
						Path: relPath,
						Line: lineNo,
						Text: string(lineBytes),
					})
				}
			}
			return nil
		})
		if err != nil && err != io.EOF {
			return textResult(fmt.Sprintf("walk error: %v", err))
		}
	}

	maxBytes := DefaultMaxToolOutputBytes
	if m.cfg != nil {
		maxBytes = m.cfg.EffectiveMaxToolOutputBytes()
	}

	// res=abstract (default, ADR pointer-first-mcp-responses): return a summary
	// envelope — match count and the set of matched file paths — without dumping
	// per-match line text into the agent context. The file paths are
	// workspace-relative and re-fetchable via cog_read_file or cog_grep_files
	// with res=full. Pass res=full to obtain the per-match path/line/text list.
	if input.Res == "" || input.Res == "abstract" {
		seen := make(map[string]struct{}, len(matches))
		var files []string
		for _, m := range matches {
			if _, ok := seen[m.Path]; !ok {
				seen[m.Path] = struct{}{}
				files = append(files, m.Path)
			}
		}
		return marshalResult(map[string]any{
			"res":           "abstract",
			"match_count":   len(matches),
			"matched_files": files,
			"truncated":     truncated,
			"note":          "pointer-first (ADR pointer-first-mcp-responses); pass res=full to retrieve per-match path/line/text",
		})
	}

	return capMarshalResult(map[string]any{
		"matches":   matches,
		"truncated": truncated,
	}, maxBytes)
}

// extractSection pulls a section from markdown by heading anchor.
func extractSection(content, anchor string) string {
	lines := strings.Split(content, "\n")
	var capturing bool
	var level int
	var result []string

	for _, line := range lines {
		if strings.Contains(line, "{#"+anchor+"}") || strings.Contains(line, "# "+anchor) {
			capturing = true
			level = strings.Count(strings.TrimLeft(line, " "), "#")
			result = append(result, line)
			continue
		}
		if capturing {
			// Stop at same or higher level heading
			trimmed := strings.TrimLeft(line, " ")
			if strings.HasPrefix(trimmed, "#") {
				headingLevel := strings.Count(trimmed, "#")
				if headingLevel <= level {
					break
				}
			}
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

// init logging for MCP operations
func init() {
	_ = slog.Default() // ensure slog is initialized
}
