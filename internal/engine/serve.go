// serve.go — CogOS v3 HTTP API
//
// Core endpoints:
//
//	GET  /health                           — liveness + readiness probe
//	GET  /v1/context                       — current attentional field (debug)
//	GET  /v1/resolve                       — resolve a cog: URI to a filesystem path
//	POST /v1/chat/completions              — OpenAI-compatible chat (streaming + non-streaming)
//	POST /v1/messages                      — Anthropic Messages-compatible chat
//	POST /v1/context/foveated              — foveated context assembly for Claude Code hook
//	GET  /v1/hud/state                     — compact kernel state snapshot for Claude Code HUD hook
//	GET  /v1/proprioceptive                — last 50 proprioceptive log entries + light cone status
//	GET  /v1/ledger                        — query the hash-chained event ledger
//	GET  /v1/lightcone                     — light cone metadata (placeholder)
//	GET  /v1/kernel-log                    — tail kernel slog (diagnostic text) JSONL sink; filter by level/substring/time
//
// Constellation / attention endpoints (Phase 3, see serve_attention.go):
//
//	POST /v1/attention                     — emit attention signal
//	GET  /v1/constellation/fovea           — current fovea state
//	GET  /v1/constellation/adjacent?uri=… — adjacent nodes by attentional proximity
//
// Channel-session forwarder (ADR-082 Wave 2, see serve_sessions_channel.go):
//
//	POST /v1/channel-sessions/register             — kernel mints session_id
//	                                                  and forwards to mod3;
//	                                                  returns merged response
//	POST /v1/channel-sessions/{id}/deregister      — proxy to mod3, drop record
//	GET  /v1/channel-sessions                      — kernel view + mod3 list
//	GET  /v1/channel-sessions/{id}                 — single-session detail
//
// The chat endpoint routes through the inference Router when one is set,
// otherwise returns 501.
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/myrgic/cogos/ui/canvas"
	"github.com/myrgic/cogos/ui/dashboard"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Server wraps the HTTP server and its dependencies.
type Server struct {
	cfg               *Config
	nucleus           *Nucleus
	process           *Process
	router            Router            // nil until SetRouter is called
	serviceSupervisor ServiceSupervisor // nil until SetServiceSupervisor; defaults to ObserverSupervisor
	srv               *http.Server
	debug             debugStore      // captures last request pipeline state
	attentionLog      *attentionLog   // per-server log (avoids global write race)
	agentController   AgentController // nil until SetAgentController is called
	mcpServer         *MCPServer      // so SetAgentController can propagate to tools

	// reconcileDaemon backs GET /v1/reconcile/coherence (First Instruments
	// Module B, M1-B). nil until SetReconcileDaemon is called — the daemon is
	// constructed after NewServer in engine.Boot, so this is wired
	// post-construction rather than passed to NewServer.
	reconcileDaemon *ReconcileDaemon

	// Track 5 Phase 3 surface — per-bus event store, SSE broker, and
	// consumer cursor registry. Scoped to the server so tests can create
	// isolated instances.
	busSessions  *BusSessionManager
	busBroker    *BusEventBroker
	busConsumers *ConsumerRegistry
	sessions     *SessionContextStore

	// Kernel-native session-management registries (hybrid design —
	// cog://mem/semantic/surveys/2026-04-21-consolidation/
	// agent-P-session-management-evaluation). The bus is ground truth;
	// these are derived views rebuilt from bus replay at startup.
	sessionRegistry *SessionRegistry
	handoffRegistry *HandoffRegistry
	// forkRegistry is the derived in-memory view of fork relationships
	// (RFC-0005). Warm cache rebuilt from bus replay at startup.
	forkRegistry *ForkRegistry

	// ADR-082 Wave 2: kernel-owned identity registry for channel-participant
	// sessions. The kernel mints session_id, mod3 stores per-channel state
	// keyed on the kernel-issued ID. Distinct from sessionRegistry above,
	// which enforces strict 3-component hyphen IDs for the agent/handoff
	// protocol. See serve_sessions_channel.go for the full rationale.
	channelSessionRegistry *ChannelSessionRegistry

	// identityGrants is the in-memory, ledger-backed store for board-task-60
	// kernel-issued identity grants (see serve_identity_grants.go). Chunk 1
	// proved the mechanism in-memory only; chunk 2 made it ledger-backed
	// (rebuilt from .cog/ledger/identity-grants/ on boot) and added revoke.
	identityGrants *IdentityGrantRegistry

	// grantMintLimiter bounds POST /v1/identity/grants throughput on the
	// bootstrap-exempt path of the write-route grant-auth gate (see
	// serve_grant_auth.go). Always non-nil after NewServer.
	grantMintLimiter *grantMintLimiter

	// mod3Client is the HTTP client used to forward channel-session calls
	// to mod3. Nil in production (falls back to the package-level
	// mod3HTTPClient); tests set this to an httptest-backed client.
	mod3Client *http.Client

	// httpRoutes is the manifest-introspection registry. Every route added
	// via s.route / s.routeH appends here; /v1/manifest serialises this
	// slice. Populated at startup only — reads are lock-free because the
	// slice is frozen by the time Start returns.
	httpRoutes []routeMeta

	// spanEmitter is wired to busSessions at startup so that withSpan
	// closures can emit KernelHandlerSpan events to bus_traces. Nil-safe:
	// withSpan is a no-op wrapper when spanEmitter is nil.
	spanEmitter spanEmitter

	// harnessBackend is the RBAC layer for HarnessBindingCRD create/resolve.
	// Wired from the root package via SetHarnessBackend after construction.
	// Threaded into the MCP server in registerMCPRoutes. Nil-safe throughout.
	harnessBackend HarnessAttacher

	// bepEngine is the running BEP cluster engine, or nil when
	// cluster.enabled=false (dark by default). Wired from Boot() after the
	// engine starts successfully; nil in all other cases. The
	// /v1/cluster/status handler reads this field — nil → {"enabled":false}.
	bepEngine *BEPEngine
}

// NewServer constructs a Server bound to the configured port.
func NewServer(cfg *Config, nucleus *Nucleus, process *Process) *Server {
	s := &Server{cfg: cfg, nucleus: nucleus, process: process}

	// Phase 3 bus/session surface. Managers are always instantiated so
	// handlers don't need nil-safety for the common case; tests can
	// override via the exported fields if they want an isolated fixture.
	s.busSessions = NewBusSessionManager(cfg.WorkspaceRoot)
	s.spanEmitter = &serverSpanEmitter{bus: s.busSessions}

	// Piece 1: install the dashboard chat inlet handler on the engine's bus
	// manager so events on bus_dashboard_chat enqueue to enginePendingMsgs.
	// Called unconditionally — the handler is a no-op until the first event
	// arrives, and the bus dirs are created idempotently.
	InstallEngineDashboardInlet(s.busSessions)
	s.busBroker = NewBusEventBroker()

	// Wire BusSessionManager → BusEventBroker so that any direct AppendEvent
	// call (e.g. enginePublishDashboardResponse from the respond tool) notifies
	// live SSE subscribers on /v1/bus/{id}/stream.  Without this hook, events
	// written via AppendEvent bypass the broker and never reach mod3's response
	// bridge subscriber.  handleBusSend already calls busBroker.Publish after
	// the AppendEvent write, so the handler below is intentionally idempotent
	// (the broker uses non-blocking channel sends and tolerates double-notify).
	// Capture the broker pointer so the closure doesn't race on s.busBroker.
	{
		broker := s.busBroker
		s.busSessions.AddEventHandler("sse-broker-notify", func(busID string, block *BusBlock) {
			broker.Publish(busID, block)
		})
	}

	s.busConsumers = NewConsumerRegistry(
		// Match root's persistence path: .cog/run/bus/{bus_id}.cursors.jsonl
		filepath.Join(cfg.WorkspaceRoot, ".cog", "run", "bus"),
	)
	s.sessions = NewSessionContextStore()

	// Kernel-native session + handoff registries. Replay from bus is done
	// below, after the bus manager + its handlers are wired up, so that
	// the warm cache is ready before any HTTP request lands.
	s.sessionRegistry = NewSessionRegistry()
	s.handoffRegistry = NewHandoffRegistry()
	s.forkRegistry = NewForkRegistry()

	// ADR-082 Wave 2 kernel-owned channel-session identity.
	s.channelSessionRegistry = NewChannelSessionRegistry()

	// Board task 60 chunk 2: kernel-issued identity grants, rebuilt from the
	// ledger on boot so a previously-issued grant still verifies after a
	// kernel restart (design §3.2; the chunk-1-to-2 verify tooth). Falls
	// back to an empty, still-ledger-backed registry on a read error (e.g.
	// a corrupt ledger file) rather than failing to boot — losing the warm
	// cache is recoverable (surfaces re-mint), refusing to start is not.
	identityGrants, err := RebuildIdentityGrantRegistryFromLedger(cfg.WorkspaceRoot)
	if err != nil {
		slog.Warn("serve: identity grant ledger rebuild failed; starting with an empty grant store", "err", err)
		identityGrants = NewIdentityGrantRegistryWithLedger(cfg.WorkspaceRoot)
	}
	s.identityGrants = identityGrants
	s.grantMintLimiter = newGrantMintLimiter(defaultGrantMintRateLimit, defaultGrantMintRateWindow)

	mux := http.NewServeMux()
	s.routeH(mux, "GET /", dashboard.Handler())
	s.routeH(mux, "GET /canvas", canvas.Handler())
	// Workspace-hosted UI artifacts (see serve_ui_artifacts.go). Unlike the
	// two embedded UIs above, these live in $WORKSPACE/ui and are
	// versioned with the workspace rather than the binary.
	s.route(mux, "GET /ui/", s.handleUIArtifacts)
	s.route(mux, "GET /v1/ui/artifacts", s.handleUIArtifactIndex)
	s.route(mux, "GET /health", s.handleHealth)
	s.route(mux, "GET /v1/context", s.handleContext)
	s.route(mux, "GET /v1/resolve", s.handleResolve)
	s.route(mux, "GET /v1/uri/resolve", s.handleURIResolve)
	s.route(mux, "GET /v1/cogdoc/read", s.handleCogDocRead)
	s.route(mux, "GET /v1/debug/last", s.handleDebugLast)
	s.route(mux, "GET /v1/debug/context", s.handleDebugContext)
	s.route(mux, "GET /v1/settings/context", s.handleGetContextSettings)
	s.route(mux, "PATCH /v1/settings/context", s.handlePatchContextSettings)
	s.route(mux, "POST /v1/chat/completions", s.handleChat)
	s.route(mux, "POST /v1/messages", s.handleAnthropicMessages)
	s.route(mux, "GET /v1/hud/state", s.handleHUDState)
	s.route(mux, "GET /v1/proprioceptive", s.handleProprioceptive)
	s.route(mux, "GET /v1/ledger", s.handleLedger)
	s.route(mux, "GET /v1/traces", s.handleTraces)
	s.route(mux, "GET /v1/lightcone", s.handleLightCone)
	s.route(mux, "POST /v1/context/foveated", s.handleFoveatedContext)
	s.route(mux, "GET /v1/kernel-log", s.handleKernelLog)
	s.route(mux, "GET /v1/tool-calls", s.handleToolCalls)
	s.route(mux, "GET /v1/conversation", s.handleConversation)
	s.route(mux, "GET /v1/manifest", s.handleManifest)
	s.route(mux, "GET /v1/reconcile/coherence", s.handleReconcileCoherence)
	s.route(mux, "GET /v1/reconcile/convergence", s.handleReconcileConvergence)
	s.route(mux, "POST /v1/reconcile/{type}/resume", s.handleReconcileResume)
	s.route(mux, "GET /v1/kernel/rates", s.handleKernelRates)

	// RFC-040 S1: Prometheus text-exposition interop door over the current
	// kernel health snapshot (provider counts + S0 host gauges). Current
	// values only — no history, no aggregation. See serve_metrics.go.
	s.route(mux, "GET /metrics", s.handleMetrics)

	// RFC-040 S2: retained vitals history. The ONE query helper
	// (window(metric, since, resolution) — N2) over /v1/vitals. See
	// serve_vitals.go.
	s.route(mux, "GET /v1/vitals", s.handleVitals)
	s.registerAgentRoutes(mux)
	s.registerSkillRoutes(mux)

	// Constellation / attention endpoints (Phase 3)
	s.registerAttentionRoutes(mux)

	// Block sync endpoints (Phase 3 block sync protocol)
	s.registerBlockRoutes(mux)
	s.registerCompatRoutes(mux)
	s.registerEventBusRoutes(mux)
	s.registerMCPRoutes(mux)
	s.registerConfigRoutes(mux)

	// Track 5 Phase 3: /v1/bus/* and /v1/sessions routes.
	s.registerBusRoutes(mux)

	// Kernel-native session & handoff management routes — the hybrid
	// design's invariance layer. Registered AFTER registerBusRoutes so
	// the specific patterns (POST /v1/sessions/register, etc.) coexist
	// cleanly with the pre-existing GET /v1/sessions[/{id}] surface.
	s.registerSessionMgmtRoutes(mux)

	// ADR-082 Wave 2: kernel-side channel-session forwarder. The four
	// /v1/channel-sessions/* routes mint session_ids, record identity
	// locally, and forward to mod3 at cfg.Mod3URL. Namespaced under
	// /v1/channel-sessions/* to coexist with the agent-session surface
	// above (incompatible session_id formats — see serve_sessions_channel.go).
	s.registerChannelSessionRoutes(mux)

	// Board task 60 chunk 1/2: kernel-issued identity grants — the
	// constellation-chat surface's kernel-verified credential (design doc
	// cog://mem/working/2026-07-21-kernel-identity-seat-design). Ledger-
	// backed + revocable as of chunk 2; see serve_identity_grants.go.
	s.registerIdentityGrantRoutes(mux)

	// Phase 1B: peer-awareness packet endpoint (READ side of the 4E
	// ambient-awareness loop; Phase 1A populates channel.<sid>.activity).
	s.registerPeerAwarenessRoutes(mux)

	// Services API: GET /v1/services and GET /v1/services/{name} (Phase 1);
	// POST /v1/services/{name}/{action} mutations (Phase 2).
	s.registerServiceRoutes(mux)
	s.registerServiceMutationRoutes(mux)

	// ACP-client surface: list/browse Claude Code projects+sessions, spawn subprocess.
	s.registerClaudeCodeRoutes(mux)

	// Diagnostic surface: pprof + expvar under /debug/, loopback-gated
	// regardless of bind address. See serve_debug.go / #505 — the daemon had
	// no way to be asked about its own memory when it developed a 36GB leak.
	s.registerDebugRoutes(mux)

	// Extension HTTP routes (e.g. observatory coverage).
	if RegisterHTTPExtensions != nil {
		RegisterHTTPExtensions(s, mux)
	}

	// Cluster / BEP transport status (Phase 2 S2). Dark by default: when
	// cluster.enabled=false the handler returns {"enabled":false} and no
	// engine state is inspected. The route is always registered so the
	// endpoint exists regardless of cluster configuration — callers can
	// probe it without needing to know whether the cluster was compiled in.
	s.route(mux, "GET /v1/cluster/status", s.handleClusterStatus)

	// Replay bus_sessions + bus_handoffs into the in-memory registries so
	// the kernel starts with an accurate derived view. Bus is authoritative
	// either way; this just warms the read path.
	//
	// ReplaySessionRegistry's session.fork case reconstructs fork-child rows
	// (cog_fork_session / POST /v1/sessions/{id}/fork register the child with
	// appendFn=nil — the fork bus event is the child's only durable record).
	// ReplayForkRegistry separately rebuilds the parent→children lineage
	// index consumed by ForkChildren/ForkAncestors, which was otherwise
	// reinitialized empty on every restart (s.forkRegistry = NewForkRegistry()
	// above) with no path back from the bus.
	_ = ReplaySessionRegistry(s.busSessions, s.sessionRegistry)
	_ = ReplayHandoffRegistry(s.busSessions, s.handoffRegistry)
	_ = ReplayForkRegistry(s.busSessions, s.forkRegistry)

	// Resolve the bind address. Default stays 127.0.0.1 (loopback-only);
	// callers may override via Config.BindAddr to listen on all interfaces
	// ("0.0.0.0") for pod/LAN/Tailnet deployments.
	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	// Wrap the mux with the write-route grant-auth gate (serve_grant_auth.go
	// — closes the CSRF gap serve_cors.go's file header documents at
	// length), then with CORS middleware so browser origins (e.g. the mod3
	// dashboard at http://localhost:7860) can still POST to /v1/* without
	// the preflight failing. Order matters: CORS stays outermost so it keeps
	// owning the OPTIONS preflight short-circuit (a custom X-Cogos-Grant
	// header forces that preflight in the first place — see
	// serve_grant_auth.go's CSRF threat-model note for why that's the actual
	// fix) and grant-auth only ever sees requests CORS has already decided
	// to forward past OPTIONS handling.
	handler := corsMiddleware(s.grantAuthMiddleware(mux))

	s.srv = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", bindAddr, cfg.Port),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 300 * time.Second, // 5 min — streaming responses can be long
		IdleTimeout:  120 * time.Second,
	}
	return s
}

// SetRouter wires an inference Router into the server.
func (s *Server) SetRouter(r Router) {
	s.router = r
}

// SetServiceSupervisor wires a ServiceSupervisor into the server for
// service mutation endpoints (start/stop/restart/enable/disable).
// If not called, all mutation endpoints use ObserverSupervisor (read-only).
func (s *Server) SetServiceSupervisor(sup ServiceSupervisor) {
	s.serviceSupervisor = sup
}

// SetAgentController wires a live AgentController into the server so the
// cog_list_agents / cog_get_agent_state / cog_trigger_agent_loop MCP
// tools have a backing implementation. Callers outside engine (like the
// root-package serveServer) can build the controller and pass it here.
// Safe to call post-construction: the MCP tool registry resolves the
// controller at call time.
//
// When ctrl is a *LocalHarnessController, also wires the dashboard chat bridge
// (Piece 2+3) by calling SetDashboardBus with the server's bus session manager.
func (s *Server) SetAgentController(ctrl AgentController) {
	s.agentController = ctrl
	if s.mcpServer != nil {
		s.mcpServer.SetAgentController(ctrl)
	}
	// Phase 2 S4: propagate the dispatcher to the BEP engine if it is already
	// running (e.g. when SetAgentController is called after Boot). The engine
	// needs a live AgentDispatcher so it can serve incoming MessageTypeDispatch
	// from remote peers. No-op when cluster is dark (bepEngine == nil).
	if s.bepEngine != nil {
		if d, ok := ctrl.(AgentDispatcher); ok {
			s.bepEngine.SetDispatcher(d)
		}
	}
	// Piece 2+3: wire dashboard bus into the harness so runCycle drains
	// pending messages and the respond tool has a publish target. Also bind
	// the controller to the inlet so incoming messages trigger an immediate
	// cycle rather than waiting for the next autonomic tick.
	if lhc, ok := ctrl.(*LocalHarnessController); ok && s.busSessions != nil {
		lhc.SetDashboardBus(s.busSessions)
		BindDashboardController(lhc)
	}
}

// Process returns the server's kernel Process handle. Exported so
// WireProviderRuntime hooks (set from internal/providers/all, which cannot
// reach unexported Server fields) can wire provider-side event emission
// (e.g. marginbridge.Provider.SetEventSink) without internal/engine importing
// the provider package directly, per ADR-085 leaf-package discipline.
func (s *Server) Process() *Process { return s.process }

// BusSessions returns the server's bus/session manager. See Process() for
// why this accessor exists.
func (s *Server) BusSessions() *BusSessionManager { return s.busSessions }

// BusBroker returns the server's SSE bus broker. See Process() for why this
// accessor exists.
func (s *Server) BusBroker() *BusEventBroker { return s.busBroker }

// SetReconcileDaemon wires the kernel's ReconcileDaemon into the server so
// GET /v1/reconcile/coherence (First Instruments Module B, M1-B) can read
// LastCoherence(). Called from Boot() after the daemon is constructed. Nil
// is safe (the handler reports coherent-by-default with an empty detail
// slice, matching ReconcileDaemon.LastCoherence's own zero-observations
// case) so this wiring is optional for callers that never registered a
// daemon (e.g. hand-built *Server instances in unrelated tests).
func (s *Server) SetReconcileDaemon(d *ReconcileDaemon) {
	s.reconcileDaemon = d
}

// SetHarnessBackend wires the RBAC harness-binding layer into the server.
// When set before Start, registerMCPRoutes threads it into the MCP server so
// cog_register_session can create HarnessBindingCRDs for sessions that supply
// an optional "subject" field. Nil clears any prior backend. Safe to call
// post-construction (before registerMCPRoutes runs during Start).
func (s *Server) SetHarnessBackend(h HarnessAttacher) {
	s.harnessBackend = h
	if s.mcpServer != nil {
		s.mcpServer.SetHarnessBackend(h)
	}
}

// SetClusterRouter wires the Phase 2 S4 BEP dispatch router into both the
// Server and its MCP server so cog_dispatch_to_harness with target_node
// forwards dispatches to remote peers over the authenticated BEP channel.
// Called from Boot() after the BEPEngine starts successfully. Nil-safe:
// when not called, target_node requests return a clear "cluster_disabled" error.
func (s *Server) SetClusterRouter(r RemoteDispatchRouter) {
	if s.mcpServer != nil {
		s.mcpServer.SetClusterRouter(r)
	}
}

// SetConstellationIndexer wires a live ConstellationIndexer into the server
// so that CogDocService.WriteAndSync / PatchAndSync perform an eager per-file
// FTS upsert, and so that the lazy drift-repair path in
// searchMemoryFTSDriftRepair can call IndexFile without importing
// sdk/constellation (package-boundary guard #2 in cogdoc_service.go:22).
// Called from Boot() via WireConstellationIndexer after NewServer.
// Nil-safe: disables eager upsert and drift repair in degraded/test mode.
func (s *Server) SetConstellationIndexer(c ConstellationIndexer) {
	if s.mcpServer != nil {
		s.mcpServer.SetConstellationIndexer(c)
	}
}

// WorkspaceRoot returns the workspace root path for this server instance.
// Used by external wiring hooks (e.g. WireConstellationIndexer in
// cmd/cogos/providers_wire.go) that need the path without importing internal
// engine state.
func (s *Server) WorkspaceRoot() string {
	return s.cfg.WorkspaceRoot
}

// Start begins serving. It blocks until the server stops.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	slog.Info("server: listening", "addr", s.srv.Addr, "bind", s.cfg.BindAddr)
	return s.srv.Serve(ln)
}

// Shutdown gracefully drains the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Handler returns the HTTP handler, useful for httptest.NewServer in tests.
func (s *Server) Handler() http.Handler {
	return s.srv.Handler
}

// nucleusCard returns the current nucleus identity card, or empty if unset.
// Used by context-assembly fallback paths to retain nucleus identity when the
// full AssembleContext pipeline isn't available.
func (s *Server) nucleusCard() string {
	if s.nucleus == nil {
		return ""
	}
	s.nucleus.mu.RLock()
	defer s.nucleus.mu.RUnlock()
	return s.nucleus.Card
}

// mergeSystemPrompts combines the nucleus identity card with any
// client-supplied system-prompt parts in a stable order
// (nucleus → client[0] → client[1] → ...), separated by "---" dividers.
// Empty parts are skipped. If every input is empty, returns "".
//
// This is used on the fallback path when ContextPackage.FormatForProvider()
// isn't available (e.g. AssembleContext failed or was skipped) so we still
// honour any client-provided system prompt — the thing BrowserOS relies on
// when it injects its browser_* tool definitions and companion system text.
func mergeSystemPrompts(nucleus string, clientParts []string) string {
	var parts []string
	if strings.TrimSpace(nucleus) != "" {
		parts = append(parts, strings.TrimRight(nucleus, "\n"))
	}
	for _, p := range clientParts {
		if strings.TrimSpace(p) != "" {
			parts = append(parts, strings.TrimRight(p, "\n"))
		}
	}
	return strings.Join(parts, "\n\n---\n\n")
}

// BoundIdentity is the result of per-request identity resolution at the
// inference gateway (G1). It is intentionally minimal — just enough to
// drive ledger attribution and embodiment gating.
//
// Subject is the resolved principal name (e.g. "cog", "alice").
// Bound is false when no client session id was present or no harness
// binding was found for it; in that case Subject is empty.
//
// WorkspaceRoot is the cog:// URI home for this identity's expression, resolved
// at binding time from the Identity CRD (G3 Part A). Empty when no CRD exists
// or the expression carries no workspace_root field.
//
// MemoryNamespace is the memory scope for this identity's expression, resolved
// at binding time from the Identity CRD (G3 Part B). Empty when no CRD exists
// or the expression carries no memory_namespace field.
type BoundIdentity struct {
	Subject         string
	Bound           bool
	WorkspaceRoot   string // cog:// URI, e.g. "cog://workspaces/cog"
	MemoryNamespace string // cog:// URI, e.g. "cog://mem/semantic/agents/sandy/"
}

// resolveBoundIdentity extracts the inbound client session id from the
// request (X-Cogos-Session-Id header, then req.User field as fallback),
// then resolves a HarnessBindingCRD via harnessBackend.
//
// Returns an unbound BoundIdentity when:
//   - No client session id is present in the request.
//   - harnessBackend is nil (backend not wired).
//   - No binding exists for the client session id.
//
// When a binding is found, it loads the Identity CRD for the subject (G3)
// and populates WorkspaceRoot and MemoryNamespace from the CRD's expression
// for the "kernel" audience (with "*" wildcard fallback). A missing CRD or
// missing expression is treated as minimal binding — the fields are left
// empty and no error is returned.
//
// This helper is reusable across attribution sites; this increment wires
// it into handleChat only. Other sites (serve_anthropic.go, process.go,
// tool_observer.go, mcp_server.go) are deferred to a later increment.
func (s *Server) resolveBoundIdentity(r *http.Request, reqUser string) BoundIdentity {
	clientSessionID := r.Header.Get("X-Cogos-Session-Id")
	if clientSessionID == "" {
		clientSessionID = reqUser
	}
	if clientSessionID == "" {
		return BoundIdentity{}
	}
	if s.harnessBackend == nil {
		return BoundIdentity{}
	}
	binding, ok := s.harnessBackend.ResolveHarnessBinding(clientSessionID, "agent")
	if !ok || binding == nil {
		return BoundIdentity{}
	}
	bi := BoundIdentity{Subject: binding.Spec.Subject, Bound: true}

	// G3 Step 0: load the Identity CRD and populate WorkspaceRoot +
	// MemoryNamespace from the "kernel" expression (with "*" wildcard fallback).
	// Best-effort: any error (missing file, parse failure) leaves fields empty.
	if binding.Spec.Subject != "" && s.cfg != nil {
		if expr := resolveIdentityExpression(s.cfg.WorkspaceRoot, binding.Spec.Subject, "kernel"); expr != nil {
			bi.WorkspaceRoot = expr.WorkspaceRoot
			bi.MemoryNamespace = expr.MemoryNamespace
		}
	}
	return bi
}

// handleHealth is the liveness/readiness probe.
//
//	200 → healthy
//	503 → nucleus not loaded or process not running
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	code := http.StatusOK

	if s.nucleus == nil {
		status = "nucleus_missing"
		code = http.StatusServiceUnavailable
	}

	identity := ""
	if s.nucleus != nil {
		identity = s.nucleus.Name
	}
	trust := s.process.TrustSnapshot()

	resp := map[string]interface{}{
		"status":   status,
		"version":  Version,
		"state":    s.process.State().String(),
		"identity": identity,
		"node_id":  s.process.NodeID,
		"trust": map[string]interface{}{
			"score":       trust.LocalScore,
			"scope":       "local",
			"fingerprint": s.process.Fingerprint(),
		},
		"workspace": s.cfg.WorkspaceRoot,
		// Compile-time feature report. build_tags.fts5 is the RUNTIME probe
		// (CREATE VIRTUAL TABLE ... USING fts5 on :memory:), not the -ldflags
		// claim, so a binary built without the module cannot report healthy
		// FTS5 (ledger L01 / C01). See BuildTagsReport.
		"build_tags": BuildTagsReport(),
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}

	if nh := s.process.NodeHealth(); nh != nil {
		if summary := nh.Summary(); len(summary) > 0 {
			resp["node"] = summary
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleContext returns the current attentional field (top-20 fovea).
func (s *Server) handleContext(w http.ResponseWriter, r *http.Request) {
	fovea := s.process.Field().Fovea(20)

	type entry struct {
		Path  string  `json:"path"`
		Score float64 `json:"score"`
	}
	entries := make([]entry, len(fovea))
	for i, fs := range fovea {
		entries[i] = entry(fs)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"nucleus":      s.nucleus.Name,
		"state":        s.process.State().String(),
		"field_size":   s.process.Field().Len(),
		"last_updated": s.process.Field().LastUpdated().Format(time.RFC3339),
		"fovea":        entries,
	})
}

// handleResolve resolves a cog: URI to a filesystem path.
//
// GET /v1/resolve?uri=cog:mem/semantic/foo.cog.md
//
//	200 → { uri, path, fragment, exists }
//	400 → { error }
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "uri parameter required"})
		return
	}

	res, err := ResolveURI(s.cfg.WorkspaceRoot, uri)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	_, statErr := os.Stat(res.Path)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"uri":      uri,
		"path":     res.Path,
		"fragment": res.Fragment,
		"exists":   statErr == nil,
	})
}

// handleCogDocRead resolves a cog: URI and returns the file content as text.
//
//	GET /v1/cogdoc/read?uri=cog:mem/semantic/insights/foo.md
//	200 → { uri, path, content, exists }
func (s *Server) handleCogDocRead(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "uri parameter required"})
		return
	}

	res, err := ResolveURI(s.cfg.WorkspaceRoot, uri)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	content, readErr := os.ReadFile(res.Path)
	exists := readErr == nil

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]interface{}{
		"uri":    uri,
		"path":   res.Path,
		"exists": exists,
	}
	if exists {
		resp["content"] = string(content)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// ── OpenAI-compatible wire types ─────────────────────────────────────────────

type oaiChatRequest struct {
	Model               string              `json:"model"`
	Messages            []oaiMessage        `json:"messages"`
	Stream              bool                `json:"stream"`
	Temperature         *float64            `json:"temperature,omitempty"`
	MaxTokens           int                 `json:"max_tokens,omitempty"`
	MaxCompletionTokens int                 `json:"max_completion_tokens,omitempty"`
	TopP                *float64            `json:"top_p,omitempty"`
	Stop                []string            `json:"stop,omitempty"`
	Tools               []oaiToolDefinition `json:"tools,omitempty"`
	ToolChoice          json.RawMessage     `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool               `json:"parallel_tool_calls,omitempty"`
	FrequencyPenalty    *float64            `json:"frequency_penalty,omitempty"`
	PresencePenalty     *float64            `json:"presence_penalty,omitempty"`
	Seed                *int                `json:"seed,omitempty"`
	User                string              `json:"user,omitempty"`
	N                   *int                `json:"n,omitempty"`
	StreamOptions       *oaiStreamOpts      `json:"stream_options,omitempty"`
}

// oaiStreamOpts carries OpenAI stream_options (e.g. include_usage).
type oaiStreamOpts struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

// oaiToolDefinition is the OpenAI-format tool envelope: {"type":"function","function":{...}}.
type oaiToolDefinition struct {
	Type     string          `json:"type"`
	Function oaiToolFunction `json:"function"`
}

// oaiToolFunction carries the tool name, description, and JSON Schema parameters.
type oaiToolFunction struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
}

// oaiToolCall is the OpenAI-format tool call in a response message.
type oaiToolCall struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Function oaiToolCallFunc `json:"function"`
}

// oaiToolCallFunc carries the function name and stringified arguments.
type oaiToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// oaiStreamToolCall is a tool call delta in a streaming response.
type oaiStreamToolCall struct {
	Index    int              `json:"index"`
	ID       string           `json:"id,omitempty"`
	Type     string           `json:"type,omitempty"`
	Function *oaiToolCallFunc `json:"function,omitempty"`
}

type oaiMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Name       string          `json:"name,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
}

// extractContent normalises the OpenAI "content" field which may arrive as
// either a plain JSON string or an array of content-parts (the multi-part
// format used by Discord gateway and other clients):
//
//	"hello"                                   → "hello"
//	[{"type":"text","text":"hello"}]           → "hello"
func extractContent(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	// Fast path: plain string.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}

	// Slow path: array of content parts.
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		// Unrecognised shape — return the raw bytes as-is so nothing is lost.
		return string(raw)
	}

	var out string
	for _, p := range parts {
		if p.Type == "text" {
			out += p.Text
		}
	}
	return out
}

// oaiContentPart represents a single element in the OpenAI multi-part content array.
type oaiContentPart struct {
	Type     string       `json:"type"`
	Text     string       `json:"text,omitempty"`
	ImageURL *oaiImageURL `json:"image_url,omitempty"`
}

// oaiImageURL carries the URL (typically a data: base64 URI) for an image content part.
type oaiImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// extractContentParts normalises the OpenAI "content" field into structured
// parts, preserving both text and image_url entries. This is used instead of
// extractContent when the caller needs to forward image data to providers.
func extractContentParts(raw json.RawMessage) []oaiContentPart {
	if len(raw) == 0 {
		return nil
	}

	// Fast path: plain string → single text part.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []oaiContentPart{{Type: "text", Text: s}}
	}

	// Slow path: array of content parts.
	var parts []oaiContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		// Unrecognised shape — wrap raw bytes as text so nothing is lost.
		return []oaiContentPart{{Type: "text", Text: string(raw)}}
	}
	return parts
}

// mustMarshalString wraps a Go string as a JSON-encoded string suitable for
// json.RawMessage (i.e. it adds the surrounding quotes and escapes).
func mustMarshalString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return json.RawMessage(b)
}

type oaiChoice struct {
	Index        int         `json:"index"`
	Message      *oaiMessage `json:"message,omitempty"`
	Delta        *oaiMessage `json:"delta,omitempty"`
	FinishReason *string     `json:"finish_reason"`
}

type oaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type oaiChatResponse struct {
	ID      string      `json:"id"`
	Object  string      `json:"object"`
	Created int64       `json:"created"`
	Model   string      `json:"model"`
	Choices []oaiChoice `json:"choices"`
	Usage   *oaiUsage   `json:"usage,omitempty"`
}

// handleChat is the OpenAI-compatible /v1/chat/completions endpoint.
// Routes through the inference Router when set; returns 501 otherwise.
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("cogos").Start(r.Context(), "chat.request")
	defer span.End()
	r = r.WithContext(ctx)

	// Check for a configured router before spending cycles on body parsing so
	// that a nil router always yields 501 regardless of the request body.
	if s.router == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"type":    "not_implemented",
				"message": "no inference router configured; run with a providers.yaml",
			},
		})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20)) // 4 MB limit
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req oaiChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "parse body: "+err.Error(), http.StatusBadRequest)
		return
	}

	block := NormalizeOpenAIRequest(&req, body, "http")
	block.SessionID = s.process.SessionID()

	// G1: resolve inbound client session → bound identity for attribution.
	// resolveBoundIdentity is nil-safe on harnessBackend and returns an
	// unbound identity when no session id or binding is present.
	bound := s.resolveBoundIdentity(r, req.User)
	if bound.Bound {
		block.TargetIdentity = bound.Subject
	} else if s.nucleus != nil {
		block.TargetIdentity = s.nucleus.Name
	}

	block.WorkspaceID = filepath.Base(s.cfg.WorkspaceRoot)
	s.process.RecordBlock(block)

	clientMsgs := block.Messages

	// Resolve any pending client-ownership tool calls whose results are
	// arriving on this turn. Each role=tool message carries a tool_call_id
	// that matches a previously-forwarded tool.call; emitting the paired
	// tool.result closes the ledger pair.
	for _, msg := range clientMsgs {
		if msg.Role == "tool" && msg.ToolCallID != "" {
			s.process.resolvePendingToolCall(msg.ToolCallID, msg.Content)
		}
	}

	// Extract the user's latest message as the query for relevance scoring.
	query := ""
	for i := len(clientMsgs) - 1; i >= 0; i-- {
		if clientMsgs[i].Role == "user" {
			query = clientMsgs[i].Content
			break
		}
	}

	// Notify the process of the incoming interaction.
	s.process.Send(NewGateEventFromBlock(block, "user.message", query))

	// Assemble foveated context — engine owns the full window.
	// It decomposes client messages, scores them alongside CogDocs,
	// and manages the budget including conversation history.

	// Resolve max tokens: prefer max_completion_tokens (OpenAI v2 field,
	// sent by Zed and newer clients) over legacy max_tokens.
	maxToks := req.MaxTokens
	if req.MaxCompletionTokens > 0 {
		maxToks = req.MaxCompletionTokens
	}

	creq := &CompletionRequest{
		MaxTokens:     maxToks,
		Temperature:   req.Temperature,
		TopP:          req.TopP,
		Stop:          req.Stop,
		InteractionID: block.ID,
		Metadata: RequestMetadata{
			RequestID:    uuid.New().String(),
			ProcessState: "active", // chat requests are always active interactions
			Priority:     PriorityNormal,
			Source:       "http",
		},
	}

	// Convert OpenAI-format tool definitions to internal ToolDefinition and
	// partition by ownership. Three ownership pools:
	//   - Bash/Read/Write/... — kernel via classifyToolOwnership (claude-CLI
	//     / MCP-bridge path, unchanged).
	//   - cog_*, mod3_*, etc. — kernel via MCPServer.CallTool when the model
	//     emits a tool_use for them (closes #94).
	//   - everything else (browser_*, agent-defined tools) — forwarded to
	//     the client as OpenAI tool_calls for client-side execution.
	//
	// Ledger L06: a client-supplied name that collides with an MCP-registered
	// kernel tool is REFUSED here, not admitted-and-withheld. Admitting it to
	// creq.Tools handed the provider a real callable whose tool_use
	// splitToolCallsByOwnership then routed into MCPServer.CallTool.
	if len(req.Tools) > 0 {
		defs := make([]ToolDefinition, 0, len(req.Tools))
		for _, t := range req.Tools {
			if t.Type != "function" {
				continue
			}
			defs = append(defs, ToolDefinition{
				Name:        t.Function.Name,
				Description: t.Function.Description,
				InputSchema: t.Function.Parameters,
			})
		}
		tools, external, rejected := admitClientSuppliedTools(defs, s.mcpServer)
		creq.Tools = tools
		creq.ExternalTools = append(creq.ExternalTools, external...)
		creq.RefusedToolNames = rejected
		if len(rejected) > 0 {
			slog.Warn("chat: refused client-supplied kernel-owned tool definitions",
				"request_id", creq.Metadata.RequestID,
				"rejected", rejected,
			)
		}
	}

	// Convert tool_choice: OpenAI sends either a string ("auto"/"none"/"required")
	// or an object {"type":"function","function":{"name":"..."}}.
	if len(req.ToolChoice) > 0 {
		var tcStr string
		if err := json.Unmarshal(req.ToolChoice, &tcStr); err == nil {
			creq.ToolChoice = tcStr
		} else {
			var tcObj struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			}
			if err := json.Unmarshal(req.ToolChoice, &tcObj); err == nil && tcObj.Function.Name != "" {
				creq.ToolChoice = tcObj.Function.Name
			}
		}
	}

	// Map OpenClaw model names to provider routing via the shared resolver.
	// ResolveModelRequest centralises all alias + router-probe logic so that
	// the gateway and dispatch tool share a single source of truth. The
	// InjectKernelTools flag is handled below (kernel-agent / ollama path).
	{
		// Kernel-boundary admission: reject a non-empty model id that resolves
		// to no known routing target (alias / "local" / provider name /
		// provider-served model) with HTTP 400 + the available menu. Without
		// this guard an unknown id (e.g. "gpt-4") falls through to the default
		// provider, which POSTs the bogus id upstream and surfaces an opaque
		// 500-wrapped 404. "" (default routing) and all known ids pass through.
		if !IsKnownModel(s.router, req.Model) {
			menu := AvailableModelIDs(s.router)
			slog.Warn("chat: rejected unknown model id at kernel boundary",
				"request_id", creq.Metadata.RequestID,
				"model", req.Model,
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"type": "invalid_request_error",
					"message": fmt.Sprintf(
						"unknown model %q; available models: %s",
						req.Model, strings.Join(menu, ", "),
					),
					"param":            "model",
					"available_models": menu,
				},
			})
			return
		}

		mres := ResolveModelRequest(s.router, req.Model, creq.Metadata.RequestID)
		creq.Metadata.PreferProvider = mres.PreferProvider
		creq.ModelOverride = mres.ModelOverride
		// kernel-agent / ollama: auto-inject the kernel's MCP tool registry when
		// the client did not supply tools of its own. Closes myrgic/cogos#89.
		// "kernel-agent" is the canonical alias for "the same harness the dispatch
		// tool uses" — currently Ollama with the default kernel-core model.
		// Eventually this aliases the kernel-managed in-host harness;
		// see .cog/scratch/audit-inference-paths/REPORT.md.
		if mres.InjectKernelTools && len(creq.Tools) == 0 && s.mcpServer != nil {
			// RefusedToolNames records CLIENT-supplied definitions we declined
			// to honour. Kernel injection is a different authority: it offers
			// the kernel's own tools on the kernel's terms. Clear the refusal
			// set before injecting, or a name refused as client-supplied
			// (e.g. cog_read_cogdoc) stays refused after the kernel itself
			// legitimately offers it — the model sees the tool, calls it,
			// and the ownership split drops the call. (Review on #606.)
			creq.RefusedToolNames = nil
			injectKernelAgentTools(creq, s.mcpServer)
			// G2 PART C: when IdentityNakedDefault is true and the request is
			// bound to an identity with a wired capResolver, filter the injected
			// tools to those permitted by the envelope. This prevents the model
			// from being told about (and attempting to call) tools the identity's
			// envelope disallows. Permit-by-default: no envelope → no filtering.
			if s.cfg.IdentityNakedDefault && bound.Bound && s.mcpServer.capResolver != nil {
				filterToolsByCapability(creq, bound.Subject, s.mcpServer.capResolver)
			}
		}
	}

	var pkg *ContextPackage
	conversationTurnsIn := 0
	for _, m := range clientMsgs {
		if m.Role != "system" {
			conversationTurnsIn++
		}
	}

	// Fetch the most-recent barge-in event from mod3 for this session.
	// Used two ways: (a) inject previous-turn speculative text into context,
	// (b) backfill the previous turn's sidecar row after RecordTurn.
	// Best-effort: a nil event means no barge-in occurred or mod3 is down.
	var bargeinEv *mod3BargeinEvent
	if s.cfg != nil && s.cfg.Mod3URL != "" {
		bargeinEv = fetchRecentBargeinEvent(r.Context(), s.cfg.Mod3URL, block.SessionID)
	}
	previousSpeculative := ""
	if bargeinEv != nil {
		previousSpeculative = bargeinEv.TextSpeculative
	}

	// Read the previous turn's TurnID now (before RecordTurn writes the new
	// row) so we can backfill speculative fields after inference completes.
	// best-effort: nil means first turn or sidecar unreadable.
	var previousTurnID string
	if bargeinEv != nil {
		if prev, err := ReadLastTurn(s.cfg.WorkspaceRoot, block.SessionID); err == nil && prev != nil {
			previousTurnID = prev.TurnID
		}
	}

	// Allow per-request budget override via the X-Cogos-Context-Budget header.
	// A value of 0 (absent or unparseable) defers to the kernel's configured
	// default_budget (or the package-level DefaultBudget fallback).
	contextBudget := 0
	if hv := r.Header.Get("X-Cogos-Context-Budget"); hv != "" {
		if v, err := strconv.Atoi(hv); err == nil && v > 0 {
			contextBudget = v
		}
	}

	// G1: embodiment gating.
	//
	// When IdentityNakedDefault is false (default), behavior is exactly
	// today's: run AssembleContext + nucleus card on every request. The ONLY
	// observable difference from before G1 is that bound sessions carry their
	// own subject in block.TargetIdentity (set above).
	//
	// When IdentityNakedDefault is true:
	//   • nucleus-bound request (bound.Subject == nucleus.Name): full embodiment.
	//   • foreign-bound or unbound: clean transport — skip AssembleContext,
	//     forward client messages verbatim (including role:system), no nucleus card.
	nucleusName := ""
	if s.nucleus != nil {
		nucleusName = s.nucleus.Name
	}
	useFullEmbodiment := !s.cfg.IdentityNakedDefault ||
		(bound.Bound && bound.Subject == nucleusName)

	// G3 Part A: spawn embodiment — set the claude-code working directory.
	//
	// flag OFF (default) → WorkDir stays empty; today's behavior (no Dir set).
	// flag ON + bound    → resolve bound.WorkspaceRoot cog:// URI to fs path.
	// flag ON + unbound  → neutral os.MkdirTemp so no cog CLAUDE.md loads.
	if s.cfg.IdentityNakedDefault {
		if bound.Bound && bound.WorkspaceRoot != "" {
			if fsPath := resolveWorkspaceRootPath(s.cfg.WorkspaceRoot, bound.WorkspaceRoot); fsPath != "" {
				creq.WorkDir = fsPath
			}
		} else if !bound.Bound {
			if tmp, err := os.MkdirTemp("", "cog-anon-"); err == nil {
				creq.WorkDir = tmp
			}
		}
	}

	// G3 Part B: memory scope — build the option slice to pass to AssembleContext.
	// flag OFF → assembleOpts are unchanged (no WithMemoryScope); today's behavior.
	// flag ON + bound + non-empty namespace → restrict foveation to namespace.
	var assembleScopeOpts []AssembleOption
	if s.cfg.IdentityNakedDefault && bound.Bound && bound.MemoryNamespace != "" {
		assembleScopeOpts = append(assembleScopeOpts, WithMemoryScope(bound.MemoryNamespace))
	}

	// Foveation / light-cone key: derive a STABLE per-conversation key, not the
	// per-request UUID (creq.Metadata.RequestID). The old UUID key made the light
	// cone never persist and leaked one orphaned entry per request. Do NOT use
	// block.SessionID here — it is Process.SessionID() (process-wide) and would
	// bleed light-cone state across every concurrent conversation/user. See
	// foveation_session_key.go.
	foveationKey := foveationSessionKey(r.Header.Get(foveationKeyHeader), req.User, clientMsgs)

	if useFullEmbodiment {
		assembleOpts := []AssembleOption{
			WithContext(r.Context()),
			WithConversationID(foveationKey),
			WithManifestMode(true),
			WithPreviousTurnSpeculative(previousSpeculative),
		}
		assembleOpts = append(assembleOpts, assembleScopeOpts...)
		if p, err := s.process.AssembleContext(query, clientMsgs, contextBudget, assembleOpts...); err != nil {
			slog.Warn("chat: context assembly failed", "err", err)
			// Fallback: preserve any client-supplied role=system messages as the
			// provider SystemPrompt, stripping them from the Messages slice so
			// Anthropic's API (which rejects role=system inside messages) still
			// works. Without this, BrowserOS-style clients that include a system
			// prompt see it silently dropped on the fallback path.
			var clientSysParts []string
			var nonSysMsgs []ProviderMessage
			for _, m := range clientMsgs {
				if m.Role == "system" {
					if strings.TrimSpace(m.Content) != "" {
						clientSysParts = append(clientSysParts, m.Content)
					}
					continue
				}
				nonSysMsgs = append(nonSysMsgs, m)
			}
			creq.Messages = nonSysMsgs
			creq.SystemPrompt = mergeSystemPrompts(s.nucleusCard(), clientSysParts)
		} else {
			pkg = p
			systemPrompt, managedMsgs := pkg.FormatForProvider()
			creq.SystemPrompt = systemPrompt
			creq.Messages = managedMsgs

			// Record metrics + span attributes.
			span.SetAttributes(
				attribute.Int("cogos.context.total_tokens", pkg.TotalTokens),
				attribute.Int("cogos.context.docs_injected", len(pkg.FovealDocs)),
				attribute.Int("cogos.context.conv_turns_kept", len(pkg.Conversation)),
				attribute.Int("cogos.context.conv_turns_in", conversationTurnsIn),
			)
			if instruments.ContextTokens != nil {
				instruments.ContextTokens.Record(ctx, int64(pkg.TotalTokens))
			}
			if instruments.DocsInjected != nil {
				instruments.DocsInjected.Record(ctx, int64(len(pkg.FovealDocs)))
			}
			evicted := conversationTurnsIn - len(pkg.Conversation)
			if evicted > 0 && instruments.TurnsEvicted != nil {
				instruments.TurnsEvicted.Add(ctx, int64(evicted))
			}

			if len(pkg.InjectedPaths) > 0 {
				slog.Info("chat: context injected",
					"docs", len(pkg.InjectedPaths),
					"conv_turns", len(pkg.Conversation),
					"tokens", pkg.TotalTokens,
				)
			}
		}
	} else {
		// Clean transport path (IdentityNakedDefault=true, foreign or unbound).
		// Forward client messages verbatim; no nucleus card; AssembleContext skipped.
		var clientSysParts []string
		var nonSysMsgs []ProviderMessage
		for _, m := range clientMsgs {
			if m.Role == "system" {
				if strings.TrimSpace(m.Content) != "" {
					clientSysParts = append(clientSysParts, m.Content)
				}
				continue
			}
			nonSysMsgs = append(nonSysMsgs, m)
		}
		creq.Messages = nonSysMsgs
		creq.SystemPrompt = mergeSystemPrompts("", clientSysParts)
	}

	provider, _, err := s.router.Route(r.Context(), creq)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": sanitizeErrorMessage(err.Error()),
				"type":    "server_error",
				"param":   nil,
				"code":    nil,
			},
		})
		return
	}

	respID := "chatcmpl-" + uuid.New().String()
	model := provider.Name()
	if req.Model != "" && req.Model != "local" {
		model = req.Model
	}

	span.SetAttributes(
		attribute.String("cogos.provider", provider.Name()),
		attribute.String("cogos.model", model),
	)
	if instruments.ChatRequests != nil {
		instruments.ChatRequests.Add(ctx, 1)
	}

	// Prepare the turn record. Fully populated by the provider path (complete/stream)
	// below, then persisted via RecordTurn once the response is on its way to the client.
	turn := &TurnRecord{
		TurnID:    uuid.New().String(),
		TurnIndex: NextTurnIndex(s.cfg.WorkspaceRoot, block.SessionID),
		SessionID: block.SessionID,
		Timestamp: time.Now().UTC(),
		Prompt:    query,
		Provider:  provider.Name(),
		Model:     model,
		BlockID:   block.ID,
	}

	inferStart := time.Now()
	var pt chatPhaseTimings
	if req.Stream {
		pt = s.streamChat(w, r.Context(), creq, provider, respID, model, req.StreamOptions, turn)
	} else {
		pt = s.completeChat(w, r.Context(), creq, provider, respID, model, turn)
	}

	inferMs := float64(time.Since(inferStart).Milliseconds())
	span.SetAttributes(attribute.Float64("cogos.inference.latency_ms", inferMs))
	if instruments.InferenceLatency != nil {
		instruments.InferenceLatency.Record(ctx, inferMs)
	}

	// Emit per-phase sub-spans to bus_traces so operators can reconstruct
	// the turn timeline without a Jaeger collector. Each event carries a
	// parent_span_id that matches the outer handler span's span_id (emitted
	// by withSpan). The outer span fires after handleChat returns so the
	// parent_span_id is not yet known here; we use a stable request-ID as a
	// correlation key instead and let the operator join on session_id+ts.
	sessionID := block.SessionID

	// Extract the W3C trace-id from the incoming traceparent header so the
	// sub-span events can share the same trace_id as the mod3 phase events that
	// injected it (CogOSProvider._make_traceparent). This lets the trace panel
	// join mod3 phase events with kernel sub-spans by trace_id.
	// Format: "00-<trace_id_32hex>-<parent_id_16hex>-<flags>"
	var subSpanTraceID string
	if tp := r.Header.Get("traceparent"); tp != "" {
		if parts := strings.SplitN(tp, "-", 4); len(parts) == 4 && len(parts[1]) == 32 {
			subSpanTraceID = parts[1]
		}
	}

	// chat.prompt_eval — covers context assembly + upstream connection setup.
	if !pt.promptEvalStart.IsZero() && !pt.promptEvalEnd.IsZero() {
		emitChatSubSpan(s.busSessions, ChatSubSpan{
			SpanName:   "chat.prompt_eval",
			TraceID:    subSpanTraceID,
			StartedAt:  pt.promptEvalStart,
			DurationMS: pt.promptEvalEnd.Sub(pt.promptEvalStart).Milliseconds(),
			Model:      model,
			SessionID:  sessionID,
		})
	}

	// chat.thinking_generation — only emitted when reasoning_content arrived.
	if !pt.thinkingStart.IsZero() {
		emitChatSubSpan(s.busSessions, ChatSubSpan{
			SpanName:    "chat.thinking_generation",
			TraceID:     subSpanTraceID,
			StartedAt:   pt.thinkingStart,
			DurationMS:  pt.thinkingEnd.Sub(pt.thinkingStart).Milliseconds(),
			TokensThink: pt.tokensThink,
			Model:       model,
			SessionID:   sessionID,
		})
	}

	// chat.answer_generation — always emitted when answer content arrived.
	if !pt.answerStart.IsZero() {
		totalToks := pt.tokensThink + pt.tokensAnswer
		emitChatSubSpan(s.busSessions, ChatSubSpan{
			SpanName:     "chat.answer_generation",
			TraceID:      subSpanTraceID,
			StartedAt:    pt.answerStart,
			DurationMS:   pt.answerEnd.Sub(pt.answerStart).Milliseconds(),
			TokensAnswer: pt.tokensAnswer,
			TokensTotal:  totalToks,
			Model:        model,
			SessionID:    sessionID,
		})
	}

	// chat.tool_call_resolution — only emitted when kernel-internal tools ran.
	if !pt.toolCallStart.IsZero() {
		emitChatSubSpan(s.busSessions, ChatSubSpan{
			SpanName:      "chat.tool_call_resolution",
			TraceID:       subSpanTraceID,
			StartedAt:     pt.toolCallStart,
			DurationMS:    pt.toolCallEnd.Sub(pt.toolCallStart).Milliseconds(),
			ToolCallCount: pt.toolCallCount,
			Model:         model,
			SessionID:     sessionID,
		})
	}

	// Persist the turn (ledger event + sidecar). Closes cogos#20 by
	// capturing the full prompt/response pair, which RecordBlock drops.
	turn.DurationMs = time.Since(inferStart).Milliseconds()
	if err := s.process.RecordTurn(turn); err != nil {
		slog.Warn("chat: RecordTurn failed", "err", err, "session", turn.SessionID)
	}

	// Backfill the previous turn's sidecar row with barge-in position data.
	// This is the speculative-output bookkeeping (Slice 4): the previous turn
	// was interrupted, so we patch its ResponseSpeculative/BargeinPositionMs/
	// BargeinPositionTextOffset fields now that we know them. Done after the
	// current turn is persisted to avoid ordering hazards.
	if bargeinEv != nil && previousTurnID != "" {
		if err := PatchTurnSpeculative(
			s.cfg.WorkspaceRoot,
			block.SessionID,
			previousTurnID,
			bargeinEv.TextSpeculative,
			bargeinEv.BargeinPositionMs,
			bargeinEv.BargeinPositionTextOffset,
		); err != nil {
			slog.Debug("chat: PatchTurnSpeculative failed (best-effort)",
				"err", err,
				"prev_turn_id", previousTurnID,
				"session", block.SessionID,
			)
		}
	}

	// Capture debug snapshot (best-effort, non-blocking).
	// Both completeChat and streamChat populate turn.Usage from the
	// provider response (final chunk for streaming, response payload for
	// non-streaming) — read it back here so the snapshot reports the
	// actual completion-token count rather than a hard-coded zero.
	responseTokens := turn.Usage.OutputTokens
	snapshotLatency := time.Since(inferStart)
	go func() {
		snap := captureDebugSnapshot(
			clientMsgs, query, req.Model, pkg, conversationTurnsIn,
			provider.Name(), model, responseTokens, snapshotLatency,
		)
		s.debug.Store(snap)
	}()
}

// chatPhaseTimings carries per-phase wall-clock timing from a single chat turn.
// Both streamChat and completeChat populate this so handleChat can emit sub-spans
// to bus_traces without modifying the hot streaming path.
type chatPhaseTimings struct {
	promptEvalStart time.Time // when the provider call was initiated (≈ inferStart)
	promptEvalEnd   time.Time // when the first byte / response arrived

	thinkingStart time.Time // first reasoning_content chunk (zero if none)
	thinkingEnd   time.Time // last reasoning_content chunk before answer starts

	answerStart time.Time // first answer-content chunk (or response returned)
	answerEnd   time.Time // stream/response complete

	toolCallStart time.Time // tool-call resolution start (zero if no tools)
	toolCallEnd   time.Time // tool-call resolution end

	tokensThink   int // approximate: len(reasoningContent) rune count (no tokenizer available)
	tokensAnswer  int // approximate: len(content) rune count
	toolCallCount int
}

// completeChat handles non-streaming chat completions.
// The optional turn record is populated with the response text, usage, and
// any tool-call traces so the caller can persist the full turn via RecordTurn.
func (s *Server) completeChat(w http.ResponseWriter, ctx context.Context, req *CompletionRequest,
	provider Provider, respID, model string, turn *TurnRecord) chatPhaseTimings {

	var pt chatPhaseTimings
	pt.promptEvalStart = time.Now()

	// Cancel-safe (#432): a non-streaming request the kernel gives up on
	// (client disconnect, handler ctx cancel) does not reliably abort
	// generation server-side on providers like LM Studio — the request still
	// gets a fully-buffered JSON response here (unchanged client contract),
	// but routing through CompleteCancelSafeIfSupported means an abandoned
	// call actually stops consuming the local inference seat.
	resp, err := CompleteCancelSafeIfSupported(ctx, provider, req)
	pt.promptEvalEnd = time.Now()
	// answerStart/End are derived after we inspect the response payload so we
	// can split the round-trip duration proportionally when reasoning content
	// is present. They are set below, after the tool loop.
	if err != nil {
		recordAbandonedInference("chat-complete", req.Metadata.RequestID, err)
		slog.Warn("chat: complete error", "err", err)
		if turn != nil {
			turn.Status = "error"
			turn.Error = err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": sanitizeErrorMessage(err.Error()),
				"type":    "server_error",
				"param":   nil,
				"code":    nil,
			},
		})
		return pt
	}

	// Server-side execution of MCP-internal tools (cog_*, mod3_*, ...).
	// When the provider emits a tool_use for a tool the kernel itself owns
	// (per MCPServer.IsInternalTool), execute it in-process, append the
	// assistant's tool_calls + a tool_result message to the conversation,
	// and re-issue Complete so the model can either fire another internal
	// tool or produce a final assistant reply. Closes #94.
	//
	// Tools we don't own (browser_*, etc.) fall through unchanged — they
	// surface to the caller as OpenAI tool_calls below.
	if s.mcpServer != nil {
		const maxInternalHops = 8
		for hop := 0; hop < maxInternalHops; hop++ {
			internal, external := splitToolCallsByOwnershipFor(resp.ToolCalls, s.mcpServer, req)
			if len(internal) == 0 {
				break
			}
			// If the provider mixed internal + external tool_use in the same
			// turn we still service the internal ones, but keep the external
			// ones queued for the response so the client gets them.
			req.Messages = appendToolHopMessages(req.Messages, resp, internal)
			if pt.toolCallStart.IsZero() {
				pt.toolCallStart = time.Now()
			}
			for _, tc := range internal {
				s.executeInternalToolCall(ctx, provider.Name(), tc)
				resultText, isErr, callErr := s.mcpServer.CallTool(ctx, tc.Name, []byte(tc.Arguments))
				if callErr != nil {
					slog.Warn("chat: internal MCP tool call failed",
						"tool", tc.Name, "err", callErr,
						"request_id", req.Metadata.RequestID,
					)
					resultText = "tool error: " + callErr.Error()
					isErr = true
				}
				s.process.resolvePendingToolCall(tc.ID, resultText)
				req.Messages = append(req.Messages, ProviderMessage{
					Role:       "tool",
					Content:    resultText,
					Name:       tc.Name,
					ToolCallID: tc.ID,
				})
				pt.toolCallCount++
				if turn != nil {
					rec := ToolCallRecord{
						ID:        tc.ID,
						Name:      tc.Name,
						Arguments: tc.Arguments,
						Result:    truncateForTurn(resultText),
					}
					if isErr {
						rec.Rejected = true
						rec.RejectReason = "tool reported error"
					}
					turn.ToolCalls = append(turn.ToolCalls, rec)
				}
			}
			// Preserve any external tool_calls for the next iteration's
			// response so we don't drop them on the floor. Cancel-safe (#432):
			// same rationale as the initial completeChat call above.
			next, nerr := CompleteCancelSafeIfSupported(ctx, provider, req)
			if nerr != nil {
				recordAbandonedInference("chat-complete-post-tool", req.Metadata.RequestID, nerr)
				slog.Warn("chat: complete after internal tool exec failed", "err", nerr)
				if turn != nil {
					turn.Status = "error"
					turn.Error = nerr.Error()
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{
						"message": sanitizeErrorMessage(nerr.Error()),
						"type":    "server_error",
					},
				})
				return pt
			}
			// If the prior turn carried external tool_use events alongside
			// the internal ones, surface them by merging into next.ToolCalls
			// so the client still sees them in the final response.
			if len(external) > 0 {
				next.ToolCalls = append(next.ToolCalls, external...)
			}
			resp = next
		}
		// Cap exhausted with kernel-owned calls still pending: hard error.
		// Same guard the Anthropic surface got in #600 round 1; this is its
		// twin (round 2). Falling through would render unexecuted cog_*
		// tool_calls to an external client.
		if internal, _ := splitToolCallsByOwnershipFor(resp.ToolCalls, s.mcpServer, req); len(internal) > 0 {
			slog.Warn("chat: internal tool hop cap exceeded", "cap", maxInternalHops, "pending", len(internal))
			if turn != nil {
				turn.Status = "error"
				turn.Error = "internal tool hop cap exceeded"
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "internal tool hop cap exceeded",
					"type":    "server_error",
				},
			})
			return pt
		}
	}
	if !pt.toolCallStart.IsZero() {
		pt.toolCallEnd = time.Now()
	}
	if turn != nil {
		turn.Response = resp.Content
		turn.Usage = TurnUsage{
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
			TotalTokens:  resp.Usage.InputTokens + resp.Usage.OutputTokens,
		}
		// Non-kernel tool calls returned to the client become transcript
		// entries with empty result (the client will execute them).
		if len(resp.ToolCalls) > 0 {
			for _, tc := range resp.ToolCalls {
				turn.ToolCalls = append(turn.ToolCalls, ToolCallRecord{
					ID:        tc.ID,
					Name:      tc.Name,
					Arguments: tc.Arguments,
				})
			}
		}
		// Audit-fidelity fix: when the model replies via speak/send_text/output
		// tool calls, resp.Content is empty. Extract the text payload from the
		// tool-call arguments so turn.Response captures what was actually said.
		if turn.Response == "" {
			turn.Response = extractTextFromToolCalls(turn.ToolCalls)
		}
	}

	// Derive per-phase timings from the response payload.
	//
	// In non-streaming mode the full HTTP round-trip is captured in
	// promptEvalStart→promptEvalEnd; we cannot separately time thinking vs
	// answer generation. Instead we split the round-trip duration
	// proportionally by rune count — the same heuristic streamChat uses on
	// the drain window — so that per-phase durations add up to the full
	// latency rather than being zero.
	//
	// Token counts (the more actionable metric) come directly from the
	// provider's usage fields and resp.ReasoningContent length.
	roundTripDur := pt.promptEvalEnd.Sub(pt.promptEvalStart)
	thinkRunes := utf8.RuneCountInString(resp.ReasoningContent)
	ansRunes := utf8.RuneCountInString(resp.Content)
	totalRunes := thinkRunes + ansRunes
	if thinkRunes > 0 && totalRunes > 0 {
		thinkFrac := float64(thinkRunes) / float64(totalRunes)
		thinkDur := time.Duration(float64(roundTripDur) * thinkFrac)
		pt.thinkingStart = pt.promptEvalStart
		pt.thinkingEnd = pt.promptEvalStart.Add(thinkDur)
		pt.answerStart = pt.thinkingEnd
		pt.tokensThink = thinkRunes // rune-count proxy; no tokenizer in the hot path
	} else {
		pt.answerStart = pt.promptEvalStart
	}
	pt.answerEnd = pt.promptEvalEnd
	pt.tokensAnswer = resp.Usage.OutputTokens

	msg := &oaiMessage{Role: "assistant", Content: mustMarshalString(resp.Content)}
	finishReason := mapStopReasonToOpenAI(resp.StopReason)
	if finishReason == "" {
		finishReason = "stop"
	}

	// Wrap CLIENT-OWNED tool calls in the OpenAI response format. The
	// renderer filters by ownership itself — it is the aperture and does not
	// trust the loop above to have drained every kernel-owned call.
	clientCalls := resp.ToolCalls
	if s.mcpServer != nil {
		_, clientCalls = splitToolCallsByOwnership(resp.ToolCalls, s.mcpServer)
	}
	if len(clientCalls) > 0 {
		finishReason = "tool_calls"
		// OpenAI spec: tool-call-only messages must have "content": null, not "".
		if resp.Content == "" {
			msg.Content = json.RawMessage("null")
		}
		calls := make([]oaiToolCall, len(clientCalls))
		for i, tc := range clientCalls {
			calls[i] = oaiToolCall{
				ID:   tc.ID,
				Type: "function",
				Function: oaiToolCallFunc{
					Name:      tc.Name,
					Arguments: tc.Arguments,
				},
			}
			// Observability: emit tool.call for every client-ownership tool
			// the server is about to forward, and register a pending entry
			// so the next-turn role=tool message resolves to a tool.result.
			s.process.emitToolCall(ToolCallEvent{
				CallID:    tc.ID,
				ToolName:  tc.Name,
				Arguments: json.RawMessage(tc.Arguments),
				Source:    ToolSourceOpenAI,
				Ownership: ToolOwnershipClient,
				Provider:  provider.Name(),
				SessionID: s.process.SessionID(),
			})
			s.process.registerPendingToolCall(tc.ID, tc.Name, ToolSourceOpenAI, 0)
		}
		raw, _ := json.Marshal(calls)
		msg.ToolCalls = json.RawMessage(raw)
	}

	oai := oaiChatResponse{
		ID:      respID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []oaiChoice{{
			Index:        0,
			Message:      msg,
			FinishReason: &finishReason,
		}},
		Usage: &oaiUsage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(oai)
	return pt
}

// streamChat handles streaming chat completions via SSE.
//
// When the inference Router emits tool_use events for kernel-internal MCP
// tools (cog_*, mod3_*, …), we cannot just forward them to the SSE client
// the way pre-#94 streamChat did — the dashboard has no executor for them
// and the turn would end silently with finish_reason: tool_calls. Instead,
// we mirror completeChat's #94 loop: drain the upstream stream into a
// local buffer (text deltas + accumulated tool calls), partition the tool
// calls by ownership, execute internal calls in-process via
// MCPServer.CallTool, append tool_result messages to the conversation, and
// re-issue Stream() up to maxStreamHops times. Only when the upstream
// stream completes with no more internal tool_use events do we replay the
// final hop's buffered chunks as SSE to the client — yielding a single
// coherent assistant turn from the client's perspective. External tool
// calls accumulated along the way are surfaced as tool_calls deltas on
// the final replay so the client still receives them. Closes #95.
//
// The optional turn record accumulates the final response text (and usage
// from the final chunk) so the caller can persist the full turn via
// RecordTurn.
func (s *Server) streamChat(w http.ResponseWriter, ctx context.Context, req *CompletionRequest,
	provider Provider, respID, model string, streamOpts *oaiStreamOpts, turn *TurnRecord) chatPhaseTimings {

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, canFlush := w.(http.Flusher)
	bw := bufio.NewWriter(w)

	flush := func() {
		_ = bw.Flush()
		if canFlush {
			flusher.Flush()
		}
	}
	writeSSE := func(data []byte) {
		_, _ = fmt.Fprintf(bw, "data: %s\n\n", data)
		flush()
	}
	writeStreamErr := func(e error) {
		// Headers may already be sent; surface the error as a final SSE
		// chunk with finish_reason:error and a clear stop. The dashboard
		// renders this as a failed turn rather than a hung stream.
		slog.Warn("chat: stream error", "err", e)
		if turn != nil {
			turn.Status = "error"
			turn.Error = e.Error()
		}
		fr := "error"
		data := oaiChatResponse{
			ID:      respID,
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   model,
			Choices: []oaiChoice{{Index: 0, FinishReason: &fr}},
		}
		b, _ := json.Marshal(data)
		writeSSE(b)
	}

	const maxStreamHops = 8

	// Carry-forward state across hops:
	//   carriedExternal  — external (client-owned) tool calls observed on
	//     prior hops that we still owe the client. These are emitted as
	//     deltas on the final replay so a model that mixes internal +
	//     external tool_use in a single turn doesn't lose the externals.
	//   firstStream      — channel from the very first provider.Stream call,
	//     opened upfront so a Stream() error returns the conventional 500.
	//     Subsequent hops re-open the stream from inside the loop.
	var pt chatPhaseTimings
	pt.promptEvalStart = time.Now()

	var carriedExternal []ToolCall

	firstStream, err := provider.Stream(ctx, req)
	if err != nil {
		// No SSE has been sent yet — return the same 500 shape the
		// non-streaming path uses. The dashboard treats this as a hard
		// failure.
		slog.Warn("chat: stream error", "err", err)
		if turn != nil {
			turn.Status = "error"
			turn.Error = err.Error()
		}
		// Replace the SSE headers we set above with a JSON error response.
		// (httptest.ResponseRecorder lets this through; net/http only
		// honours the first WriteHeader, so a header overwrite here is
		// safe before any body bytes have been written.)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": sanitizeErrorMessage(err.Error()),
				"type":    "server_error",
				"param":   nil,
				"code":    nil,
			},
		})
		return pt
	}

	chunks := firstStream
	isFirstHop := true
	for hop := 0; hop < maxStreamHops; hop++ {
		drainStart := time.Now()
		hopBuf, hopErr := drainStreamHop(chunks)
		drainEnd := time.Now()

		// On the first (and usually only) hop, derive per-phase timings from
		// the buffer contents.  Reasoning deltas precede answer deltas in the
		// stream; we split the drain wall-time proportionally by rune count.
		if isFirstHop {
			isFirstHop = false
			pt.promptEvalEnd = drainStart // drain start ≈ first upstream byte
			thinkRunes := utf8.RuneCountInString(hopBuf.reasoningContent.String())
			ansRunes := utf8.RuneCountInString(hopBuf.content.String())
			totalRunes := thinkRunes + ansRunes
			drainDur := drainEnd.Sub(drainStart)
			if totalRunes > 0 && thinkRunes > 0 {
				thinkFrac := float64(thinkRunes) / float64(totalRunes)
				thinkDur := time.Duration(float64(drainDur) * thinkFrac)
				pt.thinkingStart = drainStart
				pt.thinkingEnd = drainStart.Add(thinkDur)
				pt.answerStart = pt.thinkingEnd
			} else {
				pt.answerStart = drainStart
			}
			pt.answerEnd = drainEnd
			pt.tokensThink = thinkRunes // proxy: chars ≈ fractional tokens
			pt.tokensAnswer = ansRunes
		}

		if hopErr != nil {
			writeStreamErr(hopErr)
			break
		}

		// Partition the accumulated tool calls. If there's no MCPServer
		// snapshot wired (legacy / test paths), splitToolCallsByOwnership
		// treats everything as external — same behaviour as completeChat.
		toolCalls := hopBuf.assembledToolCalls()
		internal, external := splitToolCallsByOwnershipFor(toolCalls, s.mcpServer, req)

		// External tool calls observed on this hop are owed to the client.
		// We don't drop them even if the same hop also produced internal
		// calls we'll service in-process.
		carriedExternal = append(carriedExternal, external...)

		// Terminal hop: no internal tool_use → flush this hop's buffered
		// content + any carried-forward external tool_calls to the client
		// and exit.
		if len(internal) == 0 {
			s.flushStreamHop(hopBuf, carriedExternal, provider, respID, model, streamOpts, turn, writeSSE)
			break
		}

		// Hop has internal tool calls. Execute them in-process, append the
		// tool_result messages to the conversation, then start a fresh
		// upstream stream. Match completeChat's message-shape contract so
		// the model sees an identical transcript whether the client
		// streams or not.
		req.Messages = appendToolHopMessages(req.Messages, &CompletionResponse{
			Content:    hopBuf.content.String(),
			ToolCalls:  toolCalls,
			StopReason: hopBuf.stopReason,
		}, internal)
		if pt.toolCallStart.IsZero() {
			pt.toolCallStart = time.Now()
		}
		for _, tc := range internal {
			s.executeInternalToolCall(ctx, provider.Name(), tc)
			resultText, isErr, callErr := s.mcpServer.CallTool(ctx, tc.Name, []byte(tc.Arguments))
			if callErr != nil {
				slog.Warn("chat: internal MCP tool call failed (stream)",
					"tool", tc.Name, "err", callErr,
					"request_id", req.Metadata.RequestID,
				)
				resultText = "tool error: " + callErr.Error()
				isErr = true
			}
			s.process.resolvePendingToolCall(tc.ID, resultText)
			req.Messages = append(req.Messages, ProviderMessage{
				Role:       "tool",
				Content:    resultText,
				Name:       tc.Name,
				ToolCallID: tc.ID,
			})
			pt.toolCallCount++
			if turn != nil {
				rec := ToolCallRecord{
					ID:        tc.ID,
					Name:      tc.Name,
					Arguments: tc.Arguments,
					Result:    truncateForTurn(resultText),
				}
				if isErr {
					rec.Rejected = true
					rec.RejectReason = "tool reported error"
				}
				turn.ToolCalls = append(turn.ToolCalls, rec)
			}
		}
		if !pt.toolCallStart.IsZero() {
			pt.toolCallEnd = time.Now()
		}

		// Hop overflow guard: if the next hop would exceed the cap, surface
		// a clean error rather than silently terminating after the loop.
		if hop+1 >= maxStreamHops {
			writeStreamErr(fmt.Errorf("stream tool-loop exceeded %d hops", maxStreamHops))
			break
		}

		nextStream, nerr := provider.Stream(ctx, req)
		if nerr != nil {
			writeStreamErr(fmt.Errorf("stream after internal tool exec: %w", nerr))
			break
		}
		chunks = nextStream
	}

	_, _ = fmt.Fprint(bw, "data: [DONE]\n\n")
	flush()
	return pt
}

// streamHopBuffer collects everything one upstream stream emitted on a
// single hop: ordered text deltas (so we can replay token-by-token on the
// final hop), tool-call deltas reassembled by index/id, the provider's
// final stop reason and usage, and any chunk-level error. Nothing here is
// emitted to the client — the chat handler decides whether to flush this
// to SSE (terminal hop) or feed it back into the conversation as a
// tool_use turn (intermediate hop).
type streamHopBuffer struct {
	deltas           []string
	content          strings.Builder
	reasoningDeltas  []string // reasoning/thinking chunks (IsReasoning=true)
	reasoningContent strings.Builder
	stopReason       string
	usage            *TokenUsage
	// toolCalls maps streaming index → assembled call. Provider-side the
	// index is stable across deltas for the same call; the ID typically
	// arrives on the first delta and arguments accumulate after.
	toolCalls map[int]*ToolCall
	// toolOrder preserves first-seen index ordering so assembledToolCalls()
	// returns calls in the order the provider emitted them.
	toolOrder []int
}

func newStreamHopBuffer() *streamHopBuffer {
	return &streamHopBuffer{toolCalls: make(map[int]*ToolCall)}
}

// assembledToolCalls returns the buffered tool calls in first-seen order.
func (b *streamHopBuffer) assembledToolCalls() []ToolCall {
	if len(b.toolOrder) == 0 {
		return nil
	}
	out := make([]ToolCall, 0, len(b.toolOrder))
	for _, idx := range b.toolOrder {
		if tc, ok := b.toolCalls[idx]; ok {
			out = append(out, *tc)
		}
	}
	return out
}

// drainStreamHop consumes one upstream stream's chunks into a buffer. It
// returns the first chunk-level error it sees (and a partially-populated
// buffer) so the caller can surface a clean SSE error. A nil-error return
// always implies the provider sent a Done chunk (terminating cleanly) or
// closed the channel.
func drainStreamHop(chunks <-chan StreamChunk) (*streamHopBuffer, error) {
	buf := newStreamHopBuffer()
	for sc := range chunks {
		if sc.Error != nil {
			return buf, sc.Error
		}
		if sc.Done {
			buf.stopReason = sc.StopReason
			buf.usage = sc.Usage
			return buf, nil
		}
		if sc.ToolCallDelta != nil {
			d := sc.ToolCallDelta
			tc, ok := buf.toolCalls[d.Index]
			if !ok {
				tc = &ToolCall{}
				buf.toolCalls[d.Index] = tc
				buf.toolOrder = append(buf.toolOrder, d.Index)
			}
			if d.ID != "" {
				tc.ID = d.ID
			}
			if d.Name != "" {
				tc.Name = d.Name
			}
			if d.ArgsDelta != "" {
				tc.Arguments += d.ArgsDelta
			}
			continue
		}
		if sc.Delta != "" {
			if sc.IsReasoning {
				buf.reasoningDeltas = append(buf.reasoningDeltas, sc.Delta)
				buf.reasoningContent.WriteString(sc.Delta)
			} else {
				buf.deltas = append(buf.deltas, sc.Delta)
				buf.content.WriteString(sc.Delta)
			}
		}
	}
	// Channel closed without an explicit Done — treat as benign EOF.
	return buf, nil
}

// flushStreamHop emits the terminal hop's buffered text deltas, any
// carried-forward external tool_calls, and the final finish_reason chunk
// to the SSE writer. This is the only place the streaming chat handler
// writes deltas to the client; intermediate hops are buffered silently.
func (s *Server) flushStreamHop(buf *streamHopBuffer, externalCalls []ToolCall,
	provider Provider, respID, model string, streamOpts *oaiStreamOpts,
	turn *TurnRecord, writeSSE func([]byte)) {

	now := time.Now().Unix()

	// Replay text deltas in order so the client sees token-by-token
	// streaming on the final assistant turn.
	for _, d := range buf.deltas {
		delta := &oaiMessage{Role: "assistant", Content: mustMarshalString(d)}
		data := oaiChatResponse{
			ID:      respID,
			Object:  "chat.completion.chunk",
			Created: now,
			Model:   model,
			Choices: []oaiChoice{{Index: 0, Delta: delta}},
		}
		b, _ := json.Marshal(data)
		writeSSE(b)
	}

	// Emit external tool_calls accumulated across all hops as a single
	// delta so the client can dispatch them. We collapse multiple deltas
	// per call into one (id + name + full arguments) — the streaming
	// reassembly happened server-side, so re-fragmenting buys nothing.
	if len(externalCalls) > 0 {
		oaiCalls := make([]oaiStreamToolCall, len(externalCalls))
		for i, tc := range externalCalls {
			oaiCalls[i] = oaiStreamToolCall{
				Index: i,
				ID:    tc.ID,
				Type:  "function",
				Function: &oaiToolCallFunc{
					Name:      tc.Name,
					Arguments: tc.Arguments,
				},
			}
			// Observability parity with completeChat: emit tool.call +
			// register pending row for every client-bound tool we surface.
			s.process.emitToolCall(ToolCallEvent{
				CallID:    tc.ID,
				ToolName:  tc.Name,
				Arguments: json.RawMessage(tc.Arguments),
				Source:    ToolSourceOpenAI,
				Ownership: ToolOwnershipClient,
				Provider:  provider.Name(),
				SessionID: s.process.SessionID(),
			})
			s.process.registerPendingToolCall(tc.ID, tc.Name, ToolSourceOpenAI, 0)
		}
		tcRaw, _ := json.Marshal(oaiCalls)
		delta := &oaiMessage{Role: "assistant", ToolCalls: json.RawMessage(tcRaw)}
		data := oaiChatResponse{
			ID:      respID,
			Object:  "chat.completion.chunk",
			Created: now,
			Model:   model,
			Choices: []oaiChoice{{Index: 0, Delta: delta}},
		}
		b, _ := json.Marshal(data)
		writeSSE(b)
	}

	// Final chunk: finish_reason. Prefer the provider-reported reason; if
	// we surfaced any external tool_calls and the provider didn't say so,
	// upgrade to "tool_calls" so the client treats it as a tool turn.
	finishReason := mapStopReasonToOpenAI(buf.stopReason)
	if finishReason == "" {
		finishReason = "stop"
	}
	if len(externalCalls) > 0 {
		finishReason = "tool_calls"
	}

	if turn != nil {
		turn.Response = buf.content.String()
		if buf.usage != nil {
			turn.Usage = TurnUsage{
				InputTokens:  buf.usage.InputTokens,
				OutputTokens: buf.usage.OutputTokens,
				TotalTokens:  buf.usage.InputTokens + buf.usage.OutputTokens,
			}
		}
		// Audit-fidelity fix: when the model replies via speak/send_text/output
		// tool calls, the streaming buffer text content is empty. Extract the
		// text payload from tool-call arguments so the turn record captures what
		// was actually said.
		if turn.Response == "" {
			turn.Response = extractTextFromToolCalls(turn.ToolCalls)
		}
	}

	data := oaiChatResponse{
		ID:      respID,
		Object:  "chat.completion.chunk",
		Created: now,
		Model:   model,
		Choices: []oaiChoice{{Index: 0, FinishReason: &finishReason}},
	}
	if streamOpts != nil && streamOpts.IncludeUsage && buf.usage != nil {
		data.Usage = &oaiUsage{
			PromptTokens:     buf.usage.InputTokens,
			CompletionTokens: buf.usage.OutputTokens,
			TotalTokens:      buf.usage.InputTokens + buf.usage.OutputTokens,
		}
	}
	b, _ := json.Marshal(data)
	writeSSE(b)
}

// handleToolCalls is the HTTP companion to cog_read_tool_calls.
//
// GET /v1/tool-calls
//
//	?session_id=&tool_name=&status=&source=&ownership=&call_id=
//	&since=&until=&limit=&order=
//	&include_args=&include_output=
//
// Returns the same stitched call+result rows as the MCP tool. Missing or
// malformed query params error with 400; empty filter set returns the
// default-limit most-recent rows.
func (s *Server) handleToolCalls(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	query := ToolCallQuery{
		SessionID:     q.Get("session_id"),
		ToolName:      q.Get("tool_name"),
		Status:        q.Get("status"),
		Source:        q.Get("source"),
		Ownership:     q.Get("ownership"),
		CallID:        q.Get("call_id"),
		Order:         q.Get("order"),
		IncludeArgs:   boolQueryParam(q.Get("include_args")),
		IncludeOutput: boolQueryParam(q.Get("include_output")),
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := parseIntQuery(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid limit"})
			return
		}
		query.Limit = n
	}
	if raw := q.Get("since"); raw != "" {
		ts, err := parseTimeOrDuration(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid since: " + err.Error()})
			return
		}
		query.Since = ts
	}
	if raw := q.Get("until"); raw != "" {
		ts, err := parseTimeOrDuration(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid until: " + err.Error()})
			return
		}
		query.Until = ts
	}

	result, err := QueryToolCalls(s.cfg.WorkspaceRoot, query)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

// boolQueryParam accepts "true", "1", "yes" (case-insensitive) as true.
func boolQueryParam(raw string) bool {
	switch regexp.MustCompile(`^(true|1|yes|on)$`).MatchString(raw) {
	case true:
		return true
	default:
		return false
	}
}

// parseIntQuery returns the int value of a query string param, or an error.
func parseIntQuery(raw string) (int, error) {
	var n int
	_, err := fmt.Sscanf(raw, "%d", &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// handleDebugLast returns the full pipeline snapshot from the most recent chat request.
func (s *Server) handleDebugLast(w http.ResponseWriter, r *http.Request) {
	snap := s.debug.Load()
	if snap == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "no requests yet"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}

// handleDebugContext returns the current context window as stability-ordered zones.
//
// Decorated with the active foveated-gating knobs so dashboards can show
// operators which floor/cap the assembler is currently using (issue #88).
func (s *Server) handleDebugContext(w http.ResponseWriter, r *http.Request) {
	snap := s.debug.Load()
	if snap == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "no requests yet"})
		return
	}

	maxDocs, floor := s.cfg.ContextGating()
	resp := map[string]interface{}{
		"zones":  snap.Context.Zones,
		"budget": snap.Context.Budget,
		"gating": map[string]interface{}{
			"max_foveal_docs": maxDocs,
			"salience_floor":  floor,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

// handleGetContextSettings returns the current foveated-gating knobs.
//
//	GET /v1/settings/context
//	200 → { max_foveal_docs, salience_floor }
func (s *Server) handleGetContextSettings(w http.ResponseWriter, r *http.Request) {
	maxDocs, floor := s.cfg.ContextGating()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"max_foveal_docs": maxDocs,
		"salience_floor":  floor,
	})
}

// handlePatchContextSettings hot-updates foveated-gating knobs. Request body
// is JSON with optional fields max_foveal_docs (int) and salience_floor (float).
// Returns the post-update snapshot.
//
//	PATCH /v1/settings/context  { "salience_floor": 0.5 }
//	200 → { max_foveal_docs, salience_floor }
//	400 → malformed JSON or out-of-range value
func (s *Server) handlePatchContextSettings(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MaxFovealDocs *int     `json:"max_foveal_docs"`
		SalienceFloor *float64 `json:"salience_floor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("decode body: %v", err)})
		return
	}
	if body.MaxFovealDocs != nil && *body.MaxFovealDocs < 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "max_foveal_docs must be >= 0"})
		return
	}
	if body.SalienceFloor != nil && *body.SalienceFloor < 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "salience_floor must be >= 0"})
		return
	}

	maxDocs, floor := s.cfg.SetContextGating(body.MaxFovealDocs, body.SalienceFloor)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"max_foveal_docs": maxDocs,
		"salience_floor":  floor,
	})
}

// handleProprioceptive returns the last 50 entries from the proprioceptive JSONL log
// plus a placeholder light cone status.
//
//	GET /v1/proprioceptive
//	200 → { entries, light_cone }
func (s *Server) handleProprioceptive(w http.ResponseWriter, r *http.Request) {
	logPath := filepath.Join(s.cfg.WorkspaceRoot, ".cog", "run", "proprioceptive.jsonl")

	entries := readLastJSONLEntries(logPath, 50)

	// Build light cone summary from real data when available.
	lcStatus := map[string]interface{}{
		"active":          false,
		"layers":          0,
		"layer_norms":     []float64{},
		"compressed_norm": 0.0,
	}
	if lcm := s.process.LightCones(); lcm != nil {
		count := lcm.Count()
		lcStatus["active"] = count > 0
		lcStatus["count"] = count
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"entries":    entries,
		"light_cone": lcStatus,
	})
}

// handleLedger exposes QueryLedger over HTTP. Query params map 1:1 with the
// MCP tool input.
//
//	GET /v1/ledger?session_id=…&event_type=…&after_seq=…&since_timestamp=…&limit=…&verify_chain=…
//	200 → { count, events, truncated, verification, next_after_seq? }
//	400 → malformed query (bad int, bad RFC3339, after_seq without session_id)
//	404 → session_id specified but no events.jsonl found
//	500 → read/JSON failure
//
// Returns 200 with verification.valid=false when the chain is broken but data
// read succeeded — tamper evidence is the point of the ledger, so hiding data
// behind 500 would defeat the purpose.
func (s *Server) handleLedger(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	query := LedgerQuery{
		SessionID:      q.Get("session_id"),
		EventType:      q.Get("event_type"),
		SinceTimestamp: q.Get("since_timestamp"),
	}
	if v := q.Get("after_seq"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeLedgerError(w, http.StatusBadRequest, fmt.Sprintf("after_seq: %v", err))
			return
		}
		query.AfterSeq = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeLedgerError(w, http.StatusBadRequest, fmt.Sprintf("limit: %v", err))
			return
		}
		if n < 0 {
			writeLedgerError(w, http.StatusBadRequest, "limit must be non-negative")
			return
		}
		query.Limit = n
	}
	if v := q.Get("verify_chain"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeLedgerError(w, http.StatusBadRequest, fmt.Sprintf("verify_chain: %v", err))
			return
		}
		query.VerifyChain = b
	}

	result, err := QueryLedger(s.cfg.WorkspaceRoot, query)
	if err != nil {
		switch {
		case errors.Is(err, ErrAfterSeqRequiresSession):
			writeLedgerError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, ErrSessionNotFound):
			writeLedgerError(w, http.StatusNotFound, err.Error())
		default:
			// Filter-parse errors from QueryLedger (e.g. bad since_timestamp)
			// are user input problems. Everything else is a 500.
			msg := err.Error()
			if strings.Contains(msg, "since_timestamp") || strings.Contains(msg, "event_type") {
				writeLedgerError(w, http.StatusBadRequest, msg)
				return
			}
			writeLedgerError(w, http.StatusInternalServerError, msg)
		}
		return
	}

	_ = json.NewEncoder(w).Encode(result)
}

// writeLedgerError writes a JSON error response with the given status code.
func writeLedgerError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// handleTraces exposes the unified kernel trace search surface.
//
// Per Agent Q's design (2026-04-21) this is additive — /v1/proprioceptive
// stays byte-for-byte identical because dashboard.html:1265 and
// canvas.html:1706 consume its exact {entries, light_cone} shape.
//
//	GET /v1/traces
//	    ?source=attention      (or "all", default)
//	    &level=…               (applies to sources with a level-like field)
//	    &session_id=…
//	    &substring=…           (case-insensitive, full-line match)
//	    &since=5m              (RFC3339 OR Go duration)
//	    &until=…               (RFC3339 upper bound)
//	    &limit=100             (1..1000)
//	    &order=desc            ("asc" | "desc")
//
//	200 → TraceQueryResult
//	400 → unknown source / unparseable since|until / limit out of range / substring too long
//	500 → I/O error
func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	q, err := parseTraceQueryFromRequest(r)
	if err != nil {
		writeTraceError(w, http.StatusBadRequest, err)
		return
	}
	result, err := QueryTraces(s.cfg.WorkspaceRoot, q)
	if err != nil {
		writeTraceError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// parseTraceQueryFromRequest maps query params onto a TraceQuery.
// Defaults: source=all, limit=100, order=desc.
func parseTraceQueryFromRequest(r *http.Request) (TraceQuery, error) {
	q := TraceQuery{
		Source:    TraceSource(strings.TrimSpace(r.URL.Query().Get("source"))),
		Level:     strings.TrimSpace(r.URL.Query().Get("level")),
		SessionID: strings.TrimSpace(r.URL.Query().Get("session_id")),
		Substring: r.URL.Query().Get("substring"),
		Order:     strings.TrimSpace(r.URL.Query().Get("order")),
	}

	if q.Source == "" {
		q.Source = SourceAll
	}
	// Validate source upfront so unknown values surface as 400, not 500.
	if _, err := resolveSources(q.Source); err != nil {
		return TraceQuery{}, err
	}

	now := time.Now()
	if s := r.URL.Query().Get("since"); s != "" {
		t, err := ParseTraceDurationOrTime(s, now)
		if err != nil {
			return TraceQuery{}, fmt.Errorf("since: %w", err)
		}
		q.Since = t
	}
	if s := r.URL.Query().Get("until"); s != "" {
		t, err := ParseTraceDurationOrTime(s, now)
		if err != nil {
			return TraceQuery{}, fmt.Errorf("until: %w", err)
		}
		q.Until = t
	}
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return TraceQuery{}, fmt.Errorf("limit: expected non-negative integer, got %q", s)
		}
		if n > maxTracesLimit {
			return TraceQuery{}, fmt.Errorf("limit: %d exceeds max %d", n, maxTracesLimit)
		}
		q.Limit = n
	}
	return q, nil
}

// writeTraceError emits a {"error": "..."} JSON body with the given status.
// Matches the existing serve.go convention (handleDebugLast etc.).
func writeTraceError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// handleLightCone returns light cone metadata from the LightConeManager.
// When TRM is loaded, returns real per-conversation light cone states.
// When TRM is not available, returns a placeholder indicating TRM is disabled.
//
//	GET /v1/lightcone
//	200 → { active, count, cones: [...] } or placeholder
func (s *Server) handleLightCone(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	lcm := s.process.LightCones()
	if lcm == nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"active":          false,
			"count":           0,
			"cones":           []LightConeInfo{},
			"layers":          0,
			"layer_norms":     []float64{},
			"compressed_norm": 0.0,
			"note":            "TRM not loaded. Configure trm_weights_path in kernel.yaml to enable.",
		})
		return
	}

	cones := lcm.List()
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"active": len(cones) > 0,
		"count":  len(cones),
		"cones":  cones,
	})
}

// handleConversation returns the conversation turns for a session.
//
//	GET /v1/conversation
//	  ?session_id=…            default: current process session
//	  &after_turn=N            pagination: turn_index > N
//	  &before_turn=N           reverse pagination: turn_index < N
//	  &since=RFC3339           time filter
//	  &limit=20                default 20, max 200
//	  &include_full=true       default true — hydrate from sidecar
//	  &include_tools=true      default true — include tool-call transcript
//	  &order=asc               asc (default) | desc
//
//	200 → ConversationQueryResult
//	400 → parse error
//
// Backed by the turn.completed ledger event stream + the per-session
// sidecar JSONL. Closes Agent F gap #4.
func (s *Server) handleConversation(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sessionID := q.Get("session_id")
	if sessionID == "" && s.process != nil {
		sessionID = s.process.SessionID()
	}
	cq := ConversationQuery{
		SessionID:    sessionID,
		IncludeFull:  parseBoolDefault(q.Get("include_full"), true),
		IncludeTools: parseBoolDefault(q.Get("include_tools"), true),
		Order:        q.Get("order"),
	}
	if v := q.Get("after_turn"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeConversationError(w, http.StatusBadRequest, "invalid after_turn: "+err.Error())
			return
		}
		cq.AfterTurn = n
	}
	if v := q.Get("before_turn"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeConversationError(w, http.StatusBadRequest, "invalid before_turn: "+err.Error())
			return
		}
		cq.BeforeTurn = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeConversationError(w, http.StatusBadRequest, "invalid limit: "+err.Error())
			return
		}
		cq.Limit = n
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeConversationError(w, http.StatusBadRequest, "invalid since (want RFC3339): "+err.Error())
			return
		}
		cq.Since = t
	}

	res, err := QueryConversation(s.cfg.WorkspaceRoot, cq)
	if err != nil {
		writeConversationError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func writeConversationError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func parseBoolDefault(s string, def bool) bool {
	if s == "" {
		return def
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return def
	}
	return b
}

// readLastJSONLEntries reads the last n lines from a JSONL file and returns
// them as a slice of parsed JSON objects. If the file does not exist or is
// empty, it returns an empty slice (never nil).
func readLastJSONLEntries(path string, n int) []json.RawMessage {
	f, err := os.Open(path)
	if err != nil {
		return []json.RawMessage{}
	}
	defer f.Close()

	// Read all lines, keeping the last n. For typical proprioceptive logs
	// (hundreds to low-thousands of entries) this is efficient enough.
	var lines []string
	scanner := bufio.NewScanner(f)
	// Allow up to 1 MB per line to handle large entries.
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}

	// Take the last n lines.
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	entries := make([]json.RawMessage, 0, len(lines))
	for _, line := range lines {
		// Validate that it's valid JSON before including it.
		raw := json.RawMessage(line)
		if json.Valid(raw) {
			entries = append(entries, raw)
		} else {
			slog.Warn("proprioceptive: skipping invalid JSON line", "path", path)
		}
	}
	return entries
}

// sanitizeErrorMessage strips URLs and long alphanumeric strings (potential API
// keys) from an error message before returning it to clients.
var (
	reURL    = regexp.MustCompile(`https?://[^\s"',]+`)
	reAPIKey = regexp.MustCompile(`\b[A-Za-z0-9_\-]{32,}\b`)
)

func sanitizeErrorMessage(msg string) string {
	msg = reURL.ReplaceAllString(msg, "[redacted-url]")
	msg = reAPIKey.ReplaceAllString(msg, "[redacted]")
	return msg
}
