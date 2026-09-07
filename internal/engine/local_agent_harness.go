package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
	"github.com/myrgic/cogos/trace"
)

// dispatchCycleIDKey is the context key that carries the cycle-trace ID minted
// by DispatchToHarness. All trace events emitted within a single dispatch
// invocation (including per-tool events in the tool loop) share this ID, so
// bus consumers can correlate them. Follows the pattern in dashboard_inlet.go.
// ADR-083 (bus cycle-trace path, not OTel), ADR-033, ADR-072.
type dispatchCycleIDKey struct{}

// withDispatchCycleID attaches a cycle-trace ID to the context.
func withDispatchCycleID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, dispatchCycleIDKey{}, id)
}

// dispatchCycleIDFromCtx returns the cycle-trace ID stored in the context, or
// an empty string when none is present (tool-loop calls outside a dispatch).
func dispatchCycleIDFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(dispatchCycleIDKey{}).(string); ok {
		return v
	}
	return ""
}

// defaultHarnessScopeName is the scope used when no scope is requested.
// Callers that don't specify a scope get exactly the tool set they always have.
const defaultHarnessScopeName = "consolidation"

// harnessToolScopes is the named-scope catalog for harness dispatches (RFC-016).
// Each scope is a named set of tool names the harness may use.
//
// "consolidation" — RFC-016 canonical 11 substrate tools: memory, observability,
//
//	identity. respond is intentionally ABSENT from this base scope — callers
//	that need respond must request consolidation_with_respond explicitly.
//	Removing respond from the default aligns with RFC-016's canonical 11.
//
// "consolidation_with_respond" — consolidation + respond tool. Used by the
//
//	autonomic cycle when pending dashboard messages are present.
//
// "consolidation_no_respond" — consolidation without respond. Used by purely
//
//	autonomic ticks (no pending user messages). The model cannot publish to
//	bus_dashboard_response during an autonomic-only cycle.
//
// "audit" — consolidation tools PLUS read-only filesystem access.
//
//	Use when the harness needs to inspect source, configs, or workspace files
//	without mutating anything.
//
// "search" — lightweight lookup micro-scope (RFC-016 §micro-scopes). Resolves
//
//	URIs, searches memory, and queries the field. No read of full CogDocs,
//	no event emission. Suitable for single-tool-call breadth fan-out.
//
// "observe" — consolidation read tools minus cog_emit_event (RFC-016 §micro-scopes).
//
//	Grants full read access to the substrate (CogDocs, state, coherence,
//	nucleus, index, context) without the ability to emit events.
//
// "emit" — single-tool micro-scope: cog_emit_event only (RFC-016 §micro-scopes).
//
//	Suitable for a slot whose sole job is to publish a structured bus event.
//
// Future scopes (add entries here when the underlying mechanisms land):
//
//	"maintenance" — read+write filesystem tools gated behind per-dispatch
//	                worktree isolation (RFC-017, dedicated PR).
//	"introspection" — audit tools plus kernel state-dump tools for deep
//	                  diagnostic cycles.
var harnessToolScopes = map[string][]string{
	// consolidation — RFC-016 canonical 11. respond removed from base scope.
	"consolidation": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_read_cogdoc",
		"cog_query_field",
		"cog_check_coherence",
		"cog_get_state",
		"cog_get_trust",
		"cog_get_nucleus",
		"cog_get_index",
		"cog_assemble_context",
		"cog_emit_event",
	},
	// consolidation_with_respond — consolidation + respond. The autonomic
	// cycle wires backgroundTools from this scope so the model can reply to
	// pending dashboard messages. Callers that need respond must request this
	// scope explicitly; the base "consolidation" scope no longer includes it.
	"consolidation_with_respond": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_read_cogdoc",
		"cog_query_field",
		"cog_check_coherence",
		"cog_get_state",
		"cog_get_trust",
		"cog_get_nucleus",
		"cog_get_index",
		"cog_assemble_context",
		"cog_emit_event",
		engineRespondToolName,
	},
	// consolidation_no_respond — consolidation without respond. Used by purely
	// autonomic ticks (no pending user messages). The model cannot publish to
	// bus_dashboard_response during an autonomic-only cycle.
	"consolidation_no_respond": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_read_cogdoc",
		"cog_query_field",
		"cog_check_coherence",
		"cog_get_state",
		"cog_get_trust",
		"cog_get_nucleus",
		"cog_get_index",
		"cog_assemble_context",
		"cog_emit_event",
	},
	"audit": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_read_cogdoc",
		"cog_query_field",
		"cog_check_coherence",
		"cog_get_state",
		"cog_get_trust",
		"cog_get_nucleus",
		"cog_get_index",
		"cog_assemble_context",
		"cog_emit_event",
		"cog_read_file",
		"cog_grep_files",
	},
	// search — RFC-016 §micro-scopes. Lightweight URI/memory/field lookup only.
	"search": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_query_field",
	},
	// observe — RFC-016 §micro-scopes. Full substrate read access, no event emission.
	"observe": {
		"cog_resolve_uri",
		"cog_search_memory",
		"cog_read_cogdoc",
		"cog_query_field",
		"cog_check_coherence",
		"cog_get_state",
		"cog_get_trust",
		"cog_get_nucleus",
		"cog_get_index",
		"cog_assemble_context",
	},
	// emit — RFC-016 §micro-scopes. Single-purpose: publish a structured bus event.
	"emit": {
		"cog_emit_event",
	},
}

const (
	localHarnessHistoryLimit   = 24
	localHarnessCycleTimeout   = 5 * time.Minute
	localHarnessAssessMaxToks  = 256
	localHarnessExecuteMaxToks = 1024
)

// harnessURIBlock is the canonical CogDoc URI reference injected into every
// harness prompt. Defined once here to prevent drift across prompts (ADR-066
// pointer-discipline: single authoritative source for URI format guidance).
const harnessURIBlock = `
CogDoc URIs use the bare form cog:<type>/<path>. Valid types:
  mem, adr, role, skill, agent, spec, status, ledger, crystal,
  kernel, canonical, config, ontology, work, handoff, artifact, docs, hooks
Examples:
  cog:adr/077                    (ADRs resolve by numeric prefix)
  cog:mem/semantic/foo/bar       (memory under .cog/mem/<sector>/...)
  cog:spec/rfc-022-foo           (specs under .cog/specs/)
Cross-workspace refs use authority form: cog://other-workspace/mem/...
Invalid: cog://adrs/..., cog://docs/... with raw fs paths.
If cog_search_memory returns a bus event path (".cog/.state/buses/.../events.jsonl#N"),
that is a chat log entry, not a readable CogDoc — do not try to read it.`

const localHarnessAssessPrompt = `You are the resident local CogOS maintenance agent.
Operate only through local inference and the kernel's local tools.
Decide whether a maintenance pass is warranted right now.
Return only one compact JSON object with these keys:
{"action":"sleep|observe|consolidate|repair|propose|escalate","reason":"short string","urgency":0..1,"target":"short string","task":"short concrete next step"}
Prefer "sleep" unless the observation names a concrete task worth doing now.`

const localHarnessExecutePrompt = `You are the resident local CogOS maintenance agent.
Stay local-only. Use the provided kernel tools when they materially improve the answer.
Prefer inspection and diagnosis over mutation. Finish with a concise plain-text result.
` + harnessURIBlock

// localHarnessChatPrompt is the system prompt used during the execute phase
// when pending dashboard user messages are present. It replaces the maintenance
// agent framing with a conversational one so the model replies naturally rather
// than narrating its own dispatch discipline.
//
// Key differences from localHarnessExecutePrompt:
//   - Framed as a workspace assistant, not a maintenance agent.
//   - Instructs the model to call `respond` with the actual reply text — not to
//     describe what it is about to do or narrate the tool invocation.
//   - Explicitly prohibits meta-commentary like "I have received your command"
//     or "My highest priority is replying to the user".
const localHarnessChatPrompt = `You are Cog, a workspace assistant for this CogOS node.
The operator has sent you a message via the dashboard chat interface.
Read the user_message in the observation, then call the respond tool with a direct, natural reply.

Rules:
- Call respond exactly once per turn. Put the actual reply text in the "text" field.
- Do NOT narrate your own process. Never say things like "I have received your command",
  "My highest priority is replying", or "I will now invoke the respond tool". Just reply.
- Speak like a person, not a status report. Be conversational and concise.
- You may use other kernel tools (cog_search_memory, cog_read_cogdoc, etc.) before
  calling respond if they would help you answer accurately. Keep it brief.
- If you are uncertain, say so plainly. Do not invent facts about the workspace.
` + harnessURIBlock

const localHarnessDispatchPrompt = `You are the resident local CogOS harness.
Stay local-only. Use only the provided kernel tools. Be concise and finish with a direct answer.
` + harnessURIBlock + `

Output contract (ADR-eigen output-contract, RFC-027 alignment layer): after any
tool calls, finish with a plain-text answer — or call respond exactly once with
the answer. Do NOT narrate tool use, emit role/channel markers, or output special
tokens such as <|...|> or respond{...} scaffolding.`

// buildHarnessOrientationBlock constructs the four-bundle orientation string
// that is prepended to localHarnessDispatchPrompt when no caller-supplied
// SystemPrompt is present (RFC-018 §stateless-approximation, ADR-066
// §pointer-discipline). The four bundles are intentionally pointer-first and
// bounded: no inline file or CogDoc content is embedded here.
//
// Bundle layout:
//  1. Identity  — name + role inline (NOT the full Card).
//  2. Directive — framing that the concrete task arrives in the user message.
//  3. Scope     — the named harness scope; signals which tools are available.
//  4. Substrate — workspace root as a cog:// pointer (no inline content).
func buildHarnessOrientationBlock(name, role, scopeName, workspaceRoot string) string {
	if name == "" {
		name = "local-harness"
	}
	if role == "" {
		role = "CogOS resident agent"
	}
	if scopeName == "" {
		scopeName = defaultHarnessScopeName
	}
	wsPointer := "cog:workspace/root"
	if workspaceRoot != "" {
		wsPointer = "cog:workspace/root (" + workspaceRoot + ")"
	}
	return strings.Join([]string{
		"[orientation]",
		"identity: " + name + " — " + role,
		"directive: your task is in the user message below; complete it using the available tools",
		"scope: " + scopeName,
		"substrate: " + wsPointer,
	}, "\n")
}

type localHarnessAssessment struct {
	Action  string  `json:"action"`
	Reason  string  `json:"reason"`
	Urgency float64 `json:"urgency"`
	Target  string  `json:"target"`
	Task    string  `json:"task"`
}

type localHarnessCycleRecord struct {
	Cycle       int64
	Timestamp   time.Time
	Duration    time.Duration
	Action      string
	Urgency     float64
	Reason      string
	Target      string
	Observation string
	Result      string
	Model       string
}

type localHarnessCycleOutcome struct {
	record   localHarnessCycleRecord
	timedOut bool
}

type LocalHarnessController struct {
	cfg             *Config
	nucleus         *Nucleus
	process         *Process
	toolRegistry    *KernelToolRegistry
	dispatchTools   *KernelToolRegistry
	backgroundTools *KernelToolRegistry
	// backgroundToolsNoRespond is the tool set for purely autonomic cycles
	// (no pending user messages). It is identical to backgroundTools but
	// with the respond tool removed — Gate A enforcement.
	backgroundToolsNoRespond *KernelToolRegistry

	agentID  string
	started  time.Time
	interval time.Duration

	// localProviderTimeout is the HTTP timeout (seconds) applied to providers
	// constructed by buildLocalProvider for this controller's dispatches.
	// Resolved once at construction time from providers(.local).yaml; 0 means
	// "fall back to localProviderDefaultTimeoutSec".
	localProviderTimeout int

	// autonomicCfg holds escalation-predicate tunables. Safe to read from
	// multiple goroutines — written once before Start().
	autonomicCfg AutonomicConfig

	// busSessions is optional; when set, each tick emits a
	// KernelHealthSnapshot to bus_kernel_proprio. Nil is a safe no-op.
	busSessions *BusSessionManager

	// dashboardBus is optional; when set, the harness drains enginePendingMsgs
	// on each runCycle and publishes agent_response events to
	// bus_dashboard_response. Wired via SetDashboardBus after construction.
	dashboardBus *BusSessionManager

	runCtx context.Context

	running atomic.Bool
	stopped atomic.Bool

	cycleSeq   atomic.Int64
	triggerSeq atomic.Int64

	// triggerPending is set to true by TriggerAgent and cleared at the start
	// of each tick's escalation check. Used so an explicit trigger always
	// fires the LLM path even when providers are green.
	triggerPendingMu sync.Mutex
	triggerPending   bool

	startOnce sync.Once

	mu              sync.RWMutex
	lastObservation string
	lastModel       string
	lastCycle       *localHarnessCycleRecord
	lastLLMCycle    time.Time // wall-clock time of the last LLM assess call
	// lastEscalatedFingerprint is the snapshotFingerprint of the snapshot
	// that last triggered an escalateDegradedHealth cycle. Used to suppress
	// repeat escalations on stable degradation: the LLM has already seen
	// this exact picture; only the idle re-checkin window should fire next.
	// Cleared whenever the snapshot returns to AllGreen.
	lastEscalatedFingerprint string
	history                  []localHarnessCycleRecord

	// ollamaMu serializes all local-backend inference calls across both the
	// metabolic cycle (runCycle) and user-initiated dispatches (DispatchToHarness).
	// LM Studio (and previously Ollama) queue concurrent requests; serializing
	// at this layer makes contention explicit and avoids queued-wait hangs.
	// One lock per controller, held for the duration of the inference call.
	ollamaMu sync.Mutex

	// harnessOrientationBlock is the four-bundle orientation string prepended to
	// localHarnessDispatchPrompt when req.SystemPrompt is empty (RFC-018 §stateless
	// approximation, ADR-066 pointer-discipline). Computed once at construction so
	// the content is stable across fan-out slots — the Wave-1 content-keyed
	// RequestID hashes the system prompt, so any per-call variation would defeat
	// KV-cache sharing. Content: identity (name + role inline, NOT the full Card),
	// directive framing, scope sentinel, and workspace pointer (cog:// URI).
	harnessOrientationBlock string

	// mcpSrv is retained (previously the constructor accepted it only to
	// build the tool registry, then dropped it) so dispatchSlot can reach a
	// capability gater wired onto the MCP server *after* this controller was
	// constructed (MCPServer.SetCapabilityResolver runs post-construction —
	// see mcp_sessions_identity.go). Read lazily via capabilityGater() on
	// every dispatch rather than snapshotted once, since the resolver can be
	// wired (or, in tests, swapped) at any point in the process lifetime.
	// Nil-safe: a nil mcpSrv (should not happen — NewLocalHarnessControllerWithScope
	// already requires a non-nil mcpSrv) degrades capabilityGater() to nil,
	// which is permit-by-default same as an unwired resolver.
	mcpSrv *MCPServer
}

// capabilityGater returns the capability gater currently wired onto this
// controller's MCP server, or nil when none is wired (permit-by-default).
// See the mcpSrv field doc for why this is a lazy read rather than a
// construction-time snapshot.
func (c *LocalHarnessController) capabilityGater() capabilityGater {
	if c == nil || c.mcpSrv == nil {
		return nil
	}
	return c.mcpSrv.capResolver
}

func NewLocalHarnessController(cfg *Config, nucleus *Nucleus, process *Process, mcpSrv *MCPServer) (*LocalHarnessController, error) {
	return NewLocalHarnessControllerWithScope(cfg, nucleus, process, mcpSrv, "")
}

// NewLocalHarnessControllerWithScope creates a LocalHarnessController using
// the named harness scope. An empty scopeName selects defaultHarnessScopeName.
// Unknown scope names return an error immediately.
func NewLocalHarnessControllerWithScope(cfg *Config, nucleus *Nucleus, process *Process, mcpSrv *MCPServer, scopeName string) (*LocalHarnessController, error) {
	if mcpSrv == nil {
		return nil, fmt.Errorf("local harness requires MCP server wiring")
	}
	if scopeName == "" {
		scopeName = defaultHarnessScopeName
	}
	toolNames, ok := harnessToolScopes[scopeName]
	if !ok {
		known := make([]string, 0, len(harnessToolScopes))
		for k := range harnessToolScopes {
			known = append(known, k)
		}
		sort.Strings(known)
		return nil, fmt.Errorf("unknown harness scope %q (known: %v)", scopeName, known)
	}
	registry := NewKernelToolRegistry(mcpSrv)
	// Piece 3b: inject the respond native tool into the full registry before
	// scoping. The bus manager is wired later via SetDashboardBus; until then
	// the executor returns errEngineDashboardNotInstalled at invocation time.
	AddRespondTool(registry)
	dispatchTools, err := registry.Scoped(toolNames)
	if err != nil {
		return nil, err
	}

	// Gate A — build cycle-appropriate tool sets from canonical scope entries.
	//
	// backgroundTools is always wired from consolidation_with_respond so the
	// autonomic cycle can call respond when pending dashboard messages are
	// present, regardless of which scope was requested at construction time.
	// This decouples the dispatch scope (what external callers get) from the
	// autonomic cycle's respond access.
	//
	// backgroundToolsNoRespond is the respond-free set for purely autonomic
	// ticks (no pending user messages); the model cannot see the respond tool
	// so it structurally cannot publish to bus_dashboard_response.
	withRespondTools, err := registry.Scoped(harnessToolScopes["consolidation_with_respond"])
	if err != nil {
		return nil, fmt.Errorf("build with-respond tool scope: %w", err)
	}
	noRespondTools, err := registry.Scoped(harnessToolScopes["consolidation_no_respond"])
	if err != nil {
		return nil, fmt.Errorf("build no-respond tool scope: %w", err)
	}

	interval := time.Minute
	if cfg != nil && cfg.HeartbeatInterval > 0 {
		interval = time.Duration(cfg.HeartbeatInterval) * time.Second
	}

	// RFC-018 §stateless-approximation, ADR-066 §pointer-discipline:
	// Compute the four-bundle orientation block once at construction so it is
	// stable across fan-out slots (required for the Wave-1 content-keyed
	// RequestID KV-cache sharing). The four bundles are:
	//   1. Identity  — name + role inline; NOT the full Card.
	//   2. Directive — framing that the task arrives in the user message.
	//   3. Scope     — the named harness scope active for this controller.
	//   4. Substrate — workspace root as a cog:// pointer (no inline content).
	identityName := ""
	identityRole := ""
	wsRoot := ""
	if nucleus != nil {
		identityName = nucleus.Name
		identityRole = nucleus.Role
	}
	if cfg != nil {
		wsRoot = cfg.WorkspaceRoot
	}
	orientationBlock := buildHarnessOrientationBlock(identityName, identityRole, scopeName, wsRoot)

	return &LocalHarnessController{
		cfg:                      cfg,
		nucleus:                  nucleus,
		process:                  process,
		toolRegistry:             registry,
		dispatchTools:            dispatchTools,
		backgroundTools:          withRespondTools,
		backgroundToolsNoRespond: noRespondTools,
		agentID:                  DefaultAgentID,
		started:                  time.Now().UTC(),
		interval:                 interval,
		localProviderTimeout:     resolveLocalProviderTimeout(cfg),
		harnessOrientationBlock:  orientationBlock,
		mcpSrv:                   mcpSrv,
	}, nil
}

// SetBusSessionManager wires in the kernel's bus layer so that each autonomic
// tick emits a KernelHealthSnapshot to bus_kernel_proprio. Optional: nil is
// a safe no-op (snapshots are computed but not persisted).
func (c *LocalHarnessController) SetBusSessionManager(mgr *BusSessionManager) {
	c.busSessions = mgr
}

// SetDashboardBus wires the dashboard chat bridge into the harness.
//
// After this call, each runCycle will:
//  1. Drain enginePendingMsgs (the queue filled by InstallEngineDashboardInlet).
//  2. Enrich the observation with pending user message text.
//  3. Stamp the cycle ctx with session IDs for fan-out respond publishing.
//  4. Fire the ensureUserTurnReply fallback if the agent did not invoke respond.
//
// The respond native tool is already registered on the tool registry at
// construction time (AddRespondTool in NewLocalHarnessControllerWithScope).
// This call simply marks the bus active so runCycle starts draining pending
// messages. Safe to call after construction and before Start().
func (c *LocalHarnessController) SetDashboardBus(mgr *BusSessionManager) {
	if mgr == nil {
		return
	}
	c.dashboardBus = mgr
}

func (c *LocalHarnessController) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		c.runCtx = ctx
		c.stopped.Store(false)
		c.tryStartCycle("startup", 0, nil)
		go c.runTicker(ctx)
	})
}

// runTicker is the autonomic control loop. Each tick:
//
//  1. Probes all Reconcilables (deterministic, near-zero cost).
//  2. Emits a KernelHealthSnapshot to bus_kernel_proprio.
//  3. Evaluates the escalation predicate.
//  4. Only calls tryStartCycle (→ LLM assess+execute) when the predicate fires.
//
// When the registry is empty or all providers are green and no explicit trigger
// is pending and the idle re-checkin window hasn't elapsed, the tick completes
// with zero LLM calls.
func (c *LocalHarnessController) runTicker(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.stopped.Store(true)
			return
		case <-ticker.C:
			c.autonomicTick(ctx)
		}
	}
}

// autonomicTick is the per-tick unit of the autonomic control loop.
// It probes providers, emits a snapshot, and conditionally escalates.
func (c *LocalHarnessController) autonomicTick(ctx context.Context) {
	// 1. Probe all Reconcilables — cheap, deterministic.
	snap := buildKernelHealthSnapshot(ctx)

	// 2. Emit snapshot to bus regardless of health state.
	emitHealthSnapshot(ctx, c.busSessions, snap)

	// 2a. Run deterministic self-heal for any degraded provider that supports
	// the full plan/apply Reconcilable contract (e.g. MLXSupervisedProvider).
	// This runs BEFORE the escalation predicate so that transient crashes are
	// repaired autonomically without waking the LLM. If self-heal succeeds,
	// the next snapshot will be green and no escalation fires.
	if !snap.AllGreen() {
		healDegradedProviders(ctx)
	}

	// 3. Consume the triggerPending flag atomically.
	c.triggerPendingMu.Lock()
	triggerPending := c.triggerPending
	c.triggerPending = false
	c.triggerPendingMu.Unlock()

	// 4. Evaluate escalation predicate.
	c.mu.RLock()
	lastLLM := c.lastLLMCycle
	lastFP := c.lastEscalatedFingerprint
	c.mu.RUnlock()

	// When the snapshot returns to all-green, clear the dedupe fingerprint
	// so the next degradation (even if it's the same shape as before) is
	// treated as a new event the LLM should see.
	if snap.AllGreen() && lastFP != "" {
		c.mu.Lock()
		c.lastEscalatedFingerprint = ""
		c.mu.Unlock()
	}

	reason := shouldEscalate(snap, triggerPending, lastLLM, c.autonomicCfg)

	// Stable-degradation suppression. If the same provider population is in
	// the same non-green buckets as the snapshot that last triggered an LLM
	// cycle, don't keep waking the LLM every minute — the agent has already
	// seen this picture. The 1h idle re-checkin remains the safety valve, so
	// the agent does check back on the same problem periodically. Explicit
	// triggers (TriggerAgent) and out-of-sync (real declared-vs-live drift)
	// bypass dedupe — those are signals the operator wants surfaced.
	if reason == escalateDegradedHealth && lastFP != "" {
		fp := snapshotFingerprint(snap)
		if fp == lastFP {
			window := c.autonomicCfg.idleRecheckIn()
			if !triggerPending && !lastLLM.IsZero() && time.Since(lastLLM) < window {
				slog.Debug("autonomic: stable degradation, suppressing escalation",
					"providers", len(snap.Providers),
					"degraded", snap.Counts.Degraded,
					"missing", snap.Counts.Missing,
					"suspended", snap.Counts.Suspended,
					"fingerprint", fp[:12],
				)
				return
			}
			// Window has elapsed — fall through to escalation. Reframe the
			// reason so the log reflects what's actually happening: the LLM
			// is checking back on a stable problem, not seeing it for the
			// first time.
			reason = escalateIdleRecheckIn
		}
	}

	if reason == "" {
		// All green, no trigger, idle window not elapsed — pure deterministic tick.
		slog.Debug("autonomic: tick complete, no escalation",
			"providers", len(snap.Providers),
			"healthy", snap.Counts.Healthy,
		)
		return
	}

	slog.Info("autonomic: escalating to LLM cycle",
		"reason", string(reason),
		"providers", len(snap.Providers),
		"degraded", snap.Counts.Degraded,
		"missing", snap.Counts.Missing,
		"anomalies", snap.Anomalies,
		"anomalies_total", snap.AnomaliesTotal,
	)
	c.tryStartCycle(string(reason), 0, nil)

	// Record the fingerprint so the next tick can suppress a repeat of the
	// same degradation. Only update on degraded_health escalations: idle
	// recheckins, explicit triggers, and out-of-sync don't represent "the
	// LLM has seen this degradation."
	if reason == escalateDegradedHealth {
		fp := snapshotFingerprint(snap)
		if fp != "" {
			c.mu.Lock()
			c.lastEscalatedFingerprint = fp
			c.mu.Unlock()
		}
	}
}

func (c *LocalHarnessController) tryStartCycle(reason string, triggerSeq int64, waiter chan<- localHarnessCycleOutcome) bool {
	if c.stopped.Load() {
		return false
	}
	if !c.running.CompareAndSwap(false, true) {
		return false
	}
	parent := c.runCtx
	if parent == nil {
		parent = context.Background()
	}
	go c.runCycle(parent, reason, triggerSeq, waiter)
	return true
}

func (c *LocalHarnessController) runCycle(parent context.Context, reason string, triggerSeq int64, waiter chan<- localHarnessCycleOutcome) {
	defer c.running.Store(false)

	ctx, cancel := context.WithTimeout(parent, localHarnessCycleTimeout)
	defer cancel()

	// --- Piece 2: Drain pending dashboard user messages ---
	//
	// Pull any messages that arrived on bus_dashboard_chat since the last cycle.
	// Enrich the observation so the LLM sees them, and stamp ctx with the
	// collected session IDs so the respond tool can fan-out replies correctly.
	var pendingMsgs []EnginePendingUserMsg
	if c.dashboardBus != nil {
		pendingMsgs = DrainEnginePendingUserMessages()
	}

	// Gate A — select cycle-appropriate tool set.
	//
	// When there are no pending user messages this cycle is purely autonomic
	// (health-check, memory consolidation, etc.). In that case, strip the
	// respond tool from the model's visible tool set so it cannot publish to
	// bus_dashboard_response. The model literally cannot see the tool; there
	// is no text-based instruction that could override this.
	//
	// When pending messages are present, use the full background tool set
	// (which includes respond) and reset the per-turn invocation counter so
	// Gate B starts fresh for this turn.
	cycleTools := c.backgroundTools
	if len(pendingMsgs) == 0 {
		if c.backgroundToolsNoRespond != nil {
			cycleTools = c.backgroundToolsNoRespond
		}
	} else {
		// Gate B: reset per-turn counter before each new user-turn execute phase.
		ResetEngineRespondPerTurnCount()
	}

	// Snapshot the respond counter BEFORE the execute phase so we can detect
	// whether the agent called it during this turn.
	respondSnapshot := EngineRespondInvokeSnapshot()

	// Thread session IDs onto ctx for the respond tool's fan-out path.
	if len(pendingMsgs) > 0 {
		ids := make([]string, 0, len(pendingMsgs))
		seen := make(map[string]bool, len(pendingMsgs))
		for _, m := range pendingMsgs {
			if m.SessionID != "" && !seen[m.SessionID] {
				seen[m.SessionID] = true
				ids = append(ids, m.SessionID)
			}
		}
		if len(ids) > 0 {
			ctx = WithSessionIDs(ctx, ids)
		} else if len(pendingMsgs) > 0 && pendingMsgs[0].SessionID != "" {
			ctx = WithSessionID(ctx, pendingMsgs[0].SessionID)
		}
	}

	start := time.Now().UTC()
	record := localHarnessCycleRecord{
		Cycle:     c.cycleSeq.Add(1),
		Timestamp: start,
		Action:    "sleep",
		Reason:    "idle",
	}
	record.Observation = c.buildObservationWithPending(reason, pendingMsgs)

	outcome := localHarnessCycleOutcome{record: record}

	target, err := detectLocalLLMTarget(ctx, "")
	if err != nil {
		outcome.record.Action = "error"
		outcome.record.Reason = err.Error()
		c.finishCycle(outcome.record)
		if waiter != nil {
			waiter <- outcome
		}
		return
	}

	model, _, note := resolveDispatchLocalModel(target.Models, c.localModelHint(), DispatchModelE4B)
	recordPinResolution("assess", c.localModelHint(), model, note)
	if model == "" {
		outcome.record.Action = "error"
		outcome.record.Reason = note
		outcome.record.Model = c.localModelHint()
		c.finishCycle(outcome.record)
		if waiter != nil {
			waiter <- outcome
		}
		return
	}
	outcome.record.Model = model

	// ollamaMu serializes this cycle's inference calls against concurrent
	// DispatchToHarness calls; local LLM servers queue concurrent requests
	// in ways that look like hangs under high concurrency.
	c.ollamaMu.Lock()
	provider := buildLocalProvider(target, model, c.localProviderTimeout)
	assessment, err := c.assessCycle(ctx, provider, outcome.record.Observation)
	if err != nil {
		c.ollamaMu.Unlock()
		outcome.record.Action = "error"
		outcome.record.Reason = err.Error()
		c.finishCycle(outcome.record)
		if waiter != nil {
			waiter <- outcome
		}
		return
	}

	outcome.record.Action = assessment.Action
	outcome.record.Reason = assessment.Reason
	outcome.record.Urgency = clampUrgency(assessment.Urgency)
	outcome.record.Target = assessment.Target
	if note != "" {
		if outcome.record.Reason == "" {
			outcome.record.Reason = note
		} else {
			outcome.record.Reason = outcome.record.Reason + "; " + note
		}
	}

	if assessment.Action != "sleep" {
		// When pending user messages are present, use the chat-tuned system
		// prompt so the model replies conversationally rather than narrating its
		// own dispatch discipline. Pure autonomic cycles use the default prompt.
		execPrompt := ""
		if len(pendingMsgs) > 0 {
			execPrompt = localHarnessChatPrompt
		}
		result, err := c.executeCycleTaskWithPrompt(ctx, provider, assessment, outcome.record.Observation, cycleTools, execPrompt)
		if err != nil {
			outcome.record.Action = "error"
			outcome.record.Reason = err.Error()
		} else {
			outcome.record.Result = result
		}
	}
	c.ollamaMu.Unlock()

	if ctx.Err() == context.DeadlineExceeded {
		outcome.timedOut = true
		if outcome.record.Action == "" || outcome.record.Action == "sleep" {
			outcome.record.Action = "error"
		}
		if outcome.record.Reason == "" {
			outcome.record.Reason = "cycle timeout"
		}
	}

	// --- Piece 3c: ensureUserTurnReply fallback ---
	//
	// If there were pending user messages this cycle AND the agent did not call
	// the respond tool, publish a contextual fallback so Mod³ doesn't wait
	// forever. The fallback text echoes the first ~40 chars of the user's message
	// so it is distinct across different inputs and clearly placeholder rather
	// than a fully-formed reply.
	if len(pendingMsgs) > 0 && !EngineRespondInvokedSince(respondSnapshot) {
		// Build a contextual fallback from the first pending message's text.
		fallbackText := buildContextualFallback(pendingMsgs)

		// Fan out across all session IDs from this turn.
		sessionIDs := sessionIDsFromContext(ctx)
		if len(sessionIDs) == 0 {
			sessionIDs = []string{sessionIDFromContext(ctx)}
		}
		for _, sid := range sessionIDs {
			if _, err := engineRespondPublish(fallbackText, "auto-fallback: model did not invoke respond tool", sid); err != nil {
				slog.Warn("dashboard-inlet: fallback publish failed", "session", sid, "err", err)
			}
		}
		slog.Info("dashboard-inlet: auto-fallback published", "sessions", len(sessionIDs), "text_preview", fallbackText[:min(len(fallbackText), 40)])
	}

	c.finishCycle(outcome.record)
	if waiter != nil {
		waiter <- outcome
	}
}

func (c *LocalHarnessController) finishCycle(record localHarnessCycleRecord) {
	record.Duration = time.Since(record.Timestamp)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastObservation = record.Observation
	c.lastModel = record.Model
	c.lastCycle = &record
	// Record the wall-clock time of this LLM cycle so the idle re-checkin
	// predicate knows when we last ran. Error-state cycles still count —
	// the LLM was called even if it returned an error.
	if record.Action != "" {
		c.lastLLMCycle = record.Timestamp
	}
	c.history = append(c.history, record)
	if len(c.history) > localHarnessHistoryLimit {
		c.history = append([]localHarnessCycleRecord(nil), c.history[len(c.history)-localHarnessHistoryLimit:]...)
	}
}

func (c *LocalHarnessController) assessCycle(ctx context.Context, provider Provider, observation string) (*localHarnessAssessment, error) {
	temp := 0.0
	requestID := fmt.Sprintf("local-harness-assess-%d", time.Now().UnixNano())
	// Cancel-safe (#432): this is the hourly/degraded-health autonomic consult
	// path identified as the "Generated prediction" (non-streaming)
	// zombie-capable class. Route through streaming so a ctx cancel/timeout
	// actually aborts generation server-side instead of running headless.
	// Track the request as in-flight for the duration of the call so a
	// resubmit racing this one is refused (retry dedup, #432 item d).
	if !beginInflightInference(requestID) {
		return nil, fmt.Errorf("local-harness: assess request %s already in flight", requestID)
	}
	defer endInflightInference(requestID)
	resp, err := CompleteCancelSafeIfSupported(ctx, provider, &CompletionRequest{
		SystemPrompt: localHarnessAssessPrompt,
		Messages: []ProviderMessage{
			{Role: "user", Content: observation},
		},
		MaxTokens:   localHarnessAssessMaxToks,
		Temperature: &temp,
		Metadata: RequestMetadata{
			RequestID:   requestID,
			PreferLocal: true,
			Source:      "local-harness",
		},
	})
	if err != nil {
		recordAbandonedInference("local-harness-assess", requestID, err)
		return nil, err
	}

	var assessment localHarnessAssessment
	if err := decodeJSONPayload(resp.Content, &assessment); err != nil {
		return nil, fmt.Errorf("parse assessment: %w", err)
	}
	if strings.TrimSpace(assessment.Action) == "" {
		assessment.Action = "sleep"
	}
	assessment.Action = strings.ToLower(strings.TrimSpace(assessment.Action))
	assessment.Reason = strings.TrimSpace(assessment.Reason)
	assessment.Target = strings.TrimSpace(assessment.Target)
	assessment.Task = strings.TrimSpace(assessment.Task)
	assessment.Urgency = clampUrgency(assessment.Urgency)
	return &assessment, nil
}

func (c *LocalHarnessController) executeCycleTask(ctx context.Context, provider Provider, assessment *localHarnessAssessment, observation string, registry *KernelToolRegistry) (string, error) {
	return c.executeCycleTaskWithPrompt(ctx, provider, assessment, observation, registry, "")
}

// executeCycleTaskWithPrompt is like executeCycleTask but accepts an explicit
// system prompt override. Pass "" to use the default localHarnessExecutePrompt.
// The chat path passes localHarnessChatPrompt when pending user messages are present.
func (c *LocalHarnessController) executeCycleTaskWithPrompt(ctx context.Context, provider Provider, assessment *localHarnessAssessment, observation string, registry *KernelToolRegistry, systemPromptOverride string) (string, error) {
	temp := 0.1
	task := c.buildExecutionTask(assessment, observation)
	sysPrompt := localHarnessExecutePrompt
	if systemPromptOverride != "" {
		sysPrompt = systemPromptOverride
	}
	req := &CompletionRequest{
		SystemPrompt: sysPrompt,
		Messages: []ProviderMessage{
			{Role: "user", Content: task},
		},
		Tools:       registry.Definitions(),
		ToolChoice:  "auto",
		MaxTokens:   localHarnessExecuteMaxToks,
		Temperature: &temp,
		Metadata: RequestMetadata{
			RequestID:   fmt.Sprintf("local-harness-exec-%d", time.Now().UnixNano()),
			PreferLocal: true,
			Source:      "local-harness",
		},
	}
	resp, clientCalls, transcript, err := c.completeWithToolLoop(ctx, provider, req, registry)
	// Structured loop-exit sentinels (ADR-031, ADR-052): surface partial content
	// rather than propagating as a hard error. The sentinel is logged below; this
	// function then returns the partial content with a nil error, so the autonomic
	// cycle proceeds as a soft success (the sentinel reason is not propagated up).
	if err != nil && !errors.Is(err, ErrToolLoopMaxTurns) && !errors.Is(err, ErrToolLoopNoProgress) {
		return "", err
	}
	if err != nil {
		slog.Warn("local harness cycle: structured loop exit",
			"sentinel", err.Error(),
			"tool_calls", len(transcript),
		)
	}
	if len(clientCalls) > 0 {
		slog.Warn("local harness produced unsupported client tool calls", "count", len(clientCalls))
	}
	var content string
	if resp != nil {
		content = strings.TrimSpace(resp.Content)
	}
	if content == "" && len(transcript) > 0 {
		content = summarizeToolTranscript(transcript)
	}
	return content, nil
}

func (c *LocalHarnessController) buildExecutionTask(assessment *localHarnessAssessment, observation string) string {
	var b strings.Builder
	b.WriteString("Observation:\n")
	b.WriteString(observation)
	b.WriteString("\n\nRequested action: ")
	b.WriteString(assessment.Action)
	if assessment.Target != "" {
		b.WriteString("\nTarget: ")
		b.WriteString(assessment.Target)
	}
	if assessment.Reason != "" {
		b.WriteString("\nWhy: ")
		b.WriteString(assessment.Reason)
	}
	if assessment.Task != "" {
		b.WriteString("\nNext step: ")
		b.WriteString(assessment.Task)
	}
	return b.String()
}

// buildObservationWithPending builds the cycle observation string enriched
// with any pending dashboard user messages. When msgs is empty it falls back
// to buildObservation (the standard autonomic observation).
func (c *LocalHarnessController) buildObservationWithPending(triggerReason string, msgs []EnginePendingUserMsg) string {
	base := c.buildObservation(triggerReason)
	if len(msgs) == 0 {
		return base
	}
	var b strings.Builder
	b.WriteString(base)
	fmt.Fprintf(&b, "\npending_user_messages=%d\n", len(msgs))
	for i, m := range msgs {
		fmt.Fprintf(&b, "user_message[%d]: session=%s text=%s\n", i, m.SessionID, m.Text)
	}
	return b.String()
}

func (c *LocalHarnessController) buildObservation(triggerReason string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "time=%s\n", time.Now().UTC().Format(time.RFC3339))
	if triggerReason != "" {
		fmt.Fprintf(&b, "trigger=%s\n", triggerReason)
	}
	if c.cfg != nil {
		fmt.Fprintf(&b, "workspace=%s\n", filepath.Base(c.cfg.WorkspaceRoot))
	}
	if c.nucleus != nil && c.nucleus.Name != "" {
		fmt.Fprintf(&b, "identity=%s\n", c.nucleus.Name)
	}
	if c.process != nil {
		fmt.Fprintf(&b, "process_state=%s\n", c.process.State().String())
		fovea := c.process.Field().Fovea(5)
		if len(fovea) > 0 {
			b.WriteString("field_top:\n")
			for _, item := range fovea {
				fmt.Fprintf(&b, "- %s score=%.3f\n", item.Path, item.Score)
			}
		}
	}

	c.mu.RLock()
	last := c.lastCycle
	c.mu.RUnlock()
	if last != nil {
		fmt.Fprintf(&b, "last_cycle=%s action=%s urgency=%.2f reason=%s\n",
			last.Timestamp.Format(time.RFC3339), last.Action, last.Urgency, last.Reason)
	}
	return b.String()
}

// buildAmbientBlock assembles the "ambient state of self" context block for
// looped kernel-interior dispatch batches (cogdoc 16, drafted 2026-05-11).
// It reuses buildObservation's gathering half — time/workspace/identity/
// process_state/fovea/last_cycle — the same per-tick ambient assembler the
// autonomic loop already builds for itself, and layers on a kernel health
// summary from buildKernelHealthSnapshot (autonomic_ticker.go).
//
// This gives bus_kernel_proprio's KernelHealthSnapshot its first reader:
// that channel has been write-only since birth (a 60s writer with zero
// readers). Sharing the snapshot source here does not attach a reader to the
// bus itself, but it does mean the same live-health computation now backs
// both the write-only bus emission and this in-loop context injection.
//
// DispatchToHarness calls this once per batch (not once per fan-out slot) —
// the ambient state does not vary within a single dispatch call, and probing
// provider health per-slot would be wasted work under N>1 fan-out.
func (c *LocalHarnessController) buildAmbientBlock(ctx context.Context) string {
	var b strings.Builder
	b.WriteString("=== ambient state of self ===\n")
	b.WriteString(c.buildObservation(""))

	// Use the non-consuming peek form: this is a concurrent, informational
	// caller relative to the autonomic ticker's own buildKernelHealthSnapshot
	// call (local_agent_harness.go's autonomicTick, above). Using the
	// consuming form here would swap-and-reset the #432 abandoned-inference
	// watermark out from under the ticker, silently suppressing
	// escalateAbandonedInference on its next tick. See
	// buildKernelHealthSnapshotPeek and abandonedInferencePeek for the full
	// rationale.
	snap := buildKernelHealthSnapshotPeek(ctx)
	fmt.Fprintf(&b, "health_counts: healthy=%d degraded=%d missing=%d suspended=%d anomalies=%d\n",
		snap.Counts.Healthy, snap.Counts.Degraded, snap.Counts.Missing, snap.Counts.Suspended, snap.Anomalies)
	if !snap.AllGreen() && len(snap.Providers) > 0 {
		unhealthy := make([]string, 0, len(snap.Providers))
		for name, st := range snap.Providers {
			if st.Health != reconcile.HealthHealthy && st.Health != "" {
				unhealthy = append(unhealthy, fmt.Sprintf("%s=%s", name, st.Health))
			}
		}
		if len(unhealthy) > 0 {
			sort.Strings(unhealthy)
			fmt.Fprintf(&b, "degraded_providers: %s\n", strings.Join(unhealthy, ", "))
		}
	}
	b.WriteString("=== end ambient state ===")
	return b.String()
}

func (c *LocalHarnessController) localModelHint() string {
	if c.cfg != nil && strings.TrimSpace(c.cfg.LocalModel) != "" {
		return strings.TrimSpace(c.cfg.LocalModel)
	}
	// No LocalModel configured. Return empty string so resolvePreferredLocalModel
	// picks the first model advertised by the server (LM Studio default loaded model).
	// Previously this fell back to defaultOllamaModel ("gemma4:e4b"); Ollama is
	// decommissioned and that constant is removed.
	return ""
}

// buildContextualFallback produces a short placeholder text for the
// ensureUserTurnReply fallback path. It echoes the first ~40 chars of the
// first pending user message so the operator sees something distinct per input
// rather than an identical canned string on every unanswered turn.
//
// The text is clearly a placeholder, not a fully-formed reply. If the pending
// queue is empty (should not happen at the call site, but defensive), it
// returns a generic acknowledgment.
func buildContextualFallback(msgs []EnginePendingUserMsg) string {
	if len(msgs) == 0 {
		return "Received your message. Working on it..."
	}
	text := strings.TrimSpace(msgs[0].Text)
	if text == "" {
		return "Received your message. Working on it..."
	}
	const maxPreview = 40
	preview := text
	ellipsis := ""
	if len([]rune(text)) > maxPreview {
		runes := []rune(text)
		preview = string(runes[:maxPreview])
		ellipsis = "..."
	}
	return fmt.Sprintf("Got: %q%s — working on it.", preview, ellipsis)
}

// resolveProviderByProcessState consults process_state_routing in the merged
// providers config and returns the configured provider name for the current
// process state. Returns ("", false) when no process is attached, no state
// routing applies, or the config cannot be loaded.
//
// This implements Path 2 in DispatchToHarness: harness dispatches without an
// explicit req.Provider consult the same routing table as the SimpleRouter,
// so the autonomic loop honours the operator's per-state provider preferences
// (e.g., receptive -> mlx-lm, active -> claude-code).
func (c *LocalHarnessController) resolveProviderByProcessState() (string, bool) {
	if c.process == nil {
		return "", false
	}
	state := c.process.State().String()
	// "unknown" is the default-case sentinel from ProcessState.String(); treat
	// it the same as empty string so an unrecognised state falls back to the
	// legacy local-LLM path rather than routing to process_state_routing["unknown"].
	if state == "" || state == "unknown" {
		return "", false
	}
	pcfg, err := loadProvidersConfig(c.cfg)
	if err != nil {
		return "", false
	}
	name, ok := pcfg.Routing.ProcessStateRouting[state]
	if !ok || name == "" {
		return "", false
	}
	return name, true
}

func (c *LocalHarnessController) summary() AgentSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s := AgentSummary{
		AgentID:   c.agentID,
		Alive:     !c.stopped.Load(),
		Running:   c.running.Load(),
		UptimeSec: int64(time.Since(c.started).Seconds()),
		Model:     c.lastModel,
		Interval:  c.interval.String(),
	}
	if c.nucleus != nil {
		s.Identity = c.nucleus.Name
	}
	if c.lastCycle != nil {
		s.CycleCount = c.lastCycle.Cycle
		s.LastAction = c.lastCycle.Action
		s.LastCycle = c.lastCycle.Timestamp.Format(time.RFC3339)
		s.LastUrgency = c.lastCycle.Urgency
		s.LastReason = c.lastCycle.Reason
		s.LastDurMs = c.lastCycle.Duration.Milliseconds()
	}
	if s.Model == "" {
		s.Model = c.localModelHint()
	}
	return s
}

func (c *LocalHarnessController) ListAgents(_ context.Context, _ bool) ([]AgentSummary, error) {
	if c.stopped.Load() {
		return nil, ErrAgentUnavailable
	}
	return []AgentSummary{c.summary()}, nil
}

func (c *LocalHarnessController) GetAgent(_ context.Context, id string, includeTrace bool, traceLimit int) (*AgentSnapshot, error) {
	if id != c.agentID {
		return nil, ErrAgentNotFound
	}
	if c.stopped.Load() {
		return nil, ErrAgentUnavailable
	}

	snap := &AgentSnapshot{
		Summary: c.summary(),
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	snap.LastObservation = c.lastObservation
	if c.nucleus != nil {
		snap.IdentityRef = c.nucleus.Name
	}
	snap.Memory = make([]AgentMemoryEntry, 0, len(c.history))
	for i := len(c.history) - 1; i >= 0; i-- {
		rec := c.history[i]
		snap.Memory = append(snap.Memory, AgentMemoryEntry{
			Cycle:    rec.Cycle,
			Action:   rec.Action,
			Urgency:  rec.Urgency,
			Sentence: summarizeMemoryEntry(rec),
			Ago:      sinceString(rec.Timestamp),
		})
	}
	if includeTrace {
		start := 0
		if traceLimit > 0 && len(c.history) > traceLimit {
			start = len(c.history) - traceLimit
		}
		for _, rec := range c.history[start:] {
			snap.Traces = append(snap.Traces, AgentCycleTrace{
				Cycle:       rec.Cycle,
				Timestamp:   rec.Timestamp.Format(time.RFC3339),
				DurationMs:  rec.Duration.Milliseconds(),
				Action:      rec.Action,
				Urgency:     rec.Urgency,
				Reason:      rec.Reason,
				Target:      rec.Target,
				Observation: rec.Observation,
				Result:      rec.Result,
			})
		}
	}
	return snap, nil
}

func (c *LocalHarnessController) TriggerAgent(ctx context.Context, id string, reason string, wait bool) (*AgentTriggerResult, error) {
	if id != c.agentID {
		return nil, ErrAgentNotFound
	}
	if c.stopped.Load() {
		return nil, ErrAgentUnavailable
	}

	seq := c.triggerSeq.Add(1)
	if !wait {
		if !c.tryStartCycle(reason, seq, nil) {
			// Cycle is already running; set triggerPending so the next
			// autonomic tick picks this up even if providers are green.
			c.triggerPendingMu.Lock()
			c.triggerPending = true
			c.triggerPendingMu.Unlock()
			return &AgentTriggerResult{
				Triggered:  false,
				AgentID:    id,
				TriggerSeq: seq,
				Message:    "already_running",
			}, nil
		}
		return &AgentTriggerResult{
			Triggered:  true,
			AgentID:    id,
			TriggerSeq: seq,
			Message:    "triggered",
		}, nil
	}

	waiter := make(chan localHarnessCycleOutcome, 1)
	if !c.tryStartCycle(reason, seq, waiter) {
		// Cycle is already running; set triggerPending so the next
		// autonomic tick picks this up.
		c.triggerPendingMu.Lock()
		c.triggerPending = true
		c.triggerPendingMu.Unlock()
		return &AgentTriggerResult{
			Triggered:  false,
			AgentID:    id,
			TriggerSeq: seq,
			Message:    "already_running",
		}, nil
	}

	select {
	case outcome := <-waiter:
		return &AgentTriggerResult{
			Triggered:  true,
			AgentID:    id,
			CycleID:    fmt.Sprintf("%s-%d", c.agentID, outcome.record.Cycle),
			TriggerSeq: seq,
			Message:    "completed",
			Action:     outcome.record.Action,
			Urgency:    outcome.record.Urgency,
			Reason:     outcome.record.Reason,
			DurationMs: outcome.record.Duration.Milliseconds(),
			TimedOut:   outcome.timedOut,
		}, nil
	case <-ctx.Done():
		return &AgentTriggerResult{
			Triggered:  true,
			AgentID:    id,
			TriggerSeq: seq,
			Message:    "triggered",
			TimedOut:   true,
		}, nil
	}
}

func (c *LocalHarnessController) DispatchToHarness(ctx context.Context, req DispatchRequest) (*DispatchBatchResult, error) {
	if c.stopped.Load() {
		return nil, ErrAgentUnavailable
	}
	if req.AgentID != "" && req.AgentID != c.agentID {
		return nil, ErrAgentNotFound
	}
	// The timeout cap is the executing node's policy: re-stamp from this
	// node's config regardless of what the transport (MCP/HTTP/BEP wire)
	// carried, so a remote sender's looser cap can't override local config.
	// DispatchTimeoutCap is nil-receiver-safe (default 600).
	req.TimeoutCapSeconds = c.cfg.DispatchTimeoutCap()
	if err := req.Normalize(); err != nil {
		return nil, err
	}

	// Resolve the inference provider. Four paths, evaluated in order:
	//   0. Model-string resolution (NEW, additive): when req.Provider is empty
	//      and req.Model resolves via the shared alias table to a known managed
	//      provider (e.g. "claude-opus-4-7", "deliberation", "foreground"),
	//      dispatch there with the resolved model override. Explicit-model-wins
	//      over process_state_routing, mirroring req.Provider's precedence over
	//      state-routing. See resolve.go / ResolveModelRequest.
	//      Preserves: "e4b"/"26b" fall through (not in alias table); "" falls
	//      through; process_state_routing fires when no model is given.
	//   1. Explicit named provider via req.Provider (RFC-0007 Layer 1):
	//      look the name up in providers.yaml + providers.local.yaml, build
	//      the matching Provider, and use its declared model.
	//   2. Process-state routing: when req.Provider is empty and the
	//      controller has an associated process, consult process_state_routing
	//      in providers config. If the current state maps to a configured
	//      provider, dispatch there. This wires the autonomic loop and harness
	//      dispatches through the same routing table as the main router.
	//   2.5. harness_provider config default (e.g. "lmstudio-darkstar").
	//   3. Legacy local-LLM probe: probes the configured endpoint (default
	//      openaiCompatDefaultEndpoint = 127.0.0.1:1234) for an OpenAI-compat
	//      server. Ollama is no longer the default; the probe order in
	//      detectLocalLLMTarget now tries OpenAI-compat first.
	//
	// Unknown provider names error fast — never silently fall through to
	// the legacy path because that would mask a config typo.
	var provider Provider
	var model string
	var routeUsed DispatchModel
	// note is a batch-level diagnostic string (e.g. state-routing path taken).
	// slotNote carries per-slot warnings that must appear on each DispatchResult
	// — distinct from note so state-routing diagnostics are not incorrectly
	// surfaced in slot Error fields.
	var note string
	var slotNote string
	var err error
	if req.Provider != "" {
		pcfg, perr := loadProvidersConfig(c.cfg)
		if perr != nil {
			return nil, &AgentControllerError{
				Code:    "internal_error",
				Message: fmt.Sprintf("failed to load providers config: %v", perr),
			}
		}
		pc, ok := pcfg.Providers[req.Provider]
		if !ok {
			known := make([]string, 0, len(pcfg.Providers))
			for k := range pcfg.Providers {
				known = append(known, k)
			}
			return nil, &AgentControllerError{
				Code:    "invalid_input",
				Message: fmt.Sprintf("provider %q is not configured (known: %v)", req.Provider, known),
			}
		}
		if !pc.IsEnabled() {
			return nil, &AgentControllerError{
				Code:    "invalid_input",
				Message: fmt.Sprintf("provider %q is disabled in config", req.Provider),
			}
		}
		p, merr := makeProvider(req.Provider, pc, nil)
		if merr != nil {
			return nil, &AgentControllerError{
				Code:    "internal_error",
				Message: fmt.Sprintf("failed to construct provider %q: %v", req.Provider, merr),
			}
		}
		provider = p
		// Per issue #430: an explicit caller-requested model must win over
		// the provider config's declared model. The config's model is the
		// default when the caller specifies none. This does not touch
		// issue #420's provider-precedence semantics — req.Provider still
		// resolves the same way; only the *model* selection within that
		// resolved provider now prefers the caller's explicit request.
		if req.RequestedModel != "" {
			model = req.RequestedModel
			note = fmt.Sprintf("explicit-model: provider=%s model=%s (config default %s overridden)", req.Provider, req.RequestedModel, pc.Model)
		} else {
			model = pc.Model
		}
		// routeUsed stays empty; ProviderUsed on each slot is the canonical
		// signal that the named-provider path fired. ServedModel (set from
		// the provider response's ProviderMeta.Model) is the canonical
		// signal for which model actually served.
	} else {
		// Path 0: model-string resolution via shared alias table (ADDITIVE).
		// When req.Model is a recognised intent alias or model id (e.g.
		// "claude-opus-4-7", "deliberation", "foreground", "opus"), build the
		// resolved managed provider directly. Uses ResolveModelRequest with a
		// nil router so only the static alias table is consulted — no live probe,
		// no I/O. Existing values "e4b", "26b", and "" are NOT in the alias
		// table and fall through to Paths 2/3 unchanged.
		//
		// Precedence decision: explicit-named-model wins over process_state_routing,
		// mirroring how explicit req.Provider wins over state routing. A caller
		// that names "claude-opus-4-7" has expressed intent that is more specific
		// than the autonomic loop's state-based preference.
		usedModelRoute := false
		if mres := ResolveModelRequest(nil, string(req.Model), ""); mres.PreferProvider != "" {
			pcfg, merr := loadProvidersConfig(c.cfg)
			if merr == nil {
				if pc, pok := pcfg.Providers[mres.PreferProvider]; pok && pc.IsEnabled() {
					p, perr := makeProvider(mres.PreferProvider, pc, nil)
					if perr != nil {
						return nil, &AgentControllerError{
							Code:    "internal_error",
							Message: fmt.Sprintf("model-routing: failed to construct provider %q: %v", mres.PreferProvider, perr),
						}
					}
					provider = p
					// Use the resolved model override when present; fall back to
					// the provider's configured model.
					if mres.ModelOverride != "" {
						model = mres.ModelOverride
					} else {
						model = pc.Model
					}
					req.Provider = mres.PreferProvider
					note = fmt.Sprintf("model-routing: model=%s -> provider=%s override=%s", req.Model, mres.PreferProvider, mres.ModelOverride)
					usedModelRoute = true
				}
				// If provider not found or disabled in this node's config: fall
				// through to process_state_routing / legacy path.
			}
		}
		if !usedModelRoute {
			// Path 2: process-state routing — try before falling back to legacy.
			// When the controller has a process with a known state, consult
			// process_state_routing in providers config. If the state maps to a
			// valid, enabled provider, dispatch there. Otherwise fall through to
			// the legacy local-LLM probe (Path 3).
			usedStateRoute := false
			if stateProvider, stateOK := c.resolveProviderByProcessState(); stateOK {
				pcfg, perr := loadProvidersConfig(c.cfg)
				if perr == nil {
					if pc, pok := pcfg.Providers[stateProvider]; pok && pc.IsEnabled() {
						p, merr := makeProvider(stateProvider, pc, nil)
						if merr != nil {
							return nil, &AgentControllerError{
								Code:    "internal_error",
								Message: fmt.Sprintf("state-routing: failed to construct provider %q: %v", stateProvider, merr),
							}
						}
						provider = p
						// Per issue #430: an explicit caller model wins over the
						// config default here too — state-routing only chooses
						// the provider, not the model.
						if req.RequestedModel != "" {
							model = req.RequestedModel
							note = fmt.Sprintf("state-routing: state=%s -> provider=%s model=%s (config default %s overridden)", c.process.State().String(), stateProvider, req.RequestedModel, pc.Model)
						} else {
							model = pc.Model
							note = fmt.Sprintf("state-routing: state=%s -> provider=%s", c.process.State().String(), stateProvider)
						}
						// Populate req.Provider so dispatchSlot records the
						// resolved provider name in ProviderUsed — otherwise it
						// stays empty and the caller can't distinguish this path
						// from the legacy local-LLM probe path.
						req.Provider = stateProvider
						usedStateRoute = true
					}
					// If provider not found or disabled: fall through to legacy path silently.
				}
			}
			// Path 2.5: configured harness_provider default. When no earlier
			// path selected a provider and cfg.HarnessProvider names a provider,
			// resolve it the same way as the explicit-provider Path 1 instead of
			// probing Ollama. This is the EXECUTING node's config, so a
			// BEP-received remote dispatch uses the target node's harness_provider
			// (e.g. eclipse -> lmstudio), not the sender's. Takes precedence over
			// the legacy local_model + detectLocalLLMTarget probe (Path 3) but
			// stays below explicit req.Provider, model-alias routing (Path 0), and
			// process-state routing (Path 2) per the field's documented intent.
			usedHarnessProvider := false
			if !usedStateRoute && c.cfg != nil && c.cfg.HarnessProvider != "" {
				hp := c.cfg.HarnessProvider
				pcfg, perr := loadProvidersConfig(c.cfg)
				if perr != nil {
					return nil, &AgentControllerError{
						Code:    "internal_error",
						Message: fmt.Sprintf("harness_provider: failed to load providers config: %v", perr),
					}
				}
				pc, ok := pcfg.Providers[hp]
				if !ok {
					known := make([]string, 0, len(pcfg.Providers))
					for k := range pcfg.Providers {
						known = append(known, k)
					}
					return nil, &AgentControllerError{
						Code:    "invalid_input",
						Message: fmt.Sprintf("harness_provider %q is not configured (known: %v)", hp, known),
					}
				}
				if !pc.IsEnabled() {
					return nil, &AgentControllerError{
						Code:    "invalid_input",
						Message: fmt.Sprintf("harness_provider %q is disabled in config", hp),
					}
				}
				p, merr := makeProvider(hp, pc, nil)
				if merr != nil {
					return nil, &AgentControllerError{
						Code:    "internal_error",
						Message: fmt.Sprintf("harness_provider: failed to construct provider %q: %v", hp, merr),
					}
				}
				provider = p
				// Per issue #430: honor an explicit caller model even when the
				// provider itself was resolved from the harness_provider config
				// default. The config's model remains the default when the
				// caller specified none.
				if req.RequestedModel != "" {
					model = req.RequestedModel
					note = fmt.Sprintf("harness-provider: provider=%s model=%s (config default %s overridden)", hp, req.RequestedModel, pc.Model)
				} else {
					model = pc.Model
					note = fmt.Sprintf("harness-provider: provider=%s", hp)
				}
				// Populate req.Provider so dispatchSlot records the resolved
				// provider name in ProviderUsed.
				req.Provider = hp
				usedHarnessProvider = true
			}
			if !usedStateRoute && !usedHarnessProvider {
				// Path 3: legacy model-enum routing via local-LLM probe.
				target, terr := detectLocalLLMTarget(ctx, "")
				if terr != nil {
					return nil, terr
				}
				m, ru, n := resolveDispatchLocalModel(target.Models, c.localModelHint(), req.Model)
				recordPinResolution("dispatch", c.localModelHint(), m, n)
				if m == "" {
					return nil, errors.New(n)
				}
				model, routeUsed = m, ru
				// Routing warnings (e.g. "26b route unavailable, using preferred local model")
				// are per-slot: each slot's result must carry the warning so callers
				// that inspect individual slots know the requested model wasn't honored.
				// These are distinct from batch-level diagnostics (state-routing note)
				// which go into batch.Notes via the outer `note` variable.
				slotNote = n
				provider = buildLocalProvider(target, model, c.localProviderTimeout)
			}
		} // end if !usedModelRoute
	}

	// Resolve the named scope. Empty scope means the harness's own default
	// scope (c.dispatchTools, already scoped at construction time). A
	// non-empty scope is resolved from the catalog; unknown names are
	// rejected here so the error is immediate and clear.
	baseRegistry := c.dispatchTools
	if req.Scope != "" {
		scopeTools, ok := harnessToolScopes[req.Scope]
		if !ok {
			known := make([]string, 0, len(harnessToolScopes))
			for k := range harnessToolScopes {
				known = append(known, k)
			}
			return nil, &AgentControllerError{
				Code:    "invalid_input",
				Message: fmt.Sprintf("unknown harness scope %q (known: %v)", req.Scope, known),
			}
		}
		baseRegistry, err = c.toolRegistry.Scoped(scopeTools)
		if err != nil {
			return nil, err
		}
	}
	registry := baseRegistry
	if len(req.Tools) > 0 {
		registry, err = baseRegistry.Scoped(req.Tools)
		if err != nil {
			return nil, err
		}
	}

	// ollamaMu serializes dispatch inference calls against concurrent metabolic
	// cycles for all local-inference backends. Local backends (LM Studio,
	// OpenAI-compat mlx-lm/vllm, mlx-supervised, pi) compete for the same
	// on-device accelerator/VRAM; a dispatch arriving while runCycle is
	// mid-stream may load a second model copy into VRAM or cause memory pressure.
	//
	// The gate is keyed on provider.Capabilities().IsLocal so it covers all
	// local backends uniformly. Remote providers (anthropic, etc.) are excluded
	// — they have no shared hardware resource with the local metabolic cycle.
	if provider.Capabilities().IsLocal {
		c.ollamaMu.Lock()
		defer c.ollamaMu.Unlock()
	}

	batch := &DispatchBatchResult{
		Results: make([]DispatchResult, req.N),
	}
	if note != "" {
		batch.Notes = append(batch.Notes, note)
	}

	// ADR-083 (bus cycle-trace), ADR-033, ADR-072: mint one cycle-trace ID per
	// DispatchToHarness invocation so that all tool-dispatch events from every
	// fan-out slot, plus the batch-level start/end ledger entries, share a
	// correlating ID. Consumers on the kernel bus reconstruct a full dispatch
	// audit trail by filtering on this ID.
	cycleID := uuid.NewString()
	ctx = withDispatchCycleID(ctx, cycleID)

	// RFC-identity-embedding I1/I2: resolve the caller subject for honest
	// attribution in both bound and anonymous states. Capability gating on
	// this subject happens later, in dispatchSlot, behind
	// cfg.IdentityNakedDefault (see dispatchSlot's own comment for the
	// three-part gate) — subject resolution here is just attribution.
	subject := req.Identity.Sub
	if subject == "" {
		subject = "anonymous"
	}

	// Emit dispatch-start ledger entry (ADR-033, ADR-072).
	_ = EmitLedgerEvent(c.cfg, map[string]any{
		"type":   "harness.dispatch.start",
		"source": "local-harness",
		"payload": map[string]any{
			"cycle_id":    cycleID,
			"n":           req.N,
			"task":        truncateDigest(req.Task),
			"provider":    req.Provider,
			"attribution": subject,
		},
	})

	// start begins the batch timer BEFORE the ambient-state block (below) is
	// built. buildAmbientBlock -> buildKernelHealthSnapshotPeek ->
	// probeAllProviders performs a real, sequential per-provider Health()
	// probe (context_blocks_health.go: up to healthProbeTimeout==200ms per
	// provider, so worst case N_providers x 200ms of wall time) — confirmed
	// non-trivial I/O, not free work. Starting the timer first ensures
	// TotalDurationSec reflects the batch's true wall-clock cost for
	// Ambient=true callers instead of undercounting by the ambient-probe
	// latency.
	start := time.Now()

	// req.Ambient (opt-in, default false — existing callers unaffected): build
	// the ambient-state-of-self block once per batch, not once per fan-out
	// slot. Health-snapshot probing is real work (probeAllProviders) and the
	// ambient state does not vary within a single DispatchToHarness call, so
	// computing it once and sharing it across all N slots is both cheaper and
	// correct — every slot in the batch observes the same instant.
	var ambientBlock string
	if req.Ambient {
		ambientBlock = c.buildAmbientBlock(ctx)
	}

	var wg sync.WaitGroup
	for i := 0; i < req.N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			batch.Results[idx] = c.dispatchSlot(ctx, provider, registry, model, routeUsed, req, idx, slotNote, subject, ambientBlock)
		}(i)
	}
	wg.Wait()
	batch.TotalDurationSec = time.Since(start).Seconds()

	// Emit dispatch-end ledger entry (ADR-033, ADR-072).
	_ = EmitLedgerEvent(c.cfg, map[string]any{
		"type":   "harness.dispatch.end",
		"source": "local-harness",
		"payload": map[string]any{
			"cycle_id":         cycleID,
			"n":                req.N,
			"total_duration_s": batch.TotalDurationSec,
			"attribution":      subject,
		},
	})

	return batch, nil
}

// dispatchSlot executes one fan-out slot. subject is the resolved dispatch
// identity (RFC-identity-embedding I1/I2): "anonymous" or req.Identity.Sub.
// ambientBlock is the (possibly empty) ambient-state-of-self block computed
// once per batch by DispatchToHarness when req.Ambient is set; empty when
// ambient injection is off (the default), leaving existing callers
// byte-identical.
func (c *LocalHarnessController) dispatchSlot(parent context.Context, provider Provider, registry *KernelToolRegistry, model string, routeUsed DispatchModel, req DispatchRequest, idx int, slotNote string, subject string, ambientBlock string) DispatchResult {
	res := DispatchResult{
		Index:        idx,
		ModelUsed:    routeUsed,
		ProviderUsed: req.Provider,
	}
	slotCtx, cancel := context.WithTimeout(parent, time.Duration(req.TimeoutSeconds)*time.Second)
	defer cancel()

	systemPrompt := strings.TrimSpace(req.SystemPrompt)
	if systemPrompt == "" {
		// RFC-018 §stateless-approximation, ADR-066 §pointer-discipline:
		// Compose the four-bundle orientation block (computed once at controller
		// construction) with the base dispatch prompt. The orientation block is
		// prefix-stable across fan-out slots so KV-cache sharing is preserved.
		// When req.SystemPrompt is set the caller owns the prompt; use it verbatim.
		systemPrompt = c.harnessOrientationBlock + "\n\n" + localHarnessDispatchPrompt
	}

	counting := &countingProvider{Provider: provider}

	// ADR-066 §models-always-swappable: temperature and max_tokens must be
	// caller-overridable. Default to the harness constants when not set.
	temp := 0.1
	if req.Temperature != nil {
		temp = *req.Temperature
	}
	maxToks := localHarnessExecuteMaxToks
	if req.MaxTokens > 0 {
		maxToks = req.MaxTokens
	}

	// ADR-066 §KV-Cache-Branching: RequestID must be content-stable so that
	// identical fan-out slots share a KV-cache prefix, AND so that a retried
	// identical dispatch dedupes against the in-flight registry (#432 retry
	// discipline, inference_inflight.go beginInflightInference). Hash the
	// BASE systemPrompt (pre-ambient) + task + sorted tool names; append idx
	// only for per-slot log uniqueness — the prefix up to the dash is
	// cache-relevant and identical across slots.
	//
	// Deliberately hashed BEFORE the ambient-state block (below) is prepended:
	// buildAmbientBlock embeds a live time=<RFC3339> line and fluctuating
	// health data, so two otherwise-identical retries of the same task would
	// hash differently if ambient content were included, producing distinct
	// RequestIDs and silently defeating the dedup guard for every
	// req.Ambient=true caller. The ambient block affects what the provider
	// sees, not the request's identity.
	//
	// registry.Definitions() returns the registry's own backing slice by
	// reference (tool_loop.go KernelToolRegistry.Definitions), and registry
	// here is c.dispatchTools — constructed once at controller-construction
	// time and shared across every fan-out slot and every subsequent
	// dispatch. filterToolsByCapability below compacts creq.Tools IN PLACE
	// (serve_kernel_agent_tools.go: `out := creq.Tools[:0]`); handing it the
	// live registry slice would let one gated dispatch permanently truncate
	// the shared registry for every later dispatch, gated or not, and races
	// under concurrent fan-out (N>1). Copy into a fresh slice first — the
	// same "allocate fresh slices so we don't accidentally alias" discipline
	// injectKernelAgentTools already applies to the chat path.
	tools := make([]ToolDefinition, len(registry.Definitions()))
	copy(tools, registry.Definitions())
	toolNames := make([]string, len(tools))
	for i, t := range tools {
		toolNames[i] = t.Name
	}
	sort.Strings(toolNames)
	h := sha256.New()
	h.Write([]byte(systemPrompt))
	h.Write([]byte(req.Task))
	for _, n := range toolNames {
		h.Write([]byte(n))
	}
	contentKey := hex.EncodeToString(h.Sum(nil))[:16]

	// Ambient-state injection (cogdoc 16), opt-in via req.Ambient. Prepended
	// ahead of the resolved system prompt — the same AdditionalContext +
	// "\n\n" + SystemPrompt pattern the PreInference hook uses on the main
	// chat/external-CLI path (harness/harness.go RunInference). That path is
	// wired to the chat harness only; this is the same sandwich applied at
	// the seam LocalHarnessController.DispatchToHarness never had one.
	//
	// Applied AFTER contentKey is computed (above) — see the comment there:
	// the ambient block is live/call-varying content that must reach the
	// provider without becoming part of the request's content-stable
	// identity.
	if ambientBlock != "" {
		systemPrompt = ambientBlock + "\n\n" + systemPrompt
	}

	compReq := &CompletionRequest{
		SystemPrompt: systemPrompt,
		Messages: []ProviderMessage{
			{Role: "user", Content: strings.TrimSpace(req.Task)},
		},
		Tools:         tools,
		ToolChoice:    "auto",
		MaxTokens:     maxToks,
		Temperature:   &temp,
		ModelOverride: model,
		Metadata: RequestMetadata{
			RequestID:      fmt.Sprintf("local-harness-dispatch-%s-%d", contentKey, idx),
			PreferLocal:    true,
			PreferProvider: req.Provider,
			Source:         "local-harness-dispatch",
			// RFC-identity-embedding I1/I2: carry attribution through to
			// provider adapters so the ledger InferenceEvent can record it.
			Attribution: subject,
		},
	}

	// Capability gating (RFC-identity-embedding, the gap the file previously
	// called out explicitly at this spot: "No capability gating here; this
	// is observability metadata only"). Reuses filterToolsByCapability
	// (serve_kernel_agent_tools.go) — the same permit-by-default filter the
	// chat path applies — rather than duplicating its logic here.
	//
	// Three-part gate, matching the chat path's own contract
	// (serve.go: `cfg.IdentityNakedDefault && bound.Bound && capResolver != nil`):
	//   - IdentityNakedDefault: the dark flag: false today by default
	//     (config.go), so this is a no-op until an operator opts in.
	//   - gater != nil: SetCapabilityResolver has zero production callers as
	//     of this change (capResolver is nil at runtime everywhere) — so
	//     even with the flag on, gating stays dark until separate boot
	//     wiring constructs a CapabilityCache+CapabilityResolver. Acceptable:
	//     same dark-flag posture as the chat path's existing (also-dark)
	//     gates (tool_observer.go, mcp_tool_catalog.go, serve.go).
	//   - subject != "anonymous": an unbound/unauthenticated dispatch has no
	//     envelope to check against; mirrors the chat path's `bound.Bound`
	//     check (bound.Subject would be irrelevant when not bound).
	if c.cfg != nil && c.cfg.IdentityNakedDefault && subject != "" && subject != "anonymous" {
		if gater := c.capabilityGater(); gater != nil {
			filterToolsByCapability(compReq, subject, gater)
		}
	}

	start := time.Now()
	resp, clientCalls, transcript, err := c.completeWithToolLoop(slotCtx, counting, compReq, registry)
	res.DurationSec = time.Since(start).Seconds()
	res.Turns = counting.CompleteCalls()

	// ADR-083 (bus cycle-trace, not OTel), ADR-033, ADR-072: emit one
	// KindToolDispatch cycle-trace event per tool call so bus consumers
	// (dashboard, audit log) can see every tool invoked during this dispatch.
	// The cycleID injected by DispatchToHarness correlates all events from one
	// cog_dispatch_to_harness invocation.
	cycleID := dispatchCycleIDFromCtx(parent)
	for _, tc := range transcript {
		var toolErr error
		if tc.Rejected {
			toolErr = fmt.Errorf("%s", tc.RejectReason)
		}
		var argsRaw json.RawMessage
		if tc.Arguments != "" {
			argsRaw = json.RawMessage(tc.Arguments)
		}
		emitTrace(trace.NewToolDispatchWithAttribution(
			TraceIdentity(),
			cycleID,
			tc.Name,
			subject, // RFC-identity-embedding I1/I2
			argsRaw,
			time.Duration(tc.DurationMs)*time.Millisecond,
			toolErr,
		))
	}

	for _, tc := range transcript {
		entry := DispatchToolCallSummary{
			Name:         tc.Name,
			ArgsDigest:   truncateDigest(tc.Arguments),
			ResultDigest: truncateDigest(tc.Result),
		}
		if tc.Rejected {
			entry.Error = tc.RejectReason
		}
		res.ToolCalls = append(res.ToolCalls, entry)
	}
	if len(clientCalls) > 0 {
		res.Error = fmt.Sprintf("unsupported client tool calls returned: %d", len(clientCalls))
	}
	// ServedModel is the model id the provider actually reported serving
	// (CompletionResponse.ProviderMeta.Model), independent of success/degraded
	// outcome — set it whenever a response came back at all. Per issue #430
	// this is the canonical "what actually served" signal, alongside
	// ProviderUsed, in both the dispatch result and (via the ledger events
	// below) the kernel trace/session record.
	if resp != nil {
		res.ServedModel = resp.ProviderMeta.Model
	}
	if err != nil {
		// Structured loop-exit sentinels (ADR-031, ADR-052): ErrToolLoopMaxTurns
		// and ErrToolLoopNoProgress carry a partial response and transcript; mark
		// the slot as Degraded rather than a hard failure so the partial content
		// is still surfaced to the caller. Hard errors (provider failure, timeout)
		// fall through to the existing failure path below.
		if errors.Is(err, ErrToolLoopMaxTurns) || errors.Is(err, ErrToolLoopNoProgress) {
			res.Success = true
			res.Degraded = true
			// Structured is_error body (ADR-031 autonomic/fail-fast, RFC-020
			// four-element subset). Serialise as JSON so MCP callers receive a
			// machine-readable error body rather than a bare string.
			if sentinel, ok := err.(*toolLoopSentinel); ok {
				if body, merr := json.Marshal(LoopExitErrorBody(sentinel)); merr == nil {
					res.Error = string(body)
				} else {
					res.Error = err.Error()
				}
			} else {
				res.Error = err.Error()
			}
			if resp != nil {
				res.Content = stripControlTokens(strings.TrimSpace(resp.Content))
			}
			if res.Content == "" && len(transcript) > 0 {
				res.Content = summarizeToolTranscript(transcript)
			}
			slog.Warn("local harness dispatch: structured loop exit",
				"sentinel", err.Error(),
				"request_id", compReq.Metadata.RequestID,
				"tool_calls", len(transcript),
			)
			return res
		}
		if slotCtx.Err() == context.DeadlineExceeded {
			res.Error = "timeout"
		} else {
			res.Error = err.Error()
		}
		return res
	}
	res.Success = true
	// Strip harmony/control-token leakage before the content becomes
	// substrate-visible. ADR-eigen (output-contract) + RFC-027 (alignment layer).
	res.Content = stripControlTokens(strings.TrimSpace(resp.Content))
	if res.Content == "" && len(transcript) > 0 {
		// Model returned no final text; fall back to a transcript summary.
		// Surface Degraded so callers know the output contract was not met.
		res.Content = summarizeToolTranscript(transcript)
		res.Degraded = true
		slog.Warn("local harness dispatch: model returned no final text; degraded to transcript summary",
			"request_id", compReq.Metadata.RequestID,
			"tool_calls", len(transcript),
		)
	}
	// slotNote carries per-slot warnings (e.g. "26b route unavailable, using
	// preferred local model") that the caller may need to surface per-result.
	// These are distinct from batch-level state-routing diagnostics, which live
	// in batch.Notes and must not be set here.
	// Informational, not an error: it goes in the dedicated Note field, never
	// Error — a routing fallback on a success=true slot previously rode along
	// in Error and made successful dispatches look failed to callers that only
	// check that field.
	res.Note = slotNote
	return res
}

func (c *LocalHarnessController) completeWithToolLoop(ctx context.Context, provider Provider, req *CompletionRequest, registry *KernelToolRegistry) (*CompletionResponse, []ToolCall, []ToolCallRecord, error) {
	requestID := req.Metadata.RequestID
	// Cancel-safe + dedup (#432): dispatch is the other non-interactive,
	// non-streaming call site the incident identified. Refuse to start a
	// second attempt under the same content-stable request identity while a
	// prior one may still be generating server-side (retry discipline, #432
	// item d); the content-stable RequestID (sha256 of prompt+task+tools,
	// see dispatchSlot) means a genuine resubmit of the same work collides
	// here rather than stacking a second server-side generation.
	if requestID != "" && !beginInflightInference(requestID) {
		return nil, nil, nil, fmt.Errorf("local-harness: dispatch request %s already in flight", requestID)
	}
	if requestID != "" {
		defer endInflightInference(requestID)
	}
	resp, err := CompleteCancelSafeIfSupported(ctx, provider, req)
	if err != nil {
		recordAbandonedInference("local-harness-dispatch", requestID, err)
		return nil, nil, nil, err
	}
	if len(resp.ToolCalls) == 0 {
		return resp, nil, nil, nil
	}
	return RunToolLoopWithTranscript(ctx, provider, req, resp, registry)
}

type countingProvider struct {
	Provider
	completeCalls atomic.Int64
}

func (p *countingProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	p.completeCalls.Add(1)
	return p.Provider.Complete(ctx, req)
}

// CompleteCancelSafe delegates to the embedded provider's CompleteCancelSafe
// when it implements CancelSafeCompleter. Required so CompleteCancelSafeIfSupported
// (#432) can still route dispatch calls through the cancel-safe path:
// countingProvider embeds Provider (the interface), and Go only promotes
// methods declared on that static interface type — CompleteCancelSafe isn't
// one of them, so without this override a *countingProvider wrapping a
// cancel-safe-capable *OpenAICompatProvider would silently fall back to
// plain Complete, reintroducing the zombie-generation risk specifically on
// the dispatch path (dispatchSlot always wraps its provider in
// countingProvider to track turn counts).
//
// Delegates to completeCancelSafeIfSupportedRaw, NOT CompleteCancelSafeIfSupported:
// dispatchSlot's call to completeWithToolLoop already invokes
// CompleteCancelSafeIfSupported(ctx, counting, req) once, which — because
// *countingProvider implements CancelSafeCompleter — dispatches straight
// into this method. Calling the instrumented CompleteCancelSafeIfSupported
// again here would re-enter the RFC-040 S0 queue/p50 tap
// (dispatch_inference_metrics.go) a second time for the same logical call,
// double-counting every dispatch-path completion.
func (p *countingProvider) CompleteCancelSafe(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	p.completeCalls.Add(1)
	return completeCancelSafeIfSupportedRaw(ctx, p.Provider, req)
}

func (p *countingProvider) CompleteCalls() int {
	return int(p.completeCalls.Load())
}

func clampUrgency(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func decodeJSONPayload(raw string, out any) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("empty response")
	}
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end >= start {
		raw = raw[start : end+1]
	}
	return json.Unmarshal([]byte(raw), out)
}

func summarizeMemoryEntry(rec localHarnessCycleRecord) string {
	switch {
	case rec.Result != "":
		return truncateDigest(rec.Result)
	case rec.Reason != "":
		return truncateDigest(rec.Reason)
	default:
		return rec.Action
	}
}

func summarizeToolTranscript(records []ToolCallRecord) string {
	if len(records) == 0 {
		return ""
	}
	last := records[len(records)-1]
	if last.Result != "" {
		return truncateDigest(last.Result)
	}
	return last.Name
}

func truncateDigest(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 200 {
		return s
	}
	return s[:200] + "..."
}

func sinceString(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return time.Since(ts).Round(time.Second).String()
}
