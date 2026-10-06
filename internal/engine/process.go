// process.go — CogOS v3 continuous process state machine
//
// Implements the always-running cognitive process described in the v3 spec.
// The process has four states and an internal event loop that runs independently
// of external HTTP requests.
//
// States:
//
//	Active       — processing an external perturbation
//	Receptive    — idle, listening for input
//	Consolidating — running internal maintenance (memory, coherence)
//	Dormant      — minimal activity, heartbeat only
//
// The select loop is the core architectural difference from v2:
// v2 is request-triggered; v3 has internal tickers that fire regardless.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myrgic/cogos/pkg/filelock"
	"github.com/myrgic/cogos/pkg/substrate/bep"
	"github.com/myrgic/cogos/trace"
)

// TraceEmitter is the bus-publish hook for cycle-trace events. The main
// package sets this at startup; engine calls it best-effort (never blocks
// the metabolic cycle on a nil hook or a slow consumer).
var TraceEmitter func(ev trace.CycleEvent)

// TraceIdentity returns the identity name stamped as the `source` field on
// emitted events. Set by the main package at startup; defaults to "cog".
var TraceIdentity = func() string { return "cog" }

// emitTrace is a best-effort wrapper around TraceEmitter that swallows
// construction errors and never blocks the cycle.
func emitTrace(ev trace.CycleEvent, err error) {
	if err != nil {
		slog.Debug("process: trace event build failed", "err", err)
		return
	}
	if TraceEmitter == nil {
		return
	}
	TraceEmitter(ev)
}

// ProcessState represents the four operational states of the v3 process.
type ProcessState int

const (
	StateActive        ProcessState = iota // Processing external input
	StateReceptive                         // Idle, waiting
	StateConsolidating                     // Internal maintenance
	StateDormant                           // Minimal activity
)

func (s ProcessState) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateReceptive:
		return "receptive"
	case StateConsolidating:
		return "consolidating"
	case StateDormant:
		return "dormant"
	default:
		return "unknown"
	}
}

// TrustState tracks kernel-local identity and coherence trust metadata.
type TrustState struct {
	LocalScore           float64   `json:"local_score"`
	LastHeartbeatHash    string    `json:"last_heartbeat_hash,omitempty"`
	LastHeartbeatAt      time.Time `json:"last_heartbeat_at,omitempty"`
	CoherenceFingerprint string    `json:"coherence_fingerprint,omitempty"`
}

// Process is the always-running cognitive process.
type Process struct {
	mu      sync.RWMutex
	state   ProcessState
	nucleus *Nucleus
	field   *AttentionalField
	gate    *Gate
	bridge  ConstellationBridge
	cfg     *Config

	// sessionID is the persistent process session identifier.
	sessionID string

	// startedAt records when this process instance was created.
	startedAt time.Time

	// NodeID is the stable kernel node identity persisted across restarts.
	NodeID string

	// TrustState carries local trust, coherence, and heartbeat metadata.
	TrustState TrustState

	// externalCh receives events from the HTTP serve layer.
	externalCh chan *GateEvent

	// tailerCh receives normalized CogBlocks from StreamTailer adapters.
	tailerCh chan CogBlock

	// tailerManager coordinates configured digestion stream tailers.
	tailerManager *TailerManager

	// index is the CogDoc index, rebuilt on each consolidation.
	indexMu sync.RWMutex
	index   *CogDocIndex

	// observer is the trajectory model that closes the trefoil loop:
	// field state → prediction → salience actions → field state.
	observer *TrajectoryModel

	// trm is the MambaTRM temporal retrieval model (nil if weights not loaded).
	trm *MambaTRM

	// embeddingIndex is the CogDoc embedding index for TRM pre-filtering (nil if not loaded).
	embeddingIndex *EmbeddingIndex

	// lightCones manages per-conversation SSM hidden states.
	lightCones *LightConeManager

	// lastConsolidation records the last dormant memory consolidation pass.
	lastConsolidation time.Time

	lastMaintenanceTick time.Time

	// lastCoherenceReport caches the most recent coherence result so the
	// heartbeat can reuse it instead of recomputing.
	lastCoherenceReport *CoherenceReport

	// nodeHealth tracks sibling service health, probed by the node watcher
	// (node_watch.go) on its own cadence, independent of process state.
	nodeHealth *NodeHealth
	// nodeManifest is the parsed node manifest (nil if not found).
	nodeManifest *NodeManifest

	// lastIndexHEAD tracks the HEAD hash at last index rebuild, so we skip
	// rebuilding when nothing has changed.
	lastIndexHEAD string

	// currentCycleID correlates all cycle-trace events emitted during one
	// iteration of the metabolic cycle (external event handling or a
	// consolidation tick). Set fresh at the start of each iteration;
	// protected by mu.
	currentCycleID string

	// hookRegistry holds ADR-072 state-transition hooks loaded from
	// .cog/hooks/transitions/*.yaml. May be nil if the directory is missing.
	hookRegistry *stateHookRegistry

	// broker fans out ledger events to live subscribers (SSE /
	// cog_tail_events). Nil if the process was constructed without one.
	// AppendEvent publishes through the package-level CurrentBroker() set
	// by NewProcess, so call sites don't need to thread this pointer.
	broker *EventBroker

	// blobStore is the content-addressed blob store rooted at
	// WorkspaceRoot/.cog/blobs/. Initialized at process startup so that
	// ADR-084 digest-ref emit paths have a store ready to write into and
	// serve_blocks handlers can reuse this shared handle instead of
	// constructing new ones per request.
	blobStore *BlobStore

	// pendingToolCalls correlates client-ownership tool.call emissions with
	// the role=tool response that arrives on a later HTTP turn. Lazily
	// initialized on first registerPendingToolCall; swept by
	// RunPendingToolCallSweeper. See tool_observer.go.
	pendingToolCalls *pendingToolCallRegistry

	// sessionActivityPublisher fans tailer blocks out to session-scoped bus
	// channels (channel.<sid>.activity) in addition to the global ledger —
	// Phase 1A of the 4E cognitive-observer loop closure. Set by the main
	// serve wire-up (see cli.go) to *BusSessionManager.AppendEvent. Nil when
	// the process is constructed without a bus (e.g. benchmarks, some tests);
	// handleTailerBlock degrades gracefully in that case.
	sessionActivityPublisher SessionActivityPublisher

	// cadence records OBSERVED behavioral-cadence events (First Instruments
	// Module E, M11/M12/M13): dormant-consolidation completions, heartbeat
	// events past the StateActive gate, and active-window-expiry
	// observations. Side-effect-free append-only telemetry (§0) — see
	// cadence_events.go. Always initialized (never nil) so recording never
	// requires a nil check at the tap sites.
	cadence *cadenceRecorder
}

// SessionActivityPublisher is the bus-append signature used by
// handleTailerBlock to fan tailer blocks onto per-session activity channels.
// It matches *BusSessionManager.AppendEvent so main can wire the method
// value directly without an adapter.
type SessionActivityPublisher func(busID, eventType, from string, payload map[string]interface{}) (*BusBlock, error)

// NewProcess constructs and initialises the process.
func NewProcess(cfg *Config, nucleus *Nucleus) *Process {
	field := NewAttentionalField(cfg)
	gate := NewGate(field, cfg)
	now := time.Now().UTC()
	broker := NewEventBroker(EventBrokerOptions{})
	// Additive registration — every process's broker receives every
	// ledger append. In production there's only one process so this is
	// functionally identical to SetCurrentBroker; in tests it lets
	// multiple parallel processes coexist without racing a single slot.
	RegisterBroker(broker)

	// Initialize the content-addressed blob store (ADR-084). Calling Init()
	// here (not lazily) guarantees `.cog/blobs/` and the manifest are ready
	// before any emit path starts writing digest-ref payloads. A failure to
	// create the directory is logged but not fatal — individual Store calls
	// will retry the mkdir and surface their own errors.
	blobStore := NewBlobStore(workspaceRootFromCfg(cfg))
	if err := blobStore.Init(); err != nil {
		slog.Warn("process: blob store init failed", "err", err)
	}

	return &Process{
		state:     StateReceptive,
		nucleus:   nucleus,
		field:     field,
		gate:      gate,
		bridge:    NilBridge{},
		cfg:       cfg,
		sessionID: uuid.New().String(),
		startedAt: now,
		NodeID:    resolveNodeID(cfg),
		TrustState: TrustState{
			LocalScore:           1.0,
			CoherenceFingerprint: "sha256:" + sha256Hex("coherence:unknown"),
		},
		externalCh:          make(chan *GateEvent, 64),
		tailerCh:            make(chan CogBlock, 64),
		observer:            NewTrajectoryModel(),
		lastConsolidation:   now,
		lastMaintenanceTick: now,
		nodeHealth:          NewNodeHealth(),
		nodeManifest:        loadManifestQuiet(cfg),
		hookRegistry:        loadStateHookRegistry(workspaceRootFromCfg(cfg)),
		broker:              broker,
		blobStore:           blobStore,
		cadence:             newCadenceRecorder(),
	}
}

// Broker returns the process event broker (nil if not initialised).
func (p *Process) Broker() *EventBroker {
	if p == nil {
		return nil
	}
	return p.broker
}

// BlobStore returns the shared content-addressed blob store (nil if the
// process was constructed without one). Emit paths and HTTP blob handlers
// should prefer this shared handle over constructing new BlobStore values
// per call so they pick up any future in-memory state (locks, caches).
func (p *Process) BlobStore() *BlobStore {
	if p == nil {
		return nil
	}
	return p.blobStore
}

// workspaceRootFromCfg returns the workspace root from the config, or "" if nil.
func workspaceRootFromCfg(cfg *Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.WorkspaceRoot
}

// loadManifestQuiet loads the node manifest, returning nil if not found.
func loadManifestQuiet(cfg *Config) *NodeManifest {
	m, err := LoadManifest(DefaultManifestPath(cfg.WorkspaceRoot))
	if err != nil {
		slog.Debug("process: node manifest not found, sibling probes disabled", "err", err)
		return nil
	}
	slog.Info("process: node manifest loaded", "services", len(m.Services))
	return m
}

// State returns the current process state (safe for concurrent reads).
func (p *Process) State() ProcessState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// Field returns the attentional field (for use by the serve layer).
func (p *Process) Field() *AttentionalField {
	return p.field
}

// Gate returns the attentional gate.
func (p *Process) Gate() *Gate {
	return p.gate
}

// Index returns the current CogDoc index (may be nil before first consolidation).
func (p *Process) Index() *CogDocIndex {
	p.indexMu.RLock()
	defer p.indexMu.RUnlock()
	return p.index
}

// SessionID returns the process session identifier.
func (p *Process) SessionID() string {
	return p.sessionID
}

// StartedAt returns when this process instance was created.
func (p *Process) StartedAt() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.startedAt
}

// TrustSnapshot returns a copy of the current trust metadata.
func (p *Process) TrustSnapshot() TrustState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.TrustState
}

// Fingerprint returns a stable trust fingerprint for the current process state.
func (p *Process) Fingerprint() string {
	p.mu.RLock()
	nodeID := p.NodeID
	coherenceState := p.TrustState.CoherenceFingerprint
	p.mu.RUnlock()

	workspaceRoot := ""
	if p.cfg != nil {
		workspaceRoot = p.cfg.WorkspaceRoot
	}

	nucleusHash := p.nucleusDigest()
	material := strings.Join([]string{nodeID, workspaceRoot, nucleusHash, coherenceState}, "|")
	return "sha256:" + sha256Hex(material)
}

// Observer returns the trajectory model (for use by the HTTP layer).
func (p *Process) Observer() *TrajectoryModel {
	return p.observer
}

// TRM returns the MambaTRM model (nil if not loaded).
func (p *Process) TRM() *MambaTRM {
	return p.trm
}

// EmbeddingIndex returns the embedding index (nil if not loaded).
func (p *Process) EmbeddingIndex() *EmbeddingIndex {
	return p.embeddingIndex
}

// LightCones returns the per-conversation light cone manager.
func (p *Process) LightCones() *LightConeManager {
	return p.lightCones
}

// SetTRM installs the TRM model and embedding index (called at startup).
func (p *Process) SetTRM(trm *MambaTRM, idx *EmbeddingIndex) {
	p.trm = trm
	p.embeddingIndex = idx
	// Stop any prior manager's background TTL sweeper before replacing it, so a
	// second SetTRM call doesn't leak the previous sweeper goroutine.
	if p.lightCones != nil {
		p.lightCones.Close()
	}
	p.lightCones = NewLightConeManager(trm)
}

// SetSessionActivityPublisher installs the bus-append hook used by
// handleTailerBlock to publish normalized tailer blocks onto per-session
// activity channels (Phase 1A of the 4E cognitive-observer loop). The
// expected value in production is (*BusSessionManager).AppendEvent wired
// in at serve startup; tests inject a fake to observe publishes.
// Passing nil disables the channel-publish side-effect while leaving the
// ledger write intact.
func (p *Process) SetSessionActivityPublisher(pub SessionActivityPublisher) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessionActivityPublisher = pub
}

// Send delivers an external event to the process loop (non-blocking).
// Returns false if the channel is full.
func (p *Process) Send(evt *GateEvent) bool {
	select {
	case p.externalCh <- evt:
		return true
	default:
		return false
	}
}

// SubmitExternal is the canonical entry point for external perturbations
// (dashboard chat, modality inlets, etc.) into the metabolic cycle. It is a
// non-blocking alias of Send(): the external channel has bounded capacity and
// the cycle must never stall on a slow or noisy producer. Callers that need
// backpressure should observe the returned bool and drop/log accordingly.
func (p *Process) SubmitExternal(evt *GateEvent) bool {
	if p == nil || evt == nil {
		return false
	}
	select {
	case p.externalCh <- evt:
		return true
	default:
		return false
	}
}

// Run starts the continuous process loop. It blocks until ctx is cancelled.
func (p *Process) Run(ctx context.Context) error {
	// Surface the blob store root in the boot log so operators can see
	// where digest-ref payloads will land. Construction + Init happened in
	// NewProcess; this is purely informational.
	if p.blobStore != nil {
		slog.Info("process: blob store ready", "root", p.blobStore.root)
	}

	// Build CogDoc index first (fast — just frontmatter parsing).
	// This must happen before field.Update() which can be slow (git log per file).
	slog.Info("process: building initial CogDoc index")
	if idx, err := BuildIndex(p.cfg.WorkspaceRoot); err != nil {
		slog.Warn("process: initial index build failed", "err", err)
	} else {
		p.indexMu.Lock()
		p.index = idx
		p.indexMu.Unlock()
		slog.Info("process: index built", "docs", len(idx.ByURI))
	}
	p.initTailers(ctx)

	// Update attentional field (slow — git log per file, runs in background).
	go func() {
		slog.Info("process: updating attentional field (background)")
		if err := p.field.Update(); err != nil {
			slog.Warn("process: field update failed", "err", err)
		} else {
			slog.Info("process: field updated", "files", p.field.Len())
		}
	}()

	// Emit genesis event.
	p.emitEvent("process.start", map[string]interface{}{
		"state":    p.State().String(),
		"session":  p.sessionID,
		"identity": p.nucleus.Name,
	})

	consolidationTicker := time.NewTicker(time.Duration(p.cfg.ConsolidationInterval) * time.Second)
	heartbeatTicker := time.NewTicker(time.Duration(p.cfg.HeartbeatInterval) * time.Second)
	defer consolidationTicker.Stop()
	defer heartbeatTicker.Stop()

	// The node watcher (#429): its own goroutine and ticker, so the sibling
	// services are probed every NodeProbeInterval even while the process is
	// active, and a slow probe or a restart never blocks this run loop.
	if w := p.newNodeWatcher(); w != nil {
		go w.Run(ctx)
	}

	slog.Info("process: running", "state", p.State(), "session", p.sessionID)

	for {
		select {
		case <-ctx.Done():
			p.emitEvent("process.stop", map[string]interface{}{
				"reason": ctx.Err().Error(),
			})
			slog.Info("process: stopped", "reason", ctx.Err())
			return nil

		case evt := <-p.externalCh:
			p.handleExternal(evt)

		case block := <-p.tailerCh:
			p.handleTailerBlock(block)

		case <-consolidationTicker.C:
			p.runConsolidation()

		case <-heartbeatTicker.C:
			p.emitHeartbeat()
		}
	}
}

func (p *Process) initTailers(ctx context.Context) {
	if p.tailerManager == nil {
		p.tailerManager = NewTailerManager(p.tailerCh)
		p.registerConfiguredTailers(p.tailerManager)
	}

	if p.tailerManager == nil {
		return
	}

	go func() {
		if err := p.tailerManager.Run(ctx); err != nil {
			slog.Warn("process: tailer manager stopped with error", "err", err)
		}
	}()
}

func (p *Process) registerConfiguredTailers(manager *TailerManager) {
	if p == nil || p.cfg == nil || manager == nil || len(p.cfg.DigestPaths) == 0 {
		return
	}

	for adapterName, configuredPath := range p.cfg.DigestPaths {
		path := strings.TrimSpace(configuredPath)
		switch adapterName {
		case claudeCodeSourceChannel:
			if path == "" {
				path = "~/.claude/"
			}
			path = expandDigestPath(path)
			if err := manager.Register(&ClaudeCodeTailer{}, path); err != nil {
				slog.Warn("process: digest tailer register failed", "adapter", adapterName, "path", path, "err", err)
				continue
			}
			slog.Info("process: digest tailer registered", "adapter", adapterName, "path", path)

		case "openclaw":
			if path == "" {
				slog.Warn("process: digest tailer path empty", "adapter", adapterName)
				continue
			}
			path = expandDigestPath(path)
			if err := manager.Register(&OpenClawTailer{}, path); err != nil {
				slog.Warn("process: digest tailer register failed", "adapter", adapterName, "path", path, "err", err)
				continue
			}
			slog.Info("process: digest tailer registered", "adapter", adapterName, "path", path)

		default:
			slog.Warn("process: unknown digest adapter", "adapter", adapterName)
		}
	}
}

func expandDigestPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return path
}

func (p *Process) handleTailerBlock(block CogBlock) {
	state := p.State()
	if state != StateReceptive && state != StateActive {
		slog.Debug("process: dropping tailer block outside receptive states", "state", state.String(), "source_channel", block.SourceChannel, "kind", block.Kind)
		return
	}

	b := block
	if b.Timestamp.IsZero() {
		b.Timestamp = time.Now().UTC()
	}

	// Capture the originating session id BEFORE RecordBlock — RecordBlock
	// substitutes the kernel's own sessionID when the block is missing one,
	// and for the activity channel we only care about externally-sourced
	// session identifiers. For Claude Code blocks this is the per-
	// conversation UUID carried on every JSONL line (see
	// normalizeClaudeCodeLine — CogBlock.SessionID and
	// BlockProvenance.OriginSession are both populated from entry.sessionId).
	// OpenClaw and other tailers that don't stamp a session id fall through
	// to the graceful-skip branch below.
	sid := strings.TrimSpace(b.SessionID)
	if sid == "" {
		sid = strings.TrimSpace(b.Provenance.OriginSession)
	}

	ref := p.RecordBlock(&b)
	slog.Info("process: tailer block ingested", "source_channel", b.SourceChannel, "kind", b.Kind, "ledger_ref", ref)

	p.publishSessionActivity(sid, &b, ref)
}

// publishSessionActivity fans a tailer block onto the session-scoped bus
// channel channel.<sid>.activity (ADR-084 dot-separated lowercase event
// naming). Phase 1A of the 4E cognitive-observer loop — downstream
// UserPromptSubmit hooks query this channel to render peer-awareness
// packets (Phase 1B). The payload is a minimal summary rather than the
// full block content: the ledger remains authoritative for payload, the
// channel is only for downstream query-driven notifications.
//
// Skips with a debug log when the originating session id is missing
// (e.g. OpenClaw blocks, or Claude Code lines lacking the sessionId
// field) or when no publisher has been wired — tailer ingestion never
// fails on a missing side-channel.
func (p *Process) publishSessionActivity(sid string, b *CogBlock, ref string) {
	if b == nil {
		return
	}
	if sid == "" {
		slog.Debug("process: session activity publish skipped — no session id on block",
			"source_channel", b.SourceChannel, "kind", b.Kind)
		return
	}

	p.mu.RLock()
	pub := p.sessionActivityPublisher
	p.mu.RUnlock()
	if pub == nil {
		slog.Debug("process: session activity publish skipped — no publisher wired",
			"sid", sid, "source_channel", b.SourceChannel)
		return
	}

	busID := "channel." + sid + ".activity"
	payload := map[string]interface{}{
		"kind":           string(b.Kind),
		"source_channel": b.SourceChannel,
		"timestamp":      b.Timestamp.UTC().Format(time.RFC3339Nano),
		"ref":            ref,
	}
	from := b.SourceChannel
	if from == "" {
		from = "kernel:tailer"
	}

	if _, err := pub(busID, "tailer.block", from, payload); err != nil {
		slog.Warn("process: session activity publish failed",
			"bus_id", busID, "source_channel", b.SourceChannel, "err", err)
		return
	}
	slog.Debug("process: session activity published",
		"bus_id", busID, "source_channel", b.SourceChannel, "kind", b.Kind)
}

// handleExternal processes an external perturbation.
func (p *Process) handleExternal(evt *GateEvent) {
	p.beginCycle()
	result := p.gate.Process(evt)
	p.transitionWithReason(result.StateTransition, fmt.Sprintf("external:%s", evt.Type))
	slog.Debug("process: external event",
		"type", evt.Type,
		"state", p.State(),
		"elevated", len(result.Elevated),
	)
}

// runConsolidation runs the internal maintenance loop.
// This is the observer's heartbeat — the tick where:
//
//	Loop 1: the field is read (field state + attention log → perception)
//	Loop 2: the TrajectoryModel is updated (prediction + error computation)
//	Loop 3: the model acts on the field (pre-warm, attenuate, coherence signal)
func (p *Process) runConsolidation() {
	p.beginCycle()
	p.transitionWithReason(StateConsolidating, "consolidation.tick")

	// Record the current tick window before updating the maintenance watermark.
	now := time.Now()
	tickStart := p.lastMaintenanceTick
	p.lastMaintenanceTick = now

	slog.Debug("process: consolidating", "window_since", tickStart.Format(time.RFC3339))

	// ── Loop 1: Update the attentional field (Field → Observer) ────────────
	if err := p.field.Update(); err != nil {
		slog.Warn("process: field update failed", "err", err)
	}

	// Rebuild the CogDoc index only if HEAD has changed.
	currentHEAD := resolveHEAD(p.cfg.WorkspaceRoot)
	if currentHEAD == "" || currentHEAD != p.lastIndexHEAD {
		if idx, err := BuildIndex(p.cfg.WorkspaceRoot); err != nil {
			slog.Warn("process: index rebuild failed", "err", err)
		} else {
			p.indexMu.Lock()
			p.index = idx
			p.indexMu.Unlock()
			p.lastIndexHEAD = currentHEAD
		}
	}

	// Run coherence check and cache the result for the heartbeat.
	p.indexMu.RLock()
	currentIdx := p.index
	p.indexMu.RUnlock()
	report := RunCoherence(p.cfg, p.nucleus, currentIdx)
	p.lastCoherenceReport = report
	if !report.Pass {
		slog.Warn("process: coherence check failed", "results", len(report.Results))
		p.emitEvent("coherence.fail", map[string]interface{}{"pass": false})
	}

	// Read attention signals since the last tick (Loop 1 percept).
	attended := readRecentAttentionSignals(p.cfg.WorkspaceRoot, tickStart)
	fieldScores := p.field.AllScores()

	// ── Loop 2: Update the trajectory model (Observer → Model) ─────────────
	u := p.observer.Update(attended, fieldScores)

	slog.Debug("process: observer",
		"cycle", u.Cycle,
		"attended", len(attended),
		"prediction_error", fmt.Sprintf("%.4f", u.PredictionError),
		"mean_error", fmt.Sprintf("%.4f", u.MeanError),
		"predicted", len(u.Prediction),
		"receding", len(u.Receding),
	)

	// Detect surprise: error above threshold on cycle > 1 (first cycle has
	// no prior prediction, so error is always 0 or 1 depending on attendance).
	surprise := u.PredictionError > surpriseThreshold && u.Cycle > 1
	if surprise {
		slog.Info("process: observer surprise",
			"error", fmt.Sprintf("%.4f", u.PredictionError),
			"threshold", surpriseThreshold,
			"cycle", u.Cycle,
		)
		p.emitEvent("observer.surprise", map[string]interface{}{
			"cycle":            u.Cycle,
			"prediction_error": u.PredictionError,
			"threshold":        surpriseThreshold,
			"attended":         attended,
		})
	}

	// Record prediction in the ledger — hash-chained, irreversible.
	// This is the arrow of time: the model's anticipation, written before
	// the outcome is known.
	p.emitEvent("observer.prediction", map[string]interface{}{
		"cycle":            u.Cycle,
		"prediction_error": u.PredictionError,
		"mean_error":       u.MeanError,
		"prediction":       u.Prediction,
		"attended":         attended,
		"surprise":         surprise,
	})

	// ── Loop 3: Model acts on the field (Model → Field) ────────────────────
	warmed, attenuated := applyObserverActions(p.field, p.observer, u.Prediction, u.Receding)

	slog.Debug("process: observer field actions", "warmed", warmed, "attenuated", attenuated)

	// Write consolidation CogDoc — the model's trace in the field.
	if err := writeConsolidationDoc(p.cfg, u, surprise); err != nil {
		slog.Warn("process: consolidation doc write failed", "err", err)
	}

	p.emitEvent("consolidation.complete", map[string]interface{}{
		"field_size":            p.field.Len(),
		"coherence_pass":        report.Pass,
		"observer_cycle":        u.Cycle,
		"prediction_error":      u.PredictionError,
		"mean_prediction_error": u.MeanError,
		"warmed":                warmed,
		"attenuated":            attenuated,
		"surprise":              surprise,
	})

	p.transitionWithReason(StateReceptive, "consolidation.complete")
}

// NodeHealth returns the current node health state (for use by the serve layer).
func (p *Process) NodeHealth() *NodeHealth {
	return p.nodeHealth
}

// NodeManifest returns the parsed node manifest (nil if not loaded).
func (p *Process) NodeManifest() *NodeManifest {
	return p.nodeManifest
}

// emitHeartbeat fires during the dormant state.
func (p *Process) emitHeartbeat() {
	// Only emit heartbeat if not already in an active state.
	if p.State() == StateActive {
		return
	}

	// First Instruments M12 tap (FROZEN tap point, blind-review-3 F9): record
	// PAST the StateActive gate above, not at the run-loop invocation site
	// (the ticker case that calls emitHeartbeat). Tapping at the invocation
	// would record an event for every tick even inside a StateActive
	// interval where the body below never runs, making the H6
	// StateActive-exclusion a no-op for M12 (the event would already be
	// recorded before the gate). Tapping here means M12 only records
	// heartbeats that actually did real work — consistent with the K12-gated
	// consolidation (M11) sharing this same suppression.
	p.cadence.recordHeartbeat(time.Now().UTC(), p.State())

	// Probe sibling services: no longer here. The node watcher (node_watch.go,
	// started in Run) probes on its own ticker whatever the process state, so a
	// busy kernel still sees its siblings every NodeProbeInterval (#429).

	// Reuse the cached coherence report from the last consolidation tick
	// instead of recomputing it. Fall back to a fresh check if no cache exists.
	report := p.lastCoherenceReport
	if report == nil {
		p.indexMu.RLock()
		currentIdx := p.index
		p.indexMu.RUnlock()
		report = RunCoherence(p.cfg, p.nucleus, currentIdx)
	}
	coherenceHash := coherenceFingerprint(report)
	now := time.Now().UTC()

	p.mu.Lock()
	p.TrustState.CoherenceFingerprint = coherenceHash
	p.mu.Unlock()

	p.beginCycle()
	p.transitionWithReason(StateDormant, "heartbeat")
	state := p.State().String()
	fieldSize := p.field.Len()
	fingerprint := p.Fingerprint()
	receipt, err := p.constellationBridge().EmitHeartbeat(KernelHeartbeatPayload{
		ProcessState:         state,
		FieldSize:            fieldSize,
		CoherenceFingerprint: coherenceHash,
		NucleusFingerprint:   p.nucleusDigest(),
		LedgerHead:           p.currentLedgerHead(),
		Timestamp:            now,
	})
	if err != nil {
		slog.Warn("process: constellation heartbeat failed", "err", err)
		receipt = HeartbeatReceipt{}
	}

	heartbeat := map[string]interface{}{
		"state":                      state,
		"field_size":                 fieldSize,
		"node_id":                    p.NodeID,
		"fingerprint":                fingerprint,
		"timestamp":                  now.Format(time.RFC3339),
		"coherence_hash":             coherenceHash,
		"constellation_receipt_hash": receipt.Hash,
		"constellation_peers_sent":   receipt.PeersSent,
	}
	if !receipt.Timestamp.IsZero() {
		heartbeat["constellation_receipt_at"] = receipt.Timestamp.Format(time.RFC3339)
	}
	p.emitEvent("heartbeat", heartbeat)

	raw, _ := json.Marshal(heartbeat)
	trust := p.TrustSnapshot()
	block := &CogBlock{
		ID:              uuid.NewString(),
		Timestamp:       now,
		SessionID:       p.sessionID,
		SourceChannel:   "internal",
		SourceTransport: "direct",
		SourceIdentity:  p.NodeID,
		WorkspaceID:     filepath.Base(p.cfg.WorkspaceRoot),
		Kind:            BlockSystemEvent,
		RawPayload:      raw,
		Messages:        []ProviderMessage{{Role: "system", Content: "heartbeat"}},
		Provenance: BlockProvenance{
			OriginSession: p.sessionID,
			OriginChannel: "internal",
			IngestedAt:    now,
			NormalizedBy:  "direct",
		},
		TrustContext: TrustContext{
			Authenticated: true,
			TrustScore:    trust.LocalScore,
			Scope:         "local",
		},
	}
	// Heartbeat is a kernel-own action (no inbound per-session request context),
	// so the nucleus remains the correct identity — no per-session resolution here.
	if p.nucleus != nil {
		block.TargetIdentity = p.nucleus.Name
	}
	if receipt.Hash != "" {
		block.Artifacts = append(block.Artifacts, BlockArtifact{
			Kind: "constellation_receipt",
			Ref:  receipt.Hash,
		})
	}
	ref := p.RecordBlock(block)

	p.mu.Lock()
	p.TrustState.LastHeartbeatHash = ref
	p.TrustState.LastHeartbeatAt = now
	p.mu.Unlock()

	interval := time.Duration(p.cfg.ConsolidationInterval) * time.Second
	if interval <= 0 || now.Sub(p.lastConsolidation) < interval {
		return
	}

	action := ConsolidationAction{WorkspaceRoot: p.cfg.WorkspaceRoot}
	count, err := action.Run()
	if err != nil {
		slog.Warn("process: memory consolidation failed", "err", err)
		// First Instruments Finding B: a persistent Run() error means the
		// gate above returns without updating p.lastConsolidation, so
		// attempts re-fire every heartbeat and the ATTEMPT cadence collapses
		// to H. Deliberately do NOT record a cadence event here — tapping
		// attempts/gate-passes instead of the success point below would
		// forge the "cadence tracks H alone" KC-3-LAW kill signature from an
		// environment fault. A run where Run() errors persist collects no
		// valid M11 cadence and is INSTRUMENT-BROKEN, never a law-kill.
		return
	}

	p.lastConsolidation = now
	// First Instruments M11 tap (FROZEN at the SUCCESS point, blind-review-4
	// Finding B): recorded co-located with p.lastConsolidation = now, i.e.
	// only on a COMPLETED action.Run(), never on a gate-pass or attempt.
	p.cadence.recordConsolidation(now, "heartbeat_gated", p.State())
	slog.Info("process: memory consolidated", "sessions", count)
}

// transition moves the process to a new state (with logging).
func (p *Process) transition(next ProcessState) {
	p.transitionWithReason(next, "")
}

// transitionWithReason is transition() with a human-readable reason string
// that is attached to the emitted cycle-trace event. Best-effort emission:
// a nil TraceEmitter or a build error never blocks the cycle.
func (p *Process) transitionWithReason(next ProcessState, reason string) {
	p.mu.Lock()
	prev := p.state
	p.state = next
	cycleID := p.currentCycleID
	p.mu.Unlock()
	if prev == next {
		return
	}
	slog.Debug("process: state transition", "from", prev, "to", next, "reason", reason)
	if cycleID == "" {
		// Fall back to a one-shot ID so events remain correlatable at least
		// within this single transition; real iterations mint their own.
		cycleID = uuid.NewString()
	}
	emitTrace(trace.NewStateTransition(TraceIdentity(), cycleID, prev, next, reason))
	// ADR-072: dispatch per-state enter handlers + declarative StateHooks on a
	// goroutine so transition() never blocks the caller (may hold p.mu upstream).
	go p.dispatchEnterHandler(prev, next)
}

// beginCycle mints a new cycle ID for the start of a metabolic-cycle
// iteration (consolidation tick or external event). All trace events emitted
// during the iteration share this ID. Returns the new ID.
func (p *Process) beginCycle() string {
	id := uuid.NewString()
	p.mu.Lock()
	p.currentCycleID = id
	p.mu.Unlock()
	return id
}

// CurrentCycleID returns the current iteration's cycle ID (may be empty
// between iterations). Exported so the agent harness and tool dispatch paths
// can correlate their own trace events with the running kernel cycle.
func (p *Process) CurrentCycleID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentCycleID
}

// emitEvent records a ledger event for the process session.
func (p *Process) emitEvent(eventType string, data map[string]interface{}) {
	_ = p.EmitEvent(eventType, data, "kernel-v3")
}

// EmitEvent appends a single event to the session ledger via AppendEvent,
// which means it flows through the hash chain AND the in-process broker.
// Callers: MCP cog_emit_event tool, internal emitEvent helper.
//
// The `source` field lands on envelope.Metadata.Source — subscribers use it
// to distinguish kernel-generated events (kernel-v3) from MCP-client events
// (mcp-client) and future external sources.
//
// This replaces the pre-PR EmitLedgerEvent in mcp_stubs.go which wrote to a
// flat .cog/ledger/events.jsonl orphan file bypassing hash chaining
// (cogos#10). Post-refactor: every event — kernel or MCP — goes through
// AppendEvent, so the live broker sees everything.
func (p *Process) EmitEvent(eventType string, data map[string]interface{}, source string) error {
	if p == nil {
		return fmt.Errorf("process: nil receiver")
	}
	if source == "" {
		source = "kernel-v3"
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: nowISO(),
			SessionID: p.sessionID,
			Data:      data,
		},
		Metadata: EventMetadata{
			Source: source,
		},
	}
	if err := AppendEvent(p.cfg.WorkspaceRoot, p.sessionID, env); err != nil {
		slog.Debug("process: ledger append failed", "err", fmt.Sprintf("%v", err))
		return err
	}
	return nil
}

// Node-id RESOLUTION (which id this kernel uses, and where it is cached) lives
// in node_identity.go — see resolveNodeID. It is machine-scoped: caching the id
// under the workspace, as this file used to, let a containerized child kernel
// read the host's file through the `-v WorkspaceRoot:WorkspaceRoot` bind mount
// and adopt the host's identity.
//
// What remains here is node-id MINTING, unchanged: the cert-anchoring chain
// from cogos#474 (RFC-036). resolveNodeID calls into it via mintNodeID.

// nodeIDCertDir resolves the BEP cert dir used for node-id anchoring. It is a
// package var solely so tests can point it at a t.TempDir() and stay hermetic;
// production always resolves the canonical dir (~/.cog/etc).
var nodeIDCertDir = func() string { return bep.ExpandCertDir("") }

// bepAnchoredNodeID returns the formatted BEP DeviceID derived from the node's
// on-disk BEP certificate, or "" when no usable cert is present. Never panics;
// any error (missing cert, unparseable, empty chain) yields "" for UUID
// fallback.
//
// It reads the DEFAULT BEP cert dir (bep.ExpandCertDir("") == bep.CertDir() ==
// ~/.cog/etc), which is exactly what NewBEPEngine resolves to when the cluster
// config sets no CertDir override — so for every deployment that uses the
// default (all of them today; the cert lives at ~/.cog/etc), the node id is
// anchored to the SAME cert BEP presents on the wire. KNOWN BOUNDARY: a node
// that overrides cluster.CertDir would need that resolved dir threaded through
// NewProcess to stay consistent; until then such a node anchors to the default
// dir or falls back to UUID. Tracked as a follow-up, not silently correct.
func bepAnchoredNodeID() string {
	cert, err := bep.LoadBEPCert(nodeIDCertDir())
	if err != nil {
		return ""
	}
	devID, err := bep.DeviceIDFromTLSCert(&cert)
	if err != nil {
		return ""
	}
	return bep.FormatDeviceID(devID)
}

// bepCertLockTimeout bounds how long ensureBEPDeviceIdentity waits for the
// cross-process cert-generation lock before giving up and falling back to a
// UUID for this boot. Cert generation itself is a few milliseconds of ECDSA
// key generation and two small file writes, so this is generous headroom
// for a contending process, not a tuning knob for slow I/O.
const bepCertLockTimeout = 5 * time.Second

// ensureBEPDeviceIdentity generates the node's BEP keypair in the canonical
// cert dir when none (usable) exists, so that node-id minting can anchor to
// a device identity on a node's very first boot — before, and independently
// of, any decision to enable clustering. Generating the keypair binds no
// port and starts no goroutine; the BEP engine remains dark until
// cluster.enabled=true. Returns an error when the identity could not be
// established, in which case the caller falls back to a UUID exactly as
// before.
//
// Single-flighted via the same pkg/filelock pattern used elsewhere in this
// codebase for comparable file mutations (pkg/alias, internal/conversations,
// internal/engine/bus_session.go) because NewProcess is called from multiple
// independent entrypoints (boot.go, cli_mcp.go, experiment.go, benchmark.go)
// that can all race against the same default cert dir on a fresh node's
// first boot.
func ensureBEPDeviceIdentity() error {
	dir := nodeIDCertDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}

	lock, err := filelock.Acquire(filepath.Join(dir, "bep-cert.lock"), bepCertLockTimeout)
	if err != nil {
		return fmt.Errorf("acquire BEP cert lock: %w", err)
	}
	defer lock.Release()

	certPath := filepath.Join(dir, "bep-cert.pem")
	keyPath := filepath.Join(dir, "bep-key.pem")
	_, certErr := os.Stat(certPath)
	_, keyErr := os.Stat(keyPath)
	certExists := certErr == nil
	keyExists := keyErr == nil

	switch {
	case certExists && keyExists:
		// Both files exist, but existence alone is not proof of a usable
		// identity: verify the pair actually loads as a matching TLS
		// keypair, via the exact same load path bepAnchoredNodeID (and
		// ultimately NewBEPEngine's LoadBEPCert) uses. Disk corruption, a
		// restored backup that mixes cert/key from two different nodes, or
		// a race against the unlocked `bep-cert gen` CLI can leave two
		// files that both pass os.Stat but do not form a valid pair —
		// treating that as "identity established" is the same
		// silent-permanent-UUID bug this function exists to close, one
		// layer deeper.
		if bepAnchoredNodeID() != "" {
			// Another process finished generation while we waited for the
			// lock (or a prior boot already did), and the pair is genuinely
			// usable. Nothing to do.
			return nil
		}
		suffix := fmt.Sprintf(".broken-%d", time.Now().UnixNano())
		slog.Warn("nodeid: BEP cert and key both exist but do not load as a valid matching identity; backing up and regenerating",
			"cert_dir", dir, "backup_suffix", suffix)
		for _, p := range []string{certPath, keyPath} {
			if err := os.Rename(p, p+suffix); err != nil && !os.IsNotExist(err) {
				// Backing up is best-effort: leaving the broken file in
				// place would recreate the exact silent-fallback bug this
				// recovery exists to close, so fall back to removing it
				// outright rather than give up.
				if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
					return fmt.Errorf("clear broken BEP file %s: %w", p, rmErr)
				}
			}
		}

	case certExists != keyExists:
		// Loud, not silent: a cert without its key (or the symmetric case)
		// is not "mostly done", it's a broken identity that would otherwise
		// make bepAnchoredNodeID fail forever and silently downgrade every
		// future boot to a UUID (RFC-036 anchoring never activates), while
		// also setting up a much later, seemingly-unrelated hard failure in
		// NewBEPEngine's LoadBEPCert the first time cluster.enabled flips to
		// true. We hold the cross-process lock here, so no concurrent writer
		// can be mid-write on these files right now — it is safe to reclaim
		// the orphan and regenerate cleanly.
		orphan, orphanKind := certPath, "cert"
		if keyExists {
			orphan, orphanKind = keyPath, "key"
		}
		slog.Warn("nodeid: found orphaned BEP "+orphanKind+" with no matching pair; removing and regenerating",
			"cert_dir", dir, "orphan", orphan)
		if rmErr := os.Remove(orphan); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("remove orphaned BEP %s: %w", orphanKind, rmErr)
		}
	}

	if err := bep.GenerateBEPCert(dir); err != nil {
		return fmt.Errorf("generate BEP cert: %w", err)
	}
	slog.Info("nodeid: minted BEP device identity for node-id anchoring (RFC-036)",
		"cert_dir", dir)
	return nil
}

func (p *Process) nucleusDigest() string {
	if p == nil || p.nucleus == nil {
		return "sha256:" + sha256Hex("nucleus:nil")
	}
	material := strings.Join([]string{p.nucleus.Name, p.nucleus.Role, p.nucleus.Card}, "|")
	return "sha256:" + sha256Hex(material)
}

func coherenceFingerprint(report *CoherenceReport) string {
	if report == nil {
		return "sha256:" + sha256Hex("coherence:nil")
	}
	parts := []string{fmt.Sprintf("pass:%t", report.Pass)}
	for _, result := range report.Results {
		rule := ""
		expected := ""
		actual := ""
		if result.Diagnostic != nil {
			rule = result.Diagnostic.Rule
			expected = result.Diagnostic.Expected
			actual = result.Diagnostic.Actual
		}
		parts = append(parts, strings.Join([]string{
			result.Layer,
			fmt.Sprintf("%t", result.Pass),
			rule,
			expected,
			actual,
		}, "|"))
	}
	return "sha256:" + sha256Hex(strings.Join(parts, "\n"))
}

func sha256Hex(input string) string {
	digest := sha256.Sum256([]byte(input))
	return hex.EncodeToString(digest[:])
}
