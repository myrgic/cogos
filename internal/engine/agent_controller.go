// agent_controller.go — AgentController interface and shared types for the
// agent-state API surface.
//
// This file breaks the import cycle between the engine MCP layer (which
// exposes the tools) and the root package (which owns the actual
// *ServeAgent goroutine). The root package implements AgentController in
// a thin adapter (see root: serve_agents.go) and injects it into the
// MCP server via NewMCPServerWithAgentController.
//
// Design rationale (see Agent T's design spike,
// cog://mem/semantic/surveys/2026-04-21-consolidation/agent-T-agent-state-design):
//   - Identities are static YAML (AgentProvider reconciles them; they don't run).
//   - Sessions are external-client conversational contexts (Agent P's lane).
//   - Agents are kernel-internal goroutines — the ServeAgent singleton today.
//
// This interface surfaces the agent. It does NOT surface identities or
// sessions; those are separate APIs.
package engine

import (
	"context"
)

// AgentController is the kernel-agnostic contract the MCP layer uses to
// list, inspect, and trigger agent harness instances. The concrete
// implementation lives in the main package because *ServeAgent is there;
// this interface keeps internal/engine import-cycle-free.
//
// Today there is exactly one agent per kernel process ("primary"). The
// interface pluralises from day one so new handles (e.g. "digestion",
// "identity-<name>") can land without an API break.
type AgentController interface {
	// ListAgents returns a summary for each running agent. includeStopped
	// is reserved for future pool managers; today every returned agent is
	// running.
	ListAgents(ctx context.Context, includeStopped bool) ([]AgentSummary, error)

	// GetAgent returns the full state snapshot for the named agent. When
	// includeTrace is true, up to traceLimit most-recent full cycle traces
	// are attached (clamped to [1,20]). Returns ErrAgentNotFound when id
	// is unknown.
	GetAgent(ctx context.Context, id string, includeTrace bool, traceLimit int) (*AgentSnapshot, error)

	// TriggerAgent manually invokes one homeostatic cycle for the named
	// agent, outside the adaptive ticker. When wait is true, blocks until
	// the cycle completes (or a 90s deadline elapses). When wait is false,
	// returns immediately with a trigger receipt.
	TriggerAgent(ctx context.Context, id string, reason string, wait bool) (*AgentTriggerResult, error)
}

// ErrAgentNotFound is returned by GetAgent/TriggerAgent when no agent
// matches the supplied id. Implementations should wrap this with %w so
// callers can errors.Is against it.
type AgentControllerError struct {
	Code    string // "not_found" | "unavailable" | "invalid_input"
	Message string
}

func (e *AgentControllerError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// ErrAgentNotFound signals that the agent_id in the request did not match
// any agent known to the controller. HTTP handlers translate this to 404;
// MCP tool handlers translate this to an IsError:true response.
var ErrAgentNotFound = &AgentControllerError{Code: "not_found", Message: "agent not found"}

// ErrAgentUnavailable signals that the controller is installed but no
// agent is currently wired (e.g. the ServeAgent singleton never started).
// HTTP handlers translate this to 503.
var ErrAgentUnavailable = &AgentControllerError{Code: "unavailable", Message: "agent not running"}

// ErrAgentInvalidInput signals malformed arguments (bad agent_id regex,
// trace_limit out of range). HTTP handlers translate this to 400.
var ErrAgentInvalidInput = &AgentControllerError{Code: "invalid_input", Message: "invalid agent input"}

// --- Value types ------------------------------------------------------------

// AgentSummary is the list-friendly projection of one agent. Fields map
// 1:1 onto ServeAgent.Status() fields that already exist in-process — this
// is a rename, not new state.
type AgentSummary struct {
	AgentID     string  `json:"agent_id"`
	Identity    string  `json:"identity,omitempty"` // nucleus.Name today ("cog", "sandy", etc.)
	Alive       bool    `json:"alive"`              // process is running
	Running     bool    `json:"running"`            // a cycle is in progress RIGHT NOW
	UptimeSec   int64   `json:"uptime_sec"`
	CycleCount  int64   `json:"cycle_count"`
	LastAction  string  `json:"last_action,omitempty"` // sleep|observe|propose|execute|escalate|skip|error|""
	LastCycle   string  `json:"last_cycle,omitempty"`  // RFC3339
	LastUrgency float64 `json:"last_urgency"`
	LastReason  string  `json:"last_reason,omitempty"`
	LastDurMs   int64   `json:"last_duration_ms"`
	Model       string  `json:"model,omitempty"`
	Interval    string  `json:"interval,omitempty"`
}

// AgentActivitySummary is the bus-delta + user-presence summary. Exact
// shape of the root package's AgentActivitySummary so handlers can pass
// through without mapping.
type AgentActivitySummary struct {
	UserPresence     string `json:"user_presence"`
	UserLastEventAgo string `json:"user_last_event_ago"`
	ClaudeCodeActive int    `json:"claude_code_active"`
	ClaudeCodeEvents int64  `json:"claude_code_events"`
	TotalEventDelta  int64  `json:"total_event_delta"`
	HottestBus       string `json:"hottest_bus,omitempty"`
	HottestDelta     int64  `json:"hottest_delta"`
}

// AgentMemoryEntry is one decomposed rolling-memory item.
type AgentMemoryEntry struct {
	Cycle    int64   `json:"cycle"`
	Action   string  `json:"action"`
	Urgency  float64 `json:"urgency"`
	Sentence string  `json:"sentence"`
	Ago      string  `json:"ago"`
}

// AgentProposalEntry is one pending proposal on disk.
type AgentProposalEntry struct {
	File    string `json:"file"`
	Title   string `json:"title"`
	Type    string `json:"type"`
	Urgency string `json:"urgency"`
	Created string `json:"created"`
}

// AgentInboxSummary is the inbox/link-feed summary. Engine-side mirror
// of linkfeed.AgentInboxSummary so tool/HTTP handlers can pass through
// without importing linkfeed from internal/engine.
type AgentInboxSummary struct {
	RawCount          int                    `json:"raw_count"`
	EnrichedCount     int                    `json:"enriched_count"`
	FailedCount       int                    `json:"failed_count"`
	TotalCount        int                    `json:"total_count"`
	LastPull          string                 `json:"last_pull,omitempty"`
	LastPullAgo       string                 `json:"last_pull_ago,omitempty"`
	NextPullIn        string                 `json:"next_pull_in,omitempty"`
	RecentEnrichments []AgentInboxEnrichItem `json:"recent_enrichments,omitempty"`
}

// AgentInboxEnrichItem is one recent link-enrichment on the inbox.
type AgentInboxEnrichItem struct {
	Title       string `json:"title"`
	Connections int    `json:"connections"`
	Ago         string `json:"ago"`
}

// AgentCycleTrace is the full record of one agent cycle: what it saw
// (observation), what it decided (action+reason+urgency+target), how
// long it took, and what it produced (result).
type AgentCycleTrace struct {
	Cycle       int64   `json:"cycle"`
	Timestamp   string  `json:"timestamp"` // RFC3339
	DurationMs  int64   `json:"duration_ms"`
	Action      string  `json:"action"`
	Urgency     float64 `json:"urgency"`
	Reason      string  `json:"reason"`
	Target      string  `json:"target,omitempty"`
	Observation string  `json:"observation,omitempty"`
	Result      string  `json:"result,omitempty"`
}

// AgentSnapshot is the full state projection for GET /v1/agents/{id}.
type AgentSnapshot struct {
	Summary         AgentSummary          `json:"summary"`
	Activity        *AgentActivitySummary `json:"activity,omitempty"`
	Memory          []AgentMemoryEntry    `json:"memory,omitempty"`
	Proposals       []AgentProposalEntry  `json:"proposals,omitempty"`
	Inbox           *AgentInboxSummary    `json:"inbox,omitempty"`
	Traces          []AgentCycleTrace     `json:"traces,omitempty"`
	LastObservation string                `json:"last_observation,omitempty"`
	IdentityRef     string                `json:"identity_ref,omitempty"`
}

// AgentTriggerResult is the outcome of a POST /v1/agents/{id}/tick call.
// When wait=false, only Triggered/AgentID/CycleID/TriggerSeq/Message are
// populated. When wait=true, Action/Urgency/Reason/DurationMs/TimedOut
// carry the cycle's result.
type AgentTriggerResult struct {
	Triggered  bool   `json:"triggered"`
	AgentID    string `json:"agent_id"`
	CycleID    string `json:"cycle_id,omitempty"`
	TriggerSeq int64  `json:"trigger_seq"`
	Message    string `json:"message"`

	// Populated only when wait=true and the cycle observably completed.
	Action     string  `json:"action,omitempty"`
	Urgency    float64 `json:"urgency,omitempty"`
	Reason     string  `json:"reason,omitempty"`
	DurationMs int64   `json:"duration_ms,omitempty"`
	TimedOut   bool    `json:"timed_out,omitempty"`
}
