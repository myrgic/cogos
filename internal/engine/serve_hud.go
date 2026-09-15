// serve_hud.go — GET /v1/hud/state
//
// Returns a compact, structured snapshot of kernel state for the Claude Code
// UserPromptSubmit HUD hook. Designed to be cheap (<50ms, no inference) and
// consumed by the hook script in cog-workspace every prompt turn.
//
//	GET /v1/hud/state
//	200 → HUDState JSON (see type below)
//
// Fields:
//   - identity: node id, nucleus name, trust score
//   - sessions: active foveated-context sessions (from SessionContextStore)
//   - recent_uris: top-N fovea entries from the attentional field
//   - node_health: per-service health from the last probe
//   - kernel: state, version, workspace root, started_at
//
// All fields degrade gracefully to zero values when the underlying data is
// unavailable (nil nucleus, no sessions, empty field). The hook caller must
// not assume any field is non-nil.
package engine

import (
	"encoding/json"
	"net/http"
	"time"
)

// HUDState is the response shape for GET /v1/hud/state.
type HUDState struct {
	// Identity reports which node this is and which seat (nucleus) is active.
	Identity HUDIdentity `json:"identity"`

	// Sessions lists all foveated-context sessions the kernel has seen.
	Sessions []HUDSession `json:"sessions"`

	// RecentURIs are the top attentional-field entries — the paths most
	// recently in the kernel's fovea. Up to 10 entries, ordered by score.
	RecentURIs []HUDURI `json:"recent_uris"`

	// NodeHealth reports the last-probed status of sibling services.
	// Map key is service name (e.g. "mod3", "cogos").
	NodeHealth map[string]string `json:"node_health"`

	// Kernel reports operational state of the running kernel.
	Kernel HUDKernel `json:"kernel"`

	// Timestamp is when this snapshot was taken.
	Timestamp time.Time `json:"timestamp"`
}

// HUDIdentity captures the node's identity at snapshot time.
type HUDIdentity struct {
	NodeID     string  `json:"node_id"`
	Name       string  `json:"name,omitempty"`        // nucleus name (seat)
	TrustScore float64 `json:"trust_score,omitempty"` // local trust score [0,1]
}

// HUDSession summarises one active foveated-context session.
type HUDSession struct {
	SessionID     string    `json:"session_id"`
	Profile       string    `json:"profile"`
	TurnNumber    int       `json:"turn_number"`
	IrisPressure  float64   `json:"iris_pressure"` // context-window fill [0,1]
	TotalTokens   int       `json:"total_tokens"`
	LastRequestAt time.Time `json:"last_request_at"`
}

// HUDUriEntry is one entry from the attentional field fovea.
type HUDURI struct {
	Path  string  `json:"path"`
	Score float64 `json:"score"`
}

// HUDKernel reports kernel operational state.
type HUDKernel struct {
	State         string    `json:"state"`
	Version       string    `json:"version"`
	WorkspaceRoot string    `json:"workspace_root"`
	StartedAt     time.Time `json:"started_at"`
}

// handleHUDState serves GET /v1/hud/state.
//
// All reads are lock-free where possible (Snapshot() methods return copies).
// No inference, no disk I/O. Target: <50ms wall time.
func (s *Server) handleHUDState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	now := time.Now().UTC()

	// Identity block.
	identity := HUDIdentity{
		NodeID: s.process.NodeID,
	}
	if s.nucleus != nil {
		s.nucleus.mu.RLock()
		identity.Name = s.nucleus.Name
		s.nucleus.mu.RUnlock()
	}
	trust := s.process.TrustSnapshot()
	identity.TrustScore = trust.LocalScore

	// Active sessions: merge SessionContextStore (foveated chat sessions) with
	// ChannelSessionRegistry (mod3 channel sessions). The channel registry
	// contains live mod3 dashboard sessions; the context store contains sessions
	// that have made /v1/chat/completions requests to the kernel. Both are
	// populated from separate code paths — include both so the HUD shows all
	// active sessions regardless of which path each session took.
	sessionSet := make(map[string]HUDSession)
	for _, st := range s.sessions.Snapshot() {
		sessionSet[st.SessionID] = HUDSession{
			SessionID:     st.SessionID,
			Profile:       st.Profile,
			TurnNumber:    st.TurnNumber,
			IrisPressure:  st.IrisPressure,
			TotalTokens:   st.TotalTokens,
			LastRequestAt: st.LastRequestAt,
		}
	}
	// Layer channel-session records on top (or as additions).
	for _, rec := range s.channelSessionRegistry.Snapshot() {
		if _, exists := sessionSet[rec.SessionID]; !exists {
			sessionSet[rec.SessionID] = HUDSession{
				SessionID:     rec.SessionID,
				Profile:       rec.ParticipantType,
				LastRequestAt: rec.LastSeen,
			}
		}
	}
	sessions := make([]HUDSession, 0, len(sessionSet))
	for _, s := range sessionSet {
		sessions = append(sessions, s)
	}

	// Recent URIs from the attentional field fovea (top 10).
	// Use BaseFovea (no inbox-raw boost) so chatgpt-archive inbox entries
	// don't dominate the list — same as the chat-read view in context_blocks.go.
	foveaEntries := s.process.Field().BaseFovea(10)
	recentURIs := make([]HUDURI, 0, len(foveaEntries))
	for _, fs := range foveaEntries {
		recentURIs = append(recentURIs, HUDURI{
			Path:  fs.Path,
			Score: fs.Score,
		})
	}

	// Node health summary.
	// If no probe has fired yet (empty map), trigger one on-demand so the HUD
	// reports the same data as /health. This is safe: Probe() is designed for
	// concurrent goroutine use and the 2s per-probe timeout is bounded.
	var nodeHealth map[string]string
	if nh := s.process.NodeHealth(); nh != nil {
		if nm := s.process.NodeManifest(); nm != nil && len(nh.Summary()) == 0 {
			nh.Probe(nm, s.cfg.Port)
		}
		nodeHealth = nh.Summary()
	}
	if nodeHealth == nil {
		nodeHealth = map[string]string{}
	}

	// Kernel state.
	kernel := HUDKernel{
		State:         s.process.State().String(),
		Version:       Version,
		WorkspaceRoot: s.cfg.WorkspaceRoot,
		StartedAt:     s.process.StartedAt(),
	}

	resp := HUDState{
		Identity:   identity,
		Sessions:   sessions,
		RecentURIs: recentURIs,
		NodeHealth: nodeHealth,
		Kernel:     kernel,
		Timestamp:  now,
	}

	_ = json.NewEncoder(w).Encode(resp)
}
