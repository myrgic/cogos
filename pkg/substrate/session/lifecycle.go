// lifecycle.go — Session-lifecycle schema for the Context Engine.
//
// A CogOS conversation may span multiple Claude CLI sessions via rotation. The types below describe
// that lifecycle — current state, retired rotations, persistent working memory
// across rotations, and the tunable thresholds that govern rotation.
//
// Per ADR-100 Step 3d (full untangle path; the minimal path landed `Tracking`
// in tracking.go): these declarations are pure schema with stdlib + time
// dependencies only. The session manager that operates on them
// (root session_manager.go) stays kernel-resident because it owns the
// concurrency invariants, the thread-keyed map, and rotation policy logic.
//
// Naming per ADR-100 substrate convention: the redundant "Session" prefix
// is dropped now that the types live in package session. Callers at the
// root use the aliases declared in context_engine_types.go.
package session

import "time"

// State tracks a CogOS-managed conversation across Claude CLI sessions.
// A CogOS conversation may span multiple Claude CLI sessions via rotation.
type State struct {
	ID              string    // CogOS session ID (stable across Claude rotations)
	ThreadID        string    // Thread this session belongs to
	ClaudeSessionID string    // Current Claude CLI session (may rotate)
	CreatedAt       time.Time
	LastActiveAt    time.Time
	TurnCount       int
	TotalTokensSent int            // Running total for pressure tracking
	WorkingMemory   *WorkingMemory
	History         []Rotation     // Past Claude sessions for this conversation
}

// Rotation records a retired Claude CLI session.
type Rotation struct {
	ClaudeSessionID string
	StartedAt       time.Time
	EndedAt         time.Time
	Reason          string // "pressure", "drift", "explicit", "idle", "error"
	TurnCount       int
}

// WorkingMemory persists across Claude session rotations. This is the
// continuity layer: what survives when the manager starts a fresh Claude
// session.
type WorkingMemory struct {
	ActiveTopics    []string          `json:"active_topics,omitempty"`
	KeyDecisions    []string          `json:"key_decisions,omitempty"`
	ActiveArtifacts []string          `json:"active_artifacts,omitempty"`
	UserPreferences map[string]string `json:"user_preferences,omitempty"`
	Summary         string            `json:"summary,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

// ManagerConfig holds tunable thresholds for session rotation.
type ManagerConfig struct {
	MaxTurnsBeforeRotation  int           // default: 50
	MaxTokensBeforeRotation int           // default: 500_000 (cumulative)
	IdleTimeout             time.Duration // default: 30min
}

// DefaultManagerConfig returns sensible defaults.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxTurnsBeforeRotation:  50,
		MaxTokensBeforeRotation: 500_000,
		IdleTimeout:             30 * time.Minute,
	}
}
