// sessions.go — kernel-native session & handoff registries.
//
// This is the "invariance layer" of the hybrid design sketched in
// cog://mem/semantic/surveys/2026-04-21-consolidation/agent-P-session-management-evaluation.
// The bus (BusSessionManager) remains ground truth; everything in this file is
// a derived view rebuilt from bus replay at startup. The registries' job is to
// enforce the state-machine invariants the bridge-only implementation was
// doing by convention:
//
//   - session_id format validation (regex) on register
//   - idempotent re-register = update-semantics (re-register a live session
//     updates the in-memory row; re-register after end is allowed only if the
//     prior session is ended or its heartbeat is outside the active window)
//   - heartbeat/end refused against unknown sessions
//   - end refused against already-ended sessions
//   - handoff.offer → claim → complete state machine with:
//   - first-wins claim (atomic check under handoff mutex)
//   - TTL enforcement (offer rejected if now - created_at > ttl_seconds)
//   - claim-before-offer rejection (phantom offers) as 404
//   - complete-before-claim rejected as 409
//   - on every rejected claim, emit a handoff.claim_rejected event on
//     bus_handoffs so operators have an audit trail (amendment #4 of the
//     user-approved plan).
//
// Everything is flushed to the bus via BusSessionManager.AppendEvent so the
// seq/hash chain remains the authoritative ledger. The in-memory maps only
// speed up reads and guard writes against out-of-order transitions.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// ─── well-known buses + event types ──────────────────────────────────────────
//
// These constants mirror the bridge-layer constants in
// cog-sandbox-mcp/src/cog_sandbox_mcp/tools/cogos_bridge.py. Keep them in sync.

const (
	BusSessions  = "bus_sessions"
	BusHandoffs  = "bus_handoffs"
	BusBroadcast = "bus_broadcast"

	EvtSessionRegister  = "session.register"
	EvtSessionHeartbeat = "session.heartbeat"
	EvtSessionEnd       = "session.end"

	EvtHandoffOffer         = "handoff.offer"
	EvtHandoffClaim         = "handoff.claim"
	EvtHandoffComplete      = "handoff.complete"
	EvtHandoffClaimRejected = "handoff.claim_rejected" // amendment #4

	// Default freshness window for "is this session still present" checks.
	// 600s matches the bridge's default; both are tunable per-call via the
	// active_within_seconds query param.
	defaultActiveWithinSeconds = 600

	// sessionReapEndReason is the EndReason recorded for rows ReapStale
	// force-ends. Emitted on BusSessions as an EvtSessionEnd event so replay
	// (ReplaySessionRegistry) treats reaped sessions identically to
	// explicitly-ended ones — no new event type needed.
	sessionReapEndReason = "ttl_reaper"

	// defaultSessionReapTTL is deliberately much larger than
	// defaultActiveWithinSeconds (600s) so the reaper never fights normal
	// heartbeat gaps — it only force-ends rows that have been silent far
	// longer than any live client's heartbeat cadence. cogos#423 observed
	// rows going un-reaped for two+ months.
	defaultSessionReapTTL = 24 * time.Hour

	// defaultSessionReapInterval is the sweep cadence for the background
	// reaper goroutine started in serve.go, mirroring
	// BusEventBroker.StartReaper's busSSEReaperInterval pattern.
	defaultSessionReapInterval = 60 * time.Second
)

// sessionIDPattern enforces the three-component hyphen-separated lowercase
// format the handoff protocol design spec requires. Three components —
// <machine>-<role>-<nonce> — or more. The first component is a single
// non-empty token; the second and third may themselves contain hyphens, which
// is why the pattern is written as "token-hyphenable-hyphenable" rather than
// "exactly three tokens."
//
// Example: node-a-laptop-cogos-gap-closure → passes.
//
//	alpha-beta-gamma                 → passes (exactly 3 tokens).
//	a-b                              → REJECTED (only 2 tokens).
var sessionIDPattern = regexp.MustCompile(`^[a-z0-9]+-[a-z0-9-]+-[a-z0-9-]+$`)

// ValidateSessionID returns nil iff id matches the lowercase-hyphen format
// with at least three components. Exported so tests and the HTTP handlers can
// share the single source of truth.
func ValidateSessionID(id string) error {
	if id == "" {
		return errors.New("session_id is required")
	}
	if !sessionIDPattern.MatchString(id) {
		return fmt.Errorf(
			"session_id %q must be ascii-lowercase with at least three "+
				"hyphen-separated components", id)
	}
	// Reject trailing/leading hyphens on inner components — the regex
	// alone would accept "a--b-c" which is visually confusing. A
	// single explicit double-hyphen check catches the common typos.
	if strings.Contains(id, "--") {
		return fmt.Errorf(
			"session_id %q must not contain consecutive hyphens", id)
	}
	return nil
}

// ─── Session registry ────────────────────────────────────────────────────────

// SessionState is the in-memory row for a session. Fields mirror the payload
// shape the bridge writes to bus_sessions so presence responses are
// byte-compat for external consumers.
type SessionState struct {
	SessionID    string                 `json:"session_id"`
	Workspace    string                 `json:"workspace"`
	Role         string                 `json:"role"`
	Task         string                 `json:"task,omitempty"`
	Model        string                 `json:"model,omitempty"`
	Hostname     string                 `json:"hostname,omitempty"`
	ContextUsage float64                `json:"context_usage,omitempty"`
	CurrentTask  string                 `json:"current_task,omitempty"`
	Status       string                 `json:"status,omitempty"`
	Extras       map[string]interface{} `json:"extras,omitempty"`

	// Lifecycle.
	RegisteredAt time.Time `json:"registered_at"`
	LastSeen     time.Time `json:"last_seen"`
	EndedAt      time.Time `json:"ended_at,omitempty"`
	EndReason    string    `json:"end_reason,omitempty"`
	EndHandoffID string    `json:"end_handoff_id,omitempty"`

	// Lifecycle flag, computed from EndedAt. Kept as its own JSON field so
	// presence responses don't need derivation on the client side.
	Ended bool `json:"ended"`
}

// IsActive returns true iff the session has been heard from within the given
// window AND has not been ended. Window ≤ 0 falls back to the protocol default.
func (s *SessionState) IsActive(window time.Duration, now time.Time) bool {
	if s.Ended {
		return false
	}
	if window <= 0 {
		window = time.Duration(defaultActiveWithinSeconds) * time.Second
	}
	return !s.LastSeen.IsZero() && now.Sub(s.LastSeen) <= window
}

// SessionRegistry is the in-memory, RWMutex-guarded map of session_id →
// SessionState. The bus is ground truth; this map is a derived, warm cache.
//
// The mutating methods take an optional `appendFn` callback that writes the
// corresponding event to the bus WHILE the registry lock is held. On
// appendFn() error, the in-memory row is left untouched so the derived view
// can never run ahead of the authoritative ledger. This fixes the inverted
// write-path critical raised in codex's PR#43 review.
type SessionRegistry struct {
	mu   sync.RWMutex
	rows map[string]*SessionState
}

// NewSessionRegistry returns an empty registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{rows: make(map[string]*SessionState)}
}

// Len returns the number of tracked sessions.
func (r *SessionRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rows)
}

// Get returns a copy of the session row, or (nil, false) if unknown.
func (r *SessionRegistry) Get(id string) (*SessionState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, false
	}
	cp := *row
	return &cp, true
}

// Snapshot returns a copy of every session row. Order is not guaranteed.
func (r *SessionRegistry) Snapshot() []*SessionState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*SessionState, 0, len(r.rows))
	for _, row := range r.rows {
		cp := *row
		out = append(out, &cp)
	}
	return out
}

// ApplyRegister records a session.register event into the map. Idempotent per
// amendment #2: same session_id updates the in-memory row; re-registration
// after end is allowed only if the prior session is ended or its heartbeat is
// outside the active window.
//
// If appendFn is non-nil it is invoked AFTER validation but BEFORE the
// registry mutation is committed, while the registry lock is held. The
// canonical RegisteredAt that will be stored on the row is passed to
// appendFn so the caller can embed the correct lineage value in the bus
// event payload — for a re-register of an active session this is the
// original registration time (not `now`). An error from appendFn aborts
// the mutation and is returned verbatim — the registry is left unchanged,
// preserving the bus-is-ground-truth invariant.
//
// Returns the resulting state (copy), a flag indicating whether the registry
// row was newly created vs updated, and an error.
func (r *SessionRegistry) ApplyRegister(
	state SessionState,
	activeWindow time.Duration,
	now time.Time,
	appendFn func(canonicalRegisteredAt time.Time) error,
) (*SessionState, bool, error) {
	if err := ValidateSessionID(state.SessionID); err != nil {
		return nil, false, err
	}
	if state.RegisteredAt.IsZero() {
		state.RegisteredAt = now
	}
	if state.LastSeen.IsZero() {
		state.LastSeen = state.RegisteredAt
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, found := r.rows[state.SessionID]
	if found && !existing.Ended {
		// Live prior row — allow re-register only if heartbeat is stale.
		if existing.IsActive(activeWindow, now) {
			// Still active; accept as update. Preserve the original
			// RegisteredAt to keep lineage. Fixes #44: the canonical value
			// is passed to appendFn so the bus event payload also reflects
			// the preserved timestamp rather than the caller's `now`.
			state.RegisteredAt = existing.RegisteredAt
		}
	}
	// Clear ended flags if re-registering (fresh lifecycle).
	state.Ended = false
	state.EndedAt = time.Time{}
	state.EndReason = ""
	state.EndHandoffID = ""
	// Append to the bus FIRST — if that fails, the registry is unchanged.
	if appendFn != nil {
		if err := appendFn(state.RegisteredAt); err != nil {
			return nil, false, err
		}
	}
	row := state
	r.rows[state.SessionID] = &row
	cp := row
	return &cp, !found, nil
}

// ApplyHeartbeat bumps LastSeen + optional fields. Returns (state, ok, err).
//   - ok=false when session is unknown → caller returns 404.
//   - ok=true with non-nil err when the session was already ended. In that
//     case the registry is NOT mutated (no LastSeen update, no status bump)
//     and no appendFn is invoked — the caller translates to 409.
//   - ok=true, err=nil on success — the mutation and (optional) bus append
//     both happened atomically under the registry lock.
//
// appendFn, if non-nil, is invoked AFTER validation/ended-check but BEFORE
// the LastSeen mutation commits. An appendFn error aborts the mutation and
// is returned verbatim so the registry stays in lockstep with the bus.
func (r *SessionRegistry) ApplyHeartbeat(
	id string,
	contextUsage float64,
	status, currentTask string,
	now time.Time,
	appendFn func() error,
) (*SessionState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, false, nil
	}
	if row.Ended {
		// Return the CURRENT (unmutated) state so the caller can still
		// surface fields like EndedAt if needed, but signal the rejection.
		cp := *row
		return &cp, true, errors.New("session already ended")
	}
	// Append FIRST — if that fails, LastSeen is NOT bumped.
	if appendFn != nil {
		if err := appendFn(); err != nil {
			return nil, true, err
		}
	}
	row.LastSeen = now
	if contextUsage > 0 {
		row.ContextUsage = contextUsage
	}
	if status != "" {
		row.Status = status
	}
	if currentTask != "" {
		row.CurrentTask = currentTask
	}
	cp := *row
	return &cp, true, nil
}

// ApplyEnd transitions a session to ended. Returns:
//   - (nil, false, nil)            when the session is unknown → 404.
//   - (&state, true, errAlready)   when the session was already ended → 409.
//   - (&state, true, nil)          on success.
//   - (nil, true, appendErr)       if appendFn failed — registry unchanged.
//
// appendFn runs AFTER validation but BEFORE the Ended transition commits,
// under the registry lock. If it errors, no mutation is applied.
func (r *SessionRegistry) ApplyEnd(
	id, reason, handoffID string,
	now time.Time,
	appendFn func() error,
) (*SessionState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, false, nil
	}
	if row.Ended {
		cp := *row
		return &cp, true, errors.New("session already ended")
	}
	if appendFn != nil {
		if err := appendFn(); err != nil {
			return nil, true, err
		}
	}
	row.Ended = true
	row.EndedAt = now
	row.EndReason = reason
	row.EndHandoffID = handoffID
	row.LastSeen = now // treat end as implicit last contact
	cp := *row
	return &cp, true, nil
}

// ReapStale is the TTL reaper for cogos#423: sessions that were never
// explicitly ended (crash, kill -9, dropped client) otherwise persist with
// Ended=false forever, since ApplyEnd only ever runs from the explicit
// POST /v1/sessions/{id}/end path. ReapStale finds every unended row whose
// LastSeen is older than ttl (relative to now) and force-ends it.
//
// Same append-before-mutate invariant as ApplyEnd: for each stale row,
// appendFn(id) is called first, under the registry lock, so the caller can
// emit a session.end bus event preserving the bus-is-ground-truth
// contract. If appendFn errors for a given row, that row is left
// unmutated and simply retried on the next sweep — one row's append
// failure must not block reaping the rest of the sweep. Returns the
// session_ids that were successfully reaped.
func (r *SessionRegistry) ReapStale(ttl time.Duration, now time.Time, appendFn func(id string) error) []string {
	if ttl <= 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var reaped []string
	for id, row := range r.rows {
		if row.Ended {
			continue
		}
		if row.LastSeen.IsZero() || now.Sub(row.LastSeen) <= ttl {
			continue
		}
		if appendFn != nil {
			if err := appendFn(id); err != nil {
				continue
			}
		}
		row.Ended = true
		row.EndedAt = now
		row.EndReason = sessionReapEndReason
		row.LastSeen = now
		reaped = append(reaped, id)
	}
	return reaped
}

// ─── Handoff registry ────────────────────────────────────────────────────────

// HandoffLifecycle values.
const (
	HandoffStateOpen      = "open"
	HandoffStateClaimed   = "claimed"
	HandoffStateCompleted = "completed"
)

// HandoffState is the in-memory row for a handoff.
type HandoffState struct {
	HandoffID   string `json:"handoff_id"`
	FromSession string `json:"from_session"`
	ToSession   string `json:"to_session,omitempty"`
	Reason      string `json:"reason,omitempty"`

	// Full offer payload (mirror of what went on the bus). Kept verbatim so
	// claimants can read it back without a separate bus fetch.
	OfferPayload map[string]interface{} `json:"offer,omitempty"`

	CreatedAt  time.Time `json:"created_at"`
	TTLSeconds int       `json:"ttl_seconds,omitempty"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`

	State             string    `json:"state"`
	ClaimingSession   string    `json:"claiming_session,omitempty"`
	ClaimedAt         time.Time `json:"claimed_at,omitempty"`
	CompletingSession string    `json:"completing_session,omitempty"`
	CompletedAt       time.Time `json:"completed_at,omitempty"`
	CompletionOutcome string    `json:"outcome,omitempty"`
	CompletionNotes   string    `json:"notes,omitempty"`
	NextHandoffID     string    `json:"next_handoff_id,omitempty"`
}

// IsExpired is true when TTL > 0 and now is past ExpiresAt.
func (h *HandoffState) IsExpired(now time.Time) bool {
	if h.TTLSeconds <= 0 || h.ExpiresAt.IsZero() {
		return false
	}
	return now.After(h.ExpiresAt)
}

// HandoffRegistry guards in-memory handoff state. The bus is still the
// source of truth; this struct is how we atomically enforce first-wins on
// claim and state-order on complete.
//
// The mutex is held across the bus append in mutating methods (via the
// appendFn callback). Yes this serializes concurrent offers/claims/completes
// against disk I/O, but: (a) these operations are low-rate, (b) the append
// is local disk — typical <10ms — and (c) correctness demands the bus-append
// and registry mutation commit as one atomic unit so the derived view can't
// run ahead of the ground-truth ledger.
type HandoffRegistry struct {
	mu   sync.RWMutex // write-heavy callers take Lock(); read paths use RLock()
	rows map[string]*HandoffState
}

// NewHandoffRegistry returns an empty registry.
func NewHandoffRegistry() *HandoffRegistry {
	return &HandoffRegistry{rows: make(map[string]*HandoffState)}
}

// Len returns the number of tracked handoffs (open + claimed + completed).
func (r *HandoffRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rows)
}

// Get returns a copy, or (nil, false).
func (r *HandoffRegistry) Get(id string) (*HandoffState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, false
	}
	cp := *row
	return &cp, true
}

// Snapshot returns a copy of every handoff row.
func (r *HandoffRegistry) Snapshot() []*HandoffState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*HandoffState, 0, len(r.rows))
	for _, row := range r.rows {
		cp := *row
		out = append(out, &cp)
	}
	return out
}

// ApplyOffer records a handoff offer. Idempotent on duplicate IDs — replaces
// the row; this shouldn't happen in normal flow (IDs are unique) but mirrors
// the bus semantics (re-emitting the same offer would just append another
// event).
//
// appendFn, if non-nil, runs under the registry lock BEFORE the mutation is
// committed. An error aborts the mutation and is returned; on nil-error the
// in-memory row is installed atomically with the bus append.
func (r *HandoffRegistry) ApplyOffer(
	h HandoffState, now time.Time, appendFn func() error,
) (*HandoffState, error) {
	if h.CreatedAt.IsZero() {
		h.CreatedAt = now
	}
	if h.TTLSeconds > 0 && h.ExpiresAt.IsZero() {
		h.ExpiresAt = h.CreatedAt.Add(time.Duration(h.TTLSeconds) * time.Second)
	}
	h.State = HandoffStateOpen
	r.mu.Lock()
	defer r.mu.Unlock()
	if appendFn != nil {
		if err := appendFn(); err != nil {
			return nil, err
		}
	}
	row := h
	r.rows[h.HandoffID] = &row
	cp := row
	return &cp, nil
}

// ClaimRejection reasons for the handoff.claim_rejected event (amendment #4).
type ClaimRejection string

const (
	ClaimRejectedOfferNotFound  ClaimRejection = "offer_not_found"
	ClaimRejectedAlreadyClaimed ClaimRejection = "already_claimed"
	ClaimRejectedTTLExpired     ClaimRejection = "ttl_expired"
	ClaimRejectedOutOfOrder     ClaimRejection = "out_of_order"
)

// ClaimResult is what the handler returns after an atomic claim attempt.
type ClaimResult struct {
	Offer              *HandoffState
	Rejection          ClaimRejection // empty on success
	ConflictingSession string         // set on already_claimed
}

// ApplyClaim is the ATOMIC first-wins check + bus-append path. Semantics:
//
//   - If the offer is missing, already claimed/completed, or TTL-expired,
//     returns a ClaimResult with a non-empty Rejection; NO mutation, and
//     appendFn is NOT invoked (the caller emits handoff.claim_rejected
//     independently for audit).
//   - Otherwise appendFn is invoked WHILE THE LOCK IS HELD. If it errors,
//     the claim is aborted — the in-memory row stays open and the error is
//     returned on the second return value. The "losing" session thus sees
//     an internal error, not a conflict, and can retry.
//   - On appendFn success, the row transitions to Claimed and the second
//     return value is nil. This makes the claim first-wins on the bus as
//     well as in memory — the whole point of the PR.
//
// Holding a lock across disk I/O is deliberate. Claims are rare, appends are
// local (<10ms typical), and correctness here beats throughput. See codex
// review of PR#43.
func (r *HandoffRegistry) ApplyClaim(
	id, claimingSession string,
	now time.Time,
	appendFn func() error,
) (ClaimResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[id]
	if !ok {
		return ClaimResult{Rejection: ClaimRejectedOfferNotFound}, nil
	}
	if row.State == HandoffStateClaimed || row.State == HandoffStateCompleted {
		return ClaimResult{
			Rejection:          ClaimRejectedAlreadyClaimed,
			ConflictingSession: row.ClaimingSession,
		}, nil
	}
	if row.IsExpired(now) {
		return ClaimResult{Rejection: ClaimRejectedTTLExpired}, nil
	}
	// All invariants pass — write to the bus FIRST, under the lock, so the
	// winning claim is durable before the derived view reflects it.
	if appendFn != nil {
		if err := appendFn(); err != nil {
			return ClaimResult{}, err
		}
	}
	row.State = HandoffStateClaimed
	row.ClaimingSession = claimingSession
	row.ClaimedAt = now
	cp := *row
	return ClaimResult{Offer: &cp}, nil
}

// ApplyComplete transitions a claimed handoff to completed. Returns:
//   - (nil,   ClaimRejectedOfferNotFound, nil)      when unknown     → 404.
//   - (&row,  ClaimRejectedOutOfOrder,    nil)      not claimed yet  → 409.
//   - (nil,   "",                         appendErr) append failed.
//   - (&row,  "",                         nil)      success.
//
// appendFn runs under the lock AFTER the state-order check but BEFORE the
// Completed transition commits; an error aborts the mutation.
func (r *HandoffRegistry) ApplyComplete(
	id, completingSession, outcome, notes, nextHandoffID string,
	now time.Time,
	appendFn func() error,
) (*HandoffState, ClaimRejection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[id]
	if !ok {
		return nil, ClaimRejectedOfferNotFound, nil
	}
	if row.State != HandoffStateClaimed {
		cp := *row
		return &cp, ClaimRejectedOutOfOrder, nil
	}
	if appendFn != nil {
		if err := appendFn(); err != nil {
			return nil, "", err
		}
	}
	row.State = HandoffStateCompleted
	row.CompletingSession = completingSession
	row.CompletedAt = now
	row.CompletionOutcome = outcome
	row.CompletionNotes = notes
	row.NextHandoffID = nextHandoffID
	cp := *row
	return &cp, "", nil
}

// ─── Replay-from-bus warmers ─────────────────────────────────────────────────

// ReplaySessionRegistry reads bus_sessions events through the given manager
// and reconstructs the in-memory session map. The bus is ground truth; this
// function just gets the derived view ready for read-traffic before the HTTP
// server starts serving. Errors are logged but non-fatal — an empty registry
// is a safe degraded start.
//
// Events are sorted ascending by Seq before replay so the reconstructed
// state is deterministic regardless of on-disk line order. ReadEvents
// already de-dupes by Seq; a seq collision with conflicting payloads keeps
// the FIRST occurrence (insertion order). Callers that need "last-write-wins
// within a seq" semantics should not rely on replay ordering beyond Seq.
func ReplaySessionRegistry(mgr *BusSessionManager, reg *SessionRegistry) error {
	if mgr == nil || reg == nil {
		return nil
	}
	events, err := mgr.ReadEvents(BusSessions)
	if err != nil {
		slog.Warn("sessions: replay read failed", "bus", BusSessions, "err", err)
		return err
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
	// bridge-v0.1 payload shape: the entire session dict lives at the top
	// level of the bus event's payload map. Some bridge code writes a
	// nested "content" (the message text) plus flat fields; we tolerate
	// both by preferring explicit top-level keys and falling back to
	// reasonable defaults.
	for _, evt := range events {
		switch evt.Type {
		case EvtSessionRegister:
			state := parseSessionPayload(evt.From, evt.Payload)
			if state == nil {
				continue
			}
			// Prefer the registered_at field embedded in the payload — after
			// the #44 fix this value is the canonical preserved timestamp for
			// re-registrations. Fall back to evt.Ts for events written before
			// the fix (legacy shape: payload had registered_at=now, not T1).
			if ra, _ := evt.Payload["registered_at"].(string); ra != "" {
				if ts, err := parseBusTS(ra); err == nil {
					state.RegisteredAt = ts
				}
			}
			if state.RegisteredAt.IsZero() {
				if ts, err := parseBusTS(evt.Ts); err == nil {
					state.RegisteredAt = ts
				}
			}
			if state.LastSeen.IsZero() {
				if ts, err := parseBusTS(evt.Ts); err == nil {
					state.LastSeen = ts
				}
			}
			// Replay bypasses the "refuse re-register when live" check —
			// we're reconstructing history, not enforcing invariants.
			reg.mu.Lock()
			row := *state
			reg.rows[state.SessionID] = &row
			reg.mu.Unlock()

		case EvtSessionHeartbeat:
			payload := evt.Payload
			if payload == nil {
				continue
			}
			id, _ := payload["session_id"].(string)
			if id == "" {
				id = evt.From
			}
			if id == "" {
				continue
			}
			reg.mu.Lock()
			row, ok := reg.rows[id]
			if ok {
				if ts, err := parseBusTS(evt.Ts); err == nil {
					row.LastSeen = ts
				}
				if v, ok := payload["context_usage"].(float64); ok && v > 0 {
					row.ContextUsage = v
				}
				if s, _ := payload["status"].(string); s != "" {
					row.Status = s
				}
				if t, _ := payload["current_task"].(string); t != "" {
					row.CurrentTask = t
				}
			}
			reg.mu.Unlock()

		case string(KindSessionFork):
			// RFC-0005 fork event. The child session is never separately
			// registered via EvtSessionRegister — cog_fork_session and the
			// HTTP fork handler both call SessionRegistry.ApplyRegister with
			// appendFn=nil (the fork bus event IS the durable record of the
			// child's existence). Without this case, the child row is
			// present only in the in-memory registry and is silently
			// dropped on cold restart. Reconstruct the child row here from
			// the fork body, mirroring deriveChildState's derivation logic
			// (mcp_fork_session.go) so replay and live-path produce the same
			// shape.
			body := parseSessionForkPayload(evt.Payload)
			if body == nil || body.ChildSessionID == "" {
				continue
			}
			regAt := evt.Ts
			registeredAt, _ := parseBusTS(regAt)
			child := SessionState{
				SessionID:    body.ChildSessionID,
				Role:         "fork-child",
				RegisteredAt: registeredAt,
				LastSeen:     registeredAt,
			}
			reg.mu.Lock()
			if parent, ok := reg.rows[body.ParentSessionID]; ok {
				child.Workspace = parent.Workspace
				child.Role = parent.Role
				child.Model = parent.Model
				child.Hostname = parent.Hostname
			}
			reg.mu.Unlock()
			if body.Overlay.Role != nil && body.Overlay.Role.Role != "" {
				child.Role = body.Overlay.Role.Role
			}
			reg.mu.Lock()
			row := child
			reg.rows[body.ChildSessionID] = &row
			reg.mu.Unlock()

		case EvtSessionEnd:
			payload := evt.Payload
			if payload == nil {
				continue
			}
			id, _ := payload["session_id"].(string)
			if id == "" {
				id = evt.From
			}
			if id == "" {
				continue
			}
			reason, _ := payload["reason"].(string)
			handoffID, _ := payload["handoff_id"].(string)
			reg.mu.Lock()
			row, ok := reg.rows[id]
			if ok {
				row.Ended = true
				if ts, err := parseBusTS(evt.Ts); err == nil {
					row.EndedAt = ts
					row.LastSeen = ts
				}
				row.EndReason = reason
				row.EndHandoffID = handoffID
			}
			reg.mu.Unlock()
		}
	}
	slog.Info("sessions: replay complete", "sessions", reg.Len(), "events", len(events))
	return nil
}

// ReplayHandoffRegistry replays bus_handoffs into the handoff registry.
// Events are sorted by Seq ascending before consumption so replay is
// deterministic even if the on-disk file has out-of-order lines (e.g. from
// a resumed write after crash). See also ReplaySessionRegistry.
func ReplayHandoffRegistry(mgr *BusSessionManager, reg *HandoffRegistry) error {
	if mgr == nil || reg == nil {
		return nil
	}
	events, err := mgr.ReadEvents(BusHandoffs)
	if err != nil {
		slog.Warn("handoffs: replay read failed", "bus", BusHandoffs, "err", err)
		return err
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
	for _, evt := range events {
		payload := evt.Payload
		if payload == nil {
			continue
		}
		switch evt.Type {
		case EvtHandoffOffer:
			h := parseHandoffOffer(evt.Payload)
			if h == nil {
				continue
			}
			if ts, err := parseBusTS(evt.Ts); err == nil && h.CreatedAt.IsZero() {
				h.CreatedAt = ts
				if h.TTLSeconds > 0 && h.ExpiresAt.IsZero() {
					h.ExpiresAt = ts.Add(time.Duration(h.TTLSeconds) * time.Second)
				}
			}
			h.State = HandoffStateOpen
			reg.mu.Lock()
			row := *h
			reg.rows[h.HandoffID] = &row
			reg.mu.Unlock()

		case EvtHandoffClaim:
			id, _ := payload["handoff_id"].(string)
			claimant, _ := payload["claiming_session"].(string)
			if id == "" {
				continue
			}
			reg.mu.Lock()
			row, ok := reg.rows[id]
			// On replay, honor first-wins by seq: only the first claim
			// for a given id transitions state.
			if ok && row.State == HandoffStateOpen {
				row.State = HandoffStateClaimed
				row.ClaimingSession = claimant
				if ts, err := parseBusTS(evt.Ts); err == nil {
					row.ClaimedAt = ts
				}
			}
			reg.mu.Unlock()

		case EvtHandoffComplete:
			id, _ := payload["handoff_id"].(string)
			session, _ := payload["completing_session"].(string)
			outcome, _ := payload["outcome"].(string)
			notes, _ := payload["notes"].(string)
			nextID, _ := payload["next_handoff_id"].(string)
			if id == "" {
				continue
			}
			reg.mu.Lock()
			row, ok := reg.rows[id]
			if ok && row.State == HandoffStateClaimed {
				row.State = HandoffStateCompleted
				row.CompletingSession = session
				row.CompletionOutcome = outcome
				row.CompletionNotes = notes
				row.NextHandoffID = nextID
				if ts, err := parseBusTS(evt.Ts); err == nil {
					row.CompletedAt = ts
				}
			}
			reg.mu.Unlock()
		}
	}
	slog.Info("handoffs: replay complete", "handoffs", reg.Len(), "events", len(events))
	return nil
}

// ─── payload helpers ─────────────────────────────────────────────────────────

// parseSessionPayload reconstructs a SessionState from a bus event's payload.
// The bridge writes nearly everything to the top level. Unknown keys are
// copied to Extras so no information is lost.
func parseSessionPayload(fromField string, p map[string]interface{}) *SessionState {
	if p == nil {
		return nil
	}
	id, _ := p["session_id"].(string)
	if id == "" {
		id = fromField
	}
	if id == "" {
		return nil
	}
	state := &SessionState{SessionID: id}
	pickString(&state.Workspace, p, "workspace")
	pickString(&state.Role, p, "role")
	pickString(&state.Task, p, "task")
	pickString(&state.Model, p, "model")
	pickString(&state.Hostname, p, "hostname")
	pickString(&state.Status, p, "status")
	pickString(&state.CurrentTask, p, "current_task")
	if v, ok := p["context_usage"].(float64); ok {
		state.ContextUsage = v
	}
	// Stash any other fields for debuggability.
	known := map[string]bool{
		"session_id": true, "workspace": true, "role": true, "task": true,
		"model": true, "hostname": true, "status": true,
		"current_task": true, "context_usage": true, "content": true,
	}
	for k, v := range p {
		if known[k] {
			continue
		}
		if state.Extras == nil {
			state.Extras = map[string]interface{}{}
		}
		state.Extras[k] = v
	}
	return state
}

// parseSessionForkPayload reconstructs a SessionForkBody from a session.fork
// bus event's payload map. The payload is written by both fork call sites
// (mcp_fork_session.go, serve_fork_session.go) via json.Marshal(body) then
// json.Unmarshal into map[string]interface{} — so a JSON round-trip back
// into the struct is exact and tolerates the nested Overlay/PinnedUntil
// shapes without hand-picking each field (unlike parseSessionPayload, which
// deals with a looser bridge-authored shape).
func parseSessionForkPayload(p map[string]interface{}) *SessionForkBody {
	if p == nil {
		return nil
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	var body SessionForkBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil
	}
	if body.ChildSessionID == "" {
		return nil
	}
	return &body
}

// parseHandoffOffer reconstructs a HandoffState from a bus.offer payload.
// The bridge writes the whole offer dict verbatim to the payload.
func parseHandoffOffer(p map[string]interface{}) *HandoffState {
	if p == nil {
		return nil
	}
	id, _ := p["handoff_id"].(string)
	if id == "" {
		return nil
	}
	h := &HandoffState{HandoffID: id}
	pickString(&h.FromSession, p, "from_session")
	pickString(&h.ToSession, p, "to_session")
	pickString(&h.Reason, p, "reason")
	if v, ok := p["ttl_seconds"].(float64); ok {
		h.TTLSeconds = int(v)
	}
	if s, _ := p["created_at"].(string); s != "" {
		if ts, err := parseBusTS(s); err == nil {
			h.CreatedAt = ts
		}
	}
	// Keep the whole payload verbatim so claimants can read it back.
	h.OfferPayload = copyMap(p)
	return h
}

func pickString(dst *string, p map[string]interface{}, key string) {
	if v, ok := p[key].(string); ok {
		*dst = v
	}
}

func copyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func parseBusTS(ts string) (time.Time, error) {
	// The bus writes RFC3339Nano; tolerate RFC3339 too.
	if ts == "" {
		return time.Time{}, errors.New("empty ts")
	}
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, strings.TrimSpace(ts))
}
