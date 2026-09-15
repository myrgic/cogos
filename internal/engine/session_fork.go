// session_fork.go — session.fork Kind handler, fork registry, and lineage
// projection (RFC-0005).
//
// The Kind constant and handler are registered in init() via RegisterKindHandler
// (ADR-090 pattern). No modification to any Kind-dispatch switch is required.
//
// ForkRegistry holds the in-memory derived view of fork relationships:
//   - parent → live children mapping (for GC root invariant)
//   - per-child expiry tracking (7-day default; pin field overrides)
//
// The bus (BusSessionManager) remains ground truth. ForkRegistry is a warm
// derived cache rebuilt from bus replay at startup, following the same pattern
// as SessionRegistry in sessions.go.
//
// ForkChildren / ForkAncestors are the internal projection functions that
// future lineage MCP tools (post-v0.5.0) will wrap. They are NOT exposed as
// MCP tools in v0.5.0 (YAGNI deferral per RFC-0005 §Future scope).
package engine

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// ─── Kind constant ────────────────────────────────────────────────────────────

// KindSessionFork is the CogBlock Kind emitted when a session is forked.
// The block's RawPayload unmarshals to SessionForkBody.
const KindSessionFork CogBlockKind = "session.fork"

// ─── Kind handler registration ───────────────────────────────────────────────

func init() {
	RegisterKindHandler(KindSessionFork, handleSessionForkBlock)
}

// handleSessionForkBlock is the KindHandler for session.fork blocks. It
// updates the in-process ForkRegistry with the new fork relationship when
// a session.fork CogBlock is dispatched through the Kind registry.
//
// In v0.5.0 the kernel does not maintain a global ForkRegistry singleton;
// the handler is a no-op at the DispatchKind call site (block routing) because
// fork operations go through the MCPServer / HTTP handler paths which
// update their own ForkRegistry directly. The Kind handler is registered so
// that future routing code (e.g. the autonomic membrane) can dispatch
// session.fork blocks without needing a switch statement.
//
// Structured log: emits operation=fork_block_dispatched per RFC-0005 §Structured logs.
func handleSessionForkBlock(block *CogBlock) error {
	if block == nil {
		return fmt.Errorf("session.fork handler: nil block")
	}
	var body SessionForkBody
	if len(block.RawPayload) > 0 {
		if err := json.Unmarshal(block.RawPayload, &body); err != nil {
			slog.Warn("session.fork: failed to unmarshal fork body",
				"operation", "fork_block_dispatched",
				"block_id", block.ID,
				"err", err,
			)
			// Non-fatal: a malformed body doesn't prevent routing.
			return nil
		}
	}
	slog.Info("session.fork: block dispatched",
		"operation", "fork_block_dispatched",
		"parent_session_id", body.ParentSessionID,
		"child_session_id", body.ChildSessionID,
		"overlay_layers", body.Overlay.OverlayLayers(),
		"ts", block.Timestamp,
	)
	return nil
}

// ─── ForkRegistry ────────────────────────────────────────────────────────────

// forkEntry is one row in the ForkRegistry's live-children map.
type forkEntry struct {
	body      SessionForkBody
	expiresAt time.Time // when this child fork is eligible for GC
}

// DefaultForkRetention is the default time-to-live for a fork child entry
// from the fork point. Corresponds to RFC-0005 §Garbage collection "7 days
// from the fork point".
const DefaultForkRetention = 7 * 24 * time.Hour

// ForkRegistry is the in-memory derived view of fork relationships.
// It maps parent session IDs to their live fork children, and tracks
// per-child GC eligibility.
//
// The bus is ground truth; this registry is a derived warm cache rebuilt
// from replay on startup.
type ForkRegistry struct {
	mu sync.RWMutex
	// children maps parentSessionID → []forkEntry
	children map[string][]forkEntry
	// byChild maps childSessionID → forkEntry (for ancestor walk)
	byChild map[string]forkEntry
}

// NewForkRegistry returns an empty ForkRegistry.
func NewForkRegistry() *ForkRegistry {
	return &ForkRegistry{
		children: make(map[string][]forkEntry),
		byChild:  make(map[string]forkEntry),
	}
}

// ReplayForkRegistry reads bus_sessions events through the given manager and
// reconstructs the in-memory fork lineage (parent→children map, byChild
// ancestor index). session.fork events live on BusSessions alongside
// session.register/heartbeat/end — the fork body's lineage fields
// (ParentSessionID, ChildSessionID, ForkPoint, PinnedUntil) are the only
// durable record of a fork relationship; before this function existed,
// ForkRegistry was reinitialized empty at every startup (serve.go), so all
// lineage was lost across a restart even though the bus event itself
// persisted.
//
// Mirrors ReplaySessionRegistry: sorted ascending by Seq for deterministic
// replay order, and safe to call with a nil manager or registry (no-op).
// Uses the event's own timestamp as forkTime so PinnedUntil / default
// 7-day-from-fork-point expiry match what was computed at fork time,
// not wall-clock-at-restart.
func ReplayForkRegistry(mgr *BusSessionManager, fr *ForkRegistry) error {
	if mgr == nil || fr == nil {
		return nil
	}
	events, err := mgr.ReadEvents(BusSessions)
	if err != nil {
		slog.Warn("session.fork: replay read failed", "bus", BusSessions, "err", err)
		return err
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
	replayed := 0
	for _, evt := range events {
		if evt.Type != string(KindSessionFork) {
			continue
		}
		body := parseSessionForkPayload(evt.Payload)
		if body == nil {
			continue
		}
		forkTime, err := parseBusTS(evt.Ts)
		if err != nil {
			forkTime = time.Now().UTC()
		}
		fr.Record(*body, forkTime)
		replayed++
	}
	slog.Info("session.fork: replay complete", "forks", replayed, "events", len(events))
	return nil
}

// Record adds a fork relationship to the registry.
// If pinnedUntil is non-nil, the entry persists until that time regardless of
// DefaultForkRetention. If nil, expiry = forkTime + DefaultForkRetention.
func (r *ForkRegistry) Record(body SessionForkBody, forkTime time.Time) {
	entry := forkEntry{body: body}
	if body.PinnedUntil != nil && !body.PinnedUntil.IsZero() {
		entry.expiresAt = *body.PinnedUntil
	} else {
		entry.expiresAt = forkTime.Add(DefaultForkRetention)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.children[body.ParentSessionID] = append(r.children[body.ParentSessionID], entry)
	r.byChild[body.ChildSessionID] = entry
}

// ForkChildren returns the child session IDs forked from parentSessionID that
// are not yet expired (GC-eligible) at the given reference time.
// The returned slice may be empty; nil parentSessionID returns empty.
//
// This is the internal projection function that future cog_fork_children MCP
// tool wrapping targets (RFC-0005 §Future scope).
func (r *ForkRegistry) ForkChildren(parentSessionID string, now time.Time) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := r.children[parentSessionID]
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if now.Before(e.expiresAt) {
			out = append(out, e.body.ChildSessionID)
		}
	}
	return out
}

// ForkAncestors returns the lineage chain from childSessionID back toward the
// root session (the session with no registered parent fork). Each element is a
// (sessionID, forkPoint, forkBlockHash) tuple.
//
// The walk terminates when it reaches a session that has no registered parent
// in the registry, or when it detects a cycle (max depth 100).
//
// This is the internal projection function that future cog_fork_ancestors MCP
// tool wrapping targets (RFC-0005 §Future scope).
func (r *ForkRegistry) ForkAncestors(childSessionID string) []ForkAncestor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	const maxDepth = 100
	var ancestors []ForkAncestor
	cur := childSessionID
	visited := make(map[string]bool)

	for depth := 0; depth < maxDepth; depth++ {
		entry, ok := r.byChild[cur]
		if !ok {
			break
		}
		if visited[cur] {
			slog.Warn("session.fork: cycle detected in ancestor walk",
				"operation", "fork_ancestor_walk",
				"child_session_id", childSessionID,
				"cycle_at", cur,
			)
			break
		}
		visited[cur] = true
		ancestors = append(ancestors, ForkAncestor{
			SessionID:     entry.body.ParentSessionID,
			ForkPoint:     entry.body.ForkPoint,
			ForkBlockHash: entry.body.ParentSessionHash,
		})
		cur = entry.body.ParentSessionID
	}
	return ancestors
}

// GCRootExpiry returns the latest expiry time across all live children of
// parentSessionID, plus a small retention margin. The parent's ledger state
// should not be GC'd before this time (RFC-0005 §Parent-reference integrity).
// Returns the zero time if there are no live children.
func (r *ForkRegistry) GCRootExpiry(parentSessionID string, now time.Time) time.Time {
	const margin = 24 * time.Hour // extra buffer beyond last child expiry

	r.mu.RLock()
	defer r.mu.RUnlock()

	var latest time.Time
	for _, e := range r.children[parentSessionID] {
		if now.Before(e.expiresAt) && e.expiresAt.After(latest) {
			latest = e.expiresAt
		}
	}
	if latest.IsZero() {
		return time.Time{}
	}
	return latest.Add(margin)
}

// PruneExpired removes entries whose expiresAt is before `now`. Safe to call
// periodically from a GC goroutine. Returns the number of entries pruned.
func (r *ForkRegistry) PruneExpired(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	pruned := 0
	for parentID, entries := range r.children {
		live := entries[:0]
		for _, e := range entries {
			if now.Before(e.expiresAt) {
				live = append(live, e)
			} else {
				delete(r.byChild, e.body.ChildSessionID)
				pruned++
			}
		}
		if len(live) == 0 {
			delete(r.children, parentID)
		} else {
			r.children[parentID] = live
		}
	}
	return pruned
}

// Len returns the number of fork entries in the registry.
func (r *ForkRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byChild)
}
