// index.go — in-memory full-text index for conversation turns.
//
// The index is a flat in-memory structure backed by a projection directory:
//
//	.cog/state/conversations/
//	  <session_id>.json   — JSON array of Turn (one file per session)
//	  _meta.json          — JSON object: session_id → SessionMeta
//
// This is intentionally not SQLite FTS5 — the observatory aims for zero
// additional runtime dependencies. Text search uses strings.Contains on
// pre-lowercased text, which is fast enough for the expected corpus size
// (~hundreds of sessions, ~tens of thousands of turns total).
//
// Embedding-based semantic search is deferred to a follow-on PR.
package conversations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/filelock"
	"github.com/myrgic/cogos/pkg/pathsafe"
)

// metaLockTimeout bounds how long UpsertSession/DeleteSession wait to acquire
// the cross-process lock on _meta.json before giving up. Generous relative to
// the write itself (a few JSON marshals + one rename) but short enough that a
// wedged peer doesn't hang the caller indefinitely.
//
// A var rather than a const solely so tests can shrink it (see
// index_batch_atomicity_test.go) to keep a deliberate-lock-timeout
// regression test fast; production code never assigns to it.
var metaLockTimeout = 5 * time.Second

// Index holds all indexed turns in memory plus a reference to the projection
// directory for persistence.
//
// All access to the sessions/turns maps is guarded by mu. The reconcile daemon
// and the autonomic ticker both drive FetchLive concurrently (which calls
// Load), and MCP tool handlers read the index concurrently with those; without
// this lock those paths raced on the maps and crashed the kernel with
// "fatal error: concurrent map writes".
type Index struct {
	// mu guards sessions and turns. Use RLock for read-only methods and Lock
	// for methods that mutate the maps. Disk I/O is performed outside the lock
	// where possible (see Load).
	mu sync.RWMutex

	projDir string

	// sessions maps session_id → SessionMeta.
	sessions map[string]SessionMeta

	// turns maps session_id → []Turn (in turn-index order).
	turns map[string][]Turn

	// lastMeta{Mtime,Size,Hash} record the (mtime, size, content hash) of
	// _meta.json as of the last Load or local write. LoadIfChanged uses them to
	// skip the full reload when the on-disk index is unchanged since this process
	// last touched it. (mtime, size) is a cheap pre-filter; the content hash is
	// the authoritative check, catching a same-size in-place rewrite by an
	// external writer that (mtime, size) alone would miss. Guarded by mu.
	lastMetaMtime time.Time
	lastMetaSize  int64
	lastMetaHash  string

	// loaded is set once Load has completed at least once. Before that the
	// in-memory maps are empty regardless of what is on disk, so readers that
	// must distinguish "no matches" from "not ready" (the fused memory search,
	// myrgic/cogos#650) check Loaded. Guarded by mu.
	loaded bool
}

// Loaded reports whether Load has completed at least once on this index.
func (idx *Index) Loaded() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.loaded
}

// metaDelta describes the change a single writeMetaFileLocked call must apply
// to _meta.json, as opposed to a full re-assertion of this process's in-memory
// idx.sessions snapshot (which could be stale relative to a peer process's
// more recent write — see writeMetaFileLocked).
type metaDelta struct {
	upserts map[string]SessionMeta
	deletes []string
}

// SessionAndTurns pairs a session's meta with its full turns list — the unit
// UpsertSessions batches. See UpsertSessions for why batching exists (issue
// #494: one writeMetaFileLocked call per session made a source rebuild of N
// sessions cost O(N) full-_meta.json rewrites, i.e. O(N²) bytes written).
type SessionAndTurns struct {
	Meta  SessionMeta
	Turns []Turn
}

// NewIndex creates an Index backed by projDir. projDir is created if absent.
func NewIndex(projDir string) (*Index, error) {
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		return nil, fmt.Errorf("conversations/index: mkdir %s: %w", projDir, err)
	}
	idx := &Index{
		projDir:  projDir,
		sessions: make(map[string]SessionMeta),
		turns:    make(map[string][]Turn),
	}
	return idx, nil
}

// Load reads all projection files from projDir into memory.
// Call once at startup (FetchLive). Subsequent operations keep state in sync.
//
// Disk reads are performed into freshly-allocated local maps with no lock held,
// then swapped into the index under a single write lock. This keeps the
// (potentially slow) file I/O off the lock while still making the visible map
// state mutation atomic with respect to concurrent readers and other Loads.
func (idx *Index) Load() error {
	// Stat before reading so a write racing this load at worst causes a
	// redundant reload next cycle (recorded mtime older than content), never a
	// stale read (recorded mtime never newer than content).
	var metaMtime time.Time
	var metaSize int64
	if fi, statErr := os.Stat(idx.metaPath()); statErr == nil {
		metaMtime = fi.ModTime()
		metaSize = fi.Size()
	}

	sessions := make(map[string]SessionMeta)
	var metaHash string
	if data, err := os.ReadFile(idx.metaPath()); err == nil {
		metaHash = sha256Hex(data)
		var m map[string]SessionMeta
		if jsonErr := json.Unmarshal(data, &m); jsonErr == nil {
			sessions = m
		}
	}

	// Load turns for each known session into a local map.
	turns := make(map[string][]Turn, len(sessions))
	for sid := range sessions {
		t, err := idx.loadTurnsFile(sid)
		if err != nil {
			// Corrupt turn file — remove from index; will be re-projected.
			delete(sessions, sid)
			continue
		}
		turns[sid] = t
	}

	// Atomically swap in the freshly loaded state.
	idx.mu.Lock()
	idx.sessions = sessions
	idx.turns = turns
	idx.lastMetaMtime = metaMtime
	idx.lastMetaSize = metaSize
	idx.lastMetaHash = metaHash
	idx.loaded = true
	idx.mu.Unlock()
	return nil
}

// LoadIfChanged reloads the index from disk only when _meta.json has changed
// since the last Load or local write, detected by (mtime, size). When this
// process is the sole writer — the common case for the daemon — every change is
// already reflected in memory by UpsertSession/DeleteSession, so the expensive
// full reload is skipped. An external writer (e.g. the cog CLI) changes the
// file's mtime/size and triggers a real reload. Returns whether a reload ran.
func (idx *Index) LoadIfChanged() (bool, error) {
	fi, err := os.Stat(idx.metaPath())
	if err != nil {
		// Missing/unstattable _meta.json (fresh index, or removed out from under
		// us): fall back to a full Load, which treats a missing file as empty.
		return true, idx.Load()
	}
	idx.mu.RLock()
	mtime, size, hash := idx.lastMetaMtime, idx.lastMetaSize, idx.lastMetaHash
	idx.mu.RUnlock()

	// Cheap pre-filter: any size or mtime change is unambiguously a change.
	if fi.Size() != size || !fi.ModTime().Equal(mtime) {
		return true, idx.Load()
	}
	// Same (mtime, size) can still be an in-place same-size rewrite by an
	// external process (the cog CLI editing fixed-width fields), which
	// (mtime, size) alone cannot detect. Confirm by content hash before trusting
	// the in-memory copy. Reading+hashing the single _meta.json file is far
	// cheaper than the full reload it guards (which re-reads every per-session
	// turns file). A torn/partial read hashes differently and falls through to a
	// reload, so it self-heals rather than sticking.
	data, readErr := os.ReadFile(idx.metaPath())
	if readErr != nil {
		return true, idx.Load()
	}
	if sha256Hex(data) == hash {
		return false, nil
	}
	return true, idx.Load()
}

// SessionOpOutcome reports one session's individual outcome within a
// UpsertSessions/DeleteSessions batch call. Err is nil on success.
//
// This exists so one session's per-sessionID lock-contention failure does
// not fail every OTHER, unrelated session in the same batch — see
// UpsertSessions' doc comment, "Per-session fault isolation" (cog-review,
// PR #495 third pass).
type SessionOpOutcome struct {
	SessionID string
	Err       error
}

// UpsertSession is a thin wrapper over UpsertSessions for single-session
// callers. See UpsertSessions for the batching rationale (issue #494) and
// full locking/merge discipline — including the #449 cross-process
// read-merge-write coordination, the #458 per-sessionID lock spanning both
// the turns write and the meta commit, and the #458 fifth-pass idx.mu
// discipline — all of which this call inherits unchanged for the N=1 case.
func (idx *Index) UpsertSession(meta SessionMeta, turns []Turn) error {
	_, err := idx.UpsertSessions([]SessionAndTurns{{Meta: meta, Turns: turns}})
	return err
}

// heldSessionLock pairs an acquired per-sessionID turnsLockPath lock with
// the sessionID it guards. Used by UpsertSessions'/DeleteSessions' phase 1
// via acquireSessionLocks.
type heldSessionLock struct {
	sid  string
	lock *filelock.FileLock
}

// acquireSessionLocks attempts to acquire every id's turnsLockPath lock
// under ONE shared deadline for the whole call, rather than giving each
// sessionID its own full metaLockTimeout budget independently.
//
// This is remedy 1's fourth review-round fix (cog-review, PR #495 fourth
// pass): the per-session-independent design (phase 1 acquiring each lock
// on its own, continuing past any single failure — see UpsertSessions'
// "Per-session fault isolation") removed the early-exit an all-or-nothing
// loop would have had, so a batch with K simultaneously-contended
// sessionIDs (all peer-held by the SAME wedged process, or several
// unrelated wedged peers — both plausible under the #449 cross-process
// scenario this whole PR is about) could each independently poll out a
// full metaLockTimeout, compounding to K×metaLockTimeout for one batch
// call. Because remedy 4's applyMu is held for ApplyPlan's entire
// duration, that also stalls every other reconcile activity for the
// provider for the same window — undermining metaLockTimeout's documented
// "a wedged peer doesn't hang the caller indefinitely" bound and remedy
// 4's own goal, through a different path.
//
// A single deadline, computed once and shared across every id's
// filelock.Acquire call, restores the ONE-metaLockTimeout bound for the
// batch as a whole: filelock.Acquire always attempts one immediate,
// non-blocking tryLock before ever consulting its timeout (see
// pkg/filelock's Acquire), so passing the (possibly already-negative)
// remaining budget to every call — rather than skipping later ids outright
// once the budget nominally runs out — costs nothing extra for an
// uncontended id (it still succeeds on that first free attempt) while
// still bounding any id that DOES need to wait to whatever's left of the
// shared window. The trade-off this introduces (an id late in sorted order
// can get less of the shared budget than one early in it, if earlier ids
// were genuinely contended) is the same shape as any shared-deadline
// design (e.g. a context.Context deadline shared across a fan-out); it
// replaces "every session gets its own full timeout" (unbounded batch
// total) with "the batch as a whole gets one timeout" (matching the
// pre-batching per-call bound), which is the trade cog-review's finding
// asked for.
//
// Returns every lock actually acquired (still held — the caller releases
// them) and a map from sessionID to the reason it did NOT get a lock, for
// every id that failed. Every id in ids ends up in exactly one of the two.
func (idx *Index) acquireSessionLocks(ids []string) ([]heldSessionLock, map[string]error) {
	held := make([]heldSessionLock, 0, len(ids))
	failed := make(map[string]error, len(ids))
	deadline := time.Now().Add(metaLockTimeout)
	for _, sid := range ids {
		lock, err := filelock.Acquire(idx.turnsLockPath(sid), time.Until(deadline))
		if err != nil {
			failed[sid] = fmt.Errorf("conversations/index: acquire session lock for %s: %w", sid, err)
			continue
		}
		held = append(held, heldSessionLock{sid: sid, lock: lock})
	}
	return held, failed
}

// UpsertSessions writes turns + meta for a whole batch of sessions,
// performing exactly ONE writeMetaFileLocked call regardless of batch size.
//
// This is remedy 1 for issue #494: UpsertSession's per-call writeMetaFileLocked
// does a full read+json.Unmarshal+merge+json.MarshalIndent+write of the
// entire _meta.json map (see that method's doc comment for why the
// read-merge-write is necessary — a peer process's concurrent write must
// never be clobbered). Calling it once per session turns an N-session
// rebuild (e.g. applyIngestSource re-parsing one ingest source) into N full
// rewrites of the file, i.e. O(N²) bytes written for a source of N sessions
// — measured on the live node as ~10.5 GB to rewrite a 4 MB file across
// 2,649 sessions. Collapsing every session's meta into ONE delta and calling
// writeMetaFileLocked once turns that into a single ~4 MB rewrite.
//
// Locking, in order:
//
//  1. Every session's turns file is written under its own per-sessionID
//     turnsLockPath lock — the same lock, and same reason, UpsertSession has
//     always taken (see its historical doc comment, preserved below):
//     serializing "turns + meta for sessionID X" as one unit so a concurrent
//     DeleteSession(X)/UpsertSession(X) on a peer Index instance can never
//     interleave between this call's turns write and its share of the meta
//     commit (cog-review, PR #458, third pass).
//  2. Because ALL of the batch's per-sessionID locks are still held when the
//     single writeMetaFileLocked call runs (released only via the deferred
//     loop after this method returns), that same interleaving guarantee
//     holds for the whole batch, not just one session — the meta commit for
//     session X cannot race a peer's turns-file operation for session X.
//  3. Per-sessionID locks are acquired in sorted-by-sessionID order (after
//     de-duplicating the batch by SessionID, last-write-wins) rather than
//     caller-supplied order. Two concurrent UpsertSessions/DeleteSessions
//     batches that share a sessionID but list it in different relative
//     positions could otherwise deadlock (batch A holds lock(X), waiting on
//     lock(Y); batch B holds lock(Y), waiting on lock(X)) — a classic
//     lock-ordering cycle. A single global order across every batch caller
//     removes the cycle. De-duplicating first also matters on its own: two
//     entries for the same sessionID in one batch would otherwise attempt to
//     Acquire that sessionID's lock file twice without an intervening
//     Release, which — like the flock self-collision issue #494 documents
//     for metaLockPath — self-blocks, since flock(2) is not re-entrant
//     within a process.
//
// Trade-off: holding one open, locked file descriptor per session in the
// batch for the duration of the call bounds this to the batch size (the
// largest observed source is ~2,650 sessions) rather than one at a time.
// That is a deliberate choice to preserve the exact same peer-safety
// guarantee UpsertSession has always provided, for every session in the
// batch, rather than only for the first/last. If a source's session count
// grows large enough for this to threaten a process's open-file limit,
// chunking the batch (trading one writeMetaFileLocked call for a few) is the
// next lever — not implemented here since no observed source is remotely
// close to that scale.
//
// Per-session fault isolation (cog-review, PR #495 third pass): an earlier
// version of this method acquired every lock in a first pass and aborted
// the WHOLE batch — including sessions whose locks it already held — the
// instant any single session's lock acquisition failed. That closed the
// atomicity gap the second review pass found (see below), but introduced a
// real regression for the CC (source_dirs) path specifically: before this
// PR, ApplyPlan called idx.UpsertSession once per action independently, so
// one contended session only failed that session's Result; the
// all-or-nothing batch coupled every other, uncontended session in the same
// cycle to that one session's bad luck. UpsertSessions now attempts every
// session's lock acquisition and turns-file write independently — a
// failure for session X only excludes X from the batch, recorded in X's
// SessionOpOutcome — and the single shared writeMetaFileLocked call at the
// end commits only the subset that succeeded through both steps. A session
// is never included in that shared commit without its own lock having been
// held across its own turns write, so the atomicity guarantee the second
// review pass required is preserved for every session that DOES make it
// into the commit; only sessions that independently failed are excluded,
// exactly mirroring the pre-batching per-action fault isolation.
//
// The one remaining shared-fate step is the meta commit itself: if
// writeMetaFileLocked fails (a metaLockPath contention or write failure,
// a different and much rarer failure mode than a single sessionID's
// turnsLockPath being contended), every session that succeeded through its
// own lock+write is affected together. This is not a new exposure —
// it is the exact same risk a single-session UpsertSession call has always
// carried for itself (turns already written, its own meta commit still
// pending) — remedy 1's whole premise is committing many sessions' meta via
// one shared write, so sessions that reach that step necessarily share its
// fate. The finer-grained failure mode cog-review's second pass actually
// found (abandoning already-completed work belonging to sessions that were
// never going to be part of the failing session's own commit) is what
// per-session independence above eliminates.
//
// The turns write path, per-sessionID locking, and idx.mu discipline below
// are otherwise identical to the original single-session UpsertSession:
//
//	The on-disk _meta.json is a shared file: the daemon's own reconcile/ticker
//	cycle and a separately-invoked "cog reconcile conversations" CLI process
//	can both call UpsertSession/DeleteSession against the same projDir
//	(issue #449). Without coordination the two read-modify-write cycles
//	interleave and the second writer's plain marshal-of-idx.sessions silently
//	drops whatever the first writer added, because each process's in-memory
//	idx.sessions only reflects its own view as of its last Load.
//
//	To close that window, the on-disk write is guarded by a cross-process
//	filelock (metaLockPath) and, while holding it, writeMetaFileLocked re-reads
//	the current _meta.json from disk and applies only *this call's* delta
//	(metaDelta) on top of it — never a full re-assertion of this process's
//	entire in-memory idx.sessions snapshot, which could itself be stale
//	relative to a peer's more recent write and would silently resurrect
//	whatever the peer already removed. See writeMetaFileLocked for why a
//	snapshot-merge (rather than a delta-merge) is unsound.
//
//	idx.mu is deliberately NOT held across the cross-process locking/disk I/O
//	above — only around the brief in-memory map mutation at the very end.
//	cog-review (PR #458, fifth pass) found that an earlier version held
//	idx.mu.Lock() for the whole call, meaning the up-to-2×metaLockTimeout
//	(≈10s) a writer could block waiting on a peer process's cross-process lock
//	also blocked every concurrent GetMeta/ListSessions/GetTurn/Search call
//	(which take idx.mu.RLock()) on the SAME in-process Index. Disk I/O and
//	cross-process coordination now happen with idx.mu unheld; only the final
//	idx.sessions/idx.turns mutation takes the lock, and only briefly.
func (idx *Index) UpsertSessions(batch []SessionAndTurns) ([]SessionOpOutcome, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// De-duplicate by SessionID (last entry for a given ID wins, matching
	// ordinary map-assignment semantics) before computing the lock
	// acquisition order — see the doc comment above for why both the
	// de-duplication and the sort are required for correctness, not just
	// determinism.
	dedup := make(map[string]SessionAndTurns, len(batch))
	for _, item := range batch {
		dedup[item.Meta.SessionID] = item
	}
	ids := make([]string, 0, len(dedup))
	for sid := range dedup {
		ids = append(ids, sid)
	}
	sort.Strings(ids)

	// Phase 1: attempt to acquire every per-sessionID lock, independently,
	// under one shared deadline for the whole batch (see
	// acquireSessionLocks). A failure for one sessionID (most plausibly a
	// peer process holding its turnsLockPath — exactly the cross-process
	// contention this whole PR is about) excludes only that sessionID; it
	// does not stop the loop or prevent other, uncontended sessions from
	// being attempted. See "Per-session fault isolation" above for why this
	// must be independent rather than all-or-nothing.
	held, failed := idx.acquireSessionLocks(ids)
	defer func() {
		for _, h := range held {
			h.lock.Release()
		}
	}()

	// Phase 2: for every successfully-locked session, write its turns file
	// independently. A write failure here (a genuine disk I/O error, not
	// lock contention) also excludes only that session — the same
	// independence as phase 1, for the same reason.
	upserts := make(map[string]SessionMeta, len(held))
	committed := make([]string, 0, len(held))
	for _, h := range held {
		item := dedup[h.sid]
		if err := idx.writeTurnsFileLocked(h.sid, item.Turns); err != nil {
			failed[h.sid] = fmt.Errorf("conversations/index: write turns %s: %w", h.sid, err)
			continue
		}
		upserts[h.sid] = item.Meta
		committed = append(committed, h.sid)
	}

	// Phase 3: exactly ONE writeMetaFileLocked call, covering only the
	// sessions that succeeded through phases 1 and 2 — this is remedy 1
	// itself. Their locks are still held at this point. A failure here is
	// shared by every session in this subset (see the doc comment above for
	// why that is an accepted, pre-existing risk shape, not a new one).
	if len(upserts) > 0 {
		if err := idx.writeMetaFileLocked(metaDelta{upserts: upserts}); err != nil {
			for _, sid := range committed {
				failed[sid] = fmt.Errorf("conversations/index: commit meta for %s: %w", sid, err)
			}
			committed = nil
		}
	}

	// Commit the in-memory view last, under a short-lived write lock — no
	// disk I/O or cross-process locking happens while idx.mu is held. Only
	// the sessions that made it all the way through phase 3.
	if len(committed) > 0 {
		idx.mu.Lock()
		for _, sid := range committed {
			item := dedup[sid]
			idx.sessions[sid] = item.Meta
			idx.turns[sid] = item.Turns
		}
		idx.mu.Unlock()
	}

	outcomes := make([]SessionOpOutcome, 0, len(ids))
	var errs []error
	for _, sid := range ids {
		outcomes = append(outcomes, SessionOpOutcome{SessionID: sid, Err: failed[sid]})
		if err := failed[sid]; err != nil {
			errs = append(errs, err)
		}
	}
	return outcomes, errors.Join(errs...)
}

// DeleteSession is a thin wrapper over DeleteSessions for single-session
// callers. See DeleteSessions for the batching rationale.
func (idx *Index) DeleteSession(sessionID string) error {
	_, err := idx.DeleteSessions([]string{sessionID})
	return err
}

// DeleteSessions removes a batch of sessions from memory and disk, performing
// exactly ONE writeMetaFileLocked call regardless of batch size — the same
// treatment UpsertSessions gives the upsert path (issue #494 remedy 1,
// applied to the prune path applyIngestSource runs after every ingest-source
// re-parse).
//
// Locking and per-session fault isolation follow UpsertSessions exactly:
// sessionIDs are de-duplicated and sorted (avoiding both a self-collision on
// a duplicate ID and a lock-ordering deadlock against a concurrent batch
// call that shares an ID); each sessionID's lock acquisition and turns-file
// removal are attempted independently, so one contended sessionID excludes
// only itself rather than failing every other session in the batch
// (cog-review, PR #495 third pass); a session is only removed from
// _meta.json in the one shared writeMetaFileLocked call if its own turns
// file was already successfully removed under its own lock — never before.
// See UpsertSessions' doc comment for the full rationale, which applies
// unchanged here (with removeTurnsFileLocked in place of
// writeTurnsFileLocked, and metaDelta.deletes in place of .upserts).
func (idx *Index) DeleteSessions(sessionIDs []string) ([]SessionOpOutcome, error) {
	if len(sessionIDs) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(sessionIDs))
	ids := make([]string, 0, len(sessionIDs))
	for _, sid := range sessionIDs {
		if _, dup := seen[sid]; dup {
			continue
		}
		seen[sid] = struct{}{}
		ids = append(ids, sid)
	}
	sort.Strings(ids)

	// Phase 1: attempt to acquire every per-sessionID lock independently,
	// under one shared deadline for the whole batch (see
	// acquireSessionLocks) — see UpsertSessions' phase-1 comment for the
	// full rationale. A failure for one sessionID excludes only that
	// sessionID from phase 2 below.
	held, failed := idx.acquireSessionLocks(ids)
	defer func() {
		for _, h := range held {
			h.lock.Release()
		}
	}()

	// Phase 2: for every successfully-locked session, remove its turns file
	// independently. A removal failure here also excludes only that
	// session.
	committed := make([]string, 0, len(held))
	for _, h := range held {
		if err := idx.removeTurnsFileLocked(h.sid); err != nil {
			failed[h.sid] = fmt.Errorf("conversations/index: remove turns file for %s: %w", h.sid, err)
			continue
		}
		committed = append(committed, h.sid)
	}

	// Phase 3: exactly ONE writeMetaFileLocked call, covering only the
	// sessions that succeeded through phases 1 and 2 — this is remedy 1
	// itself. A failure here is shared by every session in this subset (see
	// UpsertSessions' doc comment for why that is an accepted, pre-existing
	// risk shape, not a new one).
	if len(committed) > 0 {
		if err := idx.writeMetaFileLocked(metaDelta{deletes: committed}); err != nil {
			for _, sid := range committed {
				failed[sid] = fmt.Errorf("conversations/index: commit meta delete for %s: %w", sid, err)
			}
			committed = nil
		}
	}

	if len(committed) > 0 {
		idx.mu.Lock()
		for _, sid := range committed {
			delete(idx.sessions, sid)
			delete(idx.turns, sid)
		}
		idx.mu.Unlock()
	}

	outcomes := make([]SessionOpOutcome, 0, len(ids))
	var errs []error
	for _, sid := range ids {
		outcomes = append(outcomes, SessionOpOutcome{SessionID: sid, Err: failed[sid]})
		if err := failed[sid]; err != nil {
			errs = append(errs, err)
		}
	}
	return outcomes, errors.Join(errs...)
}

// removeTurnsFileLocked deletes sessionID's turns file. Callers must already
// hold the turnsLockPath(sessionID) lock (see UpsertSession/DeleteSession).
func (idx *Index) removeTurnsFileLocked(sessionID string) error {
	if err := os.Remove(idx.turnsPath(sessionID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GetMeta returns the SessionMeta for a session, or false if not indexed.
func (idx *Index) GetMeta(sessionID string) (SessionMeta, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	m, ok := idx.sessions[sessionID]
	return m, ok
}

// ListSessions returns all indexed SessionMetas sorted by LastTurnAt descending.
func (idx *Index) ListSessions(since, until time.Time, identity string) []SessionMeta {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var out []SessionMeta
	for _, m := range idx.sessions {
		if !since.IsZero() && m.LastTurnAt.Before(since) {
			continue
		}
		if !until.IsZero() && m.FirstTurnAt.After(until) {
			continue
		}
		if identity != "" && !strings.EqualFold(m.Identity, identity) {
			continue
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastTurnAt.After(out[j].LastTurnAt)
	})
	return out
}

// SessionIDsBySource groups every indexed session ID by its Source field in
// a single unsorted O(n) pass over idx.sessions.
//
// This is remedy 2 for issue #494: applyIngestSource's prune pass used to
// call idx.ListSessions(time.Time{}, time.Time{}, "") — which allocates a
// full copy of every indexed SessionMeta and then sort.Slice's it by
// LastTurnAt, an ordering the prune pass never uses — once per ingest source
// with a create/update action in the plan, every reconcile cycle. With
// several ingest sources typically drifting in the same cycle, that meant
// several redundant full-index sorts of the same ~7,500 sessions just to
// answer "which session IDs belong to source X" for each X in turn. Calling
// this once per ApplyPlan cycle (hoisted out of the per-source
// applyIngestSource call into the action loop in ApplyPlan) turns that into
// one full-index walk — with no sort — shared by every source that needs it,
// and returns only session IDs, which is all the prune pass compares against
// its parsed set.
func (idx *Index) SessionIDsBySource() map[string][]string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	out := make(map[string][]string)
	for sid, m := range idx.sessions {
		out[m.Source] = append(out[m.Source], sid)
	}
	return out
}

// GetTurn returns the Turn at turnIndex within session, or false.
func (idx *Index) GetTurn(sessionID string, turnIndex int) (Turn, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	turns, ok := idx.turns[sessionID]
	if !ok || turnIndex < 0 || turnIndex >= len(turns) {
		return Turn{}, false
	}
	return turns[turnIndex], true
}

// SessionTurns returns a copy of the full, in-order turns slice currently
// indexed for sessionID, or nil if the session is not indexed. Used by
// ApplyPlan's incremental-parse path (issue #558) to seed the
// already-indexed prefix of a session before appending newly parsed tail
// turns, without re-reading the turns projection file from disk (idx.turns
// is this process's live in-memory copy, kept in sync with disk by
// Load/UpsertSession(s)).
//
// Returns a copy so callers can freely append to it without racing a
// concurrent Load/UpsertSession mutation of the index's own slice.
func (idx *Index) SessionTurns(sessionID string) []Turn {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	turns, ok := idx.turns[sessionID]
	if !ok {
		return nil
	}
	out := make([]Turn, len(turns))
	copy(out, turns)
	return out
}

// Search performs a case-insensitive multi-term search over all indexed turns.
//
// Query parsing rules:
//   - Double-quoted substrings are matched as exact (case-insensitive) phrases.
//   - Unquoted tokens separated by whitespace form an AND conjunction: a turn
//     matches only when ALL tokens are present as substrings of its text.
//   - A single unquoted token behaves identically to the original single-term
//     substring match (backward-compatible).
//
// Filters: since/until bound timestamps; sessionID restricts to one session;
// identity filters by session identity; limit caps results (0 = no limit).
func (idx *Index) Search(query string, since, until time.Time, sessionID, identity string, limit int) []SearchHit {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	terms := parseSearchQuery(query)
	var hits []SearchHit

	// Collect sessions to search.
	var sids []string
	if sessionID != "" {
		sids = []string{sessionID}
	} else {
		for sid := range idx.turns {
			sids = append(sids, sid)
		}
		sort.Strings(sids)
	}

	for _, sid := range sids {
		meta, hasMeta := idx.sessions[sid]
		if identity != "" && hasMeta && !strings.EqualFold(meta.Identity, identity) {
			continue
		}

		turns := idx.turns[sid]
		for _, t := range turns {
			if !since.IsZero() && t.Timestamp.Before(since) {
				continue
			}
			if !until.IsZero() && t.Timestamp.After(until) {
				continue
			}
			if !matchesAllTerms(t.Text, terms) {
				continue
			}
			// Use the first term as the excerpt anchor for multi-term queries.
			anchor := query
			if len(terms) > 0 {
				anchor = terms[0]
			}
			excerpt := makeExcerpt(t.Text, anchor, 300)
			hit := SearchHit{
				SessionID: sid,
				TurnIndex: t.TurnIndex,
				UUID:      t.UUID,
				Timestamp: t.Timestamp,
				Role:      t.Role,
				Excerpt:   excerpt,
				Identity:  meta.Identity,
			}
			if hasMeta {
				hit.SessionTitle = meta.Title
				hit.Source = meta.Source
			}
			hits = append(hits, hit)
			if limit > 0 && len(hits) >= limit {
				return hits
			}
		}
	}
	return hits
}

// parseSearchQuery splits a query string into individual match terms.
// Double-quoted substrings are extracted as single exact-match terms.
// Remaining text is split on whitespace.
//
// Examples:
//
//	`foo bar`            → ["foo", "bar"]
//	`"foo bar" baz`      → ["foo bar", "baz"]
//	`"exact phrase"`     → ["exact phrase"]
//	`hello`              → ["hello"]
func parseSearchQuery(query string) []string {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	var terms []string
	rest := query
	for {
		rest = strings.TrimSpace(rest)
		if rest == "" {
			break
		}
		if rest[0] == '"' {
			// Find closing quote.
			end := strings.Index(rest[1:], "\"")
			if end < 0 {
				// Unclosed quote: treat remainder as literal term.
				terms = append(terms, rest[1:])
				break
			}
			phrase := rest[1 : end+1]
			if phrase != "" {
				terms = append(terms, phrase)
			}
			rest = rest[end+2:]
		} else {
			// Take until next whitespace or quote.
			i := strings.IndexAny(rest, " \t\r\n\"")
			if i < 0 {
				terms = append(terms, rest)
				break
			}
			terms = append(terms, rest[:i])
			rest = rest[i:]
		}
	}
	return terms
}

// matchesAllTerms returns true when text contains ALL terms as
// case-insensitive substrings.
func matchesAllTerms(text string, terms []string) bool {
	if len(terms) == 0 {
		return true
	}
	ltext := strings.ToLower(text)
	for _, term := range terms {
		if !strings.Contains(ltext, strings.ToLower(term)) {
			return false
		}
	}
	return true
}

// ─── persistence helpers ─────────────────────────────────────────────────────

func (idx *Index) metaPath() string {
	return filepath.Join(idx.projDir, "_meta.json")
}

// metaLockPath is the advisory cross-process lock file guarding the
// read-modify-write cycle on _meta.json. A sibling file, not metaPath itself,
// so the lock's own lifecycle (create/lock/unlock) never touches the JSON
// content file that Load/LoadIfChanged read.
func (idx *Index) metaLockPath() string {
	return filepath.Join(idx.projDir, "_meta.json.lock")
}

// sha256Hex returns the hex-encoded SHA-256 of b.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// turnsFilename derives a safe flat filename from sessionID. Composite keys
// for normalized ingest sessions take the form "<source>/<session_id>"; the
// "/" is replaced with "__" first (preserving the existing on-disk naming
// convention for that common case) so the file stays in projDir without
// creating subdirectories. The result then goes through
// pathsafe.SanitizeComponent to escape anything else NTFS forbids (a colon
// in a source or session id, control characters, ...) — see myrgic/cogos#489.
// Plain slugs/UUIDs (the overwhelming majority of session IDs) round-trip
// unchanged through both steps.
func turnsFilename(sessionID string) string {
	flat := strings.ReplaceAll(sessionID, "/", "__")
	return pathsafe.SanitizeComponent(flat) + ".json"
}

func (idx *Index) turnsPath(sessionID string) string {
	return filepath.Join(idx.projDir, turnsFilename(sessionID))
}

// writeMetaFileLocked persists delta (this call's own upserts/deletes) into
// _meta.json. Callers must NOT hold idx.mu — this method takes its own
// short-lived idx.mu R/W locks internally, separately from the blocking
// cross-process metaLockPath acquisition, so that a peer-process write
// contending on metaLockPath never also blocks concurrent in-process readers
// (GetMeta/ListSessions/GetTurn/Search) that only need idx.mu.RLock(). See
// UpsertSession's doc comment for the availability regression this avoids
// (cog-review, PR #458, fifth pass).
//
// The write is atomic (tmp + rename, same pattern as
// BusSessionManager.saveRegistry in internal/engine/bus_session.go) — a plain
// truncate-before-write left _meta.json empty (and every subsequent Load
// silently falling back to an empty index) if the process was killed
// mid-write.
//
// It also takes the cross-process filelock (pkg/filelock) around the full
// read-modify-write cycle: re-read whatever is currently on disk, apply only
// delta on top of it, then atomically write the result. This is what closes
// issue #449 — a CLI reconcile and the daemon's own reconcile cycle can each
// read-modify-write _meta.json independently; taking the lock serializes the
// two cycles, and applying a per-call delta (rather than a full marshal of
// this process's in-memory idx.sessions) means a writer never re-asserts a
// stale snapshot that could silently resurrect a session a peer has since
// deleted, or drop one a peer has since added. A snapshot-merge was tried
// first and found unsound by TestCrossProcessDeleteSurvivesConcurrentUpsert:
// re-adding every key from idx.sessions on top of the freshly-read on-disk
// map reintroduces this process's own stale copy of a session a peer already
// deleted, because idx.sessions only reflects this process's last Load, not
// the peer's more recent delete.
//
// Deliberately does NOT unconditionally adopt the disk-merged map (which may
// contain a peer process's session keys) into idx.sessions, and does NOT
// unconditionally update lastMeta{Mtime,Size,Hash} to describe the merged
// file's on-disk stats. An earlier version of this fix did both
// unconditionally, and cog-review (PR #458) correctly flagged the resulting
// metadata/turns split-brain: idx.sessions would gain a peer's session key
// while idx.turns — populated only by this process's own Load/UpsertSession —
// never got that session's turns, and because lastMetaHash was set to match
// the bytes just written, the next LoadIfChanged saw "no external change" and
// skipped the full reload that would otherwise have backfilled idx.turns.
// cog_list_conversations would then list a session that
// cog_get_turn/cog_search could never actually serve.
//
// The fix: after computing merged, compare it against what idx.sessions will
// be once the caller applies its own delta (UpsertSession/DeleteSession
// apply the in-memory mutation after this call returns, not before — see
// their doc comments — so this method computes the expected post-delta view
// itself from a fresh idx.mu.RLock() snapshot plus delta, rather than reading
// idx.sessions as literally-currently-mutated). If they're equal, no peer
// wrote anything this process doesn't already fully know about (turns
// included) — the common sole-writer case — so it's safe to keep the
// LoadIfChanged fast-path bookkeeping (lastMeta{Mtime,Size,Hash}) in sync
// with what was just written, same as before this fix. If they differ, a
// peer's content is present in merged that idx.sessions doesn't have turns
// for; leave the lastMeta bookkeeping untouched by this call, so the next
// LoadIfChanged detects the file changed out from under its recorded
// baseline and performs a full reload — sessions map AND turns map together —
// keeping the two maps consistent with each other at the cost of that one
// reload.
//
// writeMetaFileLockedHook, when non-nil, is invoked once at the start of
// every writeMetaFileLocked call. Production code never sets it; it exists
// so tests can count how many times the full _meta.json read-merge-write
// round trip actually runs — the cost issue #494 is about — without
// resorting to fragile mtime/inode polling around a call that (post-fix)
// completes in well under a second. See index_batch_test.go.
var writeMetaFileLockedHook func()

func (idx *Index) writeMetaFileLocked(delta metaDelta) error {
	if writeMetaFileLockedHook != nil {
		writeMetaFileLockedHook()
	}

	lock, err := filelock.Acquire(idx.metaLockPath(), metaLockTimeout)
	if err != nil {
		return fmt.Errorf("conversations/index: acquire meta lock: %w", err)
	}
	defer lock.Release()

	merged := make(map[string]SessionMeta)
	if data, readErr := os.ReadFile(idx.metaPath()); readErr == nil {
		var onDisk map[string]SessionMeta
		if jsonErr := json.Unmarshal(data, &onDisk); jsonErr == nil {
			for k, v := range onDisk {
				merged[k] = v
			}
		}
		// A corrupt/unparseable on-disk file is treated as empty rather than
		// aborting the write — delta is still applied and this call becomes
		// the one restoring a valid file.
	} else if !os.IsNotExist(readErr) {
		return fmt.Errorf("conversations/index: read meta for merge: %w", readErr)
	}

	for k, v := range delta.upserts {
		merged[k] = v
	}
	for _, k := range delta.deletes {
		delete(merged, k)
	}

	b, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return err
	}
	tmp := idx.metaPath() + fmt.Sprintf(".tmp.%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, idx.metaPath()); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// Compute what idx.sessions will look like once the caller applies its
	// own delta (which happens after this method returns — see
	// UpsertSession/DeleteSession), from a fresh idx.mu.RLock() snapshot.
	// This is a short, non-blocking critical section — no disk I/O or
	// cross-process locking happens while idx.mu is held.
	idx.mu.RLock()
	expected := make(map[string]SessionMeta, len(idx.sessions))
	for k, v := range idx.sessions {
		expected[k] = v
	}
	idx.mu.RUnlock()
	for k, v := range delta.upserts {
		expected[k] = v
	}
	for _, k := range delta.deletes {
		delete(expected, k)
	}

	// Only refresh the fast-path bookkeeping when merged exactly matches the
	// expected post-delta idx.sessions — i.e. no peer content is present that
	// this process's idx.turns doesn't already cover. See the func comment
	// above for why an unconditional refresh reintroduces the sessions/turns
	// split-brain cog-review flagged on PR #458. Bookkeeping fields are
	// mutated under their own short idx.mu.Lock(), separate from the
	// RLock() snapshot above.
	if sessionMapsEqual(merged, expected) {
		idx.mu.Lock()
		idx.lastMetaHash = sha256Hex(b)
		if fi, statErr := os.Stat(idx.metaPath()); statErr == nil {
			idx.lastMetaMtime = fi.ModTime()
			idx.lastMetaSize = fi.Size()
		}
		idx.mu.Unlock()
	}
	// else: leave lastMeta* exactly as they were, so the next LoadIfChanged's
	// content-hash check notices the file changed out from under its
	// recorded baseline (it no longer matches lastMetaHash) and reloads
	// properly — sessions map AND turns map together.
	return nil
}

// sessionMapsEqual reports whether a and b contain exactly the same set of
// session IDs mapped to byte-for-byte-equal SessionMeta values.
//
// This is remedy 3 for issue #494. The original implementation range'd over
// every key and called json.Marshal on each side's value individually — up
// to 2 marshals per common key, i.e. O(n) *small* marshals plus O(n) string
// comparisons, measured at ~24ms of the ~66ms writeMetaFileLocked round trip
// against the live 7,547-session index. encoding/json marshals
// map[string]T's keys in sorted order (guaranteed by the stdlib since map
// key ordering is otherwise undefined), so two maps with identical
// key/value content always produce byte-identical JSON regardless of Go's
// randomized map iteration order. That makes one whole-map marshal per side
// plus a single byte comparison exactly equivalent to the old per-value
// walk, at a fraction of the allocation and call overhead — 2 marshals
// total instead of up to 2n.
func sessionMapsEqual(a, b map[string]SessionMeta) bool {
	if len(a) != len(b) {
		return false
	}
	aj, aerr := json.Marshal(a)
	bj, berr := json.Marshal(b)
	if aerr != nil || berr != nil {
		return false
	}
	return string(aj) == string(bj)
}

// turnsLockPath is the advisory cross-process lock file guarding a single
// session's turns file. Keyed per-session (not a single shared lock across
// all sessions) so two different sessions' writers never contend on the same
// lock — only two writers of the *same* sessionID, which is the actual race.
//
// Deliberately NOT simply turnsPath(sessionID)+".lock": turnsPath("_meta")
// resolves to the same path as metaPath() ("_meta.json"), since turnsFilename
// just appends ".json" to the (slash-escaped) sessionID with no reserved-name
// exclusion. A session literally named "_meta" would otherwise make
// turnsLockPath collide with metaLockPath, and UpsertSession/DeleteSession —
// which hold the turnsLockPath lock for the whole call, including the nested
// writeMetaFileLocked call that acquires metaLockPath — would self-deadlock
// against its own second acquisition until metaLockTimeout elapses. The
// distinct ".sessions.lock" suffix (rather than plain ".lock", which would
// reproduce exactly metaLockPath's "_meta.json.lock" for sessionID=="_meta")
// guarantees turns-lock paths never collide with metaLockPath regardless of
// sessionID content.
func (idx *Index) turnsLockPath(sessionID string) string {
	return idx.turnsPath(sessionID) + ".sessions.lock"
}

// writeTurnsFileLocked persists the full turns list for sessionID. Callers
// must already hold the turnsLockPath(sessionID) lock (see
// UpsertSession/DeleteSession, which hold it across both this write and the
// companion _meta.json write as one unit).
//
// Same shape as writeMetaFileLocked and fixed for the same reason
// (cog-review, PR #458): the daemon's reconcile ticker and a
// separately-invoked "cog reconcile conversations" CLI process can both
// detect drift on the same session's source JSONL in the same window and
// each call UpsertSession(meta, turns) for the identical sessionID
// concurrently, racing on this exact file. Unlike _meta.json (a shared map
// needing a read-merge-write), each turns file is owned wholly by one
// sessionID and every writer replaces the full content, so no on-disk merge
// is needed here — only atomicity (tmp + rename, preventing a torn read on
// loadTurnsFile) and the cross-process lock the caller holds (preventing the
// interleaving itself, so writes for a given sessionID fully serialize
// rather than merely not tearing each other's bytes). Without atomicity,
// loadTurnsFile's json.Unmarshal can fail on a corrupted file and Load drops
// the session's meta and turns entirely (index.go's Load: on a per-session
// parse error it deletes the session from the in-memory map rather than
// surfacing a partial result) — a stronger data-loss outcome than #449's
// "last writer wins on one field".
//
// Cost note (issue #558): this always marshals and rewrites the ENTIRE
// turns list, not just what changed. provider.go's indexSessionIncremental
// made the parse side of a reconcile cycle O(delta) for an actively-growing
// session, but every UpsertSessions call still round-trips through here for
// its full turns slice, so the write side of a cycle with any new turns
// remains O(session-size) — unaddressed by that change. A large,
// steadily-growing session can still cost close to its full projection
// size per cycle on the write path alone.
func (idx *Index) writeTurnsFileLocked(sessionID string, turns []Turn) error {
	b, err := json.MarshalIndent(turns, "", "  ")
	if err != nil {
		return err
	}
	path := idx.turnsPath(sessionID)
	tmp := path + fmt.Sprintf(".tmp.%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (idx *Index) loadTurnsFile(sessionID string) ([]Turn, error) {
	data, err := os.ReadFile(idx.turnsPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var turns []Turn
	if err := json.Unmarshal(data, &turns); err != nil {
		return nil, fmt.Errorf("parse turns file: %w", err)
	}
	return turns, nil
}

// ─── text helpers ─────────────────────────────────────────────────────────────

// makeExcerpt returns a ~maxLen-char snippet from text that contains query.
// If the query is not found (shouldn't happen after Search filter) returns
// the beginning of the text.
func makeExcerpt(text, query string, maxLen int) string {
	ltext := strings.ToLower(text)
	lq := strings.ToLower(query)

	pos := strings.Index(ltext, lq)
	if pos < 0 {
		if len(text) <= maxLen {
			return text
		}
		return text[:maxLen] + "…"
	}

	// Center the excerpt on the match.
	half := maxLen / 2
	start := pos - half
	if start < 0 {
		start = 0
	}
	end := start + maxLen
	if end > len(text) {
		end = len(text)
		start = end - maxLen
		if start < 0 {
			start = 0
		}
	}

	excerpt := text[start:end]
	if start > 0 {
		excerpt = "…" + excerpt
	}
	if end < len(text) {
		excerpt = excerpt + "…"
	}
	return excerpt
}
