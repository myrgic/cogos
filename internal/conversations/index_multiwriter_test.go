// index_multiwriter_test.go — cross-process write-race regression test for
// _meta.json (issue #449).
//
// index_race_test.go covers concurrency within a single *Index (in-memory map
// races between goroutines sharing one process's view). This file covers the
// distinct failure mode reported in #449: two independent *Index instances —
// standing in for a CLI-invoked "cog reconcile conversations" process and the
// daemon's own reconcile cycle — pointed at the same on-disk projDir, each
// doing its own read-modify-write of _meta.json with no shared in-memory
// state. Before the filelock+merge fix, whichever instance's
// writeMetaFileLocked ran last would marshal only its own idx.sessions and
// silently drop whatever the other instance had already added — the exact
// "last writer wins" bug the issue describes.
package conversations

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/filelock"
)

// upsertRetryAttempts/upsertRetryBackoff bound how many times
// upsertWithRetry will retry a call that fails solely with
// filelock.ErrLockTimeout before giving up.
//
// This is the fix for the CI flake tracked by this test: filelock.Acquire's
// own doc comment documents ErrLockTimeout as the expected, recoverable
// outcome of losing a bounded race for a busy advisory lock — not evidence
// of a stuck peer or data corruption. TestCrossProcessUpsertRace drives 200
// combined UpsertSession calls (2 writers x 100 disjoint sessions) through
// the single shared metaLockPath lock, each call also creating its own
// unique turns file + per-session lock file on disk (~400 distinct files
// created/renamed in one directory over the test's run). On a quiet local
// machine that finishes in ~1.2s total. On GitHub's shared runners, this
// package's tests run as part of `go test -race -count=1 ./...`, where
// every other package's tests — including internal/testkernel, which alone
// takes ~230s and spawns real subprocesses — execute concurrently across
// the runner's small vCPU budget. Every historical CI failure of this test
// (confirmed via `gh run view --log-failed` across runs from 2026-07-17
// through today, predating the internal/conversations commit 59606d7
// investigated as the suspected regression by 5 days, i.e. the flake is not
// new and not caused by that commit) shows the identical clean error --
// "conversations/index: acquire meta lock: filelock: timed out waiting for
// lock" -- never a data-mismatch or corruption assertion. That rules out a
// production correctness race: the merge-by-delta write path
// (writeMetaFileLocked) has never once been observed to drop or corrupt an
// entry under this stress. The test's actual flaw is treating one expected,
// documented, bounded-wait timeout under synthetic 200-call contention on a
// loaded shared runner as an unconditional hard failure, rather than
// retrying it the way any real caller contending on this advisory lock
// should. Retrying here is deliberately scoped to the test: it does not
// touch the shared metaLockTimeout production constant (still 5s for every
// real caller), so real cross-process callers keep the same fail-fast
// bound against a genuinely wedged peer.
const (
	upsertRetryAttempts = 5
	upsertRetryBackoff  = 150 * time.Millisecond
)

// upsertWithRetry calls idx.UpsertSession, retrying up to upsertRetryAttempts
// additional times if (and only if) the failure is a filelock.ErrLockTimeout
// -- see the doc comment on upsertRetryAttempts for why that specific error
// is safe and correct to retry here. UpsertSession is safe to retry: a
// timeout on the metaLockPath acquisition happens only after this call's own
// turns-file write already completed (and is idempotent — the retry
// rewrites identical content), so re-running the whole call is not a partial
// double-apply of anything user-visible. Any other error is returned
// immediately, unretried, so a genuine bug still fails the test on the first
// occurrence.
func upsertWithRetry(idx *Index, meta SessionMeta, turns []Turn) error {
	var lastErr error
	for attempt := 0; attempt <= upsertRetryAttempts; attempt++ {
		lastErr = idx.UpsertSession(meta, turns)
		if lastErr == nil {
			return nil
		}
		if !errors.Is(lastErr, filelock.ErrLockTimeout) {
			return lastErr
		}
		time.Sleep(upsertRetryBackoff)
	}
	return fmt.Errorf("upsertWithRetry: %s: %d attempts exhausted: %w", meta.SessionID, upsertRetryAttempts+1, lastErr)
}

// TestCrossProcessUpsertRace drives two separate *Index instances against the
// same projDir concurrently, each upserting its own disjoint set of sessions,
// and asserts that every session from both instances survives on disk in a
// well-formed _meta.json — i.e. no interleaving of the two writers silently
// drops an entry or corrupts the file.
func TestCrossProcessUpsertRace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	idxA, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex A: %v", err)
	}
	idxB, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}

	const perWriter = 100
	now := time.Now()

	writer := func(idx *Index, tag string, wg *sync.WaitGroup, errs chan<- error) {
		defer wg.Done()
		for i := 0; i < perWriter; i++ {
			sid := tag + "-" + strconv.Itoa(i)
			meta := SessionMeta{SessionID: sid, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now}
			turns := []Turn{{UUID: sid, SessionID: sid, TurnIndex: 0, Role: "user", Timestamp: now, Text: "hi " + sid}}
			if err := upsertWithRetry(idx, meta, turns); err != nil {
				errs <- err
				return
			}
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go writer(idxA, "proc-a", &wg, errs)
	go writer(idxB, "proc-b", &wg, errs)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("writer error: %v", err)
	}

	// The on-disk _meta.json must be valid JSON and contain every session
	// from both writers — nothing dropped, nothing torn.
	raw, err := os.ReadFile(idxA.metaPath())
	if err != nil {
		t.Fatalf("read _meta.json: %v", err)
	}
	var onDisk map[string]SessionMeta
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatalf("_meta.json is not valid JSON after concurrent writers: %v\ncontent: %s", err, raw)
	}

	want := 2 * perWriter
	if len(onDisk) != want {
		missingA, missingB := 0, 0
		for i := 0; i < perWriter; i++ {
			if _, ok := onDisk["proc-a-"+strconv.Itoa(i)]; !ok {
				missingA++
			}
			if _, ok := onDisk["proc-b-"+strconv.Itoa(i)]; !ok {
				missingB++
			}
		}
		t.Fatalf("_meta.json has %d sessions, want %d (missing from proc-a: %d, missing from proc-b: %d) — writers clobbered each other",
			len(onDisk), want, missingA, missingB)
	}
}

// TestCrossProcessDeleteSurvivesConcurrentUpsert verifies that a delete from
// one Index instance is not resurrected by a concurrent upsert wave from a
// second instance touching unrelated session keys — the pendingDeletes /
// merge-then-delete ordering in writeMetaFileLocked.
func TestCrossProcessDeleteSurvivesConcurrentUpsert(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	idxA, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex A: %v", err)
	}
	idxB, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}

	now := time.Now()
	doomed := SessionMeta{SessionID: "doomed", TurnCount: 1, FirstTurnAt: now, LastTurnAt: now}
	if err := idxA.UpsertSession(doomed, []Turn{{UUID: "doomed", SessionID: "doomed", TurnIndex: 0, Role: "user", Timestamp: now, Text: "x"}}); err != nil {
		t.Fatalf("seed UpsertSession: %v", err)
	}
	// idxB has not yet Loaded, so it doesn't know about "doomed" — mirrors a
	// freshly-started peer process.
	if _, err := idxB.LoadIfChanged(); err != nil {
		t.Fatalf("idxB LoadIfChanged: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := idxA.DeleteSession("doomed"); err != nil {
			errs <- err
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			sid := "other-" + strconv.Itoa(i)
			meta := SessionMeta{SessionID: sid, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now}
			if err := idxB.UpsertSession(meta, []Turn{{UUID: sid, SessionID: sid, TurnIndex: 0, Role: "user", Timestamp: now, Text: "y"}}); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("writer error: %v", err)
	}

	raw, err := os.ReadFile(idxA.metaPath())
	if err != nil {
		t.Fatalf("read _meta.json: %v", err)
	}
	var onDisk map[string]SessionMeta
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatalf("_meta.json is not valid JSON: %v", err)
	}
	if _, stillThere := onDisk["doomed"]; stillThere {
		t.Fatalf("deleted session %q was resurrected by a concurrent peer upsert", "doomed")
	}
	if len(onDisk) != 20 {
		t.Fatalf("_meta.json has %d sessions, want 20 (the concurrent 'other-*' upserts)", len(onDisk))
	}
}

// TestPeerWriteDoesNotDesyncSessionsAndTurns is the regression test for the
// finding cog-review raised on an earlier version of this fix (PR #458):
// writeMetaFileLocked must never let idx.sessions gain a peer process's
// session key without idx.turns having that session's turns loaded, because
// idx.sessions backs ListSessions/GetMeta (used by cog_list_conversations)
// while idx.turns backs GetTurn/Search (used by cog_get_turn/cog_search) —
// a split between them means a session is listed but its content is
// unreachable.
//
// Drives two Index instances against the same projDir: idxB writes a session
// idxA never loads via Load/LoadIfChanged. idxA then performs its own,
// unrelated UpsertSession (writeMetaFileLocked observes idxB's session on
// disk while merging). The invariant checked is: for every session_id idxA's
// in-memory idx.sessions knows about, idx.turns must also know about it —
// i.e. UpsertSession must never introduce a sessions/turns split-brain as a
// side effect of coordinating with a peer's write.
func TestPeerWriteDoesNotDesyncSessionsAndTurns(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	idxA, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex A: %v", err)
	}
	idxB, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}

	now := time.Now()

	// idxA establishes a baseline session so it has a non-empty in-memory
	// view before the peer write happens.
	if err := idxA.UpsertSession(
		SessionMeta{SessionID: "a-baseline", TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
		[]Turn{{UUID: "a-baseline", SessionID: "a-baseline", TurnIndex: 0, Role: "user", Timestamp: now, Text: "hi"}},
	); err != nil {
		t.Fatalf("idxA baseline UpsertSession: %v", err)
	}

	// idxB, standing in for a separate CLI process, writes a session idxA
	// never observes via Load/LoadIfChanged.
	if err := idxB.UpsertSession(
		SessionMeta{SessionID: "b-peer-only", TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
		[]Turn{{UUID: "b-peer-only", SessionID: "b-peer-only", TurnIndex: 0, Role: "user", Timestamp: now, Text: "hi from B"}},
	); err != nil {
		t.Fatalf("idxB UpsertSession: %v", err)
	}

	// idxA now does its own, unrelated write. Its writeMetaFileLocked call
	// will read _meta.json off disk (which now contains "b-peer-only") while
	// merging in its own delta.
	if err := idxA.UpsertSession(
		SessionMeta{SessionID: "a-second", TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
		[]Turn{{UUID: "a-second", SessionID: "a-second", TurnIndex: 0, Role: "user", Timestamp: now, Text: "hi again"}},
	); err != nil {
		t.Fatalf("idxA second UpsertSession: %v", err)
	}

	// Invariant: every session_id in idxA's in-memory idx.sessions must also
	// be present in idx.turns. "b-peer-only" must NOT appear in idxA's
	// idx.sessions unless idxA has actually loaded its turns (which it
	// hasn't, since it never called Load/LoadIfChanged after idxB's write).
	// Scoped explicitly (not deferred) so the RLock is released before the
	// LoadIfChanged call further down, which takes its own write lock.
	func() {
		idxA.mu.RLock()
		sids := make([]string, 0, len(idxA.sessions))
		for sid := range idxA.sessions {
			sids = append(sids, sid)
		}
		_, peerKnown := idxA.sessions["b-peer-only"]
		idxA.mu.RUnlock()
		for _, sid := range sids {
			if idxA.SessionTurns(sid) == nil {
				t.Fatalf("idx.sessions contains %q but its turns are not resolvable — sessions/turns split-brain (peer-write desync)", sid)
			}
		}
		if peerKnown {
			t.Fatalf("idxA.sessions unexpectedly contains peer-only session %q without ever loading its turns", "b-peer-only")
		}
	}()

	// Meanwhile the on-disk file must still contain all three sessions —
	// idxA's write must not have dropped idxB's peer session even though
	// idxA didn't adopt it into memory.
	raw, err := os.ReadFile(idxA.metaPath())
	if err != nil {
		t.Fatalf("read _meta.json: %v", err)
	}
	var onDisk map[string]SessionMeta
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatalf("_meta.json is not valid JSON: %v", err)
	}
	for _, want := range []string{"a-baseline", "b-peer-only", "a-second"} {
		if _, ok := onDisk[want]; !ok {
			t.Fatalf("_meta.json missing expected session %q after idxA's second write; got keys: %v", want, onDisk)
		}
	}

	// And idxA's own view is self-consistent when it does reload: turns for
	// everything idxA has ever upserted are still resolvable via GetTurn.
	if _, ok := idxA.GetTurn("a-baseline", 0); !ok {
		t.Fatalf("idxA lost its own baseline turn after the peer-write interleaving")
	}
	if _, ok := idxA.GetTurn("a-second", 0); !ok {
		t.Fatalf("idxA lost its own second turn after the peer-write interleaving")
	}

	// A subsequent LoadIfChanged must detect the peer's on-disk addition
	// (content differs from idxA's own lastMetaHash baseline) and perform a
	// full reload that backfills idx.turns for "b-peer-only" too.
	reloaded, err := idxA.LoadIfChanged()
	if err != nil {
		t.Fatalf("LoadIfChanged: %v", err)
	}
	if !reloaded {
		t.Fatalf("LoadIfChanged reported no change, but the on-disk file contains idxB's peer write that idxA never adopted")
	}
	if _, ok := idxA.GetMeta("b-peer-only"); !ok {
		t.Fatalf("after LoadIfChanged, idxA still doesn't know about peer session %q", "b-peer-only")
	}
	if _, ok := idxA.GetTurn("b-peer-only", 0); !ok {
		t.Fatalf("after LoadIfChanged, idxA knows about peer session %q via sessions but still can't resolve its turns", "b-peer-only")
	}
}

// TestCrossProcessSameSessionTurnsFileRace is the regression test for the
// finding cog-review raised on PR #458's second review pass: writeTurnsFile
// itself (not just _meta.json) needs an atomic write and a cross-process
// lock, because two Index instances (standing in for the daemon's reconcile
// ticker and a separately-invoked "cog reconcile conversations" CLI process)
// can detect drift on the SAME sessionID's source JSONL in the same window
// and both call UpsertSession(meta, turns) for that identical sessionID
// concurrently — racing on the one turns file rather than two different
// ones. Before the fix this could interleave/tear the write; loadTurnsFile
// would then fail to json.Unmarshal the corrupted file and Load() drops the
// session's meta+turns from the index entirely (index.go's Load: a
// per-session parse error deletes that session from the in-memory sessions
// map) — a stronger data-loss outcome than a merely-missing field.
//
// This test can't deterministically force the interleaving (the lock
// serializes the writers), but it repeatedly drives many concurrent
// same-sessionID writers under `-race` and asserts the turns file is always
// valid, non-torn JSON afterward with the last writer's content —
// serialization, not corruption.
func TestCrossProcessSameSessionTurnsFileRace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const sessionID = "shared-session"
	const writers = 8
	const itersPerWriter = 30

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	now := time.Now()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			idx, err := NewIndex(dir)
			if err != nil {
				errs <- err
				return
			}
			for i := 0; i < itersPerWriter; i++ {
				turns := []Turn{
					{UUID: sessionID, SessionID: sessionID, TurnIndex: 0, Role: "user", Timestamp: now, Text: "writer " + strconv.Itoa(w) + " iter " + strconv.Itoa(i)},
				}
				meta := SessionMeta{SessionID: sessionID, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now}
				if err := idx.UpsertSession(meta, turns); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("writer error: %v", err)
	}

	// The turns file must be valid, untorn JSON — never partially-written
	// bytes from two interleaved writers.
	verify, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex for verify: %v", err)
	}
	turns, err := verify.loadTurnsFile(sessionID)
	if err != nil {
		t.Fatalf("turns file is corrupted after concurrent same-session writers: %v", err)
	}
	if len(turns) != 1 {
		t.Fatalf("turns file has %d entries, want exactly 1 (one writer's full replace, not a merge/tear)", len(turns))
	}
}

// TestCrossProcessDeleteUpsertSameSessionNoSplitBrain is the regression test
// for cog-review's third finding on PR #458: DeleteSession(sid) on one Index
// instance racing UpsertSession(sid, ...) on another, for the IDENTICAL
// sessionID, must never leave _meta.json listing a session whose turns file
// is missing (or vice versa) on disk. The original fix took the per-session
// turns lock and the global meta lock as two SEPARATE, independently
// released critical sections, which left a window: instance B's turns write
// completes and releases the turns lock; instance A acquires that freed
// lock, removes the turns file, and moves on to the meta lock; B then writes
// sid back into _meta.json. The fix holds ONE per-sessionID lock across both
// the turns operation and the meta write in each of UpsertSession and
// DeleteSession, so the two calls for the same sessionID are fully mutually
// exclusive end-to-end.
//
// This drives many rounds of concurrent delete-vs-upsert on the same
// sessionID from two Index instances and asserts, after each round settles,
// that the on-disk state is one of exactly two consistent outcomes:
// (a) sid absent from _meta.json AND its turns file absent, or
// (b) sid present in _meta.json AND its turns file present and valid.
// A split-brain (present in one but not the other) fails the test.
func TestCrossProcessDeleteUpsertSameSessionNoSplitBrain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const sessionID = "delete-upsert-race"
	now := time.Now()

	idxA, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex A: %v", err)
	}
	idxB, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}

	const rounds = 50
	for r := 0; r < rounds; r++ {
		// Seed the session so there's something for the delete side to race
		// against on this round.
		if err := idxB.UpsertSession(
			SessionMeta{SessionID: sessionID, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
			[]Turn{{UUID: sessionID, SessionID: sessionID, TurnIndex: 0, Role: "user", Timestamp: now, Text: "seed"}},
		); err != nil {
			t.Fatalf("round %d: seed UpsertSession: %v", r, err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := idxA.DeleteSession(sessionID); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if err := idxB.UpsertSession(
				SessionMeta{SessionID: sessionID, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
				[]Turn{{UUID: sessionID, SessionID: sessionID, TurnIndex: 0, Role: "user", Timestamp: now, Text: "round " + strconv.Itoa(r)}},
			); err != nil {
				errs <- err
			}
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("round %d: writer error: %v", r, err)
		}

		// Check on-disk consistency for this round.
		raw, err := os.ReadFile(idxA.metaPath())
		if err != nil {
			t.Fatalf("round %d: read _meta.json: %v", r, err)
		}
		var onDisk map[string]SessionMeta
		if err := json.Unmarshal(raw, &onDisk); err != nil {
			t.Fatalf("round %d: _meta.json is not valid JSON: %v", r, err)
		}
		_, listedInMeta := onDisk[sessionID]

		verify, err := NewIndex(dir)
		if err != nil {
			t.Fatalf("round %d: NewIndex for verify: %v", r, err)
		}
		// os.Stat, not loadTurnsFile, so presence/absence is unambiguous
		// regardless of turns-list length (loadTurnsFile returns nil for
		// both "file missing" and "file present but empty array").
		_, statErr := os.Stat(verify.turnsPath(sessionID))
		hasTurnsFile := statErr == nil
		if statErr != nil && !os.IsNotExist(statErr) {
			t.Fatalf("round %d: stat turns file: %v", r, statErr)
		}
		if _, parseErr := verify.loadTurnsFile(sessionID); parseErr != nil {
			t.Fatalf("round %d: turns file corrupted: %v", r, parseErr)
		}

		if listedInMeta != hasTurnsFile {
			t.Fatalf("round %d: split-brain — sid listed in _meta.json=%v but turns file present=%v", r, listedInMeta, hasTurnsFile)
		}

		// Re-sync both instances' in-memory view for the next round.
		if _, err := idxA.LoadIfChanged(); err != nil {
			t.Fatalf("round %d: idxA LoadIfChanged: %v", r, err)
		}
		if _, err := idxB.LoadIfChanged(); err != nil {
			t.Fatalf("round %d: idxB LoadIfChanged: %v", r, err)
		}
	}
}

// TestTurnsLockPathDoesNotCollideWithMetaLockPath guards against the
// self-deadlock cog-review flagged (unverified, low-likelihood) on PR #458's
// fourth review pass: a session literally named "_meta" would make
// turnsPath("_meta") resolve to the same path as metaPath() ("_meta.json"),
// so a naive turnsLockPath == turnsPath+".lock" would collide with
// metaLockPath's "_meta.json.lock" — and since UpsertSession/DeleteSession
// hold the turnsLockPath lock across the whole call, including the nested
// writeMetaFileLocked call that acquires metaLockPath, that collision would
// make UpsertSession("_meta", ...) self-deadlock against its own second lock
// acquisition until metaLockTimeout elapses.
func TestTurnsLockPathDoesNotCollideWithMetaLockPath(t *testing.T) {
	t.Parallel()

	idx, err := NewIndex(t.TempDir())
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	if got, meta := idx.turnsLockPath("_meta"), idx.metaLockPath(); got == meta {
		t.Fatalf("turnsLockPath(%q) == metaLockPath() == %q — collision would self-deadlock UpsertSession/DeleteSession for this sessionID", "_meta", meta)
	}

	// End-to-end: UpsertSession/DeleteSession for a session literally named
	// "_meta" must complete promptly, not hang until metaLockTimeout.
	now := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- idx.UpsertSession(
			SessionMeta{SessionID: "_meta", TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
			[]Turn{{UUID: "_meta", SessionID: "_meta", TurnIndex: 0, Role: "user", Timestamp: now, Text: "hi"}},
		)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("UpsertSession(\"_meta\", ...): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpsertSession(\"_meta\", ...) did not return within 2s — likely self-deadlocked on a turnsLockPath/metaLockPath collision")
	}
}

// TestReadsNotBlockedByPeerHoldingCrossProcessLock is the regression test for
// cog-review's fifth-round finding on PR #458: an earlier version of
// UpsertSession/DeleteSession held idx.mu.Lock() (the in-process
// sync.RWMutex also taken by GetMeta/ListSessions/GetTurn/Search) across the
// blocking cross-process filelock.Acquire calls, so a daemon write blocked
// waiting on a peer process's lock also froze every concurrent MCP read on
// the same Index for up to ~2×metaLockTimeout. This simulates "a peer
// process holds the per-session lock" by acquiring turnsLockPath directly
// (bypassing the Index API, the way a genuinely separate OS process would
// hold it) and asserts that concurrent GetMeta/ListSessions/Search calls on
// the same Index still return promptly rather than blocking for the
// contending UpsertSession call's lock-wait.
func TestReadsNotBlockedByPeerHoldingCrossProcessLock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	const sessionID = "contended-session"
	now := time.Now()
	if err := idx.UpsertSession(
		SessionMeta{SessionID: sessionID, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
		[]Turn{{UUID: sessionID, SessionID: sessionID, TurnIndex: 0, Role: "user", Timestamp: now, Text: "hello world"}},
	); err != nil {
		t.Fatalf("seed UpsertSession: %v", err)
	}

	// Simulate a peer process holding the per-session lock for longer than
	// this test's read-latency budget, by acquiring it directly rather than
	// through a concurrent UpsertSession call (avoids a timing race on which
	// goroutine acquires first).
	peerLock, err := filelock.Acquire(idx.turnsLockPath(sessionID), 2*time.Second)
	if err != nil {
		t.Fatalf("peer filelock.Acquire: %v", err)
	}

	// A concurrent UpsertSession for the SAME sessionID will now block
	// waiting for peerLock, up to metaLockTimeout (5s). It runs in the
	// background — this test's assertions do not wait for it — but the test
	// as a whole must not return until it has, because t.TempDir()'s
	// registered cleanup (os.RemoveAll(dir)) races this goroutine's
	// filesystem writes otherwise: releasing peerLock unblocks the write,
	// which can still be in flight (or not yet started) when RemoveAll
	// begins tearing down dir underneath it (issue #481, ~33% flake).
	//
	// t.Cleanup here is registered after t.TempDir()'s own cleanup (dir was
	// obtained above), so cleanups run LIFO: this one — release the peer
	// lock, then block until the writer goroutine actually exits — runs to
	// completion before t.TempDir()'s RemoveAll ever starts, instead of
	// racing it.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		_ = idx.UpsertSession(
			SessionMeta{SessionID: sessionID, TurnCount: 1, FirstTurnAt: now, LastTurnAt: now},
			[]Turn{{UUID: sessionID, SessionID: sessionID, TurnIndex: 0, Role: "user", Timestamp: now, Text: "contended write"}},
		)
	}()
	t.Cleanup(func() {
		peerLock.Release()
		select {
		case <-writerDone:
		case <-time.After(metaLockTimeout + 2*time.Second):
			t.Error("contended UpsertSession goroutine did not exit after peer lock release — would have raced t.TempDir() cleanup")
		}
	})

	// Give the goroutine above a moment to actually reach and block on
	// filelock.Acquire before asserting reads are unaffected.
	time.Sleep(100 * time.Millisecond)

	const readBudget = 500 * time.Millisecond
	checks := []struct {
		name string
		run  func()
	}{
		{"GetMeta", func() { idx.GetMeta(sessionID) }},
		{"ListSessions", func() { idx.ListSessions(time.Time{}, time.Time{}, "") }},
		{"Search", func() { idx.Search("hello", time.Time{}, time.Time{}, "", "", 0) }},
		{"GetTurn", func() { idx.GetTurn(sessionID, 0) }},
	}
	for _, c := range checks {
		done := make(chan struct{})
		go func() {
			c.run()
			close(done)
		}()
		select {
		case <-done:
			// OK — returned promptly, not blocked by the peer's held lock.
		case <-time.After(readBudget):
			t.Fatalf("%s did not return within %v while a peer held the per-session cross-process lock — idx.mu is being held across the blocking lock acquisition again", c.name, readBudget)
		}
	}
}
