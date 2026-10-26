package conversations

import (
	"fmt"
	"testing"
	"time"
)

// countReadsBy installs loadTurnsFileHook for the duration of the test and
// returns a pointer to the running count of turns-file reads.
func countReadsBy(t *testing.T) *int {
	t.Helper()
	reads := new(int)
	old := loadTurnsFileHook
	loadTurnsFileHook = func(string) { *reads++ }
	t.Cleanup(func() { loadTurnsFileHook = old })
	return reads
}

// raiseTurnsCacheEntries lifts the entry bound for the test so 100 sessions
// all fit, restoring the previous value on cleanup.
func raiseTurnsCacheEntries(t *testing.T, n int) {
	t.Helper()
	old := turnsCacheMaxEntries
	turnsCacheMaxEntries = n
	t.Cleanup(func() { turnsCacheMaxEntries = old })
}

func sidFor(i int) string { return fmt.Sprintf("mem-sess-%05d", i) }

// TestInvalidate_PeerUpsertEvictsOnlyChangedSession: a peer writes one session
// with new turns. B's next LoadIfChanged must drop only that session's cached
// turns, so B re-reads exactly one file and returns the peer's new turns.
func TestInvalidate_PeerUpsertEvictsOnlyChangedSession(t *testing.T) {
	raiseTurnsCacheEntries(t, 4096)
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 100, 3, 16, 50)

	reads := countReadsBy(t)

	b, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}
	if err := b.Load(); err != nil {
		t.Fatalf("B Load: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, ok := b.GetTurn(sidFor(i), 1); !ok {
			t.Fatalf("warm GetTurn(%d) failed", i)
		}
	}
	if *reads != 100 {
		t.Fatalf("warm-up read %d turns files, want 100", *reads)
	}

	// Peer C changes exactly one session. IndexedAt advances, as a real
	// projector would stamp it.
	c, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex C: %v", err)
	}
	if err := c.Load(); err != nil {
		t.Fatalf("C Load: %v", err)
	}
	const changed = 42
	csid := sidFor(changed)
	cMeta := c.sessions[csid]
	cMeta.IndexedAt = cMeta.IndexedAt.Add(time.Hour)
	cMeta.TurnCount = 2
	newTurns := []Turn{
		{UUID: csid + "-n0", SessionID: csid, TurnIndex: 0, Role: RoleUser, Text: "peer-new-0"},
		{UUID: csid + "-n1", SessionID: csid, TurnIndex: 1, Role: RoleUser, Text: "peer-new-1"},
	}
	if err := c.UpsertSession(cMeta, newTurns); err != nil {
		t.Fatalf("C UpsertSession: %v", err)
	}

	reloaded, err := b.LoadIfChanged()
	if err != nil {
		t.Fatalf("B LoadIfChanged: %v", err)
	}
	if !reloaded {
		t.Fatal("B.LoadIfChanged = false after peer write, want true")
	}

	*reads = 0
	for i := 0; i < 100; i++ {
		tr, ok := b.GetTurn(sidFor(i), 1)
		if !ok {
			t.Fatalf("GetTurn(%d) after reload failed", i)
		}
		if i == changed && tr.Text != "peer-new-1" {
			t.Fatalf("changed session returned stale turn text %q, want peer-new-1", tr.Text)
		}
	}
	if *reads != 1 {
		t.Fatalf("after peer write, GetTurn over 100 sessions read %d turns files, want 1", *reads)
	}
}

// TestInvalidate_PeerDeleteKeepsOtherSessionsCached: a peer deletes one
// session. B's reload must report it gone and serve the other 99 from cache.
func TestInvalidate_PeerDeleteKeepsOtherSessionsCached(t *testing.T) {
	raiseTurnsCacheEntries(t, 4096)
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 100, 3, 16, 50)

	reads := countReadsBy(t)

	b, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex B: %v", err)
	}
	if err := b.Load(); err != nil {
		t.Fatalf("B Load: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, ok := b.GetTurn(sidFor(i), 1); !ok {
			t.Fatalf("warm GetTurn(%d) failed", i)
		}
	}

	c, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex C: %v", err)
	}
	if err := c.Load(); err != nil {
		t.Fatalf("C Load: %v", err)
	}
	const gone = 7
	if err := c.DeleteSession(sidFor(gone)); err != nil {
		t.Fatalf("C DeleteSession: %v", err)
	}

	if reloaded, err := b.LoadIfChanged(); err != nil || !reloaded {
		t.Fatalf("B LoadIfChanged = (%v, %v), want (true, nil)", reloaded, err)
	}

	*reads = 0
	if _, ok := b.GetTurn(sidFor(gone), 1); ok {
		t.Fatal("GetTurn on deleted session returned ok, want false")
	}
	for i := 0; i < 100; i++ {
		if i == gone {
			continue
		}
		if _, ok := b.GetTurn(sidFor(i), 1); !ok {
			t.Fatalf("GetTurn(%d) after peer delete failed", i)
		}
	}
	if *reads != 0 {
		t.Fatalf("after peer delete, other sessions read %d turns files, want 0", *reads)
	}
}
