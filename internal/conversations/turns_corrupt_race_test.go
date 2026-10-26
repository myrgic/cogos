package conversations

import (
	"os"
	"sync"
	"testing"
)

// TestLazy_CorruptReadDoesNotClobberConcurrentWrite is the cog-review #718
// round-2 regression: a reader that parses STALE corrupt bytes must not
// delete a session that a concurrent writer committed while the read was in
// flight. The hook fires inside loadTurnsFile before the read, with no
// idx.mu held, and commits a writer's in-memory upsert (the cache put that
// UpsertSessions performs under idx.mu) — the read then sees the old
// corrupt bytes on disk, exactly the interleaving in the finding.
func TestLazy_CorruptReadDoesNotClobberConcurrentWrite(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	const sid = "race-sess"
	if err := idx.UpsertSession(SessionMeta{SessionID: sid, TurnCount: 1},
		[]Turn{{UUID: "old", SessionID: sid, Text: "old"}}); err != nil {
		t.Fatal(err)
	}
	// Corrupt the file and drop the cached copy so the next read hits disk.
	if err := os.WriteFile(idx.turnsPath(sid), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx.cache.clear()

	fresh := []Turn{{UUID: "fresh", SessionID: sid, Text: "fresh"}}
	var once sync.Once
	old := loadTurnsFileHook
	loadTurnsFileHook = func(s string) {
		if s != sid {
			return
		}
		once.Do(func() {
			// Concurrent writer commits (in-memory half of UpsertSessions).
			idx.mu.Lock()
			idx.sessions[sid] = SessionMeta{SessionID: sid, TurnCount: 1}
			idx.cache.put(sid, fresh)
			idx.mu.Unlock()
		})
	}
	t.Cleanup(func() { loadTurnsFileHook = old })

	tr, ok := idx.GetTurn(sid, 0)
	if !ok {
		t.Fatal("GetTurn = false: stale corrupt read deleted the concurrently committed session")
	}
	if tr.UUID != "fresh" {
		t.Fatalf("GetTurn returned %q, want the writer's fresh turn", tr.UUID)
	}
	if _, ok := idx.GetMeta(sid); !ok {
		t.Fatal("session meta gone after stale corrupt read")
	}
}
