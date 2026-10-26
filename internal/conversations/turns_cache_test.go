package conversations

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// newIndexForTest returns an in-memory Index with no backing projection
// directory. Turns are seeded with cache.put (see setTurnsForTest).
func newIndexForTest(t *testing.T) *Index {
	t.Helper()
	return &Index{
		sessions: make(map[string]SessionMeta),
		cache:    newTurnsCache(),
	}
}

// setTurnsForTest sets sid's meta and turns in memory. Turns go into the cache,
// so no turns file is needed on disk.
func (idx *Index) setTurnsForTest(sid string, meta SessionMeta, turns []Turn) {
	idx.mu.Lock()
	idx.sessions[sid] = meta
	idx.mu.Unlock()
	idx.cache.put(sid, turns)
}

func TestTurnsCache_EvictsByEntries(t *testing.T) {
	oldE, oldB := turnsCacheMaxEntries, turnsCacheMaxBytes
	defer func() { turnsCacheMaxEntries, turnsCacheMaxBytes = oldE, oldB }()
	turnsCacheMaxEntries, turnsCacheMaxBytes = 2, 64<<20

	c := newTurnsCache()
	for i := 0; i < 3; i++ {
		c.put(fmt.Sprintf("s%d", i), []Turn{{UUID: "u", Text: "x"}})
	}
	if n, _ := c.stats(); n != 2 {
		t.Fatalf("entries = %d, want 2", n)
	}
	if _, ok := c.get("s0"); ok {
		t.Fatal("least-recently-used s0 should have been evicted")
	}
	if _, ok := c.get("s2"); !ok {
		t.Fatal("most-recent s2 should be cached")
	}
}

func TestTurnsCache_EvictsByBytes(t *testing.T) {
	oldE, oldB := turnsCacheMaxEntries, turnsCacheMaxBytes
	defer func() { turnsCacheMaxEntries, turnsCacheMaxBytes = oldE, oldB }()
	turnsCacheMaxEntries, turnsCacheMaxBytes = 1000, 1000

	// One turn with 100 bytes of text: estimate = 100 + 160 = 260 (< 1000/4).
	big := func() []Turn { return []Turn{{Text: string(make([]byte, 100))}} }
	c := newTurnsCache()
	for i := 0; i < 10; i++ {
		c.put(fmt.Sprintf("s%d", i), big())
	}
	_, bytes := c.stats()
	if bytes > turnsCacheMaxBytes {
		t.Fatalf("bytes = %d exceeds cap %d", bytes, turnsCacheMaxBytes)
	}
	if n, _ := c.stats(); n >= 10 {
		t.Fatalf("byte bound did not evict: entries = %d", n)
	}
}

func TestTurnsCache_OversizedEntryNotCached(t *testing.T) {
	oldB := turnsCacheMaxBytes
	defer func() { turnsCacheMaxBytes = oldB }()
	turnsCacheMaxBytes = 1000 // quarter = 250

	c := newTurnsCache()
	c.put("huge", []Turn{{Text: string(make([]byte, 500))}})
	if _, ok := c.get("huge"); ok {
		t.Fatal("entry above maxBytes/4 must not be cached")
	}
	if n, b := c.stats(); n != 0 || b != 0 {
		t.Fatalf("stats = (%d,%d), want (0,0)", n, b)
	}
}

func TestTurnsCache_PutIfGenRejectedAfterWriterOps(t *testing.T) {
	stale := []Turn{{UUID: "stale"}}

	// put after capture
	c := newTurnsCache()
	g := c.gen("s")
	c.put("s", []Turn{{UUID: "fresh"}})
	c.putIfGen("s", stale, g)
	if got, _ := c.get("s"); len(got) != 1 || got[0].UUID != "fresh" {
		t.Fatalf("putIfGen after put overwrote fresher value: %+v", got)
	}

	// invalidate after capture
	c = newTurnsCache()
	g = c.gen("s")
	c.invalidate("s")
	c.putIfGen("s", stale, g)
	if _, ok := c.get("s"); ok {
		t.Fatal("putIfGen after invalidate must be rejected")
	}

	// clear after capture
	c = newTurnsCache()
	c.put("s", []Turn{{UUID: "x"}})
	g = c.gen("s")
	c.clear()
	c.putIfGen("s", stale, g)
	if _, ok := c.get("s"); ok {
		t.Fatal("putIfGen after clear must be rejected")
	}

	// no writer op: accepted
	c = newTurnsCache()
	g = c.gen("s")
	c.putIfGen("s", stale, g)
	if _, ok := c.get("s"); !ok {
		t.Fatal("putIfGen with unchanged generation must store")
	}
}

// writeSyntheticIndex writes sessions×turnsPer turns of ~textLen bytes each
// into dir using the real UpsertSessions path, in batches of batch.
func writeSyntheticIndex(t *testing.T, dir string, sessions, turnsPer, textLen, batch int) {
	t.Helper()
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	text := string(make([]byte, textLen))
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for start := 0; start < sessions; start += batch {
		end := start + batch
		if end > sessions {
			end = sessions
		}
		var items []SessionAndTurns
		for s := start; s < end; s++ {
			sid := fmt.Sprintf("mem-sess-%05d", s)
			turns := make([]Turn, turnsPer)
			for i := range turns {
				turns[i] = Turn{
					UUID: fmt.Sprintf("%s-u%d", sid, i), SessionID: sid, TurnIndex: i,
					Role: RoleUser, Timestamp: now, Text: sid + " " + text,
				}
			}
			items = append(items, SessionAndTurns{
				Meta:  SessionMeta{SessionID: sid, TurnCount: turnsPer, FirstTurnAt: now, LastTurnAt: now},
				Turns: turns,
			})
		}
		if _, err := idx.UpsertSessions(items); err != nil {
			t.Fatalf("UpsertSessions batch %d: %v", start, err)
		}
	}
}

func heapAllocAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func TestTurnsCache_MemoryBound(t *testing.T) {
	oldE, oldB := turnsCacheMaxEntries, turnsCacheMaxBytes
	defer func() { turnsCacheMaxEntries, turnsCacheMaxBytes = oldE, oldB }()
	turnsCacheMaxEntries, turnsCacheMaxBytes = 256, 4<<20

	dir := t.TempDir()
	const sessions, turnsPer, textLen = 5000, 10, 2048 // ~100 MB of text total
	writeSyntheticIndex(t, dir, sessions, turnsPer, textLen, 500)

	// Fresh index, lazy load: Load must not read any turns file.
	var loadTurnsReads int
	oldHook := loadTurnsFileHook
	loadTurnsFileHook = func(string) { loadTurnsReads++ }
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	loadTurnsFileHook = oldHook
	if loadTurnsReads != 0 {
		t.Fatalf("Load read %d turns files; want 0 (turns must be lazy)", loadTurnsReads)
	}

	before := heapAllocAfterGC()
	for s := 0; s < sessions; s++ {
		sid := fmt.Sprintf("mem-sess-%05d", s)
		if _, ok := idx.GetTurn(sid, 0); !ok {
			t.Fatalf("GetTurn(%s, 0) not found", sid)
		}
		if n, b := idx.cache.stats(); n > turnsCacheMaxEntries || b > turnsCacheMaxBytes {
			t.Fatalf("cache out of bounds at session %d: entries=%d bytes=%d", s, n, b)
		}
	}
	after := heapAllocAfterGC()

	var growth int64 = int64(after) - int64(before)
	if growth > 40<<20 {
		t.Fatalf("HeapAlloc grew by %d bytes over the scan; want < 40 MB (cache cap is 4 MB)", growth)
	}
	t.Logf("HeapAlloc before=%d after=%d delta=%d bytes", before, after, growth)
}

func TestLazy_LoadReadsNoTurnsFile(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 20, 3, 16, 10)

	var reads int
	oldHook := loadTurnsFileHook
	loadTurnsFileHook = func(string) { reads++ }
	defer func() { loadTurnsFileHook = oldHook }()

	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if reads != 0 {
		t.Fatalf("Load read %d turns files, want 0", reads)
	}
	if _, ok := idx.GetTurn("mem-sess-00003", 1); !ok {
		t.Fatal("lazy GetTurn after Load failed")
	}
	if reads != 1 {
		t.Fatalf("first access read %d turns files, want 1", reads)
	}
	if _, ok := idx.GetTurn("mem-sess-00003", 2); !ok {
		t.Fatal("second GetTurn failed")
	}
	if reads != 1 {
		t.Fatalf("cache hit re-read the turns file (reads=%d)", reads)
	}
}

func TestLazy_PeekDoesNotInsert(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 3, 2, 8, 3)
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	turns, ok := idx.peekOrReadTurns("mem-sess-00001")
	if !ok || len(turns) != 2 {
		t.Fatalf("peekOrReadTurns = (%d turns, %v), want (2, true)", len(turns), ok)
	}
	if n, _ := idx.cache.stats(); n != 0 {
		t.Fatalf("peekOrReadTurns inserted into cache (entries=%d)", n)
	}
	if _, ok := idx.peekOrReadTurns("no-such-session"); ok {
		t.Fatal("unindexed session must report false")
	}
}

func TestLazy_ParityWriterAndColdReader(t *testing.T) {
	dir := t.TempDir()
	const n = 50
	writeSyntheticIndex(t, dir, n, 4, 32, 25)

	writer, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := writer.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cold, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex cold: %v", err)
	}
	if err := cold.Load(); err != nil {
		t.Fatalf("Load cold: %v", err)
	}

	for _, idx := range []*Index{writer, cold} {
		for s := 0; s < n; s++ {
			sid := fmt.Sprintf("mem-sess-%05d", s)
			got := idx.SessionTurns(sid)
			if len(got) != 4 {
				t.Fatalf("%s: SessionTurns len=%d want 4", sid, len(got))
			}
			for i, tr := range got {
				if tr.UUID != fmt.Sprintf("%s-u%d", sid, i) || tr.TurnIndex != i {
					t.Fatalf("%s turn %d mismatch: %+v", sid, i, tr)
				}
			}
		}
		// Search must find a session-unique term on the right session only.
		probe := fmt.Sprintf("mem-sess-%05d", 7)
		hits := idx.Search(probe, time.Time{}, time.Time{}, "", "", 0)
		if len(hits) != 4 {
			t.Fatalf("Search(%q) returned %d hits, want 4 (one session's turns)", probe, len(hits))
		}
		for _, h := range hits {
			if h.SessionID != probe {
				t.Fatalf("Search(%q) hit from session %q", probe, h.SessionID)
			}
		}
	}
}

func TestLazy_CorruptTurnsFileDropsSession(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 3, 2, 8, 3)

	bad := filepath.Join(dir, turnsFilename("mem-sess-00001"))
	if err := os.WriteFile(bad, []byte("not json{"), 0o644); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := idx.GetTurn("mem-sess-00001", 0); ok {
		t.Fatal("GetTurn on corrupt session must return false")
	}
	for _, m := range idx.ListSessions(time.Time{}, time.Time{}, "") {
		if m.SessionID == "mem-sess-00001" {
			t.Fatal("corrupt session must be removed from ListSessions")
		}
	}
	if _, ok := idx.GetTurn("mem-sess-00002", 0); !ok {
		t.Fatal("healthy neighbour session must still resolve")
	}
}

func TestLazy_RaceConcurrentReadAndUpsert(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticIndex(t, dir, 1, 2, 8, 1)
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if err := idx.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	sid := "mem-sess-00000"
	meta := SessionMeta{SessionID: sid, TurnCount: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			_, _ = idx.GetTurn(sid, 0)
		}
	}()
	var last []Turn
	for i := 0; i < 50; i++ {
		last = []Turn{{UUID: fmt.Sprintf("final-%d", i), SessionID: sid, TurnIndex: 0, Role: RoleUser, Text: "t"}}
		if err := idx.UpsertSession(meta, last); err != nil {
			t.Fatalf("UpsertSession: %v", err)
		}
	}
	<-done
	got := idx.SessionTurns(sid)
	if len(got) != 1 || got[0].UUID != last[0].UUID {
		t.Fatalf("final SessionTurns = %+v, want last upsert %q", got, last[0].UUID)
	}
}
