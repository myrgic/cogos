package conversations

import (
	"fmt"
	"testing"
	"time"
)

// streamFixture returns sessions s00..s(n-1), each with turnsPer user turns.
// Session s starts 1000 minutes after session s-1, so session time ranges are
// disjoint and ascending in sid order. Turn i is at start+i minutes.
func streamFixture(sessions, turnsPer int) []SessionAndTurns {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	out := make([]SessionAndTurns, 0, sessions)
	for s := 0; s < sessions; s++ {
		sid := fmt.Sprintf("s%02d", s)
		start := base.Add(time.Duration(s) * 1000 * time.Minute)
		turns := make([]Turn, turnsPer)
		for i := range turns {
			turns[i] = Turn{
				UUID:      fmt.Sprintf("%s-u%d", sid, i),
				SessionID: sid,
				TurnIndex: i,
				Role:      RoleUser,
				Timestamp: start.Add(time.Duration(i) * time.Minute),
				Text:      fmt.Sprintf("turn text %s-%d", sid, i),
			}
		}
		out = append(out, SessionAndTurns{
			Meta: SessionMeta{
				SessionID:   sid,
				TurnCount:   turnsPer,
				FirstTurnAt: start,
				LastTurnAt:  start.Add(time.Duration(turnsPer-1) * time.Minute),
			},
			Turns: turns,
		})
	}
	return out
}

// buildDiskIndex writes data to a fresh projection dir and returns an Index
// loaded from it, so turns are read from disk (cache cleared by Load).
func buildDiskIndex(t *testing.T, data []SessionAndTurns) *Index {
	t.Helper()
	dir := t.TempDir()
	idx, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	if _, err := idx.UpsertSessions(data); err != nil {
		t.Fatalf("UpsertSessions: %v", err)
	}
	idx2, err := NewIndex(dir)
	if err != nil {
		t.Fatalf("NewIndex reopen: %v", err)
	}
	if err := idx2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	return idx2
}

type turnKey struct {
	sid string
	idx int
}

// refOldAlgorithm is an inline copy of the pre-#716 resolver: collect every
// matching turn in ascending sid order, reverse for desc, then apply offset
// and limit (limit 0 = unlimited).
func refOldAlgorithm(data []SessionAndTurns, since, until time.Time, desc bool, offset, limit int) []turnKey {
	var all []turnKey
	for _, st := range data {
		for _, t := range st.Turns {
			if !since.IsZero() && t.Timestamp.Before(since) {
				continue
			}
			if !until.IsZero() && t.Timestamp.After(until) {
				continue
			}
			all = append(all, turnKey{t.SessionID, t.TurnIndex})
		}
	}
	if desc {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}
	if offset > 0 {
		if offset >= len(all) {
			all = nil
		} else {
			all = all[offset:]
		}
	}
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all
}

func sliceKeys(s *ResolvedSlice) []turnKey {
	out := make([]turnKey, 0, len(s.Turns))
	for _, rt := range s.Turns {
		out = append(out, turnKey{rt.SessionID, rt.TurnIndex})
	}
	return out
}

func equalKeys(a, b []turnKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// countTurnsReads installs loadTurnsFileHook for the duration of the test.
func countTurnsReads(t *testing.T) *int {
	t.Helper()
	reads := new(int)
	old := loadTurnsFileHook
	loadTurnsFileHook = func(string) { *reads++ }
	t.Cleanup(func() { loadTurnsFileHook = old })
	return reads
}

// Pre-filter: a since query over three sessions with disjoint ranges reads
// only the session whose range can match, and returns the same hits as the
// unfiltered reference.
func TestScanStream_SincePreFilterReadsOnlyMatchingSession(t *testing.T) {
	data := streamFixture(3, 10)
	idx := buildDiskIndex(t, data)
	reads := countTurnsReads(t)

	since := data[2].Turns[0].Timestamp // session s02 starts here
	uq := &URIQuery{Since: since, Order: "asc", Res: ResFull}
	slice, err := resolveQuery("cog:conversations?since=x", uq, idx)
	if err != nil {
		t.Fatalf("resolveQuery: %v", err)
	}
	if *reads != 1 {
		t.Errorf("since query read %d turns files, want 1 (s02 only)", *reads)
	}
	want := refOldAlgorithm(data, since, time.Time{}, false, 0, 0)
	if !equalKeys(sliceKeys(slice), want) {
		t.Errorf("since result mismatch: got %d turns, want %d", len(slice.Turns), len(want))
	}

	*reads = 0
	hits := idx.Search("turn", since, time.Time{}, "", "", 0)
	if *reads != 1 {
		t.Errorf("Search with since read %d turns files, want 1", *reads)
	}
	gotHits := make([]turnKey, 0, len(hits))
	for _, h := range hits {
		gotHits = append(gotHits, turnKey{h.SessionID, h.TurnIndex})
	}
	if !equalKeys(gotHits, want) {
		t.Errorf("Search since hits mismatch: got %d, want %d", len(gotHits), len(want))
	}
}

// Early stop: for each limit/offset/order combo over 20 sessions x 10 turns,
// the streamed result must equal the old algorithm's result.
func TestScanStream_EarlyStopMatchesOldAlgorithm(t *testing.T) {
	data := streamFixture(20, 10)
	idx := buildDiskIndex(t, data)

	combos := []struct{ limit, offset int }{{5, 0}, {5, 7}, {25, 3}}
	for _, order := range []string{"asc", "desc"} {
		desc := order == "desc"
		for _, c := range combos {
			uq := &URIQuery{Limit: c.limit, Offset: c.offset, Order: order, Res: ResFull}
			slice, err := resolveQuery("cog:conversations", uq, idx)
			if err != nil {
				t.Fatalf("resolveQuery %s %+v: %v", order, c, err)
			}
			want := refOldAlgorithm(data, time.Time{}, time.Time{}, desc, c.offset, c.limit)
			if !equalKeys(sliceKeys(slice), want) {
				t.Errorf("order=%s limit=%d offset=%d: got %d turns, want %d (sequence differs)",
					order, c.limit, c.offset, len(slice.Turns), len(want))
			}
		}
	}
}

// Default cap: an all-sessions query with no limit returns exactly 200 turns
// when more exist, equal to the first 200 of the old unlimited result.
func TestScanStream_AllSessionsDefaultCap(t *testing.T) {
	data := streamFixture(30, 10) // 300 turns
	idx := buildDiskIndex(t, data)

	for _, order := range []string{"asc", "desc"} {
		desc := order == "desc"
		uq := &URIQuery{Limit: 0, Order: order, Res: ResFull}
		slice, err := resolveQuery("cog:conversations", uq, idx)
		if err != nil {
			t.Fatalf("resolveQuery %s: %v", order, err)
		}
		if len(slice.Turns) != allSessionsDefaultLimit {
			t.Fatalf("order=%s: got %d turns, want %d", order, len(slice.Turns), allSessionsDefaultLimit)
		}
		full := refOldAlgorithm(data, time.Time{}, time.Time{}, desc, 0, 0)
		if !equalKeys(sliceKeys(slice), full[:allSessionsDefaultLimit]) {
			t.Errorf("order=%s: first 200 differ from old algorithm", order)
		}
	}
}

// Files read: an asc limit=5 all-sessions query over 20 sessions reads at most
// 2 turns files (the first session already has enough turns).
func TestScanStream_AscLimitReadsFewFiles(t *testing.T) {
	data := streamFixture(20, 10)
	idx := buildDiskIndex(t, data)
	reads := countTurnsReads(t)

	uq := &URIQuery{Limit: 5, Order: "asc", Res: ResFull}
	if _, err := resolveQuery("cog:conversations", uq, idx); err != nil {
		t.Fatalf("resolveQuery: %v", err)
	}
	if *reads > 2 {
		t.Errorf("asc limit=5 read %d turns files, want <= 2", *reads)
	}
}

// Single-session scope with no limit stays unlimited: all 10 turns returned.
func TestScanStream_SingleSessionNoLimitUnchanged(t *testing.T) {
	data := streamFixture(20, 10)
	idx := buildDiskIndex(t, data)

	uq := &URIQuery{SessionID: "s03", Limit: 0, Order: "asc", Res: ResFull}
	slice, err := resolveQuery("cog:conversations/x/s03", uq, idx)
	if err != nil {
		t.Fatalf("resolveQuery: %v", err)
	}
	if len(slice.Turns) != 10 {
		t.Errorf("single session no limit: got %d turns, want 10", len(slice.Turns))
	}
}
