package conversations

import (
	"context"
	"errors"
	"testing"
	"time"
)

// buildRecencyIndex builds an index where lexical session-id order is the
// OPPOSITE of recency: "a-old" sorts first but holds the oldest matches. The
// unranked Search returns a-old's turns first; SearchRecent must not.
func buildRecencyIndex(t *testing.T) *Index {
	t.Helper()
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	idx := &Index{
		sessions: map[string]SessionMeta{
			"a-old":          {SessionID: "a-old", Title: "old session"},
			"src-x/z-new":    {SessionID: "src-x/z-new", Source: "src-x", Title: "new session"},
			"m-unrelated-01": {SessionID: "m-unrelated-01"},
		},
		turns: map[string][]Turn{
			"a-old": {
				{UUID: "u-old-0", TurnIndex: 0, Timestamp: base, Text: "the widget frobnicator idea"},
				{UUID: "u-old-1", TurnIndex: 1, Timestamp: base.Add(time.Minute), Text: "widget frobnicator again"},
			},
			"src-x/z-new": {
				{UUID: "u-new-0", TurnIndex: 0, Timestamp: base.Add(48 * time.Hour), Text: "Widget FROBNICATOR revisited"},
				{UUID: "", TurnIndex: 1, Timestamp: base.Add(49 * time.Hour), Text: "no match here"},
				{UUID: "", TurnIndex: 2, Timestamp: base.Add(50 * time.Hour), Text: "frobnicator widget, newest"},
			},
			"m-unrelated-01": {
				{UUID: "u-m-0", TurnIndex: 0, Timestamp: base.Add(100 * time.Hour), Text: "only frobnicator, no w-word"},
			},
		},
		loaded: true,
	}
	return idx
}

func TestSearchRecent_NewestFirstNotSessionIDOrder(t *testing.T) {
	idx := buildRecencyIndex(t)

	// Control: the unranked Search, limit 2, returns the lexically-first
	// session's (oldest) turns — the defect SearchRecent exists to avoid.
	unranked := idx.Search("widget frobnicator", time.Time{}, time.Time{}, "", "", 2)
	if len(unranked) != 2 || unranked[0].SessionID != "a-old" {
		t.Fatalf("control: expected unranked Search to start with a-old, got %+v", unranked)
	}

	hits, err := idx.SearchRecent(context.Background(), "widget frobnicator", time.Time{}, 2)
	if err != nil {
		t.Fatalf("SearchRecent: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("want 2 hits, got %d: %+v", len(hits), hits)
	}
	if hits[0].SessionID != "src-x/z-new" || hits[0].TurnIndex != 2 {
		t.Errorf("hit[0] = %s#%d, want newest src-x/z-new#2", hits[0].SessionID, hits[0].TurnIndex)
	}
	if hits[1].SessionID != "src-x/z-new" || hits[1].TurnIndex != 0 {
		t.Errorf("hit[1] = %s#%d, want src-x/z-new#0", hits[1].SessionID, hits[1].TurnIndex)
	}
	if hits[0].Source != "src-x" || hits[0].SessionTitle != "new session" {
		t.Errorf("meta not carried onto hit: %+v", hits[0])
	}
	if hits[0].Excerpt == "" {
		t.Error("excerpt empty")
	}

	// Full set, strictly newest first; AND semantics exclude the one-term turn.
	all, _ := idx.SearchRecent(context.Background(), "widget frobnicator", time.Time{}, 50)
	if len(all) != 4 {
		t.Fatalf("want 4 AND-matches, got %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Timestamp.After(all[i-1].Timestamp) {
			t.Fatalf("not newest-first at %d: %v after %v", i, all[i].Timestamp, all[i-1].Timestamp)
		}
	}
}

func TestSearchRecent_SinceAndEmptyAndZeroLimit(t *testing.T) {
	idx := buildRecencyIndex(t)
	since := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	hits, _ := idx.SearchRecent(context.Background(), "widget frobnicator", since, 10)
	for _, h := range hits {
		if h.Timestamp.Before(since) {
			t.Errorf("hit older than since: %+v", h)
		}
	}
	if len(hits) != 2 {
		t.Errorf("want 2 hits after since, got %d", len(hits))
	}
	if h, _ := idx.SearchRecent(context.Background(), "   ", time.Time{}, 10); len(h) != 0 {
		t.Errorf("empty query must not list every turn, got %d", len(h))
	}
	if h, _ := idx.SearchRecent(context.Background(), "widget", time.Time{}, 0); h != nil {
		t.Errorf("limit 0 must return nil, got %d", len(h))
	}
}

func TestSearchRecent_HonoursCancelledContext(t *testing.T) {
	idx := buildRecencyIndex(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := idx.SearchRecent(ctx, "widget", time.Time{}, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// TestHitURI_ResolvesToExactlyThatTurn: every URI HitURI emits must resolve,
// through the same resolver /v1/uri/resolve uses, to exactly the hit's turn.
func TestHitURI_ResolvesToExactlyThatTurn(t *testing.T) {
	idx := buildRecencyIndex(t)
	hits, _ := idx.SearchRecent(context.Background(), "widget frobnicator", time.Time{}, 50)
	if len(hits) == 0 {
		t.Fatal("no hits")
	}
	for _, h := range hits {
		uri := HitURI(h)
		if uri == "" {
			t.Errorf("no URI for %s#%d", h.SessionID, h.TurnIndex)
			continue
		}
		slice, err := ResolveConversationURI(uri, idx)
		if err != nil {
			t.Errorf("resolve %q: %v", uri, err)
			continue
		}
		if len(slice.Turns) != 1 {
			t.Errorf("%q resolved to %d turns, want exactly 1", uri, len(slice.Turns))
			continue
		}
		if got := slice.Turns[0]; got.TurnIndex != h.TurnIndex {
			t.Errorf("%q resolved to turn %d, want %d", uri, got.TurnIndex, h.TurnIndex)
		}
	}

	// Source-less, UUID-less turn: no resolvable form; must be "" not a lie.
	if got := HitURI(SearchHit{SessionID: "bare", TurnIndex: 3}); got != "" {
		t.Errorf("unresolvable hit got URI %q", got)
	}
}

// TestProviderSearchRecent_NotReady: an uninitialised or never-loaded index
// must report ErrIndexNotReady, never an empty (i.e. "no matches") result.
func TestProviderSearchRecent_NotReady(t *testing.T) {
	var nilP *Provider
	if _, err := nilP.SearchRecent(context.Background(), "x", time.Time{}, 5); !errors.Is(err, ErrIndexNotReady) {
		t.Errorf("nil provider: want ErrIndexNotReady, got %v", err)
	}

	p := NewProvider()
	if _, err := p.SearchRecent(context.Background(), "x", time.Time{}, 5); !errors.Is(err, ErrIndexNotReady) {
		t.Errorf("no LoadConfig: want ErrIndexNotReady, got %v", err)
	}

	root := t.TempDir()
	if _, err := p.LoadConfig(root); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, err := p.SearchRecent(context.Background(), "x", time.Time{}, 5); !errors.Is(err, ErrIndexNotReady) {
		t.Errorf("index created but never loaded: want ErrIndexNotReady, got %v", err)
	}

	if _, err := p.FetchLive(context.Background(), nil); err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	hits, err := p.SearchRecent(context.Background(), "x", time.Time{}, 5)
	if err != nil {
		t.Fatalf("after load: want nil error, got %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("empty index returned %d hits", len(hits))
	}
}
