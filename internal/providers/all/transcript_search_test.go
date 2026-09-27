package all

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/conversations"
	"github.com/myrgic/cogos/internal/engine"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// TestRegisterConversations_TranscriptSearchEndToEnd drives the real adapter
// chain behind GET /memory/search's transcript half (myrgic/cogos#650):
// RegisterConversations → engine.WiredTranscriptSearcher → Provider.SearchRecent
// → on-disk index, and proves each returned URI resolves through the engine's
// wired conversations resolver (the /v1/uri/resolve seam) to that exact turn.
func TestRegisterConversations_TranscriptSearchEndToEnd(t *testing.T) {
	// RegisterConversations mutates process globals (engine hooks and the
	// reconcile registry, which panics on a duplicate name); restore them all.
	prevTS := engine.WiredTranscriptSearcher()
	prevCR := engine.WiredConversationsResolver()
	prevWS := engine.SetProvidersWorkspace
	prevMCP := engine.RegisterMCPExtensions
	prevHTTP := engine.RegisterHTTPExtensions
	t.Cleanup(func() {
		engine.SetTranscriptSearcher(prevTS)
		engine.SetConversationsResolver(prevCR)
		engine.SetProvidersWorkspace = prevWS
		engine.RegisterMCPExtensions = prevMCP
		engine.RegisterHTTPExtensions = prevHTTP
		reconcile.UnregisterProvider("conversations")
	})

	p := conversations.NewProvider()
	RegisterConversations(p)

	ts := engine.WiredTranscriptSearcher()
	if ts == nil {
		t.Fatal("RegisterConversations did not set engine transcript searcher")
	}

	// Before the index exists: unavailable, never an empty "no matches".
	if _, err := ts.SearchTranscripts(context.Background(), "gapped", 5); !errors.Is(err, engine.ErrTranscriptsUnavailable) {
		t.Fatalf("uninitialised index: want ErrTranscriptsUnavailable, got %v", err)
	}

	// Seed an on-disk index the way the reconcile sweep would: one ingest
	// session (source-keyed) and one source-less native session.
	root := t.TempDir()
	seed, err := conversations.NewIndex(filepath.Join(root, ".cog", "state", "conversations"))
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(seed.UpsertSession(
		conversations.SessionMeta{SessionID: "src-a/sess-1", Source: "src-a", Title: "ingest session"},
		[]conversations.Turn{
			{UUID: "u-a0", SessionID: "src-a/sess-1", TurnIndex: 0, Role: conversations.RoleUser, Timestamp: base, Text: "warmup"},
			{UUID: "u-a1", SessionID: "src-a/sess-1", TurnIndex: 1, Role: conversations.RoleAssistant, Timestamp: base.Add(2 * time.Hour), Text: "temperature-gapped decoding, newer"},
		}))
	must(seed.UpsertSession(
		conversations.SessionMeta{SessionID: "native-1", Title: "native session"},
		[]conversations.Turn{
			{UUID: "u-n0", SessionID: "native-1", TurnIndex: 0, Role: conversations.RoleUser, Timestamp: base.Add(time.Hour), Text: "temperature-gapped decoding, older"},
		}))

	if _, err := p.LoadConfig(root); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, err := p.FetchLive(context.Background(), nil); err != nil {
		t.Fatalf("FetchLive: %v", err)
	}

	hits, err := ts.SearchTranscripts(context.Background(), "temperature-gapped", 10)
	if err != nil {
		t.Fatalf("SearchTranscripts: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("want 2 hits, got %d: %+v", len(hits), hits)
	}
	if hits[0].SessionID != "src-a/sess-1" || hits[1].SessionID != "native-1" {
		t.Errorf("want newest first (src-a/sess-1, native-1), got %s, %s", hits[0].SessionID, hits[1].SessionID)
	}
	if hits[0].Source != "src-a" || hits[0].Excerpt == "" || hits[0].Timestamp.IsZero() {
		t.Errorf("provenance missing on hit: %+v", hits[0])
	}

	resolver := engine.WiredConversationsResolver()
	for _, h := range hits {
		if h.URI == "" {
			t.Errorf("hit %s#%d has no URI", h.SessionID, h.TurnIndex)
			continue
		}
		raw, err := resolver.ResolveURI(context.Background(), h.URI)
		if err != nil {
			t.Errorf("resolve %q: %v", h.URI, err)
			continue
		}
		slice, ok := raw.(*conversations.ResolvedSlice)
		if !ok || len(slice.Turns) != 1 || slice.Turns[0].TurnIndex != h.TurnIndex {
			t.Errorf("%q did not resolve to exactly turn %d: %+v", h.URI, h.TurnIndex, raw)
		}
	}
}
