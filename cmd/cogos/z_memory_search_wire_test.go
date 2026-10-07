// z_memory_search_wire_test.go — binary-assembly regression guard for the
// transcript half of GET /memory/search (myrgic/cogos#650).
//
// Same failure class as z_conversations_wire_test.go: unit tests of the
// fusion logic and of the adapter pass on their own while the daemon binary
// never calls engine.SetTranscriptSearcher, so memory search silently answers
// with cogdocs only (reported as sources.transcript = "unavailable"). These
// tests run inside cmd/cogos, after the package's init() chain — the exact
// wiring the daemon boots with.
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/myrgic/cogos/internal/engine"
)

// TestTranscriptSearcherWiredIntoEngine fails at `go test ./cmd/cogos` if the
// daemon binary stops wiring the observatory into memory search.
func TestTranscriptSearcherWiredIntoEngine(t *testing.T) {
	if engine.WiredTranscriptSearcher() == nil {
		t.Fatal("engine transcript searcher is nil after cmd/cogos init — " +
			"GET /memory/search cannot see conversation transcripts. " +
			"engine.SetTranscriptSearcher must be called from " +
			"providers/all.RegisterConversations (linked via z_conversations_wire.go)")
	}
}

// TestTranscriptSearcherDelegatesToDaemonProvider proves the wired searcher
// is connected to the daemon's conversations provider singleton, not a stub:
// its readiness must track that provider's index.
func TestTranscriptSearcherDelegatesToDaemonProvider(t *testing.T) {
	ts := engine.WiredTranscriptSearcher()
	if ts == nil {
		t.Fatal("transcript searcher not wired (see TestTranscriptSearcherWiredIntoEngine)")
	}

	// Initialise and load the shared provider's index against a scratch
	// workspace, mirroring engine.SetProvidersWorkspace + the first reconcile
	// FetchLive at daemon boot.
	if _, err := daemonConversationsProvider.LoadConfig(t.TempDir()); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, err := daemonConversationsProvider.FetchLive(context.Background(), nil); err != nil {
		t.Fatalf("FetchLive: %v", err)
	}

	hits, err := ts.SearchTranscripts(context.Background(), "anything", 5)
	if errors.Is(err, engine.ErrTranscriptsUnavailable) {
		t.Fatalf("wired searcher reports unavailable after the daemon provider's "+
			"index loaded — it is not delegating to daemonConversationsProvider: %v", err)
	}
	if err != nil {
		t.Fatalf("SearchTranscripts: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("empty index returned %d hits", len(hits))
	}
}
