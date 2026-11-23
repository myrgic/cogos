// conversations_search_test.go — tests for the transcript searcher adapter and
// its wiring (myrgic/cogos#650).
package all

import (
	"strings"
	"testing"

	"github.com/myrgic/cogos/internal/conversations"
	"github.com/myrgic/cogos/internal/engine"
)

// transcriptURI must address exactly one turn. A source-scoped session uses the
// source form. A Claude Code session (empty source) uses the whole-observatory
// form, since a leading source segment would be read as a source name.
func TestTranscriptURI_Forms(t *testing.T) {
	if got := transcriptURI("hermes-cog", "sess-1", "uuid-1"); got != "cog:conversations/hermes-cog/sess-1#id-uuid-1" {
		t.Errorf("source form = %q", got)
	}
	if got := transcriptURI("", "sess-1", "uuid-1"); got != "cog:conversations#id-uuid-1" {
		t.Errorf("CC form = %q", got)
	}
}

// The adapter must surface the provider's unloaded-index error unchanged, so
// the engine can report transcripts: unavailable.
func TestConversationsSearcher_PropagatesUninitialised(t *testing.T) {
	s := &conversationsSearcher{p: conversations.NewProvider()}
	_, err := s.SearchTranscripts("anything", 10)
	if err == nil {
		t.Fatal("want error from uninitialised provider, got nil")
	}
}

// RegisterConversations must wire the searcher into engine. This is the
// package-level half of the wiring check. The binary-level half is in
// cmd/cogos.
func TestRegisterConversations_WiresEngineSearcher(t *testing.T) {
	prev := engine.WiredConversationSearcher()
	t.Cleanup(func() { engine.SetConversationSearcher(prev) })
	engine.SetConversationSearcher(nil)

	RegisterConversations(conversations.NewProvider())

	if engine.WiredConversationSearcher() == nil {
		t.Fatal("RegisterConversations did not wire engine.ConversationSearcher")
	}
}

var _ = strings.Contains

func TestBareSessionID(t *testing.T) {
	if got := bareSessionID("hermes-cog", "hermes-cog/sess-1"); got != "sess-1" {
		t.Errorf("source-prefixed key: got %q, want sess-1", got)
	}
	if got := bareSessionID("", "sess-1"); got != "sess-1" {
		t.Errorf("CC key: got %q, want sess-1", got)
	}
	if got := bareSessionID("hermes-cog", "other/sess-1"); got != "other/sess-1" {
		t.Errorf("foreign prefix must be left alone: got %q", got)
	}
}
