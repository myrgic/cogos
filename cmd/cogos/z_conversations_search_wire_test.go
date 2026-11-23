// z_conversations_search_wire_test.go — binary-assembly regression guard for
// transcript search in GET /memory/search (myrgic/cogos#650).
//
// Like z_conversations_wire_test.go, this runs inside cmd/cogos, after the
// package's init() chain has run. It proves the daemon binary wires the
// conversations searcher into the engine. A unit test of the adapter alone
// cannot catch a missing call in the daemon.
package main

import (
	"testing"

	"github.com/myrgic/cogos/internal/engine"
)

// TestConversationSearcherWiredIntoDaemon fails if the searcher is not wired
// after the daemon init sequence, so /memory/search would silently return
// cogdoc-only results. It must report transcripts: unavailable instead.
func TestConversationSearcherWiredIntoDaemon(t *testing.T) {
	if engine.WiredConversationSearcher() == nil {
		t.Fatal("engine ConversationSearcher is nil after cmd/cogos init — " +
			"SetConversationSearcher is not reached from RegisterConversations " +
			"in the daemon binary (/memory/search would be cogdoc-only)")
	}
}
