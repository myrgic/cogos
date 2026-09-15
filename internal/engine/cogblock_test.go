package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNormalizeOpenAIRequestProducesCogBlock(t *testing.T) {
	t.Parallel()

	req := &oaiChatRequest{
		Model: "claude",
		Messages: []oaiMessage{
			{Role: "system", Content: mustMarshalString("system context")},
			{Role: "user", Content: mustMarshalString("hello")},
			{Role: "assistant", Content: mustMarshalString("hi")},
			{Role: "user", Content: mustMarshalString("what context do you see?")},
		},
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	block := NormalizeOpenAIRequest(req, raw, "http")
	if block.ID == "" {
		t.Fatal("block ID should not be empty")
	}
	if block.Timestamp.IsZero() {
		t.Fatal("block timestamp should be set")
	}
	if block.Kind != BlockMessage {
		t.Fatalf("block.Kind = %q; want %q", block.Kind, BlockMessage)
	}
	if block.SourceChannel != "http" {
		t.Fatalf("SourceChannel = %q; want http", block.SourceChannel)
	}
	if block.SourceTransport != "openai-compat" {
		t.Fatalf("SourceTransport = %q; want openai-compat", block.SourceTransport)
	}
	if len(block.RawPayload) == 0 {
		t.Fatal("raw payload should be preserved")
	}
	if len(block.Messages) != 4 {
		t.Fatalf("messages len = %d; want 4", len(block.Messages))
	}
	if block.Messages[3].Content != "what context do you see?" {
		t.Fatalf("last normalized message = %q", block.Messages[3].Content)
	}
	if !block.TrustContext.Authenticated || block.TrustContext.TrustScore != 1.0 {
		t.Fatalf("unexpected trust context: %+v", block.TrustContext)
	}
	if block.Provenance.NormalizedBy != "http-openai" {
		t.Fatalf("NormalizedBy = %q; want http-openai", block.Provenance.NormalizedBy)
	}
}

func TestRecordBlockReturnsLedgerRef(t *testing.T) {
	t.Parallel()

	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := &Process{
		cfg:       cfg,
		sessionID: "session-record-block",
	}

	block := &CogBlock{
		ID:              "block-1",
		SourceChannel:   "http",
		SourceTransport: "openai-compat",
		TargetIdentity:  "Cog",
		WorkspaceID:     filepath.Base(root),
		Kind:            BlockMessage,
		Timestamp:       mustTime(t, "2026-04-02T12:00:00Z"),
	}

	ref := p.RecordBlock(block)
	if ref == "" {
		t.Fatal("RecordBlock should return a ledger ref")
	}
	if block.LedgerRef == "" {
		t.Fatal("block should be annotated with ledger ref")
	}

	eventsPath := filepath.Join(root, ".cog", "ledger", p.SessionID(), "events.jsonl")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("ledger should contain a recorded event")
	}
}

func mustTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", ts, err)
	}
	return parsed
}

// TestCogBlockRoundtripEmptySectionsAndPrev verifies that a CogBlock with
// no Sections and no Prev roundtrips cleanly and that both fields remain
// nil/empty after unmarshal (omitempty semantics on the wire).
func TestCogBlockRoundtripEmptySectionsAndPrev(t *testing.T) {
	t.Parallel()

	original := CogBlock{
		ID:              "block-empty",
		Timestamp:       mustTime(t, "2026-04-22T12:00:00Z"),
		Kind:            BlockMessage,
		SourceChannel:   "http",
		SourceTransport: "openai-compat",
	}

	data, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Omitempty: neither "sections" nor "prev" should appear in the wire form.
	s := string(data)
	if strings.Contains(s, `"sections"`) {
		t.Errorf("empty Sections should omit from JSON; got: %s", s)
	}
	if strings.Contains(s, `"prev"`) {
		t.Errorf("empty Prev should omit from JSON; got: %s", s)
	}

	var decoded CogBlock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decoded.Sections) != 0 {
		t.Errorf("decoded Sections = %v; want empty", decoded.Sections)
	}
	if len(decoded.Prev) != 0 {
		t.Errorf("decoded Prev = %v; want empty", decoded.Prev)
	}
}

// TestCogBlockRoundtripPopulatedSectionsAndPrev verifies that a CogBlock
// with multiple Sections and a DAG-style Prev (len>1, i.e. a merge block)
// roundtrips identically.
func TestCogBlockRoundtripPopulatedSectionsAndPrev(t *testing.T) {
	t.Parallel()

	original := CogBlock{
		ID:              "block-full",
		Timestamp:       mustTime(t, "2026-04-22T12:00:00Z"),
		Kind:            BlockMessage,
		SourceChannel:   "bus",
		SourceTransport: "bus",
		Sections: []Section{
			{Title: "Intro", Anchor: "#intro", Hash: "sha256:abc123", Size: 512},
			{Title: "Body", Anchor: "#body", Hash: "sha256:def456", Size: 2048},
		},
		// DAG merge: two predecessors. Confirms len(Prev)>1 is supported.
		Prev: []string{"sha256:aaa", "sha256:bbb"},
	}

	data, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded CogBlock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(decoded.Sections) != 2 {
		t.Fatalf("Sections len = %d; want 2", len(decoded.Sections))
	}
	if decoded.Sections[0] != original.Sections[0] {
		t.Errorf("Sections[0] = %+v; want %+v", decoded.Sections[0], original.Sections[0])
	}
	if decoded.Sections[1] != original.Sections[1] {
		t.Errorf("Sections[1] = %+v; want %+v", decoded.Sections[1], original.Sections[1])
	}

	// DAG merge semantics: Prev has two entries, order preserved.
	if len(decoded.Prev) != 2 {
		t.Fatalf("Prev len = %d; want 2 (DAG merge)", len(decoded.Prev))
	}
	if decoded.Prev[0] != "sha256:aaa" || decoded.Prev[1] != "sha256:bbb" {
		t.Errorf("Prev = %v; want [sha256:aaa sha256:bbb]", decoded.Prev)
	}
}

// TestCogBlockLinearChainPrev verifies the V2 DAG linear-chain shape:
// Prev with exactly one predecessor (len(Prev)==1 — linear chain).
func TestCogBlockLinearChainPrev(t *testing.T) {
	t.Parallel()

	original := CogBlock{
		ID:              "block-linear",
		Timestamp:       mustTime(t, "2026-04-22T12:00:00Z"),
		Kind:            BlockMessage,
		SourceChannel:   "http",
		SourceTransport: "openai-compat",
		Prev:            []string{"sha256:predecessor"},
	}

	data, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded CogBlock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decoded.Prev) != 1 {
		t.Fatalf("Prev len = %d; want 1 (linear chain)", len(decoded.Prev))
	}
	if decoded.Prev[0] != "sha256:predecessor" {
		t.Errorf("Prev[0] = %q; want sha256:predecessor", decoded.Prev[0])
	}
}
