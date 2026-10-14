package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// writeTurnsForTest synthesises n turns in the given session and returns
// their turn_ids in order. Uses NextTurnIndex so turn_index is 1..n.
func writeTurnsForTest(t *testing.T, p *Process, root, sessionID string, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		turn := &TurnRecord{
			TurnID:    uuid.NewString(),
			TurnIndex: NextTurnIndex(root, sessionID),
			SessionID: sessionID,
			Prompt:    "prompt-" + string(rune('A'+i)),
			Response:  "response-" + string(rune('A'+i)),
			Timestamp: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC),
			Provider:  "stub",
			Model:     "test",
		}
		if err := p.RecordTurn(turn); err != nil {
			t.Fatalf("RecordTurn[%d]: %v", i, err)
		}
		ids = append(ids, turn.TurnID)
	}
	return ids
}

func TestQueryEmptySession(t *testing.T) {
	root := makeWorkspace(t)
	resetTurnIndexCacheForTests()

	res, err := QueryConversation(root, ConversationQuery{SessionID: "no-such-session"})
	if err != nil {
		t.Fatalf("QueryConversation: %v", err)
	}
	if res.Count != 0 {
		t.Errorf("Count = %d; want 0", res.Count)
	}
	if len(res.Turns) != 0 {
		t.Errorf("Turns len = %d; want 0", len(res.Turns))
	}
	if res.Truncated {
		t.Errorf("Truncated = true; want false")
	}
}

func TestQuerySingleSessionTurnsOrdered(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	ids := writeTurnsForTest(t, p, root, sessionID, 5)
	_ = ids

	// Ascending (default).
	resAsc, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true, IncludeTools: true})
	if err != nil {
		t.Fatalf("asc: %v", err)
	}
	if resAsc.Count != 5 {
		t.Errorf("asc Count = %d; want 5", resAsc.Count)
	}
	for i, tr := range resAsc.Turns {
		if tr.TurnIndex != i+1 {
			t.Errorf("asc[%d].TurnIndex = %d; want %d", i, tr.TurnIndex, i+1)
		}
	}

	// Descending.
	resDesc, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true, Order: "desc"})
	if err != nil {
		t.Fatalf("desc: %v", err)
	}
	for i, tr := range resDesc.Turns {
		if tr.TurnIndex != 5-i {
			t.Errorf("desc[%d].TurnIndex = %d; want %d", i, tr.TurnIndex, 5-i)
		}
	}
}

func TestQueryAfterTurnPagination(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	_ = writeTurnsForTest(t, p, root, sessionID, 10)

	res, err := QueryConversation(root, ConversationQuery{
		SessionID:   sessionID,
		AfterTurn:   3,
		IncludeFull: true,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if res.Count != 7 {
		t.Errorf("Count = %d; want 7", res.Count)
	}
	if res.Turns[0].TurnIndex != 4 {
		t.Errorf("first TurnIndex = %d; want 4", res.Turns[0].TurnIndex)
	}
	if res.NextAfterTurn != 10 {
		t.Errorf("NextAfterTurn = %d; want 10", res.NextAfterTurn)
	}
}

func TestQueryIncludeFullFalse(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	writeTurnsForTest(t, p, root, sessionID, 2)

	// Destroy the sidecar so hydration would fail if we try to hit it.
	_ = os.Remove(turnSidecarPath(root, sessionID))

	res, err := QueryConversation(root, ConversationQuery{
		SessionID:   sessionID,
		IncludeFull: false,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if res.Count != 2 {
		t.Fatalf("Count = %d; want 2", res.Count)
	}
	// Prompts served from ledger preview must be non-empty.
	for _, tr := range res.Turns {
		if tr.Prompt == "" {
			t.Errorf("empty prompt with IncludeFull=false; expected ledger preview to carry text")
		}
	}
}

func TestQueryBadFilterCombo(t *testing.T) {
	root := makeWorkspace(t)
	_, err := QueryConversation(root, ConversationQuery{
		SessionID:  "x",
		AfterTurn:  10,
		BeforeTurn: 5,
	})
	if err == nil {
		t.Error("expected error for after_turn >= before_turn")
	}
}

func TestQueryBadOrder(t *testing.T) {
	root := makeWorkspace(t)
	_, err := QueryConversation(root, ConversationQuery{
		SessionID: "x",
		Order:     "weird",
	})
	if err == nil {
		t.Error("expected error for invalid order")
	}
}

func TestQueryLimitTruncation(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	_ = writeTurnsForTest(t, p, root, sessionID, 25)

	res, err := QueryConversation(root, ConversationQuery{
		SessionID:   sessionID,
		Limit:       5,
		IncludeFull: true,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if res.Count != 5 {
		t.Errorf("Count = %d; want 5", res.Count)
	}
	if !res.Truncated {
		t.Error("Truncated = false; want true (25 turns, limit=5)")
	}
}

func TestQuerySinceFilter(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	_ = writeTurnsForTest(t, p, root, sessionID, 5)

	// Turn timestamps are 2026-01-01 00:00, 00:01, 00:02, ...
	since := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
	res, err := QueryConversation(root, ConversationQuery{
		SessionID:   sessionID,
		Since:       since,
		IncludeFull: true,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if res.Count != 3 { // turns 3,4,5 have ts >= 00:02
		t.Errorf("Count = %d; want 3", res.Count)
	}
}

func TestQueryIncludeFullHydratesFromSidecar(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)

	// Write a turn whose prompt exceeds the preview cap → ledger has a
	// truncated preview. Sidecar carries the full text.
	fullPrompt := strings.Repeat("X", DefaultPromptPreviewCap*2+11)
	turn := &TurnRecord{
		TurnID:    uuid.NewString(),
		TurnIndex: NextTurnIndex(root, sessionID),
		SessionID: sessionID,
		Prompt:    fullPrompt,
		Response:  "r",
	}
	if err := p.RecordTurn(turn); err != nil {
		t.Fatal(err)
	}

	// IncludeFull=true (default): hydrated Prompt == fullPrompt.
	res, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Turns) != 1 {
		t.Fatalf("Turns len = %d; want 1", len(res.Turns))
	}
	got := res.Turns[0].Prompt
	if got != fullPrompt {
		t.Errorf("hydrated prompt len = %d; want %d", len(got), len(fullPrompt))
	}
	if res.Turns[0].PromptTruncated {
		t.Error("hydrated PromptTruncated = true; want false")
	}
}

// TestQueryColonSessionID covers the readTurnCompletedEvents sibling
// path-construction site (#489) end-to-end: a channel-scoped session key
// containing a colon ("http:cog") must round-trip through RecordTurn
// (ledger + sidecar) and back out through QueryConversation, with neither
// on-disk path containing the raw colon.
func TestQueryColonSessionID(t *testing.T) {
	p, root, _ := newTestProcessForTurns(t)
	sessionID := "http:cog"

	turn := &TurnRecord{
		TurnID:    uuid.NewString(),
		TurnIndex: NextTurnIndex(root, sessionID),
		SessionID: sessionID,
		Prompt:    "hello from a colon-bearing session",
		Response:  "hi",
	}
	if err := p.RecordTurn(turn); err != nil {
		t.Fatalf("RecordTurn: %v", err)
	}

	res, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true})
	if err != nil {
		t.Fatalf("QueryConversation(%q): %v", sessionID, err)
	}
	if len(res.Turns) != 1 {
		t.Fatalf("Turns len = %d; want 1", len(res.Turns))
	}
	if res.Turns[0].Prompt != turn.Prompt {
		t.Errorf("Prompt = %q; want %q", res.Turns[0].Prompt, turn.Prompt)
	}

	// Neither the ledger dir nor the sidecar file may contain the raw colon.
	ledgerDir := filepath.Join(root, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID))
	if _, err := os.Stat(ledgerDir); err != nil {
		t.Errorf("expected sanitized ledger dir to exist: %v", err)
	}
	sidecarPath := turnSidecarPath(root, sessionID)
	if strings.Contains(filepath.Base(sidecarPath), ":") {
		t.Errorf("sidecar filename %q still contains a colon", filepath.Base(sidecarPath))
	}
	if _, err := os.Stat(sidecarPath); err != nil {
		t.Errorf("expected sanitized sidecar file to exist: %v", err)
	}
}

func TestQueryToolCallsIncludedByDefault(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)

	turn := &TurnRecord{
		TurnID:    uuid.NewString(),
		TurnIndex: NextTurnIndex(root, sessionID),
		SessionID: sessionID,
		Prompt:    "q",
		Response:  "r",
		ToolCalls: []ToolCallRecord{
			{ID: "call-1", Name: "lookup", Arguments: `{"q":"x"}`, Result: `{"ok":true}`, DurationMs: 3},
			{ID: "call-2", Name: "lookup", Arguments: `{"q":"y"}`, Rejected: true, RejectReason: "bad"},
		},
	}
	if err := p.RecordTurn(turn); err != nil {
		t.Fatal(err)
	}

	// include_tools default = true.
	res, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true, IncludeTools: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Turns) != 1 {
		t.Fatalf("Turns len = %d", len(res.Turns))
	}
	if len(res.Turns[0].ToolCalls) != 2 {
		t.Errorf("ToolCalls len = %d; want 2", len(res.Turns[0].ToolCalls))
	}
	// with IncludeTools=false → tool calls omitted.
	res2, err := QueryConversation(root, ConversationQuery{SessionID: sessionID, IncludeFull: true, IncludeTools: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Turns[0].ToolCalls) != 0 {
		t.Errorf("IncludeTools=false ToolCalls len = %d; want 0", len(res2.Turns[0].ToolCalls))
	}
}

func TestQueryCountMatchesTruncationFlag(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	_ = writeTurnsForTest(t, p, root, sessionID, 20)

	res, err := QueryConversation(root, ConversationQuery{
		SessionID:   sessionID,
		IncludeFull: true,
		// Limit omitted = default 20.
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 20 {
		t.Errorf("Count = %d; want 20", res.Count)
	}
	if res.Truncated {
		t.Error("Truncated = true; want false (exactly at default limit)")
	}
}

func TestQueryCrossSessionIgnored(t *testing.T) {
	p, root, sessionID := newTestProcessForTurns(t)
	_ = writeTurnsForTest(t, p, root, sessionID, 2)

	// SessionID = "" → return empty (cross-session reads require explicit opt-in in v1).
	res, err := QueryConversation(root, ConversationQuery{SessionID: "", IncludeFull: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Errorf("Count with empty SessionID = %d; want 0", res.Count)
	}
}

// Cache/spend fields written by RecordTurn must survive the read projection;
// pre-change events (no such keys) project to zero values without error.
func TestProjectTurnFromEventUsageFields(t *testing.T) {
	mk := func(usage map[string]interface{}) EventEnvelope {
		return EventEnvelope{HashedPayload: EventPayload{
			Type: "turn.completed",
			Data: map[string]interface{}{"turn_id": "t1", "usage": usage},
		}}
	}
	got := projectTurnFromEvent(mk(map[string]interface{}{
		"input_tokens": float64(22), "output_tokens": float64(1), "total_tokens": float64(54402),
		"cache_read_tokens": float64(54367), "cache_write_tokens": float64(12),
		"request_id": "r1", "wall_ms": float64(1500),
	}), nil, false, false).Usage
	want := TurnUsage{InputTokens: 22, OutputTokens: 1, TotalTokens: 54402,
		CacheReadTokens: 54367, CacheWriteTokens: 12, RequestID: "r1", WallMS: 1500}
	if got != want {
		t.Fatalf("usage = %+v; want %+v", got, want)
	}

	old := projectTurnFromEvent(mk(map[string]interface{}{
		"input_tokens": float64(5), "output_tokens": float64(2), "total_tokens": float64(7),
	}), nil, false, false).Usage
	if old != (TurnUsage{InputTokens: 5, OutputTokens: 2, TotalTokens: 7}) {
		t.Fatalf("pre-change usage = %+v; want only in/out/total", old)
	}
}
