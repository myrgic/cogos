package engine

import "testing"

// A step with several parallel tool calls reaches buildAnthropicRequest as one assistant message carrying N
// tool_use blocks, followed by N role:"tool" messages (one per result). Each becomes its own user message, and the
// normalizer must merge them BEFORE tool pairing. Before the fix, pairing closed the tool_use scope at the first user
// message: results 2..N were dropped as orphans, then tool_use 2..N as unanswered, so the model never saw its own
// later calls and re-issued them (observed live 2026-10-13: orphan_tool_use=14 on one request, a command run twice).
func TestParallelToolResultsSurviveNormalization(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{Messages: []ProviderMessage{
		{Role: "user", Content: "go"},
		{Role: "assistant", ToolCalls: []ToolCall{
			{ID: "tu1", Name: "write_file", Arguments: "{}"},
			{ID: "tu2", Name: "terminal", Arguments: "{}"},
			{ID: "tu3", Name: "read_file", Arguments: "{}"},
		}},
		{Role: "tool", ToolCallID: "tu1", Content: "wrote"},
		{Role: "tool", ToolCallID: "tu2", Content: "ran"},
		{Role: "tool", ToolCallID: "tu3", Content: "read"},
	}}
	ar := buildAnthropicRequest("claude-opus-5-5", req, false, 1024)

	uses, results := map[string]bool{}, map[string]bool{}
	for _, m := range ar.Messages {
		for _, b := range toBlocks(m.Content) {
			switch b.Type {
			case "tool_use":
				uses[b.ID] = true
			case "tool_result":
				results[b.ToolUseID] = true
			}
		}
	}
	for _, id := range []string{"tu1", "tu2", "tu3"} {
		if !uses[id] || !results[id] {
			t.Errorf("%s lost: tool_use=%v tool_result=%v (every call of a parallel step must survive)", id, uses[id], results[id])
		}
	}
	if len(ar.Messages) != 3 {
		t.Errorf("want user, assistant, user (results merged into one message); got %d messages", len(ar.Messages))
	}
	if errs := validateAnthropicMessages(ar.Messages); len(errs) != 0 {
		t.Errorf("normalized request still invalid: %v", errs)
	}
}
