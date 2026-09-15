// provider_anthropic_normalizer_poisoned_test.go — poisoned-session replay test.
//
// Replays a captured 73-message poisoned session (session 20260530_113602) at
// truncation lengths [12,20,40,60,73]. The fixture is a real session capture
// used to reproduce a normalizer regression and is intentionally NOT checked
// into this public repo (it contains real conversational content, not
// synthetic data) — set COGOS_POISONED_SESSION_FIXTURE to its path locally to
// exercise this test; it skips cleanly when unset or the path doesn't exist,
// same as every other run of this suite including CI.
// For each length:
//  1. Convert the raw JSON to []ProviderMessage.
//  2. Call buildAnthropicRequest to produce an anthropicRequest (exercises normalizer).
//  3. Assert validateAnthropicMessages(ar.Messages) == [] (clean after normalize).
//  4. Assert idempotency: a second normalize pass produces identical output.
//
// RED-GREEN discipline: for lengths where the RAW converted messages violate an
// invariant, assert the violation BEFORE asserting the normalized output is clean.
//
// No network calls; no API key required.
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// rawPoisonedMessage is the JSON shape of the poisoned_session.json file.
type rawPoisonedMessage struct {
	Role       string `json:"role"`
	Content    string `json:"content"`
	ToolCallID string `json:"tool_call_id"`
	ToolCalls  []struct {
		ID       string `json:"id"`
		CallID   string `json:"call_id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls"`
}

// TestPoisonedSessionReplay replays the poisoned 73-message session at multiple
// truncation lengths and asserts that buildAnthropicRequest (via the normalizer)
// produces a checker-clean output.
func TestPoisonedSessionReplay(t *testing.T) {
	sessionPath := os.Getenv("COGOS_POISONED_SESSION_FIXTURE")
	if sessionPath == "" {
		t.Skip("COGOS_POISONED_SESSION_FIXTURE not set — skipping (see file header)")
	}

	data, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Skipf("poisoned session file not found (%v) — skipping", err)
	}

	var rawMsgs []rawPoisonedMessage
	if err := json.Unmarshal(data, &rawMsgs); err != nil {
		t.Fatalf("unmarshal poisoned session: %v", err)
	}

	total := len(rawMsgs)
	lengths := []int{12, 20, 40, 60, 73}

	for _, l := range lengths {
		l := l // capture
		t.Run(fmt.Sprintf("tail-%d", l), func(t *testing.T) {
			t.Parallel()

			// Tail slice: msgs[total-l:]
			start := total - l
			if start < 0 {
				start = 0
			}
			slice := rawMsgs[start:]

			// Convert to []ProviderMessage.
			providerMsgs := convertPoisonedToProvider(slice)

			// Call buildAnthropicRequest — this runs the normalizer internally.
			req := &CompletionRequest{
				Messages:     providerMsgs,
				SystemPrompt: "test system",
			}
			ar := buildAnthropicRequest("claude-sonnet-4-20250514", req, false, 8192)

			// First check the RAW converted messages (before normalization was applied
			// inside buildAnthropicRequest) for RED-GREEN discipline. We need to
			// reproduce the raw conversion to check it separately.
			rawConverted := convertRawToAnthropicMessages(providerMsgs)
			rawViolations := validateAnthropicMessages(rawConverted)
			// RED (hard): the poisoned session MUST demonstrate a real violation at
			// every length, else this test proves nothing — a green-only test on a
			// clean input is worthless. All five lengths 500'd live before R1.
			if len(rawViolations) == 0 {
				t.Fatalf("RED phase failed: tail-%d raw input was already clean — "+
					"the poisoned-session replay must exhibit a real /v1/messages violation", l)
			}
			t.Logf("RED confirmed: tail-%d raw input has %d violation(s): %v",
				l, len(rawViolations), rawViolations)

			// GREEN: the normalized output from buildAnthropicRequest must be clean.
			vs := validateAnthropicMessages(ar.Messages)
			if len(vs) != 0 {
				t.Errorf("GREEN phase FAILED: buildAnthropicRequest output has %d violation(s): %v",
					len(vs), vs)
			}

			// Idempotency: a second normalize pass must produce identical output.
			normalized2, _ := normalizeAnthropicMessages(ar.Messages)
			if !messagesEqual(ar.Messages, normalized2) {
				t.Errorf("idempotency FAILED: second normalize pass changed the output")
			}

			t.Logf("tail-%d: %d messages after normalization (from %d)", l, len(ar.Messages), l)
		})
	}
}

// convertPoisonedToProvider maps rawPoisonedMessage to ProviderMessage,
// matching the ScoredMessage/FormatForProvider field layout used in production.
func convertPoisonedToProvider(raw []rawPoisonedMessage) []ProviderMessage {
	out := make([]ProviderMessage, 0, len(raw))
	for _, m := range raw {
		pm := ProviderMessage{
			Role:       m.Role,
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
		}
		for _, tc := range m.ToolCalls {
			id := tc.ID
			if id == "" {
				id = tc.CallID
			}
			args := tc.Function.Arguments
			if args == "" {
				args = "{}"
			}
			pm.ToolCalls = append(pm.ToolCalls, ToolCall{
				ID:        id,
				Name:      tc.Function.Name,
				Arguments: args,
			})
		}
		out = append(out, pm)
	}
	return out
}

// convertRawToAnthropicMessages performs the same ProviderMessage->anthropicMessage
// conversion as buildAnthropicRequest (without the normalizer) so we can observe
// the raw wire-format violations for RED-GREEN discipline.
func convertRawToAnthropicMessages(msgs []ProviderMessage) []anthropicMessage {
	var out []anthropicMessage
	for _, m := range msgs {
		switch {
		case m.Role == "tool" && m.ToolCallID != "":
			out = append(out, anthropicMessage{
				Role: "user",
				Content: []anthropicContentBlock{{
					Type:      "tool_result",
					ToolUseID: m.ToolCallID,
					Content:   m.Content,
				}},
			})
		case m.Role == "assistant" && len(m.ToolCalls) > 0:
			var blocks []anthropicContentBlock
			if m.Content != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "text", Text: m.Content})
			}
			for _, tc := range m.ToolCalls {
				blocks = append(blocks, anthropicContentBlock{
					Type:  "tool_use",
					ID:    tc.ID,
					Name:  tc.Name,
					Input: json.RawMessage(tc.Arguments),
				})
			}
			out = append(out, anthropicMessage{Role: "assistant", Content: blocks})
		default:
			out = append(out, anthropicMessage{Role: m.Role, Content: m.Content})
		}
	}
	return out
}
