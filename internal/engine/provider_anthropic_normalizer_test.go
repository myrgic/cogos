// provider_anthropic_normalizer_test.go — Golden-case and checker-self-test for
// normalizeAnthropicMessages + validateAnthropicMessages.
//
// Structure:
//   - TestCheckerSelfTest: each I1-I6 invariant with a known-violating input
//   - Test_GoldenCase_*: RED (raw violates) -> GREEN (normalized clean)
//   - TestFuzzNormalizer / TestFuzzerNonVacuity: property-based fuzzer
package engine

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// ── expected violation string constants ──────────────────────────────────────

const (
	wantI1Violation         = "I1: first message must be user, got"
	wantI2UserViolation     = "I2: consecutive user at index 1"
	wantI2AssistViolation   = "I2: consecutive assistant at index 2"
	wantI3OrphanTUViolation = "I3: assistant tool_use id="
	wantI3OrphanTRViolation = "I3: orphan tool_result tool_use_id="
	wantI4Violation         = "I4: tool_result is not block-0 in user message at index"
	wantI5Violation         = "I5: assistant at index"
	wantI6Violation         = "I6: thinking block with signature in non-final assistant at index"
)

// ── checker self-test ────────────────────────────────────────────────────────

// TestCheckerSelfTest verifies that validateAnthropicMessages correctly flags
// exactly one known violation for each invariant with a minimal input.
func TestCheckerSelfTest(t *testing.T) {
	t.Parallel()

	// I1: first message must be user.
	t.Run("I1", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "hi"}}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsPrefix(vs, "I1:") {
			t.Fatalf("RED: checker did not flag I1 violation; got %v", vs)
		}
		// Exactly one violation (the I1 one); no extra I2-I6.
		for _, v := range vs {
			if strings.HasPrefix(v, "I2:") || strings.HasPrefix(v, "I3:") ||
				strings.HasPrefix(v, "I4:") || strings.HasPrefix(v, "I5:") ||
				strings.HasPrefix(v, "I6:") {
				t.Errorf("unexpected extra violation %q on minimal I1 input", v)
			}
		}
	})

	// I2-user: consecutive user messages.
	t.Run("I2-user", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "a"},
			{Role: "user", Content: "b"},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, wantI2UserViolation) {
			t.Fatalf("RED: checker did not flag consecutive-user violation; got %v", vs)
		}
	})

	// I2-assistant: consecutive assistant messages.
	t.Run("I2-assistant", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "q"},
			{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "a1"}}},
			{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "a2"}}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, wantI2AssistViolation) {
			t.Fatalf("RED: checker did not flag consecutive-assistant violation; got %v", vs)
		}
	})

	// I3: orphan tool_use (no immediately-following tool_result).
	t.Run("I3-orphan-tool_use", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "go"},
			{Role: "assistant", Content: []anthropicContentBlock{
				{Type: "tool_use", ID: "x", Name: "fn", Input: json.RawMessage("{}")},
			}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, "I3: assistant tool_use id=x") {
			t.Fatalf("RED: checker did not flag orphan tool_use; got %v", vs)
		}
	})

	// I3: orphan tool_result (no preceding assistant tool_use).
	t.Run("I3-orphan-tool_result", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "q"},
			{Role: "user", Content: []anthropicContentBlock{
				{Type: "tool_result", ToolUseID: "y", Content: "r"},
			}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, "I3: orphan tool_result tool_use_id=y") {
			t.Fatalf("RED: checker did not flag orphan tool_result; got %v", vs)
		}
	})

	// I4: text block before tool_result.
	t.Run("I4", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: []anthropicContentBlock{
				{Type: "text", Text: "t"},
				{Type: "tool_result", ToolUseID: "z", Content: "r"},
			}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, "I4:") {
			t.Fatalf("RED: checker did not flag I4 block-order violation; got %v", vs)
		}
	})

	// I5: empty assistant content.
	t.Run("I5", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "q"},
			{Role: "assistant", Content: []anthropicContentBlock{}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, "I5: assistant at index 1") {
			t.Fatalf("RED: checker did not flag I5 empty-assistant violation; got %v", vs)
		}
	})

	// I6: thinking block with signature in a non-final assistant turn.
	t.Run("I6", func(t *testing.T) {
		t.Parallel()
		msgs := []anthropicMessage{
			{Role: "user", Content: "q1"},
			{Role: "assistant", Content: []anthropicContentBlock{
				{Type: "thinking", Thinking: "t", Signature: "sig"},
			}},
			{Role: "user", Content: "q2"},
			{Role: "assistant", Content: []anthropicContentBlock{
				{Type: "text", Text: "ok"},
			}},
		}
		vs := validateAnthropicMessages(msgs)
		if !containsSubstr(vs, "I6:") {
			t.Fatalf("RED: checker did not flag I6 thinking-signature violation; got %v", vs)
		}
	})
}

// ── golden cases ─────────────────────────────────────────────────────────────

// Test_GoldenCase_LeadingAssistant: I1 violation, leading assistant dropped.
func Test_GoldenCase_LeadingAssistant(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "hi"}}},
		{Role: "user", Content: "hello"},
	}

	// RED: raw input must fail the checker with an I1 violation.
	rawViolations := validateAnthropicMessages(input)
	if !containsPrefix(rawViolations, "I1:") {
		t.Fatalf("RED phase failed: raw input did not trigger I1 violation; got %v", rawViolations)
	}

	// GREEN: normalized output must be checker-clean.
	normalized, _ := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: normalized output still violates: %v", greenViolations)
	}
	if len(normalized) != 1 || normalized[0].Role != "user" {
		t.Errorf("expected single user message; got %+v", normalized)
	}
}

// Test_GoldenCase_TrailingAssistant: I7 — a conversation ending with an assistant
// message must be repaired to end with a user message (Anthropic rejects
// assistant-terminated requests with HTTP 400 "must end with a user message").
func Test_GoldenCase_TrailingAssistant(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "trailing"}}},
	}

	// RED: raw input must fail the checker with an I7 violation.
	rawViolations := validateAnthropicMessages(input)
	if !containsPrefix(rawViolations, "I7:") {
		t.Fatalf("RED phase failed: raw input did not trigger I7 violation; got %v", rawViolations)
	}

	// GREEN: normalized output must be checker-clean and end with a user message.
	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: normalized output still violates: %v", greenViolations)
	}
	if len(normalized) != 1 || normalized[len(normalized)-1].Role != "user" {
		t.Errorf("expected sequence ending in user; got %+v", normalized)
	}
	if rpt.TrailingDropped == 0 {
		t.Error("expected TrailingDropped > 0 in RepairReport")
	}
}

// Test_GoldenCase_EvictionOrphanedToolCall: the live-bug scenario. Upstream context
// eviction drops the trailing tool_result that followed an assistant tool_use turn;
// passToolPairing (I3) strips the now-orphaned tool_use, leaving an assistant
// text-only tail; passTrailingUser (I7) must then drop that tail so the request ends
// with the user message — preventing the 400 "must end with a user message".
func Test_GoldenCase_EvictionOrphanedToolCall(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "do the thing"},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "text", Text: "calling tool"},
			{Type: "tool_use", ID: "toolu_1", Name: "search", Input: json.RawMessage(`{}`)},
		}},
		// tool_result for toolu_1 was evicted upstream — assistant tool_use is now orphaned.
	}

	normalized, _ := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("normalized output violates invariants: %v", greenViolations)
	}
	if len(normalized) == 0 || normalized[len(normalized)-1].Role != "user" {
		t.Errorf("expected sequence ending in user after orphan+trailing repair; got %+v", normalized)
	}
}

// Test_GoldenCase_ConsecutiveUser: I2 user adjacency, merged.
func Test_GoldenCase_ConsecutiveUser(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "a"},
		{Role: "user", Content: "b"},
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "ok"}}},
		// trailing user keeps the fixture I2-only (a valid request ends with user).
		{Role: "user", Content: "c"},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, wantI2UserViolation) {
		t.Fatalf("RED phase failed: expected consecutive-user violation; got %v", rawViolations)
	}

	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
	if len(normalized) != 3 {
		t.Errorf("expected 3 messages after merge (user, assistant, user); got %d", len(normalized))
	}
	if rpt.ConsecutiveMerged == 0 {
		t.Error("expected ConsecutiveMerged > 0 in RepairReport")
	}
}

// Test_GoldenCase_ConsecutiveAssistant: I2 assistant adjacency, merged with thinking strip.
func Test_GoldenCase_ConsecutiveAssistant(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "text", Text: "ans1"},
			{Type: "thinking", Thinking: "t", Signature: "sig"},
		}},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "text", Text: "ans2"},
			{Type: "thinking", Thinking: "t2", Signature: "sig2"},
		}},
		// trailing user keeps the fixture I2-only (a valid request ends with user).
		{Role: "user", Content: "followup"},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, wantI2AssistViolation) {
		t.Fatalf("RED phase failed: expected consecutive-assistant violation; got %v", rawViolations)
	}

	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
	if len(normalized) != 3 {
		t.Errorf("expected 3 messages (user, merged-assistant, user); got %d", len(normalized))
	}
	if rpt.ConsecutiveMerged == 0 {
		t.Error("expected ConsecutiveMerged > 0")
	}
}

// Test_GoldenCase_OrphanToolUse: I3 orphan tool_use dropped.
func Test_GoldenCase_OrphanToolUse(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "go"},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "tool_use", ID: "id1", Name: "fn", Input: json.RawMessage("{}")},
		}},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, "I3: assistant tool_use id=id1") {
		t.Fatalf("RED phase failed: expected orphan tool_use violation; got %v", rawViolations)
	}

	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
	if rpt.OrphanToolUseDropped == 0 && rpt.EmptyMsgAfterPairing == 0 {
		t.Error("expected OrphanToolUseDropped or EmptyMsgAfterPairing > 0")
	}
	// The assistant with only an orphan tool_use should be gone.
	for _, m := range normalized {
		if m.Role == "assistant" {
			for _, b := range toBlocks(m.Content) {
				if b.Type == "tool_use" && b.ID == "id1" {
					t.Error("orphan tool_use survived normalization")
				}
			}
		}
	}
}

// Test_GoldenCase_OrphanToolResult: I3 orphan tool_result dropped.
func Test_GoldenCase_OrphanToolResult(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "q"},
		{Role: "user", Content: []anthropicContentBlock{
			{Type: "tool_result", ToolUseID: "orphan-id", Content: "r"},
		}},
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "text", Text: "ok"}}},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, "I3: orphan tool_result tool_use_id=orphan-id") {
		t.Fatalf("RED phase failed: expected orphan tool_result violation; got %v", rawViolations)
	}

	normalized, _ := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
}

// Test_GoldenCase_SplitPairF5: F5 split-pair (tool_result evicted by foveation).
func Test_GoldenCase_SplitPairF5(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "tool_use", ID: "id1", Name: "fn", Input: json.RawMessage("{}")},
		}},
		{Role: "user", Content: "interrupt"}, // tool_result evicted — no tool_result follows
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, "I3: assistant tool_use id=id1") {
		t.Fatalf("RED phase failed: expected orphan tool_use violation; got %v", rawViolations)
	}

	normalized, _ := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
}

// Test_GoldenCase_TextBeforeToolResultF7: I4 block-order violation (the paid 500).
func Test_GoldenCase_TextBeforeToolResultF7(t *testing.T) {
	t.Parallel()
	// Build a full valid sequence first (so I3 is satisfied) but with block-order wrong.
	input := []anthropicMessage{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: []anthropicContentBlock{
			{Type: "tool_use", ID: "id1", Name: "fn", Input: json.RawMessage("{}")},
		}},
		{Role: "user", Content: []anthropicContentBlock{
			{Type: "text", Text: "injected-system"}, // text BEFORE tool_result -- I4 violation
			{Type: "tool_result", ToolUseID: "id1", Content: "r"},
		}},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, "I4:") {
		t.Fatalf("RED phase failed: expected I4 block-order violation; got %v", rawViolations)
	}

	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
	if rpt.BlockOrderReordered == 0 {
		t.Error("expected BlockOrderReordered > 0")
	}
	// Verify tool_result is now first.
	for _, m := range normalized {
		if m.Role == "user" {
			blocks := toBlocks(m.Content)
			if len(blocks) > 1 && blocks[0].Type != "tool_result" {
				t.Errorf("after normalization first block should be tool_result; got %q", blocks[0].Type)
			}
		}
	}
}

// Test_GoldenCase_EmptyAssistant: I5 empty assistant dropped.
func Test_GoldenCase_EmptyAssistant(t *testing.T) {
	t.Parallel()
	input := []anthropicMessage{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: []anthropicContentBlock{}},
		{Role: "user", Content: "r"},
	}

	rawViolations := validateAnthropicMessages(input)
	if !containsSubstr(rawViolations, "I5: assistant at index 1") {
		t.Fatalf("RED phase failed: expected I5 empty-assistant violation; got %v", rawViolations)
	}

	normalized, rpt := normalizeAnthropicMessages(input)
	greenViolations := validateAnthropicMessages(normalized)
	if len(greenViolations) != 0 {
		t.Fatalf("GREEN phase failed: %v", greenViolations)
	}
	if rpt.EmptyAssistantDropped == 0 {
		t.Error("expected EmptyAssistantDropped > 0")
	}
}

// ── property-based fuzzer ─────────────────────────────────────────────────────

// TestFuzzNormalizer generates random sequences and asserts the three core
// properties: (A) normalize returns a slice, (B) validate(normalize(x)) == [],
// (C) idempotency: normalize(normalize(x)) produces identical output.
func TestFuzzNormalizer(t *testing.T) {
	t.Parallel()
	const iterations = 10_000
	const minViolationRate = 0.60 // at least 60% of inputs must have a raw violation

	rng := rand.New(rand.NewSource(42))
	violatingInputs := 0

	for i := 0; i < iterations; i++ {
		input := generateRandomMessages(rng)

		// Non-vacuity tracking.
		if len(validateAnthropicMessages(input)) > 0 {
			violatingInputs++
		}

		// (A) normalize returns a slice.
		normalized, _ := normalizeAnthropicMessages(input)

		// (B) validate(normalize(x)) == [].
		vs := validateAnthropicMessages(normalized)
		if len(vs) > 0 {
			t.Errorf("iteration %d: normalized output violates: %v\ninput: %s",
				i, vs, debugMsgs(input))
			if t.Failed() {
				return
			}
		}

		// (C) idempotency: normalize(normalize(x)) produces identical structure.
		normalized2, _ := normalizeAnthropicMessages(normalized)
		vs2 := validateAnthropicMessages(normalized2)
		if len(vs2) > 0 {
			t.Errorf("iteration %d: second normalize pass violates: %v", i, vs2)
		}
		if !messagesEqual(normalized, normalized2) {
			t.Errorf("iteration %d: normalize is not idempotent\nfirst:  %s\nsecond: %s",
				i, debugMsgs(normalized), debugMsgs(normalized2))
			if t.Failed() {
				return
			}
		}
	}

	// Non-vacuity assertion: at least 60% of inputs must have triggered a violation.
	pct := float64(violatingInputs) / float64(iterations)
	if pct < minViolationRate {
		t.Errorf("TestFuzzerNonVacuity: only %.1f%% of inputs had raw violations (threshold %.0f%%); "+
			"fuzzer is not generating enough invalid inputs",
			pct*100, minViolationRate*100)
	}
}

// TestFuzzerNonVacuity is a standalone test asserting the non-vacuity condition
// (referred to explicitly in the test spec).
func TestFuzzerNonVacuity(t *testing.T) {
	t.Parallel()
	const iterations = 10_000
	const minViolationRate = 0.60

	rng := rand.New(rand.NewSource(99))
	violatingInputs := 0
	for i := 0; i < iterations; i++ {
		input := generateRandomMessages(rng)
		if len(validateAnthropicMessages(input)) > 0 {
			violatingInputs++
		}
	}
	pct := float64(violatingInputs) / float64(iterations)
	if pct < minViolationRate {
		t.Errorf("non-vacuity assertion failed: only %.1f%% of %d inputs triggered violations (need %.0f%%)",
			pct*100, iterations, minViolationRate*100)
	}
	t.Logf("non-vacuity: %.1f%% of %d inputs had at least one raw violation", pct*100, iterations)
}

// ── fuzzer generator ─────────────────────────────────────────────────────────

func generateRandomMessages(rng *rand.Rand) []anthropicMessage {
	n := rng.Intn(30) + 1
	msgs := make([]anthropicMessage, 0, n)

	// Pool of tool IDs we can reference.
	toolIDPool := []string{"tool-a", "tool-b", "tool-c", "tool-d", "tool-e"}
	usedIDs := make(map[string]bool)

	// 20% chance of leading assistant (I1 violation seed).
	if rng.Float64() < 0.20 {
		msgs = append(msgs, anthropicMessage{
			Role:    "assistant",
			Content: []anthropicContentBlock{{Type: "text", Text: "leading"}},
		})
	}

	for i := 0; i < n; i++ {
		msgType := rng.Intn(100)
		switch {
		case msgType < 30: // user-text
			msgs = append(msgs, anthropicMessage{Role: "user", Content: "user text"})

		case msgType < 55: // assistant-text
			msgs = append(msgs, anthropicMessage{
				Role:    "assistant",
				Content: []anthropicContentBlock{{Type: "text", Text: "assistant text"}},
			})

		case msgType < 65: // assistant with tool_use (may be orphan)
			id := toolIDPool[rng.Intn(len(toolIDPool))]
			coin := rng.Float64()
			msgs = append(msgs, anthropicMessage{
				Role: "assistant",
				Content: []anthropicContentBlock{
					{Type: "tool_use", ID: id, Name: "fn", Input: json.RawMessage("{}")},
				},
			})
			if coin < 0.50 { // 50%: add matching tool_result immediately
				usedIDs[id] = true
				msgs = append(msgs, anthropicMessage{
					Role: "user",
					Content: []anthropicContentBlock{
						{Type: "tool_result", ToolUseID: id, Content: "result"},
					},
				})
			}
			// else: orphan tool_use

		case msgType < 75: // user with tool_result (may be orphan)
			var id string
			if len(usedIDs) > 0 && rng.Float64() < 0.80 {
				for k := range usedIDs {
					id = k
					break
				}
			} else {
				id = fmt.Sprintf("random-%d", rng.Intn(100))
			}
			msgs = append(msgs, anthropicMessage{
				Role: "user",
				Content: []anthropicContentBlock{
					{Type: "tool_result", ToolUseID: id, Content: "result"},
				},
			})

		case msgType < 80: // assistant-empty (I5 seed)
			msgs = append(msgs, anthropicMessage{
				Role:    "assistant",
				Content: []anthropicContentBlock{},
			})

		case msgType < 85: // assistant with thinking (I6 seed if signed)
			sig := ""
			if rng.Float64() < 0.5 {
				sig = "some-signature"
			}
			msgs = append(msgs, anthropicMessage{
				Role: "assistant",
				Content: []anthropicContentBlock{
					{Type: "thinking", Thinking: "thought", Signature: sig},
					{Type: "text", Text: "answer"},
				},
			})

		case msgType < 95: // mixed user (may have I4 block-order violation)
			blocks := []anthropicContentBlock{{Type: "text", Text: "user text"}}
			if rng.Float64() < 0.30 { // 30%: add tool_result after text (I4 seed)
				id := toolIDPool[rng.Intn(len(toolIDPool))]
				blocks = append(blocks, anthropicContentBlock{
					Type: "tool_result", ToolUseID: id, Content: "result",
				})
			}
			if rng.Float64() < 0.30 { // 30%: shuffle blocks
				rng.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
			}
			msgs = append(msgs, anthropicMessage{Role: "user", Content: blocks})

		default: // consecutive-same-role seed (I2 seed)
			if len(msgs) > 0 {
				prev := msgs[len(msgs)-1]
				msgs = append(msgs, anthropicMessage{
					Role:    prev.Role,
					Content: []anthropicContentBlock{{Type: "text", Text: "dup"}},
				})
				if prev.Role == "assistant" {
					msgs[len(msgs)-1].Content = []anthropicContentBlock{{Type: "text", Text: "dup"}}
				}
			} else {
				msgs = append(msgs, anthropicMessage{Role: "user", Content: "seed"})
			}
		}
	}

	return msgs
}

// ── helpers ───────────────────────────────────────────────────────────────────

func containsPrefix(vs []string, prefix string) bool {
	for _, v := range vs {
		if strings.HasPrefix(v, prefix) {
			return true
		}
	}
	return false
}

func containsSubstr(vs []string, substr string) bool {
	for _, v := range vs {
		if strings.Contains(v, substr) {
			return true
		}
	}
	return false
}

func messagesEqual(a, b []anthropicMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Role != b[i].Role {
			return false
		}
		// Compare content by JSON round-trip (handles interface{}).
		aj, _ := json.Marshal(a[i].Content)
		bj, _ := json.Marshal(b[i].Content)
		if string(aj) != string(bj) {
			return false
		}
	}
	return true
}

func debugMsgs(msgs []anthropicMessage) string {
	b, _ := json.Marshal(msgs)
	return string(b)
}
