package acp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestParseLine_UserFrame_TruncatedFromGoldenCorpus is a small, self-
// contained regression for the ParseLine dispatch gap this lane fixes:
// EventUser was declared but had no case in the switch, so every
// tool_result frame fell through to Unknown. The line below is lifted
// verbatim from testdata/golden_tool_turn_baseline.ndjson (see
// TestParseLine_UserFrame_RealGoldenFixtures for the full-corpus check).
func TestParseLine_UserFrame_TruncatedFromGoldenCorpus(t *testing.T) {
	const line = `{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_01J63H6L1A8fbuu6ESUg1D9P","type":"tool_result","content":"1\tmodule github.com/myrgic/cogos"}]},"parent_tool_use_id":null,"session_id":"3bf3ff20-8504-4616-a747-bd1511afced4","uuid":"39efbc79-fae9-40b1-b163-e0edfc31dd42"}`

	ev, err := ParseLine([]byte(line))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	if ev.Unknown != nil {
		t.Fatalf("user frame fell through to Unknown (type=%q) — EventUser dispatch gap not fixed", ev.Unknown.Type)
	}
	if ev.User == nil {
		t.Fatalf("expected Event.User to be populated, got %+v", ev)
	}
	if ev.User.Message.Role != "user" {
		t.Errorf("Message.Role = %q, want %q", ev.User.Message.Role, "user")
	}
	if len(ev.User.Message.Content) != 1 {
		t.Fatalf("Message.Content len = %d, want 1", len(ev.User.Message.Content))
	}
	item := ev.User.Message.Content[0]
	if item.Type != "tool_result" {
		t.Errorf("Content[0].Type = %q, want %q", item.Type, "tool_result")
	}
	if item.ToolUseID != "toolu_01J63H6L1A8fbuu6ESUg1D9P" {
		t.Errorf("Content[0].ToolUseID = %q, want the captured tool_use_id", item.ToolUseID)
	}
	var text string
	if err := json.Unmarshal(item.Content, &text); err != nil {
		t.Fatalf("Content[0].Content did not decode as a string: %v", err)
	}
	if text == "" {
		t.Errorf("Content[0].Content decoded empty")
	}
	if ev.User.SessionID != "3bf3ff20-8504-4616-a747-bd1511afced4" {
		t.Errorf("SessionID = %q, unexpected", ev.User.SessionID)
	}
}

func TestParseLine_RateLimitEvent(t *testing.T) {
	const line = `{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":1787920200,"rateLimitType":"five_hour","overageStatus":"rejected","overageDisabledReason":"out_of_credits","isUsingOverage":false,"unifiedWindows":{"five_hour":{"utilization":0.07,"resetsAt":1787920200},"seven_day":{"utilization":0.26,"resetsAt":1788184800}}},"uuid":"4b16c8d9-c72a-4f6f-a2c7-6b442b6dafcd","session_id":"3bf3ff20-8504-4616-a747-bd1511afced4"}`

	ev, err := ParseLine([]byte(line))
	if err != nil {
		t.Fatalf("ParseLine: %v", err)
	}
	if ev.Unknown != nil {
		t.Fatalf("rate_limit_event fell through to Unknown (type=%q) — EventRateLimit dispatch gap not fixed", ev.Unknown.Type)
	}
	if ev.RateLimit == nil {
		t.Fatalf("expected Event.RateLimit to be populated, got %+v", ev)
	}
	if ev.RateLimit.RateLimitInfo.Status != "allowed" {
		t.Errorf("RateLimitInfo.Status = %q, want %q", ev.RateLimit.RateLimitInfo.Status, "allowed")
	}
	fiveHour, ok := ev.RateLimit.RateLimitInfo.UnifiedWindows["five_hour"]
	if !ok {
		t.Fatalf("UnifiedWindows missing five_hour key")
	}
	if fiveHour.Utilization != 0.07 {
		t.Errorf("UnifiedWindows[five_hour].Utilization = %v, want 0.07", fiveHour.Utilization)
	}
}

// TestParseLine_SystemSubtypes_AlreadyRouteAsSystem documents (and locks
// in) that the four contract-drift subtypes L1 found —
// thinking_tokens/status/task_summary/post_turn_summary — were never
// actually falling into Unknown: ParseLine's EventSystem case dispatches
// on the "system" type alone, ignoring Subtype, so any subtype decodes
// into SystemEvent. This test exists so a future refactor of that switch
// can't silently regress these into Unknown without a test noticing.
func TestParseLine_SystemSubtypes_AlreadyRouteAsSystem(t *testing.T) {
	subtypes := []string{
		SystemSubtypeInit,
		SystemSubtypeHookStarted,
		SystemSubtypeHookResponse,
		SystemSubtypeThinkingTokens,
		SystemSubtypeStatus,
		SystemSubtypeTaskSummary,
		SystemSubtypePostTurnSummary,
	}
	for _, st := range subtypes {
		line := `{"type":"system","subtype":"` + st + `","session_id":"x"}`
		ev, err := ParseLine([]byte(line))
		if err != nil {
			t.Fatalf("subtype %q: ParseLine: %v", st, err)
		}
		if ev.System == nil {
			t.Fatalf("subtype %q: expected Event.System, got %+v", st, ev)
		}
		if ev.System.Subtype != st {
			t.Errorf("subtype %q: SystemEvent.Subtype = %q", st, ev.System.Subtype)
		}
	}
}

// TestParseLine_RealGoldenFixtures replays every recorded frame in the L1
// golden corpus through ParseLine and asserts that the only frame types
// still landing in Unknown are ones nobody has claimed to handle yet
// (there are none as of this lane — the corpus's user and rate_limit_event
// frames were the last gap). Skips if the fixtures aren't present.
func TestParseLine_RealGoldenFixtures(t *testing.T) {
	files, err := filepath.Glob("testdata/golden_*.ndjson")
	if err != nil {
		t.Fatalf("glob testdata: %v", err)
	}
	if len(files) == 0 {
		t.Skip("no golden_*.ndjson fixtures in testdata/")
	}

	var sawUser, sawRateLimit bool
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, line := range splitNDJSONLines(data) {
			if len(line) == 0 {
				continue
			}
			ev, err := ParseLine(line)
			if err != nil {
				t.Fatalf("%s: ParseLine: %v (line: %s)", f, err, line)
			}
			if ev.Unknown != nil {
				t.Errorf("%s: frame type %q fell through to Unknown: %s", f, ev.Unknown.Type, line)
			}
			if ev.User != nil {
				sawUser = true
			}
			if ev.RateLimit != nil {
				sawRateLimit = true
			}
		}
	}
	if !sawUser {
		t.Errorf("expected at least one real user (tool_result) frame across the golden corpus")
	}
	if !sawRateLimit {
		t.Errorf("expected at least one real rate_limit_event frame across the golden corpus")
	}
}
