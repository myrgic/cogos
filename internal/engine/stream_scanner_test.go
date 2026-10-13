package engine

import (
	"context"
	"os"
	"strings"
	"testing"
)

// bigToolArgSSE builds an Anthropic stream whose single input_json_delta line
// is n bytes of argument, like a model writing a large file in one tool call.
func bigToolArgSSE(n int) string {
	arg := strings.Repeat("x", n)
	return strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"m","usage":{"input_tokens":1}}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t1","name":"write_file","input":{}}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"content\":\"` + arg + `\"}"}}`,
		``,
		`event: content_block_stop`,
		`data: {"type":"content_block_stop","index":0}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}`,
		``,
		`event: message_stop`,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
}

// A tool_use whose argument streams as one SSE line larger than bufio's
// 64 KiB default must arrive intact, not end the stream with
// "bufio.Scanner: token too long".
func TestParseAnthropicSSE_LargeToolArgumentLine(t *testing.T) {
	const n = 300 << 10 // 300 KiB in one line
	ch := make(chan StreamChunk, 64)
	go func() {
		defer close(ch)
		parseAnthropicSSE(context.Background(), strings.NewReader(bigToolArgSSE(n)), ch, "x", "anthropic")
	}()
	var errs []error
	done, argBytes := false, 0
	for c := range ch {
		if c.Error != nil {
			errs = append(errs, c.Error)
		}
		if c.Done {
			done = true
		}
		if c.ToolCallDelta != nil {
			argBytes += len(c.ToolCallDelta.ArgsDelta)
		}
	}
	if len(errs) > 0 {
		t.Fatalf("stream errored on a %d-byte SSE line: %v", n, errs[0])
	}
	if !done {
		t.Fatal("stream never reached its final chunk")
	}
	if argBytes < n {
		t.Fatalf("tool argument truncated: got %d bytes, want >= %d", argBytes, n)
	}
}

func TestNewStreamScanner_LimitIsBounded(t *testing.T) {
	sc := newStreamScanner(strings.NewReader(strings.Repeat("y", maxStreamLineBytes+1)))
	if sc.Scan() {
		t.Fatal("a line over maxStreamLineBytes must still be refused")
	}
	if sc.Err() == nil {
		t.Fatal("expected an error for an over-limit line")
	}
}

// The subprocess providers read NDJSON with the same scanner. A Claude Code
// stream_event whose input_json_delta is one 300 KiB line (over the old
// 256 KiB cap) must parse into a complete tool call.
func TestDrainStreamJSON_LargeToolArgumentLine(t *testing.T) {
	const n = 300 << 10
	arg := strings.Repeat("x", n)
	lines := []string{
		`{"type":"system","subtype":"init","session_id":"s","tools":[],"mcp_servers":[]}`,
		`{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_01","name":"write_file"}}}`,
		`{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"content\":\"` + arg + `\"}"}}}`,
		`{"type":"stream_event","event":{"type":"content_block_stop","index":0}}`,
		`{"type":"result","subtype":"success","result":"","is_error":false,"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`,
	}
	p := &ClaudeCodeProvider{name: "test", model: "sonnet"}
	_, toolCalls, _, _, err := p.drainStreamJSON(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatalf("stream errored on a %d-byte NDJSON line: %v", n, err)
	}
	if len(toolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(toolCalls))
	}
	if got := len(toolCalls[0].Arguments); got < n {
		t.Fatalf("tool argument truncated: got %d bytes, want >= %d", got, n)
	}
}

// Every file that streams model output must build its line scanner with
// newStreamScanner, so the per-line limit is one decision, not one per
// provider (four providers were left at 256 KiB / 1 MiB when only two were
// raised). A bare bufio.NewScanner in these files is the regression.
func TestProviders_UseStreamScanner(t *testing.T) {
	files := []string{
		"provider_anthropic.go", "provider_openai.go", "provider_ollama.go",
		"provider_claudecode.go", "provider_codex.go", "provider_pi.go", "chat.go",
	}
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		s := string(src)
		if !strings.Contains(s, "newStreamScanner(") {
			t.Errorf("%s: no newStreamScanner call (the streaming read must use it)", f)
		}
		for i, line := range strings.Split(s, "\n") {
			if strings.Contains(line, "bufio.NewScanner(") && !strings.Contains(line, "os.Stdin") {
				t.Errorf("%s:%d: bare bufio.NewScanner on a stream; use newStreamScanner", f, i+1)
			}
		}
	}
}
