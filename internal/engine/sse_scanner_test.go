package engine

import (
	"context"
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

func TestNewSSEScanner_LimitIsBounded(t *testing.T) {
	sc := newSSEScanner(strings.NewReader(strings.Repeat("y", maxSSELineBytes+1)))
	if sc.Scan() {
		t.Fatal("a line over maxSSELineBytes must still be refused")
	}
	if sc.Err() == nil {
		t.Fatal("expected an error for an over-limit line")
	}
}
