package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func cacheReq(msgs ...anthropicMessage) *anthropicRequest {
	return &anthropicRequest{
		Model:    "m",
		System:   []anthropicSystemBlock{{Type: "text", Text: "canonical"}, {Type: "text", Text: "nucleus"}},
		Tools:    []anthropicTool{{Name: "a"}, {Name: "b"}},
		Messages: msgs,
	}
}

func countBreakpoints(t *testing.T, r *anthropicRequest) int {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(b), `"cache_control":{"type":"ephemeral"}`)
}

func TestCacheBreakpoints_PlacedOnStableRegionEnds(t *testing.T) {
	t.Parallel()
	r := cacheReq(
		anthropicMessage{Role: "user", Content: "hi"},
		anthropicMessage{Role: "assistant", Content: []anthropicContentBlock{{Type: "tool_use", ID: "c1", Name: "a", Input: json.RawMessage(`{}`)}}},
		anthropicMessage{Role: "user", Content: []anthropicContentBlock{{Type: "tool_result", ToolUseID: "c1", Content: "ok"}}},
	)
	applyAnthropicCacheBreakpoints(r)
	sys := r.System.([]anthropicSystemBlock)
	if sys[0].CacheControl != nil || sys[1].CacheControl == nil {
		t.Fatalf("system: only the LAST block must carry cache_control; got %+v", sys)
	}
	if r.Tools[0].CacheControl != nil || r.Tools[1].CacheControl == nil {
		t.Fatalf("tools: only the LAST tool must carry cache_control")
	}
	// second-to-last message (the assistant tool_use) is marked; the final
	// tool_result — the only cold region — is not.
	if blocks := r.Messages[1].Content.([]anthropicContentBlock); blocks[0].CacheControl == nil {
		t.Fatal("second-to-last message must carry the history breakpoint")
	}
	if blocks := r.Messages[2].Content.([]anthropicContentBlock); blocks[0].CacheControl != nil {
		t.Fatal("final message must NOT carry cache_control (it is the cold region)")
	}
	if n := countBreakpoints(t, r); n != 3 {
		t.Fatalf("breakpoints = %d; want 3 (Anthropic max is 4)", n)
	}
}

func TestCacheBreakpoints_StringContentConvertedToBlock(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "hello"}, anthropicMessage{Role: "assistant", Content: "hi"})
	applyAnthropicCacheBreakpoints(r)
	blocks, ok := r.Messages[0].Content.([]anthropicContentBlock)
	if !ok || len(blocks) != 1 || blocks[0].Type != "text" || blocks[0].Text != "hello" || blocks[0].CacheControl == nil {
		t.Fatalf("string content must become one marked text block; got %+v", r.Messages[0].Content)
	}
	if _, isStr := r.Messages[1].Content.(string); !isStr {
		t.Fatal("final message content must be left untouched")
	}
}

func TestCacheBreakpoints_SingleMessageNoHistoryMark(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "first turn"})
	applyAnthropicCacheBreakpoints(r)
	if n := countBreakpoints(t, r); n != 2 {
		t.Fatalf("with one message only system+tools are marked; breakpoints = %d", n)
	}
}

func TestCacheBreakpoints_Idempotent(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "a"}, anthropicMessage{Role: "assistant", Content: "b"}, anthropicMessage{Role: "user", Content: "c"})
	applyAnthropicCacheBreakpoints(r)
	applyAnthropicCacheBreakpoints(r)
	if n := countBreakpoints(t, r); n != 3 {
		t.Fatalf("double application must not add breakpoints; got %d", n)
	}
}

// The property that makes caching WORK: Anthropic's cache matches on the
// content of the three regions in spec order (system, tools, messages) up to
// each breakpoint — not on raw JSON bytes. So across consecutive agentic
// steps: system and tools must be unchanged, and the messages up to step N's
// history breakpoint must be a prefix of step N+1's messages. If the
// breakpoint ever landed on content that changes per step, every step would
// be a cold read — the bug this fixes.
func TestCacheBreakpoints_PrefixStableAcrossAgenticSteps(t *testing.T) {
	t.Parallel()
	mustJSON := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	turn := []anthropicMessage{{Role: "user", Content: "list files"}}
	hop1 := []anthropicMessage{
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "tool_use", ID: "c1", Name: "a", Input: json.RawMessage(`{}`)}}},
		{Role: "user", Content: []anthropicContentBlock{{Type: "tool_result", ToolUseID: "c1", Content: "x.go"}}},
	}
	hop2 := []anthropicMessage{
		{Role: "assistant", Content: []anthropicContentBlock{{Type: "tool_use", ID: "c2", Name: "b", Input: json.RawMessage(`{}`)}}},
		{Role: "user", Content: []anthropicContentBlock{{Type: "tool_result", ToolUseID: "c2", Content: "y.go"}}},
	}
	cat := func(parts ...[]anthropicMessage) []anthropicMessage {
		var out []anthropicMessage
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	stepN := cacheReq(cat(turn, hop1)...)
	stepN1 := cacheReq(cat(turn, hop1, hop2)...)
	applyAnthropicCacheBreakpoints(stepN)
	applyAnthropicCacheBreakpoints(stepN1)

	// stable regions identical
	if mustJSON(stepN.System) != mustJSON(stepN1.System) {
		t.Fatal("system region changed between steps")
	}
	if mustJSON(stepN.Tools) != mustJSON(stepN1.Tools) {
		t.Fatal("tools region changed between steps")
	}
	// The CONTENT of step N's messages (markers stripped) must be a prefix of
	// step N+1's content. Anthropic keys the cache on content hashes of the
	// prefix at each breakpoint and looks prior breakpoints up automatically,
	// so N+1 hits N's cached span even though N+1's marker has moved forward.
	strip := func(s string) string { return strings.ReplaceAll(s, `,"cache_control":{"type":"ephemeral"}`, "") }
	cN := strings.TrimSuffix(strip(mustJSON(stepN.Messages)), "]")
	cN1 := strip(mustJSON(stepN1.Messages))
	if !strings.HasPrefix(cN1, cN) {
		t.Fatalf("step N's message content is not a prefix of step N+1\n N : %s\n N1: %s", cN[:min(len(cN), 240)], cN1[:min(len(cN1), 240)])
	}
	// and N+1's breakpoint advanced strictly past N's
	if len(stepN1.Messages)-1 <= len(stepN.Messages)-1 {
		t.Fatal("step N+1's breakpoint must advance past step N's")
	}
}

// ── Review round 2: twin surface + usage forwarding ───────────────────────────

// Finding 1: the API-key AnthropicProvider shares buildAnthropicRequest with
// the OAuth path. Breakpoints must ship from that shared exit, not just the
// OAuth call sites.
func TestCacheBreakpoints_ApiKeyPathShipsBreakpoints(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "nucleus",
		Messages: []ProviderMessage{
			{Role: "user", Content: "list files"},
			{Role: "assistant", ToolCalls: []ToolCall{{ID: "c1", Name: "ls", Arguments: `{}`}}},
			{Role: "tool", ToolCallID: "c1", Content: "x.go"},
		},
		Tools: []ToolDefinition{{Name: "ls", Description: "list", InputSchema: map[string]any{"type": "object"}}},
	}
	payload := buildAnthropicRequest("claude-sonnet-4-6", req, false, 100)
	b, _ := json.Marshal(payload)
	if n := strings.Count(string(b), `"cache_control":{"type":"ephemeral"}`); n < 2 {
		t.Fatalf("API-key path (buildAnthropicRequest) shipped %d breakpoints; want >=2 (tools + history)", n)
	}
	if payload.Tools[len(payload.Tools)-1].CacheControl == nil {
		t.Fatal("last tool def must carry cache_control on the API-key path")
	}
}

// Finding 2a: the SSE parser must capture cache accounting from message_start.
func TestParseAnthropicSSE_CapturesCacheUsage(t *testing.T) {
	t.Parallel()
	sse := strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"m","role":"assistant","content":[],"model":"x","usage":{"input_tokens":17,"output_tokens":0,"cache_read_input_tokens":1637,"cache_creation_input_tokens":72}}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`,
		``,
		`event: content_block_stop`,
		`data: {"type":"content_block_stop","index":0}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
		``,
		`event: message_stop`,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	ch := make(chan StreamChunk, 16)
	go func() { // mirrors the provider: caller owns close()
		defer close(ch)
		parseAnthropicSSE(context.Background(), strings.NewReader(sse), ch, "x", "anthropic")
	}()
	var final *StreamChunk
	for c := range ch {
		if c.Done {
			cc := c
			final = &cc
		}
	}
	if final == nil || final.Usage == nil {
		t.Fatal("final chunk must carry Usage")
	}
	if final.Usage.CacheReadTokens != 1637 || final.Usage.CacheWriteTokens != 72 {
		t.Fatalf("cache accounting dropped: read=%d write=%d; want 1637/72", final.Usage.CacheReadTokens, final.Usage.CacheWriteTokens)
	}
}

// Finding 2b: /v1/messages must forward cache accounting to the client —
// non-streaming response body and streaming message_delta.usage.
func TestHandleAnthropicMessages_ForwardsCacheUsage(t *testing.T) {
	t.Parallel()
	mk := func() *Server {
		srv := newTestServer(t)
		router := NewSimpleRouter(RoutingConfig{Default: "stub"})
		router.RegisterProvider(NewStubProvider("stub", "ok").WithUsage(TokenUsage{InputTokens: 17, OutputTokens: 1, CacheReadTokens: 1637, CacheWriteTokens: 72}))
		srv.SetRouter(router)
		return srv
	}
	t.Run("non-stream", func(t *testing.T) {
		srv := mk()
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":false}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.handleAnthropicMessages(w, req)
		var resp struct {
			Usage map[string]int `json:"usage"`
		}
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatal(err)
		}
		if resp.Usage["cache_read_input_tokens"] != 1637 || resp.Usage["cache_creation_input_tokens"] != 72 {
			t.Fatalf("non-stream usage did not forward cache fields: %v", resp.Usage)
		}
	})
	t.Run("stream", func(t *testing.T) {
		srv := mk()
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.handleAnthropicMessages(w, req)
		var delta map[string]any
		for _, block := range strings.Split(w.Body.String(), "\n\n") {
			if !strings.Contains(block, "event: message_delta") {
				continue
			}
			for _, line := range strings.Split(block, "\n") {
				if strings.HasPrefix(line, "data: ") {
					_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &delta)
				}
			}
		}
		u, _ := delta["usage"].(map[string]any)
		if u == nil || u["cache_read_input_tokens"] != float64(1637) {
			t.Fatalf("stream message_delta.usage did not forward cache_read: %v", delta)
		}
	})
}

// Review round 2: the API-key path builds System as a STRING. The system
// breakpoint must apply there too — it is the largest stable region.
func TestCacheBreakpoints_StringSystemGetsBreakpoint(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: strings.Repeat("nucleus ", 200),
		Messages:     []ProviderMessage{{Role: "user", Content: "hi"}},
	}
	payload := buildAnthropicRequest("claude-sonnet-4-6", req, false, 100)
	blocks, ok := payload.System.([]anthropicSystemBlock)
	if !ok || len(blocks) != 1 || blocks[0].CacheControl == nil || !strings.HasPrefix(blocks[0].Text, "nucleus") {
		t.Fatalf("API-key string system must become one marked text block; got %T %+v", payload.System, payload.System)
	}
	if _, isStr := payload.System.(string); isStr {
		t.Fatal("system left as a string — marker cannot attach")
	}
	// empty system stays absent (omitempty), never an empty marked block
	empty := buildAnthropicRequest("claude-sonnet-4-6", &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}}, false, 100)
	if b, _ := json.Marshal(empty); strings.Contains(string(b), `"system"`) && strings.Contains(string(b), `"text":""`) {
		t.Fatal("empty system must not become an empty marked block")
	}
}

// ── OAuth single-turn relocated-prefix breakpoint (K2) ──────────────────────────

// oauthSingleTurnWire mirrors the OAuth Complete/Stream wire steps for a
// single-turn request: relocate system text into the first user turn, then the
// second normalize pass (which I2-merges it with the user's text), then breakpoints.
func oauthSingleTurnWire(t *testing.T, relocated, userText string) *anthropicRequest {
	t.Helper()
	r := cacheReq(anthropicMessage{Role: "user", Content: userText})
	prependOAuthSystemToUserTurn(r, relocated)
	repaired, _ := normalizeAnthropicMessages(r.Messages)
	r.Messages = repaired
	applyAnthropicCacheBreakpoints(r)
	applyOAuthRelocatedCacheBreakpoint(r, relocated)
	return r
}

func TestOAuthRelocated_SingleTurnMarksRelocatedBlockOnly(t *testing.T) {
	t.Parallel()
	const relocated = "<!-- context id=\"c1\" zone=core salience=0.90 -->\nstable nucleus text"
	r := oauthSingleTurnWire(t, relocated, "what is up")

	if len(r.Messages) != 1 {
		t.Fatalf("single-turn wire must be one message after I2 merge; got %d", len(r.Messages))
	}
	blocks, ok := r.Messages[0].Content.([]anthropicContentBlock)
	if !ok || len(blocks) != 2 {
		t.Fatalf("expected relocated + user text as two blocks; got %+v", r.Messages[0].Content)
	}
	if blocks[0].Text != relocated || blocks[0].CacheControl == nil {
		t.Fatalf("relocated block must be block[0] with cache_control; got %+v", blocks[0])
	}
	if blocks[1].CacheControl != nil {
		t.Fatal("volatile user text must NOT carry cache_control")
	}
	// system (last block) + tools (last def) + relocated block = exactly 3.
	if n := countBreakpoints(t, r); n != 3 {
		t.Fatalf("breakpoints = %d; want 3 (system, tools, relocated)", n)
	}
}

func TestOAuthRelocated_SingleTurnStringContentBecomesMarkedBlock(t *testing.T) {
	t.Parallel()
	// No user text at all: the relocated message stands alone with string content.
	const relocated = "only relocated text"
	r := cacheReq(anthropicMessage{Role: "user", Content: relocated})
	applyOAuthRelocatedCacheBreakpoint(r, relocated)
	blocks, ok := r.Messages[0].Content.([]anthropicContentBlock)
	if !ok || len(blocks) != 1 || blocks[0].Text != relocated || blocks[0].CacheControl == nil {
		t.Fatalf("string relocated content must become one marked block; got %+v", r.Messages[0].Content)
	}
}

func TestOAuthRelocated_MultiMessageUnchanged(t *testing.T) {
	t.Parallel()
	const relocated = "relocated"
	build := func() *anthropicRequest {
		return cacheReq(
			anthropicMessage{Role: "user", Content: relocated + "\n\nfirst"},
			anthropicMessage{Role: "assistant", Content: "answer"},
			anthropicMessage{Role: "user", Content: "second"},
		)
	}
	base := build()
	applyAnthropicCacheBreakpoints(base)
	withNew := build()
	applyAnthropicCacheBreakpoints(withNew)
	applyOAuthRelocatedCacheBreakpoint(withNew, relocated)

	a, _ := json.Marshal(base)
	b, _ := json.Marshal(withNew)
	if string(a) != string(b) {
		t.Fatalf("3-message request must be byte-identical to today's breakpoints\n base: %s\n new:  %s", a, b)
	}
}

func TestOAuthRelocated_NoRelocatedTextIsNoop(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "hi"})
	applyOAuthRelocatedCacheBreakpoint(r, "")
	if n := countBreakpoints(t, r); n != 0 {
		t.Fatalf("empty relocated text must add no breakpoint; got %d", n)
	}
}

func TestOAuthRelocated_MismatchedFirstBlockIsNoop(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "unrelated user text"})
	applyOAuthRelocatedCacheBreakpoint(r, "some relocated text")
	if blocks := r.Messages[0].Content.(string); blocks != "unrelated user text" {
		t.Fatalf("non-matching single message must be untouched; got %q", blocks)
	}
	if n := countBreakpoints(t, r); n != 0 {
		t.Fatalf("non-matching single message must add no breakpoint; got %d", n)
	}
}

func TestOAuthRelocated_Idempotent(t *testing.T) {
	t.Parallel()
	const relocated = "relocated once"
	r := oauthSingleTurnWire(t, relocated, "hi")
	applyOAuthRelocatedCacheBreakpoint(r, relocated)
	if n := countBreakpoints(t, r); n != 3 {
		t.Fatalf("double application must not add breakpoints; got %d", n)
	}
}
