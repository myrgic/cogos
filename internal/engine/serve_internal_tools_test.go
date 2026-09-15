// serve_internal_tools_test.go — coverage for myrgic/cogos#94 and #95:
// when a kernel-agent chat request lands and the provider emits a
// tool_use event for an MCP-internal tool (cog_*, mod3_*, ...), the
// kernel must execute the tool in-process, append a tool_result message,
// re-call the provider, and surface the final assistant response — not
// forward the call to a dashboard that has no executor for it. Both the
// non-streaming (#94) and streaming (#95) paths share this guarantee.
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptedToolUseProvider is a Provider whose Complete() returns a
// preconfigured sequence of CompletionResponses. Each call advances the
// cursor; the final scripted entry repeats on overflow so a runaway tool
// loop fails loudly rather than panicking in the test harness.
type scriptedToolUseProvider struct {
	name     string
	mu       sync.Mutex
	cursor   int
	scripted []*CompletionResponse
	requests []*CompletionRequest // captured for assertions
}

func newScriptedToolUseProvider(name string, scripted ...*CompletionResponse) *scriptedToolUseProvider {
	return &scriptedToolUseProvider{name: name, scripted: scripted}
}

func (p *scriptedToolUseProvider) Name() string                     { return p.name }
func (p *scriptedToolUseProvider) Model() string                    { return "scripted" }
func (p *scriptedToolUseProvider) Available(_ context.Context) bool { return true }
func (p *scriptedToolUseProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities:     []Capability{CapToolUse},
		MaxContextTokens: 64_000,
		MaxOutputTokens:  4096,
		IsLocal:          true,
	}
}
func (p *scriptedToolUseProvider) Ping(_ context.Context) (time.Duration, error) { return 0, nil }

func (p *scriptedToolUseProvider) Complete(_ context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Deep-copy the messages slice so later mutations by the chat handler
	// (appending tool_result) don't retroactively change what the test
	// observes for earlier turns.
	msgsCopy := make([]ProviderMessage, len(req.Messages))
	copy(msgsCopy, req.Messages)
	captured := *req
	captured.Messages = msgsCopy
	p.requests = append(p.requests, &captured)

	idx := p.cursor
	if idx >= len(p.scripted) {
		idx = len(p.scripted) - 1
	}
	p.cursor++
	resp := *p.scripted[idx]
	return &resp, nil
}

func (p *scriptedToolUseProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	// Streaming path isn't exercised by this test; return a closed channel
	// so the surface stays compatible.
	ch := make(chan StreamChunk)
	close(ch)
	return ch, nil
}

// TestServerSideExecutionOfInternalCogTool is the primary regression test
// for #94. It scripts a provider that:
//
//  1. First turn: emits a tool_use event for cog_read_cogdoc against a
//     fixture cogdoc the kernel can resolve.
//  2. Second turn: emits a final assistant message that echoes a marker
//     string the test can pin against.
//
// The kernel must execute the tool itself (no client passthrough), append
// the tool_result to the conversation, re-call the provider, and return
// the final assistant message. Acceptance evidence:
//
//   - exactly two provider calls were made (tool_use → final).
//   - the second call's request contains a role=tool message whose content
//     contains the fixture's body (proving CallTool actually ran).
//   - the HTTP response carries the final assistant marker.
func TestServerSideExecutionOfInternalCogTool(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t)
	if srv.mcpServer == nil {
		t.Fatal("test server has no mcpServer wired; #94 path cannot run")
	}
	if !srv.mcpServer.IsInternalTool("cog_read_cogdoc") {
		t.Fatal("mcpServer snapshot missing cog_read_cogdoc; the auto-injection path is broken")
	}

	// Stage a fixture CogDoc the model will "read". The marker string is
	// distinctive so we can prove it round-tripped through the tool result
	// into the conversation and out via the final assistant message.
	const fixtureMarker = "FIXTURE-MARKER-94-3F2A"
	fixtureRel := "semantic/insights/issue-94-fixture.md"
	fixtureAbs := filepath.Join(srv.cfg.WorkspaceRoot, ".cog", "mem", fixtureRel)
	if err := os.MkdirAll(filepath.Dir(fixtureAbs), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	fixtureBody := "---\ntitle: issue 94 fixture\n---\n\n" + fixtureMarker + "\n"
	if err := os.WriteFile(fixtureAbs, []byte(fixtureBody), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	args := map[string]any{"uri": "cog://mem/" + fixtureRel}
	argsRaw, _ := json.Marshal(args)

	scripted := []*CompletionResponse{
		{
			Content:    "",
			StopReason: "tool_use",
			ToolCalls: []ToolCall{{
				ID:        "call_1",
				Name:      "cog_read_cogdoc",
				Arguments: string(argsRaw),
			}},
			ProviderMeta: ProviderMeta{Provider: "scripted", Model: "scripted"},
		},
		{
			Content:      "I read the fixture and saw " + fixtureMarker + ".",
			StopReason:   "end_turn",
			ProviderMeta: ProviderMeta{Provider: "scripted", Model: "scripted"},
		},
	}

	prov := newScriptedToolUseProvider("scripted", scripted...)
	router := NewSimpleRouter(RoutingConfig{Default: "scripted"})
	router.RegisterProvider(prov)
	srv.SetRouter(router)

	body := `{"model":"kernel-agent","messages":[{"role":"user","content":"please read the fixture"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}

	// 1. Two provider calls: turn-1 (tool_use) and turn-2 (final assistant).
	if got := len(prov.requests); got != 2 {
		t.Fatalf("provider was called %d time(s); want 2 (tool_use + final)", got)
	}

	// 2. The second request must include a role=tool message carrying the
	//    fixture's body — proving the kernel executed the tool itself
	//    rather than forwarding to a (nonexistent) client executor.
	turn2 := prov.requests[1]
	var toolMsg *ProviderMessage
	for i := range turn2.Messages {
		if turn2.Messages[i].Role == "tool" && turn2.Messages[i].ToolCallID == "call_1" {
			toolMsg = &turn2.Messages[i]
			break
		}
	}
	if toolMsg == nil {
		roles := make([]string, 0, len(turn2.Messages))
		for _, m := range turn2.Messages {
			roles = append(roles, m.Role)
		}
		t.Fatalf("turn-2 messages missing role=tool entry for call_1; roles=%v", roles)
	}
	if !strings.Contains(toolMsg.Content, fixtureMarker) {
		t.Errorf("tool_result content missing fixture marker %q; got %q",
			fixtureMarker, toolMsg.Content)
	}

	// 3. The HTTP response surfaces the final assistant message — the user
	//    sees a real reply instead of the silent turn-end #94 documents.
	var resp oaiChatResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Choices) == 0 || resp.Choices[0].Message == nil {
		t.Fatalf("response missing assistant choice; raw=%s", w.Body.String())
	}
	final := extractContent(resp.Choices[0].Message.Content)
	if !strings.Contains(final, fixtureMarker) {
		t.Errorf("final assistant content missing fixture marker %q; got %q",
			fixtureMarker, final)
	}
	if resp.Choices[0].FinishReason == nil || *resp.Choices[0].FinishReason != "stop" {
		got := "<nil>"
		if resp.Choices[0].FinishReason != nil {
			got = *resp.Choices[0].FinishReason
		}
		t.Errorf("finish_reason = %q; want stop (tool_calls would mean we forwarded the cog_* call)", got)
	}
}

// scriptedStreamUseProvider is a Provider whose Stream() returns a
// preconfigured sequence of StreamChunk slices, one per Stream() call.
// Each call advances the cursor, so a two-element script lets the test
// simulate "tool_use turn → final assistant turn" — the exact transcript
// the streaming chat handler must collapse server-side per #95.
//
// Complete() is intentionally unimplemented in the script; a streaming
// chat request never goes through Complete(), and routing the test
// through Stream() proves the streaming path executes the loop. The few
// Provider surface methods we don't care about delegate to sane defaults.
type scriptedStreamUseProvider struct {
	name     string
	mu       sync.Mutex
	cursor   int
	scripts  [][]StreamChunk
	requests []*CompletionRequest
}

func newScriptedStreamUseProvider(name string, scripts ...[]StreamChunk) *scriptedStreamUseProvider {
	return &scriptedStreamUseProvider{name: name, scripts: scripts}
}

func (p *scriptedStreamUseProvider) Name() string                     { return p.name }
func (p *scriptedStreamUseProvider) Model() string                    { return "scripted-stream" }
func (p *scriptedStreamUseProvider) Available(_ context.Context) bool { return true }
func (p *scriptedStreamUseProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities:     []Capability{CapStreaming, CapToolUse},
		MaxContextTokens: 64_000,
		MaxOutputTokens:  4096,
		IsLocal:          true,
	}
}
func (p *scriptedStreamUseProvider) Ping(_ context.Context) (time.Duration, error) {
	return 0, nil
}

func (p *scriptedStreamUseProvider) Complete(_ context.Context, _ *CompletionRequest) (*CompletionResponse, error) {
	// streamChat never calls Complete(); failing loud here would surface a
	// regression where someone wired the streaming path back through
	// Complete().
	return &CompletionResponse{StopReason: "end_turn"}, nil
}

func (p *scriptedStreamUseProvider) Stream(_ context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Capture the request as the chat handler sees it, with a defensive
	// copy of Messages so later in-place mutation by the handler doesn't
	// retroactively change earlier turns the test inspects.
	msgsCopy := append([]ProviderMessage(nil), req.Messages...)
	captured := *req
	captured.Messages = msgsCopy
	p.requests = append(p.requests, &captured)

	idx := p.cursor
	if idx >= len(p.scripts) {
		idx = len(p.scripts) - 1
	}
	p.cursor++
	chunks := p.scripts[idx]

	ch := make(chan StreamChunk, len(chunks))
	for _, c := range chunks {
		ch <- c
	}
	close(ch)
	return ch, nil
}

// TestServerSideExecutionOfInternalCogToolStreaming is the #95 twin of
// TestServerSideExecutionOfInternalCogTool: same fixture, same assertion
// shape, but exercised through streamChat with stream:true. The provider
// scripts two streams:
//
//  1. First Stream() call: tool_use for cog_read_cogdoc → Done(tool_use).
//  2. Second Stream() call: text deltas with the fixture marker → Done.
//
// Acceptance evidence:
//
//   - exactly two Stream() calls were made.
//   - the second call's request contains a role=tool message carrying the
//     fixture's body — proves the kernel executed the call server-side.
//   - the SSE response assembles to text containing the fixture marker.
//   - the final SSE chunk reports finish_reason == "stop", not
//     "tool_calls" (the silent-end failure mode #95 fixes).
//   - no SSE delta forwarded the cog_read_cogdoc call to the client.
func TestServerSideExecutionOfInternalCogToolStreaming(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t)
	if srv.mcpServer == nil {
		t.Fatal("test server has no mcpServer wired; #95 path cannot run")
	}
	if !srv.mcpServer.IsInternalTool("cog_read_cogdoc") {
		t.Fatal("mcpServer snapshot missing cog_read_cogdoc; the auto-injection path is broken")
	}

	const fixtureMarker = "FIXTURE-MARKER-95-7B19"
	fixtureRel := "semantic/insights/issue-95-fixture.md"
	fixtureAbs := filepath.Join(srv.cfg.WorkspaceRoot, ".cog", "mem", fixtureRel)
	if err := os.MkdirAll(filepath.Dir(fixtureAbs), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	fixtureBody := "---\ntitle: issue 95 fixture\n---\n\n" + fixtureMarker + "\n"
	if err := os.WriteFile(fixtureAbs, []byte(fixtureBody), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	args := map[string]any{"uri": "cog://mem/" + fixtureRel}
	argsRaw, _ := json.Marshal(args)

	// Turn 1: provider streams a single tool_use delta then Done(tool_use).
	turn1 := []StreamChunk{
		{
			ToolCallDelta: &ToolCallDelta{
				Index:     0,
				ID:        "call_stream_1",
				Name:      "cog_read_cogdoc",
				ArgsDelta: string(argsRaw),
			},
		},
		{
			Done:       true,
			StopReason: "tool_use",
			Usage:      &TokenUsage{InputTokens: 10, OutputTokens: 5},
		},
	}

	// Turn 2: provider streams the final assistant message in two text
	// deltas (so we can assert deltas are replayed in order) then Done.
	finalText1 := "I read the fixture and saw "
	finalText2 := fixtureMarker + "."
	turn2 := []StreamChunk{
		{Delta: finalText1},
		{Delta: finalText2},
		{
			Done:       true,
			StopReason: "end_turn",
			Usage:      &TokenUsage{InputTokens: 30, OutputTokens: 15},
		},
	}

	prov := newScriptedStreamUseProvider("scripted-stream", turn1, turn2)
	router := NewSimpleRouter(RoutingConfig{Default: "scripted-stream"})
	router.RegisterProvider(prov)
	srv.SetRouter(router)

	body := `{"model":"kernel-agent","messages":[{"role":"user","content":"please read the fixture"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q; want text/event-stream", ct)
	}

	// 1. Exactly two Stream() calls — the kernel ran a second pass after
	//    executing cog_read_cogdoc itself.
	if got := len(prov.requests); got != 2 {
		t.Fatalf("provider Stream() was called %d time(s); want 2 (tool_use + final)", got)
	}

	// 2. The second request must carry a role=tool message for call_stream_1
	//    with the fixture marker — proves CallTool actually ran.
	turn2Req := prov.requests[1]
	var toolMsg *ProviderMessage
	for i := range turn2Req.Messages {
		if turn2Req.Messages[i].Role == "tool" && turn2Req.Messages[i].ToolCallID == "call_stream_1" {
			toolMsg = &turn2Req.Messages[i]
			break
		}
	}
	if toolMsg == nil {
		roles := make([]string, 0, len(turn2Req.Messages))
		for _, m := range turn2Req.Messages {
			roles = append(roles, m.Role)
		}
		t.Fatalf("turn-2 messages missing role=tool entry for call_stream_1; roles=%v", roles)
	}
	if !strings.Contains(toolMsg.Content, fixtureMarker) {
		t.Errorf("tool_result content missing fixture marker %q; got %q",
			fixtureMarker, toolMsg.Content)
	}

	// 3. Parse the SSE response. Reconstruct the assembled assistant text,
	//    capture the final finish_reason, and assert no cog_* tool_calls
	//    delta leaked to the client.
	var assembled strings.Builder
	var finalFinishReason string
	sawCogToolCallDelta := false
	scanner := bufio.NewScanner(w.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var chunk oaiChatResponse
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("decode SSE chunk %q: %v", data, err)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]
		if choice.Delta != nil {
			if len(choice.Delta.Content) > 0 {
				assembled.WriteString(extractContent(choice.Delta.Content))
			}
			if len(choice.Delta.ToolCalls) > 0 &&
				strings.Contains(string(choice.Delta.ToolCalls), "cog_") {
				sawCogToolCallDelta = true
			}
		}
		if choice.FinishReason != nil {
			finalFinishReason = *choice.FinishReason
		}
	}

	wantAssembled := finalText1 + finalText2
	if assembled.String() != wantAssembled {
		t.Errorf("assembled SSE content = %q; want %q", assembled.String(), wantAssembled)
	}
	if !strings.Contains(assembled.String(), fixtureMarker) {
		t.Errorf("assembled SSE content missing fixture marker %q; got %q",
			fixtureMarker, assembled.String())
	}
	if finalFinishReason != "stop" {
		t.Errorf("final finish_reason = %q; want stop (tool_calls would mean we forwarded the cog_* call)",
			finalFinishReason)
	}
	if sawCogToolCallDelta {
		t.Errorf("SSE response leaked a cog_* tool_calls delta to the client; #95 requires server-side execution")
	}
}

// TestSplitToolCallsByOwnership pins the partitioning helper #94 introduces
// against three cases: a kernel MCP tool (cog_read_cogdoc), a known
// client-only name (browser_click), and a nil MCPServer (chat path before
// the snapshot was wired — must fall through to client forwarding).
func TestSplitToolCallsByOwnership(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	if srv.mcpServer == nil {
		t.Fatal("test server has no mcpServer; partition test cannot run")
	}

	calls := []ToolCall{
		{ID: "1", Name: "cog_read_cogdoc", Arguments: "{}"},
		{ID: "2", Name: "browser_click", Arguments: "{}"},
	}
	internal, external := splitToolCallsByOwnership(calls, srv.mcpServer)
	if len(internal) != 1 || internal[0].Name != "cog_read_cogdoc" {
		t.Errorf("internal = %v; want [cog_read_cogdoc]", internal)
	}
	if len(external) != 1 || external[0].Name != "browser_click" {
		t.Errorf("external = %v; want [browser_click]", external)
	}

	// Nil MCPServer → everything is external (pre-snapshot fallback).
	internal2, external2 := splitToolCallsByOwnership(calls, nil)
	if len(internal2) != 0 {
		t.Errorf("nil MCPServer must produce no internal calls; got %v", internal2)
	}
	if len(external2) != 2 {
		t.Errorf("nil MCPServer must forward all calls externally; got %d", len(external2))
	}
}

// TestHandleChat_HopCapNeverLeaksKernelTool is the OpenAI-surface twin of
// TestHandleAnthropicMessagesNonStreaming_HopCapNeverLeaksKernelTool (#600
// review round 2: the fix landed on one surface and not its sibling). A
// provider re-emitting a kernel-owned call on every hop exhausts the cap;
// the response must be a 500 with no kernel tool name or cog:// URI in it.
func TestHandleChat_HopCapNeverLeaksKernelTool(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	if srv.mcpServer == nil || !srv.mcpServer.IsInternalTool("cog_read_cogdoc") {
		t.Fatal("test server lacks an mcpServer with cog_read_cogdoc")
	}
	prov := newScriptedToolUseProvider("scripted", &CompletionResponse{
		StopReason: "tool_use",
		ToolCalls: []ToolCall{{
			ID: "call_loop", Name: "cog_read_cogdoc",
			Arguments: `{"uri":"cog://mem/does-not-exist.md"}`,
		}},
		ProviderMeta: ProviderMeta{Provider: "scripted", Model: "scripted"},
	})
	router := NewSimpleRouter(RoutingConfig{Default: "scripted"})
	router.RegisterProvider(prov)
	srv.SetRouter(router)

	body := `{"model":"kernel-agent","messages":[{"role":"user","content":"go"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if strings.Contains(w.Body.String(), "cog_read_cogdoc") || strings.Contains(w.Body.String(), "cog://") {
		t.Fatalf("kernel-owned tool name/URI leaked to client on hop-cap exhaustion: %s", w.Body.String())
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500; body=%s", w.Code, w.Body.String())
	}
	if len(prov.requests) > 10 {
		t.Fatalf("provider called %d times; cap is not bounding the loop", len(prov.requests))
	}
}
