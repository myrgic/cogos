// provider_anthropic_test.go — AnthropicProvider unit tests
//
// All tests use httptest.NewServer to mock the Anthropic API.
// No real API calls are made; the ANTHROPIC_API_KEY env var is not required.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// newTestAnthropicProvider creates an AnthropicProvider pointed at the given
// test server URL with a fake API key so Available() returns true.
// The API key is set directly on the struct (package main access) to avoid
// t.Setenv, which would prevent t.Parallel() in the caller.
func newTestAnthropicProvider(t *testing.T, endpoint string) *AnthropicProvider {
	t.Helper()
	p := NewAnthropicProvider("anthropic", ProviderConfig{
		Endpoint:  endpoint,
		Model:     "claude-sonnet-4-20250514",
		MaxTokens: 8192,
		Timeout:   5,
	})
	p.apiKey = "test-key-123" // set directly; no env lookup needed in tests
	return p
}

// anthropicResponseBody returns a minimal non-streaming Anthropic response JSON.
func anthropicResponseBody(text string) anthropicResponse {
	return anthropicResponse{
		Type: "message",
		Content: []anthropicContent{
			{Type: "text", Text: text},
		},
		StopReason: "end_turn",
		Usage:      anthropicUsage{InputTokens: 10, OutputTokens: 5},
	}
}

// ── buildAnthropicRequest ─────────────────────────────────────────────────────

// systemText flattens the System field to its text regardless of wire form.
// Since the cache-breakpoint change, a non-empty system is always emitted as
// a []anthropicSystemBlock (so it can carry cache_control) on BOTH the API-key
// and OAuth paths; Anthropic accepts either form.
func systemText(t *testing.T, sys any) string {
	t.Helper()
	switch v := sys.(type) {
	case nil:
		return ""
	case string:
		return v
	case []anthropicSystemBlock:
		var b strings.Builder
		for _, blk := range v {
			b.WriteString(blk.Text)
		}
		return b.String()
	}
	t.Fatalf("unexpected System type %T", sys)
	return ""
}

func TestBuildAnthropicRequestSystemPrompt(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "You are helpful.",
		Messages: []ProviderMessage{
			{Role: "user", Content: "hello"},
		},
	}
	ar := buildAnthropicRequest("claude-sonnet-4-20250514", req, false, 8192)

	if ar.Model != "claude-sonnet-4-20250514" {
		t.Errorf("model = %q; want claude-sonnet-4-20250514", ar.Model)
	}
	if ar.MaxTokens != 8192 {
		t.Errorf("max_tokens = %d; want 8192", ar.MaxTokens)
	}
	if got := systemText(t, ar.System); got != "You are helpful." {
		t.Errorf("system = %q; want 'You are helpful.'", got)
	}
	if ar.Stream {
		t.Error("stream should be false")
	}
	if len(ar.Messages) != 1 {
		t.Fatalf("messages len = %d; want 1", len(ar.Messages))
	}
	if ar.Messages[0].Role != "user" || ar.Messages[0].Content != "hello" {
		t.Errorf("message = %+v; want {user, hello}", ar.Messages[0])
	}
}

func TestBuildAnthropicRequestNoSystemPrompt(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}
	ar := buildAnthropicRequest("m", req, true, 1024)
	if ar.System != "" {
		t.Errorf("system = %q; want empty when no prompt and no context", ar.System)
	}
	if !ar.Stream {
		t.Error("stream should be true")
	}
}

func TestBuildAnthropicRequestContextItems(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "Identity block.",
		Context: []ContextItem{
			{ID: "cog://mem/note", Zone: ZoneFoveal, Salience: 0.9, Content: "relevant note"},
			{ID: "cog://mem/bg", Zone: ZoneParafoveal, Salience: 0.4, Content: "background"},
		},
	}
	ar := buildAnthropicRequest("m", req, false, 1024)

	// Both paths now emit System as a block array so the last block can carry
	// a prompt-cache breakpoint (anthropic_cache.go).
	if _, ok := ar.System.([]anthropicSystemBlock); !ok {
		t.Fatalf("System should be a block array (cache breakpoint carrier); got %T", ar.System)
	}
	sys := systemText(t, ar.System)
	// System should contain both context items and the system prompt.
	if !strings.Contains(sys, "relevant note") {
		t.Error("system should contain context item content")
	}
	if !strings.Contains(sys, "background") {
		t.Error("system should contain background context item")
	}
	if !strings.Contains(sys, "Identity block.") {
		t.Error("system should contain SystemPrompt after context items")
	}
	// Context items appear before the identity block.
	contextIdx := strings.Index(sys, "relevant note")
	identityIdx := strings.Index(sys, "Identity block.")
	if contextIdx > identityIdx {
		t.Error("context items should be prepended before SystemPrompt")
	}
}

func TestBuildAnthropicRequestEmptyContextItemsSkipped(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "Base.",
		Context: []ContextItem{
			{ID: "empty", Zone: ZoneFoveal, Salience: 0.9, Content: ""},
		},
	}
	ar := buildAnthropicRequest("m", req, false, 1024)
	// Empty context item should not pollute the system text.
	if got := systemText(t, ar.System); got != "Base." {
		t.Errorf("system = %q; want 'Base.' (empty context item skipped)", got)
	}
}

func TestBuildAnthropicRequestTools(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "search"}},
		Tools: []ToolDefinition{
			{
				Name:        "web_search",
				Description: "Search the web",
				InputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"query": map[string]interface{}{"type": "string"}}},
			},
		},
		ToolChoice: "auto",
	}
	ar := buildAnthropicRequest("m", req, false, 1024)

	if len(ar.Tools) != 1 {
		t.Fatalf("tools len = %d; want 1", len(ar.Tools))
	}
	if ar.Tools[0].Name != "web_search" {
		t.Errorf("tool name = %q; want web_search", ar.Tools[0].Name)
	}
	if ar.ToolChoice == nil || ar.ToolChoice.Type != "auto" {
		t.Errorf("tool_choice = %+v; want {type: auto}", ar.ToolChoice)
	}
}

func TestBuildAnthropicRequestNoTools(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}}
	ar := buildAnthropicRequest("m", req, false, 1024)
	if ar.Tools != nil {
		t.Error("tools should be nil when no tools provided")
	}
	if ar.ToolChoice != nil {
		t.Error("tool_choice should be nil when no ToolChoice set")
	}
}

// ── mapAnthropicToolChoice ────────────────────────────────────────────────────

func TestMapAnthropicToolChoice(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input    string
		wantType string
		wantName string
	}{
		{"auto", "auto", ""},
		{"none", "none", ""},
		{"required", "any", ""},
		{"my_tool", "tool", "my_tool"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := mapAnthropicToolChoice(tc.input)
			if got == nil {
				t.Fatal("mapAnthropicToolChoice returned nil")
			}
			if got.Type != tc.wantType {
				t.Errorf("Type = %q; want %q", got.Type, tc.wantType)
			}
			if got.Name != tc.wantName {
				t.Errorf("Name = %q; want %q", got.Name, tc.wantName)
			}
		})
	}
}

// ── Capabilities ──────────────────────────────────────────────────────────────

func TestAnthropicCapabilities(t *testing.T) {
	t.Parallel()
	p := NewAnthropicProvider("anthropic", ProviderConfig{Model: "claude-sonnet-4-20250514", MaxTokens: 8192})
	caps := p.Capabilities()

	if caps.IsLocal {
		t.Error("IsLocal should be false for Anthropic (cloud provider)")
	}
	for _, cap := range []Capability{CapStreaming, CapToolUse, CapVision, CapLongContext, CapJSON, CapCaching} {
		if !caps.HasCapability(cap) {
			t.Errorf("missing capability: %s", cap)
		}
	}
	if caps.MaxContextTokens != 200_000 {
		t.Errorf("MaxContextTokens = %d; want 200000", caps.MaxContextTokens)
	}
	if caps.MaxOutputTokens != 8192 {
		t.Errorf("MaxOutputTokens = %d; want 8192", caps.MaxOutputTokens)
	}
	if caps.CostPerInputToken <= 0 {
		t.Error("CostPerInputToken should be > 0")
	}
	if caps.CostPerOutputToken <= 0 {
		t.Error("CostPerOutputToken should be > 0")
	}
}

func TestAnthropicCapabilitiesDefaultMaxTokens(t *testing.T) {
	t.Parallel()
	p := NewAnthropicProvider("anthropic", ProviderConfig{Model: "m"}) // MaxTokens unset
	caps := p.Capabilities()
	if caps.MaxOutputTokens != anthropicDefaultMaxToks {
		t.Errorf("MaxOutputTokens = %d; want %d (default)", caps.MaxOutputTokens, anthropicDefaultMaxToks)
	}
}

// ── Available ─────────────────────────────────────────────────────────────────

func TestAnthropicAvailableWithKey(t *testing.T) {
	// Not parallel: uses t.Setenv which is incompatible with t.Parallel.
	t.Setenv("ANTHROPIC_KEY_SET", "sk-ant-test")
	p := NewAnthropicProvider("anthropic", ProviderConfig{
		Model:     "m",
		APIKeyEnv: "ANTHROPIC_KEY_SET",
	})
	if !p.Available(context.Background()) {
		t.Error("Available() = false; want true when API key is set")
	}
}

func TestAnthropicAvailableWithoutKey(t *testing.T) {
	// Not parallel: uses os.Unsetenv which affects the shared environment.
	os.Unsetenv("ANTHROPIC_KEY_DEFINITELY_NOT_SET")
	p := NewAnthropicProvider("anthropic", ProviderConfig{
		Model:     "m",
		APIKeyEnv: "ANTHROPIC_KEY_DEFINITELY_NOT_SET",
	})
	if p.Available(context.Background()) {
		t.Error("Available() = true; want false when API key env var is missing")
	}
}

func TestAnthropicAvailableNoAPIKeyEnvConfigured(t *testing.T) {
	t.Parallel()
	p := NewAnthropicProvider("anthropic", ProviderConfig{Model: "m"}) // APIKeyEnv not set
	if p.Available(context.Background()) {
		t.Error("Available() = true; want false when no APIKeyEnv configured")
	}
}

// ── Ping ──────────────────────────────────────────────────────────────────────

func TestAnthropicPing(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("x-api-key") == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	lat, err := p.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if lat <= 0 {
		t.Errorf("latency = %v; want > 0", lat)
	}
}

func TestAnthropicPingUnauthorized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	_, err := p.Ping(context.Background())
	if err == nil {
		t.Error("Ping should return error for 401 response")
	}
}

// ── Complete ──────────────────────────────────────────────────────────────────

func TestAnthropicComplete(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("x-api-key") == "" {
			http.Error(w, "no key", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("anthropic-version") != anthropicAPIVersion {
			http.Error(w, "bad version", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("Hello there!"))
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Content != "Hello there!" {
		t.Errorf("Content = %q; want Hello there!", resp.Content)
	}
	if resp.Usage.InputTokens != 10 || resp.Usage.OutputTokens != 5 {
		t.Errorf("Usage = %+v; want {10, 5}", resp.Usage)
	}
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn", resp.StopReason)
	}
	if resp.ProviderMeta.Provider != "anthropic" {
		t.Errorf("ProviderMeta.Provider = %q; want anthropic", resp.ProviderMeta.Provider)
	}
}

func TestAnthropicCompleteHTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"type":"invalid_request_error"}}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	_, err := p.Complete(context.Background(), &CompletionRequest{})
	if err == nil {
		t.Error("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status code, got: %v", err)
	}
}

func TestAnthropicCompleteToolUseResponse(t *testing.T) {
	t.Parallel()
	toolInput := json.RawMessage(`{"query":"golang testing"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(anthropicResponse{
			Type: "message",
			Content: []anthropicContent{
				{Type: "tool_use", ID: "tu_abc", Name: "web_search", Input: toolInput},
			},
			StopReason: "tool_use",
			Usage:      anthropicUsage{InputTokens: 20, OutputTokens: 15},
		})
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "search for me"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("ToolCalls len = %d; want 1", len(resp.ToolCalls))
	}
	tc := resp.ToolCalls[0]
	if tc.ID != "tu_abc" {
		t.Errorf("ToolCall.ID = %q; want tu_abc", tc.ID)
	}
	if tc.Name != "web_search" {
		t.Errorf("ToolCall.Name = %q; want web_search", tc.Name)
	}
	if tc.Arguments != `{"query":"golang testing"}` {
		t.Errorf("ToolCall.Arguments = %q; want json", tc.Arguments)
	}
	if resp.StopReason != "tool_use" {
		t.Errorf("StopReason = %q; want tool_use", resp.StopReason)
	}
}

func TestAnthropicCompleteMixedContent(t *testing.T) {
	t.Parallel()
	// Response with both text and a tool call.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(anthropicResponse{
			Type: "message",
			Content: []anthropicContent{
				{Type: "text", Text: "Let me search for that."},
				{Type: "tool_use", ID: "tu_1", Name: "search", Input: json.RawMessage(`{}`)},
			},
			StopReason: "tool_use",
			Usage:      anthropicUsage{InputTokens: 8, OutputTokens: 12},
		})
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Content != "Let me search for that." {
		t.Errorf("Content = %q; want 'Let me search for that.'", resp.Content)
	}
	if len(resp.ToolCalls) != 1 {
		t.Errorf("ToolCalls len = %d; want 1", len(resp.ToolCalls))
	}
}

// ── parseAnthropicResponse ────────────────────────────────────────────────────

func TestParseAnthropicResponseMissingStopReason(t *testing.T) {
	t.Parallel()
	ar := &anthropicResponse{
		Content:    []anthropicContent{{Type: "text", Text: "hi"}},
		StopReason: "", // omitted
	}
	resp := parseAnthropicResponse(ar, "m", "anthropic", 0)
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn (default)", resp.StopReason)
	}
}

func TestParseAnthropicResponseNilToolInput(t *testing.T) {
	t.Parallel()
	ar := &anthropicResponse{
		Content: []anthropicContent{
			{Type: "tool_use", ID: "x", Name: "fn", Input: nil},
		},
		StopReason: "tool_use",
	}
	resp := parseAnthropicResponse(ar, "m", "anthropic", 0)
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("ToolCalls len = %d; want 1", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].Arguments != "{}" {
		t.Errorf("Arguments = %q; want {} for nil input", resp.ToolCalls[0].Arguments)
	}
}

// ── Stream ────────────────────────────────────────────────────────────────────

// sseLines formats SSE data lines for use in a test server response.
func sseLines(events []anthropicSSEEvent) string {
	var sb strings.Builder
	for _, e := range events {
		b, _ := json.Marshal(e)
		fmt.Fprintf(&sb, "data: %s\n\n", b)
	}
	return sb.String()
}

func TestAnthropicStream(t *testing.T) {
	t.Parallel()

	events := []anthropicSSEEvent{
		{Type: "message_start", Message: &anthropicSSEMsg{Usage: anthropicUsage{InputTokens: 7}}},
		{Type: "content_block_start", Index: 0, ContentBlock: &anthropicContent{Type: "text"}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{Type: "text_delta", Text: "Hello"}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{Type: "text_delta", Text: " world"}},
		{Type: "content_block_stop", Index: 0},
		{Type: "message_delta", Delta: &anthropicSSEDelta{StopReason: "end_turn"}, Usage: &anthropicSSEUsage{OutputTokens: 4}},
		{Type: "message_stop"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseLines(events))
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var content strings.Builder
	var lastChunk StreamChunk
	for sc := range ch {
		if sc.Error != nil {
			t.Fatalf("stream error: %v", sc.Error)
		}
		if sc.Delta != "" {
			content.WriteString(sc.Delta)
		}
		lastChunk = sc
	}

	if content.String() != "Hello world" {
		t.Errorf("streamed content = %q; want 'Hello world'", content.String())
	}
	if !lastChunk.Done {
		t.Error("last chunk should have Done=true")
	}
	if lastChunk.Usage == nil {
		t.Fatal("last chunk usage should not be nil")
	}
	if lastChunk.Usage.InputTokens != 7 {
		t.Errorf("InputTokens = %d; want 7", lastChunk.Usage.InputTokens)
	}
	if lastChunk.Usage.OutputTokens != 4 {
		t.Errorf("OutputTokens = %d; want 4", lastChunk.Usage.OutputTokens)
	}
	if lastChunk.ProviderMeta == nil || lastChunk.ProviderMeta.Provider != "anthropic" {
		t.Errorf("ProviderMeta.Provider = %v; want anthropic", lastChunk.ProviderMeta)
	}
}

func TestAnthropicStreamToolCall(t *testing.T) {
	t.Parallel()

	// SSE stream for a tool_use response.
	events := []anthropicSSEEvent{
		{Type: "message_start", Message: &anthropicSSEMsg{Usage: anthropicUsage{InputTokens: 12}}},
		{Type: "content_block_start", Index: 0, ContentBlock: &anthropicContent{
			Type: "tool_use", ID: "tu_xyz", Name: "search",
		}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{
			Type: "input_json_delta", PartialJSON: `{"q`,
		}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{
			Type: "input_json_delta", PartialJSON: `uery":"test"}`,
		}},
		{Type: "content_block_stop", Index: 0},
		{Type: "message_delta", Delta: &anthropicSSEDelta{StopReason: "tool_use"}, Usage: &anthropicSSEUsage{OutputTokens: 8}},
		{Type: "message_stop"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseLines(events))
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "search"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var toolStartChunk, argChunk1, argChunk2, doneChunk StreamChunk
	var allChunks []StreamChunk
	for sc := range ch {
		if sc.Error != nil {
			t.Fatalf("stream error: %v", sc.Error)
		}
		allChunks = append(allChunks, sc)
	}

	// Find the tool start chunk (has ID and Name).
	for _, sc := range allChunks {
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ID == "tu_xyz" {
			toolStartChunk = sc
		}
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ArgsDelta == `{"q` {
			argChunk1 = sc
		}
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ArgsDelta == `uery":"test"}` {
			argChunk2 = sc
		}
		if sc.Done {
			doneChunk = sc
		}
	}

	if toolStartChunk.ToolCallDelta == nil {
		t.Error("expected a ToolCallDelta chunk with tool ID and name")
	} else {
		if toolStartChunk.ToolCallDelta.Name != "search" {
			t.Errorf("tool name = %q; want search", toolStartChunk.ToolCallDelta.Name)
		}
		if toolStartChunk.ToolCallDelta.Index != 0 {
			t.Errorf("tool index = %d; want 0", toolStartChunk.ToolCallDelta.Index)
		}
	}
	if argChunk1.ToolCallDelta == nil {
		t.Error("expected first arg delta chunk")
	}
	if argChunk2.ToolCallDelta == nil {
		t.Error("expected second arg delta chunk")
	}
	if !doneChunk.Done {
		t.Error("expected a Done chunk at end of stream")
	}
	if doneChunk.Usage == nil || doneChunk.Usage.OutputTokens != 8 {
		t.Errorf("done chunk usage = %+v; want OutputTokens=8", doneChunk.Usage)
	}
}

func TestAnthropicStreamMalformedEvent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {not valid json}\n\n")
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var gotError bool
	for sc := range ch {
		if sc.Error != nil {
			gotError = true
		}
	}
	if !gotError {
		t.Error("expected a StreamChunk with Error for malformed JSON event")
	}
}

func TestAnthropicStreamContextCancelled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// Send one chunk then block until client disconnects.
		flusher := w.(http.Flusher)
		e := anthropicSSEEvent{
			Type:  "content_block_delta",
			Index: 0,
			Delta: &anthropicSSEDelta{Type: "text_delta", Text: "hello"},
		}
		b, _ := json.Marshal(e)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Stream(ctx, &CompletionRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Read first chunk then cancel.
	<-ch
	cancel()

	// Channel should close cleanly without hanging.
	for range ch {
	}
}

func TestAnthropicStreamHTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"overloaded"}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	_, err := p.Stream(context.Background(), &CompletionRequest{})
	if err == nil {
		t.Error("expected error for 503 response before streaming begins")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should mention status code, got: %v", err)
	}
}

// ── effectiveModel ─────────────────────────────────────────────────────────────

func TestAnthropicEffectiveModel(t *testing.T) {
	t.Parallel()
	p := NewAnthropicProvider("anthropic", ProviderConfig{Model: "claude-sonnet-4-20250514"})

	// No override: use configured model.
	if got := p.effectiveModel(&CompletionRequest{}); got != "claude-sonnet-4-20250514" {
		t.Errorf("effectiveModel = %q; want claude-sonnet-4-20250514", got)
	}
	// With override: use the override.
	req := &CompletionRequest{ModelOverride: "claude-opus-4-20250514"}
	if got := p.effectiveModel(req); got != "claude-opus-4-20250514" {
		t.Errorf("effectiveModel = %q; want claude-opus-4-20250514", got)
	}
}

// ── Name ──────────────────────────────────────────────────────────────────────

func TestAnthropicName(t *testing.T) {
	t.Parallel()
	p := NewAnthropicProvider("my-anthropic", ProviderConfig{Model: "m"})
	if p.Name() != "my-anthropic" {
		t.Errorf("Name() = %q; want my-anthropic", p.Name())
	}
}

// TestBuildAnthropicRequest_TemperatureDeprecation: models that reject the
// temperature/top_p parameters (Opus 4.7+ and the Claude 5 family) must have
// them stripped at the wire builder — the agent harness sets a default
// temperature on every dispatch, and forwarding it 400s the whole request
// ("`temperature` is deprecated for this model", verified live 2026-07-17).
// Older models keep the caller's values.
func TestBuildAnthropicRequest_TemperatureDeprecation(t *testing.T) {
	t.Parallel()
	temp, topp := 0.7, 0.9
	req := &CompletionRequest{Temperature: &temp, TopP: &topp,
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}}}

	for _, model := range []string{
		"claude-opus-4-7", "claude-opus-4-8", "claude-sonnet-5",
		"claude-fable-5", "claude-sonnet-5-20260501",
		// Verified live 2026-10-07 through this kernel: claude-haiku-5-5 400s on
		// temperature=0.7, top_p=0.5 and temperature=0 ("temperature is
		// deprecated for this model"); Anthropic's docs say the same.
		"claude-haiku-5-5", "claude-haiku-5",
	} {
		ar := buildAnthropicRequest(model, req, false, 1024)
		if ar.Temperature != nil || ar.TopP != nil {
			t.Errorf("%s: temperature/top_p not stripped (temp=%v topp=%v)",
				model, ar.Temperature, ar.TopP)
		}
	}
	for _, model := range []string{
		"claude-sonnet-4-6", "claude-haiku-4-5-20251001",
	} {
		ar := buildAnthropicRequest(model, req, false, 1024)
		if ar.Temperature == nil || *ar.Temperature != temp {
			t.Errorf("%s: temperature wrongly stripped", model)
		}
	}
}
