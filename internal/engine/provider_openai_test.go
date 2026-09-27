// provider_openai_test.go — OpenAICompatProvider unit tests
//
// All tests use httptest.NewServer to mock OpenAI-compatible API responses.
// No real API calls are made.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// newTestOpenAIProvider creates an OpenAICompatProvider pointed at the given
// test server URL.
func newTestOpenAIProvider(t *testing.T, endpoint, model string) *OpenAICompatProvider {
	t.Helper()
	return NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Endpoint:  endpoint,
		Model:     model,
		MaxTokens: 4096,
		Timeout:   5,
	})
}

// openaiModelsResponseJSON returns a /v1/models response body.
func openaiModelsResponseJSON(ids ...string) openaiModelsResponse {
	var models []openaiModel
	for _, id := range ids {
		models = append(models, openaiModel{ID: id})
	}
	return openaiModelsResponse{Data: models}
}

// openaiChatResponseJSON returns a minimal non-streaming chat completion response.
func openaiChatResponseJSON(content, finishReason string) openaiChatResponse {
	return openaiChatResponse{
		ID: "chatcmpl-test",
		Choices: []openaiChoice{
			{
				Index:        0,
				Message:      openaiMessage{Role: "assistant", Content: content},
				FinishReason: finishReason,
			},
		},
		Usage: &openaiUsageResponse{
			PromptTokens:     10,
			CompletionTokens: 5,
			TotalTokens:      15,
		},
	}
}

// ── buildOpenAIRequest ───────────────────────────────────────────────────────

func TestBuildOpenAIRequestSystemPrompt(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "You are helpful.",
		Messages: []ProviderMessage{
			{Role: "user", Content: "hello"},
		},
	}
	r := buildOpenAIRequest("test-model", req, false, 4096)

	if r.Model != "test-model" {
		t.Errorf("model = %q; want test-model", r.Model)
	}
	if r.Stream {
		t.Error("Stream should be false for non-streaming request")
	}
	// System prompt + user message.
	if len(r.Messages) != 2 {
		t.Fatalf("messages len = %d; want 2", len(r.Messages))
	}
	if r.Messages[0].Role != "system" || r.Messages[0].Content != "You are helpful." {
		t.Errorf("first message = %+v; want system/helpful", r.Messages[0])
	}
	if r.Messages[1].Role != "user" || r.Messages[1].Content != "hello" {
		t.Errorf("second message = %+v; want user/hello", r.Messages[1])
	}
}

func TestBuildOpenAIRequestNoSystemPrompt(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}
	r := buildOpenAIRequest("m", req, true, 0)
	if len(r.Messages) != 1 {
		t.Errorf("messages len = %d; want 1 (no system prepended)", len(r.Messages))
	}
	if !r.Stream {
		t.Error("Stream should be true")
	}
}

// TestBuildOpenAIRequestStreamOptions guards inference-pipeline-robustness FIX 2:
// a streaming request must set stream_options.include_usage so the server emits a
// usage-bearing terminal chunk; a non-streaming request must not set it (usage is
// in the response body there). A missing include_usage is what left every
// cancel-safe streaming completion reporting usage 0/0.
func TestBuildOpenAIRequestStreamOptions(t *testing.T) {
	t.Parallel()
	streaming := buildOpenAIRequest("m", &CompletionRequest{}, true, 0)
	if streaming.StreamOptions == nil {
		t.Fatal("streaming request: StreamOptions is nil; want include_usage set")
	}
	if !streaming.StreamOptions.IncludeUsage {
		t.Error("streaming request: include_usage = false; want true")
	}
	// It must serialize into the wire body under the OpenAI key.
	b, err := json.Marshal(streaming)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"stream_options":{"include_usage":true}`) {
		t.Errorf("marshaled body missing stream_options: %s", b)
	}

	nonStreaming := buildOpenAIRequest("m", &CompletionRequest{}, false, 0)
	if nonStreaming.StreamOptions != nil {
		t.Error("non-streaming request: StreamOptions should be nil (usage is in the response body)")
	}
	nb, err := json.Marshal(nonStreaming)
	if err != nil {
		t.Fatalf("marshal non-streaming: %v", err)
	}
	if strings.Contains(string(nb), "stream_options") {
		t.Errorf("non-streaming body should omit stream_options: %s", nb)
	}
}

func TestBuildOpenAIRequestOptions(t *testing.T) {
	t.Parallel()
	temp := 0.7
	req := &CompletionRequest{
		Temperature: &temp,
		MaxTokens:   512,
	}
	r := buildOpenAIRequest("m", req, false, 4096)
	if r.Temperature == nil || *r.Temperature != 0.7 {
		t.Errorf("temperature = %v; want 0.7", r.Temperature)
	}
	if r.MaxTokens != 512 {
		t.Errorf("max_tokens = %v; want 512 (request override)", r.MaxTokens)
	}
}

func TestBuildOpenAIRequestContextItems(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		SystemPrompt: "Identity block.",
		Context: []ContextItem{
			{ID: "cog://mem/note", Zone: ZoneFoveal, Salience: 0.9, Content: "relevant note"},
		},
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}
	r := buildOpenAIRequest("m", req, false, 4096)

	// System prompt + context item + user message = 3.
	if len(r.Messages) != 3 {
		t.Fatalf("messages len = %d; want 3", len(r.Messages))
	}
	if r.Messages[0].Role != "system" {
		t.Errorf("first message role = %q; want system", r.Messages[0].Role)
	}
	if !strings.Contains(r.Messages[1].Content, "relevant note") {
		t.Error("context item content not found in messages")
	}
}

func TestBuildOpenAIRequestTools(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "search"}},
		Tools: []ToolDefinition{
			{
				Name:        "web_search",
				Description: "Search the web",
				InputSchema: map[string]interface{}{"type": "object"},
			},
		},
		ToolChoice: "auto",
	}
	r := buildOpenAIRequest("m", req, false, 4096)

	if len(r.Tools) != 1 {
		t.Fatalf("tools len = %d; want 1", len(r.Tools))
	}
	if r.Tools[0].Function.Name != "web_search" {
		t.Errorf("tool name = %q; want web_search", r.Tools[0].Function.Name)
	}
	if r.Tools[0].Type != "function" {
		t.Errorf("tool type = %q; want function", r.Tools[0].Type)
	}
	if r.ToolChoice != "auto" {
		t.Errorf("tool_choice = %v; want auto", r.ToolChoice)
	}
}

// TestBuildOpenAIRequestSuppressesToolsWhenNone verifies that the tools array
// is omitted from the wire body when tool_choice is "none", and that it is
// included for other values ("auto", "required", specific tool name).
func TestBuildOpenAIRequestSuppressesToolsWhenNone(t *testing.T) {
	t.Parallel()

	tools := []ToolDefinition{
		{
			Name:        "search",
			Description: "Search the memory index",
			InputSchema: map[string]interface{}{"type": "object"},
		},
	}
	msgs := []ProviderMessage{{Role: "user", Content: "hi"}}

	// tool_choice == "none" → tools field must be absent from the wire body.
	t.Run("none suppresses tools", func(t *testing.T) {
		t.Parallel()
		req := &CompletionRequest{Messages: msgs, Tools: tools, ToolChoice: "none"}
		r := buildOpenAIRequest("m", req, false, 4096)
		if len(r.Tools) != 0 {
			t.Errorf("tools len = %d; want 0 when tool_choice is none", len(r.Tools))
		}
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if _, present := raw["tools"]; present {
			t.Errorf("wire body contains 'tools' key; want omitted when tool_choice is none")
		}
	})

	// Other tool_choice values must still send the tools schema.
	for _, tc := range []string{"auto", "required", "search"} {
		tc := tc
		t.Run("includes tools for "+tc, func(t *testing.T) {
			t.Parallel()
			req := &CompletionRequest{Messages: msgs, Tools: tools, ToolChoice: tc}
			r := buildOpenAIRequest("m", req, false, 4096)
			if len(r.Tools) != 1 {
				t.Errorf("tools len = %d; want 1 for tool_choice=%q", len(r.Tools), tc)
			}
		})
	}

	// No tools in the request → tools field absent regardless of tool_choice.
	t.Run("no tools always omits tools field", func(t *testing.T) {
		t.Parallel()
		req := &CompletionRequest{Messages: msgs, ToolChoice: "auto"}
		r := buildOpenAIRequest("m", req, false, 4096)
		if len(r.Tools) != 0 {
			t.Errorf("tools len = %d; want 0 when no tools defined", len(r.Tools))
		}
	})
}

// ── parseOpenAIResponse ──────────────────────────────────────────────────────

func TestParseOpenAIResponseBasic(t *testing.T) {
	t.Parallel()
	or := &openaiChatResponse{
		Choices: []openaiChoice{
			{
				Message:      openaiMessage{Role: "assistant", Content: "Hello!"},
				FinishReason: "stop",
			},
		},
		Usage: &openaiUsageResponse{PromptTokens: 10, CompletionTokens: 3},
	}
	resp := parseOpenAIResponse(or, "model", "test", 0)
	if resp.Content != "Hello!" {
		t.Errorf("Content = %q; want Hello!", resp.Content)
	}
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn", resp.StopReason)
	}
	if resp.Usage.InputTokens != 10 || resp.Usage.OutputTokens != 3 {
		t.Errorf("Usage = %+v; want {10, 3}", resp.Usage)
	}
}

func TestParseOpenAIResponseToolCalls(t *testing.T) {
	t.Parallel()
	or := &openaiChatResponse{
		Choices: []openaiChoice{
			{
				Message: openaiMessage{
					Role: "assistant",
					ToolCalls: []openaiToolCall{
						{
							ID:   "call_abc",
							Type: "function",
							Function: openaiToolCallDetail{
								Name:      "search",
								Arguments: `{"query":"test"}`,
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
		Usage: &openaiUsageResponse{PromptTokens: 15, CompletionTokens: 8},
	}
	resp := parseOpenAIResponse(or, "model", "test", 0)
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("ToolCalls len = %d; want 1", len(resp.ToolCalls))
	}
	tc := resp.ToolCalls[0]
	if tc.ID != "call_abc" {
		t.Errorf("ToolCall.ID = %q; want call_abc", tc.ID)
	}
	if tc.Name != "search" {
		t.Errorf("ToolCall.Name = %q; want search", tc.Name)
	}
	if tc.Arguments != `{"query":"test"}` {
		t.Errorf("ToolCall.Arguments = %q; want json", tc.Arguments)
	}
	if resp.StopReason != "tool_use" {
		t.Errorf("StopReason = %q; want tool_use", resp.StopReason)
	}
}

func TestParseOpenAIResponseNoChoices(t *testing.T) {
	t.Parallel()
	or := &openaiChatResponse{Choices: nil}
	resp := parseOpenAIResponse(or, "model", "test", 0)
	if resp.Content != "" {
		t.Errorf("Content = %q; want empty", resp.Content)
	}
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn (default)", resp.StopReason)
	}
}

func TestParseOpenAIResponseNoUsage(t *testing.T) {
	t.Parallel()
	or := &openaiChatResponse{
		Choices: []openaiChoice{
			{Message: openaiMessage{Content: "hi"}, FinishReason: "stop"},
		},
		Usage: nil,
	}
	resp := parseOpenAIResponse(or, "model", "test", 0)
	if resp.Usage.InputTokens != 0 || resp.Usage.OutputTokens != 0 {
		t.Errorf("Usage should be zero when not provided, got %+v", resp.Usage)
	}
}

// ── mapOpenAIFinishReason ────────────────────────────────────────────────────

func TestMapOpenAIFinishReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"stop", "end_turn"},
		{"length", "max_tokens"},
		{"tool_calls", "tool_use"},
		{"", ""},
		{"unknown", "unknown"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := mapOpenAIFinishReason(tc.input)
			if got != tc.want {
				t.Errorf("mapOpenAIFinishReason(%q) = %q; want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ── Available ────────────────────────────────────────────────────────────────

func TestOpenAIAvailableModelPresent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("gemma-2-9b", "llama-3.1-8b"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "gemma-2-9b")
	if !p.Available(context.Background()) {
		t.Error("Available() = false; want true when model is present")
	}
}

func TestOpenAIAvailableModelAbsent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("llama-3.1-8b"))
	}))
	defer srv.Close()

	// Server has models, but not the configured one — should be unavailable.
	p := newTestOpenAIProvider(t, srv.URL, "nonexistent-model")
	if p.Available(context.Background()) {
		t.Error("Available() = true; want false when configured model is not loaded")
	}
}

func TestOpenAIAvailableNoModels(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON())
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "any")
	if p.Available(context.Background()) {
		t.Error("Available() = true; want false when server has no models")
	}
}

func TestOpenAIAvailableServerDown(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Endpoint: "http://localhost:1", // nothing listening
		Model:    "any",
		Timeout:  1,
	})
	if p.Available(context.Background()) {
		t.Error("Available() = true; want false when server is down")
	}
}

// TestOpenAIAvailableCachesWithinTTL is the regression guard for #441: repeated
// Available() calls within availCacheTTL must be served from the cache and hit
// the upstream /v1/models exactly once, not on every call.
func TestOpenAIAvailableCachesWithinTTL(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			hits.Add(1)
		}
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("gemma-2-9b"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "gemma-2-9b")

	// First call probes the upstream; the next 5 must be cache hits.
	for i := 0; i < 6; i++ {
		if !p.Available(context.Background()) {
			t.Fatalf("Available() = false on call %d; want true", i)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("upstream /v1/models hit %d times; want 1 (calls within TTL should be cached)", got)
	}
}

// TestOpenAIAvailableCancelledContextDoesNotPoisonCache guards the #441-review
// fix: a probe that fails only because the caller's context was cancelled must
// not be cached as a negative, or it would mark a healthy provider unavailable
// to every other caller for the whole TTL.
func TestOpenAIAvailableCancelledContextDoesNotPoisonCache(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("gemma-2-9b"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "gemma-2-9b")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.Available(cancelled) // probe fails on the cancelled ctx; must not cache false

	if !p.Available(context.Background()) {
		t.Error("Available() = false after a cancelled probe — the cancelled context poisoned the cache")
	}
}

// TestOpenAIAvailableCachesDeadlineExceededAsUnavailable guards the second
// #441-review round: a probe whose DEADLINE expires (the router's probeTimeout
// on a slow/hung provider) is a real "unavailable" signal and MUST be cached —
// unlike a caller cancellation. Otherwise a hung provider stays cached as
// available forever because every tick re-hits the deadline and discards it.
func TestOpenAIAvailableCachesDeadlineExceededAsUnavailable(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-release // hang past the caller's deadline (simulates a slow/hung server)
	}))
	defer srv.Close()
	defer close(release)

	p := newTestOpenAIProvider(t, srv.URL, "any")

	deadlined, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if p.Available(deadlined) {
		t.Fatal("Available() = true on a hung probe; want false")
	}
	// The negative must be cached (deadline != cancellation): the next call
	// within TTL must be served from cache without re-probing.
	if p.Available(context.Background()) {
		t.Error("Available() = true after a deadline-exceeded probe; want cached false")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("upstream hit %d times; want 1 (timeout result should be cached, not re-probed)", got)
	}
}

// TestOpenAIProbeBoundedByInternalTimeout is the regression guard for
// inference-pipeline-robustness FIX 1: probeAvailable must cap its own HTTP call
// at probeHTTPTimeout, independent of the (much larger) client timeout, so a
// hung-but-accepting upstream can't hold availMu for the full client timeout and
// block the router's probeAll goroutine / Route()'s inline fallback behind
// availMu.Lock().
//
// The provider's client timeout is set to an hour; against a server that accepts
// the connection and then hangs, an uncapped probe would block for that hour.
// The internal probeHTTPTimeout bound must make Available() return false in ~10s.
// A second concurrent caller (which blocks on availMu behind the first) must also
// be released within the same bound — proving the mutex hold is bounded too.
func TestOpenAIProbeBoundedByInternalTimeout(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // accept then hang, like a wedged LM Studio
	}))
	defer srv.Close()
	defer close(release)

	// Client timeout of an hour: only the internal probeHTTPTimeout can bound this.
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Endpoint: srv.URL,
		Model:    "any",
		Timeout:  3600,
	})

	// Two concurrent callers: the second must queue on availMu behind the first.
	type result struct {
		avail   bool
		elapsed time.Duration
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			start := time.Now()
			avail := p.Available(context.Background())
			results <- result{avail: avail, elapsed: time.Since(start)}
		}()
	}

	// Generous ceiling: probeHTTPTimeout (10s) for the first probe + the second
	// caller's turn under availMu, plus slack. Far below the 1h client timeout, so
	// a pass proves the internal cap — not the client timeout — bounded the hold.
	bound := 2*probeHTTPTimeout + 5*time.Second
	deadline := time.After(bound)
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.avail {
				t.Errorf("Available() = true against a hung server; want false")
			}
			if res.elapsed >= p.timeout {
				t.Errorf("probe took %v (>= client timeout %v); internal probeHTTPTimeout did not bound it",
					res.elapsed, p.timeout)
			}
		case <-deadline:
			t.Fatalf("Available() did not return within %v — probeAvailable is not internally bounded (availMu held for the client timeout)", bound)
		}
	}
}

// ── Capabilities ─────────────────────────────────────────────────────────────

func TestOpenAICapabilities(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{Model: "test-model", MaxTokens: 8192})
	caps := p.Capabilities()
	if !caps.IsLocal {
		t.Error("IsLocal should be true for OpenAI-compat (local server)")
	}
	if !caps.HasCapability(CapStreaming) {
		t.Error("should support streaming")
	}
	if !caps.HasCapability(CapJSON) {
		t.Error("should support JSON output")
	}
	if caps.MaxOutputTokens != 8192 {
		t.Errorf("MaxOutputTokens = %d; want 8192", caps.MaxOutputTokens)
	}
	if caps.CostPerInputToken != 0 || caps.CostPerOutputToken != 0 {
		t.Error("cost should be 0 for local provider")
	}
}

func TestOpenAICapabilitiesDefaultMaxTokens(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{Model: "m"})
	caps := p.Capabilities()
	if caps.MaxOutputTokens != openaiCompatDefaultMaxToks {
		t.Errorf("MaxOutputTokens = %d; want %d (default)", caps.MaxOutputTokens, openaiCompatDefaultMaxToks)
	}
}

// ── Ping ─────────────────────────────────────────────────────────────────────

func TestOpenAIPing(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("m"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	lat, err := p.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if lat <= 0 {
		t.Errorf("latency = %v; want > 0", lat)
	}
}

func TestOpenAIPingWithAuth(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(openaiModelsResponseJSON("m"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	p.apiKey = "test-key"
	lat, err := p.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if lat <= 0 {
		t.Errorf("latency = %v; want > 0", lat)
	}
}

// ── Complete ─────────────────────────────────────────────────────────────────

func TestOpenAIComplete(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		// Verify the request was properly formed.
		var payload openaiChatRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if payload.Stream {
			http.Error(w, "stream should be false", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(openaiChatResponseJSON("Hello!", "stop"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "test-model")
	resp, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Content != "Hello!" {
		t.Errorf("Content = %q; want Hello!", resp.Content)
	}
	if resp.Usage.InputTokens != 10 || resp.Usage.OutputTokens != 5 {
		t.Errorf("Usage = %+v; want {10, 5}", resp.Usage)
	}
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn", resp.StopReason)
	}
	if resp.ProviderMeta.Provider != "openai-compat" {
		t.Errorf("ProviderMeta.Provider = %q; want openai-compat", resp.ProviderMeta.Provider)
	}
}

func TestOpenAICompleteHTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"model not found"}}`, http.StatusNotFound)
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	_, err := p.Complete(context.Background(), &CompletionRequest{})
	if err == nil {
		t.Error("expected error for 404 response")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should mention status code, got: %v", err)
	}
}

func TestOpenAICompleteToolUseResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openaiChatResponse{
			ID: "chatcmpl-test",
			Choices: []openaiChoice{
				{
					Message: openaiMessage{
						Role: "assistant",
						ToolCalls: []openaiToolCall{
							{
								ID:   "call_xyz",
								Type: "function",
								Function: openaiToolCallDetail{
									Name:      "web_search",
									Arguments: `{"query":"golang testing"}`,
								},
							},
						},
					},
					FinishReason: "tool_calls",
				},
			},
			Usage: &openaiUsageResponse{PromptTokens: 20, CompletionTokens: 15},
		})
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
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
	if tc.ID != "call_xyz" {
		t.Errorf("ToolCall.ID = %q; want call_xyz", tc.ID)
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

// ── Stream ───────────────────────────────────────────────────────────────────

// sseOpenAILines formats SSE data lines in the OpenAI SSE format.
func sseOpenAILines(chunks []openaiStreamChunk) string {
	var sb strings.Builder
	for _, c := range chunks {
		b, _ := json.Marshal(c)
		fmt.Fprintf(&sb, "data: %s\n\n", b)
	}
	sb.WriteString("data: [DONE]\n\n")
	return sb.String()
}

func TestOpenAIStream(t *testing.T) {
	t.Parallel()

	stop := "stop"
	chunks := []openaiStreamChunk{
		{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Role: "assistant"}}},
		},
		{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: "Hello"}}},
		},
		{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: " world"}}},
		},
		{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{}, FinishReason: &stop}},
			Usage:   &openaiUsageResponse{PromptTokens: 7, CompletionTokens: 4, TotalTokens: 11},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseOpenAILines(chunks))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "test-model")
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
	if lastChunk.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn", lastChunk.StopReason)
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
	if lastChunk.ProviderMeta == nil || lastChunk.ProviderMeta.Provider != "openai-compat" {
		t.Errorf("ProviderMeta.Provider = %v; want openai-compat", lastChunk.ProviderMeta)
	}
}

func TestOpenAIStreamToolCall(t *testing.T) {
	t.Parallel()

	stop := "tool_calls"
	chunks := []openaiStreamChunk{
		{
			ID: "chatcmpl-1",
			Choices: []openaiStreamChoice{{
				Index: 0,
				Delta: openaiStreamDelta{
					Role: "assistant",
					ToolCalls: []openaiStreamToolCall{{
						Index: 0,
						ID:    "call_abc",
						Type:  "function",
						Function: openaiToolCallDetail{
							Name:      "search",
							Arguments: "",
						},
					}},
				},
			}},
		},
		{
			ID: "chatcmpl-1",
			Choices: []openaiStreamChoice{{
				Index: 0,
				Delta: openaiStreamDelta{
					ToolCalls: []openaiStreamToolCall{{
						Index:    0,
						Function: openaiToolCallDetail{Arguments: `{"q`},
					}},
				},
			}},
		},
		{
			ID: "chatcmpl-1",
			Choices: []openaiStreamChoice{{
				Index: 0,
				Delta: openaiStreamDelta{
					ToolCalls: []openaiStreamToolCall{{
						Index:    0,
						Function: openaiToolCallDetail{Arguments: `uery":"test"}`},
					}},
				},
			}},
		},
		{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{}, FinishReason: &stop}},
			Usage:   &openaiUsageResponse{PromptTokens: 12, CompletionTokens: 8},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseOpenAILines(chunks))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	ch, err := p.Stream(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "search"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var allChunks []StreamChunk
	for sc := range ch {
		if sc.Error != nil {
			t.Fatalf("stream error: %v", sc.Error)
		}
		allChunks = append(allChunks, sc)
	}

	// Find tool start, arg deltas, and done chunks.
	var toolStart, argDelta1, argDelta2, done StreamChunk
	for _, sc := range allChunks {
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ID == "call_abc" {
			toolStart = sc
		}
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ArgsDelta == `{"q` {
			argDelta1 = sc
		}
		if sc.ToolCallDelta != nil && sc.ToolCallDelta.ArgsDelta == `uery":"test"}` {
			argDelta2 = sc
		}
		if sc.Done {
			done = sc
		}
	}

	if toolStart.ToolCallDelta == nil {
		t.Error("expected a ToolCallDelta chunk with tool ID and name")
	} else {
		if toolStart.ToolCallDelta.Name != "search" {
			t.Errorf("tool name = %q; want search", toolStart.ToolCallDelta.Name)
		}
		if toolStart.ToolCallDelta.Index != 0 {
			t.Errorf("tool index = %d; want 0", toolStart.ToolCallDelta.Index)
		}
	}
	if argDelta1.ToolCallDelta == nil {
		t.Error("expected first arg delta chunk")
	}
	if argDelta2.ToolCallDelta == nil {
		t.Error("expected second arg delta chunk")
	}
	if !done.Done {
		t.Error("expected a Done chunk at end of stream")
	}
	if done.StopReason != "tool_use" {
		t.Errorf("StopReason = %q; want tool_use", done.StopReason)
	}
	if done.Usage == nil || done.Usage.OutputTokens != 8 {
		t.Errorf("done chunk usage = %+v; want OutputTokens=8", done.Usage)
	}
}

func TestOpenAIStreamMalformedSSE(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {not valid json}\n\n")
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
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
		t.Error("expected a StreamChunk with Error for malformed JSON SSE data")
	}
}

func TestOpenAIStreamContextCancelled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		// Send one chunk then block.
		chunk := openaiStreamChunk{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: "hello"}}},
		}
		b, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Stream(ctx, &CompletionRequest{})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Read first chunk then cancel.
	<-ch
	cancel()

	// Channel should close cleanly.
	for range ch {
	}
}

func TestOpenAIStreamHTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"overloaded"}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	_, err := p.Stream(context.Background(), &CompletionRequest{})
	if err == nil {
		t.Error("expected error for 503 response")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should mention status code, got: %v", err)
	}
}

// ── effectiveModel ───────────────────────────────────────────────────────────

func TestOpenAIEffectiveModel(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{Model: "gemma-2-9b"})

	// No override: use configured model.
	if got := p.effectiveModel(&CompletionRequest{}); got != "gemma-2-9b" {
		t.Errorf("effectiveModel = %q; want gemma-2-9b", got)
	}
	// With override.
	req := &CompletionRequest{ModelOverride: "llama-3.1-70b"}
	if got := p.effectiveModel(req); got != "llama-3.1-70b" {
		t.Errorf("effectiveModel = %q; want llama-3.1-70b", got)
	}
}

// ── Name ─────────────────────────────────────────────────────────────────────

func TestOpenAIName(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("my-lmstudio", ProviderConfig{Model: "m"})
	if p.Name() != "my-lmstudio" {
		t.Errorf("Name() = %q; want my-lmstudio", p.Name())
	}
}

func TestNewOpenAICompatProviderNormalizesTrailingV1(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Endpoint: "http://localhost:1234/v1/",
		Model:    "local-model",
	})
	if p.endpoint != "http://localhost:1234" {
		t.Fatalf("endpoint = %q; want http://localhost:1234", p.endpoint)
	}
}

// ── marshalRequest / default_options ─────────────────────────────────────────

// TestMarshalRequestNoDefaultOptions verifies that marshalRequest without any
// defaultOptions produces the same JSON as a plain json.Marshal.
func TestMarshalRequestNoDefaultOptions(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{Model: "m"})
	payload := buildOpenAIRequest("m", &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}, false, 4096)
	got, err := p.marshalRequest(payload)
	if err != nil {
		t.Fatalf("marshalRequest: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(got, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, hasExtra := raw["reasoning_effort"]; hasExtra {
		t.Error("reasoning_effort should not be present when no defaultOptions configured")
	}
}

// TestMarshalRequestDefaultOptionsInjected verifies that defaultOptions keys are
// merged into the wire body when configured.
func TestMarshalRequestDefaultOptionsInjected(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Model: "m",
		Options: map[string]interface{}{
			"default_options": map[string]interface{}{
				"reasoning_effort": "none",
			},
		},
	})
	payload := buildOpenAIRequest("m", &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}, false, 4096)
	got, err := p.marshalRequest(payload)
	if err != nil {
		t.Fatalf("marshalRequest: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(got, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw["reasoning_effort"] != "none" {
		t.Errorf("reasoning_effort = %v; want none", raw["reasoning_effort"])
	}
	// Standard fields must still be present.
	if _, ok := raw["model"]; !ok {
		t.Error("model field missing from wire body")
	}
	if _, ok := raw["messages"]; !ok {
		t.Error("messages field missing from wire body")
	}
}

// TestMarshalRequestStructFieldsWinOverDefaults verifies that per-request struct
// fields take precedence over defaultOptions when both address the same key.
func TestMarshalRequestStructFieldsWinOverDefaults(t *testing.T) {
	t.Parallel()
	// Simulate a case where a future struct field might overlap with a default option.
	// Use "stream" as the overlap field (struct sets it to false; we try to
	// inject true via defaultOptions — struct must win).
	p := NewOpenAICompatProvider("openai-compat", ProviderConfig{
		Model: "m",
		Options: map[string]interface{}{
			"default_options": map[string]interface{}{
				"reasoning_effort": "none",
				"stream":           true, // attempt to override the struct's stream=false
			},
		},
	})
	payload := buildOpenAIRequest("m", &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}, false /* stream=false */, 4096)
	got, err := p.marshalRequest(payload)
	if err != nil {
		t.Fatalf("marshalRequest: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(got, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// struct value (false) must win over defaultOptions value (true).
	if stream, _ := raw["stream"].(bool); stream != false {
		t.Errorf("stream = %v; want false (struct field must win over default_options)", raw["stream"])
	}
	// Non-conflicting default must still be injected.
	if raw["reasoning_effort"] != "none" {
		t.Errorf("reasoning_effort = %v; want none", raw["reasoning_effort"])
	}
}

// TestNewOpenAICompatProviderLoadsDefaultOptions verifies that ProviderConfig
// Options["default_options"] is correctly parsed into the provider's defaultOptions map.
func TestNewOpenAICompatProviderLoadsDefaultOptions(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("lmstudio-remote", ProviderConfig{
		Endpoint: "http://192.0.2.10:1234",
		Model:    "google/gemma-4-26b-a4b",
		Options: map[string]interface{}{
			"is_local":    true,
			"health_path": "/v1/models",
			"default_options": map[string]interface{}{
				"reasoning_effort": "none",
			},
		},
	})
	if p.defaultOptions == nil {
		t.Fatal("defaultOptions should not be nil when default_options is present in Options")
	}
	if p.defaultOptions["reasoning_effort"] != "none" {
		t.Errorf("defaultOptions[reasoning_effort] = %v; want none", p.defaultOptions["reasoning_effort"])
	}
}

// TestNewOpenAICompatProviderNoDefaultOptions verifies that a provider with no
// default_options entry leaves defaultOptions nil (zero-cost path: no map alloc,
// marshalRequest falls through to plain json.Marshal).
func TestNewOpenAICompatProviderNoDefaultOptions(t *testing.T) {
	t.Parallel()
	p := NewOpenAICompatProvider("lmstudio-remote", ProviderConfig{
		Endpoint: "http://192.0.2.10:1234",
		Model:    "google/gemma-4-26b-a4b",
		Options: map[string]interface{}{
			"is_local":    true,
			"health_path": "/v1/models",
		},
	})
	if p.defaultOptions != nil {
		t.Errorf("defaultOptions should be nil when not configured; got %v", p.defaultOptions)
	}
}

// ── CompleteCancelSafe / cancellation propagation (#432) ────────────────────

// TestOpenAICompleteDoesNotPropagateCancelToServer is a regression probe that
// documents the #432 defect this fix closes: a plain non-streaming Complete()
// request that the client abandons via ctx cancel does NOT reliably cause the
// server to observe the cancellation within a bounded window. This is the
// exact "kernel gives up, LM Studio keeps generating headless" failure mode
// from the incident forensics. The test asserts the (undesirable) status quo
// for Complete() specifically so a future change to Complete()'s transport
// behavior is caught here rather than silently reintroducing the zombie
// class; CompleteCancelSafe (tested below) is the supported fix.
func TestOpenAICompleteDoesNotPropagateCancelToServer(t *testing.T) {
	t.Parallel()
	closed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			close(closed)
		case <-time.After(3 * time.Second):
		}
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _ = p.Complete(ctx, &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}})

	select {
	case <-closed:
		t.Fatal("Complete() propagated cancellation to the server — if this now passes, the non-streaming transport behavior changed and CompleteCancelSafe's rationale comment should be revisited")
	case <-time.After(500 * time.Millisecond):
		// Expected: server never saw the cancellation within a short window.
	}
}

// TestOpenAICompleteCancelSafePropagatesToServer is the core #432 regression
// test: CompleteCancelSafe must cause the server to observe ctx cancellation
// (connection closes) so an abandoned generation actually aborts server-side
// instead of running headless.
func TestOpenAICompleteCancelSafePropagatesToServer(t *testing.T) {
	t.Parallel()
	closed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		chunk := openaiStreamChunk{
			ID:      "chatcmpl-1",
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: "partial"}}},
		}
		b, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
		select {
		case <-r.Context().Done():
			close(closed)
		case <-time.After(3 * time.Second):
		}
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := p.CompleteCancelSafe(ctx, &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}})
	if err == nil {
		t.Error("expected an error from CompleteCancelSafe when ctx is canceled mid-stream")
	}

	select {
	case <-closed:
		// Expected: the server observed the cancellation (connection closed).
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed ctx cancellation within 2s — CompleteCancelSafe did not propagate cancellation")
	}
}

// TestOpenAICompleteCancelSafeAggregatesContent verifies the happy path:
// CompleteCancelSafe must aggregate streamed deltas (text, reasoning, tool
// calls, usage, stop reason) into the same shape Complete() would have
// returned, since callers switching from Complete to CompleteCancelSafe
// must see no behavioral difference beyond cancellation semantics.
func TestOpenAICompleteCancelSafeAggregatesContent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload openaiChatRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !payload.Stream {
			http.Error(w, "expected stream:true from CompleteCancelSafe", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, delta := range []string{"Hel", "lo", "!"} {
			chunk := openaiStreamChunk{Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: delta}}}}
			b, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
		stop := "stop"
		final := openaiStreamChunk{
			Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{}, FinishReason: &stop}},
			Usage:   &openaiUsageResponse{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10},
		}
		b, _ := json.Marshal(final)
		fmt.Fprintf(w, "data: %s\n\n", b)
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "test-model")
	resp, err := p.CompleteCancelSafe(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("CompleteCancelSafe: %v", err)
	}
	if resp.Content != "Hello!" {
		t.Errorf("Content = %q; want Hello!", resp.Content)
	}
	if resp.StopReason != "end_turn" {
		t.Errorf("StopReason = %q; want end_turn", resp.StopReason)
	}
	if resp.Usage.InputTokens != 7 || resp.Usage.OutputTokens != 3 {
		t.Errorf("Usage = %+v; want {7, 3}", resp.Usage)
	}
	if resp.ProviderMeta.Provider != "openai-compat" {
		t.Errorf("ProviderMeta.Provider = %q; want openai-compat", resp.ProviderMeta.Provider)
	}
}

// TestOpenAICompleteCancelSafeUsageFromTerminalChunk is the regression guard for
// inference-pipeline-robustness FIX 2. It models LM Studio's include_usage wire
// behavior exactly: content chunks with usage:null, then a FINAL chunk carrying
// usage with an EMPTY choices array (the shape a server sends only when the
// request set stream_options.include_usage). The server also asserts the request
// actually carried that flag. Before the fix, buildOpenAIRequest never set
// stream_options, so LM Studio sent no usage chunk and every cancel-safe
// completion reported usage 0/0.
func TestOpenAICompleteCancelSafeUsageFromTerminalChunk(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload openaiChatRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// The whole point of the fix: the streaming request must ask for usage.
		if payload.StreamOptions == nil || !payload.StreamOptions.IncludeUsage {
			http.Error(w, "request did not set stream_options.include_usage", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		// Content chunks carry no usage (usage:null), just like LM Studio.
		stop := "stop"
		for _, delta := range []string{"Hel", "lo"} {
			chunk := openaiStreamChunk{Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{Content: delta}}}}
			b, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
		// Finish-reason chunk (still no usage).
		finish := openaiStreamChunk{Choices: []openaiStreamChoice{{Index: 0, Delta: openaiStreamDelta{}, FinishReason: &stop}}}
		fb, _ := json.Marshal(finish)
		fmt.Fprintf(w, "data: %s\n\n", fb)
		flusher.Flush()
		// Terminal usage chunk: usage present, choices EMPTY — the include_usage shape.
		usageChunk := openaiStreamChunk{
			Choices: []openaiStreamChoice{},
			Usage:   &openaiUsageResponse{PromptTokens: 42, CompletionTokens: 17, TotalTokens: 59},
		}
		ub, _ := json.Marshal(usageChunk)
		fmt.Fprintf(w, "data: %s\n\n", ub)
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "test-model")
	resp, err := p.CompleteCancelSafe(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("CompleteCancelSafe: %v", err)
	}
	if resp.Content != "Hello" {
		t.Errorf("Content = %q; want Hello", resp.Content)
	}
	if resp.Usage.InputTokens != 42 || resp.Usage.OutputTokens != 17 {
		t.Errorf("Usage = %+v; want {InputTokens:42, OutputTokens:17} from the terminal usage chunk", resp.Usage)
	}
}

// TestOpenAICompleteCancelSafeSurfacesStreamError verifies that a mid-stream
// error is surfaced as an error from CompleteCancelSafe rather than silently
// returning a partial/empty success.
func TestOpenAICompleteCancelSafeSurfacesStreamError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		fmt.Fprint(w, "data: {not valid json\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	_, err := p.CompleteCancelSafe(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Error("expected error from malformed SSE chunk")
	}
}

// TestCompleteCancelSafeIfSupportedUsesCancelSafeForOpenAICompat verifies the
// dispatch helper routes to CompleteCancelSafe when the provider implements
// CancelSafeCompleter.
func TestCompleteCancelSafeIfSupportedUsesCancelSafeForOpenAICompat(t *testing.T) {
	t.Parallel()
	var sawStream bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload openaiChatRequest
		_ = json.NewDecoder(r.Body).Decode(&payload)
		sawStream = payload.Stream
		if payload.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
			return
		}
		_ = json.NewEncoder(w).Encode(openaiChatResponseJSON("non-stream", "stop"))
	}))
	defer srv.Close()

	p := newTestOpenAIProvider(t, srv.URL, "m")
	_, err := CompleteCancelSafeIfSupported(context.Background(), p, &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("CompleteCancelSafeIfSupported: %v", err)
	}
	if !sawStream {
		t.Error("CompleteCancelSafeIfSupported did not route an *OpenAICompatProvider through the streaming (cancel-safe) path")
	}
}

// stubProviderNoCancelSafe is a minimal Provider that does NOT implement
// CancelSafeCompleter, used to verify CompleteCancelSafeIfSupported falls
// back to plain Complete for providers outside the #432 fix's scope.
type stubProviderNoCancelSafe struct {
	completeCalled bool
}

func (s *stubProviderNoCancelSafe) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	s.completeCalled = true
	return &CompletionResponse{Content: "stub"}, nil
}
func (s *stubProviderNoCancelSafe) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	ch := make(chan StreamChunk)
	close(ch)
	return ch, nil
}
func (s *stubProviderNoCancelSafe) Name() string                       { return "stub" }
func (s *stubProviderNoCancelSafe) Model() string                      { return "stub-model" }
func (s *stubProviderNoCancelSafe) Available(ctx context.Context) bool { return true }
func (s *stubProviderNoCancelSafe) Capabilities() ProviderCapabilities { return ProviderCapabilities{} }
func (s *stubProviderNoCancelSafe) Ping(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

func TestCompleteCancelSafeIfSupportedFallsBackForPlainProvider(t *testing.T) {
	t.Parallel()
	s := &stubProviderNoCancelSafe{}
	resp, err := CompleteCancelSafeIfSupported(context.Background(), s, &CompletionRequest{})
	if err != nil {
		t.Fatalf("CompleteCancelSafeIfSupported: %v", err)
	}
	if !s.completeCalled {
		t.Error("expected fallback to Complete() for a provider that does not implement CancelSafeCompleter")
	}
	if resp.Content != "stub" {
		t.Errorf("Content = %q; want stub", resp.Content)
	}
}
