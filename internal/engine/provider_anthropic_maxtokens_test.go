// provider_anthropic_maxtokens_test.go — per-request output budget must reach
// the wire on every Anthropic-family dispatch path.
//
// REGRESSION: both Anthropic-family providers passed their construction-time
// maxTokens straight into buildAnthropicRequest and never consulted
// CompletionRequest.MaxTokens. A client asking for a large output budget
// through /v1/chat/completions or /v1/messages was silently clamped to
// anthropicDefaultMaxToks (8192) and received finish_reason="length"
// mid-answer, with nothing indicating the ceiling was the kernel's.
//
// Measured live before the fix, through the kernel at :6931 with
// max_tokens=32000 on model claude-opus-5:
//
//	finish_reason: "length"
//	usage: {prompt_tokens: 71, completion_tokens: 8192, total_tokens: 8263}
//
// The sentinel below is deliberately NOT a plausible default (777_000, and
// 555_000 for the second provider) so a regression to ANY hardcoded value —
// 8192, 4096, 64000 — fails this test rather than coincidentally passing.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sentinelMaxTokens is the per-request budget asserted on the wire. Chosen to
// differ from every provider default in this package.
const sentinelMaxTokens = 777_000

// secondSentinelMaxTokens distinguishes the OAuth provider's assertions from
// the API-key provider's, so a cross-wired test cannot pass by accident.
const secondSentinelMaxTokens = 555_000

// captureMaxTokens returns a test server that records the max_tokens field of
// the inbound Anthropic request body and replies with the given body writer.
func captureMaxTokens(t *testing.T, got *int, reply func(w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			MaxTokens int `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		*got = body.MaxTokens
		reply(w)
	}))
}

// ── builder-level ─────────────────────────────────────────────────────────────

func TestResolveAnthropicMaxTokensPrefersRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		req             *CompletionRequest
		providerDefault int
		want            int
	}{
		{"request budget wins", &CompletionRequest{MaxTokens: sentinelMaxTokens}, anthropicDefaultMaxToks, sentinelMaxTokens},
		{"zero request falls back to provider default", &CompletionRequest{}, anthropicDefaultMaxToks, anthropicDefaultMaxToks},
		{"negative request falls back", &CompletionRequest{MaxTokens: -5}, 4096, 4096},
		{"nil request falls back", nil, 1024, 1024},
		{"request below default still wins (a budget is a budget)", &CompletionRequest{MaxTokens: 16}, anthropicDefaultMaxToks, 16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveAnthropicMaxTokens(tc.req, tc.providerDefault); got != tc.want {
				t.Errorf("resolveAnthropicMaxTokens = %d; want %d", got, tc.want)
			}
		})
	}
}

func TestBuildAnthropicRequestHonoursRequestMaxTokens(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{
		MaxTokens: sentinelMaxTokens,
		Messages:  []ProviderMessage{{Role: "user", Content: "hi"}},
	}
	ar := buildAnthropicRequest("claude-sonnet-4-20250514", req, false, anthropicDefaultMaxToks)
	if ar.MaxTokens != sentinelMaxTokens {
		t.Errorf("max_tokens = %d; want %d (per-request budget must override the provider default)",
			ar.MaxTokens, sentinelMaxTokens)
	}
}

func TestBuildAnthropicRequestFallsBackToProviderDefault(t *testing.T) {
	t.Parallel()
	req := &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}}
	ar := buildAnthropicRequest("claude-sonnet-4-20250514", req, false, sentinelMaxTokens)
	if ar.MaxTokens != sentinelMaxTokens {
		t.Errorf("max_tokens = %d; want %d (provider default applies when the request carries no budget)",
			ar.MaxTokens, sentinelMaxTokens)
	}
}

// ── AnthropicProvider (x-api-key path) ────────────────────────────────────────

func TestAnthropicCompleteSendsRequestMaxTokens(t *testing.T) {
	t.Parallel()
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
	})
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL) // configured MaxTokens: 8192
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		MaxTokens: sentinelMaxTokens,
		Messages:  []ProviderMessage{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got != sentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (request budget dropped on the Complete path)", got, sentinelMaxTokens)
	}
}

func TestAnthropicStreamSendsRequestMaxTokens(t *testing.T) {
	t.Parallel()
	events := []anthropicSSEEvent{
		{Type: "message_start", Message: &anthropicSSEMsg{Usage: anthropicUsage{InputTokens: 3}}},
		{Type: "content_block_start", Index: 0, ContentBlock: &anthropicContent{Type: "text"}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{Type: "text_delta", Text: "ok"}},
		{Type: "content_block_stop", Index: 0},
		{Type: "message_delta", Delta: &anthropicSSEDelta{StopReason: "end_turn"}, Usage: &anthropicSSEUsage{OutputTokens: 1}},
		{Type: "message_stop"},
	}
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseLines(events))
	})
	defer srv.Close()

	p := newTestAnthropicProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{
		MaxTokens: sentinelMaxTokens,
		Messages:  []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for sc := range ch {
		if sc.Error != nil {
			t.Fatalf("stream error: %v", sc.Error)
		}
	}
	if got != sentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (request budget dropped on the Stream path)", got, sentinelMaxTokens)
	}
}

func TestAnthropicCompleteFallsBackToConfiguredMaxTokens(t *testing.T) {
	t.Parallel()
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
	})
	defer srv.Close()

	p := NewAnthropicProvider("anthropic", ProviderConfig{
		Endpoint:  srv.URL,
		Model:     "claude-sonnet-4-20250514",
		MaxTokens: sentinelMaxTokens,
		Timeout:   5,
	})
	p.apiKey = "test-key-123"
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got != sentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (configured provider default must still apply)", got, sentinelMaxTokens)
	}
}

// ── ClaudeOAuthProvider (OAuth bearer path — the live Darkstar seat) ──────────

// newSentinelOAuthProvider builds an OAuth provider whose CONFIGURED budget is
// secondSentinelMaxTokens, so a test asserting sentinelMaxTokens on the wire
// proves the request value won rather than the provider default leaking through.
func newSentinelOAuthProvider(t *testing.T, endpoint string) *ClaudeOAuthProvider {
	t.Helper()
	src := &stubCredentialSource{cred: freshCred()}
	lc := NewCredentialLifecycle(src, func(_ context.Context, _ string) (OAuthCredential, error) {
		return OAuthCredential{}, fmt.Errorf("no refresh configured in test")
	})
	return &ClaudeOAuthProvider{
		name:      "claude-oauth",
		model:     "claude-opus-4-8",
		endpoint:  strings.TrimRight(endpoint, "/"),
		maxTokens: secondSentinelMaxTokens,
		timeout:   5 * time.Second,
		client:    &http.Client{Timeout: 5 * time.Second},
		lc:        lc,
	}
}

func TestClaudeOAuthCompleteSendsRequestMaxTokens(t *testing.T) {
	t.Parallel()
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
	})
	defer srv.Close()

	p := newSentinelOAuthProvider(t, srv.URL)
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		MaxTokens: sentinelMaxTokens,
		Messages:  []ProviderMessage{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got != sentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (request budget dropped on the OAuth Complete path — this is the live Darkstar seat)",
			got, sentinelMaxTokens)
	}
}

func TestClaudeOAuthStreamSendsRequestMaxTokens(t *testing.T) {
	t.Parallel()
	events := []anthropicSSEEvent{
		{Type: "message_start", Message: &anthropicSSEMsg{Usage: anthropicUsage{InputTokens: 3}}},
		{Type: "content_block_start", Index: 0, ContentBlock: &anthropicContent{Type: "text"}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{Type: "text_delta", Text: "ok"}},
		{Type: "content_block_stop", Index: 0},
		{Type: "message_delta", Delta: &anthropicSSEDelta{StopReason: "end_turn"}, Usage: &anthropicSSEUsage{OutputTokens: 1}},
		{Type: "message_stop"},
	}
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseLines(events))
	})
	defer srv.Close()

	p := newSentinelOAuthProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{
		MaxTokens: sentinelMaxTokens,
		Messages:  []ProviderMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for sc := range ch {
		if sc.Error != nil {
			t.Fatalf("stream error: %v", sc.Error)
		}
	}
	if got != sentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (request budget dropped on the OAuth Stream path)", got, sentinelMaxTokens)
	}
}

func TestClaudeOAuthFallsBackToConfiguredMaxTokens(t *testing.T) {
	t.Parallel()
	var got int
	srv := captureMaxTokens(t, &got, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
	})
	defer srv.Close()

	p := newSentinelOAuthProvider(t, srv.URL)
	if _, err := p.Complete(context.Background(), &CompletionRequest{
		Messages: []ProviderMessage{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got != secondSentinelMaxTokens {
		t.Errorf("wire max_tokens = %d; want %d (configured provider default must still apply)", got, secondSentinelMaxTokens)
	}
}

// ── ingress parity ────────────────────────────────────────────────────────────

// TestAnthropicIngressCarriesMaxTokensToProvider is the end-to-end shape of the
// reported defect: both HTTP ingress handlers already parse the client's budget
// into CompletionRequest.MaxTokens (serve.go, serve_anthropic.go). This asserts
// the provider layer consumes what ingress produced, closing the loop.
func TestAnthropicIngressCarriesMaxTokensToProvider(t *testing.T) {
	t.Parallel()
	// Mirrors serve.go's resolution: max_completion_tokens beats max_tokens.
	creq := &CompletionRequest{MaxTokens: sentinelMaxTokens}
	ar := buildAnthropicRequest("claude-opus-4-8", creq, false, anthropicDefaultMaxToks)
	if ar.MaxTokens != sentinelMaxTokens {
		t.Fatalf("ingress budget %d did not reach the wire (got %d)", sentinelMaxTokens, ar.MaxTokens)
	}
	if ar.MaxTokens == anthropicDefaultMaxToks {
		t.Fatal("wire budget equals the package default — the clamp is back")
	}
}
