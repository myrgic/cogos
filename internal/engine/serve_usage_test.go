package engine

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var chatUsageCases = []struct {
	name        string
	in          TokenUsage
	wantPrompt  int
	wantTotal   int
	wantDetails *oaiPromptTokenDetail
}{
	{"no cache", TokenUsage{InputTokens: 22, OutputTokens: 5}, 22, 27, nil},
	{"cache read", TokenUsage{InputTokens: 22, OutputTokens: 5, CacheReadTokens: 10000}, 10022, 10027,
		&oaiPromptTokenDetail{CachedTokens: 10000}},
	{"cache write", TokenUsage{InputTokens: 22, OutputTokens: 5, CacheWriteTokens: 300}, 322, 327,
		&oaiPromptTokenDetail{CacheWriteTokens: 300}},
	{"read and write", TokenUsage{InputTokens: 2, OutputTokens: 1, CacheReadTokens: 100, CacheWriteTokens: 10}, 112, 113,
		&oaiPromptTokenDetail{CachedTokens: 100, CacheWriteTokens: 10}},
}

func checkOAIUsage(t *testing.T, got *oaiUsage, prompt, total, out int, details *oaiPromptTokenDetail) {
	t.Helper()
	if got == nil {
		t.Fatal("usage missing")
	}
	if got.PromptTokens != prompt || got.TotalTokens != total || got.CompletionTokens != out {
		t.Errorf("usage = %+v; want prompt=%d total=%d completion=%d", got, prompt, total, out)
	}
	switch {
	case details == nil && got.PromptTokensDetails != nil:
		t.Errorf("prompt_tokens_details = %+v; want omitted", got.PromptTokensDetails)
	case details != nil && (got.PromptTokensDetails == nil || *got.PromptTokensDetails != *details):
		t.Errorf("prompt_tokens_details = %+v; want %+v", got.PromptTokensDetails, details)
	}
}

func TestChatUsageNonStreamingCacheTokens(t *testing.T) {
	t.Parallel()
	for _, tc := range chatUsageCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t)
			router := NewSimpleRouter(RoutingConfig{Default: "stub"})
			router.RegisterProvider(NewStubProvider("stub", "ok").WithUsage(tc.in))
			srv.SetRouter(router)

			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"local","messages":[{"role":"user","content":"hi"}]}`))
			w := httptest.NewRecorder()
			srv.handleChat(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}
			var body oaiChatResponse
			if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			checkOAIUsage(t, body.Usage, tc.wantPrompt, tc.wantTotal, tc.in.OutputTokens, tc.wantDetails)
			if tc.wantDetails == nil && strings.Contains(w.Body.String(), "prompt_tokens_details") {
				t.Error("zero-cache response must not carry prompt_tokens_details")
			}
		})
	}
}

func TestChatUsageStreamingFinalChunkCacheTokens(t *testing.T) {
	t.Parallel()
	for _, tc := range chatUsageCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t)
			router := NewSimpleRouter(RoutingConfig{Default: "stub"})
			router.RegisterProvider(NewStubProvider("stub", "ok").WithUsage(tc.in))
			srv.SetRouter(router)

			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"local","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`))
			w := httptest.NewRecorder()
			srv.handleChat(w, req)

			var last *oaiUsage
			sc := bufio.NewScanner(w.Body)
			sc.Buffer(make([]byte, 0, 1<<16), 1<<20)
			for sc.Scan() {
				data, ok := strings.CutPrefix(sc.Text(), "data: ")
				if !ok || data == "[DONE]" {
					continue
				}
				var chunk oaiChatResponse
				if err := json.Unmarshal([]byte(data), &chunk); err != nil {
					t.Fatal(err)
				}
				if chunk.Usage != nil {
					last = chunk.Usage
				}
			}
			checkOAIUsage(t, last, tc.wantPrompt, tc.wantTotal, tc.in.OutputTokens, tc.wantDetails)
		})
	}
}

// codex reports input_tokens INCLUDING cached tokens; TokenUsage.InputTokens
// must be uncached-only so the chat route does not double count the cache.
func TestCodexUsageExcludesCachedFromInput(t *testing.T) {
	t.Parallel()
	p := &CodexProvider{}
	_, u, done := p.parseEventLine([]byte(`{"type":"turn.completed","usage":{"input_tokens":10022,"cached_input_tokens":10000,"output_tokens":5}}`))
	if !done || u == nil {
		t.Fatalf("done=%v usage=%v", done, u)
	}
	if u.InputTokens != 22 || u.CacheReadTokens != 10000 {
		t.Errorf("usage = %+v; want input=22 cacheRead=10000", u)
	}
	if got := oaiUsageFrom(*u).PromptTokens; got != 10022 {
		t.Errorf("prompt_tokens = %d; want 10022", got)
	}
}
