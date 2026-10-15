package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// ccOf returns the cache_control object of a decoded wire block, or nil.
func ccOf(blk any) map[string]any {
	m, _ := blk.(map[string]any)
	cc, _ := m["cache_control"].(map[string]any)
	return cc
}

// wantCC fails unless cc is an ephemeral marker with the given ttl ("" = no ttl key).
func wantCC(t *testing.T, where string, cc map[string]any, ttl string) {
	t.Helper()
	if cc == nil || cc["type"] != "ephemeral" {
		t.Fatalf("%s: want an ephemeral cache_control, got %v", where, cc)
	}
	got, _ := cc["ttl"].(string)
	if got != ttl {
		t.Fatalf("%s: ttl = %q; want %q (cache_control=%v)", where, got, ttl, cc)
	}
}

func wireOf(t *testing.T, r *anthropicRequest) map[string]any {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestResolveCacheTTL_Policy(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, hint, prov, fallback, want string }{
		{"hint wins over provider and fallback", "5m", "1h", "1h", "5m"},
		{"hint 1h wins over provider 5m", "1h", "5m", "5m", "1h"},
		{"provider default beats fallback", "", "5m", "1h", "5m"},
		{"fallback when nothing set (oauth)", "", "", "1h", "1h"},
		{"fallback when nothing set (others)", "", "", "5m", "5m"},
		{"invalid hint falls through to provider", "2h", "1h", "5m", "1h"},
		{"case and space tolerant", " 1H ", "", "5m", "1h"},
		{"nothing valid anywhere is 5m", "x", "y", "z", "5m"},
	}
	for _, c := range cases {
		if got := resolveCacheTTL(c.hint, c.prov, c.fallback); got != c.want {
			t.Errorf("%s: resolveCacheTTL(%q,%q,%q) = %q; want %q", c.name, c.hint, c.prov, c.fallback, got, c.want)
		}
	}
}

func TestCacheBreakpoints_TTLOnStablePrefixOnly(t *testing.T) {
	t.Parallel()
	r := cacheReq(
		anthropicMessage{Role: "user", Content: "hi"},
		anthropicMessage{Role: "assistant", Content: "hello"},
		anthropicMessage{Role: "user", Content: "again"},
	)
	applyAnthropicCacheBreakpoints(r, cacheTTL1h)
	wire := wireOf(t, r)

	sys, _ := wire["system"].([]any)
	if ccOf(sys[0]) != nil {
		t.Fatal("only the last system block may carry a marker")
	}
	wantCC(t, "system last", ccOf(sys[len(sys)-1]), "1h")
	tools, _ := wire["tools"].([]any)
	wantCC(t, "tools last", ccOf(tools[len(tools)-1]), "1h")
	msgs, _ := wire["messages"].([]any)
	tail, _ := msgs[len(msgs)-2].(map[string]any)["content"].([]any)
	wantCC(t, "conversation tail", ccOf(tail[len(tail)-1]), "") // default lifetime

	if n := countBreakpoints(t, r) + countTTLBreakpoints(t, r); n != 3 {
		t.Fatalf("breakpoint budget: want 3 total, got %d", n)
	}
}

func countTTLBreakpoints(t *testing.T, r *anthropicRequest) int {
	t.Helper()
	b, _ := json.Marshal(r)
	return strings.Count(string(b), `"ttl":"1h"`)
}

func TestCacheBreakpoints_FiveMinuteShapeUnchanged(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "hi"}, anthropicMessage{Role: "user", Content: "x"})
	applyAnthropicCacheBreakpoints(r, cacheTTL5m)
	if n := countTTLBreakpoints(t, r); n != 0 {
		t.Fatalf("5m must not emit a ttl field; found %d", n)
	}
}

func TestCacheBreakpoints_RestampSwitchesLifetime(t *testing.T) {
	t.Parallel()
	r := cacheReq(anthropicMessage{Role: "user", Content: "hi"}, anthropicMessage{Role: "user", Content: "x"})
	applyAnthropicCacheBreakpoints(r, cacheTTL5m)
	applyAnthropicCacheBreakpoints(r, cacheTTL1h)
	wire := wireOf(t, r)
	sys, _ := wire["system"].([]any)
	wantCC(t, "system after restamp", ccOf(sys[len(sys)-1]), "1h")
}

func TestOAuthRelocatedBreakpoint_CarriesTTL(t *testing.T) {
	t.Parallel()
	const relocated = "relocated operator system text"
	r := cacheReq(anthropicMessage{Role: "user", Content: "question"})
	prependOAuthSystemToUserTurn(r, relocated)
	repaired, _ := normalizeAnthropicMessages(r.Messages)
	r.Messages = repaired
	applyAnthropicCacheBreakpoints(r, cacheTTL1h)
	applyOAuthRelocatedCacheBreakpoint(r, relocated, cacheTTL1h)
	wire := wireOf(t, r)

	msgs, _ := wire["messages"].([]any)
	blocks, _ := msgs[0].(map[string]any)["content"].([]any)
	wantCC(t, "relocated prefix", ccOf(blocks[0]), "1h")
	if len(blocks) > 1 && ccOf(blocks[len(blocks)-1]) != nil {
		t.Fatal("volatile user text after the relocated prefix must stay unmarked")
	}
	sys, _ := wire["system"].([]any)
	wantCC(t, "system", ccOf(sys[len(sys)-1]), "1h")
	if n := countBreakpoints(t, r) + countTTLBreakpoints(t, r); n != 3 {
		t.Fatalf("breakpoint budget: want 3 total (system, tools, relocated), got %d", n)
	}
}

// captureOAuthWire runs one OAuth Complete and returns the decoded request body.
func captureOAuthWire(t *testing.T, configTTL, hint string) map[string]any {
	t.Helper()
	var wire map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&wire)
		_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
	}))
	t.Cleanup(srv.Close)
	p := newTestOAuthProvider(t, srv.URL, freshCred())
	p.cacheTTL = normalizeCacheTTL(configTTL)
	_, err := p.Complete(context.Background(), &CompletionRequest{
		SystemPrompt: "nucleus identity card",
		Messages:     []ProviderMessage{{Role: "user", Content: "hi"}},
		Tools:        []ToolDefinition{{Name: "ls", Description: "list", InputSchema: map[string]any{"type": "object"}}},
		Metadata:     RequestMetadata{CacheTTL: hint},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return wire
}

func TestClaudeOAuth_DefaultsToOneHourOnTheWire(t *testing.T) {
	t.Parallel()
	wire := captureOAuthWire(t, "", "")
	sys, _ := wire["system"].([]any)
	wantCC(t, "system", ccOf(sys[len(sys)-1]), "1h")
	tools, _ := wire["tools"].([]any)
	wantCC(t, "tools", ccOf(tools[len(tools)-1]), "1h")
	msgs, _ := wire["messages"].([]any)
	blocks, _ := msgs[0].(map[string]any)["content"].([]any)
	wantCC(t, "relocated prefix", ccOf(blocks[0]), "1h")
}

func TestClaudeOAuth_HintOverridesProviderDefault(t *testing.T) {
	t.Parallel()
	// header 5m beats the built-in 1h
	wire := captureOAuthWire(t, "", "5m")
	sys, _ := wire["system"].([]any)
	wantCC(t, "system (hint 5m)", ccOf(sys[len(sys)-1]), "")
	// header 1h beats configured 5m
	wire = captureOAuthWire(t, "5m", "1h")
	sys, _ = wire["system"].([]any)
	wantCC(t, "system (hint 1h over config 5m)", ccOf(sys[len(sys)-1]), "1h")
	// configured 5m with no hint
	wire = captureOAuthWire(t, "5m", "")
	tools, _ := wire["tools"].([]any)
	wantCC(t, "tools (config 5m)", ccOf(tools[len(tools)-1]), "")
}

func TestAnthropicProvider_NonOAuthDefaultsToFiveMinutes(t *testing.T) {
	t.Parallel()
	capture := func(cfgTTL, hint string) map[string]any {
		var wire map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&wire)
			_ = json.NewEncoder(w).Encode(anthropicResponseBody("ok"))
		}))
		defer srv.Close()
		p := newTestAnthropicProvider(t, srv.URL)
		p.cacheTTL = normalizeCacheTTL(cfgTTL)
		_, err := p.Complete(context.Background(), &CompletionRequest{
			SystemPrompt: "sys",
			Messages:     []ProviderMessage{{Role: "user", Content: "hi"}},
			Tools:        []ToolDefinition{{Name: "ls", InputSchema: map[string]any{"type": "object"}}},
			Metadata:     RequestMetadata{CacheTTL: hint},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	lastSys := func(w map[string]any) map[string]any {
		s, _ := w["system"].([]any)
		return ccOf(s[len(s)-1])
	}
	wantCC(t, "default", lastSys(capture("", "")), "")
	wantCC(t, "config 1h", lastSys(capture("1h", "")), "1h")
	wantCC(t, "hint 1h", lastSys(capture("", "1h")), "1h")
	wantCC(t, "hint 5m over config 1h", lastSys(capture("1h", "5m")), "")
}

func TestUsage_CacheCreationSplit_NonStream(t *testing.T) {
	t.Parallel()
	body := `{"type":"message","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn",
	 "usage":{"input_tokens":3,"output_tokens":2,"cache_read_input_tokens":0,
	 "cache_creation_input_tokens":148000,
	 "cache_creation":{"ephemeral_5m_input_tokens":8000,"ephemeral_1h_input_tokens":140000}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, body)
	}))
	defer srv.Close()
	p := newTestAnthropicProvider(t, srv.URL)
	resp, err := p.Complete(context.Background(), &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	u := resp.Usage
	if u.CacheWriteTokens != 148000 || u.CacheWrite5mTokens != 8000 || u.CacheWrite1hTokens != 140000 {
		t.Fatalf("usage = %+v", u)
	}
	if u.CacheWriteTokens != u.CacheWrite5mTokens+u.CacheWrite1hTokens {
		t.Fatal("CacheWriteTokens must equal the sum of the lifetimes")
	}
}

func TestUsage_CacheCreationMissing_StaysZero(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"type":"message","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2,"cache_creation_input_tokens":500}}`)
	}))
	defer srv.Close()
	p := newTestAnthropicProvider(t, srv.URL)
	resp, err := p.Complete(context.Background(), &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	u := resp.Usage
	if u.CacheWriteTokens != 500 || u.CacheWrite5mTokens != 0 || u.CacheWrite1hTokens != 0 {
		t.Fatalf("legacy response: usage = %+v; want total 500 and a zero split", u)
	}
}

func TestUsage_CacheCreationSplit_Stream(t *testing.T) {
	t.Parallel()
	events := []anthropicSSEEvent{
		{Type: "message_start", Message: &anthropicSSEMsg{Usage: anthropicUsage{
			InputTokens: 7, CacheCreationInputTokens: 90,
			CacheCreation: &anthropicCacheCreation{Ephemeral5mInputTokens: 30, Ephemeral1hInputTokens: 60},
		}}},
		{Type: "content_block_start", Index: 0, ContentBlock: &anthropicContent{Type: "text"}},
		{Type: "content_block_delta", Index: 0, Delta: &anthropicSSEDelta{Type: "text_delta", Text: "x"}},
		{Type: "content_block_stop", Index: 0},
		{Type: "message_delta", Delta: &anthropicSSEDelta{StopReason: "end_turn"}, Usage: &anthropicSSEUsage{OutputTokens: 1}},
		{Type: "message_stop"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseLines(events))
	}))
	defer srv.Close()
	p := newTestAnthropicProvider(t, srv.URL)
	ch, err := p.Stream(context.Background(), &CompletionRequest{Messages: []ProviderMessage{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var last StreamChunk
	for sc := range ch {
		if sc.Error != nil {
			t.Fatal(sc.Error)
		}
		last = sc
	}
	if last.Usage == nil {
		t.Fatal("no usage on the final chunk")
	}
	u := *last.Usage
	if u.CacheWriteTokens != 90 || u.CacheWrite5mTokens != 30 || u.CacheWrite1hTokens != 60 {
		t.Fatalf("stream usage = %+v", u)
	}
}

func TestUsage_CacheSplitCarriedToLedgerAndOpenAIBlock(t *testing.T) {
	t.Parallel()
	tu := newTurnUsage(3, 2, 10, 90).withCacheSplit(30, 60)
	if tu.CacheWriteTokens != 90 || tu.CacheWrite5mTokens != 30 || tu.CacheWrite1hTokens != 60 {
		t.Fatalf("turn usage = %+v", tu)
	}
	// ledger round trip: the map RecordTurn writes projects back to the same record.
	// JSON-roundtrip the map so numbers arrive as float64, as they do from the ledger.
	raw, _ := json.Marshal(turnUsageLedgerMap(tu))
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	ev := EventEnvelope{HashedPayload: EventPayload{Type: "turn.completed", Data: map[string]interface{}{"turn_id": "t", "usage": m}}}
	if got := projectTurnFromEvent(ev, nil, false, false).Usage; got != tu {
		t.Fatalf("ledger round trip = %+v; want %+v", got, tu)
	}
	// OpenAI-compat block
	oai := oaiUsageFrom(TokenUsage{InputTokens: 3, OutputTokens: 2, CacheReadTokens: 10, CacheWriteTokens: 90, CacheWrite5mTokens: 30, CacheWrite1hTokens: 60})
	d := oai.PromptTokensDetails
	if d == nil || d.CacheWriteTokens != 90 || d.CacheWrite5mTokens != 30 || d.CacheWrite1hTokens != 60 {
		t.Fatalf("openai details = %+v", d)
	}
	// /v1/messages block carries the split in Anthropic's own shape.
	b, _ := json.Marshal(anthropicMessagesUsage{CacheCreationInputTokens: 90, CacheCreation: cacheCreationFrom(30, 60)})
	want := `"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":60}`
	if !strings.Contains(string(b), want) {
		t.Fatalf("messages usage = %s; want it to contain %s", b, want)
	}
	if cacheCreationFrom(0, 0) != nil {
		t.Fatal("no split reported must omit cache_creation")
	}
}

func TestCacheTTL_ConfigKeyReachesProviders(t *testing.T) {
	t.Parallel()
	var pc ProviderConfig
	if err := yaml.Unmarshal([]byte("type: claude-oauth\nmodel: m\ncache_ttl: \"5m\"\n"), &pc); err != nil {
		t.Fatal(err)
	}
	if pc.CacheTTL != "5m" {
		t.Fatalf("cache_ttl = %q; want 5m", pc.CacheTTL)
	}
	p, err := makeProvider("claude-oauth", pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.(*ClaudeOAuthProvider).effectiveCacheTTL(&CompletionRequest{}); got != "5m" {
		t.Fatalf("configured 5m ignored: effective = %q", got)
	}
	// Unset config: claude-oauth defaults to 1h, anthropic to 5m.
	p, _ = makeProvider("claude-oauth", ProviderConfig{Type: "claude-oauth", Model: "m"}, nil)
	if got := p.(*ClaudeOAuthProvider).effectiveCacheTTL(&CompletionRequest{}); got != "1h" {
		t.Fatalf("claude-oauth default = %q; want 1h", got)
	}
	var y struct {
		ClaudeOAuth ClaudeOAuthAutoConfig `yaml:"claude_oauth"`
	}
	if err := yaml.Unmarshal([]byte("claude_oauth:\n  cache_ttl: 5m\n"), &y); err != nil || y.ClaudeOAuth.CacheTTL != "5m" {
		t.Fatalf("claude_oauth.cache_ttl not parsed: %v %+v", err, y)
	}
}
