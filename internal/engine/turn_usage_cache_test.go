package engine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// K1: the stored turn record must carry the provider's cache accounting, the
// kernel request id, and wall time, so the kernel ledger is not blind to
// cache-hitting calls. input_tokens stays the uncached tail.
func TestTurnRecordCarriesCacheUsage(t *testing.T) {
	t.Parallel()
	usage := TokenUsage{InputTokens: 22, OutputTokens: 1, CacheReadTokens: 10000, CacheWriteTokens: 500}
	bodies := map[string]string{
		"non-stream": `{"model":"claude","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":false}`,
		"stream":     `{"model":"claude","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}`,
	}
	for name, body := range bodies {
		body := body
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := newTestServer(t)
			router := NewSimpleRouter(RoutingConfig{Default: "stub"})
			router.RegisterProvider(NewStubProvider("stub", "ok").WithUsage(usage))
			srv.SetRouter(router)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.handleAnthropicMessages(w, req)

			turn, err := ReadLastTurn(srv.cfg.WorkspaceRoot, srv.process.SessionID())
			if err != nil {
				t.Fatalf("read turn: %v", err)
			}
			if turn == nil {
				t.Fatal("no turn record stored")
			}
			u := turn.Usage
			if u.InputTokens != 22 || u.OutputTokens != 1 {
				t.Fatalf("in/out = %d/%d; want 22/1", u.InputTokens, u.OutputTokens)
			}
			if u.CacheReadTokens != 10000 || u.CacheWriteTokens != 500 {
				t.Fatalf("cache read/write = %d/%d; want 10000/500", u.CacheReadTokens, u.CacheWriteTokens)
			}
			if u.RequestID == "" {
				t.Fatal("request_id not recorded on turn usage")
			}
			if u.WallMS < 0 {
				t.Fatalf("wall_ms = %d; want >= 0", u.WallMS)
			}
			if u.TotalTokens != 22+1+10000+500 {
				t.Fatalf("total = %d; want %d", u.TotalTokens, 22+1+10000+500)
			}
		})
	}
}

// errProvider fails every completion, simulating a provider-error turn.
type errProvider struct{ StubProvider }

func (p *errProvider) Complete(context.Context, *CompletionRequest) (*CompletionResponse, error) {
	return nil, errors.New("boom")
}

// An errored turn has no provider usage, so its turn.completed event must keep
// omitting the "usage" key (request_id/wall_ms are stamped only with usage).
func TestErroredTurnOmitsUsageKey(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(&errProvider{StubProvider: *NewStubProvider("stub", "ok")})
	srv.SetRouter(router)

	body := `{"model":"claude","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	srv.handleAnthropicMessages(httptest.NewRecorder(), req)

	evs, err := readTurnCompletedEvents(srv.cfg.WorkspaceRoot, srv.process.SessionID())
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("turn.completed events = %d; want 1", len(evs))
	}
	d := evs[0].HashedPayload.Data
	if d["status"] != "error" {
		t.Fatalf("status = %v; want error", d["status"])
	}
	if _, ok := d["usage"]; ok {
		t.Fatalf("errored turn emitted usage key: %v", d["usage"])
	}
}
