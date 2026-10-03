package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// bigImageChatBody builds an OpenAI chat request whose JSON is at least
// minBytes long, carried in a base64 image_url part — the shape Hermes sends
// for long multimodal conversations.
func bigImageChatBody(t *testing.T, minBytes int) []byte {
	t.Helper()
	payload := strings.Repeat("A", minBytes)
	req := map[string]any{
		"model":  "local",
		"stream": false,
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "what is in this screenshot?"},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64," + payload}},
				},
			},
		},
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func bigAnthropicBody(t *testing.T, minBytes int) []byte {
	t.Helper()
	req := map[string]any{
		"model":      "claude",
		"max_tokens": 16,
		"stream":     false,
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "describe"},
					map[string]any{"type": "image", "source": map[string]any{
						"type": "base64", "media_type": "image/png", "data": strings.Repeat("A", minBytes),
					}},
				},
			},
		},
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReadLimitedBody(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		size    int
		limit   int64
		tooBig  bool
		wantLen int
	}{
		{"under", 10, 16, false, 10},
		{"exact", 16, 16, false, 16},
		{"over-by-one", 17, 16, true, 0},
		{"way-over", 1000, 16, true, 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(make([]byte, tc.size)))
			w := httptest.NewRecorder()
			got, err := readLimitedBody(w, r, tc.limit)
			if tc.tooBig {
				if !errors.Is(err, errRequestBodyTooLarge) {
					t.Fatalf("err = %v; want errRequestBodyTooLarge", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if len(got) != tc.wantLen {
				t.Fatalf("len = %d; want %d (body must not be truncated)", len(got), tc.wantLen)
			}
		})
	}
}

func TestHandleChat_BodyOver4MBUnderLimitParses(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	stub := NewStubProvider("stub", "ok")
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(stub)
	srv.SetRouter(router)

	body := bigImageChatBody(t, 6<<20) // > old 4 MB cap, < new cap
	if int64(len(body)) >= maxInferenceRequestBodyBytes {
		t.Fatalf("test body %d not under limit %d", len(body), maxInferenceRequestBodyBytes)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body: %.300s", w.Code, w.Body.String())
	}
	if stub.lastRequest == nil {
		t.Fatal("provider never received the request")
	}
}

func TestHandleChat_BodyOverLimitReturns413(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	stub := NewStubProvider("stub", "ok")
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(stub)
	srv.SetRouter(router)

	body := bigImageChatBody(t, int(maxInferenceRequestBodyBytes))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d; want 413; body: %.300s", w.Code, w.Body.String())
	}
	var resp struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("413 body is not JSON: %v: %s", err, w.Body.String())
	}
	if resp.Error.Type != "request_too_large" || !strings.Contains(resp.Error.Message, "too large") {
		t.Fatalf("unexpected error payload: %+v", resp.Error)
	}
	if stub.lastRequest != nil {
		t.Fatal("provider must not be called for an oversized body")
	}
}

func TestHandleChat_MalformedSmallBodyReturns400(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(NewStubProvider("stub", "ok"))
	srv.SetRouter(router)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"local","messages":[`))
	w := httptest.NewRecorder()
	srv.handleChat(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400", w.Code)
	}
}

func TestHandleAnthropicMessages_BodyOver4MBUnderLimitParses(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(NewStubProvider("stub", "hello"))
	srv.SetRouter(router)

	body := bigAnthropicBody(t, 6<<20)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleAnthropicMessages(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body: %.300s", w.Code, w.Body.String())
	}
}

func TestHandleAnthropicMessages_BodyOverLimitReturns413(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(NewStubProvider("stub", "hello"))
	srv.SetRouter(router)

	body := bigAnthropicBody(t, int(maxInferenceRequestBodyBytes))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleAnthropicMessages(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d; want 413; body: %.300s", w.Code, w.Body.String())
	}
	var resp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("413 body is not JSON: %v: %s", err, w.Body.String())
	}
	if resp.Type != "error" || resp.Error.Type != "request_too_large" {
		t.Fatalf("unexpected Anthropic error payload: %+v", resp)
	}
}

func TestHandleAnthropicMessages_MalformedSmallBodyReturns400(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	router := NewSimpleRouter(RoutingConfig{Default: "stub"})
	router.RegisterProvider(NewStubProvider("stub", "hello"))
	srv.SetRouter(router)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":`))
	w := httptest.NewRecorder()
	srv.handleAnthropicMessages(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400", w.Code)
	}
}

func TestConfigMutation_BodyOverLimitReturns413(t *testing.T) {
	t.Parallel()
	handler := newConfigGateTestServer(t, true)
	big := `{"patch":"` + strings.Repeat("x", int(maxConfigRequestBodyBytes)) + `"}`
	for _, tc := range []struct{ method, path string }{
		{http.MethodPatch, "/v1/config"},
		{http.MethodPost, "/v1/config/rollback"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(big))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("%s %s: status=%d; want 413; body=%.200s", tc.method, tc.path, rec.Code, rec.Body.String())
		}
	}
}
