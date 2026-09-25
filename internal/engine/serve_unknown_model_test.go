// serve_unknown_model_test.go — kernel-boundary guard for unknown model ids.
//
// Regression for the opaque-500 bug: a non-empty `model` string that matches
// NONE of (intent alias / "local" / provider name / provider-served model)
// used to fall through to the default provider (claude-oauth), which POSTed
// the bogus id to Anthropic and surfaced a 404 not_found_error wrapped as an
// HTTP 500. The guard rejects unknown models at the gateway boundary with a
// 400 + the available-model menu, and must NOT regress the empty-string,
// known-alias, or provider-served-model paths.
package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// postChat issues a /v1/chat/completions request against srv.handleChat and
// returns the recorder.
func postChat(t *testing.T, srv *Server, model string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"model":` + jsonQuote(model) + `,"messages":[{"role":"user","content":"hi"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)
	return w
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// TestGateway_PolicyDeniedModel_Returns403 is the regression for the Incident:
// a composite model id routed to a denylisted provider (openrouter serving a
// first-party Anthropic id) must be rejected at the gateway boundary with HTTP
// 403 policy_denied BEFORE any provider call — not forwarded to an aggregator
// that re-bills an already-paid Max-tier model.
func TestGateway_PolicyDeniedModel_Returns403(t *testing.T) {
	t.Parallel()
	SetProviderModelDeny(nil)

	ccStub := NewStubProvider("claude-oauth", "should not be reached")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)
	srv := newTestServerWithRouter(t, router)

	w := postChat(t, srv, "openrouter/anthropic/claude-fable-5.1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "policy_denied") {
		t.Errorf("body missing error.type=policy_denied: %s", body)
	}
	if !strings.Contains(body, "allowed_alternative") || !strings.Contains(body, "fable") {
		t.Errorf("body missing allowed_alternative=fable: %s", body)
	}
	if !strings.Contains(body, "openrouter/anthropic/claude-fable-5.1") {
		t.Errorf("body does not name the denied model: %s", body)
	}
	if ccStub.lastRequest != nil {
		t.Error("denied model reached the provider; want short-circuit before any call")
	}
}

// TestGateway_PolicyDenied_DoesNotBlockAllowedModels guards against
// over-blocking: the intent alias "fable" still resolves to claude-oauth and is
// served, and an openrouter id for a non-Anthropic model passes the deny check
// (it may still fail unknown-model admission if openrouter isn't registered —
// assert only on the deny outcome: NOT 403 policy_denied).
func TestGateway_PolicyDenied_DoesNotBlockAllowedModels(t *testing.T) {
	t.Parallel()
	SetProviderModelDeny(nil)

	ccStub := NewStubProvider("claude-oauth", "ok")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)
	srv := newTestServerWithRouter(t, router)

	// "fable" is an intent alias → claude-oauth. Not denied by policy.
	w := postChat(t, srv, "fable")
	if w.Code != http.StatusOK {
		t.Fatalf("fable: status = %d; want 200 (not denied)", w.Code)
	}
	if ccStub.lastRequest == nil {
		t.Error("fable: provider was not called")
	}

	// openrouter/deepseek/... passes the deny table (non-Anthropic id). It may
	// still be rejected as unknown admission since openrouter isn't registered
	// here — assert only that the deny outcome is NOT policy_denied 403.
	ccStub.lastRequest = nil
	w = postChat(t, srv, "openrouter/deepseek/deepseek-v4-flash-0731")
	if w.Code == http.StatusForbidden {
		t.Error("openrouter/deepseek/deepseek-v4-flash-0731: got 403 policy_denied; want not denied by policy")
	}
	if reason, denied := DeniedByPolicy("openrouter", "deepseek/deepseek-v4-flash-0731"); denied {
		t.Errorf("DeniedByPolicy(openrouter, deepseek/...): denied=%v reason=%q; want not denied", denied, reason)
	}
}

// TestGateway_UnknownModel_Returns400 is the core failing-first assertion:
// a bogus model id must be rejected with HTTP 400 before any provider call,
// and the body must name the unknown model and list the available menu.
func TestGateway_UnknownModel_Returns400(t *testing.T) {
	t.Parallel()

	ccStub := NewStubProvider("claude-oauth", "should not be reached")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)

	srv := newTestServerWithRouter(t, router)

	for _, bogus := range []string{"gpt-4", "totally-bogus-model-xyz"} {
		w := postChat(t, srv, bogus)
		if w.Code != http.StatusBadRequest {
			t.Errorf("model=%q: status = %d; want 400", bogus, w.Code)
		}
		// The default provider must NOT have been invoked.
		if ccStub.lastRequest != nil {
			t.Errorf("model=%q: default provider was called; want guard to short-circuit", bogus)
			ccStub.lastRequest = nil // reset for next iteration
		}
		// The body should name the unknown model and advertise the menu.
		bodyStr := w.Body.String()
		if !strings.Contains(bodyStr, bogus) {
			t.Errorf("model=%q: body does not name the unknown model: %s", bogus, bodyStr)
		}
		if !strings.Contains(bodyStr, "foreground") {
			t.Errorf("model=%q: body does not advertise the available menu (missing 'foreground'): %s", bogus, bodyStr)
		}
	}
}

// TestGateway_EmptyModel_StillDefaults verifies the "" path is unchanged:
// default routing reaches the configured provider and returns 200.
func TestGateway_EmptyModel_StillDefaults(t *testing.T) {
	t.Parallel()

	ccStub := NewStubProvider("claude-oauth", "default response")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)

	srv := newTestServerWithRouter(t, router)
	w := postChat(t, srv, "")
	if w.Code != http.StatusOK {
		t.Errorf("empty model: status = %d; want 200", w.Code)
	}
	if ccStub.lastRequest == nil {
		t.Error("empty model: default provider was not called")
	}
}

// TestGateway_KnownAlias_StillResolves verifies a known intent alias still
// routes to its provider (no false 400).
func TestGateway_KnownAlias_StillResolves(t *testing.T) {
	t.Parallel()

	ccStub := NewStubProvider("claude-oauth", "alias response")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)

	srv := newTestServerWithRouter(t, router)
	w := postChat(t, srv, "foreground")
	if w.Code != http.StatusOK {
		t.Errorf("foreground: status = %d; want 200", w.Code)
	}
	if ccStub.lastRequest == nil {
		t.Error("foreground: provider was not called")
	}
}

// TestGateway_ProviderServedModel_StillResolves verifies a model id served by
// a registered provider (ProviderForModel match) still resolves and is NOT
// rejected by the guard.
func TestGateway_ProviderServedModel_StillResolves(t *testing.T) {
	t.Parallel()

	served := newModelStub("my-provider", "served-model-v2", "served response")
	router := NewSimpleRouter(RoutingConfig{Default: "my-provider"})
	router.RegisterProvider(served)

	srv := newTestServerWithRouter(t, router)
	w := postChat(t, srv, "served-model-v2")
	if w.Code != http.StatusOK {
		t.Errorf("served-model-v2: status = %d; want 200", w.Code)
	}
	if served.lastRequest == nil {
		t.Error("served-model-v2: provider was not called")
	}
}

// TestGateway_ProviderName_StillResolves verifies that targeting a provider by
// its registered name (ProviderForName match) still resolves.
func TestGateway_ProviderName_StillResolves(t *testing.T) {
	t.Parallel()

	p := NewStubProvider("my-provider", "by-name response")
	router := NewSimpleRouter(RoutingConfig{Default: "my-provider"})
	router.RegisterProvider(p)

	srv := newTestServerWithRouter(t, router)
	w := postChat(t, srv, "my-provider")
	if w.Code != http.StatusOK {
		t.Errorf("provider-name: status = %d; want 200", w.Code)
	}
	if p.lastRequest == nil {
		t.Error("provider-name: provider was not called")
	}
}

// TestIsKnownModel_NilRouterUnknownStringNotAGate guards invariant #2: the
// dispatch (nil-router) path must NOT be hardened into an error path by this
// change. ResolveModelRequest(nil, <unknown>, ...) must still fall through to an
// empty ModelResolution{} (no 400, no panic) — IsKnownModel is only a hard gate
// on the gateway path where a live router is always present. We assert both the
// IsKnownModel verdict (false: unverifiable without a router) AND that the
// underlying ResolveModelRequest contract is unchanged for the nil-router case.
func TestIsKnownModel_NilRouterUnknownStringNotAGate(t *testing.T) {
	t.Parallel()

	// "" is always known (default routing is valid) regardless of router.
	if !IsKnownModel(nil, "") {
		t.Error(`IsKnownModel(nil, "") = false; want true (empty = default routing)`)
	}
	// A static alias is knowable without a router.
	if !IsKnownModel(nil, "foreground") {
		t.Error(`IsKnownModel(nil, "foreground") = false; want true (static alias)`)
	}
	// An unknown string is *unverifiable* without a router → false. This is why
	// the dispatch (nil-router) path must NOT treat IsKnownModel as a hard gate.
	if IsKnownModel(nil, "totally-bogus-model-xyz") {
		t.Error(`IsKnownModel(nil, "totally-bogus-model-xyz") = true; want false (unverifiable, no router)`)
	}

	// Invariant #2 contract: the nil-router resolve path still degrades to an
	// empty ModelResolution{} for an unknown string — no error, no panic.
	res := ResolveModelRequest(nil, "totally-bogus-model-xyz", "req-nilrouter")
	if res.PreferProvider != "" || res.ModelOverride != "" || res.InjectKernelTools {
		t.Errorf("nil-router unknown model: want zero ModelResolution{}, got %+v", res)
	}
}

// newModelStub returns a StubProvider whose Model() reports modelID, so
// ProviderForModel(modelID) resolves to this provider.
func newModelStub(name, modelID, response string) *StubProvider {
	s := NewStubProvider(name, response)
	s.model = modelID
	return s
}
