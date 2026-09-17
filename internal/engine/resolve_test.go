// resolve_test.go — tests for ResolveModelRequest (G1) and the /v1/models
// G2 both-menu.
//
// G1 tests:
//   - Gateway regression: every pre-existing model string resolves identically.
//   - New intent aliases: foreground → claude-oauth/sonnet; deliberation → claude-oauth/opus.
//   - Dispatch model-string path: claude-opus-4-7 / deliberation / opus all reach
//     claude-oauth+override when the provider is configured.
//   - Dispatch with no model uses process_state_routing (via existing path).
//   - Dispatch with e4b / 26b uses legacy path.
//
// G2 tests:
//   - /v1/models returns the both-menu in declared order with tier+description.
//   - Intent aliases are present with correct tiers.
//   - eclipse-26b absent when provider not configured.
//   - eclipse-26b present when eclipse or lmstudio provider is registered.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── G1: ResolveModelRequest unit tests ───────────────────────────────────────

// stubRouter is a minimal Router implementation for resolver tests.
// It only implements the three methods ResolveModelRequest calls.
type stubRouter struct {
	byName  map[string]string // name → name
	byModel map[string]string // model → provider name
	local   []string          // providers with IsLocal=true, in order
	provs   []Provider        // providers visited by RangeProviders (opt-in)
}

func newStubRouter() *stubRouter {
	return &stubRouter{
		byName:  make(map[string]string),
		byModel: make(map[string]string),
	}
}

func (r *stubRouter) addProvider(name string, model string, isLocal bool) *stubRouter {
	r.byName[name] = name
	if model != "" {
		r.byModel[model] = name
	}
	if isLocal {
		r.local = append(r.local, name)
	}
	return r
}

// Router interface stubs — only ProviderForName / ProviderForModel /
// FirstLocalProvider are exercised by ResolveModelRequest.
func (r *stubRouter) ProviderForName(name string) (string, bool) {
	n, ok := r.byName[name]
	return n, ok
}
func (r *stubRouter) ProviderForModel(model string) (string, bool) {
	n, ok := r.byModel[model]
	return n, ok
}
func (r *stubRouter) FirstLocalProvider() (string, bool) {
	if len(r.local) > 0 {
		return r.local[0], true
	}
	return "", false
}

// RangeProviders visits the stub's registered providers. The stub tracks only
// provider names/models, not Provider values, so it visits nothing by default;
// tests that need live enumeration wire providers via provs (see below).
func (r *stubRouter) RangeProviders(fn func(p Provider)) {
	for _, p := range r.provs {
		fn(p)
	}
}

// Unused Router interface methods — satisfy the interface without panicking.
func (r *stubRouter) Route(_ context.Context, _ *CompletionRequest) (Provider, *RoutingDecision, error) {
	return nil, nil, nil
}
func (r *stubRouter) RegisterProvider(_ Provider) {}
func (r *stubRouter) DeregisterProvider(_ string) {}
func (r *stubRouter) Stats() RouterStats          { return RouterStats{} }

// ── Regression: existing strings resolve identically ─────────────────────────

func TestResolveModelRequest_EmptyModel(t *testing.T) {
	t.Parallel()
	r := newStubRouter()
	res := ResolveModelRequest(r, "", "req-0")
	if res.PreferProvider != "" || res.ModelOverride != "" || res.InjectKernelTools {
		t.Errorf("empty model: want zero ModelResolution, got %+v", res)
	}
}

func TestResolveModelRequest_EmptyModelNilRouter(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "", "req-0")
	if res.PreferProvider != "" || res.ModelOverride != "" || res.InjectKernelTools {
		t.Errorf("empty model nil router: want zero ModelResolution, got %+v", res)
	}
}

func TestResolveModelRequest_Claude(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "claude", "req-1")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("claude: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "" {
		t.Errorf("claude: ModelOverride = %q; want empty", res.ModelOverride)
	}
}

func TestResolveModelRequest_Codex(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "codex", "req-2")
	if res.PreferProvider != "codex" {
		t.Errorf("codex: PreferProvider = %q; want codex", res.PreferProvider)
	}
}

func TestResolveModelRequest_OllamaInjectTools(t *testing.T) {
	t.Parallel()
	// The "ollama" alias is a convenience spelling that now routes to the live
	// local backend (lmstudio-darkstar) after PR #417 decommissioned Ollama.
	res := ResolveModelRequest(nil, "ollama", "req-3")
	if res.PreferProvider != "lmstudio-darkstar" {
		t.Errorf("ollama: PreferProvider = %q; want lmstudio-darkstar", res.PreferProvider)
	}
	if !res.InjectKernelTools {
		t.Error("ollama: InjectKernelTools should be true")
	}
}

func TestResolveModelRequest_KernelAgentInjectTools(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "kernel-agent", "req-4")
	if res.PreferProvider != "lmstudio-darkstar" {
		t.Errorf("kernel-agent: PreferProvider = %q; want lmstudio-darkstar", res.PreferProvider)
	}
	if !res.InjectKernelTools {
		t.Error("kernel-agent: InjectKernelTools should be true")
	}
}

func TestResolveModelRequest_Local_PrefersLMStudioDarkstar(t *testing.T) {
	t.Parallel()
	// When both the live default (lmstudio-darkstar) and a legacy "ollama"
	// provider are registered, "local" must prefer lmstudio-darkstar.
	r := newStubRouter().
		addProvider("lmstudio-darkstar", "", true).
		addProvider("ollama", "", true)
	res := ResolveModelRequest(r, "local", "req-5b")
	if res.PreferProvider != "lmstudio-darkstar" {
		t.Errorf("local: PreferProvider = %q; want lmstudio-darkstar", res.PreferProvider)
	}
}

func TestResolveModelRequest_Local_WithOllama(t *testing.T) {
	t.Parallel()
	// An install that still declares an "ollama" provider (no lmstudio-darkstar)
	// keeps its behavior: "local" falls back to it.
	r := newStubRouter().addProvider("ollama", "", true)
	res := ResolveModelRequest(r, "local", "req-5")
	if res.PreferProvider != "ollama" {
		t.Errorf("local: PreferProvider = %q; want ollama", res.PreferProvider)
	}
}

func TestResolveModelRequest_Local_FallsToFirstLocal(t *testing.T) {
	t.Parallel()
	r := newStubRouter().addProvider("my-local", "", true)
	res := ResolveModelRequest(r, "local", "req-6")
	if res.PreferProvider != "my-local" {
		t.Errorf("local no-ollama: PreferProvider = %q; want my-local", res.PreferProvider)
	}
}

func TestResolveModelRequest_Local_NoLocalProvider(t *testing.T) {
	t.Parallel()
	// No local providers registered → returns zero (default routing + slog warning).
	r := newStubRouter().addProvider("cloud", "", false)
	res := ResolveModelRequest(r, "local", "req-7")
	if res.PreferProvider != "" {
		t.Errorf("local no-locals: PreferProvider = %q; want empty", res.PreferProvider)
	}
}

func TestResolveModelRequest_Local_NilRouter(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "local", "req-8")
	// nil router: can't resolve local → zero resolution.
	if res.PreferProvider != "" {
		t.Errorf("local nil router: PreferProvider = %q; want empty", res.PreferProvider)
	}
}

func TestResolveModelRequest_ProviderName(t *testing.T) {
	t.Parallel()
	r := newStubRouter().addProvider("mlx-gemma", "gemma4:27b", false)
	res := ResolveModelRequest(r, "mlx-gemma", "req-9")
	if res.PreferProvider != "mlx-gemma" {
		t.Errorf("provider name: PreferProvider = %q; want mlx-gemma", res.PreferProvider)
	}
	// Provider name match: no ModelOverride (provider uses its configured model).
	if res.ModelOverride != "" {
		t.Errorf("provider name: ModelOverride = %q; want empty", res.ModelOverride)
	}
}

func TestResolveModelRequest_ModelID_NoMatchingProvider(t *testing.T) {
	t.Parallel()
	r := newStubRouter()
	res := ResolveModelRequest(r, "gpt-5.4", "req-10")
	// Unknown model id: ModelOverride set, PreferProvider empty.
	if res.ModelOverride != "gpt-5.4" {
		t.Errorf("unknown model: ModelOverride = %q; want gpt-5.4", res.ModelOverride)
	}
	if res.PreferProvider != "" {
		t.Errorf("unknown model: PreferProvider = %q; want empty", res.PreferProvider)
	}
}

func TestResolveModelRequest_ModelID_WithMatchingProvider(t *testing.T) {
	t.Parallel()
	r := newStubRouter()
	r.byModel["my-model-v2"] = "my-provider"
	r.byName["my-provider"] = "my-provider"
	res := ResolveModelRequest(r, "my-model-v2", "req-11")
	if res.PreferProvider != "my-provider" {
		t.Errorf("model id match: PreferProvider = %q; want my-provider", res.PreferProvider)
	}
	if res.ModelOverride != "my-model-v2" {
		t.Errorf("model id match: ModelOverride = %q; want my-model-v2", res.ModelOverride)
	}
}

// ── G1: new intent aliases ────────────────────────────────────────────────────

func TestResolveModelRequest_Foreground(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "foreground", "req-fg")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("foreground: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-sonnet-5" {
		t.Errorf("foreground: ModelOverride = %q; want claude-sonnet-5", res.ModelOverride)
	}
	if res.InjectKernelTools {
		t.Error("foreground: InjectKernelTools should be false")
	}
}

func TestResolveModelRequest_Deliberation(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "deliberation", "req-delib")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("deliberation: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-opus-4-8" {
		t.Errorf("deliberation: ModelOverride = %q; want claude-opus-4-8", res.ModelOverride)
	}
}

// Previous-generation raw claude ids: on the DISPATCH path (nil router) they
// resolve via dispatchFrontierAliases (the #430 protection); on the GATEWAY
// path (live router) they resolve DYNAMICALLY via resolveLiveCatalog →
// frontierProviderName, like every other claude id — a claude-code-only
// install must route them to the provider it actually has, not a hardcoded
// claude-oauth pin.
func TestResolveModelRequest_SonnetModelID(t *testing.T) {
	t.Parallel()
	// Dispatch path (nil router): static table keeps the pin.
	res := ResolveModelRequest(nil, "claude-sonnet-4-6", "req-s")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("dispatch claude-sonnet-4-6: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-sonnet-4-6" {
		t.Errorf("dispatch claude-sonnet-4-6: ModelOverride = %q; want claude-sonnet-4-6", res.ModelOverride)
	}
	// Gateway path (live router, claude-code-only frontier): dynamic resolution
	// must route to the registered frontier provider, not a static pin.
	router := newStubRouter().addProvider("claude-code", "", false)
	res = ResolveModelRequest(router, "claude-sonnet-4-6", "req-s2")
	if res.PreferProvider != "claude-code" {
		t.Errorf("gateway claude-sonnet-4-6: PreferProvider = %q; want claude-code (dynamic frontier)", res.PreferProvider)
	}
	if res.ModelOverride != "claude-sonnet-4-6" {
		t.Errorf("gateway claude-sonnet-4-6: ModelOverride = %q; want claude-sonnet-4-6", res.ModelOverride)
	}
}

func TestResolveModelRequest_OpusModelID(t *testing.T) {
	t.Parallel()
	// Dispatch path (nil router): static table keeps the pin.
	res := ResolveModelRequest(nil, "claude-opus-4-7", "req-o")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("dispatch claude-opus-4-7: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-opus-4-7" {
		t.Errorf("dispatch claude-opus-4-7: ModelOverride = %q; want claude-opus-4-7", res.ModelOverride)
	}
	// Gateway path (live router with claude-oauth registered): dynamic
	// resolution still lands on claude-oauth via frontierProviderName.
	router := newStubRouter().addProvider("claude-oauth", "", false)
	res = ResolveModelRequest(router, "claude-opus-4-7", "req-o2")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("gateway claude-opus-4-7: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-opus-4-7" {
		t.Errorf("gateway claude-opus-4-7: ModelOverride = %q; want claude-opus-4-7", res.ModelOverride)
	}
}

// TestIntentAliases_NoRawClaudeIDs is the regrowth guard for the alias-shadowing
// defect: a raw "claude-*" model-id key in intentAliases is consulted BEFORE
// resolveLiveCatalog in ResolveModelRequest, so it statically shadows the
// dynamic gateway-path resolution that claude-code-only installs depend on
// (see TestHandleModels_HaikuAdmitsUnderClaudeCodeOnly). Raw claude ids for the
// nil-router dispatch path belong in dispatchFrontierAliases instead.
func TestIntentAliases_NoRawClaudeIDs(t *testing.T) {
	t.Parallel()
	for key := range intentAliases {
		if strings.HasPrefix(key, "claude-") {
			t.Errorf("intentAliases contains raw claude model id %q; raw claude-* ids must resolve dynamically via resolveLiveCatalog (gateway) or live in dispatchFrontierAliases (dispatch)", key)
		}
	}
}

func TestResolveModelRequest_OpusShortAlias(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "opus", "req-o2")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("opus: PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != "claude-opus-4-8" {
		t.Errorf("opus: ModelOverride = %q; want claude-opus-4-8", res.ModelOverride)
	}
}

// ── G1: dispatch model-string path integration ───────────────────────────────
//
// These tests exercise the gateway's handleChat + ResolveModelRequest to verify
// the new aliases work end-to-end without touching the dispatch harness directly
// (which would require a live LLM). The relevant assertion is that the provider
// stub registered as "claude-oauth" receives the request when model=foreground or
// model=deliberation.

func TestGateway_ForegroundRoutes_ToClaudeCode(t *testing.T) {
	t.Parallel()

	ccStub := NewStubProvider("claude-oauth", "cc response")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)

	srv := newTestServerWithRouter(t, router)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"foreground","messages":[{"role":"user","content":"hello"}],"stream":false}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("foreground: status = %d; want 200", w.Code)
	}
	if ccStub.lastRequest == nil {
		t.Error("foreground: claude-oauth stub not called")
	}
	// ModelOverride should be forwarded to the provider.
	if ccStub.lastRequest != nil && ccStub.lastRequest.ModelOverride != "claude-sonnet-5" {
		t.Errorf("foreground: ModelOverride = %q; want claude-sonnet-5",
			ccStub.lastRequest.ModelOverride)
	}
}

func TestGateway_DeliberationRoutes_ToClaudeCode(t *testing.T) {
	t.Parallel()

	ccStub := NewStubProvider("claude-oauth", "cc opus response")
	router := NewSimpleRouter(RoutingConfig{Default: "claude-oauth"})
	router.RegisterProvider(ccStub)

	srv := newTestServerWithRouter(t, router)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"deliberation","messages":[{"role":"user","content":"think hard"}],"stream":false}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleChat(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("deliberation: status = %d; want 200", w.Code)
	}
	if ccStub.lastRequest == nil {
		t.Error("deliberation: claude-oauth stub not called")
	}
	if ccStub.lastRequest != nil && ccStub.lastRequest.ModelOverride != "claude-opus-4-8" {
		t.Errorf("deliberation: ModelOverride = %q; want claude-opus-4-8",
			ccStub.lastRequest.ModelOverride)
	}
}

// ── G2: /v1/models both-menu ──────────────────────────────────────────────────

// modelsResponseModel mirrors the JSON shape of a model entry in the response.
type modelsResponseModel struct {
	ID            string   `json:"id"`
	OwnedBy       string   `json:"owned_by"`
	Tier          string   `json:"tier"`
	Description   string   `json:"description"`
	ContextLength int      `json:"context_length"`
	Capabilities  []string `json:"capabilities"`
}

type modelsResponse struct {
	Object string                `json:"object"`
	Data   []modelsResponseModel `json:"data"`
}

func fetchModels(t *testing.T, srv *Server) modelsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	srv.handleModels(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("handleModels: status = %d; want 200", w.Code)
	}
	var resp modelsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode /v1/models: %v", err)
	}
	return resp
}

func TestModelsMenu_OrderAndTier(t *testing.T) {
	t.Parallel()

	// Wire a router with both a frontier provider (anthropic) and a local
	// provider (ollama stub) so all tiers appear. Eclipse is intentionally
	// absent to keep the assertion list stable.
	frontierStub := newCloudStub("anthropic", "frontier response")
	localStub := NewStubProvider("ollama", "local response") // IsLocal = true by default
	router := NewSimpleRouter(RoutingConfig{Default: "anthropic"})
	router.RegisterProvider(frontierStub)
	router.RegisterProvider(localStub)

	srv := newTestServerWithRouter(t, router)
	resp := fetchModels(t, srv)

	// Must have at least 5 entries (2 frontier aliases + 1 local alias + 2 raw ids).
	if len(resp.Data) < 5 {
		t.Fatalf("expected >= 5 models, got %d: %+v", len(resp.Data), resp.Data)
	}

	// Declared order: foreground, deliberation, local, claude-sonnet-4-6, claude-opus-4-7.
	want := []struct{ id, ownedBy, tier string }{
		{"foreground", "cogos", "frontier-managed"},
		{"deliberation", "cogos", "frontier-managed"},
		{"local", "cogos", "local-sovereign"},
		{"claude-sonnet-4-6", "anthropic", "frontier-managed"},
		{"claude-opus-4-7", "anthropic", "frontier-managed"},
	}
	for i, w := range want {
		if i >= len(resp.Data) {
			t.Errorf("missing model at index %d: want %q", i, w.id)
			continue
		}
		m := resp.Data[i]
		if m.ID != w.id {
			t.Errorf("Data[%d].ID = %q; want %q", i, m.ID, w.id)
		}
		if m.OwnedBy != w.ownedBy {
			t.Errorf("Data[%d].OwnedBy = %q; want %q", i, m.OwnedBy, w.ownedBy)
		}
		if m.Tier != w.tier {
			t.Errorf("Data[%d].Tier = %q; want %q", i, m.Tier, w.tier)
		}
	}
}

func TestModelsMenu_IntentAliasesHaveDescriptions(t *testing.T) {
	t.Parallel()

	// Frontier + local provider so all intent aliases appear.
	frontierStub := newCloudStub("anthropic", "frontier response")
	localStub := NewStubProvider("ollama", "local response")
	router := NewSimpleRouter(RoutingConfig{Default: "anthropic"})
	router.RegisterProvider(frontierStub)
	router.RegisterProvider(localStub)

	srv := newTestServerWithRouter(t, router)
	resp := fetchModels(t, srv)

	byID := make(map[string]modelsResponseModel, len(resp.Data))
	for _, m := range resp.Data {
		byID[m.ID] = m
	}

	aliases := []string{"foreground", "deliberation", "local"}
	for _, id := range aliases {
		m, ok := byID[id]
		if !ok {
			t.Errorf("alias %q missing from /v1/models", id)
			continue
		}
		if m.Description == "" {
			t.Errorf("alias %q has empty description", id)
		}
	}
}

func TestModelsMenu_Eclipse26B_AbsentWhenNotConfigured(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t) // no router
	body, _ := json.Marshal(fetchModels(t, srv))

	if bytes.Contains(body, []byte("eclipse-26b")) {
		t.Errorf("eclipse-26b should NOT appear when no eclipse provider is registered; body: %s", body)
	}
}

func TestModelsMenu_Eclipse26B_PresentWhenModelServed(t *testing.T) {
	t.Parallel()

	// A provider that actually SERVES the eclipse-26b model string. This is the
	// only condition under which IsKnownModel(router,"eclipse-26b") is true, so it
	// is the only condition under which the static eclipse-26b id may be emitted
	// (emit ⇔ admit — the admission-parity invariant). A provider merely NAMED
	// "eclipse"/"lmstudio" that serves some other model must NOT surface it (see
	// TestModelsMenu_Eclipse26B_AbsentWhenNameOnly).
	eclipseStub := NewStubProvider("eclipse", "eclipse response")
	eclipseStub.model = "eclipse-26b"
	router := NewSimpleRouter(RoutingConfig{Default: "eclipse"})
	router.RegisterProvider(eclipseStub)

	srv := newTestServerWithRouter(t, router)
	resp := fetchModels(t, srv)

	var found bool
	for _, m := range resp.Data {
		if m.ID == "eclipse-26b" {
			found = true
			if m.Tier != "lan-local" {
				t.Errorf("eclipse-26b tier = %q; want lan-local", m.Tier)
			}
			if m.OwnedBy != "cogos" {
				t.Errorf("eclipse-26b owned_by = %q; want cogos", m.OwnedBy)
			}
		}
	}
	if !found {
		t.Errorf("eclipse-26b model entry not found in response Data: %+v", resp.Data)
	}

	// Admission parity: the advertised id must satisfy IsKnownModel.
	if !IsKnownModel(router, "eclipse-26b") {
		t.Error("eclipse-26b advertised but IsKnownModel returned false (advertise-then-reject)")
	}
}

func TestModelsMenu_Eclipse26B_AbsentWhenNameOnly(t *testing.T) {
	t.Parallel()

	// Provider named "lmstudio"/"eclipse" but serving a DIFFERENT model — the
	// pre-existing name-detection would have emitted eclipse-26b here, but the
	// kernel boundary would then 400 a client selecting it (IsKnownModel false).
	// Emit-gate is now ProviderForModel("eclipse-26b"), so it must NOT appear.
	lmsStub := NewStubProvider("lmstudio", "lms response")
	lmsStub.model = "ornith-1.0-35b"
	router := NewSimpleRouter(RoutingConfig{Default: "lmstudio"})
	router.RegisterProvider(lmsStub)

	srv := newTestServerWithRouter(t, router)
	body, _ := json.Marshal(fetchModels(t, srv))

	if bytes.Contains(body, []byte("eclipse-26b")) {
		t.Errorf("eclipse-26b must NOT be advertised when no provider serves it (IsKnownModel would 400 it); body: %s", body)
	}
	if IsKnownModel(router, "eclipse-26b") {
		t.Error("IsKnownModel(eclipse-26b) should be false when no provider serves that model")
	}
}

func TestModelsMenu_Object_IsListType(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	resp := fetchModels(t, srv)
	if resp.Object != "list" {
		t.Errorf("Object = %q; want list", resp.Object)
	}
}

// ── Availability gating (issue #316) ─────────────────────────────────────────

func TestModelsMenu_FrontierAbsentWhenNoFrontierProvider(t *testing.T) {
	t.Parallel()

	// Only a local provider — no anthropic/claude-oauth.
	localStub := NewStubProvider("ollama", "local response")
	router := NewSimpleRouter(RoutingConfig{Default: "ollama"})
	router.RegisterProvider(localStub)
	srv := newTestServerWithRouter(t, router)

	body, _ := json.Marshal(fetchModels(t, srv))

	for _, id := range []string{"claude-sonnet-4-6", "claude-opus-4-7", "foreground", "deliberation"} {
		if bytes.Contains(body, []byte(id)) {
			t.Errorf("%q should NOT appear when no frontier provider is registered; body: %s", id, body)
		}
	}
	// local alias should still appear.
	if !bytes.Contains(body, []byte("local")) {
		t.Errorf(`"local" should appear when a local provider is registered; body: %s`, body)
	}
}

func TestModelsMenu_LocalAbsentWhenNoLocalProvider(t *testing.T) {
	t.Parallel()

	// Only a frontier provider — no local backend.
	frontierStub := newCloudStub("anthropic", "frontier response")
	router := NewSimpleRouter(RoutingConfig{Default: "anthropic"})
	router.RegisterProvider(frontierStub)
	srv := newTestServerWithRouter(t, router)

	body, _ := json.Marshal(fetchModels(t, srv))

	if bytes.Contains(body, []byte(`"local"`)) {
		t.Errorf(`"local" model should NOT appear when no local provider is registered; body: %s`, body)
	}
	// Frontier entries should still appear.
	for _, id := range []string{"claude-sonnet-4-6", "claude-opus-4-7", "foreground", "deliberation"} {
		if !bytes.Contains(body, []byte(id)) {
			t.Errorf("%q should appear when frontier provider is registered; body: %s", id, body)
		}
	}
}

func TestModelsMenu_EmptyWhenNoRouter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t) // no router at all
	resp := fetchModels(t, srv)

	if len(resp.Data) != 0 {
		ids := make([]string, len(resp.Data))
		for i, m := range resp.Data {
			ids[i] = m.ID
		}
		t.Errorf("expected empty model list when no providers are configured; got %v", ids)
	}
}

// TestModelsMenu_AliasesMatchResolver verifies that every static intent alias
// in the /v1/models menu is also known to ResolveModelRequest (alias coherence).
// The server router must advertise the aliases in the first place: with
// availability gating (issue #316) handleModels emits nothing when no provider
// is registered, so frontier + local stubs are wired so the menu is non-empty
// and the test actually exercises alias↔resolver coherence (not vacuously).
// "local" is router-dynamic (requires FirstLocalProvider walk); "eclipse-26b"
// is hardware-gated and absent from the static alias table by design.
func TestModelsMenu_AliasesMatchResolver(t *testing.T) {
	t.Parallel()

	// Frontier + local provider so the full cogos-owned alias set is advertised.
	frontierStub := newCloudStub("anthropic", "frontier response")
	localStub := NewStubProvider("ollama", "local response")
	router := NewSimpleRouter(RoutingConfig{Default: "anthropic"})
	router.RegisterProvider(frontierStub)
	router.RegisterProvider(localStub)

	srv := newTestServerWithRouter(t, router)
	resp := fetchModels(t, srv)

	// Guard against vacuous pass: the menu must contain the cogos-owned aliases.
	var cogosAliases int
	for _, m := range resp.Data {
		if m.OwnedBy == "cogos" && m.ID != "eclipse-26b" {
			cogosAliases++
		}
	}
	if cogosAliases == 0 {
		t.Fatal("no cogos-owned aliases in /v1/models; test would pass vacuously")
	}

	// Build a minimal router that covers every alias the resolver maps:
	// the "local" alias (ollama present, IsLocal) plus the frontier-managed
	// claude aliases routed through the claude-oauth provider.
	r := newStubRouter().
		addProvider("ollama", "", true).
		addProvider("claude-oauth", "", false)

	for _, m := range resp.Data {
		if m.OwnedBy != "cogos" {
			continue
		}
		if m.ID == "eclipse-26b" {
			continue // hardware-gated; not in static alias table by design
		}
		res := ResolveModelRequest(r, m.ID, "test")
		if res.PreferProvider == "" {
			t.Errorf("alias %q in /v1/models does not resolve via ResolveModelRequest (even with stub router)", m.ID)
		}
	}
}

// ── G1: dispatch e4b/26b unchanged ───────────────────────────────────────────

// TestResolveModelRequest_E4B_NotInAliasTable ensures "e4b" falls through
// (not in intentAliases; ResolveModelRequest returns zero with nil router).
func TestResolveModelRequest_E4B_FallsThrough(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "e4b", "req-e4b")
	if res.PreferProvider != "" {
		t.Errorf("e4b: PreferProvider = %q; want empty (fall-through)", res.PreferProvider)
	}
	if res.ModelOverride != "" {
		t.Errorf("e4b: ModelOverride = %q; want empty (fall-through)", res.ModelOverride)
	}
}

func TestResolveModelRequest_26B_FallsThrough(t *testing.T) {
	t.Parallel()
	res := ResolveModelRequest(nil, "26b", "req-26b")
	if res.PreferProvider != "" {
		t.Errorf("26b: PreferProvider = %q; want empty (fall-through)", res.PreferProvider)
	}
}

// ── Live-catalog parity: composite ids + newly-shipped claude ids ────────────
//
// These exercise resolveLiveCatalog through both ResolveModelRequest (routing)
// and IsKnownModel (admission) so the two never diverge — the admission-parity
// invariant that keeps GET /v1/models from advertising an id the boundary 400s.

func TestResolveLiveCatalog_CompositeID_ResolvesAndAdmits(t *testing.T) {
	t.Parallel()

	router := NewSimpleRouter(RoutingConfig{})
	router.RegisterProvider(NewStubProvider("lmstudio-eclipse", "r"))

	const id = "lmstudio-eclipse/ornith-1.0-35b"
	res := ResolveModelRequest(router, id, "t")
	if res.PreferProvider != "lmstudio-eclipse" {
		t.Errorf("composite PreferProvider = %q; want lmstudio-eclipse", res.PreferProvider)
	}
	if res.ModelOverride != "ornith-1.0-35b" {
		t.Errorf("composite ModelOverride = %q; want ornith-1.0-35b", res.ModelOverride)
	}
	if !IsKnownModel(router, id) {
		t.Errorf("composite id %q must be admissible", id)
	}
}

func TestResolveLiveCatalog_CompositeID_UnregisteredPrefixFallsThrough(t *testing.T) {
	t.Parallel()

	// "openrouter" is NOT a registered provider, so an "openrouter/x/y"-style id
	// must NOT be mis-split: it falls through to the generic model-id path
	// (ModelOverride = whole string, no PreferProvider) and IsKnownModel is false.
	router := NewSimpleRouter(RoutingConfig{})
	router.RegisterProvider(NewStubProvider("lmstudio-darkstar", "r"))

	const id = "openrouter/anthropic/claude-3"
	res := ResolveModelRequest(router, id, "t")
	if res.PreferProvider != "" {
		t.Errorf("unregistered-prefix PreferProvider = %q; want empty (fall-through)", res.PreferProvider)
	}
	if res.ModelOverride != id {
		t.Errorf("unregistered-prefix ModelOverride = %q; want whole id %q", res.ModelOverride, id)
	}
	if IsKnownModel(router, id) {
		t.Errorf("unregistered-prefix id %q must NOT admit", id)
	}
}

func TestResolveLiveCatalog_NewClaudeID_RoutesToOAuthAndAdmits(t *testing.T) {
	t.Parallel()

	router := NewSimpleRouter(RoutingConfig{})
	router.RegisterProvider(NewStubProvider("claude-oauth", "r"))

	// A newly-shipped claude id not present in intentAliases.
	const id = "claude-opus-4-9-20260101"
	res := ResolveModelRequest(router, id, "t")
	if res.PreferProvider != "claude-oauth" {
		t.Errorf("new claude id PreferProvider = %q; want claude-oauth", res.PreferProvider)
	}
	if res.ModelOverride != id {
		t.Errorf("new claude id ModelOverride = %q; want %q", res.ModelOverride, id)
	}
	if !IsKnownModel(router, id) {
		t.Errorf("new claude id %q must be admissible", id)
	}
}

func TestResolveLiveCatalog_NewClaudeID_NoFrontierProviderRejects(t *testing.T) {
	t.Parallel()

	// No frontier provider registered → a bare claude id is NOT admissible and
	// does not resolve to a frontier provider.
	router := NewSimpleRouter(RoutingConfig{})
	router.RegisterProvider(NewStubProvider("lmstudio-darkstar", "r"))

	const id = "claude-opus-4-9-20260101"
	if IsKnownModel(router, id) {
		t.Errorf("claude id %q must NOT admit without a frontier provider", id)
	}
	res := ResolveModelRequest(router, id, "t")
	if res.PreferProvider != "" {
		t.Errorf("no-frontier claude id PreferProvider = %q; want empty", res.PreferProvider)
	}
}

func TestFrontierProviderName_MatchesEmitGate(t *testing.T) {
	t.Parallel()

	// claude-code alone must yield a routable frontier provider (Finding-1: the
	// admit gate must match isFrontierConfigured's emit gate, which includes the
	// claude-code name).
	rCC := NewSimpleRouter(RoutingConfig{})
	rCC.RegisterProvider(NewStubProvider("claude-code", "r"))
	if name, ok := frontierProviderName(rCC); !ok || name != "claude-code" {
		t.Errorf("frontierProviderName(claude-code) = (%q,%v); want (claude-code,true)", name, ok)
	}

	// A provider serving claude-sonnet-4-6 under a non-canonical name also
	// qualifies (matches isFrontierConfigured's ProviderForModel branch).
	rModel := NewSimpleRouter(RoutingConfig{})
	sp := NewStubProvider("my-anthropic", "r")
	sp.model = "claude-sonnet-4-6"
	rModel.RegisterProvider(sp)
	if name, ok := frontierProviderName(rModel); !ok || name != "my-anthropic" {
		t.Errorf("frontierProviderName(serves sonnet) = (%q,%v); want (my-anthropic,true)", name, ok)
	}

	// Preference: claude-oauth wins when several are registered.
	rPref := NewSimpleRouter(RoutingConfig{})
	rPref.RegisterProvider(NewStubProvider("claude-code", "r"))
	rPref.RegisterProvider(NewStubProvider("anthropic", "r"))
	rPref.RegisterProvider(NewStubProvider("claude-oauth", "r"))
	if name, ok := frontierProviderName(rPref); !ok || name != "claude-oauth" {
		t.Errorf("frontierProviderName preference = (%q,%v); want (claude-oauth,true)", name, ok)
	}
}

// ── isEclipseConfigured unit tests ───────────────────────────────────────────

func TestIsEclipseConfigured_NilRouter(t *testing.T) {
	t.Parallel()
	if isEclipseConfigured(nil) {
		t.Error("nil router should not report eclipse configured")
	}
}

func TestIsEclipseConfigured_NoEclipse(t *testing.T) {
	t.Parallel()
	r := newStubRouter().addProvider("ollama", "", true)
	if isEclipseConfigured(r) {
		t.Error("no eclipse provider: should return false")
	}
}

func TestIsEclipseConfigured_EclipseName(t *testing.T) {
	t.Parallel()
	r := newStubRouter().addProvider("eclipse", "", false)
	if !isEclipseConfigured(r) {
		t.Error("eclipse provider registered: should return true")
	}
}

func TestIsEclipseConfigured_LMStudioName(t *testing.T) {
	t.Parallel()
	r := newStubRouter().addProvider("lmstudio", "", false)
	if !isEclipseConfigured(r) {
		t.Error("lmstudio provider registered: should return true")
	}
}

func TestIsEclipseConfigured_ModelMatch(t *testing.T) {
	t.Parallel()
	r := newStubRouter()
	r.byModel["eclipse-26b"] = "my-eclipse-provider"
	r.byName["my-eclipse-provider"] = "my-eclipse-provider"
	if !isEclipseConfigured(r) {
		t.Error("eclipse-26b model match: should return true")
	}
}

// Verify that the http.Handler interface is satisfied (compile-time check via
// use in httptest). This function is intentionally unused at runtime.
var _ http.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// TestResolve_Claude5Generation covers the 2026-07 alias refresh: the current-
// generation short aliases (sonnet/opus/fable) and every raw Claude-5-era id a
// dispatch caller may pin. Dispatch resolves with a nil router, so any id
// missing from intentAliases silently falls to the local default (the #430
// wrong-model trap) — these entries are what keep that from happening.
func TestResolve_Claude5Generation(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"sonnet":                    "claude-sonnet-5",
		"opus":                      "claude-opus-4-8",
		"fable":                     "claude-fable-5",
		"haiku":                     "claude-haiku-4-5-20251001",
		"claude-sonnet-5":           "claude-sonnet-5",
		"claude-opus-4-8":           "claude-opus-4-8",
		"claude-fable-5":            "claude-fable-5",
		"claude-haiku-4-5-20251001": "claude-haiku-4-5-20251001",
	}
	for model, want := range cases {
		res := ResolveModelRequest(nil, model, "req-c5")
		if res.PreferProvider != "claude-oauth" {
			t.Errorf("%s: PreferProvider = %q; want claude-oauth", model, res.PreferProvider)
		}
		if res.ModelOverride != want {
			t.Errorf("%s: ModelOverride = %q; want %q", model, res.ModelOverride, want)
		}
	}
}

// ── Per-provider model DENYLIST tests ───────────────────────────────────────

// TestDeniedByPolicy_OpenRouterAnthropic is the table-driven spec of the
// default deny table: OpenRouter must never serve first-party Anthropic model
// ids (the Incident — an operator on Anthropic Max billed via OpenRouter for
// claude-fable), while non-Anthropic ids on openrouter and first-party ids on
// other providers pass.
func TestDeniedByPolicy_OpenRouterAnthropic(t *testing.T) {
	t.Parallel()
	// Deterministic irrespective of test ordering: restore the default table.
	SetProviderModelDeny(nil)

	cases := []struct {
		name     string
		provider string
		model    string
		denied   bool
	}{
		{
			name:     "openrouter composite anthropic id denied",
			provider: "openrouter",
			model:    "anthropic/claude-fable-5.1", // the Incident
			denied:   true,
		},
		{
			name:     "openrouter composite claude- prefix id denied",
			provider: "openrouter",
			model:    "claude-sonnet-5",
			denied:   true,
		},
		{
			name:     "openrouter bare claude id denied",
			provider: "openrouter",
			model:    "claude-fable-5",
			denied:   true,
		},
		{
			name:     "openrouter deepseek id allowed",
			provider: "openrouter",
			model:    "deepseek/deepseek-v4-flash-0731",
			denied:   false,
		},
		{
			name:     "openrouter third-party id allowed",
			provider: "openrouter",
			model:    "meta-llama/llama-3.3-70b-instruct",
			denied:   false,
		},
		{
			name:     "provider not keyed passes",
			provider: "claude-oauth",
			model:    "claude-sonnet-5",
			denied:   false,
		},
		{
			name:     "empty provider never denied",
			provider: "",
			model:    "anthropic/claude-fable-5.1",
			denied:   false,
		},
		{
			name:     "empty model never denied",
			provider: "openrouter",
			model:    "",
			denied:   false,
		},
	}
	for _, tc := range cases {
		reason, denied := DeniedByPolicy(tc.provider, tc.model)
		if denied != tc.denied {
			t.Errorf("%s: DeniedByPolicy(%q, %q) denied=%v; want %v (reason=%q)",
				tc.name, tc.provider, tc.model, denied, tc.denied, reason)
		}
		if tc.denied && reason == "" {
			t.Errorf("%s: denied but reason empty", tc.name)
		}
		if !tc.denied && reason != "" {
			t.Errorf("%s: not denied but reason=%q", tc.name, reason)
		}
	}
}

// TestDeniedModelProvider_CompositeId verifies deniedModelProvider derives the
// provider from a composite "<provider>/<model>" id and only fires on composites.
func TestDeniedModelProvider_CompositeId(t *testing.T) {
	t.Parallel()
	SetProviderModelDeny(nil)

	if _, denied := deniedModelProvider("openrouter/anthropic/claude-fable-5.1"); !denied {
		t.Error("openrouter/anthropic/claude-fable-5.1: want denied")
	}
	if _, denied := deniedModelProvider("openrouter/deepseek/deepseek-v4-flash-0731"); denied {
		t.Error("openrouter/deepseek/deepseek-v4-flash-0731: want NOT denied")
	}
	if _, denied := deniedModelProvider("claude-fable-5"); denied {
		t.Error("bare claude-fable-5 (no provider prefix): want NOT denied")
	}
	if _, denied := deniedModelProvider(""); denied {
		t.Error("empty model: want NOT denied")
	}
}
