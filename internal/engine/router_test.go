// router_test.go — SimpleRouter unit tests
package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// ── Registration ──────────────────────────────────────────────────────────────

func TestRouterRegisterDeregister(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "a"})

	a := NewStubProvider("a", "from-a")
	b := NewStubProvider("b", "from-b")
	r.RegisterProvider(a)
	r.RegisterProvider(b)

	if len(r.providers) != 2 {
		t.Fatalf("providers len = %d; want 2", len(r.providers))
	}

	r.DeregisterProvider("a")
	if len(r.providers) != 1 {
		t.Fatalf("after deregister providers len = %d; want 1", len(r.providers))
	}
	if r.providers[0].Name() != "b" {
		t.Errorf("remaining provider = %q; want b", r.providers[0].Name())
	}
}

// ── Route ─────────────────────────────────────────────────────────────────────

func TestRouterSelectsDefault(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "alpha"})
	r.RegisterProvider(NewStubProvider("alpha", "reply"))

	req := &CompletionRequest{Metadata: RequestMetadata{RequestID: "r1"}}
	p, dec, err := r.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "alpha" {
		t.Errorf("selected = %q; want alpha", p.Name())
	}
	if dec.SelectedProvider != "alpha" {
		t.Errorf("decision provider = %q; want alpha", dec.SelectedProvider)
	}
	if dec.FallbackUsed {
		t.Error("FallbackUsed should be false for default selection")
	}
}

func TestRouterFallbackWhenPrimaryUnavailable(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{
		Default:       "primary",
		FallbackChain: []string{"primary", "backup"},
	})

	primary := NewStubProvider("primary", "")
	primary.available = false // simulate down

	backup := NewStubProvider("backup", "backup reply")
	r.RegisterProvider(primary)
	r.RegisterProvider(backup)

	p, dec, err := r.Route(context.Background(), &CompletionRequest{
		Metadata: RequestMetadata{RequestID: "r2"},
	})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "backup" {
		t.Errorf("selected = %q; want backup", p.Name())
	}
	if !dec.FallbackUsed {
		t.Error("FallbackUsed should be true")
	}
	if dec.FallbackFrom != "primary" {
		t.Errorf("FallbackFrom = %q; want primary", dec.FallbackFrom)
	}
}

// TestRouterFallbackSkipsProvidersThatCannotServeOverride is the regression
// guard for inference-pipeline-robustness FIX 3. When the primary local provider
// (serving ornith-1.0-35b) is down, the fallback carries the ModelOverride into
// the chain. A LOCAL model-serving sibling that has a DIFFERENT model loaded
// (gemma) would 404 on the ornith id, so the router must skip it and reach the
// sibling that actually serves ornith-1.0-35b. Only local model-serving
// providers are gated this way — see TestRouterFallbackReachesFrontierBackup
// for the complementary guarantee that frontier fallbacks stay eligible.
func TestRouterFallbackSkipsProvidersThatCannotServeOverride(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{
		Default:       "lmstudio-darkstar",
		FallbackChain: []string{"lmstudio-darkstar", "lmstudio-gemma", "lmstudio-eclipse"},
	})

	// Primary: serves ornith-1.0-35b but is down.
	darkstar := NewStubProvider("lmstudio-darkstar", "")
	darkstar.model = "ornith-1.0-35b"
	darkstar.available = false

	// Local model-serving sibling with a DIFFERENT model loaded. It would 404 on
	// an ornith id, so it MUST be skipped for an ornith override.
	gemma := NewStubProvider("lmstudio-gemma", "gemma reply")
	gemma.model = "gemma-4-26b"
	gemma.capabilities = ProviderCapabilities{
		Capabilities:    []Capability{CapStreaming, CapToolUse, CapJSON},
		ModelsAvailable: []string{"gemma-4-26b"},
		IsLocal:         true,
	}

	// Sibling local provider that DOES serve ornith-1.0-35b.
	eclipse := NewStubProvider("lmstudio-eclipse", "ornith reply")
	eclipse.model = "ornith-1.0-35b"

	r.RegisterProvider(darkstar)
	r.RegisterProvider(gemma)
	r.RegisterProvider(eclipse)

	p, dec, err := r.Route(context.Background(), &CompletionRequest{
		ModelOverride: "ornith-1.0-35b",
		Metadata:      RequestMetadata{RequestID: "fix3-1"},
	})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "lmstudio-eclipse" {
		t.Errorf("selected = %q; want lmstudio-eclipse (lmstudio-gemma must be skipped — it has a different model loaded and can't serve ornith-1.0-35b)", p.Name())
	}
	if !dec.FallbackUsed {
		t.Error("FallbackUsed should be true")
	}
}

// TestRouterFallbackReachesFrontierBackup is the missing coverage the flight
// review flagged: a frontier provider reached as a FALLBACK (i > 0) with a
// differing-but-serviceable Claude-family override must be SELECTED, not skipped.
// A "sonnet" request resolves to a preferred claude-oauth (configured for opus)
// that is DOWN; the router must fall to a backup frontier provider carrying the
// sonnet override and select it, because frontier providers honour the override
// verbatim within their family. Before the fix, providerCanServe gated every
// frontier fallback and this returned "no available provider".
func TestRouterFallbackReachesFrontierBackup(t *testing.T) {
	t.Parallel()

	// Two flavours of frontier backup, plus a local agentic CLI, all reachable
	// only as fallbacks (i > 0). Each must be selectable for a differing override.
	newFrontier := func(name, model string, models []string, local, agentic bool) *StubProvider {
		p := NewStubProvider(name, name+" reply")
		p.model = model
		p.capabilities = ProviderCapabilities{
			Capabilities:    []Capability{CapStreaming, CapToolUse, CapJSON},
			ModelsAvailable: models,
			IsLocal:         local,
			AgenticHarness:  agentic,
		}
		return p
	}

	cases := []struct {
		name       string
		backupName string
		backup     *StubProvider
	}{
		{
			// A second claude-oauth / the anthropic provider: remote, !IsLocal,
			// advertises a date-stamped id that does not match the short override.
			name:       "remote anthropic backup",
			backupName: "anthropic",
			backup:     newFrontier("anthropic", "claude-sonnet-4-20250514", []string{"claude-sonnet-4-20250514"}, false, false),
		},
		{
			// The claude-code CLI fallback: IsLocal AND AgenticHarness, advertises
			// only short aliases that don't match the date-stamped override.
			name:       "local claude-code CLI backup",
			backupName: "claude-code",
			backup:     newFrontier("claude-code", "sonnet", []string{"sonnet", "opus", "haiku"}, true, true),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewSimpleRouter(RoutingConfig{
				Default:       "claude-oauth",
				FallbackChain: []string{"claude-oauth", tc.backupName},
			})

			// Preferred frontier, configured for opus, is DOWN.
			primary := newFrontier("claude-oauth", "claude-opus-4-7", []string{"claude-opus-4-7"}, false, false)
			primary.available = false

			r.RegisterProvider(primary)
			r.RegisterProvider(tc.backup)

			p, dec, err := r.Route(context.Background(), &CompletionRequest{
				ModelOverride: "claude-sonnet-4-6",
				Metadata:      RequestMetadata{RequestID: "fix3-frontier"},
			})
			if err != nil {
				t.Fatalf("Route: %v (frontier backup must be reachable as a fallback)", err)
			}
			if p.Name() != tc.backupName {
				t.Errorf("selected = %q; want %q (frontier fallback must serve the differing Claude-family override, not be skipped)", p.Name(), tc.backupName)
			}
			if !dec.FallbackUsed {
				t.Error("FallbackUsed should be true — primary was down, backup reached as fallback")
			}
		})
	}
}

// TestRouterFallbackNoOverridePreservesBehavior confirms the compat-aware skip
// does not change routing when there is no ModelOverride: the first available
// provider in the chain wins even if its Model() differs from the primary's.
func TestRouterFallbackNoOverridePreservesBehavior(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{
		Default:       "lmstudio-darkstar",
		FallbackChain: []string{"lmstudio-darkstar", "claude-oauth", "lmstudio-eclipse"},
	})

	darkstar := NewStubProvider("lmstudio-darkstar", "")
	darkstar.model = "ornith-1.0-35b"
	darkstar.available = false

	oauth := NewStubProvider("claude-oauth", "claude reply")
	oauth.model = "claude-opus-4-7"
	oauth.capabilities = ProviderCapabilities{
		Capabilities:    []Capability{CapStreaming, CapJSON},
		ModelsAvailable: []string{"claude-opus-4-7"},
		IsLocal:         false,
	}

	eclipse := NewStubProvider("lmstudio-eclipse", "ornith reply")
	eclipse.model = "ornith-1.0-35b"

	r.RegisterProvider(darkstar)
	r.RegisterProvider(oauth)
	r.RegisterProvider(eclipse)

	// No ModelOverride: claude-oauth is the next available candidate and must be
	// selected exactly as before the fix (the skip only applies to overrides).
	p, dec, err := r.Route(context.Background(), &CompletionRequest{
		Metadata: RequestMetadata{RequestID: "fix3-2"},
	})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "claude-oauth" {
		t.Errorf("selected = %q; want claude-oauth (no override → first available wins, unchanged)", p.Name())
	}
	if !dec.FallbackUsed {
		t.Error("FallbackUsed should be true")
	}
}

// TestRouterPreferredProviderNotSkippedForDifferingOverride guards the
// managed-frontier case (FIX 3): the compat-aware skip applies only to fallback
// candidates, never the preferred provider. A "sonnet"/"opus"-style request
// resolves to a frontier provider with a ModelOverride that intentionally
// differs from that provider's configured model; the router must still select
// it (position 0), not skip it because Model() != override.
func TestRouterPreferredProviderNotSkippedForDifferingOverride(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{
		Default:       "lmstudio-darkstar",
		FallbackChain: []string{"lmstudio-darkstar"},
	})

	// Frontier provider configured for opus but asked (via override) for sonnet —
	// it is model-agnostic within its family and honours the override.
	oauth := NewStubProvider("claude-oauth", "sonnet reply")
	oauth.model = "claude-opus-4-7"
	oauth.capabilities = ProviderCapabilities{
		Capabilities:    []Capability{CapStreaming, CapJSON},
		ModelsAvailable: []string{"claude-opus-4-7"},
		IsLocal:         false,
	}
	r.RegisterProvider(oauth)

	// Request explicitly prefers claude-oauth with a differing override.
	p, dec, err := r.Route(context.Background(), &CompletionRequest{
		ModelOverride: "claude-sonnet-4-6",
		Metadata: RequestMetadata{
			RequestID:      "fix3-3",
			PreferProvider: "claude-oauth",
		},
	})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "claude-oauth" {
		t.Errorf("selected = %q; want claude-oauth (preferred provider must not be skipped for a differing override)", p.Name())
	}
	if dec.FallbackUsed {
		t.Error("FallbackUsed should be false — claude-oauth is the preferred provider")
	}
}

// TestProviderCanServe locks the compat-aware fallback predicate (FIX 3). The
// gate is scoped to LOCAL, NON-AGENTIC model-serving providers; frontier
// providers (remote APIs and local agentic CLIs) are always eligible because
// they honour an arbitrary override verbatim within their model family.
func TestProviderCanServe(t *testing.T) {
	t.Parallel()

	// Local model-serving provider (lmstudio/ollama/mlx shape): IsLocal, not an
	// agentic harness, advertises exactly the model it has loaded.
	localServing := NewStubProvider("lmstudio-eclipse", "")
	localServing.model = "ornith-1.0-35b"
	localServing.capabilities = ProviderCapabilities{
		ModelsAvailable: []string{"ornith-1.0-35b"},
		IsLocal:         true,
	}

	// Remote frontier provider (anthropic / claude-oauth shape): !IsLocal,
	// advertises only its configured model but honours any Claude-family id.
	remoteFrontier := NewStubProvider("claude-oauth", "")
	remoteFrontier.model = "claude-opus-4-7"
	remoteFrontier.capabilities = ProviderCapabilities{
		ModelsAvailable: []string{"claude-opus-4-7"},
		IsLocal:         false,
	}

	// Local agentic CLI (claude-code / codex shape): IsLocal AND AgenticHarness.
	// IsLocal alone would wrongly gate it; the AgenticHarness carve-out keeps it
	// eligible because it passes ModelOverride straight to `--model`.
	agenticCLI := NewStubProvider("claude-code", "")
	agenticCLI.model = "sonnet"
	agenticCLI.capabilities = ProviderCapabilities{
		ModelsAvailable: []string{"sonnet", "opus", "haiku"},
		IsLocal:         true,
		AgenticHarness:  true,
	}

	// Local model-serving provider that advertises no model at all.
	localAgnostic := NewStubProvider("generic-local", "")
	localAgnostic.capabilities = ProviderCapabilities{IsLocal: true}

	cases := []struct {
		name     string
		p        Provider
		override string
		want     bool
	}{
		{"empty override always serves (local serving)", localServing, "", true},
		{"empty override always serves (remote frontier)", remoteFrontier, "", true},
		{"exact model match on local serving", localServing, "ornith-1.0-35b", true},
		{"non-matching override on local serving provider is skipped", localServing, "gemma-4-26b", false},
		{"prefix: request family, local serving provider serves variant", localServing, "ornith-1.0", true},
		{"remote frontier serves any Claude-family override (never gated)", remoteFrontier, "claude-sonnet-4-6", true},
		{"remote frontier serves even a non-family override (honours verbatim)", remoteFrontier, "ornith-1.0-35b", true},
		{"local agentic CLI serves an override outside its advertised list", agenticCLI, "claude-sonnet-4-6", true},
		{"local model-agnostic provider serves any override", localAgnostic, "ornith-1.0-35b", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := providerCanServe(tc.p, tc.override); got != tc.want {
				t.Errorf("providerCanServe(%q, %q) = %v; want %v", tc.p.Name(), tc.override, got, tc.want)
			}
		})
	}
}

func TestRouterErrorWhenNoneAvailable(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "p"})

	p := NewStubProvider("p", "")
	p.available = false
	r.RegisterProvider(p)

	_, _, err := r.Route(context.Background(), &CompletionRequest{
		Metadata: RequestMetadata{RequestID: "r3"},
	})
	if err == nil {
		t.Error("expected error when no provider is available")
	}
}

func TestRouterProcessStateOverride(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{
		Default: "cloud",
		ProcessStateRouting: map[string]string{
			"consolidating": "local",
		},
		FallbackChain: []string{"cloud", "local"},
	})

	cloud := NewStubProvider("cloud", "cloud reply")
	local := NewStubProvider("local", "local reply")
	r.RegisterProvider(cloud)
	r.RegisterProvider(local)

	req := &CompletionRequest{
		Metadata: RequestMetadata{
			RequestID:    "r4",
			ProcessState: "consolidating",
		},
	}
	p, _, err := r.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	// "local" should be preferred for consolidating state.
	if p.Name() != "local" {
		t.Errorf("selected = %q; want local (process_state_routing override)", p.Name())
	}
}

func TestRouterCapabilityFilter(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "basic"})

	basic := NewStubProvider("basic", "")
	basic.capabilities = ProviderCapabilities{
		Capabilities: []Capability{CapJSON},
		IsLocal:      true,
	}
	full := NewStubProvider("full", "")
	full.capabilities = ProviderCapabilities{
		Capabilities: []Capability{CapJSON, CapToolUse},
		IsLocal:      true,
	}
	r.RegisterProvider(basic)
	r.RegisterProvider(full)

	req := &CompletionRequest{
		Metadata: RequestMetadata{
			RequestID:            "r5",
			RequiredCapabilities: []Capability{CapToolUse},
		},
	}
	p, _, err := r.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if p.Name() != "full" {
		t.Errorf("selected = %q; want full (has tool_use)", p.Name())
	}
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func TestRouterStats(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "p"})
	r.RegisterProvider(NewStubProvider("p", "reply"))

	req := &CompletionRequest{Metadata: RequestMetadata{RequestID: "s1"}}
	for range 3 {
		if _, _, err := r.Route(context.Background(), req); err != nil {
			t.Fatalf("Route: %v", err)
		}
	}

	stats := r.Stats()
	if stats.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d; want 3", stats.TotalRequests)
	}
	if stats.RequestsByProvider["p"] != 3 {
		t.Errorf("RequestsByProvider[p] = %d; want 3", stats.RequestsByProvider["p"])
	}
	if stats.SovereigntyRatio != 1.0 {
		t.Errorf("SovereigntyRatio = %f; want 1.0 (local only)", stats.SovereigntyRatio)
	}
}

// ── makeProvider ─────────────────────────────────────────────────────────────

func TestMakeProviderOllama(t *testing.T) {
	t.Parallel()
	p, err := makeProvider("ollama", ProviderConfig{Type: "ollama", Model: "qwen2.5:9b"}, nil)
	if err != nil {
		t.Fatalf("makeProvider: %v", err)
	}
	if p.Name() != "ollama" {
		t.Errorf("name = %q; want ollama", p.Name())
	}
}

func TestMakeProviderStub(t *testing.T) {
	t.Parallel()
	p, err := makeProvider("stub", ProviderConfig{Type: "stub"}, nil)
	if err != nil {
		t.Fatalf("makeProvider: %v", err)
	}
	if p.Name() != "stub" {
		t.Errorf("name = %q; want stub", p.Name())
	}
}

func TestMakeProviderUnknown(t *testing.T) {
	t.Parallel()
	_, err := makeProvider("x", ProviderConfig{Type: "unknown_type"}, nil)
	if err == nil {
		t.Error("expected error for unknown provider type")
	}
}

func TestMakeProviderInfersTypeFromName(t *testing.T) {
	t.Parallel()
	// No Type field — should infer "ollama" from name.
	p, err := makeProvider("ollama", ProviderConfig{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("makeProvider: %v", err)
	}
	if _, ok := p.(*OllamaProvider); !ok {
		t.Errorf("expected OllamaProvider, got %T", p)
	}
}

// TestMakeProviderVLLM verifies that type "vllm" is registered and routes to
// the OpenAI-compatible provider implementation. vLLM exposes the OpenAI
// /v1/chat/completions and /v1/models contract; no dedicated provider type
// is required for the unsupervised case.
func TestMakeProviderVLLM(t *testing.T) {
	t.Parallel()
	p, err := makeProvider("vllm-local", ProviderConfig{
		Type:     "vllm",
		Endpoint: "http://localhost:8000",
		Model:    "gemma4:e4b",
	}, nil)
	if err != nil {
		t.Fatalf("makeProvider(vllm): %v", err)
	}
	if _, ok := p.(*OpenAICompatProvider); !ok {
		t.Errorf("vllm should resolve to *OpenAICompatProvider, got %T", p)
	}
	if p.Name() != "vllm-local" {
		t.Errorf("Name() = %q; want vllm-local", p.Name())
	}
}

// ── defaultProvidersConfig ────────────────────────────────────────────────────

func TestDefaultProvidersConfig(t *testing.T) {
	t.Parallel()
	pcfg := defaultProvidersConfig(defaultOllamaModel)
	// defaultProvidersConfig probes LM Studio (:1234) then Ollama (:11434) and
	// selects whichever is reachable. On machines with neither running it
	// returns the no-local-model placeholder. Any of these three outcomes is valid.
	switch {
	case pcfg.Providers["ollama"].Type == "ollama":
		if pcfg.Providers["ollama"].Model != defaultOllamaModel {
			t.Errorf("ollama model = %q; want %q", pcfg.Providers["ollama"].Model, defaultOllamaModel)
		}
		if pcfg.Routing.Default != "ollama" {
			t.Errorf("routing default = %q; want ollama", pcfg.Routing.Default)
		}
	case pcfg.Providers["lmstudio"].Type == "openai-compat":
		if pcfg.Routing.Default != "lmstudio" {
			t.Errorf("routing default = %q; want lmstudio", pcfg.Routing.Default)
		}
	case pcfg.Providers["no-local-model"].Type == "stub":
		// No local backend reachable — placeholder config is correct.
	default:
		t.Errorf("unexpected default providers config: %+v", pcfg.Providers)
	}
}

func TestLoadProvidersConfigAppliesExplicitLocalModel(t *testing.T) {
	t.Parallel()

	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), "local_model: gemma4:e2b\n")
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    enabled: true
    endpoint: "http://localhost:11434"
    model: "qwen3.5:9b"
    timeout: 60
routing:
  default: ollama
  fallback_chain:
    - ollama
`)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		t.Fatalf("loadProvidersConfig: %v", err)
	}

	if pcfg.Providers["ollama"].Model != "gemma4:e2b" {
		t.Errorf("ollama model = %q; want gemma4:e2b", pcfg.Providers["ollama"].Model)
	}
}

// TestLoadProvidersConfigDeepMergesLocalYAML verifies that providers.local.yaml
// is deep-merged over providers.yaml — node-specific endpoints, env-var-backed
// API keys, and additional providers all land in the merged config without
// requiring the user to copy the entire defaults file.
func TestLoadProvidersConfigDeepMergesLocalYAML(t *testing.T) {
	t.Parallel()

	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    enabled: true
    endpoint: "http://localhost:11434"
    model: "gemma4:e4b"
    timeout: 60
  claude-code:
    type: claude-code
    model: sonnet
    timeout: 300
routing:
  default: claude-code
  fallback_chain: [claude-code, ollama]
  process_state_routing:
    active: claude-code
    receptive: ollama
`)

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.local.yaml"), `providers:
  ollama:
    context_window: 32768
  lmstudio-mlx:
    type: openai
    endpoint: http://localhost:1234
    model: gemma-mlx-id
    api_key_env: LMS_API_KEY
    options:
      is_local: true
routing:
  fallback_chain: [claude-code, lmstudio-mlx, ollama]
  process_state_routing:
    consolidating: lmstudio-mlx
`)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		t.Fatalf("loadProvidersConfig: %v", err)
	}

	// Existing provider field-level merge: ollama keeps base type/endpoint,
	// gains context_window from overlay.
	ollama := pcfg.Providers["ollama"]
	if ollama.Type != "ollama" {
		t.Errorf("ollama type = %q; want preserved 'ollama'", ollama.Type)
	}
	if ollama.Endpoint != "http://localhost:11434" {
		t.Errorf("ollama endpoint changed unexpectedly: %q", ollama.Endpoint)
	}
	if ollama.ContextWindow != 32768 {
		t.Errorf("ollama context_window = %d; want 32768 from overlay", ollama.ContextWindow)
	}

	// New provider added wholesale.
	lms, ok := pcfg.Providers["lmstudio-mlx"]
	if !ok {
		t.Fatalf("lmstudio-mlx not added by overlay; providers=%v", keysOf(pcfg.Providers))
	}
	if lms.Type != "openai" || lms.Endpoint != "http://localhost:1234" || lms.APIKeyEnv != "LMS_API_KEY" {
		t.Errorf("lmstudio-mlx fields wrong: %+v", lms)
	}
	if v, ok := lms.Options["is_local"].(bool); !ok || !v {
		t.Errorf("lmstudio-mlx options.is_local not preserved: %v", lms.Options)
	}

	// Routing merged: fallback_chain replaced, process_state_routing extended
	// (active/receptive preserved from base, consolidating added by overlay).
	if got := pcfg.Routing.FallbackChain; len(got) != 3 || got[1] != "lmstudio-mlx" {
		t.Errorf("fallback_chain = %v; want overlay value", got)
	}
	if pcfg.Routing.ProcessStateRouting["active"] != "claude-code" {
		t.Error("process_state_routing.active lost during merge")
	}
	if pcfg.Routing.ProcessStateRouting["consolidating"] != "lmstudio-mlx" {
		t.Errorf("process_state_routing.consolidating = %q; want overlay value",
			pcfg.Routing.ProcessStateRouting["consolidating"])
	}

	// Untouched provider preserved.
	if pcfg.Providers["claude-code"].Type != "claude-code" {
		t.Error("claude-code provider lost during merge")
	}
}

// TestLoadProvidersConfigSkipsMissingLocalYAML verifies that a missing
// providers.local.yaml is not an error — the base config is returned as-is.
func TestLoadProvidersConfigSkipsMissingLocalYAML(t *testing.T) {
	t.Parallel()

	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    model: gemma4:e4b
routing:
  default: ollama
`)
	// Deliberately do NOT write providers.local.yaml.

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		t.Fatalf("loadProvidersConfig: %v", err)
	}
	if pcfg.Providers["ollama"].Model != "gemma4:e4b" {
		t.Errorf("base config not returned cleanly: %+v", pcfg.Providers["ollama"])
	}
}

func keysOf(m map[string]ProviderConfig) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ── ProviderForName / ProviderForModel ───────────────────────────────────────

// modelStub wraps a StubProvider but overrides Model() to return a fixed string.
// Used to test ProviderForModel resolution without modifying StubProvider.
type modelStub struct {
	*StubProvider
	model string
}

func (m *modelStub) Model() string { return m.model }

// TestProviderForName_AliasHit verifies that when req.Model matches a registered
// provider's Name(), ProviderForName returns (name, true) so the caller knows
// NOT to set a ModelOverride — the provider already knows its configured model.
func TestProviderForName_AliasHit(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "mlx-gemma"})
	r.RegisterProvider(NewStubProvider("mlx-gemma", ""))

	name, ok := r.ProviderForName("mlx-gemma")
	if !ok {
		t.Fatal("ProviderForName: expected hit, got miss")
	}
	if name != "mlx-gemma" {
		t.Errorf("ProviderForName: got %q; want mlx-gemma", name)
	}
}

// TestProviderForName_Miss verifies that a string that is not a registered
// provider name returns ("", false), so the caller falls through to ModelOverride.
func TestProviderForName_Miss(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{})
	r.RegisterProvider(NewStubProvider("mlx-gemma", ""))

	_, ok := r.ProviderForName("gemma-3-4b-it")
	if ok {
		t.Error("ProviderForName: expected miss for non-name model string, got hit")
	}
}

// TestProviderForModel_HitSetsPreferProvider verifies that when req.Model is not
// a provider alias (ProviderForName miss) but matches a provider's Model() string,
// ProviderForModel returns (name, true) — the caller should set PreferProvider AND
// ModelOverride so the targeted provider gets the explicit model id.
func TestProviderForModel_HitSetsPreferProvider(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{Default: "cloud"})
	cloud := &modelStub{StubProvider: NewStubProvider("cloud", ""), model: "claude-opus-4"}
	r.RegisterProvider(cloud)

	name, ok := r.ProviderForModel("claude-opus-4")
	if !ok {
		t.Fatal("ProviderForModel: expected hit, got miss")
	}
	if name != "cloud" {
		t.Errorf("ProviderForModel: got %q; want cloud", name)
	}
}

// TestProviderForModel_Miss verifies ("", false) when no provider's Name or Model
// matches — the caller should route via defaults with ModelOverride only.
func TestProviderForModel_Miss(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{})
	r.RegisterProvider(NewStubProvider("ollama", ""))

	_, ok := r.ProviderForModel("gpt-5")
	if ok {
		t.Error("ProviderForModel: expected miss for unknown model, got hit")
	}
}

// TestProviderForModel_DeterministicTiebreak verifies that when two providers
// declare the same Model() string, ProviderForModel always resolves to the same
// one. Providers are sorted by Name() on registration, so the alphabetically first
// name wins consistently regardless of registration order.
func TestProviderForModel_DeterministicTiebreak(t *testing.T) {
	t.Parallel()

	const sharedModel = "gemma-3-4b-it"

	// Register in reverse-alpha order to confirm sort, not insertion, governs.
	r := NewSimpleRouter(RoutingConfig{Default: "zebra"})
	r.RegisterProvider(&modelStub{StubProvider: NewStubProvider("zebra", ""), model: sharedModel})
	r.RegisterProvider(&modelStub{StubProvider: NewStubProvider("alpha", ""), model: sharedModel})
	r.RegisterProvider(&modelStub{StubProvider: NewStubProvider("mango", ""), model: sharedModel})

	name, ok := r.ProviderForModel(sharedModel)
	if !ok {
		t.Fatal("ProviderForModel: expected hit, got miss")
	}
	// "alpha" sorts first — that should always win.
	if name != "alpha" {
		t.Errorf("ProviderForModel: got %q; want alpha (alphabetically first)", name)
	}

	// Call multiple times to confirm stability.
	for range 10 {
		got, _ := r.ProviderForModel(sharedModel)
		if got != name {
			t.Errorf("ProviderForModel: non-deterministic — got %q on repeat, want %q", got, name)
		}
	}
}

// ── mergeProvidersConfig ────────────────────────────────────────────────────
//
// Regression coverage for the providers.local.yaml overlay merge (PR #509
// cog-review finding): mergeProvidersConfig must carry every field of
// ProvidersConfig — Providers, Routing, AND ClaudeOAuth — from overlay onto
// base. A field silently dropped here means a node-local override in
// providers.local.yaml is discarded without warning.

func TestMergeProvidersConfig_ClaudeOAuth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		baseAuto     *bool
		overlayAuto  *bool
		wantAuto     *bool
		wantIsAutoEn bool
	}{
		{
			name:         "base unset, overlay unset -> stays unset (defaults enabled)",
			baseAuto:     nil,
			overlayAuto:  nil,
			wantAuto:     nil,
			wantIsAutoEn: true,
		},
		{
			name:         "base opts out, overlay omits field entirely -> base opt-out survives",
			baseAuto:     boolPtr(false),
			overlayAuto:  nil,
			wantAuto:     boolPtr(false),
			wantIsAutoEn: false,
		},
		{
			name:         "base unset, overlay opts out -> overlay opt-out applies",
			baseAuto:     nil,
			overlayAuto:  boolPtr(false),
			wantAuto:     boolPtr(false),
			wantIsAutoEn: false,
		},
		{
			name:         "base enabled, overlay opts out -> overlay wins",
			baseAuto:     boolPtr(true),
			overlayAuto:  boolPtr(false),
			wantAuto:     boolPtr(false),
			wantIsAutoEn: false,
		},
		{
			name:         "base opted out, overlay re-enables -> overlay wins",
			baseAuto:     boolPtr(false),
			overlayAuto:  boolPtr(true),
			wantAuto:     boolPtr(true),
			wantIsAutoEn: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := ProvidersConfig{ClaudeOAuth: ClaudeOAuthAutoConfig{Auto: tc.baseAuto}}
			overlay := ProvidersConfig{ClaudeOAuth: ClaudeOAuthAutoConfig{Auto: tc.overlayAuto}}

			merged := mergeProvidersConfig(base, overlay)

			if (merged.ClaudeOAuth.Auto == nil) != (tc.wantAuto == nil) {
				t.Fatalf("ClaudeOAuth.Auto = %v; want %v", merged.ClaudeOAuth.Auto, tc.wantAuto)
			}
			if merged.ClaudeOAuth.Auto != nil && tc.wantAuto != nil && *merged.ClaudeOAuth.Auto != *tc.wantAuto {
				t.Errorf("ClaudeOAuth.Auto = %v; want %v", *merged.ClaudeOAuth.Auto, *tc.wantAuto)
			}
			if got := merged.ClaudeOAuth.IsAutoEnabled(); got != tc.wantIsAutoEn {
				t.Errorf("IsAutoEnabled() = %v; want %v", got, tc.wantIsAutoEn)
			}
		})
	}
}

// TestMergeProvidersConfig_AllFields is a table test over base/overlay
// combinations for every field mergeProvidersConfig is supposed to carry
// (Providers, Routing, ClaudeOAuth), guarding against a future field being
// added to ProvidersConfig without a matching merge clause.
func TestMergeProvidersConfig_AllFields(t *testing.T) {
	t.Parallel()

	t.Run("Providers: overlay adds new key, overrides existing key, leaves untouched key", func(t *testing.T) {
		base := ProvidersConfig{
			Providers: map[string]ProviderConfig{
				"kept":     {Model: "kept-model"},
				"override": {Model: "old-model"},
			},
		}
		overlay := ProvidersConfig{
			Providers: map[string]ProviderConfig{
				"override": {Model: "new-model"},
				"added":    {Model: "added-model"},
			},
		}

		merged := mergeProvidersConfig(base, overlay)

		if got := merged.Providers["kept"].Model; got != "kept-model" {
			t.Errorf("Providers[kept].Model = %q; want kept-model", got)
		}
		if got := merged.Providers["override"].Model; got != "new-model" {
			t.Errorf("Providers[override].Model = %q; want new-model", got)
		}
		if got := merged.Providers["added"].Model; got != "added-model" {
			t.Errorf("Providers[added].Model = %q; want added-model", got)
		}
	})

	t.Run("Providers: overlay nil map leaves base untouched", func(t *testing.T) {
		base := ProvidersConfig{Providers: map[string]ProviderConfig{"a": {Model: "a-model"}}}
		overlay := ProvidersConfig{}

		merged := mergeProvidersConfig(base, overlay)

		if got := merged.Providers["a"].Model; got != "a-model" {
			t.Errorf("Providers[a].Model = %q; want a-model", got)
		}
	})

	t.Run("Routing: overlay non-zero fields override, zero fields preserve base", func(t *testing.T) {
		base := ProvidersConfig{Routing: RoutingConfig{Default: "base-default", LocalThreshold: 0.5}}
		overlay := ProvidersConfig{Routing: RoutingConfig{Default: "overlay-default"}}

		merged := mergeProvidersConfig(base, overlay)

		if merged.Routing.Default != "overlay-default" {
			t.Errorf("Routing.Default = %q; want overlay-default", merged.Routing.Default)
		}
		if merged.Routing.LocalThreshold != 0.5 {
			t.Errorf("Routing.LocalThreshold = %v; want 0.5 (preserved from base)", merged.Routing.LocalThreshold)
		}
	})

	t.Run("ClaudeOAuth: overlay omitted entirely preserves base opt-out (the PR #509 regression)", func(t *testing.T) {
		base := ProvidersConfig{
			Providers:   map[string]ProviderConfig{"local": {Model: "local-model"}},
			Routing:     RoutingConfig{Default: "local"},
			ClaudeOAuth: ClaudeOAuthAutoConfig{Auto: boolPtr(false)},
		}
		// overlay mirrors a providers.local.yaml that only sets an unrelated
		// field and never mentions claude_oauth at all.
		overlay := ProvidersConfig{
			Providers: map[string]ProviderConfig{"local": {Endpoint: "http://127.0.0.1:9999"}},
		}

		merged := mergeProvidersConfig(base, overlay)

		if merged.ClaudeOAuth.Auto == nil || *merged.ClaudeOAuth.Auto != false {
			t.Fatalf("ClaudeOAuth.Auto = %v; want false (base opt-out must survive an overlay that never mentions claude_oauth)", merged.ClaudeOAuth.Auto)
		}
		if merged.ClaudeOAuth.IsAutoEnabled() {
			t.Error("IsAutoEnabled() = true; want false — operator's opt-out was silently dropped")
		}
	})
}

// TestRouterRefusesDeniedProviderDefaultModel is the regression test for
// cog-review round 2 on #624: a bare provider-name request (ModelOverride
// empty) must not reach a provider whose OWN configured default model is
// policy-denied. Before this fix, Route()'s deny check only ever looked at
// req.ModelOverride, so DeniedByPolicy's model=="" short-circuit let a
// provider serve its denied default model on every request that just named
// the provider (e.g. an operator's "openrouter" entry configured with
// `model: anthropic/claude-fable-5.1`, reproducing the exact Max-subscription
// billing incident this policy exists to block).
func TestRouterRefusesDeniedProviderDefaultModel(t *testing.T) {
	t.Parallel()
	SetProviderModelDeny(nil)
	t.Cleanup(func() { SetProviderModelDeny(nil) })

	r := NewSimpleRouter(RoutingConfig{Default: "openrouter"})
	denied := NewStubProvider("openrouter", "reply")
	denied.model = "anthropic/claude-fable-5.1" // the provider's own configured default
	r.RegisterProvider(denied)

	req := &CompletionRequest{Metadata: RequestMetadata{RequestID: "r-deny-default"}}
	_, _, err := r.Route(context.Background(), req)
	if err == nil {
		t.Fatal("Route: expected an error (no available provider) when the only candidate's default model is denied; got a selection")
	}
}

// TestRouterRefusesDeniedProviderDefaultModel_Fallback covers the same gap
// on a FALLBACK candidate: the preferred provider is unavailable, and the
// fallback's own configured default model is denied.
func TestRouterRefusesDeniedProviderDefaultModel_Fallback(t *testing.T) {
	t.Parallel()
	SetProviderModelDeny(nil)
	t.Cleanup(func() { SetProviderModelDeny(nil) })

	r := NewSimpleRouter(RoutingConfig{
		Default:       "primary",
		FallbackChain: []string{"primary", "openrouter"},
	})
	primary := NewStubProvider("primary", "reply")
	primary.available = false
	r.RegisterProvider(primary)
	denied := NewStubProvider("openrouter", "reply")
	denied.model = "anthropic/claude-fable-5.1"
	r.RegisterProvider(denied)

	req := &CompletionRequest{Metadata: RequestMetadata{RequestID: "r-deny-fallback"}}
	_, _, err := r.Route(context.Background(), req)
	if err == nil {
		t.Fatal("Route: expected an error when the only reachable fallback's default model is denied; got a selection")
	}
}
