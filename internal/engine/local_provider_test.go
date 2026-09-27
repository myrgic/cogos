package engine

import (
	"strings"
	"testing"
)

// agenticStub is an on-machine agentic CLI (claude-code, codex, pi): IsLocal
// because it runs as a local process, AgenticHarness because it forwards
// prompts to a hosted model.
func agenticStub(name string) *StubProvider {
	s := NewStubProvider(name, "resp")
	s.capabilities.AgenticHarness = true
	return s
}

// Regression: "local" is advertised as private, no egress. Agentic CLIs that
// forward to a hosted model must never satisfy it, even though they sort
// first by name ("claude-code" < "lmstudio-...").
func TestLocalProvider_SkipsAgenticHarness(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{})
	r.RegisterProvider(agenticStub("claude-code"))
	r.RegisterProvider(agenticStub("codex"))
	r.RegisterProvider(NewStubProvider("zz-on-device", "resp"))
	name, ok := r.LocalProvider()
	if !ok || name != "zz-on-device" {
		t.Fatalf("LocalProvider = (%q,%v); want (zz-on-device,true): agentic CLIs are not on-device", name, ok)
	}
}

func TestLocalProvider_OnlyAgenticHarness_None(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{})
	r.RegisterProvider(agenticStub("codex"))
	if name, ok := r.LocalProvider(); ok {
		t.Fatalf("LocalProvider = %q; want none when only agentic CLIs are registered", name)
	}
}

// routing.default_local makes the choice explicit instead of alphabetical.
func TestLocalProvider_DefaultLocalWins(t *testing.T) {
	t.Parallel()
	r := NewSimpleRouter(RoutingConfig{DefaultLocal: "b-backend"})
	r.RegisterProvider(NewStubProvider("a-backend", "resp"))
	r.RegisterProvider(NewStubProvider("b-backend", "resp"))
	if name, _ := r.LocalProvider(); name != "b-backend" {
		t.Fatalf("LocalProvider = %q; want b-backend (routing.default_local)", name)
	}
}

// A default_local that is missing or not on-device falls back to the first
// on-device provider instead of routing somewhere wrong.
func TestLocalProvider_DefaultLocalInvalidFallsBack(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"not-registered", "codex"} {
		r := NewSimpleRouter(RoutingConfig{DefaultLocal: bad})
		r.RegisterProvider(agenticStub("codex"))
		r.RegisterProvider(NewStubProvider("m-backend", "resp"))
		if name, _ := r.LocalProvider(); name != "m-backend" {
			t.Errorf("default_local=%q: LocalProvider = %q; want m-backend", bad, name)
		}
	}
}

func TestMergeRoutingConfig_DefaultLocalOverlay(t *testing.T) {
	t.Parallel()
	got := mergeRoutingConfig(RoutingConfig{DefaultLocal: "a"}, RoutingConfig{DefaultLocal: "b"})
	if got.DefaultLocal != "b" {
		t.Fatalf("overlay DefaultLocal = %q; want b", got.DefaultLocal)
	}
	got = mergeRoutingConfig(RoutingConfig{DefaultLocal: "a"}, RoutingConfig{})
	if got.DefaultLocal != "a" {
		t.Fatalf("empty overlay DefaultLocal = %q; want a (kept)", got.DefaultLocal)
	}
}

// Tier comes from declarations (on-device + endpoint host), never the name.
func TestLocalityTier(t *testing.T) {
	t.Parallel()
	loop := NewOpenAICompatProvider("any-name", ProviderConfig{Endpoint: "http://127.0.0.1:1234"})
	lan := NewOpenAICompatProvider("any-name", ProviderConfig{Endpoint: "http://192.0.2.10:1234"})
	ollamaLAN := NewOllamaProvider("any-name", ProviderConfig{Endpoint: "http://192.0.2.10:11434"})
	if got := localityTier(loop); got != "local-sovereign" {
		t.Errorf("loopback: tier = %q; want local-sovereign", got)
	}
	if got := localityTier(lan); got != "lan-local" {
		t.Errorf("other host: tier = %q; want lan-local", got)
	}
	if got := localityTier(ollamaLAN); got != "lan-local" {
		t.Errorf("ollama on other host: tier = %q; want lan-local", got)
	}
	noEndpoint := NewStubProvider("any-name", "r")
	noEndpoint.endpoint = ""
	if got := localityTier(noEndpoint); got != "lan-local" {
		t.Errorf("on-device, endpoint unknown: tier = %q; want lan-local (no-egress needs evidence)", got)
	}
	if got := localityTier(agenticStub("codex")); got != "frontier-managed" {
		t.Errorf("agentic CLI: tier = %q; want frontier-managed", got)
	}
	if got := localityTier(newCloudStub("hosted", "r")); got != "frontier-managed" {
		t.Errorf("hosted: tier = %q; want frontier-managed", got)
	}
}

// Review finding on this PR: the "local" alias entry in /v1/models must take
// its tier (and its "no egress" claim) from the provider it resolves to, and
// agree with that provider's own live entry. A LAN backend is not "private,
// no egress".
func TestModelsMenu_LocalAliasTierFollowsResolvedProvider(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		endpoint, wantTier string
	}{
		{"http://192.0.2.10:1234", "lan-local"},
		{"http://127.0.0.1:1234", "local-sovereign"},
	} {
		backend := newContextListerStub("backend-x", true, ModelListing{ID: "m1", ContextLength: 4096})
		backend.endpoint = tc.endpoint
		backend.capabilities.ModelsAvailable = []string{"m1"}
		router := NewSimpleRouter(RoutingConfig{DefaultLocal: "backend-x"})
		router.RegisterProvider(backend)
		byID := modelIDSet(fetchModels(t, freshModelsServer(t, router)))
		alias, ok := byID["local"]
		if !ok {
			t.Fatalf("%s: local alias missing", tc.endpoint)
		}
		if alias.Tier != tc.wantTier {
			t.Errorf("%s: local alias tier = %q; want %q", tc.endpoint, alias.Tier, tc.wantTier)
		}
		if live := byID["backend-x/m1"]; live.Tier != alias.Tier {
			t.Errorf("%s: alias tier %q disagrees with the backend's live entry %q", tc.endpoint, alias.Tier, live.Tier)
		}
		if noEgress := strings.Contains(alias.Description, "no egress"); noEgress != (tc.wantTier == "local-sovereign") {
			t.Errorf("%s: description %q; \"no egress\" only for a loopback backend", tc.endpoint, alias.Description)
		}
	}
}
