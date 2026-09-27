package engine

import "testing"

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
	if got := localityTier(loop); got != "local-sovereign" {
		t.Errorf("loopback: tier = %q; want local-sovereign", got)
	}
	if got := localityTier(lan); got != "lan-local" {
		t.Errorf("other host: tier = %q; want lan-local", got)
	}
	if got := localityTier(agenticStub("codex")); got != "frontier-managed" {
		t.Errorf("agentic CLI: tier = %q; want frontier-managed", got)
	}
	if got := localityTier(newCloudStub("hosted", "r")); got != "frontier-managed" {
		t.Errorf("hosted: tier = %q; want frontier-managed", got)
	}
}
