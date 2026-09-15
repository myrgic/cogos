// provider_availability_test.go — tests for honest Available() on the
// claude-code and codex providers: binary presence alone must not report
// available; the auth probe must pass too.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ── codex ────────────────────────────────────────────────────────────────────

func codexOnPath(t *testing.T) {
	t.Helper()
	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) { return "/usr/local/bin/codex", nil }
}

func TestCodexAvailableBinaryPresentAuthGood(t *testing.T) {
	resetCodexResolverForTest(t)
	codexOnPath(t)
	codexAuthProbe = func(ctx context.Context, binary string) error { return nil }

	p := NewCodexProvider("codex", ProviderConfig{})
	if !p.Available(context.Background()) {
		t.Fatal("Available should be true with binary present and auth good")
	}
}

func TestCodexAvailableBinaryPresentAuthBad(t *testing.T) {
	resetCodexResolverForTest(t)
	codexOnPath(t)
	codexAuthProbe = func(ctx context.Context, binary string) error {
		return errors.New("not logged in")
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when the binary exists but auth fails")
	}
}

func TestCodexAvailableBinaryAbsent(t *testing.T) {
	resetCodexResolverForTest(t)
	codexGOOS = "linux"
	codexLookPath = func(file string) (string, error) { return "", errors.New("not found") }
	codexAuthProbe = func(ctx context.Context, binary string) error {
		t.Fatal("auth probe must not run when the binary is absent")
		return nil
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when the binary is absent")
	}
}

func TestCodexAvailableCachesResult(t *testing.T) {
	resetCodexResolverForTest(t)
	codexOnPath(t)
	calls := 0
	codexAuthProbe = func(ctx context.Context, binary string) error {
		calls++
		return nil
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	for i := 0; i < 5; i++ {
		if !p.Available(context.Background()) {
			t.Fatal("Available should be true")
		}
	}
	if calls != 1 {
		t.Fatalf("auth probe ran %d times within TTL, want 1", calls)
	}

	// Expire the cache; the probe must run again.
	p.availAt = time.Now().Add(-availCacheTTL - time.Second)
	p.Available(context.Background())
	if calls != 2 {
		t.Fatalf("auth probe ran %d times after TTL expiry, want 2", calls)
	}
}

// ── claude-code ──────────────────────────────────────────────────────────────

func stubClaudeAuthProbe(t *testing.T, out []byte, err error) *int {
	t.Helper()
	old := claudeCodeAuthProbe
	t.Cleanup(func() { claudeCodeAuthProbe = old })
	calls := new(int)
	claudeCodeAuthProbe = func(ctx context.Context, binary string) ([]byte, error) {
		*calls++
		return out, err
	}
	return calls
}

// claudeCodeTestProvider returns a provider whose cliBinary resolves on any
// host ("sh" is on PATH everywhere the tests run) so LookPath passes and the
// auth probe decides the outcome.
func claudeCodeTestProvider() *ClaudeCodeProvider {
	return NewClaudeCodeProvider("claude-code", ProviderConfig{Endpoint: "sh"}, nil)
}

func TestClaudeCodeAvailableBinaryPresentAuthGood(t *testing.T) {
	stubClaudeAuthProbe(t, []byte(`{"loggedIn":true}`), nil)
	p := claudeCodeTestProvider()
	if !p.Available(context.Background()) {
		t.Fatal("Available should be true with binary present and loggedIn=true")
	}
}

func TestClaudeCodeAvailableBinaryPresentAuthLoggedOut(t *testing.T) {
	stubClaudeAuthProbe(t, []byte(`{"loggedIn":false}`), nil)
	p := claudeCodeTestProvider()
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when auth status reports loggedIn=false")
	}
}

func TestClaudeCodeAvailableBinaryPresentProbeError(t *testing.T) {
	stubClaudeAuthProbe(t, nil, errors.New("exit status 1"))
	p := claudeCodeTestProvider()
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when the auth probe errors")
	}
}

func TestClaudeCodeAvailableBinaryPresentProbeGarbage(t *testing.T) {
	stubClaudeAuthProbe(t, []byte("not json"), nil)
	p := claudeCodeTestProvider()
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when the auth probe output is unparseable")
	}
}

func TestClaudeCodeAvailableBinaryAbsent(t *testing.T) {
	calls := stubClaudeAuthProbe(t, []byte(`{"loggedIn":true}`), nil)
	p := NewClaudeCodeProvider("claude-code", ProviderConfig{Endpoint: "definitely-not-a-real-binary-xyz"}, nil)
	if p.Available(context.Background()) {
		t.Fatal("Available must be false when the binary is absent")
	}
	if *calls != 0 {
		t.Fatal("auth probe must not run when the binary is absent")
	}
}

func TestClaudeCodeAvailableCachesResult(t *testing.T) {
	calls := stubClaudeAuthProbe(t, []byte(`{"loggedIn":true}`), nil)
	p := claudeCodeTestProvider()
	for i := 0; i < 5; i++ {
		if !p.Available(context.Background()) {
			t.Fatal("Available should be true")
		}
	}
	if *calls != 1 {
		t.Fatalf("auth probe ran %d times within TTL, want 1", *calls)
	}

	p.availAt = time.Now().Add(-availCacheTTL - time.Second)
	p.Available(context.Background())
	if *calls != 2 {
		t.Fatalf("auth probe ran %d times after TTL expiry, want 2", *calls)
	}
}

// ── pi ───────────────────────────────────────────────────────────────────────
//
// pi has no credential of its own; it fronts a local OpenAI-compatible backend
// (LM Studio by default). "Available" therefore means: binary on PATH AND the
// backend answers /v1/models. Binary presence alone must not report available.

func resetPiProbesForTest(t *testing.T) {
	t.Helper()
	origLook, origProbe := piLookPath, piBackendProbe
	t.Cleanup(func() { piLookPath, piBackendProbe = origLook, origProbe })
}

func piOnPath(t *testing.T) {
	t.Helper()
	piLookPath = func(file string) (string, error) { return "/usr/local/bin/pi", nil }
}

func newLocalPi() *PiProvider {
	// Empty config resolves to defaultLocalPiProvider (lmstudio).
	return NewPiProvider("pi", ProviderConfig{}, nil)
}

func TestPiAvailableBinaryPresentBackendUp(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	piBackendProbe = func(ctx context.Context, baseURL string) error { return nil }
	// Available also verifies the configured --provider is in pi's registry
	// (#629); stub a registry that has it so this test still isolates the
	// binary+backend behavior it's named for.
	stubPiHomeDir(t, "testdata/pi_home_present")

	if !newLocalPi().Available(context.Background()) {
		t.Fatal("Available should be true with binary present, backend up, and provider registered")
	}
}

func TestPiAvailableBinaryPresentBackendDown(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	stubPiHomeDir(t, "testdata/pi_home_present")
	piBackendProbe = func(ctx context.Context, baseURL string) error {
		return errors.New("connection refused")
	}

	if newLocalPi().Available(context.Background()) {
		t.Fatal("Available must be false when the binary exists but the backend is down — this was the LookPath-only defect")
	}
}

func TestPiAvailableBinaryAbsent(t *testing.T) {
	resetPiProbesForTest(t)
	piLookPath = func(file string) (string, error) { return "", errors.New("not found") }
	piBackendProbe = func(ctx context.Context, baseURL string) error {
		t.Fatal("backend probe must not run when the binary is absent")
		return nil
	}

	if newLocalPi().Available(context.Background()) {
		t.Fatal("Available must be false when the binary is absent")
	}
}

func TestPiAvailableProbesDefaultBackendURL(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	stubPiHomeDir(t, "testdata/pi_home_present")
	var got string
	piBackendProbe = func(ctx context.Context, baseURL string) error { got = baseURL; return nil }

	newLocalPi().Available(context.Background())
	if got != defaultPiBackendURL {
		t.Fatalf("probe hit %q, want default backend %q", got, defaultPiBackendURL)
	}
}

func TestPiAvailableCachesResult(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	stubPiHomeDir(t, "testdata/pi_home_present")
	calls := 0
	piBackendProbe = func(ctx context.Context, baseURL string) error { calls++; return nil }

	p := newLocalPi()
	p.Available(context.Background())
	p.Available(context.Background())
	if calls != 1 {
		t.Fatalf("probe ran %d times within availCacheTTL, want 1", calls)
	}
	// Expire the cache and confirm it re-probes.
	p.availAt = time.Now().Add(-2 * availCacheTTL)
	p.Available(context.Background())
	if calls != 2 {
		t.Fatalf("probe ran %d times after cache expiry, want 2", calls)
	}
}

// ── pi provider registry (#629) ─────────────────────────────────────────────
//
// pi maintains its own provider registry (agent/models.json under its home
// dir, the same source `pi --list-models` reads) independent of the engine's
// defaultLocalPiProvider config default. Comments prior to #629 claimed PR
// #417 already made "lmstudio" a safe default; it did not verify pi's
// registry actually has an "lmstudio" entry. These tests pin the fix:
// Available() must confirm the configured --provider exists in the registry,
// not merely that the backend HTTP probe answers — and this check must run
// for EVERY configured --provider, not just the compiled-in local default,
// otherwise a PiProvider configured for "ollama" (or any non-default
// provider) reopens the identical defect class untested (caught in review
// on the first cut of this PR).

func stubPiHomeDir(t *testing.T, dir string) {
	t.Helper()
	orig := piHomeDir
	piHomeDir = func() (string, error) { return dir, nil }
	t.Cleanup(func() { piHomeDir = orig })
}

func TestPiAvailable_ProviderMissingFromRegistry(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	piBackendProbe = func(ctx context.Context, baseURL string) error { return nil }
	stubPiHomeDir(t, "testdata/pi_home_missing")

	// newLocalPi() defaults provider to defaultLocalPiProvider ("lmstudio"),
	// which the fixture registry does not contain (only "ollama").
	if newLocalPi().Available(context.Background()) {
		t.Fatal("Available must be false when the configured --provider is missing from pi's registry")
	}
}

func TestPiAvailable_ProviderPresent(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	piBackendProbe = func(ctx context.Context, baseURL string) error { return nil }
	stubPiHomeDir(t, "testdata/pi_home_present")

	if !newLocalPi().Available(context.Background()) {
		t.Fatal("Available must be true when backend is up and the configured --provider exists in pi's registry")
	}
}

func piWithProvider(t *testing.T, provider string) *PiProvider {
	t.Helper()
	return NewPiProvider("pi", ProviderConfig{
		Options: map[string]interface{}{"provider": provider},
	}, nil)
}

// TestPiAvailable_NonDefaultProviderMissingFromRegistry pins the review
// finding on the first cut of #629: the registry check must not be gated
// behind `p.provider == defaultLocalPiProvider`. A PiProvider explicitly
// configured for a non-default provider (here "ollama", matching this
// repo's real ~/.pi registry state) that isn't actually in the registry
// must still report unavailable — with no backend probe involved, since
// ollama isn't the locally-probed backend.
func TestPiAvailable_NonDefaultProviderMissingFromRegistry(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	piBackendProbe = func(ctx context.Context, baseURL string) error {
		t.Fatal("backend probe must not run for a non-default provider")
		return nil
	}
	stubPiHomeDir(t, "testdata/pi_home_missing_ollama")

	if piWithProvider(t, "ollama").Available(context.Background()) {
		t.Fatal("Available must be false when a non-default configured --provider is missing from pi's registry")
	}
}

// TestPiAvailable_NonDefaultProviderPresent confirms a non-default provider
// that IS registered reports available without requiring the (inapplicable)
// local HTTP backend probe.
func TestPiAvailable_NonDefaultProviderPresent(t *testing.T) {
	resetPiProbesForTest(t)
	piOnPath(t)
	piBackendProbe = func(ctx context.Context, baseURL string) error {
		t.Fatal("backend probe must not run for a non-default provider")
		return nil
	}
	stubPiHomeDir(t, "testdata/pi_home_missing") // has "ollama", lacks "lmstudio"

	if !piWithProvider(t, "ollama").Available(context.Background()) {
		t.Fatal("Available must be true when a non-default configured --provider is present in pi's registry")
	}
}
