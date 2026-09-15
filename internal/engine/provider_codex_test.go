package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeFileInfo struct {
	name string
	mode os.FileMode
	dir  bool
}

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return 1 }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.dir }
func (f fakeFileInfo) Sys() any           { return nil }

func resetCodexResolverForTest(t *testing.T) {
	t.Helper()

	oldLookPath := codexLookPath
	oldStat := codexStat
	oldGOOS := codexGOOS
	oldAuthProbe := codexAuthProbe
	t.Cleanup(func() {
		codexLookPath = oldLookPath
		codexStat = oldStat
		codexGOOS = oldGOOS
		codexAuthProbe = oldAuthProbe
	})
	// Default: auth probe succeeds. Tests exercising bad auth override this.
	codexAuthProbe = func(ctx context.Context, binary string) error { return nil }
}

func TestCodexResolveBinaryPrefersPath(t *testing.T) {
	resetCodexResolverForTest(t)

	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) {
		if file != "codex" {
			t.Fatalf("LookPath called with %q, want codex", file)
		}
		return "/usr/local/bin/codex", nil
	}
	codexStat = func(path string) (os.FileInfo, error) {
		t.Fatalf("stat should not be called when PATH resolves, got %q", path)
		return nil, errors.New("unreachable")
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	path, err := p.resolveBinary()
	if err != nil {
		t.Fatalf("resolveBinary returned error: %v", err)
	}
	if path != "/usr/local/bin/codex" {
		t.Fatalf("resolveBinary returned %q, want PATH result", path)
	}
}

func TestCodexResolveBinaryFallsBackToAppBundleOnDarwin(t *testing.T) {
	resetCodexResolverForTest(t)

	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) {
		return "", errors.New("not found")
	}
	codexStat = func(path string) (os.FileInfo, error) {
		if path == codexAppBundleBinary {
			return fakeFileInfo{name: "codex", mode: 0o755}, nil
		}
		return nil, os.ErrNotExist
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	path, err := p.resolveBinary()
	if err != nil {
		t.Fatalf("resolveBinary returned error: %v", err)
	}
	if path != codexAppBundleBinary {
		t.Fatalf("resolveBinary returned %q, want app bundle path", path)
	}
	if !p.Available(context.Background()) {
		t.Fatal("Available should use the app bundle fallback resolver")
	}
}

func TestCodexResolveBinarySkipsNonExecutableAppBundlePath(t *testing.T) {
	resetCodexResolverForTest(t)

	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) {
		return "", errors.New("not found")
	}
	codexStat = func(path string) (os.FileInfo, error) {
		if path == codexAppBundleBinary {
			return fakeFileInfo{name: "codex", mode: 0o644}, nil
		}
		return nil, os.ErrNotExist
	}

	p := NewCodexProvider("codex", ProviderConfig{})
	if _, err := p.resolveBinary(); err == nil {
		t.Fatal("resolveBinary should fail when the app bundle path is not executable")
	}
	if p.Available(context.Background()) {
		t.Fatal("Available should be false when the app bundle path is not executable")
	}
}

func TestCodexResolveBinaryDoesNotFallbackForExplicitEndpoint(t *testing.T) {
	resetCodexResolverForTest(t)

	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) {
		if file != "codex" {
			t.Fatalf("LookPath called with %q, want explicit endpoint", file)
		}
		return "", errors.New("not found")
	}
	codexStat = func(path string) (os.FileInfo, error) {
		t.Fatalf("stat should not be called for explicit endpoint, got %q", path)
		return nil, errors.New("unreachable")
	}

	p := NewCodexProvider("codex", ProviderConfig{Endpoint: "codex"})
	if _, err := p.resolveBinary(); err == nil {
		t.Fatal("resolveBinary should fail for an explicit endpoint that is not on PATH")
	}
	if p.Available(context.Background()) {
		t.Fatal("Available should be false for an unresolved explicit endpoint")
	}
}

func TestCodexResolveBinaryDoesNotFallbackForExplicitPath(t *testing.T) {
	resetCodexResolverForTest(t)

	codexGOOS = "darwin"
	codexLookPath = func(file string) (string, error) {
		if file != "/opt/codex/bin/codex" {
			t.Fatalf("LookPath called with %q, want explicit path", file)
		}
		return "", errors.New("not found")
	}
	codexStat = func(path string) (os.FileInfo, error) {
		t.Fatalf("stat should not be called for explicit path, got %q", path)
		return nil, errors.New("unreachable")
	}

	p := NewCodexProvider("codex", ProviderConfig{Endpoint: "/opt/codex/bin/codex"})
	if _, err := p.resolveBinary(); err == nil {
		t.Fatal("resolveBinary should fail for an explicit path that does not exist")
	}
}

// resetCodexHomeDirForTest overrides codexHomeDir to point at a fixture
// directory for the duration of the test, restoring the original on cleanup.
func resetCodexHomeDirForTest(t *testing.T, dir string) {
	t.Helper()
	old := codexHomeDir
	codexHomeDir = func() string { return dir }
	t.Cleanup(func() { codexHomeDir = old })
}

// TestCodexBuildArgs_NoFullAuto is the regression test for #627: codex-cli
// 0.154.0 removed the --full-auto flag (exit 2 if passed). buildArgs must
// never emit it, must still pass --sandbox, and must omit -m entirely when no
// model is configured (letting the CLI apply its own default) rather than
// passing an empty string as the model id.
//
// Negative control (removing the fix must fail this test): with
// `--full-auto` re-added to buildArgs, `git stash` + this test fails with:
//
//	provider_codex_test.go:NNN: buildArgs contains "--full-auto"; codex-cli 0.154.0 exits 2 on this flag (#627)
//
// confirming the test actually exercises the regression.
func TestCodexBuildArgs_NoFullAuto(t *testing.T) {
	resetCodexResolverForTest(t)

	p := NewCodexProvider("codex", ProviderConfig{Model: "gpt-5.6-terra"})
	args := p.buildArgs(&CompletionRequest{})

	for _, a := range args {
		if a == "--full-auto" {
			t.Fatalf(`buildArgs contains "--full-auto"; codex-cli 0.154.0 exits 2 on this flag (#627): %v`, args)
		}
	}

	foundSandbox := false
	for _, a := range args {
		if a == "--sandbox" {
			foundSandbox = true
		}
	}
	if !foundSandbox {
		t.Fatalf("buildArgs missing --sandbox: %v", args)
	}

	// With no model configured, -m must be omitted entirely.
	p2 := NewCodexProvider("codex", ProviderConfig{})
	p2.model = "" // force the "no model discovered" case regardless of host ~/.codex
	args2 := p2.buildArgs(&CompletionRequest{})
	for _, a := range args2 {
		if a == "-m" {
			t.Fatalf("buildArgs included -m with no model configured: %v", args2)
		}
	}
}

func TestCodexListModels_FromCache(t *testing.T) {
	resetCodexResolverForTest(t)
	resetCodexHomeDirForTest(t, "testdata/codex_home_fixture")

	p := NewCodexProvider("codex", ProviderConfig{})
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels returned error: %v", err)
	}
	want := []string{"gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.5"}
	if len(models) != len(want) {
		t.Fatalf("ListModels returned %d slugs, want %d: %v", len(models), len(want), models)
	}
	for i, w := range want {
		if models[i] != w {
			t.Fatalf("ListModels[%d] = %q, want %q (full: %v)", i, models[i], w, models)
		}
	}
}

func TestCodexDefaultModel_FromConfigToml(t *testing.T) {
	resetCodexHomeDirForTest(t, "testdata/codex_home_fixture")

	got := codexDefaultModel()
	if got != "x" {
		t.Fatalf("codexDefaultModel() = %q, want %q", got, "x")
	}
}

func TestCodexDefaultModel_MissingFileReturnsEmpty(t *testing.T) {
	resetCodexHomeDirForTest(t, "testdata/does-not-exist")

	got := codexDefaultModel()
	if got != "" {
		t.Fatalf("codexDefaultModel() = %q, want empty string when config.toml is missing", got)
	}
}

// TestCodexParse_GoldenNDJSON runs the provider's real NDJSON event parser
// over a golden fixture captured from an actual `codex exec --json` invocation
// (codex-cli 0.154.0, see internal/engine/testdata/codex_exec_pong.ndjson),
// guarding against silent drift in the CLI's event schema.
func TestCodexParse_GoldenNDJSON(t *testing.T) {
	data, err := os.ReadFile("testdata/codex_exec_pong.ndjson")
	if err != nil {
		t.Fatalf("reading golden fixture: %v", err)
	}

	p := &CodexProvider{name: "codex"}
	var gotText string
	var gotUsage *TokenUsage
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		text, usage, _ := p.parseEventLine([]byte(line))
		if text != "" {
			gotText = text
		}
		if usage != nil {
			gotUsage = usage
		}
	}

	if gotText != "pong" {
		t.Fatalf("parsed agent_message text = %q, want %q", gotText, "pong")
	}
	if gotUsage == nil {
		t.Fatal("expected turn.completed usage to be parsed, got nil")
	}
	if gotUsage.InputTokens == 0 {
		t.Fatalf("expected non-zero InputTokens from golden usage block, got %+v", gotUsage)
	}
}
