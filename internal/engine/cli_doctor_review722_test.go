//go:build !windows

package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// -- model probe: stage room preferences and the real kernel error envelope --

func TestConfiguredProbeModels_StageRoomsJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	state := t.TempDir()
	t.Setenv("STAGE_STATE", state)
	writeFileMode(t, filepath.Join(state, "stage.env"), "STAGE_VOICE=x\n", 0o644)
	writeFileMode(t, filepath.Join(state, "rooms.json"), `{"rooms":[
		{"id":"a","prefs":{"model":"claude-opus-5-5","model_catalog_choice":"custom:cogos:claude-fable-7"}},
		{"id":"b","prefs":{"model":"opus"},"messages":[{"model_id":"custom:cogos:claude-opus-5-5"}]},
		{"id":"c","model":"not-a-pref"}]}`, 0o644)

	models, notes := configuredProbeModels(DoctorOptions{host: newFakeProc(t).host(home)})
	got := map[string]string{}
	for _, m := range models {
		got[m.id] = strings.Join(m.sources, ";")
	}
	for _, want := range []string{"claude-opus-5-5", "claude-fable-7", "claude-opus-4-8"} {
		if !strings.Contains(got[want], "rooms.json") {
			t.Errorf("model %q not found from rooms.json: %v", want, got)
		}
	}
	if _, bad := got["not-a-pref"]; bad {
		t.Errorf("model outside prefs must not be picked up")
	}
	if !strings.Contains(strings.Join(notes, "|"), "kernel's own default model") {
		t.Errorf("unscanned kernel sources not named: %v", notes)
	}
}

func TestDoctorModelProbes_DefaultRunNamesUnscannedSources(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("STAGE_STATE", "")
	g := &DoctorGroup{Name: "t"}
	doctorModelProbes(g, t.TempDir(), DoctorOptions{host: newFakeProc(t).host(home)})
	c := groupCheck(g, "model probe")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "Not scanned") || !strings.Contains(c.Detail, "rooms.json") {
		t.Fatalf("got %s", allDetail(g))
	}
}

func TestClassifyProbeFailure_RealKernelEnvelope(t *testing.T) {
	// serve.go wraps every upstream failure as HTTP 500 / server_error / null code.
	refused := `{"error":{"type":"server_error","code":null,"message":"claude-oauth: status 400: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Claude Code 2.1.74 does not support this model; version 2.1.280 or newer is required\"}}"}}`
	st, d := classifyProbeFailure(500, []byte(refused))
	if st != StatusFail || !strings.Contains(d, "HTTP 400") || !strings.Contains(d, "invalid_request_error") || !strings.Contains(d, "2.1.280 or newer") {
		t.Fatalf("provider 4xx should be a refusal: %s %s", st, d)
	}
	transient := `{"error":{"type":"server_error","code":null,"message":"claude-oauth: status 529: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}"}}`
	if st, d := classifyProbeFailure(500, []byte(transient)); st != StatusUnknown || !strings.Contains(d, "overloaded_error") {
		t.Fatalf("provider 5xx is inconclusive: %s %s", st, d)
	}
	if st, _ := classifyProbeFailure(500, []byte(`{"error":{"type":"server_error","message":"context deadline exceeded"}}`)); st != StatusUnknown {
		t.Fatalf("kernel 500 without provider status is inconclusive, got %s", st)
	}
	if st, _ := classifyProbeFailure(429, []byte(`{}`)); st != StatusUnknown {
		t.Fatalf("429 is inconclusive, got %s", st)
	}
}

// -- Hermes resolution: profile .env, helper hang, shell shadow, grant vars --

func TestResolve_ProfileDotenvBeatsHelperUnlessOverride(t *testing.T) {
	home := t.TempDir()
	cfg := filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml")
	writeFileMode(t, filepath.Join(filepath.Dir(cfg), ".env"), "V=from-dotenv\n", 0o600)
	c := &hermesProfileCache{helper: map[string]helperResult{}, getenv: func(string) string { return "from-env" },
		run: func(string, time.Duration) (map[string]string, error) {
			return map[string]string{"V": "from-helper"}, nil
		}}
	p := hermesProfile{Name: "cog", Path: cfg, SecretsEnabled: true, SecretsCommand: "x"}
	if v, how, _, _ := c.resolve(p, "${env:V}"); v != "from-dotenv" || !strings.HasPrefix(how, ".env") {
		t.Fatalf("profile .env must win: %q %q", v, how)
	}
	p.OverrideExisting = true
	if v, how, _, _ := c.resolve(p, "${env:V}"); v != "from-helper" || !strings.Contains(how, "override_existing") {
		t.Fatalf("override_existing helper must win: %q %q", v, how)
	}
}

func TestHermesGrantHygiene_StaleProfileDotenvIsNotOK(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	const liveTok, staleTok = "live-token-aaaa", "old-token-bbbb"
	writeGrantLedger(t, root,
		[3]string{"node-root", staleTok, "identity.grant.issued"},
		[3]string{"node-root", liveTok, "identity.grant.superseded"},
	)
	// the helper prints the live grant, but the profile .env holds the stale one
	cfg := writeHermesProfile(t, home, "http://127.0.0.1:6931", liveTok, "false")
	writeFileMode(t, filepath.Join(filepath.Dir(cfg), ".env"), "COGOS_HERMES_GRANT="+staleTok+"\n", 0o600)
	opts := DoctorOptions{host: newFakeProc(t).host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	c := groupCheck(g, "hermes grant provenance: cog")
	if c == nil || c.Status != StatusFail || !strings.Contains(c.Detail, ".env") {
		t.Fatalf("want FAIL naming .env:\n%s", allDetail(g))
	}
	if strings.Contains(allDetail(g), staleTok) {
		t.Fatalf("leak")
	}
}

func TestHermesGrantHygiene_ShellEnvIsWarnNotProfileFail(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	const liveTok, staleTok = "live-token-aaaa", "old-token-bbbb"
	writeGrantLedger(t, root,
		[3]string{"node-root", staleTok, "identity.grant.superseded"},
		[3]string{"node-root", liveTok, "identity.grant.issued"},
	)
	writeHermesProfile(t, home, "http://127.0.0.1:6931", liveTok, "false")
	opts := DoctorOptions{host: newFakeProc(t).host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{},
		getenv: func(k string) string {
			if k == "COGOS_HERMES_GRANT" {
				return staleTok
			}
			return ""
		}}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	if c := groupCheck(g, "hermes shell env: cog"); c == nil || c.Status != StatusWarn || !strings.Contains(c.Detail, grantFingerprint(staleTok)) {
		t.Fatalf("want shell-env WARN:\n%s", allDetail(g))
	}
	if c := groupCheck(g, "hermes grant provenance: cog"); c == nil || c.Status != StatusOK {
		t.Fatalf("profile verdict must follow the helper's value:\n%s", allDetail(g))
	}
}

func TestHermesGrantHygiene_OnlyCogosGrantVarIsClassified(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	const liveTok = "live-token-aaaa"
	writeGrantLedger(t, root, [3]string{"node-root", liveTok, "identity.grant.issued"})
	cfg := writeHermesProfile(t, home, "http://127.0.0.1:6931", liveTok, "true")
	data, _ := os.ReadFile(cfg)
	writeFileMode(t, cfg, "providers:\n  anthropic:\n    base_url: https://api.example.test\n    api_key: ${env:ANTHROPIC_API_KEY}\n"+
		strings.TrimPrefix(string(data), "providers:\n"), 0o600)
	fp := newFakeProc(t)
	fp.add(20, 1000, "tool", map[string]string{"ANTHROPIC_API_KEY": "sk-foreign-never-in-ledger"}, "", 0)
	opts := DoctorOptions{host: fp.host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	if c := groupCheck(g, "hermes inherited env: running processes"); c == nil || c.Status != StatusOK {
		t.Fatalf("another provider's key is not a stale grant:\n%s", allDetail(g))
	}
	p := loadHermesProfiles(home)[0]
	if v := firstVarOf(p); v != "COGOS_HERMES_GRANT" {
		t.Fatalf("firstVarOf = %q", v)
	}
}

func TestRunHermesSecretsHelper_TimeoutKillsGrandchildren(t *testing.T) {
	start := time.Now()
	_, err := runHermesSecretsHelper("sleep 30 & sleep 30; echo X=1", 500*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("want timeout, got %v", err)
	}
	if el := time.Since(start); el > 8*time.Second {
		t.Fatalf("timeout not enforced on grandchildren: took %s", el)
	}
}

func TestRunHermesSecretsHelper_OutputCapped(t *testing.T) {
	vals, err := runHermesSecretsHelper("echo A=1; head -c 3000000 /dev/zero | tr '\\0' 'x'", 5*time.Second)
	if err != nil || vals["A"] != "1" {
		t.Fatalf("got %v %v", vals, err)
	}
}

func TestLoadHermesProfiles_HelperTimeoutSeconds(t *testing.T) {
	home := t.TempDir()
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml"),
		"secrets:\n  command:\n    enabled: true\n    command: x\n    helper_timeout_seconds: 7\n", 0o600)
	if p := loadHermesProfiles(home); len(p) != 1 || p[0].HelperTimeout != 7*time.Second {
		t.Fatalf("got %+v", p)
	}
}

// -- claude code: kernel started before the binary changed --

func TestDoctorClaudeCode_KernelOlderThanBinaryWarnsRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	bin := t.TempDir()
	writeFileMode(t, filepath.Join(bin, "claude"), "#!/bin/sh\necho '2.1.314 (Claude Code)'\n", 0o755)
	fp := newFakeProc(t)
	fp.add(100, 1000, "cogos", map[string]string{"PATH": bin}, "/system.slice/cogos-kernel.service", 6931)
	started := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(filepath.Join(fp.root, "100"), started, started); err != nil {
		t.Fatal(err)
	}
	opts := DoctorOptions{host: fp.host(home), kernelEndpoint: "http://127.0.0.1:6931"}
	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if c := groupCheck(g, "claude code"); c == nil || c.Status != StatusWarn || !strings.Contains(c.Detail, "restart the kernel") || !strings.Contains(c.Detail, "after the kernel process started") {
		t.Fatalf("got %s", allDetail(g))
	}

	// binary older than the kernel: OK
	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(filepath.Join(bin, "claude"), old, old); err != nil {
		t.Fatal(err)
	}
	g = &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if c := groupCheck(g, "claude code"); c == nil || c.Status != StatusOK {
		t.Fatalf("got %s", allDetail(g))
	}
}

// -- lookPath: one walker, windows PATHEXT --

func TestLookPathInOS_WindowsPathext(t *testing.T) {
	dir := t.TempDir()
	writeFileMode(t, filepath.Join(dir, "claude.exe"), "x", 0o644) // no exec bit: windows has none
	if got := lookPathInOS("claude", dir, "windows", ".COM;.EXE"); len(got) != 1 || !strings.HasSuffix(got[0], "claude.exe") {
		t.Fatalf("windows: %v", got)
	}
	if got := lookPathInOS("claude", dir, "linux", ""); len(got) != 0 {
		t.Fatalf("linux must not resolve claude.exe or non-exec files: %v", got)
	}
}

// -- supervision: the kernel's own listener is invisible --

func TestSupervision_StageInKernelUnitWithKernelInvisible(t *testing.T) {
	root := t.TempDir()
	writeFileMode(t, filepath.Join(root, ".cog", "config", "node", "manifest.yaml"), supervisionManifest, 0o644)
	fp := newFakeProc(t)
	fp.add(101, 1000, "python", nil, "/system.slice/cogos-kernel.service", 8080) // kernel (other user) not visible
	g := &DoctorGroup{Name: "t"}
	doctorServiceSupervision(g, root, DoctorOptions{host: fp.host(t.TempDir())})
	c := groupCheck(g, "service supervision: cogos-kernel.service")
	if c == nil || c.Status != StatusWarn || !strings.Contains(c.Detail, "stage") || !strings.Contains(c.Detail, "kernel") {
		t.Fatalf("got %s", allDetail(g))
	}
}

// A RunDoctor-level test with the ambient HOME must never execute the
// operator's real secrets.command helper.
func TestHelperValues_RefusesRealHomeUnderGoTest(t *testing.T) {
	c := newHermesProfileCache()
	p := hermesProfile{Path: "/Users/nobody/.hermes/profiles/cog/config.yaml", SecretsEnabled: true, SecretsCommand: "touch /nonexistent-canary"}
	if _, err := c.helperValues(p); err == nil || !strings.Contains(err.Error(), "not run under go test") {
		t.Fatalf("got %v", err)
	}
}
