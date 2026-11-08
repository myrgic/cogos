//go:build !windows

package engine

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRoomModelsFromJSON_CurrentPrefsOnlyAndOpencodeApart(t *testing.T) {
	models, outside := roomModelsFromJSON([]byte(`{"rooms":[
		{"id":"a","prefs":{"model":"codex/gpt-6-luna"},
		 "board":[{"state":"done","runtime":"opencode","model_id":"opencode/deepseek-v4.1-flash-free"}],
		 "work_queue":[{"model_id":"claude-history-only"}]},
		{"id":"b","prefs":{"model":"opencode/x-free","model_catalog_choice":"custom:cogos:claude-fable-7"}}]}`))
	if strings.Join(models, ",") != "codex/gpt-6-luna,claude-fable-7" {
		t.Fatalf("models = %v", models)
	}
	if strings.Join(outside, ",") != "opencode/x-free" {
		t.Fatalf("outside = %v", outside)
	}
}

func TestConfiguredProbeModels_HermesProviderRoutingAndLiveAliases(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("STAGE_STATE", "")
	for name, model := range map[string]string{
		"viakernel": "{default: m-kernel, provider: cogos}",
		"direct":    "{default: m-direct, provider: openrouter}",
		"localurl":  "{default: m-local, base_url: 'http://127.0.0.1:6931/v1'}",
		"otherport": "{default: m-other, base_url: 'http://127.0.0.1:6932/v1'}",
	} {
		writeFileMode(t, filepath.Join(home, ".hermes", "profiles", name, "config.yaml"), "model: "+model+"\n", 0o600)
	}
	models, notes := configuredProbeModels(t.TempDir(), DoctorOptions{host: newFakeProc(t).host(home)}, []string{"foreground", "sonnet", "claude-sonnet-5", "something-else"})
	got := map[string]bool{}
	for _, m := range models {
		got[m.id] = true
	}
	for _, want := range []string{"m-kernel", "m-local", "foreground", "sonnet"} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
	for _, bad := range []string{"m-direct", "m-other", "something-else"} {
		if got[bad] {
			t.Errorf("%q must not be probed", bad)
		}
	}
	n := strings.Join(notes, "|")
	if !strings.Contains(n, "m-direct") || !strings.Contains(n, "openrouter") || !strings.Contains(n, "m-other") {
		t.Errorf("unrouted not named: %s", n)
	}
	if strings.Contains(n, "aliases above come from this doctor binary") {
		t.Errorf("live catalog was supplied; compiled-table caveat must be gone: %s", n)
	}
}

func TestProbeOneModel_DeadlineIsNotUnreachable(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	old := modelProbeTimeout
	modelProbeTimeout = 50 * time.Millisecond
	defer func() { modelProbeTimeout = old }()
	if st, d := probeOneModel(srv.URL, "t", "m"); st != StatusUnknown || strings.Contains(d, "unreachable") || !strings.Contains(d, "no answer") {
		t.Fatalf("%s %s", st, d)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	if st, d := probeOneModel(closed.URL, "t", "m"); st != StatusUnknown || !strings.Contains(d, "unreachable") {
		t.Fatalf("%s %s", st, d)
	}
}

func TestHermesGrantHygiene_PortMismatchIsUnknownNotFail(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	writeGrantLedger(t, root, [3]string{"node-root", "this-kernels-grant", "identity.grant.issued"})
	writeHermesProfile(t, home, "http://127.0.0.1:6931", "other-kernels-grant", "true")
	opts := DoctorOptions{host: newFakeProc(t).host(home), kernelEndpoint: "http://127.0.0.1:6932",
		hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	c := groupCheck(g, "hermes grant provenance: cog")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "6931") || !strings.Contains(c.Detail, "6932") {
		t.Fatalf("got:\n%s", allDetail(g))
	}
}

func TestHermesGrantHygiene_WriteRouteAuthDisabledIsNotFail(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	writeFileMode(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), "disable_write_route_grant_auth: true\n", 0o644)
	writeGrantLedger(t, root, [3]string{"node-root", "this-kernels-grant", "identity.grant.issued"})
	writeHermesProfile(t, home, "http://127.0.0.1:6931", "other-kernels-grant", "true")
	opts := DoctorOptions{host: newFakeProc(t).host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	c := groupCheck(g, "hermes grant provenance: cog")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "disable_write_route_grant_auth") {
		t.Fatalf("got:\n%s", allDetail(g))
	}
}

func TestRedactKeyRef_NeverEchoesLiteralPart(t *testing.T) {
	for _, raw := range []string{"SECRETLITERALPART${env:NOPE_UNSET_VAR}", "sk-literal-key"} {
		if out := redactKeyRef(raw); strings.Contains(out, "SECRETLITERAL") || strings.Contains(out, "sk-literal") {
			t.Errorf("leak: %q", out)
		}
	}
	if out := redactKeyRef("${env:COGOS_HERMES_GRANT}"); out != "${env:COGOS_HERMES_GRANT}" {
		t.Errorf("pure ref should render as is: %q", out)
	}

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "p", "config.yaml"),
		"providers:\n  cogos:\n    base_url: http://127.0.0.1:6931/v1\n    api_key: SECRETLITERALPART${env:NOPE_UNSET_VAR}\n", 0o600)
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, t.TempDir(), DoctorOptions{host: newFakeProc(t).host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}})
	if d := allDetail(g); strings.Contains(d, "SECRETLITERALPART") || !strings.Contains(d, "NOPE_UNSET_VAR") {
		t.Fatalf("got:\n%s", d)
	}
	targets := collectHermesProfileTargetsWith(home, &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv})
	for _, tg := range targets {
		if strings.Contains(tg.credRef, "SECRETLITERALPART") {
			t.Fatalf("credRef leaks: %q", tg.credRef)
		}
	}
}

func TestHermesHelper_NotRunUnderSkipNetwork(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	marker := filepath.Join(home, "ran")
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "p", "config.yaml"),
		"providers:\n  cogos:\n    base_url: http://127.0.0.1:6931/v1\n    api_key: ${env:COGOS_HERMES_GRANT}\nsecrets:\n  command:\n    enabled: true\n    command: \"touch "+marker+"; echo COGOS_HERMES_GRANT=x\"\n    override_existing: true\n", 0o600)
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, t.TempDir(), DoctorOptions{SkipNetwork: true, host: newFakeProc(t).host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}})
	if fileExists(marker) {
		t.Fatal("secrets.command helper ran under --skip-network")
	}
	c := groupCheck(g, "hermes grant provenance: p")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "--skip-network") {
		t.Fatalf("got:\n%s", allDetail(g))
	}
}
