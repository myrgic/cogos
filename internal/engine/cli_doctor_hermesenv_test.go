//go:build !windows

package engine

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureProfile = `providers:
  cogos:
    base_url: %s/v1
    api_key: ${env:COGOS_HERMES_GRANT}
secrets:
  command:
    enabled: true
    command: %q
    override_existing: %s
model:
  default: claude-opus-5-5
`

// writeHermesProfile writes a profile whose secrets helper is a fake script
// printing COGOS_HERMES_GRANT=<token>. The script path is returned.
func writeHermesProfile(t *testing.T, home, baseURL, token, override string) string {
	t.Helper()
	helper := filepath.Join(home, "helper.sh")
	writeFileMode(t, helper, "#!/bin/sh\necho COGOS_HERMES_GRANT="+token+"\n", 0o755)
	cfg := filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml")
	writeFileMode(t, cfg, strings.NewReplacer().Replace(sprintfProfile(baseURL, helper, override)), 0o600)
	return cfg
}

func sprintfProfile(baseURL, helper, override string) string {
	s := fixtureProfile
	s = strings.Replace(s, "%s/v1", baseURL+"/v1", 1)
	s = strings.Replace(s, "%q", `"`+helper+`"`, 1)
	s = strings.Replace(s, "%s", override, 1)
	return s
}

func noEnv(string) string { return "" }

// F2: ${env:VAR} resolved via the helper; grant not in the ledger -> foreign, no false 401 claim from a literal "${env:...}".
func TestExternalClient_ResolvesEnvRefViaHelperAndClassifiesForeign(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_grant"}}`))
	}))
	defer srv.Close()
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeHermesProfile(t, home, srv.URL, secretCanary, "true")
	root := t.TempDir()
	kernelYAMLFromURL(t, root, srv.URL)
	writeGrantLedger(t, root, [3]string{"node-root", "some-other-token", "identity.grant.issued"})

	opts := DoctorOptions{hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorExternalClientsToKernelWithHome(g, root, opts, home)

	if gotAuth != "Bearer "+secretCanary {
		t.Fatalf("kernel saw %q, want the helper's resolved value (not the literal reference)", gotAuth)
	}
	d := allDetail(g)
	if strings.Contains(d, secretCanary) {
		t.Fatalf("output leaks the grant:\n%s", d)
	}
	for _, want := range []string{"NOT issued by this kernel", "sha256:" + grantFingerprint(secretCanary), "another node"} {
		if !strings.Contains(d, want) {
			t.Errorf("missing %q in:\n%s", want, d)
		}
	}
}

func TestExternalClient_UnresolvableEnvRefIsUnknownNotFail(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml")
	writeFileMode(t, cfg, "providers:\n  cogos:\n    base_url: http://127.0.0.1:6931/v1\n    api_key: ${env:COGOS_HERMES_GRANT}\n", 0o600)
	g := &DoctorGroup{Name: "t"}
	doctorExternalClientsToKernelWithHome(g, t.TempDir(), DoctorOptions{hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}, home)
	c := groupCheck(g, "external client: ~/.hermes")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "not resolvable") {
		t.Fatalf("got %s", allDetail(g))
	}
}

func TestResolve_OverrideExistingPrecedence(t *testing.T) {
	c := &hermesProfileCache{helper: map[string]helperResult{}, getenv: func(string) string { return "from-env" },
		run: func(string) (map[string]string, error) { return map[string]string{"V": "from-helper"}, nil }}
	p := hermesProfile{Path: "p", SecretsEnabled: true, SecretsCommand: "x"}
	if v, how, _, _ := c.resolve(p, "${env:V}"); v != "from-env" || how != "process env" {
		t.Fatalf("default precedence: %q %q", v, how)
	}
	p.OverrideExisting = true
	if v, how, _, _ := c.resolve(p, "${env:V}"); v != "from-helper" || !strings.Contains(how, "override_existing") {
		t.Fatalf("override precedence: %q %q", v, how)
	}
}

// F2 offline provenance + F3 override warn + F3 stale inherited process grant.
func TestHermesGrantHygiene_ProvenanceOverrideAndStaleProcess(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	const liveTok, staleTok = "live-token-aaaa", "old-token-bbbb"
	writeGrantLedger(t, root,
		[3]string{"node-root", staleTok, "identity.grant.issued"},
		[3]string{"node-root", liveTok, "identity.grant.superseded"},
	)
	writeHermesProfile(t, home, "http://127.0.0.1:6931", liveTok, "false")

	fp := newFakeProc(t)
	fp.add(10, 1000, "hermes", map[string]string{"COGOS_HERMES_GRANT": staleTok}, "/user.slice/x.service", 0)
	fp.add(11, 1000, "stage", map[string]string{"COGOS_HERMES_GRANT": liveTok}, "/user.slice/y.service", 0)
	fp.add(12, 0, "rootproc", map[string]string{"COGOS_HERMES_GRANT": "foreign-token-cccc"}, "", 0) // other user: not inspected
	fp.add(13, 1000, "other", map[string]string{"COGOS_HERMES_GRANT": "foreign-token-dddd"}, "", 0)
	opts := DoctorOptions{host: fp.host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}

	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	d := allDetail(g)
	for _, tok := range []string{liveTok, staleTok, "foreign-token-cccc", "foreign-token-dddd"} {
		if strings.Contains(d, tok) {
			t.Fatalf("output leaks %q:\n%s", tok, d)
		}
	}
	if c := groupCheck(g, "hermes inherited env: cog"); c == nil || c.Status != StatusWarn || !strings.Contains(c.Detail, "override_existing: true") {
		t.Errorf("want override_existing WARN, got:\n%s", d)
	}
	if c := groupCheck(g, "hermes grant provenance: cog"); c == nil || c.Status != StatusOK || !strings.Contains(c.Detail, "issued by this kernel (surface node-root)") {
		t.Errorf("want live provenance OK, got:\n%s", d)
	}
	c := groupCheck(g, "hermes inherited env: running processes")
	if c == nil || c.Status != StatusWarn {
		t.Fatalf("want stale-process WARN, got:\n%s", d)
	}
	for _, want := range []string{"pid 10", "sha256:" + grantFingerprint(staleTok), "no longer live", "pid 13", "NOT issued"} {
		if !strings.Contains(c.Detail, want) {
			t.Errorf("missing %q in %s", want, c.Detail)
		}
	}
	if strings.Contains(c.Detail, "pid 11") || strings.Contains(c.Detail, "pid 12") {
		t.Errorf("live holder or other-user process wrongly reported: %s", c.Detail)
	}
}

func TestHermesGrantHygiene_OverrideTrueIsQuietAndForeignProfileGrantFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	writeGrantLedger(t, root, [3]string{"node-root", "some-live-one", "identity.grant.issued"})
	writeHermesProfile(t, home, "http://127.0.0.1:6931", "another-nodes-grant", "true")
	fp := newFakeProc(t)
	opts := DoctorOptions{host: fp.host(home), hermes: &hermesProfileCache{helper: map[string]helperResult{}, getenv: noEnv}}
	g := &DoctorGroup{Name: "t"}
	doctorHermesGrantHygiene(g, root, opts)
	if c := groupCheck(g, "hermes inherited env: cog"); c != nil {
		t.Errorf("override_existing true should not warn: %s", c.Detail)
	}
	if c := groupCheck(g, "hermes grant provenance: cog"); c == nil || c.Status != StatusFail || !strings.Contains(c.Detail, "NOT issued by this kernel") {
		t.Errorf("want FAIL foreign:\n%s", allDetail(g))
	}
	if c := groupCheck(g, "hermes inherited env: running processes"); c == nil || c.Status != StatusOK {
		t.Errorf("want OK (no holders):\n%s", allDetail(g))
	}
}
