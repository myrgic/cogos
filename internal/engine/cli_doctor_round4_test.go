//go:build !windows

package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// R3-F2: the helper prints a stale grant and the shell exports the SAME value
// (a shell Hermes spawned). The profile is broken; the verdict must not
// depend on the shell, so both checks FAIL.
func TestF2_ShellEqualsStaleHelperStillFails(t *testing.T) {
	const liveTok, staleTok = "live-token-aaaa", "old-token-bbbb"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+liveTok {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"invalid_grant"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	for _, shell := range []string{staleTok, ""} {
		home := t.TempDir()
		t.Setenv("HOME", home)
		t.Setenv("STAGE_STATE", "")
		t.Setenv("COGOS_HERMES_GRANT", shell)
		writeHermesProfile(t, home, srv.URL, staleTok, "false")
		root := t.TempDir()
		kernelYAMLFromURL(t, root, srv.URL)
		writeGrantLedger(t, root, [3]string{"node-root", liveTok, "identity.grant.issued"})
		g := &DoctorGroup{Name: "t"}
		doctorExternalClientsToKernelWithHome(g, root, DoctorOptions{}, home)
		doctorHermesGrantHygiene(g, root, DoctorOptions{host: newFakeProc(t).host(home)})
		if pv := groupCheck(g, "hermes grant provenance: cog"); pv == nil || pv.Status != StatusFail {
			t.Errorf("shell=%q: provenance should FAIL, got %+v", shell, pv)
		}
		if ec := groupCheck(g, "external client: ~/.hermes"); ec == nil || ec.Status != StatusFail {
			t.Errorf("shell=%q: external client should FAIL, got %+v", shell, ec)
		}
	}
}

// R3-F4: the real /v1/models lists only foreground/deliberation/local; the
// kernel-accepted aliases must still be probed/named.
func TestConfiguredProbeModels_RealShapedCatalog(t *testing.T) {
	models, notes := configuredProbeModels(t.TempDir(), DoctorOptions{host: newFakeProc(t).host(t.TempDir())},
		[]string{"foreground", "deliberation", "local", "claude-sonnet-5", "openrouter/x"})
	got := map[string]bool{}
	for _, m := range models {
		got[m.id] = true
	}
	for _, want := range []string{"haiku", "sonnet", "opus", "fable", "foreground"} {
		if !got[want] {
			t.Errorf("alias %q not probed: %v", want, got)
		}
	}
	n := strings.Join(notes, "|")
	for _, a := range []string{"claude", "codex", "kernel-agent", "ollama"} {
		if !strings.Contains(n, a) {
			t.Errorf("unprobed alias %q not named: %s", a, n)
		}
	}
}

// R3-W9: the foreign-grant fix text names the target's own source, not the
// hermes surface with an empty path.
func TestGrantProvenanceClause_ForeignNamesTargetSurface(t *testing.T) {
	root := t.TempDir()
	writeGrantLedger(t, root, [3]string{"node-root", "live-token-aaaa", "identity.grant.issued"})
	ledger := loadGrantLedger(root)
	tgt := externalClientTarget{
		source:  "~/.claude.json (mcpServers.cogos)",
		headers: map[string]string{"Authorization": "Bearer foreign-token-zzzz"},
	}
	out := grantProvenanceClause(tgt, ledger)
	if strings.Contains(out, "hermes") || strings.Contains(out, "in ;") {
		t.Errorf("wrong surface / empty path: %s", out)
	}
	if !strings.Contains(out, "~/.claude.json") {
		t.Errorf("target source not named: %s", out)
	}
}
