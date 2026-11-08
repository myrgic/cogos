//go:build !windows

package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDoctorModelProbes_DefaultIsUnknownWithFlag(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeHermesProfile(t, home, "http://127.0.0.1:6931", "x", "true")
	g := &DoctorGroup{Name: "t"}
	doctorModelProbes(g, t.TempDir(), DoctorOptions{host: newFakeProc(t).host(home)})
	c := groupCheck(g, "model probe")
	if c == nil || c.Status != StatusUnknown || !strings.Contains(c.Detail, "--live") || !strings.Contains(c.Detail, "claude-opus-5-5") {
		t.Fatalf("got %s", allDetail(g))
	}
}

func TestDoctorModelProbes_LiveReportsPerModelRefusal(t *testing.T) {
	var seen []string
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		auth = r.Header.Get("Authorization")
		seen = append(seen, req.Model)
		if req.MaxTokens != 1 {
			t.Errorf("max_tokens = %d", req.MaxTokens)
		}
		if req.Model == "claude-opus-5-5" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","code":"model_not_supported","message":"Claude Code 2.1.74 does not support this model; version 2.1.280 or newer is required"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeHermesProfile(t, home, "http://127.0.0.1:6931", "x", "true")
	grant := home + "/node-root-grant"
	writeFileMode(t, grant, secretCanary+"\n", 0o600)
	opts := DoctorOptions{Live: true, host: newFakeProc(t).host(home), kernelEndpoint: srv.URL, grantPath: grant}

	g := &DoctorGroup{Name: "t"}
	doctorModelProbes(g, t.TempDir(), opts)
	d := allDetail(g)
	if auth != "Bearer "+secretCanary {
		t.Errorf("probe did not authenticate")
	}
	if strings.Contains(d, secretCanary) {
		t.Fatalf("leak:\n%s", d)
	}
	bad := groupCheck(g, "model probe: claude-opus-5-5")
	if bad == nil || bad.Status != StatusFail || !strings.Contains(bad.Detail, "model_not_supported") || !strings.Contains(bad.Detail, "2.1.280") || !strings.Contains(bad.Detail, "hermes profile cog model.default") {
		t.Fatalf("got:\n%s", d)
	}
	if ok := groupCheck(g, "model probe: claude-sonnet-5"); ok == nil || ok.Status != StatusOK {
		t.Fatalf("alias target should be OK:\n%s", d)
	}
}

func TestDoctorModelProbes_CapNamesSkipped(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	var env strings.Builder
	for i := 0; i < modelProbeCap+3; i++ {
		env.WriteString("STAGE_MODEL_X" + string(rune('A'+i)) + "=m" + string(rune('a'+i)) + "\n")
	}
	t.Setenv("STAGE_STATE", home+"/stage")
	writeFileMode(t, home+"/stage/stage.env", env.String(), 0o644)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	grant := home + "/g"
	writeFileMode(t, grant, "tok", 0o600)
	g := &DoctorGroup{Name: "t"}
	doctorModelProbes(g, t.TempDir(), DoctorOptions{Live: true, host: newFakeProc(t).host(home), kernelEndpoint: srv.URL, grantPath: grant})
	sk := groupCheck(g, "model probe: skipped")
	if sk == nil || !strings.Contains(sk.Detail, "not probed") {
		t.Fatalf("want skipped note:\n%s", allDetail(g))
	}
}
