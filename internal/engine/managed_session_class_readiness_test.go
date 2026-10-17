package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/acprpc/acpfake"
)

func readinessClassConfig(t *testing.T, name string, rc RuntimeClass) *ManagedSessionsConfig {
	t.Helper()
	return &ManagedSessionsConfig{RuntimeClasses: map[string]RuntimeClass{name: rc}}
}

func readinessDriver(t *testing.T, cfg *ManagedSessionsConfig, mut func(*HermesACPDriverConfig)) *HermesACPDriver {
	t.Helper()
	reg, err := NewRuntimeClassRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dcfg := HermesACPDriverConfig{
		Classes:           reg,
		Env:               os.Environ(),
		InitTimeout:       5 * time.Second,
		CrashWindow:       time.Hour,
		RestartBackoff:    time.Millisecond,
		PermissionTimeout: 10 * time.Second,
	}
	if mut != nil {
		mut(&dcfg)
	}
	d := NewHermesACPDriver(dcfg)
	t.Cleanup(d.Close)
	return d
}

func TestClassReadiness_NonexistentArgvUnavailableAndRejectsCreate(t *testing.T) {
	cfg := readinessClassConfig(t, "bad", RuntimeClass{Argv: []string{"/no/such/binary-for-class-readiness"}})
	d := readinessDriver(t, cfg, nil)
	st := d.ClassStatus(context.Background(), "bad")
	if st.Status != "unavailable" {
		t.Fatalf("status = %q, want unavailable", st.Status)
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{Class: "bad", Cwd: t.TempDir()}); err == nil {
		t.Fatal("create with nonexistent argv should fail")
	}
	if len(d.List()) != 0 {
		t.Fatalf("expected no session row, got %d", len(d.List()))
	}
}

func TestClassReadiness_FakeAnswersReadyAndServesSession(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1"}
	cfg := readinessClassConfig(t, "fake", rc)
	d := readinessDriver(t, cfg, nil)
	st := d.ClassStatus(context.Background(), "fake")
	if st.Status != "ready" {
		t.Fatalf("status = %q, want ready (reason=%s)", st.Status, st.Reason)
	}
	if st.ProtocolVersion != 1 {
		t.Fatalf("protocol_version = %d", st.ProtocolVersion)
	}
	var caps map[string]any
	if err := json.Unmarshal(st.AgentCapabilities, &caps); err != nil || caps["loadSession"] != true {
		t.Fatalf("capabilities = %s", st.AgentCapabilities)
	}
	if st.Receipt == nil || st.Receipt.Class != "fake" {
		t.Fatalf("receipt = %+v", st.Receipt)
	}
	s, err := d.Create(context.Background(), HermesACPOpts{Class: "fake", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if s.State() != StateLive {
		t.Fatalf("state = %s", s.State())
	}
	turn, err := s.Prompt(json.RawMessage(`"hello"`))
	if err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, s, 0, func(e ManagedSessionEvent) bool {
		return e.Kind == MSEventTurnEnd
	})
	if !containsString(string(ev.Data), turn) {
		t.Fatalf("turn_end = %s", ev.Data)
	}
}

func TestClassReadiness_IncompatibleVersionRejectsCreate(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1", "ACPFAKE_PROTOCOL_VERSION": "2"}
	cfg := readinessClassConfig(t, "badver", rc)
	d := readinessDriver(t, cfg, nil)
	st := d.ClassStatus(context.Background(), "badver")
	if st.Status != "unavailable" {
		t.Fatalf("status = %q, want unavailable", st.Status)
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{Class: "badver", Cwd: t.TempDir()}); err == nil {
		t.Fatal("create with incompatible version should fail")
	}
	if len(d.List()) != 0 {
		t.Fatalf("expected no session row, got %d", len(d.List()))
	}
}

func TestClassReadiness_InitializeTimeoutCleansUp(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1", "ACPFAKE_INIT_HANG": "1"}
	cfg := readinessClassConfig(t, "hang", rc)
	d := readinessDriver(t, cfg, func(c *HermesACPDriverConfig) { c.InitTimeout = 200 * time.Millisecond })
	st := d.ClassStatus(context.Background(), "hang")
	if st.Status != "unavailable" {
		t.Fatalf("status = %q, want unavailable", st.Status)
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{Class: "hang", Cwd: t.TempDir()}); err == nil {
		t.Fatal("create with hanging initialize should fail")
	}
	if len(d.List()) != 0 {
		t.Fatalf("expected no session row, got %d", len(d.List()))
	}
}

func TestClassReadiness_CapabilityParsing(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1"}
	cfg := readinessClassConfig(t, "cap", rc)
	d := readinessDriver(t, cfg, nil)
	st := d.ProbeClass(context.Background(), "cap")
	if st.Status != "ready" {
		t.Fatalf("status = %q", st.Status)
	}
	var caps map[string]any
	if err := json.Unmarshal(st.AgentCapabilities, &caps); err != nil {
		t.Fatal(err)
	}
	if caps["loadSession"] != true {
		t.Fatalf("loadSession = %v", caps["loadSession"])
	}
}

func TestClassReadiness_ClassScopedListDoesNotProbeOrDiscloseForbidden(t *testing.T) {
	srv, ts := classServer(t)
	chatOnly := mintScopedTestGrant(t, srv, "guest-surface", ScopeManagedSessionsClassPrefix+"chat")
	var out struct {
		Classes []ManagedSessionClassStatus `json:"classes"`
	}
	code := g0Do(t, ts, chatOnly, "GET", "/v1/managed-sessions/classes", nil, &out)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if len(out.Classes) != 1 || out.Classes[0].Class != "chat" {
		t.Fatalf("classes = %+v, want only chat", out.Classes)
	}
	for _, c := range out.Classes {
		if c.Class == "collab" {
			t.Fatal("forbidden class disclosed")
		}
	}
}

func TestClassReadiness_AnonymousClassListDenied(t *testing.T) {
	_, ts := classServer(t)
	code := g0Do(t, ts, "", "GET", "/v1/managed-sessions/classes", nil, nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous status = %d, want 401", code)
	}
}

func containsString(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
