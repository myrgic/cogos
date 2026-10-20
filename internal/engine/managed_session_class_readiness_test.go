package engine

import (
	"context"
	"encoding/json"
	"errors"
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

// Review #704 round 1, finding 1: a cached readiness result must expire. A
// transient first-probe failure may not mark a usable class unavailable for
// the life of the kernel.
func TestClassReadiness_CachedResultExpiresAfterTTL(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1"}
	cfg := readinessClassConfig(t, "flaky", rc)
	now := time.Now()
	d := readinessDriver(t, cfg, func(c *HermesACPDriverConfig) {
		c.ProbeCacheTTL = time.Minute
		c.now = func() time.Time { return now }
	})
	// Seed what a transient first-probe failure leaves behind.
	d.markClassUnavailable("flaky", errors.New("initialize timed out"))
	if st := d.ClassStatus(context.Background(), "flaky"); st.Status != "unavailable" {
		t.Fatalf("inside the TTL the cached result should be served, got %q", st.Status)
	}
	now = now.Add(2 * time.Minute)
	if st := d.ClassStatus(context.Background(), "flaky"); st.Status != "ready" {
		t.Fatalf("past the TTL the class must be re-probed: status = %q reason=%s", st.Status, st.Reason)
	}
}

// Review #704 round 1, finding 2: when a class's process crashes out of its
// restart budget the cached "ready" receipt is stale and must be dropped.
func TestClassReadiness_CrashExhaustionInvalidatesCachedReady(t *testing.T) {
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1"}
	cfg := readinessClassConfig(t, "fake", rc)
	d := readinessDriver(t, cfg, func(c *HermesACPDriverConfig) { c.ProbeCacheTTL = 24 * time.Hour })
	s, err := d.Create(context.Background(), HermesACPOpts{Class: "fake", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := d.getCachedClassReadiness("fake"); !ok || st.Status != "ready" {
		t.Fatalf("create should cache ready, got %+v ok=%v", st, ok)
	}
	_, _ = s.Prompt(json.RawMessage(`"crash now"`)) // CrashWindow = 1h: terminal
	waitState(t, s, StateCrashed)
	if st, ok := d.getCachedClassReadiness("fake"); ok {
		t.Fatalf("crash exhaustion left a cached readiness entry: %+v", st)
	}
}

func slowInitClass(t *testing.T, name string) *HermesACPDriver {
	t.Helper()
	rc := RuntimeClass{Argv: []string{os.Args[0], "-test.run=^$"}}
	rc.Env.Set = map[string]string{acpfake.EnvVar: "1", "ACPFAKE_INIT_DELAY": "1s"}
	return readinessDriver(t, readinessClassConfig(t, name, rc), func(c *HermesACPDriverConfig) { c.ProbeCacheTTL = time.Minute })
}

// Review #704 round 3, finding 1: a probe whose own caller goes away
// mid-initialize must not leave an "unavailable" entry for everyone else.
func TestClassReadiness_CallerCancelDuringProbeIsNotCached(t *testing.T) {
	d := slowInitClass(t, "slow")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = d.ClassStatus(ctx, "slow") // the caller's ctx ends mid-initialize
	if st, ok := d.getCachedClassReadiness("slow"); ok && st.Status == "unavailable" {
		t.Fatalf("caller cancellation was cached as unavailable: %+v", st)
	}
	if st := d.ClassStatus(context.Background(), "slow"); st.Status != "ready" {
		t.Fatalf("second caller: status = %q reason=%s, want ready", st.Status, st.Reason)
	}
}

// Review #704 round 3, finding 2: a create whose request ctx ends
// mid-start must not mark the class unavailable.
func TestClassReadiness_CallerCancelDuringCreateIsNotCached(t *testing.T) {
	d := slowInitClass(t, "slow")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := d.Create(ctx, HermesACPOpts{Class: "slow", Cwd: t.TempDir()}); err == nil {
		t.Fatal("create with a cancelled ctx should fail")
	}
	if st, ok := d.getCachedClassReadiness("slow"); ok && st.Status == "unavailable" {
		t.Fatalf("caller cancellation marked the class unavailable: %+v", st)
	}
}
