// daemon_test.go — verifies that importing this package registers all 8
// expected Reconcilable providers with pkg/reconcile.
//
// This is the canonical test for verification gate 3: "after the
// engine.Main-equivalent boot path runs, reconcile.ListProviders() returns
// the 8 expected names (including identity — Wave 6b, and mlx-inference)."
//
// We don't start a server or invoke engine.Main() — we only confirm that
// the package's init() side-effect wired the providers. This is sufficient
// because cmd/cogos/providers_wire.go imports this package, so the same
// init() fires at daemon boot.
package daemon_test

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/myrgic/cogos/internal/providers/daemon"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"

	// Blank import fires daemon.init(), registering all 10 providers; the named
	// import above is for the new SetWorkspaceRoot regression test.
	_ "github.com/myrgic/cogos/internal/providers/daemon"
)

var expectedProviders = []string{
	"agent",
	"component",
	"discord",
	"eval",
	"identity",
	"mlx-inference",
	"pin",
	"service",
}

func TestDaemonInit_RegistersAll8Providers(t *testing.T) {
	got := reconcile.ListProviders()
	sort.Strings(got)

	want := make([]string, len(expectedProviders))
	copy(want, expectedProviders)
	sort.Strings(want)

	if len(got) < len(want) {
		t.Fatalf("provider count: got %d want at least %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}

	missing := []string{}
	for _, name := range want {
		if !reconcile.HasProvider(name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Errorf("missing providers after daemon init: %v\nregistered: %v", missing, got)
	}
}

func TestDaemonProviders_TypeMethodsMatchNames(t *testing.T) {
	for _, name := range expectedProviders {
		p, err := reconcile.GetProvider(name)
		if err != nil {
			t.Errorf("GetProvider(%q): %v", name, err)
			continue
		}
		if got := p.Type(); got != name {
			t.Errorf("provider %q: Type()=%q, want %q", name, got, name)
		}
	}
}

func TestDaemonProviders_HealthDoesNotPanic(t *testing.T) {
	for _, name := range expectedProviders {
		p, err := reconcile.GetProvider(name)
		if err != nil {
			t.Errorf("GetProvider(%q): %v", name, err)
			continue
		}
		// Health() must not panic. We don't assert on the return value
		// because it depends on filesystem state (workspace presence) which varies by environment.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("provider %q Health() panicked: %v", name, r)
				}
			}()
			_ = p.Health()
		}()
	}
}

// TestPinProvider_DaemonHealth_SurfacesDrift verifies Codex Bug 1:
// the daemon's pin provider calls the full Reconcile cycle before Health()
// so that drift declared in .cog/pins/*.yaml files is surfaced rather than
// silently reporting "no pins declared" (the pre-fix behaviour when pinStates
// was always empty because only LoadConfig was called).
//
// The test writes a pin file declaring a stale ref for a target that does not
// exist locally. FetchLive returns "unreachable" for any non-local target, so
// ComputePlan marks the pin as missing. Health() must therefore be Degraded,
// not Healthy.
func TestPinProvider_DaemonHealth_SurfacesDrift(t *testing.T) {
	// Build a temporary workspace with a .cog/pins/ directory and one pin
	// declaring a target that cannot be resolved locally.
	tmp := t.TempDir()
	pinsDir := filepath.Join(tmp, ".cog", "pins")
	if err := os.MkdirAll(pinsDir, 0o755); err != nil {
		t.Fatalf("mkdir pins: %v", err)
	}
	pinYAML := `target: myrgic/nonexistent-target
pin:
  ref: abc000000000
branch: main
sync: read-only
`
	if err := os.WriteFile(filepath.Join(pinsDir, "myrgic_nonexistent-target.yaml"), []byte(pinYAML), 0o644); err != nil {
		t.Fatalf("write pin yaml: %v", err)
	}

	// Also create the minimal workspace structure so agent / service providers
	// don't interfere.
	agentsDir := filepath.Join(tmp, ".cog", "bin", "agents")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		t.Fatalf("mkdir agents: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentsDir, "registry.yaml"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write registry.yaml: %v", err)
	}

	daemon.SetWorkspaceRoot(tmp)
	defer daemon.SetWorkspaceRoot("")

	p, err := reconcile.GetProvider("pin")
	if err != nil {
		t.Fatalf("GetProvider(pin): %v", err)
	}

	h := p.Health()
	// The target "myrgic/nonexistent-target" cannot be found locally,
	// so FetchLive marks it unreachable and ComputePlan emits a missing action.
	// Health() must NOT be Healthy — it must be Degraded.
	if h.Health == reconcile.HealthHealthy {
		t.Errorf("pin provider Health()=Healthy with a drifted pin file; want Degraded.\n"+
			"This indicates pinStates was empty (Reconcile not called before Health).\n"+
			"message: %q", h.Message)
	}
	// Specifically expect Degraded (missing/unreachable maps to Degraded in Health()).
	if h.Health != reconcile.HealthDegraded {
		t.Errorf("pin provider Health()=%s; want Degraded; message=%q", h.Health, h.Message)
	}
}

// TestSetWorkspaceRoot_HealthReflectsWorkspaceState verifies that after
// SetWorkspaceRoot is called with a real workspace path, providers that depend
// only on filesystem presence (agent, service) return a non-"workspace-not-found"
// status. This is the core regression test for the daemon workspace seam fix.
func TestSetWorkspaceRoot_HealthReflectsWorkspaceState(t *testing.T) {
	// Build a minimal fake workspace: <tmp>/.cog/bin/agents/registry.yaml
	// so the agent provider can return Healthy.
	tmp := t.TempDir()
	agentsDir := filepath.Join(tmp, ".cog", "bin", "agents")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		t.Fatalf("mkdir agents: %v", err)
	}
	registryPath := filepath.Join(agentsDir, "registry.yaml")
	if err := os.WriteFile(registryPath, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write registry.yaml: %v", err)
	}

	// Before SetWorkspaceRoot: agent Health() should say workspace-not-found
	// (or at least not be Healthy).
	daemon.SetWorkspaceRoot("")
	p, err := reconcile.GetProvider("agent")
	if err != nil {
		t.Fatalf("GetProvider(agent): %v", err)
	}
	before := p.Health()
	if before.Health == reconcile.HealthHealthy {
		t.Error("agent provider unexpectedly Healthy before SetWorkspaceRoot — test environment has a real workspace wired")
	}

	// After SetWorkspaceRoot with our fake workspace: agent should be Healthy.
	daemon.SetWorkspaceRoot(tmp)
	after := p.Health()
	if after.Health != reconcile.HealthHealthy {
		t.Errorf("agent provider Health()=%s after SetWorkspaceRoot(%s); want Healthy; message=%q",
			after.Health, tmp, after.Message)
	}

	// service provider should also be Healthy (no services dir = no services = fine).
	svc, err := reconcile.GetProvider("service")
	if err != nil {
		t.Fatalf("GetProvider(service): %v", err)
	}
	svcStatus := svc.Health()
	if svcStatus.Health != reconcile.HealthHealthy {
		t.Errorf("service provider Health()=%s; want Healthy; message=%q",
			svcStatus.Health, svcStatus.Message)
	}

	// Reset for other tests.
	daemon.SetWorkspaceRoot("")
}

// TestPinProvider_ConcurrentHealth_NoParallelRefresh verifies that concurrent
// Health() calls on the pin provider serialise correctly and do not trigger
// parallel RefreshState invocations (Bug B fix).
//
// The test fires N goroutines that each call p.Health() simultaneously on a
// workspace with a pin file. With the Bug B fix the mutex is held through the
// entire refresh, so the calls serialise rather than racing. Without the fix,
// concurrent stale-state checks both pass the staleness gate and both run
// RefreshState in parallel, which can race on pinStates writes.
//
// The test is also run with -race in CI (go test -race) to surface any data
// races that the serialisation fix is intended to prevent.
func TestPinProvider_ConcurrentHealth_NoParallelRefresh(t *testing.T) {
	tmp := t.TempDir()
	pinsDir := filepath.Join(tmp, ".cog", "pins")
	if err := os.MkdirAll(pinsDir, 0o755); err != nil {
		t.Fatalf("mkdir pins: %v", err)
	}
	pinYAML := `target: myrgic/nonexistent-target
pin:
  ref: abc000000000
branch: main
sync: read-only
`
	if err := os.WriteFile(filepath.Join(pinsDir, "myrgic_nonexistent-target.yaml"), []byte(pinYAML), 0o644); err != nil {
		t.Fatalf("write pin yaml: %v", err)
	}

	// Also create the minimal workspace structure so agent / service providers
	// don't interfere.
	agentsDir := filepath.Join(tmp, ".cog", "bin", "agents")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		t.Fatalf("mkdir agents: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentsDir, "registry.yaml"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write registry.yaml: %v", err)
	}

	daemon.SetWorkspaceRoot(tmp)
	defer daemon.SetWorkspaceRoot("")

	p, err := reconcile.GetProvider("pin")
	if err != nil {
		t.Fatalf("GetProvider(pin): %v", err)
	}

	const concurrency = 8
	var wg sync.WaitGroup
	statuses := make([]reconcile.ResourceStatus, concurrency)

	// Fire all goroutines at roughly the same time. All will see a stale
	// lastProbe (zero value). With the fix, only one runs RefreshState; the
	// others block on the mutex and see a fresh result when they acquire it.
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			defer wg.Done()
			statuses[i] = p.Health()
		}()
	}
	wg.Wait()

	// All goroutines must have received a coherent status — not a zero value.
	for i, s := range statuses {
		if s.Health == "" {
			t.Errorf("goroutine %d: Health() returned zero-value status", i)
		}
	}

	// The workspace has a pin that is unreachable locally, so every status
	// must be Degraded (not Healthy).
	for i, s := range statuses {
		if s.Health == reconcile.HealthHealthy {
			t.Errorf("goroutine %d: Health()=Healthy with unreachable pin; want Degraded; message=%q", i, s.Message)
		}
	}
}

// ── mlx-inference provider tests ──────────────────────────────────────────────

// TestMLXInference_Registered: mlx-inference provider is registered by init().
func TestMLXInference_Registered(t *testing.T) {
	if !reconcile.HasProvider("mlx-inference") {
		t.Fatal("mlx-inference provider not registered after daemon init()")
	}
}

// TestMLXInference_TypeMethod: Type() returns "mlx-inference".
func TestMLXInference_TypeMethod(t *testing.T) {
	p, err := reconcile.GetProvider("mlx-inference")
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	if got := p.Type(); got != "mlx-inference" {
		t.Errorf("Type()=%q; want mlx-inference", got)
	}
}

// TestMLXInference_HealthDoesNotPanic: Health() must never panic regardless of
// environment (missing workspace, no launchd, no mlx config).
func TestMLXInference_HealthDoesNotPanic(t *testing.T) {
	p, err := reconcile.GetProvider("mlx-inference")
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Health() panicked: %v", r)
		}
	}()
	_ = p.Health()
}

// TestMLXInference_HealthSuspendedWhenNoConfig: when the workspace has no
// mlx-supervised provider entries, Health() returns HealthSuspended — the
// feature is opt-in and absence is not an error.
func TestMLXInference_HealthSuspendedWhenNoConfig(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, ".cog", "config"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Write a providers.yaml with no mlx-supervised entries.
	if err := os.WriteFile(
		filepath.Join(tmp, ".cog", "config", "providers.yaml"),
		[]byte("providers:\n  ollama:\n    type: ollama\n    model: gemma4:e4b\n"),
		0o644,
	); err != nil {
		t.Fatalf("write providers.yaml: %v", err)
	}

	daemon.SetWorkspaceRoot(tmp)
	defer daemon.SetWorkspaceRoot("")

	p, err := reconcile.GetProvider("mlx-inference")
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Errorf("Health()=%q; want HealthSuspended when no mlx-supervised entries", h.Health)
	}
}

// TestMLXInference_HealthDegradedWhenConfiguredButDown: when an mlx-supervised
// provider is declared but the launchd label is not registered, Health()
// returns HealthDegraded (not Suspended, not Healthy).
func TestMLXInference_HealthDegradedWhenConfiguredButDown(t *testing.T) {
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, ".cog", "config"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Write a providers.yaml with an mlx-supervised entry pointing at a
	// non-existent launchd label (guaranteed not to be registered in CI).
	if err := os.WriteFile(
		filepath.Join(tmp, ".cog", "config", "providers.yaml"),
		[]byte("providers:\n  mlx-test:\n    type: mlx-supervised\n    endpoint: http://localhost:19876\n    model: /vol/no-such-model\n"),
		0o644,
	); err != nil {
		t.Fatalf("write providers.yaml: %v", err)
	}

	daemon.SetWorkspaceRoot(tmp)
	defer daemon.SetWorkspaceRoot("")

	p, err := reconcile.GetProvider("mlx-inference")
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	h := p.Health()
	// Either Degraded (launchd probe failed) or Progressing (rare race) but
	// never Healthy or Suspended when a config entry exists and the service is down.
	if h.Health == reconcile.HealthHealthy || h.Health == reconcile.HealthSuspended {
		t.Errorf("Health()=%q; want Degraded or Progressing when service is configured but down", h.Health)
	}
}
