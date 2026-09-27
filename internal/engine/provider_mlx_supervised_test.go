// provider_mlx_supervised_test.go — unit tests for MLXSupervisedProvider.
//
// Test shape follows local_llm_test.go and the conventions in testhelper_test.go:
//   - Package engine (whitebox)
//   - t.TempDir() for filesystem isolation
//   - No sleeping; no live ports; no launchctl
//   - All supervisor interactions use ObserverSupervisor or a fake
package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// ── helpers ────────────────────────────────────────────────────────────────────

// makeMLXConfig returns a minimal ProviderConfig for type=mlx-supervised.
func makeMLXConfig(endpoint, model string) ProviderConfig {
	enabled := true
	return ProviderConfig{
		Type:     mlxSupervisedType,
		Endpoint: endpoint,
		Model:    model,
		Timeout:  30,
		Enabled:  &enabled,
	}
}

// makeMLXProvider builds an MLXSupervisedProvider with an ObserverSupervisor and
// a temp directory substituted as the home directory for plist path resolution.
// The plist path is overridden to a temp location so tests don't write to
// ~/Library/LaunchAgents/.
func makeMLXProvider(t *testing.T, name, endpoint, model string) *MLXSupervisedProvider {
	t.Helper()
	cfg := makeMLXConfig(endpoint, model)
	p, err := newMLXSupervisedProvider(name, cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("newMLXSupervisedProvider: %v", err)
	}
	// Override plist path to temp dir so we don't touch ~/Library/.
	tmpDir := t.TempDir()
	p.plistPath = filepath.Join(tmpDir, p.launchdLabel+".plist")
	return p
}

// mlxModelsHandler returns an httptest.Handler that serves a /v1/models response
// containing the given model IDs.
func mlxModelsHandler(models ...string) http.Handler {
	type model struct {
		ID string `json:"id"`
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		var data []model
		for _, m := range models {
			data = append(data, model{ID: m})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": data})
	})
}

// ── construction ───────────────────────────────────────────────────────────────

// TestNewMLXSupervisedProviderDefaults: default options produce expected label and port.
func TestNewMLXSupervisedProviderDefaults(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "mlx-test", "http://localhost:1235", "my-model")
	if p.Name() != "mlx-test" {
		t.Errorf("Name: got %q; want %q", p.Name(), "mlx-test")
	}
	if p.Model() != "my-model" {
		t.Errorf("Model: got %q; want %q", p.Model(), "my-model")
	}
	if p.launchdLabel != mlxDefaultLaunchdPrefix+"mlx-test" {
		t.Errorf("launchdLabel: got %q; want %q", p.launchdLabel, mlxDefaultLaunchdPrefix+"mlx-test")
	}
	if p.port != 1235 {
		t.Errorf("port: got %d; want 1235", p.port)
	}
	if p.binary != "mlx_lm.server" {
		t.Errorf("binary: got %q; want %q", p.binary, "mlx_lm.server")
	}
}

// TestNewMLXSupervisedProviderCustomLabel: launchd_label option is respected.
func TestNewMLXSupervisedProviderCustomLabel(t *testing.T) {
	t.Parallel()
	cfg := makeMLXConfig("http://localhost:1235", "model")
	cfg.Options = map[string]interface{}{
		"launchd_label": "com.example.custom",
	}
	p, err := newMLXSupervisedProvider("x", cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.launchdLabel != "com.example.custom" {
		t.Errorf("launchdLabel: got %q; want %q", p.launchdLabel, "com.example.custom")
	}
}

// TestNewMLXSupervisedProviderCustomBinary: binary option is respected.
func TestNewMLXSupervisedProviderCustomBinary(t *testing.T) {
	t.Parallel()
	cfg := makeMLXConfig("http://localhost:1235", "model")
	cfg.Options = map[string]interface{}{
		"binary": "/opt/homebrew/bin/mlx_lm.server",
	}
	p, err := newMLXSupervisedProvider("x", cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.binary != "/opt/homebrew/bin/mlx_lm.server" {
		t.Errorf("binary: got %q; want %q", p.binary, "/opt/homebrew/bin/mlx_lm.server")
	}
}

// TestNewMLXSupervisedProviderExtraArgs: args option is parsed from []interface{}.
func TestNewMLXSupervisedProviderExtraArgs(t *testing.T) {
	t.Parallel()
	cfg := makeMLXConfig("http://localhost:1235", "model")
	cfg.Options = map[string]interface{}{
		"args": []interface{}{"--max-tokens", "4096"},
	}
	p, err := newMLXSupervisedProvider("x", cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.extraArgs) != 2 || p.extraArgs[0] != "--max-tokens" || p.extraArgs[1] != "4096" {
		t.Errorf("extraArgs: got %v; want [--max-tokens 4096]", p.extraArgs)
	}
}

// ── port extraction ────────────────────────────────────────────────────────────

func TestExtractPort(t *testing.T) {
	t.Parallel()
	cases := []struct {
		endpoint string
		def      int
		want     int
	}{
		{"http://localhost:1235", 1234, 1235},
		{"http://127.0.0.1:8080/", 1234, 8080},
		{"http://localhost:1234/v1", 9999, 1234},
		{"", 1235, 1235},
		{"http://localhost", 1235, 1235},
		{"not-a-url", 4321, 4321},
	}
	for _, tc := range cases {
		got := extractPort(tc.endpoint, tc.def)
		if got != tc.want {
			t.Errorf("extractPort(%q, %d) = %d; want %d", tc.endpoint, tc.def, got, tc.want)
		}
	}
}

// ── plist generation ───────────────────────────────────────────────────────────

// TestWritePlist: writePlist produces a valid launchd plist at the expected path.
func TestWritePlist(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	cfg := makeMLXConfig("http://localhost:1235", "/Volumes/SD/model")
	p, err := newMLXSupervisedProvider("plist-test", cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	p.plistPath = filepath.Join(tmpDir, "com.cogos.mlx-plist-test.plist")

	if err := p.writePlist(); err != nil {
		t.Fatalf("writePlist: %v", err)
	}
	data, err := os.ReadFile(p.plistPath)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	content := string(data)
	for _, want := range []string{
		"com.cogos.mlx-plist-test",
		"mlx_lm.server",
		"--model",
		"/Volumes/SD/model",
		"--port",
		"1235",
		"--host",
		"127.0.0.1",
		"KeepAlive",
		"<true/>",
	} {
		if !mlxContains(content, want) {
			t.Errorf("plist missing %q\ncontent:\n%s", want, content)
		}
	}
}

// TestWritePlistXMLEscape: model paths with special chars are safely escaped.
func TestWritePlistXMLEscape(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	cfg := makeMLXConfig("http://localhost:1235", "/path/with <angle> & 'quotes'")
	p, err := newMLXSupervisedProvider("escape-test", cfg, &ObserverSupervisor{})
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	p.plistPath = filepath.Join(tmpDir, "test.plist")
	if err := p.writePlist(); err != nil {
		t.Fatalf("writePlist: %v", err)
	}
	data, _ := os.ReadFile(p.plistPath)
	content := string(data)
	if mlxContains(content, "<angle>") {
		t.Error("plist contains unescaped < — XML escaping broken")
	}
	if !mlxContains(content, "&lt;angle&gt;") {
		t.Error("plist missing &lt;angle&gt; — XML escaping not applied")
	}
}

// ── Health() ──────────────────────────────────────────────────────────────────

// TestHealthBeforeAnyProbe: unprobed provider reports Progressing.
func TestHealthBeforeAnyProbe(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "h-test", "http://localhost:1235", "model")
	h := p.Health()
	// Plist doesn't exist and no probe yet — should be Missing (plist absent).
	if h.Health != reconcile.HealthMissing && h.Health != reconcile.HealthProgressing {
		t.Errorf("Health before probe: got %q; want Missing or Progressing", h.Health)
	}
}

// TestHealthPlistAbsent: missing plist → OutOfSync / Missing.
func TestHealthPlistAbsent(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "no-plist", "http://localhost:1235", "model")
	// Inject a cached status saying process is running — plist is still absent.
	p.UpdateCachedStatus(&ServiceStatus{Running: true, At: time.Now()})
	p.mu.Lock()
	p.lastHTTP = true
	p.lastProbed = time.Now()
	p.mu.Unlock()
	h := p.Health()
	if h.Sync != reconcile.SyncStatusOutOfSync {
		t.Errorf("Sync: got %q; want OutOfSync", h.Sync)
	}
	if h.Health != reconcile.HealthMissing {
		t.Errorf("Health: got %q; want Missing", h.Health)
	}
}

// TestHealthProcessNotRunning: plist present, process stopped → Degraded.
func TestHealthProcessNotRunning(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "stopped", "http://localhost:1235", "model")
	// Create the plist file so the existence check passes.
	if err := os.WriteFile(p.plistPath, []byte("<?xml"), 0644); err != nil {
		t.Fatalf("write plist stub: %v", err)
	}
	// Inject: process not running, probe done.
	p.UpdateCachedStatus(&ServiceStatus{Running: false, LaunchdRegistered: true, At: time.Now()})
	p.mu.Lock()
	p.lastProbed = time.Now()
	p.mu.Unlock()
	h := p.Health()
	if h.Health != reconcile.HealthDegraded {
		t.Errorf("Health: got %q; want Degraded", h.Health)
	}
}

// TestHealthProcessRunningHTTPFailed: running but HTTP probe failed → Progressing.
func TestHealthProcessRunningHTTPFailed(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "loading", "http://localhost:1235", "model")
	if err := os.WriteFile(p.plistPath, []byte("<?xml"), 0644); err != nil {
		t.Fatalf("write plist stub: %v", err)
	}
	p.UpdateCachedStatus(&ServiceStatus{Running: true, PID: 12345, At: time.Now()})
	p.mu.Lock()
	p.lastHTTP = false
	p.lastProbed = time.Now()
	p.mu.Unlock()
	h := p.Health()
	if h.Health != reconcile.HealthProgressing {
		t.Errorf("Health: got %q; want Progressing", h.Health)
	}
}

// TestHealthAllGreen: plist present, running, HTTP OK → Synced / Healthy.
func TestHealthAllGreen(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "healthy", "http://localhost:1235", "model")
	if err := os.WriteFile(p.plistPath, []byte("<?xml"), 0644); err != nil {
		t.Fatalf("write plist stub: %v", err)
	}
	p.UpdateCachedStatus(&ServiceStatus{Running: true, PID: 42, At: time.Now()})
	p.mu.Lock()
	p.lastHTTP = true
	p.lastProbed = time.Now()
	p.mu.Unlock()
	h := p.Health()
	if h.Sync != reconcile.SyncStatusSynced {
		t.Errorf("Sync: got %q; want Synced", h.Sync)
	}
	if h.Health != reconcile.HealthHealthy {
		t.Errorf("Health: got %q; want Healthy", h.Health)
	}
	if h.Operation != reconcile.OperationIdle {
		t.Errorf("Operation: got %q; want Idle", h.Operation)
	}
}

// ── Available() ───────────────────────────────────────────────────────────────

// TestAvailableHitsHTTPEndpoint: Available() queries /v1/models and returns true
// when the server reports the configured model.
func TestAvailableHitsHTTPEndpoint(t *testing.T) {
	t.Parallel()
	modelID := "/Volumes/SD/gemma-4-mlx"
	srv := httptest.NewServer(mlxModelsHandler(modelID))
	defer srv.Close()

	p := makeMLXProvider(t, "avail-test", srv.URL, modelID)
	// Inject cached status indicating process is running.
	p.UpdateCachedStatus(&ServiceStatus{Running: true, PID: 1, At: time.Now()})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !p.Available(ctx) {
		t.Error("Available: got false; want true when server returns the model")
	}
}

// TestAvailableReturnsFalseOnServerDown: Available() returns false when the
// HTTP endpoint is not reachable (no launchd state injected either).
func TestAvailableReturnsFalseOnServerDown(t *testing.T) {
	t.Parallel()
	// Use a port that has no listener.
	p := makeMLXProvider(t, "down-test", "http://127.0.0.1:19999", "model")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if p.Available(ctx) {
		t.Error("Available: got true; want false when no server is listening")
	}
}

// TestAvailableShortCircuitsWhenNotRunning: if launchd cache says not running,
// Available() returns false without making an HTTP call.
func TestAvailableShortCircuitsWhenNotRunning(t *testing.T) {
	t.Parallel()
	// Any request to this handler means Available() made an HTTP call.
	probed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probed = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	p := makeMLXProvider(t, "short-circuit", srv.URL, "model")
	// Inject cached status: process is NOT running.
	p.UpdateCachedStatus(&ServiceStatus{Running: false, At: time.Now()})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := p.Available(ctx)
	if got {
		t.Error("Available: got true; want false when cached status shows not running")
	}
	if probed {
		t.Error("Available: made HTTP call despite cached not-running status (should short-circuit)")
	}
}

// ── ComputePlan ───────────────────────────────────────────────────────────────

// TestComputePlanAllPresent: when plist exists and process is running, plan is empty.
func TestComputePlanAllPresent(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "plan-noop", "http://localhost:1235", "model")
	if err := os.WriteFile(p.plistPath, []byte("<?xml"), 0644); err != nil {
		t.Fatalf("write plist stub: %v", err)
	}
	live := &ServiceStatus{Running: true}
	plan, err := p.ComputePlan(nil, live, nil)
	if err != nil {
		t.Fatalf("ComputePlan: %v", err)
	}
	if len(plan.Actions) != 0 {
		t.Errorf("ComputePlan: got %d action(s); want 0 when all present", len(plan.Actions))
	}
}

// TestComputePlanPlistMissing: plist absent → plan has a create action.
func TestComputePlanPlistMissing(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "plan-plist", "http://localhost:1235", "model")
	// plistPath points to temp dir but file is not written.
	live := &ServiceStatus{Running: true}
	plan, err := p.ComputePlan(nil, live, nil)
	if err != nil {
		t.Fatalf("ComputePlan: %v", err)
	}
	if plan.Summary.Creates != 1 {
		t.Errorf("ComputePlan: Creates = %d; want 1", plan.Summary.Creates)
	}
}

// TestComputePlanServiceNotRunning: process not running → plan has an update action.
func TestComputePlanServiceNotRunning(t *testing.T) {
	t.Parallel()
	p := makeMLXProvider(t, "plan-start", "http://localhost:1235", "model")
	if err := os.WriteFile(p.plistPath, []byte("<?xml"), 0644); err != nil {
		t.Fatalf("write plist stub: %v", err)
	}
	live := &ServiceStatus{Running: false}
	plan, err := p.ComputePlan(nil, live, nil)
	if err != nil {
		t.Fatalf("ComputePlan: %v", err)
	}
	if plan.Summary.Updates != 1 {
		t.Errorf("ComputePlan: Updates = %d; want 1", plan.Summary.Updates)
	}
}

// ── BuildRouter integration ───────────────────────────────────────────────────

// TestBuildRouterRegistersMLXSupervisedType: when providers.yaml declares a
// type=mlx-supervised entry, BuildRouter registers an MLXSupervisedProvider.
//
// mlx-supervised requires Apple Metal; BuildRouter's makeProvider gate
// (router.go, case mlxSupervisedType) skips it on non-darwin platforms. The
// registration assertions below therefore only hold on darwin.
func TestBuildRouterRegistersMLXSupervisedType(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("mlx-supervised only registers on darwin")
	}
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `
providers:
  mlx-gemma:
    type: mlx-supervised
    endpoint: http://localhost:1235
    model: /Volumes/SD/gemma-4-mlx
    context_window: 32768
    timeout: 300
routing:
  default: mlx-gemma
  fallback_chain: [mlx-gemma]
`)
	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	// WithoutAutoDiscovery keeps the test hermetic: otherwise BuildRouter live-
	// probes LM Studio on :1234 and, when one is running on the host, registers
	// "lmstudio" ahead of the configured mlx-gemma, breaking LocalProvider.
	router, err := BuildRouter(cfg, WithoutAutoDiscovery())
	if err != nil {
		t.Fatalf("BuildRouter: %v", err)
	}
	name, ok := router.ProviderForName("mlx-gemma")
	if !ok || name != "mlx-gemma" {
		t.Errorf("ProviderForName(mlx-gemma): got %q, %v; want mlx-gemma, true", name, ok)
	}
	// Also verify it is an on-device provider via LocalProvider.
	localName, localOK := router.LocalProvider()
	if !localOK {
		t.Error("LocalProvider: got false; want true (mlx-supervised is on-device)")
	}
	if localName != "mlx-gemma" {
		t.Errorf("LocalProvider: got %q; want mlx-gemma", localName)
	}
}

// ── probeMLXEndpoint helper ───────────────────────────────────────────────────

// TestProbeMLXEndpointSuccess: returns true when server reports the model.
func TestProbeMLXEndpointSuccess(t *testing.T) {
	t.Parallel()
	model := "/vol/model"
	srv := httptest.NewServer(mlxModelsHandler(model))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if !probeMLXEndpoint(ctx, srv.URL, model) {
		t.Error("probeMLXEndpoint: got false; want true")
	}
}

// TestProbeMLXEndpointModelNotFound: returns false when model is not in list.
func TestProbeMLXEndpointModelNotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mlxModelsHandler("other-model"))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if probeMLXEndpoint(ctx, srv.URL, "/vol/my-model") {
		t.Error("probeMLXEndpoint: got true; want false when model not in list")
	}
}

// TestProbeMLXEndpointServerDown: returns false when no server.
func TestProbeMLXEndpointServerDown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if probeMLXEndpoint(ctx, "http://127.0.0.1:19998", "model") {
		t.Error("probeMLXEndpoint: got true; want false when server is down")
	}
}

// ── helpers ────────────────────────────────────────────────────────────────────

// mlxContains checks whether s contains sub. Named to avoid collision with
// other test helpers in the same package (e.g. containsStr in context_assembly_test.go).
func mlxContains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
