// provider_lms_model_state_test.go — unit tests for LMSModelStateProvider.
//
// Test discipline (mirrors provider_mlx_supervised_test.go):
//   - Package engine (whitebox)
//   - t.TempDir() for filesystem isolation
//   - No sleeping; no live LM Studio; no real load/unload
//   - The actuator is replaced with a FAKE script that records argv+env to a
//     file so ApplyPlan tests assert the exact invocation WITHOUT loading a model
//     (GUARDRAIL 1).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// TestLMSModelStateProviderConcurrentFieldAccess exercises the mutable provider
// fields (token via SetToken, actuatorScript via LoadConfig) concurrently with
// the readers (FetchLive→probeModels reads token; Health reads actuatorScript),
// so `go test -race` flags any unguarded access. The provider is the shared,
// globally-registered singleton the framework's ConfigureProvider/Tokenable path
// writes while the autonomic ticker + ReconcileDaemon read.
func TestLMSModelStateProviderConcurrentFieldAccess(t *testing.T) {
	p := makeLMSProvider(t, "http://127.0.0.1:1", "target", 262144) // unreachable is fine; the race is on the field access
	var wg sync.WaitGroup
	stop := make(chan struct{})
	spin := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fn()
				}
			}
		}()
	}
	root := t.TempDir()
	spin(func() { p.SetToken("t") })                               // writes token
	spin(func() { _, _ = p.LoadConfig(root) })                     // writes actuatorScript
	spin(func() { _, _ = p.FetchLive(context.Background(), nil) }) // reads token (probeModels)
	spin(func() { _ = p.Health() })                                // reads actuatorScript
	time.Sleep(40 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// ── fixtures ─────────────────────────────────────────────────────────────────

// modelsJSON is a raw /api/v0/models body builder. Each entry is (id, state,
// loadedCtx, maxCtx); a negative loadedCtx omits the field entirely (the
// not-loaded/null case).
type modelFixture struct {
	id, state string
	loadedCtx int // <0 ⇒ omit loaded_context_length
	maxCtx    int
	typ       string
}

func modelsHandler(fixtures ...modelFixture) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/models" {
			http.NotFound(w, r)
			return
		}
		var data []map[string]any
		for _, f := range fixtures {
			row := map[string]any{
				"id":                 f.id,
				"state":              f.state,
				"max_context_length": f.maxCtx,
			}
			if f.typ != "" {
				row["type"] = f.typ
			}
			if f.loadedCtx >= 0 {
				row["loaded_context_length"] = f.loadedCtx
			}
			// else: omit the key entirely — the null/not-loaded case.
			data = append(data, row)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "object": "list"})
	})
}

// makeLMSProvider builds a managed provider pointed at endpoint with a target.
func makeLMSProvider(t *testing.T, endpoint, model string, ctxLen int) *LMSModelStateProvider {
	t.Helper()
	cfg := ProviderConfig{
		Type:     "lmstudio",
		Endpoint: endpoint,
		Options: map[string]interface{}{
			"model_state": map[string]interface{}{
				"manage":         true,
				"model":          model,
				"context_length": ctxLen,
			},
		},
	}
	p, err := newLMSModelStateProvider("lms-test", cfg, "tok", t.TempDir())
	if err != nil {
		t.Fatalf("newLMSModelStateProvider: %v", err)
	}
	// Point the actuator at a fake so Health()'s install-check passes and
	// ApplyPlan runs the fake. Individual tests may override.
	p.actuatorScript = writeFakeActuator(t)
	p.nodeBin = "/bin/sh" // run the fake as a shell script
	p.local = false       // force SDK-actuator path (not lms CLI)
	return p
}

// writeFakeActuator writes a shell script that records argv + the token env var
// to <dir>/actuator-calls.log and exits 0. It NEVER contacts a backend.
func writeFakeActuator(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "actuator-calls.log")
	script := "#!/bin/sh\n" +
		"echo \"ARGV: $@\" >> \"" + logPath + "\"\n" +
		"echo \"TOKEN: ${LMS_ACTUATOR_TOKEN}\" >> \"" + logPath + "\"\n" +
		"echo '{\"ok\":true}'\n"
	path := filepath.Join(dir, "fake-actuator.sh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake actuator: %v", err)
	}
	return path
}

func readActuatorLog(t *testing.T, actuatorScript string) string {
	t.Helper()
	logPath := filepath.Join(filepath.Dir(actuatorScript), "actuator-calls.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		return "" // no calls
	}
	return string(data)
}

// ── FetchLive: parsing incl. the null loaded_context_length case ────────────────

func TestFetchLiveParsesNullContext(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "loaded-model", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
		modelFixture{id: "cold-model", state: "not-loaded", loadedCtx: -1, maxCtx: 262144}, // null
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "loaded-model", 262144)
	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	rows, ok := live.([]lmsModelRow)
	if !ok || len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %#v", live)
	}
	// Loaded row has a non-nil pointer.
	loaded := findModelRow(rows, "loaded-model")
	if loaded == nil || loaded.LoadedContextLength == nil || *loaded.LoadedContextLength != 262144 {
		t.Errorf("loaded row context: got %v, want 262144", loaded)
	}
	// not-loaded row's loaded_context_length must be nil (not 0) — the null case.
	cold := findModelRow(rows, "cold-model")
	if cold == nil || cold.LoadedContextLength != nil {
		t.Errorf("not-loaded row should have nil LoadedContextLength, got %v", cold.LoadedContextLength)
	}
}

func TestFetchLiveNullDoesNotShadowLoaded(t *testing.T) {
	// A not-loaded duplicate id must not shadow the loaded one when matching.
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "dup", state: "not-loaded", loadedCtx: -1, maxCtx: 262144},
		modelFixture{id: "dup", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "dup", 262144)
	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	rows := live.([]lmsModelRow)
	r := findModelRow(rows, "dup")
	if r == nil || r.State != "loaded" {
		t.Errorf("findModelRow must prefer the loaded row, got %#v", r)
	}
}

// ── FetchLive: local parallel probe merge ────────────────────────────────────

// writeFakePs writes a shell script standing in for `lms ps --json`. body is the
// raw JSON it prints on stdout; if exitNonZero is true it exits 1 instead.
func writeFakePs(t *testing.T, body string, exitNonZero bool) string {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\n"
	if exitNonZero {
		script += "echo 'boom' >&2\nexit 1\n"
	} else {
		script += "cat <<'EOF'\n" + body + "\nEOF\n"
	}
	path := filepath.Join(dir, "lms")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake lms CLI: %v", err)
	}
	return path
}

func TestFetchLiveMergesLocalParallel(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "example-35b", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "example-35b", 262144)
	p.local = true
	p.target.Parallel = 1 // must be declared — FetchLive gates the probe on a declared target
	p.lmsCLI = writeFakePs(t, `[{"identifier":"example-35b","modelKey":"example-35b","parallel":1}]`, false)

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	rows := live.([]lmsModelRow)
	row := findModelRow(rows, "example-35b")
	if row == nil || row.Parallel == nil || *row.Parallel != 1 {
		t.Fatalf("expected merged Parallel=1, got %#v", row)
	}
}

func TestFetchLiveSkipsParallelProbeWhenNoTargetDeclared(t *testing.T) {
	// Without a declared parallel target there is nothing to compare against —
	// FetchLive must not pay the lms CLI fork/exec cost on every cycle for
	// local backends that have never heard of `parallel:`. Point lmsCLI at a
	// script that would fail loudly (a mismatching parallel value) if invoked.
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "example-35b", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "example-35b", 262144)
	p.local = true
	p.target.Parallel = 0 // no target declared

	var invocations int
	psPath := writeFakePs(t, `[{"identifier":"example-35b","parallel":99}]`, false)
	// Wrap the fake with a counter so we can assert it was never invoked.
	countedPath := filepath.Join(t.TempDir(), "lms")
	script := "#!/bin/sh\necho called >> " + filepath.Join(filepath.Dir(countedPath), "calls.log") + "\nexec " + psPath + " \"$@\"\n"
	if err := os.WriteFile(countedPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write counting wrapper: %v", err)
	}
	p.lmsCLI = countedPath

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	row := findModelRow(live.([]lmsModelRow), "example-35b")
	if row == nil || row.Parallel != nil {
		t.Errorf("expected nil Parallel with no target declared, got %#v", row)
	}
	if data, err := os.ReadFile(filepath.Join(filepath.Dir(countedPath), "calls.log")); err == nil {
		invocations = strings.Count(string(data), "called")
	}
	if invocations != 0 {
		t.Errorf("expected the lms CLI fast-path never invoked with no parallel target declared, got %d invocations", invocations)
	}
}

func TestFetchLiveParallelProbeFailureIsNonFatal(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "example-35b", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "example-35b", 262144)
	p.local = true
	p.target.Parallel = 1               // must be declared — FetchLive gates the probe on a declared target
	p.lmsCLI = writeFakePs(t, "", true) // exits non-zero

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive must succeed despite parallel-probe failure: %v", err)
	}
	rows := live.([]lmsModelRow)
	row := findModelRow(rows, "example-35b")
	if row == nil {
		t.Fatal("expected the /api/v0/models row to still be present")
	}
	if row.Parallel != nil {
		t.Errorf("expected nil (unobserved) Parallel on probe failure, got %v", *row.Parallel)
	}
	if row.LoadedContextLength == nil || *row.LoadedContextLength != 262144 {
		t.Errorf("context data must remain intact despite parallel-probe failure, got %v", row.LoadedContextLength)
	}
}

func TestFetchLiveParallelProbeGarbageJSONIsNonFatal(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "example-35b", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "example-35b", 262144)
	p.local = true
	p.target.Parallel = 1 // must be declared — FetchLive gates the probe on a declared target
	p.lmsCLI = writeFakePs(t, "not json", false)

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive must succeed despite garbage parallel-probe output: %v", err)
	}
	row := findModelRow(live.([]lmsModelRow), "example-35b")
	if row == nil || row.Parallel != nil {
		t.Errorf("expected nil Parallel on unparseable lms ps output, got %#v", row)
	}
}

func TestFetchLiveRemoteBackendSkipsParallelProbe(t *testing.T) {
	// A remote backend must never attempt the lms CLI fast-path (no --host flag
	// makes it meaningless there). Point lmsCLI at a script that would fail loudly
	// if invoked, and assert the row's Parallel stays nil without erroring.
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "example-35b", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()

	p := makeLMSProvider(t, srv.URL, "example-35b", 262144)
	p.local = false       // remote
	p.target.Parallel = 1 // declared, so this test exercises the local-gate specifically
	p.lmsCLI = writeFakePs(t, `[{"identifier":"example-35b","parallel":1}]`, false)

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	row := findModelRow(live.([]lmsModelRow), "example-35b")
	if row == nil || row.Parallel != nil {
		t.Errorf("remote backend must not merge a parallel probe, got %#v", row)
	}
}

// ── ComputePlan: drift states ──────────────────────────────────────────────────

func TestComputePlanLoadWhenAbsent(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	rows := []lmsModelRow{{ID: "other", State: "loaded", LoadedContextLength: ip(4096)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/load") {
		t.Fatalf("expected one /load action, got %#v", plan.Actions)
	}
}

func TestComputePlanContextDriftUnloadReload(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(65536)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/context") {
		t.Fatalf("expected one /context action, got %#v", plan.Actions)
	}
}

func TestComputePlanSyncedEmptyPlan(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("expected empty plan when at target, got %#v", plan.Actions)
	}
}

// TestComputePlanParallelOnlyDriftEmitsAction is the plan-emission test the
// remediation scope requires: parallel-only drift (context is fine, parallel
// is not) must produce a NON-EMPTY plan that actuates a reload at the
// declared parallel — mirroring how context_length drift is remediated.
// Before this fix, ComputePlan emitted nothing for a parallel mismatch,
// pinning the resource permanently Degraded and re-triggering the autonomic
// ticker's LLM escalation path on every tick with nothing the deterministic
// self-heal could do about it.
func TestComputePlanParallelOnlyDriftEmitsAction(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(4)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/parallel") {
		t.Fatalf("expected one /parallel action, got %#v", plan.Actions)
	}
	// context_length must be carried in the action's Details so ApplyPlan's
	// reload doesn't silently fall back to LM Studio's default context.
	if got := plan.Actions[0].Details["context_length"]; got != 262144 {
		t.Errorf("expected context_length=262144 preserved in the /parallel action, got %v", got)
	}
}

// TestComputePlanContextMismatchTakesPriorityOverParallel: when BOTH context
// and parallel are wrong, only the /context action fires — its reload already
// carries the declared parallel via buildActuatorCmd's unconditional
// --parallel on every local reload, so a second /parallel action in the same
// plan would be redundant.
func TestComputePlanContextMismatchTakesPriorityOverParallel(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(65536), Parallel: ip(4)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/context") {
		t.Fatalf("expected exactly one /context action (parallel folded in), got %#v", plan.Actions)
	}
}

// TestComputePlanParallelUnobservedStaysEmpty: a nil observed Parallel (remote
// backend, or the local probe found no observation) must never be treated as
// a mismatch — ComputePlan must not manufacture an action from "we don't
// know".
func TestComputePlanParallelUnobservedStaysEmpty(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: nil}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("expected empty plan on unobserved parallel, got %#v", plan.Actions)
	}
}

// TestComputePlanParallelActionDampedAfterApply is the SAFETY test the
// remediation scope requires (repair item 2, the third-round reviewer's
// non-convergence risk): once ApplyPlan has run a "/parallel" action against
// an observed parallel value, ComputePlan must not re-emit that action
// against the SAME unchanged observed value on the next cycle — otherwise a
// non-converging reload (LM Studio clamping/ignoring --parallel, or the
// reload simply not moving the observed number — never independently
// verified live) would unload+reload the model on every autonomic tick
// forever. The gate lifts the moment FetchLive reports a DIFFERENT observed
// value, even if that value is still a mismatch.
func TestComputePlanParallelActionDampedAfterApply(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1

	rowsAtParallel := func(observed int) []lmsModelRow {
		return []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(observed)}}
	}

	// First cycle: mismatch (observed 4, want 1) — action must fire.
	plan, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/parallel") {
		t.Fatalf("expected one /parallel action on first cycle, got %#v", plan.Actions)
	}

	// Apply it. The fake actuator never actually changes anything, mirroring
	// a non-converging reload.
	if _, err := p.ApplyPlan(context.Background(), plan); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	// Second cycle: observed value UNCHANGED (still 4) — must be damped.
	plan2, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan2.Actions) != 0 {
		t.Fatalf("expected the /parallel action to be damped on unchanged observed value, got %#v", plan2.Actions)
	}

	// Third cycle: observed value CHANGED (now 2, still a mismatch) — the
	// gate lifts and a fresh attempt is allowed.
	plan3, err := p.ComputePlan(&p.target, rowsAtParallel(2), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan3.Actions) != 1 || !strings.HasSuffix(plan3.Actions[0].Name, "/parallel") {
		t.Fatalf("expected the gate to lift once the observed value changed, got %#v", plan3.Actions)
	}
}

// TestComputePlanParallelActionReArmsOnConvergence covers the fourth-round
// reviewer's finding on cogos#555: the dampening gate must re-arm once the
// drift it was damping against actually resolves, so a later re-drift to the
// SAME observed value fires a fresh action instead of being permanently
// shadowed by the pre-convergence record. Sequence: target=1; observed 4 (a
// mismatch) fires the action; observed 1 (converged — e.g. a successful
// reload) produces an empty Synced plan AND clears the dampening record;
// observed 4 again (e.g. LM Studio or the operator reloading at the app
// default, the single most likely recurrence since that default is a fixed
// number) must fire the action again rather than being damped against the
// stale pre-convergence record for (model, "4").
func TestComputePlanParallelActionReArmsOnConvergence(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1

	rowsAtParallel := func(observed int) []lmsModelRow {
		return []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(observed)}}
	}

	// Cycle 1: observed 4, want 1 — mismatch, action fires.
	plan1, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan1.Actions) != 1 || !strings.HasSuffix(plan1.Actions[0].Name, "/parallel") {
		t.Fatalf("cycle 1: expected one /parallel action, got %#v", plan1.Actions)
	}
	if _, err := p.ApplyPlan(context.Background(), plan1); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	// Cycle 2: observed 1 — converged. Empty plan (Synced), and the
	// dampening record for (model, "4") must be cleared as a side effect.
	plan2, err := p.ComputePlan(&p.target, rowsAtParallel(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan2.Actions) != 0 {
		t.Fatalf("cycle 2: expected an empty (Synced) plan on convergence, got %#v", plan2.Actions)
	}

	// Cycle 3: observed 4 again — the SAME value cycle 1 was damped against.
	// Without the re-arm, parallelActionDamped(model, "4") would still be
	// true from cycle 1 and this would wrongly stay empty forever.
	plan3, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan3.Actions) != 1 || !strings.HasSuffix(plan3.Actions[0].Name, "/parallel") {
		t.Fatalf("cycle 3: expected the action to fire again after re-drifting to a previously-damped value, got %#v", plan3.Actions)
	}
}

// TestComputePlanParallelActionGateSurvivesUnobservedProbe covers the
// fifth-round reviewer's blocking finding on cogos#555: an unobserved row
// (Parallel == nil — the `lms ps --json` probe failed, per FetchLive's
// documented non-fatal-probe-failure behavior) must NOT be read as
// convergence. Sequence: target=1; observed 4 fires the action; a probe
// failure on the next cycle (row present, State=="loaded", Parallel==nil)
// must leave the gate armed; observed 4 again must stay damped, not
// re-fire — a naive "!parallelMismatch" convergence check would wrongly
// clear the gate on the unobserved cycle (parallelMismatch(nil, ...) is
// false) and re-fire here, re-entering the exact thrash this gate exists to
// close on every failed probe.
func TestComputePlanParallelActionGateSurvivesUnobservedProbe(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1

	rowsAtParallel := func(observed int) []lmsModelRow {
		return []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(observed)}}
	}
	rowUnobserved := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: nil}}

	// Cycle 1: observed 4, want 1 — mismatch, action fires.
	plan1, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan1.Actions) != 1 || !strings.HasSuffix(plan1.Actions[0].Name, "/parallel") {
		t.Fatalf("cycle 1: expected one /parallel action, got %#v", plan1.Actions)
	}
	if _, err := p.ApplyPlan(context.Background(), plan1); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	// Cycle 2: the probe failed — row present, loaded, but Parallel unobserved
	// (nil). This must NOT be read as convergence and must NOT clear the gate.
	plan2, err := p.ComputePlan(&p.target, rowUnobserved, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan2.Actions) != 0 {
		t.Fatalf("cycle 2: expected an empty plan on an unobserved row, got %#v", plan2.Actions)
	}

	// Cycle 3: observed 4 again — the SAME value cycle 1 was damped against.
	// The gate must still be armed: no action.
	plan3, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan3.Actions) != 0 {
		t.Fatalf("cycle 3: expected the gate to remain armed across an unobserved probe cycle, got %#v", plan3.Actions)
	}
}

// TestComputePlanParallelActionGateSurvivesNotLoadedRow is the SAME-ROOT-CAUSE
// sibling of TestComputePlanParallelActionGateSurvivesUnobservedProbe: a
// row with State=="not-loaded" (row present, so the targetRow != nil guard
// passes, but Parallel is nil and the model isn't actually loaded) must also
// not be read as convergence. Sequence mirrors the unobserved-probe case.
func TestComputePlanParallelActionGateSurvivesNotLoadedRow(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Parallel = 1

	rowsAtParallel := func(observed int) []lmsModelRow {
		return []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(observed)}}
	}
	rowNotLoaded := []lmsModelRow{{ID: "target", State: "not-loaded"}}

	// Cycle 1: observed 4, want 1 — mismatch, action fires.
	plan1, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan1.Actions) != 1 || !strings.HasSuffix(plan1.Actions[0].Name, "/parallel") {
		t.Fatalf("cycle 1: expected one /parallel action, got %#v", plan1.Actions)
	}
	if _, err := p.ApplyPlan(context.Background(), plan1); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}

	// Cycle 2: a not-loaded row — e.g. after a manual unload, or a half-failed
	// actuator invocation. This must NOT be read as convergence and must NOT
	// clear the gate.
	plan2, err := p.ComputePlan(&p.target, rowNotLoaded, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sawLoad bool
	for _, a := range plan2.Actions {
		if strings.HasSuffix(a.Name, "/parallel") {
			t.Fatalf("cycle 2: expected no /parallel action against a not-loaded row, got %#v", plan2.Actions)
		}
		if strings.HasSuffix(a.Name, "/load") {
			sawLoad = true
		}
	}
	_ = sawLoad // a /load action here is expected and fine; only /parallel is disallowed.

	// Cycle 3: observed 4 again — the SAME value cycle 1 was damped against.
	// The gate must still be armed: no action.
	plan3, err := p.ComputePlan(&p.target, rowsAtParallel(4), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan3.Actions) != 0 {
		t.Fatalf("cycle 3: expected the gate to remain armed across a not-loaded cycle, got %#v", plan3.Actions)
	}
}

// TestComputePlanParallelActionWithUnmanagedContext ports the third-round
// reviewer's TestRR3_E_ParallelActionWithUnmanagedContext scenario (repair
// item 3): target.ContextLength == 0 (context not being managed) and the
// model is loaded at parallel 4 (want 1) with an observed live context of
// 262144. The /parallel action's "context_length" detail must carry the
// OBSERVED live context, not the unset target value — otherwise the reload
// this action triggers would silently drop the model back to LM Studio's
// default context.
func TestComputePlanParallelActionWithUnmanagedContext(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 0) // ContextLength 0 ⇒ unmanaged
	p.target.Parallel = 1
	rows := []lmsModelRow{{ID: "target", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(4)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || !strings.HasSuffix(plan.Actions[0].Name, "/parallel") {
		t.Fatalf("expected one /parallel action, got %#v", plan.Actions)
	}
	if got := plan.Actions[0].Details["context_length"]; got != 262144 {
		t.Errorf("expected the observed live context (262144) carried into the action when target.ContextLength==0, got %v", got)
	}
}

// TestApplyPlanParallelActionWithUnmanagedContext is the end-to-end mirror of
// TestComputePlanParallelActionWithUnmanagedContext via the local lms CLI
// fast-path: argv must include --context-length with the observed value
// rather than omitting it (buildActuatorCmd only adds --context-length when
// ctxLen > 0, so without the ComputePlan-side fix this would have reloaded
// at LM Studio's default context).
func TestApplyPlanParallelActionWithUnmanagedContext(t *testing.T) {
	p := makeLMSProvider(t, "http://127.0.0.1:1234", "target-model", 0)
	p.local = true
	p.target.Parallel = 1
	dir := t.TempDir()
	logPath := filepath.Join(dir, "lms-calls.log")
	script := "#!/bin/sh\n" + "echo \"$@\" >> \"" + logPath + "\"\n"
	lmsPath := filepath.Join(dir, "lms")
	if err := os.WriteFile(lmsPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake lms CLI: %v", err)
	}
	p.lmsCLI = lmsPath

	rows := []lmsModelRow{{ID: "target-model", State: "loaded", LoadedContextLength: ip(262144), Parallel: ip(4)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 {
		t.Fatalf("expected one action, got %#v", plan.Actions)
	}
	results, err := p.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplySucceeded {
		t.Fatalf("expected one succeeded result, got %#v", results)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("expected the lms CLI fast-path to have been invoked: %v", err)
	}
	log := string(logData)
	if !strings.Contains(log, "--context-length 262144") {
		t.Errorf("expected the observed live context preserved on the reload; argv:\n%s", log)
	}
	if !strings.Contains(log, "--parallel 1") {
		t.Errorf("expected --parallel 1 threaded onto the reload; log:\n%s", log)
	}
}

func TestComputePlanJITEvict(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.JITEvict = true
	rows := []lmsModelRow{
		{ID: "target", State: "loaded", LoadedContextLength: ip(262144)},
		{ID: "crowder", State: "loaded", LoadedContextLength: ip(8192)},
	}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	var sawUnload bool
	for _, a := range plan.Actions {
		if strings.HasSuffix(a.Name, "/unload") && a.Details["model"] == "crowder" {
			sawUnload = true
		}
	}
	if !sawUnload {
		t.Fatalf("expected /unload of crowder under jit_evict, got %#v", plan.Actions)
	}
}

func TestComputePlanLoadingNotDisrupted(t *testing.T) {
	// A model mid-load reports state=="loading" with a nil loaded context. The
	// plan MUST be empty so the load can finish — never an unload+reload, which
	// (under the 30s ReconcileDaemon poll vs a multi-minute load) would restart
	// the load every cycle and prevent it from ever completing.
	p := makeLMSProvider(t, "http://x", "target", 262144)
	rows := []lmsModelRow{{ID: "target", State: "loading", LoadedContextLength: nil}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("expected empty plan while target is loading, got %#v", plan.Actions)
	}
}

func TestComputePlanJITEvictUnloadBeforeLoad(t *testing.T) {
	// When both a fresh load and a jit_evict are due in one cycle, the eviction
	// MUST be planned before the load so VRAM is freed before we load onto the card.
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.JITEvict = true
	rows := []lmsModelRow{{ID: "crowder", State: "loaded", LoadedContextLength: ip(8192)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	unloadIdx, loadIdx := -1, -1
	for i, a := range plan.Actions {
		if strings.HasSuffix(a.Name, "/unload") {
			unloadIdx = i
		}
		if strings.HasSuffix(a.Name, "/load") {
			loadIdx = i
		}
	}
	if unloadIdx == -1 || loadIdx == -1 {
		t.Fatalf("expected both /unload and /load actions, got %#v", plan.Actions)
	}
	if unloadIdx > loadIdx {
		t.Fatalf("eviction must precede load: unload at %d, load at %d (%#v)", unloadIdx, loadIdx, plan.Actions)
	}
}

func TestComputePlanOptOutEmpty(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Manage = false
	rows := []lmsModelRow{{ID: "other", State: "loaded", LoadedContextLength: ip(4096)}}
	plan, err := p.ComputePlan(&p.target, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("opt-out must yield empty plan, got %#v", plan.Actions)
	}
}

// ── Health: three-axis mapping ─────────────────────────────────────────────────

func TestHealthSyncedWhenLoadedAtTarget(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusSynced, reconcile.HealthHealthy)
}

func TestHealthDegradedOnWrongContext(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 65536, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusOutOfSync, reconcile.HealthDegraded)
}

func TestHealthProgressingWhenLoading(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loading", loadedCtx: -1, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	mustFetch(t, p)
	h := p.Health()
	if h.Health != reconcile.HealthProgressing {
		t.Fatalf("loading ⇒ Progressing; got %s", h.Health)
	}
}

func TestHealthMissingWhenAbsent(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "other", state: "loaded", loadedCtx: 4096, maxCtx: 4096},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusOutOfSync, reconcile.HealthMissing)
}

func TestHealthSuspendedWhenNotManaged(t *testing.T) {
	p := makeLMSProvider(t, "http://x", "target", 262144)
	p.target.Manage = false
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Fatalf("unmanaged ⇒ Suspended; got %s", h.Health)
	}
}

func TestHealthSuspendedWhenUnreachable(t *testing.T) {
	// Point at a closed port — FetchLive errors, Health maps to Suspended (NOT
	// Degraded): don't self-heal an unreachable box.
	p := makeLMSProvider(t, "http://127.0.0.1:1", "target", 262144)
	_, err := p.FetchLive(context.Background(), nil)
	if err == nil {
		t.Fatal("expected FetchLive to fail against closed port")
	}
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Fatalf("unreachable ⇒ Suspended (not Degraded); got %s (%s)", h.Health, h.Message)
	}
}

func TestHealthSuspendedWhenActuatorMissing(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	p.actuatorScript = filepath.Join(t.TempDir(), "does-not-exist.mjs")
	p.local = false
	p.lmsCLI = "" // no CLI fallback
	mustFetch(t, p)
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Fatalf("missing actuator ⇒ Suspended; got %s (%s)", h.Health, h.Message)
	}
}

// ── Health: parallel drift (local-only, alarm-only) ───────────────────────────

func TestHealthDegradedOnParallelMismatch(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	p.target.Parallel = 1
	p.local = true
	p.lmsCLI = writeFakePs(t, `[{"identifier":"target","parallel":4}]`, false)
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusOutOfSync, reconcile.HealthDegraded)
	if !strings.Contains(h.Message, "parallel") {
		t.Errorf("expected parallel mismatch in message, got %q", h.Message)
	}
}

func TestHealthHealthyWhenParallelUnset(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	p.target.Parallel = 0 // unset — no parallel check even though observed differs
	p.local = true
	p.lmsCLI = writeFakePs(t, `[{"identifier":"target","parallel":4}]`, false)
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusSynced, reconcile.HealthHealthy)
}

func TestHealthRemoteBackendDoesNotFalseAlarmOnParallel(t *testing.T) {
	// A remote backend cannot be probed for parallel (no --host flag on lms CLI).
	// A declared parallel target must not cause a false Degraded, but the gap
	// must be visible in the message rather than silently presenting as full
	// coverage.
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	p.target.Parallel = 1
	p.local = false
	mustFetch(t, p)
	h := p.Health()
	assertHealth(t, h, reconcile.SyncStatusSynced, reconcile.HealthHealthy)
	if !strings.Contains(h.Message, "not observable via lms ps") {
		t.Errorf("expected remote-gap annotation in message, got %q", h.Message)
	}
}

// ── BuildState: observed_parallel attribute ────────────────────────────────────

func TestBuildStateReportsObservedParallel(t *testing.T) {
	srv := httptest.NewServer(modelsHandler(
		modelFixture{id: "target", state: "loaded", loadedCtx: 262144, maxCtx: 262144},
	))
	defer srv.Close()
	p := makeLMSProvider(t, srv.URL, "target", 262144)
	p.target.Parallel = 1
	p.local = true
	p.lmsCLI = writeFakePs(t, `[{"identifier":"target","parallel":1}]`, false)
	mustFetch(t, p)

	live, err := p.FetchLive(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
	state, err := p.BuildState(nil, live, nil)
	if err != nil {
		t.Fatalf("BuildState: %v", err)
	}
	if len(state.Resources) != 1 {
		t.Fatalf("expected one resource, got %d", len(state.Resources))
	}
	attrs := state.Resources[0].Attributes
	if got := attrs["observed_parallel"]; got != "1" {
		t.Errorf("observed_parallel: got %v, want \"1\"", got)
	}
	if got := attrs["parallel"]; got != 1 {
		t.Errorf("declared parallel attribute must be unchanged, got %v", got)
	}
}

// ── ApplyPlan: invokes the FAKE actuator with correct argv+env, no real load ──

func TestApplyPlanInvokesActuatorWithArgvAndEnv(t *testing.T) {
	p := makeLMSProvider(t, "http://192.0.2.10:1234", "target-model", 262144)
	plan := &reconcile.Plan{
		ResourceType: lmsModelStateType,
		Actions: []reconcile.Action{{
			Action:       reconcile.ActionUpdate,
			ResourceType: lmsModelStateType,
			Name:         p.name + "/load",
			Details: map[string]any{
				"model":          "target-model",
				"context_length": 262144,
			},
		}},
	}
	results, err := p.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplySucceeded {
		t.Fatalf("expected one succeeded result, got %#v", results)
	}
	log := readActuatorLog(t, p.actuatorScript)
	// argv assertions: op, host, port, model, context-length.
	// NOTE: --parallel is intentionally absent: LM Studio's SDK load config has no
	// per-load parallelism knob, so the actuator does not accept/forward it.
	for _, want := range []string{
		"load", "--host 192.0.2.10", "--port 1234",
		"--model target-model", "--context-length 262144",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("actuator argv missing %q; log:\n%s", want, log)
		}
	}
	// Token must arrive via ENV, never argv.
	if !strings.Contains(log, "TOKEN: tok") {
		t.Errorf("token not passed via LMS_ACTUATOR_TOKEN env; log:\n%s", log)
	}
	if strings.Contains(log, "tok") && strings.Contains(log, "ARGV:") {
		argvLine := firstLine(log, "ARGV:")
		if strings.Contains(argvLine, "tok") {
			t.Errorf("token leaked into argv: %q", argvLine)
		}
	}
}

// A future actuator footgun: prints {"ok":false} but exits 0 (dropped await /
// swallowed catch). ApplyPlan must NOT report ApplySucceeded — the result-line
// parse folds the actuator's error into an ApplyFailed.
func TestApplyPlanFailsOnOkFalseWithZeroExit(t *testing.T) {
	p := makeLMSProvider(t, "http://192.0.2.10:1234", "target-model", 262144)
	dir := t.TempDir()
	script := "#!/bin/sh\n" +
		"echo '{\"ok\":false,\"error\":\"load timed out\"}'\n" +
		"exit 0\n"
	path := filepath.Join(dir, "fake-actuator-okfalse.sh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake actuator: %v", err)
	}
	p.actuatorScript = path

	plan := &reconcile.Plan{
		ResourceType: lmsModelStateType,
		Actions: []reconcile.Action{{
			Action:  reconcile.ActionUpdate,
			Name:    p.name + "/load",
			Details: map[string]any{"model": "target-model", "context_length": 262144},
		}},
	}
	results, err := p.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplyFailed {
		t.Fatalf("expected ApplyFailed on ok=false/exit-0, got %#v", results)
	}
	if !strings.Contains(results[0].Error, "load timed out") {
		t.Errorf("expected actuator error folded into result, got %q", results[0].Error)
	}
}

func TestApplyPlanContextActionUsesSetContext(t *testing.T) {
	p := makeLMSProvider(t, "http://192.0.2.10:1234", "target-model", 262144)
	plan := &reconcile.Plan{
		ResourceType: lmsModelStateType,
		Actions: []reconcile.Action{{
			Action: reconcile.ActionUpdate,
			Name:   p.name + "/context",
			Details: map[string]any{
				"model":          "target-model",
				"context_length": 262144,
			},
		}},
	}
	if _, err := p.ApplyPlan(context.Background(), plan); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	log := readActuatorLog(t, p.actuatorScript)
	if !strings.Contains(log, "set-context") {
		t.Errorf("context action should invoke set-context; log:\n%s", log)
	}
}

// TestApplyPlanParallelActionUsesSetContext mirrors
// TestApplyPlanContextActionUsesSetContext: a "/parallel" action must reuse
// the same unload+reload "set-context" verb — LM Studio has no live
// parallelism resize either, and buildActuatorCmd's local fast-path threads
// --parallel unconditionally on that verb regardless of which drift triggered
// it.
func TestApplyPlanParallelActionUsesSetContext(t *testing.T) {
	p := makeLMSProvider(t, "http://192.0.2.10:1234", "target-model", 262144)
	plan := &reconcile.Plan{
		ResourceType: lmsModelStateType,
		Actions: []reconcile.Action{{
			Action: reconcile.ActionUpdate,
			Name:   p.name + "/parallel",
			Details: map[string]any{
				"model":          "target-model",
				"context_length": 262144,
				"parallel":       1,
			},
		}},
	}
	results, err := p.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplySucceeded {
		t.Fatalf("expected one succeeded result, got %#v", results)
	}
	log := readActuatorLog(t, p.actuatorScript)
	if !strings.Contains(log, "set-context") {
		t.Errorf("parallel action should invoke set-context; log:\n%s", log)
	}
	// context_length must survive into the actuator call too — otherwise the
	// reload this action triggers would silently drop back to LM Studio's
	// default context length instead of preserving the previously-correct one.
	if !strings.Contains(log, "--context-length 262144") {
		t.Errorf("parallel action's reload must preserve context_length; log:\n%s", log)
	}
}

// TestApplyPlanParallelActionThreadsParallelOnLocalFastPath exercises the
// local `lms load` fast-path end to end for a "/parallel" action: the
// declared p.target.Parallel must be threaded onto the reload the action
// triggers (buildActuatorCmd's local branch appends --parallel unconditionally
// on every load/set-context, see its doc comment).
func TestApplyPlanParallelActionThreadsParallelOnLocalFastPath(t *testing.T) {
	p := makeLMSProvider(t, "http://127.0.0.1:1234", "target-model", 262144)
	p.local = true
	p.target.Parallel = 1
	dir := t.TempDir()
	logPath := filepath.Join(dir, "lms-calls.log")
	script := "#!/bin/sh\n" + "echo \"$@\" >> \"" + logPath + "\"\n"
	lmsPath := filepath.Join(dir, "lms")
	if err := os.WriteFile(lmsPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake lms CLI: %v", err)
	}
	p.lmsCLI = lmsPath

	plan := &reconcile.Plan{
		ResourceType: lmsModelStateType,
		Actions: []reconcile.Action{{
			Action: reconcile.ActionUpdate,
			Name:   p.name + "/parallel",
			Details: map[string]any{
				"model":          "target-model",
				"context_length": 262144,
				"parallel":       1,
			},
		}},
	}
	results, err := p.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplySucceeded {
		t.Fatalf("expected one succeeded result, got %#v", results)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("expected the lms CLI fast-path to have been invoked: %v", err)
	}
	log := string(logData)
	if !strings.Contains(log, "--parallel 1") {
		t.Errorf("expected --parallel 1 threaded onto the local reload; log:\n%s", log)
	}
	if !strings.Contains(log, "--context-length 262144") {
		t.Errorf("expected --context-length 262144 preserved on the local reload; log:\n%s", log)
	}
}

// ── construction / parsing ─────────────────────────────────────────────────────

func TestParseModelStateOptions(t *testing.T) {
	opts := map[string]interface{}{
		"model_state": map[string]interface{}{
			"manage":         true,
			"model":          "m",
			"context_length": 262144,
			"parallel":       4,
			"keep_warm":      true,
			"jit_evict":      false,
		},
	}
	c := parseModelStateOptions(opts)
	if !c.Manage || c.Model != "m" || c.ContextLength != 262144 || c.Parallel != 4 || !c.KeepWarm || c.JITEvict {
		t.Fatalf("parse mismatch: %#v", c)
	}
}

// TestHostPortBracketedIPv6 covers hostPort's bracket-aware port strip: a
// naive strings.LastIndex(s, ":") split cuts INSIDE the brackets on a
// bracketed IPv6 literal with no port suffix ("[::1]" → "[:", silently
// failing isLocalHost's loopback match), and only produced the right host on
// "[::1]:1234" by accident (LastIndex happened to land on the port-separator
// colon rather than an internal IPv6 colon).
func TestHostPortBracketedIPv6(t *testing.T) {
	cases := []struct {
		endpoint  string
		wantHost  string
		wantPort  int
		wantLocal bool
	}{
		{"http://[::1]", "[::1]", 9999, true},
		{"http://[::1]/v1", "[::1]", 9999, true},
		{"http://[::1]:1234", "[::1]", 1234, true},
		// Case-insensitivity: the engine gate must agree with the daemon's
		// isLocalHostEndpoint (which lowercases) on the same backend — the
		// engine copy is the one that actually probes and remediates.
		{"http://LocalHost:1234", "LocalHost", 1234, true},
		{"http://LOCALHOST", "LOCALHOST", 9999, true},
		{"HTTP://LocalHost:1234/v1", "LocalHost", 1234, true},
	}
	for _, tc := range cases {
		host, port := hostPort(tc.endpoint, 9999)
		if host != tc.wantHost || port != tc.wantPort {
			t.Errorf("hostPort(%q) = (%q, %d); want (%q, %d)", tc.endpoint, host, port, tc.wantHost, tc.wantPort)
		}
		if got := isLocalHost(host); got != tc.wantLocal {
			t.Errorf("isLocalHost(hostPort(%q)) = %v; want %v", tc.endpoint, got, tc.wantLocal)
		}
	}
}

func TestLocalDetection(t *testing.T) {
	cfg := ProviderConfig{Endpoint: "http://127.0.0.1:1234", Options: map[string]interface{}{
		"model_state": map[string]interface{}{"manage": true, "model": "m"},
	}}
	p, _ := newLMSModelStateProvider("x", cfg, "", "")
	if !p.local {
		t.Errorf("127.0.0.1 should be detected as local")
	}
	cfg.Endpoint = "http://192.0.2.10:1234"
	p2, _ := newLMSModelStateProvider("x", cfg, "", "")
	if p2.local {
		t.Errorf("192.0.2.10 should NOT be local")
	}
	if p2.host != "192.0.2.10" || p2.port != 1234 {
		t.Errorf("host/port parse: got %s:%d", p2.host, p2.port)
	}
	if p2.wsURL != "ws://192.0.2.10:1234" {
		t.Errorf("wsURL: got %s", p2.wsURL)
	}
}

// ── helpers ────────────────────────────────────────────────────────────────────

func ip(v int) *int { return &v }

func mustFetch(t *testing.T, p *LMSModelStateProvider) {
	t.Helper()
	if _, err := p.FetchLive(context.Background(), nil); err != nil {
		t.Fatalf("FetchLive: %v", err)
	}
}

func assertHealth(t *testing.T, h reconcile.ResourceStatus, sync reconcile.SyncStatus, health reconcile.HealthStatus) {
	t.Helper()
	if h.Sync != sync || h.Health != health {
		t.Fatalf("health: got (%s,%s), want (%s,%s): %s", h.Sync, h.Health, sync, health, h.Message)
	}
}

func firstLine(s, prefix string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	return ""
}

// Guard: this file must compile on all platforms (no darwin-only deps).
var _ = runtime.GOOS
var _ = fmt.Sprint
