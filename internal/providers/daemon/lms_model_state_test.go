// lms_model_state_test.go — unit tests for the daemon-side lms-model-state provider.
//
// Whitebox (package daemon). Exercises the model_state YAML parser, the opt-in
// Health() gate, and the /api/v0/models probe (loading→progressing, prefer the
// loaded row over a not-loaded duplicate) via httptest.
package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// ── probeModelStateEntry: HTTP path (loading + duplicate-row ordering) ───────────

type msRow struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Ctx   *int   `json:"loaded_context_length"`
}

func msIntp(n int) *int { return &n }

func msModelsServer(t *testing.T, rows ...msRow) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestProbeModelStateEntry_LoadingIsProgressingNotError(t *testing.T) {
	// A model mid-load (state=="loading", nil context) must report progressing,
	// NOT a Degraded-inducing error — a high-context load can take minutes.
	srv := msModelsServer(t, msRow{ID: "target", State: "loading", Ctx: nil})
	e := modelStateEntry{name: "b", endpoint: srv.URL, model: "target", contextLength: 262144}
	progressing, _, err := probeModelStateEntry(context.Background(), e)
	if err != nil {
		t.Fatalf("loading must not be an error, got %v", err)
	}
	if !progressing {
		t.Fatalf("loading must report progressing=true")
	}
}

func TestProbeModelStateEntry_PrefersLoadedOverDuplicate(t *testing.T) {
	// A not-loaded duplicate ordered BEFORE the loaded row must not shadow it.
	srv := msModelsServer(t,
		msRow{ID: "target", State: "not-loaded", Ctx: nil},
		msRow{ID: "target", State: "loaded", Ctx: msIntp(262144)},
	)
	e := modelStateEntry{name: "b", endpoint: srv.URL, model: "target", contextLength: 262144}
	progressing, _, err := probeModelStateEntry(context.Background(), e)
	if err != nil {
		t.Fatalf("loaded duplicate must satisfy the probe, got %v", err)
	}
	if progressing {
		t.Fatalf("loaded-at-target must not report progressing")
	}
}

func TestProbeModelStateEntry_WrongContextIsError(t *testing.T) {
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(65536)})
	e := modelStateEntry{name: "b", endpoint: srv.URL, model: "target", contextLength: 262144}
	if _, _, err := probeModelStateEntry(context.Background(), e); err == nil {
		t.Fatalf("wrong loaded context must be an error")
	}
}

// ── probeModelStateEntry: parallel drift (local-only, checkParallelDrift) ───────

// writeFakeLmsPs writes a shell script standing in for `lms ps --json`.
func writeFakeLmsPs(t *testing.T, body string, exitNonZero bool) string {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\n"
	if exitNonZero {
		script += "echo boom >&2\nexit 1\n"
	} else {
		script += "cat <<'EOF'\n" + body + "\nEOF\n"
	}
	path := filepath.Join(dir, "lms")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake lms CLI: %v", err)
	}
	return path
}

func TestProbeModelStateEntry_ParallelMismatchIsError(t *testing.T) {
	// The error return here folds into Health()'s existing (progressing, err)
	// issues aggregation exactly like the context-length check above — no
	// separate aggregation path was added, so this covers that contract.
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(262144)})
	e := modelStateEntry{
		name: "b", endpoint: srv.URL, model: "target", contextLength: 262144,
		parallel: 1, local: true,
		lmsCLIPath: writeFakeLmsPs(t, `[{"identifier":"target","parallel":4}]`, false),
	}
	_, _, err := probeModelStateEntry(context.Background(), e)
	if err == nil {
		t.Fatal("expected an error on observed parallel mismatch")
	}
	if !strings.Contains(err.Error(), "parallel") {
		t.Errorf("expected 'parallel' in the error message, got %q", err.Error())
	}
}

func TestProbeModelStateEntry_ParallelSkippedOnRemote(t *testing.T) {
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(262144)})
	// Point lmsCLIPath at a script that would fail loudly (non-empty stdout
	// asserting a mismatch) if ever invoked — local:false must never shell out.
	e := modelStateEntry{
		name: "b", endpoint: srv.URL, model: "target", contextLength: 262144,
		parallel: 1, local: false,
		lmsCLIPath: writeFakeLmsPs(t, `[{"identifier":"target","parallel":99}]`, false),
	}
	progressing, gapNote, err := probeModelStateEntry(context.Background(), e)
	if err != nil {
		t.Fatalf("remote entry must skip the parallel probe entirely, got error: %v", err)
	}
	if progressing {
		t.Fatal("expected progressing=false for a loaded, non-progressing entry")
	}
	if gapNote == "" {
		t.Error("expected a gap note for a declared parallel target on a remote backend")
	}
}

func TestProbeModelStateEntry_ParallelProbeFailureNonFatal(t *testing.T) {
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(262144)})
	e := modelStateEntry{
		name: "b", endpoint: srv.URL, model: "target", contextLength: 262144,
		parallel: 1, local: true,
		lmsCLIPath: writeFakeLmsPs(t, "", true), // exits non-zero
	}
	progressing, gapNote, err := probeModelStateEntry(context.Background(), e)
	if err != nil {
		t.Fatalf("a failed lms CLI probe must be non-fatal (unobserved, not wrong), got: %v", err)
	}
	if progressing {
		t.Fatal("expected progressing=false")
	}
	if gapNote == "" {
		t.Error("expected a gap note when the local parallel probe produces no observation")
	}
}

// TestCheckParallelDrift_DuplicateInstanceMatching reproduces the reviewer's
// scenario A: `lms ps` lists a duplicate instance's row ("target:2", loaded at
// parallel 4) BEFORE the exact-id row ("target", loaded at parallel 1) in its
// own array order. The declared target is parallel 1 against loadedID
// "target". The old first-satisfying-row-in-array-order matching picked
// "target:2" (a prefix match) purely because of array position, reporting a
// false mismatch (parallel 4, want 1) against live state that is actually
// correct. Exact-id-match-first must find "target" regardless of array order.
func TestCheckParallelDrift_DuplicateInstanceMatching(t *testing.T) {
	lmsCLI := writeFakeLmsPs(t, `[{"identifier":"target:2","parallel":4},{"identifier":"target","parallel":1}]`, false)
	e := modelStateEntry{name: "b", model: "target", parallel: 1, local: true, lmsCLIPath: lmsCLI}
	observed, err := checkParallelDrift(context.Background(), e, "target")
	if !observed {
		t.Fatal("expected observed=true — an exact-id row is present")
	}
	if err != nil {
		t.Fatalf("exact-id match must resolve to the correct row (parallel 1, matches target) and report Healthy, got: %v", err)
	}
}

// TestCheckParallelDrift_DuplicateInstanceMatching_RealMismatch is the mirror
// case: the exact-id row genuinely IS at the wrong parallel, with a
// non-matching duplicate ordered first. The fix must not swallow a real
// mismatch just because it now prefers exact matches.
func TestCheckParallelDrift_DuplicateInstanceMatching_RealMismatch(t *testing.T) {
	lmsCLI := writeFakeLmsPs(t, `[{"identifier":"target:2","parallel":1},{"identifier":"target","parallel":4}]`, false)
	e := modelStateEntry{name: "b", model: "target", parallel: 1, local: true, lmsCLIPath: lmsCLI}
	observed, err := checkParallelDrift(context.Background(), e, "target")
	if !observed {
		t.Fatal("expected observed=true")
	}
	if err == nil {
		t.Fatal("expected a mismatch error — the exact-id row is genuinely at parallel 4, want 1")
	}
	if !strings.Contains(err.Error(), "parallel 4") {
		t.Errorf("expected the error to cite the exact-id row's parallel (4), got %q", err.Error())
	}
}

// TestCheckParallelDrift_PrefixSiblingModel reproduces the reviewer's scenario
// B: a prefix-sibling model ("qwen3-coder-30b", parallel 8) is loaded and
// listed BEFORE the declared model's own row ("qwen3", parallel 1) in `lms ps`
// array order. The old broad prefix-either-direction predicate matched the
// sibling first purely by array position; exact-match-first must resolve to
// "qwen3" itself since an exact match exists.
func TestCheckParallelDrift_PrefixSiblingModel(t *testing.T) {
	lmsCLI := writeFakeLmsPs(t, `[{"identifier":"qwen3-coder-30b","parallel":8},{"identifier":"qwen3","parallel":1}]`, false)
	e := modelStateEntry{name: "b", model: "qwen3", parallel: 1, local: true, lmsCLIPath: lmsCLI}
	observed, err := checkParallelDrift(context.Background(), e, "qwen3")
	if !observed {
		t.Fatal("expected observed=true — an exact-id row is present")
	}
	if err != nil {
		t.Fatalf("exact-id match must resolve to \"qwen3\" (parallel 1) and report Healthy, not the sibling \"qwen3-coder-30b\", got: %v", err)
	}
}

// TestCheckParallelDrift_PrefixFallbackWhenNoExactMatch covers the case the
// prefix fallback still needs to handle: no row has the exact declared id
// (e.g. the loaded row's id itself carries a quant-suffix difference from the
// declared model — "target-q4" vs "target"), so the fallback must still find
// it rather than reporting unobserved.
func TestCheckParallelDrift_PrefixFallbackWhenNoExactMatch(t *testing.T) {
	lmsCLI := writeFakeLmsPs(t, `[{"identifier":"target-q4","parallel":1}]`, false)
	e := modelStateEntry{name: "b", model: "target", parallel: 1, local: true, lmsCLIPath: lmsCLI}
	observed, err := checkParallelDrift(context.Background(), e, "target")
	if !observed {
		t.Fatal("expected observed=true via the prefix fallback")
	}
	if err != nil {
		t.Fatalf("expected the prefix-matched row to report Healthy, got: %v", err)
	}
}

// TestCheckParallelDrift_ExactTierOrderDependence ports the third-round
// reviewer's TestRR3_D_ExactTierOrderDependence scenario: e.model="qwen3",
// loadedID="qwen3-30b" (the prefix-matched loaded row from /api/v0/models —
// loadedID != e.model), and two exact-id rows are both present in `lms ps`:
// one named "qwen3" (matches e.model, parallel 8) and one named "qwen3-30b"
// (matches loadedID, the actually-loaded instance, parallel 1). The single
// OR-combined pass picked whichever row came first in array order, so the
// daemon would sometimes report a false mismatch against the sibling's
// parallel value instead of the live row's own value. loadedID must win
// regardless of array order, since it identifies the row that IS the loaded
// instance.
func TestCheckParallelDrift_ExactTierOrderDependence(t *testing.T) {
	e := modelStateEntry{name: "b", model: "qwen3", parallel: 1, local: true}

	t.Run("loadedID row first", func(t *testing.T) {
		lmsCLI := writeFakeLmsPs(t, `[{"identifier":"qwen3-30b","parallel":1},{"identifier":"qwen3","parallel":8}]`, false)
		e := e
		e.lmsCLIPath = lmsCLI
		observed, err := checkParallelDrift(context.Background(), e, "qwen3-30b")
		if !observed {
			t.Fatal("expected observed=true")
		}
		if err != nil {
			t.Fatalf("loadedID's own row (parallel 1) must win — got: %v", err)
		}
	})

	t.Run("loadedID row last", func(t *testing.T) {
		lmsCLI := writeFakeLmsPs(t, `[{"identifier":"qwen3","parallel":8},{"identifier":"qwen3-30b","parallel":1}]`, false)
		e := e
		e.lmsCLIPath = lmsCLI
		observed, err := checkParallelDrift(context.Background(), e, "qwen3-30b")
		if !observed {
			t.Fatal("expected observed=true")
		}
		if err != nil {
			t.Fatalf("loadedID's own row (parallel 1) must win regardless of array order — got: %v", err)
		}
	})
}

func TestProbeModelStateEntry_ParallelUnsetSkipsProbeEntirely(t *testing.T) {
	// parallel==0 must skip the check even when local — no CLI invocation needed.
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(262144)})
	e := modelStateEntry{
		name: "b", endpoint: srv.URL, model: "target", contextLength: 262144,
		parallel: 0, local: true,
		lmsCLIPath: writeFakeLmsPs(t, `[{"identifier":"target","parallel":99}]`, false),
	}
	_, gapNote, err := probeModelStateEntry(context.Background(), e)
	if err != nil {
		t.Fatalf("parallel==0 must not trigger a mismatch, got: %v", err)
	}
	if gapNote != "" {
		t.Errorf("parallel==0 must not produce a gap note either, got %q", gapNote)
	}
}

// ── healthIssuesMessage: gapNotes must not be discarded on the issues path ──────

func TestHealthIssuesMessage_GapNotesSurviveAlongsideIssues(t *testing.T) {
	// Two managed backends in one Health() cycle: A is unreachable (an issue),
	// B's parallel watch is dead (a gap note, no issue of its own). The old
	// code returned early on len(issues)>0 and discarded gapNotes entirely, so
	// B's coverage gap was invisible for exactly as long as A was also loud.
	got := healthIssuesMessage(
		[]string{"backend-a: unreachable: dial tcp: connection refused"},
		[]string{"backend-b: parallel target declared but not observed — lms ps probe failed, lms CLI missing, or no matching row"},
	)
	if !strings.Contains(got, "backend-a") {
		t.Errorf("expected the issue to survive, got %q", got)
	}
	if !strings.Contains(got, "backend-b") {
		t.Errorf("expected the gap note to survive alongside the issue, got %q", got)
	}
}

func TestHealthIssuesMessage_NoGapNotes(t *testing.T) {
	got := healthIssuesMessage([]string{"backend-a: unreachable"}, nil)
	if got != "backend-a: unreachable" {
		t.Errorf("got %q; want no trailing separator when there are no gap notes", got)
	}
}

// ── isLocalHostEndpoint ──────────────────────────────────────────────────────────

func TestIsLocalHostEndpoint(t *testing.T) {
	cases := map[string]bool{
		"":                            true,
		"http://localhost:1234":       true,
		"http://LOCALHOST:1234":       true, // case-insensitive — a config typo must not silently disable the parallel watch
		"http://127.0.0.1:1234":       true,
		"http://192.0.2.10:1234":      false,
		"https://lan-host.local:1234": false,
		// Bracketed IPv6 literals: the naive strings.LastIndex(host, ":") port
		// strip cuts INSIDE the brackets on a literal with no port suffix
		// ("[::1]" → "[:", a miss), and only works on "[::1]:1234" by
		// accident (LastIndex happens to land on the port-separator colon).
		"http://[::1]":      true,
		"http://[::1]/v1":   true,
		"http://[::1]:1234": true,
	}
	for endpoint, want := range cases {
		if got := isLocalHostEndpoint(endpoint); got != want {
			t.Errorf("isLocalHostEndpoint(%q) = %v, want %v", endpoint, got, want)
		}
	}
}

// ── parseModelStateEntriesFromYAML: parallel field ──────────────────────────────

func TestParseModelStateEntries_ParallelField(t *testing.T) {
	t.Parallel()
	yaml := `providers:
  local:
    type: lmstudio
    endpoint: http://localhost:1234
    options:
      model_state:
        manage: true
        model: example-35b
        context_length: 262144
        parallel: 1
`
	got := parseModelStateEntriesFromYAML([]byte(yaml))
	if len(got) != 1 {
		t.Fatalf("got %d; want 1", len(got))
	}
	if got[0].parallel != 1 {
		t.Errorf("parallel: got %d; want 1", got[0].parallel)
	}
}

// ── parseModelStateEntriesFromYAML ──────────────────────────────────────────────

func TestParseModelStateEntries_Empty(t *testing.T) {
	t.Parallel()
	if got := parseModelStateEntriesFromYAML([]byte("")); len(got) != 0 {
		t.Errorf("empty YAML: got %d; want 0", len(got))
	}
}

func TestParseModelStateEntries_NoOptIn(t *testing.T) {
	t.Parallel()
	// A backend with no model_state, and one with manage:false — neither opts in.
	yaml := `providers:
  lmstudio:
    type: lmstudio
    endpoint: http://localhost:1234
    model: some-model
  lmstudio-remote:
    type: openai
    endpoint: http://192.0.2.10:1234
    options:
      model_state:
        manage: false
        model: example-35b
`
	got := parseModelStateEntriesFromYAML([]byte(yaml))
	if len(got) != 0 {
		t.Errorf("no opt-in: got %d; want 0", len(got))
	}
}

func TestParseModelStateEntries_OptIn(t *testing.T) {
	t.Parallel()
	yaml := `providers:
  lmstudio-remote:
    type: openai
    endpoint: http://192.0.2.10:1234
    api_key_env: LMSTUDIO_REMOTE_API_KEY
    options:
      model_state:
        manage: true
        model: example-35b
        context_length: 262144
`
	got := parseModelStateEntriesFromYAML([]byte(yaml))
	if len(got) != 1 {
		t.Fatalf("got %d; want 1", len(got))
	}
	e := got[0]
	if e.name != "lmstudio-remote" {
		t.Errorf("name: got %q", e.name)
	}
	if e.endpoint != "http://192.0.2.10:1234" {
		t.Errorf("endpoint: got %q", e.endpoint)
	}
	if e.apiKeyEnv != "LMSTUDIO_REMOTE_API_KEY" {
		t.Errorf("apiKeyEnv: got %q", e.apiKeyEnv)
	}
	if e.model != "example-35b" {
		t.Errorf("model: got %q", e.model)
	}
	if e.contextLength != 262144 {
		t.Errorf("contextLength: got %d; want 262144", e.contextLength)
	}
}

func TestParseModelStateEntries_MalformedYAML(t *testing.T) {
	t.Parallel()
	if got := parseModelStateEntriesFromYAML([]byte("::: not yaml :::")); len(got) != 0 {
		t.Errorf("malformed YAML: got %d; want 0", len(got))
	}
}

// ── Health opt-in gate ──────────────────────────────────────────────────────────

// TestHealthSuspendedWhenNoWorkspace: with no workspace root configured, the
// stub reports Missing (workspace not configured) — a distinct pre-condition.
// With a workspace root but no opted-in backend, it reports Suspended.
func TestHealthSuspendedWhenNoOptIn(t *testing.T) {
	// Set an empty temp workspace (no providers files) so resolveRoot passes but
	// loadModelStateEntries finds nothing.
	SetWorkspaceRoot(t.TempDir())
	t.Cleanup(func() { SetWorkspaceRoot("") })

	p := &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}}
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Fatalf("no opt-in ⇒ Suspended; got %s (%s)", h.Health, h.Message)
	}
}

func TestType(t *testing.T) {
	t.Parallel()
	p := &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}}
	if p.Type() != "lms-model-state" {
		t.Errorf("Type: got %q", p.Type())
	}
}

// ── Health: unreachable vs reachable-but-wrong (the Suspended/Degraded split) ──

// writeProvidersLocal writes a minimal providers.local.yaml under a fresh
// workspace root with one model_state-managed entry pointing at endpoint, and
// points SetWorkspaceRoot there. Returns a cleanup-free root; caller sets
// SetWorkspaceRoot("") in a defer/Cleanup as the other Health tests do.
func writeProvidersLocal(t *testing.T, endpoint string) string {
	t.Helper()
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := "providers:\n" +
		"  backend-a:\n" +
		"    type: openai\n" +
		"    endpoint: " + endpoint + "\n" +
		"    options:\n" +
		"      model_state:\n" +
		"        manage: true\n" +
		"        model: target\n" +
		"        context_length: 262144\n"
	if err := os.WriteFile(filepath.Join(cfgDir, "providers.local.yaml"), []byte(body), 0o644); err != nil {
		t.Fatalf("write providers.local.yaml: %v", err)
	}
	return root
}

// TestHealth_UnreachableBackendIsSuspendedNotDegraded is the regression test
// for the daemon/engine twin-instance bug (2026-10-01, a remote host unreachable):
// an endpoint that refuses the connection must map to Suspended, matching the
// engine-layer provider's precedent ("do NOT self-heal a box that is simply
// off or unreachable"). Before this fix, Health() returned Degraded here,
// which autonomic_ticker.healDegradedProviders treats as actionable, driving
// an unbounded self-heal reconcile loop against a box that no action can fix.
func TestHealth_UnreachableBackendIsSuspendedNotDegraded(t *testing.T) {
	// A closed TCP port: connection refused, not merely a slow/absent server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadEndpoint := srv.URL
	srv.Close() // now guaranteed unreachable

	root := writeProvidersLocal(t, deadEndpoint)
	SetWorkspaceRoot(root)
	t.Cleanup(func() { SetWorkspaceRoot("") })

	p := &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}}
	h := p.Health()
	if h.Health != reconcile.HealthSuspended {
		t.Fatalf("unreachable backend: Health = %s (want Suspended); message: %q", h.Health, h.Message)
	}
	if h.Sync != reconcile.SyncStatusUnknown {
		t.Errorf("unreachable backend: Sync = %s (want Unknown)", h.Sync)
	}
}

// TestHealth_ReachableWrongStateIsStillDegraded is the negative control: a
// backend that IS reachable but reports the wrong model/context must still
// land on Degraded — only the unreachable path moved to Suspended.
func TestHealth_ReachableWrongStateIsStillDegraded(t *testing.T) {
	srv := msModelsServer(t, msRow{ID: "target", State: "loaded", Ctx: msIntp(65536)}) // wrong ctx, want 262144
	root := writeProvidersLocal(t, srv.URL)
	SetWorkspaceRoot(root)
	t.Cleanup(func() { SetWorkspaceRoot("") })

	p := &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}}
	h := p.Health()
	if h.Health != reconcile.HealthDegraded {
		t.Fatalf("reachable-wrong-context: Health = %s (want Degraded); message: %q", h.Health, h.Message)
	}
	if h.Sync != reconcile.SyncStatusOutOfSync {
		t.Errorf("reachable-wrong-context: Sync = %s (want OutOfSync)", h.Sync)
	}
}

// Review #659 round 1: "unreachable" means the backend gave no usable answer,
// not only "connection refused". The engine-layer twin maps every probe
// failure (transport, non-200, undecodable body) to Suspended; the daemon
// stub must agree, or a proxy answering 502/503 (or a mid-restart server
// with a truncated body) re-triggers the same unbounded self-heal loop.
func TestHealth_UnusableAnswerIsSuspendedNotDegraded(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"non-200 (proxy 503)": func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
		"non-200 (502)":       func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) },
		"undecodable body":    func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data": [{"id": "tar`)) },
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(handler)
			t.Cleanup(srv.Close)
			root := writeProvidersLocal(t, srv.URL)
			SetWorkspaceRoot(root)
			t.Cleanup(func() { SetWorkspaceRoot("") })

			p := &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}}
			h := p.Health()
			if h.Health != reconcile.HealthSuspended || h.Sync != reconcile.SyncStatusUnknown {
				t.Fatalf("Health=%s Sync=%s (want Suspended/Unknown); message: %q", h.Health, h.Sync, h.Message)
			}
		})
	}
}
