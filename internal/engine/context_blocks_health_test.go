package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// stubProvider is a minimal Reconcilable used for unit-testing the health
// block builder. Only Type() and Health() are exercised; the other methods
// satisfy the interface but should not be called by buildHealthBlock.
type stubProvider struct {
	name      string
	status    reconcile.ResourceStatus
	delay     time.Duration
	panicWith any
}

func (s *stubProvider) Type() string { return s.name }

func (s *stubProvider) LoadConfig(string) (any, error) {
	return nil, nil
}

func (s *stubProvider) FetchLive(context.Context, any) (any, error) {
	return nil, nil
}

func (s *stubProvider) ComputePlan(any, any, *reconcile.State) (*reconcile.Plan, error) {
	return nil, nil
}

func (s *stubProvider) ApplyPlan(context.Context, *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, nil
}

func (s *stubProvider) BuildState(any, any, *reconcile.State) (*reconcile.State, error) {
	return nil, nil
}

func (s *stubProvider) Health() reconcile.ResourceStatus {
	if s.panicWith != nil {
		panic(s.panicWith)
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.status
}

// healthRegistryMu serializes registry mutations across tests; Go's testing
// package runs t.Parallel tests concurrently and the registry is global.
var healthRegistryMu sync.Mutex

// withProviders sets up the global reconcile registry with the supplied
// providers, runs fn, and resets the registry at the end. NOT safe for
// t.Parallel() — uses a package-level mutex.
//
// Also resets the #432 abandoned-inference delta baseline before fn runs.
// buildKernelHealthSnapshot folds that process-global counter into
// snap.Anomalies (and AllGreen()/shouldEscalate consult it), so an unrelated
// test elsewhere in the package that legitimately hits an error path through
// CompleteCancelSafeIfSupported (e.g. a dispatch test asserting a provider
// failure) would otherwise leak a nonzero delta into whichever test next
// calls buildKernelHealthSnapshot — exactly the kind of cross-test pollution
// this fixture already guards against for the provider registry.
func withProviders(t *testing.T, providers []*stubProvider, fn func()) {
	t.Helper()
	healthRegistryMu.Lock()
	defer healthRegistryMu.Unlock()

	reconcile.ResetProviders()
	defer reconcile.ResetProviders()
	resetAbandonedInferenceCounterForTest()

	for _, p := range providers {
		reconcile.RegisterProvider(p.name, p)
	}
	fn()
}

func TestBuildHealthBlock_EmptyRegistry(t *testing.T) {
	withProviders(t, nil, func() {
		blk := buildHealthBlock(context.Background())
		if blk != nil {
			t.Fatalf("expected nil block when registry empty, got %+v", blk)
		}
	})
}

func TestBuildHealthBlock_AllGreenCollapses(t *testing.T) {
	providers := []*stubProvider{
		{name: "alpha", status: greenStatus()},
		{name: "beta", status: greenStatus()},
		{name: "gamma", status: greenStatus()},
	}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected non-nil block")
		}
		if blk.Name != BlockHealth {
			t.Errorf("block name: got %q want %q", blk.Name, BlockHealth)
		}
		// All-green should NOT include a markdown table.
		if strings.Contains(blk.Content, "| Provider |") {
			t.Errorf("all-green block should collapse to one line, got table:\n%s", blk.Content)
		}
		if !strings.Contains(blk.Content, "3 providers") {
			t.Errorf("expected '3 providers' summary, got:\n%s", blk.Content)
		}
		if !strings.Contains(blk.Content, "Synced/Healthy/Idle") {
			t.Errorf("expected green summary phrasing, got:\n%s", blk.Content)
		}
	})
}

func TestBuildHealthBlock_MixedStatusEmitsTable(t *testing.T) {
	providers := []*stubProvider{
		{name: "alpha", status: greenStatus()},
		{name: "beta", status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "baseline stale (2h)",
		}},
		{name: "gamma", status: greenStatus()},
	}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected non-nil block")
		}
		c := blk.Content
		if !strings.Contains(c, "3 providers") {
			t.Errorf("missing total summary, got:\n%s", c)
		}
		if !strings.Contains(c, "2 healthy") {
			t.Errorf("missing healthy count, got:\n%s", c)
		}
		if !strings.Contains(c, "1 need attention") {
			t.Errorf("missing attention count, got:\n%s", c)
		}
		if !strings.Contains(c, "| Provider |") {
			t.Errorf("expected markdown table, got:\n%s", c)
		}
		if !strings.Contains(c, "baseline stale (2h)") {
			t.Errorf("expected message in note column, got:\n%s", c)
		}
		// Non-green should appear before greens. Find the row indices.
		betaIdx := strings.Index(c, "| beta |")
		alphaIdx := strings.Index(c, "| alpha |")
		gammaIdx := strings.Index(c, "| gamma |")
		if betaIdx == -1 || alphaIdx == -1 || gammaIdx == -1 {
			t.Fatalf("missing one of beta/alpha/gamma rows, got:\n%s", c)
		}
		if betaIdx > alphaIdx || betaIdx > gammaIdx {
			t.Errorf("non-green provider should sort before greens; got beta@%d alpha@%d gamma@%d\n%s",
				betaIdx, alphaIdx, gammaIdx, c)
		}
		if alphaIdx > gammaIdx {
			t.Errorf("greens should be alphabetical; got alpha@%d gamma@%d\n%s", alphaIdx, gammaIdx, c)
		}
	})
}

func TestBuildHealthBlock_PanicRecovered(t *testing.T) {
	providers := []*stubProvider{
		{name: "fine", status: greenStatus()},
		{name: "broken", panicWith: "kaboom"},
	}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected non-nil block — panic should be recovered")
		}
		c := blk.Content
		if !strings.Contains(c, "broken") {
			t.Errorf("broken provider missing from output:\n%s", c)
		}
		if !strings.Contains(strings.ToLower(c), "panic") {
			t.Errorf("expected 'panic' in note for broken provider:\n%s", c)
		}
		if !strings.Contains(c, "Unknown") && !strings.Contains(c, "Missing") {
			t.Errorf("expected Unknown/Missing axis on panicked provider:\n%s", c)
		}
	})
}

func TestBuildHealthBlock_TimeoutMarkedUnknown(t *testing.T) {
	providers := []*stubProvider{
		{name: "snail", delay: healthProbeTimeout * 3, status: greenStatus()},
	}
	withProviders(t, providers, func() {
		start := time.Now()
		blk := buildHealthBlock(context.Background())
		elapsed := time.Since(start)
		if blk == nil {
			t.Fatal("expected non-nil block — slow provider should not nil out the block")
		}
		// Probe should have bailed near the timeout, not waited the full delay.
		if elapsed > healthProbeTimeout*2 {
			t.Errorf("expected probe to bail near %s, took %s", healthProbeTimeout, elapsed)
		}
		c := blk.Content
		if !strings.Contains(c, "snail") {
			t.Errorf("missing slow provider in output:\n%s", c)
		}
		if !strings.Contains(c, "exceeded") {
			t.Errorf("expected 'exceeded' in timeout note:\n%s", c)
		}
	})
}

func TestBuildHealthBlock_PipeInMessageSanitized(t *testing.T) {
	providers := []*stubProvider{
		{name: "noisy", status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "drift in column | row mismatch\nat path /etc",
		}},
	}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected non-nil block")
		}
		c := blk.Content
		// Find the noisy row line specifically (not the header rule line).
		var row string
		for _, line := range strings.Split(c, "\n") {
			if strings.Contains(line, "| noisy |") {
				row = line
				break
			}
		}
		if row == "" {
			t.Fatalf("missing noisy row in:\n%s", c)
		}
		// Markdown table integrity: each row should have exactly 6 pipes
		// (5 columns + leading/trailing). If any of the message's | leaked
		// through, the count would change.
		if got := strings.Count(row, "|"); got != 6 {
			t.Errorf("row should have 6 pipes after sanitization, got %d: %q", got, row)
		}
		// Newlines should be flattened.
		if strings.Contains(row, "\n") {
			t.Errorf("row should not contain newlines: %q", row)
		}
	})
}

func TestCompactNote_Truncation(t *testing.T) {
	long := strings.Repeat("word ", 40) // ~200 chars, lots of word boundaries
	got := compactNote(long)
	if len(got) > 105 { // 100 chars + ellipsis allowance
		t.Errorf("compactNote should truncate long messages, got length %d: %q", len(got), got)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated message should end in ellipsis, got %q", got)
	}
}

// TestFoveatedContext_IncludesSubstrateHealth is the end-to-end integration:
// register a stub Reconcilable, hit the foveated handler, confirm Substrate
// Health appears in the rendered context string. This is the closure of the
// sensorium → cognitive substrate proprioception loop — what Claude Code
// actually receives via the UserPromptSubmit hook.
func TestFoveatedContext_IncludesSubstrateHealth(t *testing.T) {
	providers := []*stubProvider{
		{name: "alpha", status: greenStatus()},
		{name: "beta", status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "drift detected",
		}},
	}

	withProviders(t, providers, func() {
		// Minimal server — no git, no cogdocs. Health block depends only on
		// the reconcile registry, which is what we want to exercise.
		tmp := t.TempDir()
		cfg := &Config{WorkspaceRoot: tmp, CogDir: tmp + "/.cog", Port: 0, SalienceDaysWindow: 90, WriteRouteGrantAuthDisabled: true}
		nucleus := &Nucleus{Name: "test", Card: "proprio test"}
		process := NewProcess(cfg, nucleus)
		srv := NewServer(cfg, nucleus, process)
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()

		body, _ := json.Marshal(foveatedRequest{
			Prompt:    "what's the substrate doing right now",
			Iris:      irisSignal{Size: 200000, Used: 5000},
			Profile:   "claude-code",
			SessionID: "proprio-int-test",
		})

		resp, err := http.Post(ts.URL+"/v1/context/foveated", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d, want 200", resp.StatusCode)
		}

		var result foveatedResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal("decode:", err)
		}

		// The health block must appear in the rendered context.
		if !strings.Contains(result.Context, "Substrate Health") {
			t.Errorf("rendered context missing 'Substrate Health' block:\n%s", result.Context)
		}
		if !strings.Contains(result.Context, "drift detected") {
			t.Errorf("non-green provider's note should appear in context:\n%s", result.Context)
		}
		if !strings.Contains(result.Context, "2 providers") {
			t.Errorf("summary line missing:\n%s", result.Context)
		}

		// The block should also be present in the structured response.blocks.
		var found *foveatedBlock
		for i := range result.Blocks {
			if result.Blocks[i].Name == BlockHealth {
				found = &result.Blocks[i]
				break
			}
		}
		if found == nil {
			names := make([]string, 0, len(result.Blocks))
			for _, b := range result.Blocks {
				names = append(names, b.Name)
			}
			t.Fatalf("health block not in response.blocks; got %v", names)
		}
		if found.Tier != "tier2" {
			t.Errorf("health tier=%q; want tier2", found.Tier)
		}
		if found.Stability != 60 {
			t.Errorf("health stability=%d; want 60", found.Stability)
		}
		if found.Hash == "" {
			t.Error("health block hash empty")
		}

		// Surface a preview of the rendered block (Preview field is truncated;
		// for the full text inspect result.Context).
		t.Logf("health block preview: %s", found.Preview)
	})
}

// TestBuildHealthBlock_RealisticSubstrate prints the proprioception block
// that would render against a substrate in a believable mid-flight state
// (some providers green, one degraded, one missing, one syncing). This
// exists so a developer running `go test -v -run RealisticSubstrate` can
// see exactly what Claude Code receives in its UserPromptSubmit context.
func TestBuildHealthBlock_RealisticSubstrate(t *testing.T) {
	providers := []*stubProvider{
		{name: "agent", status: greenStatus()},
		{name: "component", status: greenStatus()},
		{name: "discord", status: greenStatus()},
		{name: "eval", status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "exp-001 baseline stale (2h since refresh); 1 regression flagged",
		}},
		{name: "service", status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusSynced,
			Health:    reconcile.HealthProgressing,
			Operation: reconcile.OperationSyncing,
			Message:   "applying plan: 2 of 5 actions",
		}},
	}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected block")
		}
		t.Logf("\n%s", blk.Content)
	})
}

// TestIsGreen_AcceptsUnknownSync verifies the relaxed isGreen contract: a
// provider that reports Health=Healthy but Sync=Unknown (the common state for
// daemon-side stubs that have no comparable declared form) is treated as
// not requiring attention. Real attention-warranting Sync states like
// OutOfSync still disqualify.
func TestIsGreen_AcceptsUnknownSync(t *testing.T) {
	cases := []struct {
		name      string
		sync      reconcile.SyncStatus
		health    reconcile.HealthStatus
		op        reconcile.OperationPhase
		wantGreen bool
	}{
		{"sync-unknown-healthy-idle", reconcile.SyncStatusUnknown, reconcile.HealthHealthy, reconcile.OperationIdle, true},
		{"sync-empty-healthy-idle", "", reconcile.HealthHealthy, reconcile.OperationIdle, true},
		{"sync-synced-healthy-idle", reconcile.SyncStatusSynced, reconcile.HealthHealthy, reconcile.OperationIdle, true},
		{"sync-outofsync-healthy-idle", reconcile.SyncStatusOutOfSync, reconcile.HealthHealthy, reconcile.OperationIdle, false},
		{"sync-unknown-degraded-idle", reconcile.SyncStatusUnknown, reconcile.HealthDegraded, reconcile.OperationIdle, false},
		{"sync-unknown-missing-idle", reconcile.SyncStatusUnknown, reconcile.HealthMissing, reconcile.OperationIdle, false},
		{"sync-unknown-suspended-idle", reconcile.SyncStatusUnknown, reconcile.HealthSuspended, reconcile.OperationIdle, false},
		{"sync-synced-healthy-syncing", reconcile.SyncStatusSynced, reconcile.HealthHealthy, reconcile.OperationSyncing, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := healthSample{
				Status: reconcile.ResourceStatus{Sync: tc.sync, Health: tc.health, Operation: tc.op},
			}
			if got := h.isGreen(); got != tc.wantGreen {
				t.Errorf("isGreen()=%v, want %v", got, tc.wantGreen)
			}
		})
	}
}

// TestBuildHealthBlock_AllUnknownSyncCollapses confirms that a registry full
// of daemon-side stubs (Sync=Unknown but Health=Healthy) renders as the
// collapsed all-green summary, not the full attention table. Before the
// isGreen relax this case rendered "0 healthy, N need attention" because
// every provider failed the Sync==Synced check.
func TestBuildHealthBlock_AllUnknownSyncCollapses(t *testing.T) {
	stub := func(name string) *stubProvider {
		return &stubProvider{name: name, status: reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthHealthy,
			Operation: reconcile.OperationIdle,
		}}
	}
	providers := []*stubProvider{stub("alpha"), stub("beta"), stub("gamma")}
	withProviders(t, providers, func() {
		blk := buildHealthBlock(context.Background())
		if blk == nil {
			t.Fatal("expected non-nil block")
		}
		if strings.Contains(blk.Content, "| Provider |") {
			t.Errorf("Sync=Unknown + Healthy should collapse to summary, got table:\n%s", blk.Content)
		}
		if !strings.Contains(blk.Content, "3 providers") {
			t.Errorf("expected '3 providers' summary, got:\n%s", blk.Content)
		}
	})
}

// Helpers ────────────────────────────────────────────────────────────────────

func greenStatus() reconcile.ResourceStatus {
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusSynced,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
	}
}
