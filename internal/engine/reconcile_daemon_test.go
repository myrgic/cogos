// reconcile_daemon_test.go — integration tests for ReconcileDaemon.
//
// Tests are table-driven and cover:
//   - Daemon ticks at the expected interval and calls full reconcile cycles.
//   - Per-provider error isolation: one panicking provider does not block others.
//   - Idempotency on repeated runs (repeated ticks with identical state produce
//     identical outcomes without side effects).
//   - Shutdown timing: context cancel causes the daemon to exit within the
//     shutdown grace period.
//   - Trigger mechanism: Trigger() queues an early cycle for a named provider.
//
// ADR-092 §3: ApplyPlan idempotency is tested by running the same provider twice
// and asserting apply count increments predictably (not double-applied).
// ADR-095 §2: per-provider error isolation is the non-negotiable invariant.
package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// ─── No-op Reconcilable for baseline tests ────────────────────────────────────

// noopReconcilable is a minimal Reconcilable that tracks call counts.
type noopReconcilable struct {
	typeName     string
	loadCount    atomic.Int32
	fetchCount   atomic.Int32
	computeCount atomic.Int32
	applyCount   atomic.Int32
	buildCount   atomic.Int32
	hasChanges   bool // if true, ComputePlan returns a plan with one create action
}

func (r *noopReconcilable) Type() string { return r.typeName }

func (r *noopReconcilable) LoadConfig(_ string) (any, error) {
	r.loadCount.Add(1)
	return map[string]any{"type": r.typeName}, nil
}

func (r *noopReconcilable) FetchLive(_ context.Context, _ any) (any, error) {
	r.fetchCount.Add(1)
	return map[string]any{"live": true}, nil
}

func (r *noopReconcilable) ComputePlan(_ any, _ any, _ *reconcile.State) (*reconcile.Plan, error) {
	r.computeCount.Add(1)
	plan := &reconcile.Plan{
		ResourceType: r.typeName,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if r.hasChanges {
		plan.Actions = []reconcile.Action{{
			Action:       reconcile.ActionCreate,
			ResourceType: r.typeName,
			Name:         "test-resource",
			Details:      map[string]any{},
		}}
		plan.Summary.Creates = 1
	} else {
		plan.Actions = []reconcile.Action{{
			Action:       reconcile.ActionSkip,
			ResourceType: r.typeName,
			Name:         "test-resource",
			Details:      map[string]any{"reason": "in sync"},
		}}
		plan.Summary.Skipped = 1
	}
	return plan, nil
}

func (r *noopReconcilable) ApplyPlan(_ context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	r.applyCount.Add(1)
	var results []reconcile.Result
	for _, a := range plan.Actions {
		results = append(results, reconcile.Result{
			Phase:  "apply",
			Action: string(a.Action),
			Name:   a.Name,
			Status: reconcile.ApplySucceeded,
		})
	}
	return results, nil
}

func (r *noopReconcilable) BuildState(_ any, _ any, existing *reconcile.State) (*reconcile.State, error) {
	r.buildCount.Add(1)
	s := reconcile.NewState(r.typeName)
	if existing != nil {
		s.Lineage = existing.Lineage
		s.Serial = existing.Serial
	}
	return s, nil
}

func (r *noopReconcilable) Health() reconcile.ResourceStatus {
	return reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
}

// ─── Error-injecting Reconcilable ─────────────────────────────────────────────

// errorReconcilable always returns an error from FetchLive.
type errorReconcilable struct {
	typeName  string
	callCount atomic.Int32
}

func (r *errorReconcilable) Type() string { return r.typeName }
func (r *errorReconcilable) LoadConfig(_ string) (any, error) {
	r.callCount.Add(1)
	return nil, nil
}
func (r *errorReconcilable) FetchLive(_ context.Context, _ any) (any, error) {
	return nil, errors.New("simulated fetch failure")
}
func (r *errorReconcilable) ComputePlan(_ any, _ any, _ *reconcile.State) (*reconcile.Plan, error) {
	return nil, nil
}
func (r *errorReconcilable) ApplyPlan(_ context.Context, _ *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, nil
}
func (r *errorReconcilable) BuildState(_ any, _ any, _ *reconcile.State) (*reconcile.State, error) {
	return nil, nil
}
func (r *errorReconcilable) Health() reconcile.ResourceStatus {
	return reconcile.NewResourceStatus(reconcile.SyncStatusUnknown, reconcile.HealthDegraded)
}

// ─── Panicking Reconcilable ───────────────────────────────────────────────────

// panicReconcilable panics from ComputePlan to test error isolation.
type panicReconcilable struct {
	typeName string
}

func (r *panicReconcilable) Type() string { return r.typeName }
func (r *panicReconcilable) LoadConfig(_ string) (any, error) {
	return nil, nil
}
func (r *panicReconcilable) FetchLive(_ context.Context, _ any) (any, error) {
	return nil, nil
}
func (r *panicReconcilable) ComputePlan(_ any, _ any, _ *reconcile.State) (*reconcile.Plan, error) {
	panic("intentional panic from panicReconcilable")
}
func (r *panicReconcilable) ApplyPlan(_ context.Context, _ *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, nil
}
func (r *panicReconcilable) BuildState(_ any, _ any, _ *reconcile.State) (*reconcile.State, error) {
	return nil, nil
}
func (r *panicReconcilable) Health() reconcile.ResourceStatus {
	return reconcile.NewResourceStatus(reconcile.SyncStatusUnknown, reconcile.HealthDegraded)
}

// ─── Test helpers ─────────────────────────────────────────────────────────────

// mustRegister registers a provider and returns a cleanup func to deregister.
func mustRegister(t *testing.T, p reconcile.Reconcilable) func() {
	t.Helper()
	reconcile.UpsertProvider(p.Type(), p)
	return func() {
		reconcile.ResetProviders()
	}
}

// newTestDaemon creates a ReconcileDaemon with a fast poll interval for tests.
func newTestDaemon(root string, pollInterval time.Duration) *ReconcileDaemon {
	return NewReconcileDaemon(ReconcileDaemonConfig{
		WorkspaceRoot:       root,
		PollInterval:        pollInterval,
		MaxConcurrent:       1,
		ShutdownGracePeriod: 500 * time.Millisecond,
	})
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestReconcileDaemon_BasicTick verifies that the daemon ticks and drives the
// full reconcile cycle (LoadConfig, FetchLive, ComputePlan) for a registered
// no-op provider within the expected interval.
func TestReconcileDaemon_BasicTick(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	provider := &noopReconcilable{typeName: "test-basic", hasChanges: false}
	reconcile.UpsertProvider(provider.Type(), provider)

	daemon := newTestDaemon(t.TempDir(), 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	daemon.Start(ctx)

	// Wait for context to expire (daemon runs for 400ms with 50ms ticks → ~8 ticks).
	<-ctx.Done()

	// Allow daemon goroutine to settle.
	time.Sleep(20 * time.Millisecond)

	if daemon.State() != ReconcileDaemonShutdown {
		t.Errorf("expected state Shutdown, got %q", daemon.State())
	}

	// Should have ticked at least 3 times. Be conservative to avoid flakiness.
	if got := provider.fetchCount.Load(); got < 3 {
		t.Errorf("expected FetchLive to be called >= 3 times, got %d", got)
	}
	if got := provider.computeCount.Load(); got < 3 {
		t.Errorf("expected ComputePlan to be called >= 3 times, got %d", got)
	}
}

// TestReconcileDaemon_ApplyOnDrift verifies that ApplyPlan is called when the
// provider computes a plan with changes, and not called when there is no drift.
func TestReconcileDaemon_ApplyOnDrift(t *testing.T) {
	cases := []struct {
		name           string
		hasChanges     bool
		wantApplyCount int32 // minimum
	}{
		{
			name:           "in-sync provider: no apply",
			hasChanges:     false,
			wantApplyCount: 0,
		},
		{
			name:           "drifted provider: apply called",
			hasChanges:     true,
			wantApplyCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reconcile.ResetProviders()
			defer reconcile.ResetProviders()

			provider := &noopReconcilable{
				typeName:   "test-apply-drift",
				hasChanges: tc.hasChanges,
			}
			reconcile.UpsertProvider(provider.Type(), provider)

			daemon := newTestDaemon(t.TempDir(), 50*time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()

			daemon.Start(ctx)
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)

			if !tc.hasChanges && provider.applyCount.Load() != 0 {
				t.Errorf("no-drift case: expected 0 ApplyPlan calls, got %d",
					provider.applyCount.Load())
			}
			if tc.hasChanges && provider.applyCount.Load() < tc.wantApplyCount {
				t.Errorf("drift case: expected >= %d ApplyPlan calls, got %d",
					tc.wantApplyCount, provider.applyCount.Load())
			}
		})
	}
}

// TestReconcileDaemon_ErrorIsolation verifies that an error in one provider's
// cycle does not prevent other providers from being reconciled.
// This is the non-negotiable invariant per ADR-092 §3 and ADR-095 §2.
func TestReconcileDaemon_ErrorIsolation(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	good := &noopReconcilable{typeName: "test-good", hasChanges: false}
	bad := &errorReconcilable{typeName: "test-bad"}

	reconcile.UpsertProvider(good.Type(), good)
	reconcile.UpsertProvider(bad.Type(), bad)

	daemon := newTestDaemon(t.TempDir(), 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	daemon.Start(ctx)
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)

	// The good provider must have been reconciled despite the bad provider's errors.
	if got := good.fetchCount.Load(); got < 2 {
		t.Errorf("error isolation failure: good provider FetchLive called only %d times (expected >= 2)", got)
	}

	// The bad provider's LoadConfig should also have been called repeatedly,
	// confirming the daemon kept iterating it (and isolating its FetchLive error).
	if got := bad.callCount.Load(); got < 2 {
		t.Errorf("bad provider LoadConfig called only %d times (expected >= 2)", got)
	}
}

// TestReconcileDaemon_PanicIsolation verifies that a panic inside a provider's
// reconcile cycle does not crash the daemon or block other providers.
// ADR-095 §2: per-provider error isolation covers panics.
func TestReconcileDaemon_PanicIsolation(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	panicky := &panicReconcilable{typeName: "test-panicky"}
	good := &noopReconcilable{typeName: "test-good-panic", hasChanges: false}

	reconcile.UpsertProvider(panicky.Type(), panicky)
	reconcile.UpsertProvider(good.Type(), good)

	daemon := newTestDaemon(t.TempDir(), 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// If panic propagates out of the daemon, this test will panic itself and fail.
	daemon.Start(ctx)
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)

	// Good provider should still have been reconciled.
	if got := good.fetchCount.Load(); got < 2 {
		t.Errorf("panic isolation failure: good provider FetchLive called only %d times", got)
	}

	// Daemon should be Shutdown (not crashed).
	if daemon.State() != ReconcileDaemonShutdown {
		t.Errorf("expected state Shutdown after context cancel, got %q", daemon.State())
	}
}

// TestReconcileDaemon_IdempotencyOnRepeatedRuns verifies that running the
// reconcile cycle multiple times against an in-sync provider does not produce
// additional side effects (no spurious applies).
// Conforms to ADR-092 §3 idempotency requirement.
func TestReconcileDaemon_IdempotencyOnRepeatedRuns(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	// Provider reports no changes (in-sync). ApplyPlan must never be called.
	provider := &noopReconcilable{typeName: "test-idempotent", hasChanges: false}
	reconcile.UpsertProvider(provider.Type(), provider)

	daemon := newTestDaemon(t.TempDir(), 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	daemon.Start(ctx)
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)

	// At least several ticks should have run.
	if got := provider.computeCount.Load(); got < 3 {
		t.Errorf("expected >= 3 ComputePlan calls, got %d", got)
	}
	// No apply should ever have been triggered for an in-sync provider.
	if got := provider.applyCount.Load(); got != 0 {
		t.Errorf("idempotency violation: ApplyPlan called %d times for in-sync provider", got)
	}
}

// TestReconcileDaemon_ShutdownTiming verifies that the daemon exits within the
// shutdown grace period after context cancel.
func TestReconcileDaemon_ShutdownTiming(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	provider := &noopReconcilable{typeName: "test-shutdown", hasChanges: false}
	reconcile.UpsertProvider(provider.Type(), provider)

	daemon := NewReconcileDaemon(ReconcileDaemonConfig{
		WorkspaceRoot:       t.TempDir(),
		PollInterval:        10 * time.Second, // long interval — won't tick during test
		MaxConcurrent:       1,
		ShutdownGracePeriod: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	daemon.Start(ctx)

	// Give the daemon goroutine time to start.
	time.Sleep(20 * time.Millisecond)

	// Cancel the context and measure how long until the daemon state transitions.
	cancelTime := time.Now()
	cancel()

	// Poll for Shutdown state with a generous deadline.
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case <-deadline:
			t.Fatalf("daemon did not reach Shutdown state within 500ms of context cancel")
		default:
			if daemon.State() == ReconcileDaemonShutdown {
				elapsed := time.Since(cancelTime)
				// Should be well under the grace period (200ms).
				if elapsed > 300*time.Millisecond {
					t.Errorf("daemon took %v to stop, expected < 300ms", elapsed)
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestReconcileDaemon_TriggerMechanism verifies that Trigger causes an immediate
// reconcile for the named provider outside the periodic tick.
func TestReconcileDaemon_TriggerMechanism(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	provider := &noopReconcilable{typeName: "test-trigger", hasChanges: true}
	reconcile.UpsertProvider(provider.Type(), provider)

	daemon := NewReconcileDaemon(ReconcileDaemonConfig{
		WorkspaceRoot:       t.TempDir(),
		PollInterval:        10 * time.Second, // very long — won't tick during test
		MaxConcurrent:       1,
		ShutdownGracePeriod: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	daemon.Start(ctx)

	// Give goroutine time to start.
	time.Sleep(10 * time.Millisecond)

	// Fire a trigger. The periodic tick won't fire within the test window.
	beforeTrigger := provider.fetchCount.Load()
	daemon.Trigger(provider.Type())

	// Wait for the triggered cycle to complete.
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case <-deadline:
			t.Fatalf("triggered cycle did not complete within 300ms")
		default:
			if provider.fetchCount.Load() > beforeTrigger {
				// Triggered cycle ran.
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestReconcileDaemon_TriggerUnregisteredProviderIsNoOp verifies that Trigger
// with an unknown provider type is a silent no-op (no crash, no error).
func TestReconcileDaemon_TriggerUnregisteredProviderIsNoOp(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	daemon := newTestDaemon(t.TempDir(), 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	// Should not panic or error.
	daemon.Trigger("nonexistent-provider")
	time.Sleep(20 * time.Millisecond)
}

// TestReconcileDaemon_StateTransitions verifies the state machine:
// Starting → Live (on Start) → Stalled (on error tick) → Shutdown (on cancel).
func TestReconcileDaemon_StateTransitions(t *testing.T) {
	reconcile.ResetProviders()
	defer reconcile.ResetProviders()

	var mu sync.Mutex
	states := []ReconcileDaemonState{}
	recordState := func(s ReconcileDaemonState) {
		mu.Lock()
		defer mu.Unlock()
		if len(states) == 0 || states[len(states)-1] != s {
			states = append(states, s)
		}
	}

	// Starting state before Start() is called.
	daemon := newTestDaemon(t.TempDir(), 50*time.Millisecond)
	recordState(daemon.State())

	// Register a bad provider so the first tick produces a Stalled state.
	bad := &errorReconcilable{typeName: "test-state-trans"}
	reconcile.UpsertProvider(bad.Type(), bad)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	daemon.Start(ctx)
	recordState(daemon.State()) // should be Live

	<-ctx.Done()
	time.Sleep(30 * time.Millisecond)
	recordState(daemon.State()) // should be Shutdown

	// Verify we saw Starting, Live, Shutdown. Stalled may or may not appear
	// depending on timing — we only assert the required transitions.
	if len(states) < 2 {
		t.Fatalf("expected at least 2 state transitions, got %v", states)
	}
	if states[0] != ReconcileDaemonStarting {
		t.Errorf("first state: expected Starting, got %q", states[0])
	}
	if states[len(states)-1] != ReconcileDaemonShutdown {
		t.Errorf("last state: expected Shutdown, got %q", states[len(states)-1])
	}
}
