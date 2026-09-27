// health_staleness_test.go — regression tests for the 2026-08-27 silent-stall.
//
// THE BUG THESE PIN
// -----------------
// On 2026-08-27 the live kernel snapshot on a node reported
//
//	vitals-retention {"health": "Healthy", "operation": "Idle", "sync": "Synced"}
//
// while the newest row on disk was 17 hours old. The autonomic ticker was
// still emitting bus_kernel_proprio events every tick (verified against
// .cog/.state/buses/bus_kernel_proprio/events.jsonl, seq 15541 at 18:08Z) and
// the recorder had written nothing since 01:02:56Z.
//
// Health() was structurally incapable of noticing. It reported Degraded only
// when lastAppendErr was non-nil within the last 10 minutes. A recorder that
// stops being invoked never sets an error, so "working perfectly" and "dead
// since last night" produced byte-identical status.
//
// That is the "monitor proven only by silence" failure class: a monitor whose
// passing signal cannot be distinguished from its own death. These tests make
// the distinction observable.
package vitalsretention

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// newTestRecorder returns a Recorder with a workspace root set, so Health()
// gets past its "no workspace root" guard and reaches the checks under test.
func newTestRecorder(t *testing.T) *Recorder {
	t.Helper()
	SetWorkspaceRoot(t.TempDir())
	t.Cleanup(func() { SetWorkspaceRoot("") })
	return &Recorder{}
}

// TestHealthDegradedWhenAppendsStop is the primary regression test: a recorder
// that succeeded once and then went quiet must report Degraded.
//
// Before the fix this returned Healthy, which is exactly what that node showed
// for 17 hours.
func TestHealthDegradedWhenAppendsStop(t *testing.T) {
	r := newTestRecorder(t)

	// A successful append well outside the staleness window, and — critically
	// — no error recorded, which is what makes the old code report green.
	r.lastAppendOKAt = time.Now().Add(-17 * time.Hour)
	r.lastAppendAt = r.lastAppendOKAt
	r.lastAppendErr = nil

	st := r.Health()
	if st.Health != reconcile.HealthDegraded {
		t.Fatalf("Health() = %v; want Degraded.\n"+
			"A recorder whose last successful append was 17h ago is not healthy — "+
			"this is the exact live state observed on a node 2026-08-27 while "+
			"the provider self-reported Healthy.", st.Health)
	}
	if !strings.Contains(st.Message, "stale") {
		t.Errorf("Health().Message = %q; want it to name the staleness so an "+
			"operator can tell this from an ordinary write failure", st.Message)
	}
}

// TestHealthHealthyWhenAppendsRecent guards the other direction: the staleness
// check must not fire on a working recorder, or it becomes noise and gets
// disabled — which would recreate the blindness by a different route.
func TestHealthHealthyWhenAppendsRecent(t *testing.T) {
	r := newTestRecorder(t)
	r.lastAppendOKAt = time.Now().Add(-30 * time.Second)
	r.lastAppendAt = r.lastAppendOKAt

	if st := r.Health(); st.Health != reconcile.HealthHealthy {
		t.Fatalf("Health() = %v (%q); want Healthy for an append 30s ago",
			st.Health, st.Message)
	}
}

// TestHealthNoFalsePositiveAtBoot: immediately after start there has been no
// append yet and that is correct, not a fault. Reporting Degraded here would
// make every daemon boot look broken for the first 15 minutes.
func TestHealthNoFalsePositiveAtBoot(t *testing.T) {
	r := newTestRecorder(t)
	r.startedAt = time.Now() // this recorder just booted

	if st := r.Health(); st.Health != reconcile.HealthHealthy {
		t.Fatalf("Health() = %v (%q); want Healthy — a fresh recorder that has "+
			"not yet seen a tick is not stale", st.Health, st.Message)
	}
}

// TestBootGraceIsPerInstance pins the fix for the cog-review note on PR #585:
// the grace period must key off each Recorder's own start time, not a shared
// package-level stamp. With a global, a recorder constructed after the
// package had been loaded a while would be born already "stale" — wrong, and
// confusing to debug.
func TestBootGraceIsPerInstance(t *testing.T) {
	SetWorkspaceRoot(t.TempDir())
	t.Cleanup(func() { SetWorkspaceRoot("") })

	// An old recorder that never appended: genuinely stale.
	old := &Recorder{startedAt: time.Now().Add(-4 * time.Hour)}
	// A brand-new one, created in the same process at the same moment.
	fresh := &Recorder{startedAt: time.Now()}

	if got := old.Health().Health; got != reconcile.HealthDegraded {
		t.Errorf("old recorder Health() = %v; want Degraded", got)
	}
	if got := fresh.Health().Health; got != reconcile.HealthHealthy {
		t.Errorf("fresh recorder Health() = %v; want Healthy — its grace period "+
			"must not be inherited from an older sibling or from package init", got)
	}
}

// TestBootStampFallsBackToPackageStart: a zero-value Recorder (every existing
// construction site) must keep working without setting startedAt.
func TestBootStampFallsBackToPackageStart(t *testing.T) {
	r := &Recorder{}
	if !r.bootStamp().Equal(processStart) {
		t.Errorf("bootStamp() = %v; want the package stamp %v for a zero-value "+
			"Recorder, so existing callers need no change", r.bootStamp(), processStart)
	}
}

// TestHealthDegradedWhenNeverAppendedAfterLongUptime catches the wiring
// failure: the process has been up for hours and the handler has never once
// been invoked, so the bus dispatch was never connected. Without this case a
// never-wired recorder reports Healthy forever, since lastAppendOKAt stays
// zero and no error is ever recorded.
func TestHealthDegradedWhenNeverAppendedAfterLongUptime(t *testing.T) {
	r := newTestRecorder(t)

	prev := processStart
	processStart = time.Now().Add(-4 * time.Hour)
	t.Cleanup(func() { processStart = prev })

	st := r.Health()
	if st.Health != reconcile.HealthDegraded {
		t.Fatalf("Health() = %v; want Degraded — 4h of uptime with zero "+
			"successful appends means the bus dispatch is not wired", st.Health)
	}
	if !strings.Contains(st.Message, "since process start") {
		t.Errorf("Health().Message = %q; want it to distinguish never-appended "+
			"from went-stale, since the two have different causes", st.Message)
	}
}

// TestRecordAppendResultOnlyStampsSuccess pins the field semantics the
// staleness check depends on: a failing append must NOT refresh the liveness
// clock, or a recorder erroring every tick would look alive.
func TestRecordAppendResultOnlyStampsSuccess(t *testing.T) {
	r := &Recorder{}

	r.recordAppendResult(nil)
	okAfterSuccess := r.lastAppendOKAt
	if okAfterSuccess.IsZero() {
		t.Fatal("lastAppendOKAt not stamped after a successful append")
	}

	time.Sleep(2 * time.Millisecond)
	r.recordAppendResult(errors.New("disk full"))

	if !r.lastAppendOKAt.Equal(okAfterSuccess) {
		t.Error("a FAILED append advanced lastAppendOKAt; the liveness clock " +
			"must only move on success, otherwise a recorder failing every " +
			"tick reports as alive")
	}
	if r.lastAppendAt.Equal(okAfterSuccess) {
		t.Error("lastAppendAt should track every attempt, including failures")
	}
}

// TestAppendErrorStillTakesPrecedence: an active write error is more specific
// and more actionable than staleness, so it must be reported first.
func TestAppendErrorStillTakesPrecedence(t *testing.T) {
	r := newTestRecorder(t)
	r.lastAppendOKAt = time.Now().Add(-17 * time.Hour)
	r.lastAppendErr = errors.New("permission denied")
	r.lastAppendAt = time.Now()

	st := r.Health()
	if st.Health != reconcile.HealthDegraded {
		t.Fatalf("Health() = %v; want Degraded", st.Health)
	}
	if !strings.Contains(st.Message, "permission denied") {
		t.Errorf("Health().Message = %q; want the concrete write error, which "+
			"is more actionable than the derived staleness", st.Message)
	}
}
