package engine

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/acp"
)

// fakeClaudePath resolves internal/acp/testdata/fakeclaude.sh — the same
// stand-in acp's own cancellation tests use to exercise Cancel's OS-level
// mechanics without a real, authenticated `claude` binary. Reused here so
// the ManagedSession state machine can be tested against real process
// lifecycle events (start, system.init, exit) without network/auth.
func fakeClaudePath(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("../acp/testdata/fakeclaude.sh")
	if err != nil {
		t.Fatalf("resolve fakeclaude.sh: %v", err)
	}
	return abs
}

func waitForState(t *testing.T, ms *ManagedSession, want ManagedSessionState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ms.State() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s: state = %s, want %s (timed out after %s)", ms.SessionID, ms.State(), want, timeout)
}

// drainInBackground keeps a ManagedSession's Events() channel empty so the
// internal pump never blocks on outCh backpressure while a test waits on
// state transitions.
func drainInBackground(ms *ManagedSession) {
	go func() {
		for range ms.Events() {
		}
	}()
}

func TestManagedSession_Lifecycle_StartingToLiveToDetached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ms, err := NewManagedSession(ctx, "fake-session", ManagedSessionOpts{ClaudePath: fakeClaudePath(t)})
	if err != nil {
		t.Fatalf("NewManagedSession: %v", err)
	}
	drainInBackground(ms)

	// fakeclaude.sh emits system.init before reading any input, so the
	// pump should observe it and flip Starting -> Live quickly.
	waitForState(t, ms, StateLive, 2*time.Second)

	if err := ms.Send(acp.NewTextPrompt("go")); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if err := ms.Detach(); err != nil {
		t.Fatalf("detach: %v", err)
	}
	// Detach() sets StateDetached synchronously; the underlying subprocess
	// finishing its stdin-close exit is asynchronous but shouldn't
	// downgrade the state (see startEventPump's "already Detached" guard).
	if got := ms.State(); got != StateDetached {
		t.Fatalf("state after Detach() = %s, want %s", got, StateDetached)
	}

	// Second Detach is idempotent (ADR-093 §6).
	if err := ms.Detach(); err != nil {
		t.Fatalf("second detach should be a no-op, got: %v", err)
	}
	if got := ms.State(); got != StateDetached {
		t.Fatalf("state after second Detach() = %s, want %s", got, StateDetached)
	}
}

func TestManagedSession_Cancel_SIGINT_GatedBeforeInit(t *testing.T) {
	// White-box: construct directly rather than through NewManagedSession
	// so we can observe the Starting/!sawInit window deterministically,
	// with no real subprocess in play. Cancel must return
	// ErrCancelBeforeInit without ever touching ms.proc (which is nil
	// here) — a nil-pointer panic instead of this error would itself be a
	// test failure.
	ms := &ManagedSession{
		SessionID: "not-yet-live",
		state:     StateStarting,
		sawInit:   false,
	}

	if err := ms.Cancel(acp.CancelSIGINT); err != ErrCancelBeforeInit {
		t.Fatalf("Cancel(SIGINT) before init = %v, want ErrCancelBeforeInit", err)
	}
}

func TestManagedSession_Cancel_StdinClose_NotGatedBeforeInit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ms, err := NewManagedSession(ctx, "fake-session-2", ManagedSessionOpts{ClaudePath: fakeClaudePath(t)})
	if err != nil {
		t.Fatalf("NewManagedSession: %v", err)
	}
	drainInBackground(ms)

	// Deliberately do NOT wait for Live — CancelStdinClose's whole point
	// (per the L1 verdict) is that it's safe regardless of turn/init
	// timing, unlike CancelSIGINT.
	if err := ms.Cancel(acp.CancelStdinClose); err != nil {
		t.Fatalf("Cancel(stdin-close) before init observed: %v", err)
	}

	waitForState(t, ms, StateDetached, 3*time.Second)
}

func TestManagedSession_Cancel_AfterDetach_IsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ms, err := NewManagedSession(ctx, "fake-session-3", ManagedSessionOpts{ClaudePath: fakeClaudePath(t)})
	if err != nil {
		t.Fatalf("NewManagedSession: %v", err)
	}
	drainInBackground(ms)
	waitForState(t, ms, StateLive, 2*time.Second)

	if err := ms.Detach(); err != nil {
		t.Fatalf("detach: %v", err)
	}

	if err := ms.Cancel(acp.CancelStdinClose); err == nil {
		t.Fatalf("Cancel after Detach should fail, got nil")
	} else if !isErrSessionNotLive(err) {
		t.Fatalf("Cancel after Detach = %v, want wrapping ErrSessionNotLive", err)
	}

	if err := ms.Send(acp.NewTextPrompt("hi")); err == nil {
		t.Fatalf("Send after Detach should fail, got nil")
	} else if !isErrSessionNotLive(err) {
		t.Fatalf("Send after Detach = %v, want wrapping ErrSessionNotLive", err)
	}
}

func isErrSessionNotLive(err error) bool {
	for err != nil {
		if err == ErrSessionNotLive {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestManagedSessionRegistry_New_IsIdempotentForLiveSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reg := NewManagedSessionRegistry()
	opts := ManagedSessionOpts{ClaudePath: fakeClaudePath(t)}

	first, err := reg.New(ctx, "fake-session-4", opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	drainInBackground(first)
	waitForState(t, first, StateLive, 2*time.Second)

	second, err := reg.New(ctx, "fake-session-4", opts)
	if err != nil {
		t.Fatalf("New (idempotent call): %v", err)
	}
	if first != second {
		t.Fatalf("New() with an already-live sessionID spawned a second subprocess instead of returning the existing one")
	}

	if got := reg.Get("fake-session-4"); got != first {
		t.Fatalf("Get() = %v, want the registered session", got)
	}

	if err := reg.Detach("fake-session-4"); err != nil {
		t.Fatalf("registry Detach: %v", err)
	}
	if got := first.State(); got != StateDetached {
		t.Fatalf("state after registry Detach = %s, want %s", got, StateDetached)
	}

	// Detaching an already-detached (still-registered) session, and an
	// entirely unregistered one, are both no-ops (ADR-093 §6).
	if err := reg.Detach("fake-session-4"); err != nil {
		t.Fatalf("second registry Detach should be a no-op: %v", err)
	}
	if err := reg.Detach("never-registered"); err != nil {
		t.Fatalf("Detach of unregistered id should be a no-op: %v", err)
	}

	// New() after Detach spawns a fresh subprocess rather than reusing the
	// detached one (ADR-093 §6 idempotency is scoped to "live", not "ever
	// existed").
	third, err := reg.New(ctx, "fake-session-4", opts)
	if err != nil {
		t.Fatalf("New after Detach: %v", err)
	}
	drainInBackground(third)
	if third == first {
		t.Fatalf("New() after Detach reused the detached session instead of spawning a fresh one")
	}
	waitForState(t, third, StateLive, 2*time.Second)
	if err := third.Detach(); err != nil {
		t.Fatalf("cleanup detach: %v", err)
	}

	snapshot := reg.List()
	if _, ok := snapshot["fake-session-4"]; !ok {
		t.Fatalf("List() missing fake-session-4: %v", snapshot)
	}
}

// stubSpawnCounter substitutes one of the package-level spawn indirections
// (spawnNewManagedSession / spawnResumeManagedSession) with a wrapper that
// counts calls while still delegating to the real fake-subprocess spawner,
// so these tests observe how many times a *spawn* happened (not just how
// many *pointers* came back distinct) without touching production code
// paths or spawning a real `claude` binary. It returns a function that
// reads the current count and a restore func that must run before the test
// returns, since the variable is package-global and other tests in this
// file rely on the real spawner.
type spawnCounter struct {
	mu    sync.Mutex
	calls int
}

func (c *spawnCounter) inc() {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
}

func (c *spawnCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestManagedSessionRegistry_New_ConcurrentSameID_SingleSpawn is the
// regression test for the check-then-act race described in the
// cog-review finding on internal/engine/managed_session.go: N goroutines
// racing New() for the same not-yet-registered sessionID must produce
// exactly one spawn and hand every caller the same *ManagedSession,
// instead of each goroutine passing the nil check and spawning its own
// subprocess (leaving all-but-one orphaned: leaked subprocess, and a
// startEventPump goroutine permanently blocked once its 32-slot outCh
// fills with nothing draining it).
func TestManagedSessionRegistry_New_ConcurrentSameID_SingleSpawn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	orig := spawnNewManagedSession
	var counter spawnCounter
	spawnNewManagedSession = func(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
		counter.inc()
		return orig(ctx, sessionID, opts)
	}
	defer func() { spawnNewManagedSession = orig }()

	reg := NewManagedSessionRegistry()
	opts := ManagedSessionOpts{ClaudePath: fakeClaudePath(t)}

	const n = 25
	results := make([]*ManagedSession, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ms, err := reg.New(ctx, "race-new-session", opts)
			results[i] = ms
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: New returned error: %v", i, err)
		}
	}
	first := results[0]
	if first == nil {
		t.Fatalf("goroutine 0 got a nil *ManagedSession with no error")
	}
	for i, ms := range results {
		if ms != first {
			t.Fatalf("goroutine %d got *ManagedSession %p, want the shared winner %p — concurrent New() calls raced past each other instead of sharing one reservation", i, ms, first)
		}
	}

	if got := counter.get(); got != 1 {
		t.Fatalf("spawnNewManagedSession invoked %d times across %d concurrent New() calls racing the same not-yet-registered sessionID; want exactly 1", got, n)
	}

	drainInBackground(first)
	waitForState(t, first, StateLive, 2*time.Second)
	if err := first.Detach(); err != nil {
		t.Fatalf("cleanup detach: %v", err)
	}
}

// TestManagedSessionRegistry_Resume_ConcurrentSameID_SingleSpawn is the
// Resume-side twin of the New test above: the same reservation race exists
// independently in Resume (it has its own
// RLock/liveLocked/RUnlock/spawn/Lock/insert sequence), so it needs its own
// regression coverage rather than relying on New's passing.
func TestManagedSessionRegistry_Resume_ConcurrentSameID_SingleSpawn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	orig := spawnResumeManagedSession
	var counter spawnCounter
	spawnResumeManagedSession = func(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
		counter.inc()
		return orig(ctx, sessionID, opts)
	}
	defer func() { spawnResumeManagedSession = orig }()

	reg := NewManagedSessionRegistry()
	opts := ManagedSessionOpts{ClaudePath: fakeClaudePath(t)}

	const n = 25
	results := make([]*ManagedSession, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ms, err := reg.Resume(ctx, "race-resume-session", opts)
			results[i] = ms
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: Resume returned error: %v", i, err)
		}
	}
	first := results[0]
	if first == nil {
		t.Fatalf("goroutine 0 got a nil *ManagedSession with no error")
	}
	for i, ms := range results {
		if ms != first {
			t.Fatalf("goroutine %d got *ManagedSession %p, want the shared winner %p — concurrent Resume() calls raced past each other instead of sharing one reservation", i, ms, first)
		}
	}

	if got := counter.get(); got != 1 {
		t.Fatalf("spawnResumeManagedSession invoked %d times across %d concurrent Resume() calls racing the same not-yet-registered sessionID; want exactly 1", got, n)
	}

	drainInBackground(first)
	waitForState(t, first, StateLive, 2*time.Second)
	if err := first.Detach(); err != nil {
		t.Fatalf("cleanup detach: %v", err)
	}
}
