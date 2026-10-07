package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/acprpc/acpfake"
)

// TestMain lets this test binary double as the fake ACP agent: the
// HermesACPDriver tests spawn os.Args[0] with ACPFAKE_AGENT=1.
func TestMain(m *testing.M) {
	acpfake.MaybeRunMain()
	os.Exit(m.Run())
}

func fakeHermesDriver(t *testing.T, mut func(*HermesACPDriverConfig)) *HermesACPDriver {
	t.Helper()
	cfg := HermesACPDriverConfig{
		Command:           func(string) []string { return []string{os.Args[0], "-test.run=^$"} },
		Env:               append(os.Environ(), acpfake.EnvVar+"=1"),
		InitTimeout:       10 * time.Second,
		CrashWindow:       time.Hour, // default in tests: any crash is terminal
		RestartBackoff:    time.Millisecond,
		PermissionTimeout: 10 * time.Second,
	}
	if mut != nil {
		mut(&cfg)
	}
	d := NewHermesACPDriver(cfg)
	t.Cleanup(d.Close)
	return d
}

// waitEvent reads the ring until pred matches or the deadline passes.
func waitEvent(t *testing.T, s *HermesACPSession, since uint64, pred func(ManagedSessionEvent) bool) ManagedSessionEvent {
	t.Helper()
	sub := s.Events().Subscribe(since, 512)
	defer s.Events().Unsubscribe(sub)
	for _, ev := range sub.Replay {
		if pred(ev) {
			return ev
		}
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Live:
			if !ok {
				t.Fatal("event stream closed before match")
			}
			if pred(ev) {
				return ev
			}
		case <-deadline:
			t.Fatal("timed out waiting for event")
		}
	}
}

func kind(k string) func(ManagedSessionEvent) bool {
	return func(ev ManagedSessionEvent) bool { return ev.Kind == k }
}

func TestHermesACPDriver_NewPromptEvents(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
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
	end := waitEvent(t, s, 0, kind(MSEventTurnEnd))
	if !strings.Contains(string(end.Data), `"stop_reason":"end_turn"`) || !strings.Contains(string(end.Data), turn) {
		t.Fatalf("turn_end = %s", end.Data)
	}
	evs, gap := s.Events().Since(0)
	if gap {
		t.Fatal("unexpected gap")
	}
	var kinds []string
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("seq %d at index %d", ev.Seq, i)
		}
		kinds = append(kinds, ev.Kind)
	}
	want := []string{MSEventState, MSEventTurnStart, MSEventSessionUpdate, MSEventTurnEnd}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	if !strings.Contains(string(evs[2].Data), "echo:hello") {
		t.Fatalf("update = %s", evs[2].Data)
	}
	if info := s.Info(); info.TurnInFlight != "" || info.Driver != DriverHermesACP {
		t.Fatalf("info = %+v", info)
	}
}

func TestHermesACPDriver_ForkSharesProcess(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	front, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	work, err := d.Create(context.Background(), HermesACPOpts{ForkOf: front.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if work.proc != front.proc || work.ID() == front.ID() || work.Info().ForkOf != front.ID() {
		t.Fatalf("fork: front=%s work=%+v", front.ID(), work.Info())
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{ForkOf: "nope"}); err == nil {
		t.Fatal("fork of unknown session should fail")
	}
	// Deleting the parent keeps the shared process alive for the fork.
	if err := d.Delete(front.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := work.Prompt(json.RawMessage(`"still here"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, work, 0, kind(MSEventTurnEnd))
	if len(d.List()) != 1 {
		t.Fatalf("list = %+v", d.List())
	}
}

func TestHermesACPDriver_LoadReplaysIntoRing(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Load: "old-session", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if s.ID() != "old-session" {
		t.Fatalf("id = %s", s.ID())
	}
	ev := waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	if !strings.Contains(string(ev.Data), "replayed history for old-session") {
		t.Fatalf("update = %s", ev.Data)
	}
	again, err := d.Create(context.Background(), HermesACPOpts{Load: "old-session"})
	if err != nil || again != s {
		t.Fatalf("load of a live session should be idempotent: %v %p %p", err, again, s)
	}
}

func TestHermesACPDriver_PermissionParksUntilAnswered(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"permission to write"`)); err != nil {
		t.Fatal(err)
	}
	req := waitEvent(t, s, 0, kind(MSEventPermissionRequest))
	var pr struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(req.Data, &pr)
	// No client is attached; the request waits in the kernel and is visible.
	time.Sleep(50 * time.Millisecond)
	if info := s.Info(); len(info.PendingPermissions) != 1 || info.TurnInFlight == "" {
		t.Fatalf("info = %+v", info)
	}
	if err := s.ResolvePermission("bogus", "allow"); err != ErrPermissionNotFound {
		t.Fatalf("bogus resolve: %v", err)
	}
	if err := s.ResolvePermission(pr.RequestID, "allow"); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, s, req.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "permission:selected:allow")
	})
	waitEvent(t, s, ev.Seq, kind(MSEventTurnEnd))
}

func TestHermesACPDriver_PermissionTimeoutCancels(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.PermissionTimeout = 50 * time.Millisecond })
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Prompt(json.RawMessage(`"permission"`))
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventPermissionResolved && strings.Contains(string(ev.Data), `"by":"timeout"`)
	})
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "permission:cancelled:")
	})
}

func TestHermesACPDriver_CancelAndSingleTurn(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`[{"type":"text","text":"slow job"}]`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"second"`)); err != ErrTurnInFlight {
		t.Fatalf("second prompt: %v", err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate)) // the fake registered the turn
	if err := s.Cancel(); err != nil {
		t.Fatal(err)
	}
	end := waitEvent(t, s, 0, kind(MSEventTurnEnd))
	if !strings.Contains(string(end.Data), `"stop_reason":"cancelled"`) {
		t.Fatalf("turn_end = %s", end.Data)
	}
	if err := s.SetMode(context.Background(), "accept_edits"); err != nil {
		t.Fatal(err)
	}
	if s.Info().Mode != "accept_edits" {
		t.Fatalf("mode = %q", s.Info().Mode)
	}
}

func TestHermesACPDriver_EarlyCrashIsTerminal(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	end := waitEvent(t, s, 0, kind(MSEventTurnEnd))
	if !strings.Contains(string(end.Data), "agent process exited") {
		t.Fatalf("turn_end = %s", end.Data)
	}
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventState && strings.Contains(string(ev.Data), `"crashed"`)
	})
	if _, err := s.Prompt(json.RawMessage(`"x"`)); err == nil {
		t.Fatal("prompt into crashed session should fail")
	}
}

func TestHermesACPDriver_LateCrashRestartsAndLoads(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.CrashWindow = time.Nanosecond })
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	ev := waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "replayed history for "+s.ID())
	})
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventState && strings.Contains(string(ev.Data), `"live"`)
	})
	if _, err := s.Prompt(json.RawMessage(`"after restart"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "echo:after restart")
	})
}

func TestEventRing_ReplayGapAndOverflow(t *testing.T) {
	r := NewEventRing("s", 3)
	for i := 0; i < 5; i++ {
		r.Append("k", map[string]int{"i": i})
	}
	evs, gap := r.Since(0)
	if !gap || len(evs) != 3 || evs[0].Seq != 3 || evs[2].Seq != 5 {
		t.Fatalf("since 0: gap=%v evs=%+v", gap, evs)
	}
	evs, gap = r.Since(2)
	if gap || len(evs) != 3 {
		t.Fatalf("since 2: gap=%v n=%d", gap, len(evs))
	}
	sub := r.Subscribe(4, 1)
	if len(sub.Replay) != 1 || sub.Replay[0].Seq != 5 || sub.LastSeq != 5 {
		t.Fatalf("subscribe replay = %+v", sub)
	}
	r.Append("k", nil) // fills the 1-slot buffer
	r.Append("k", nil) // overflows: subscriber is dropped, not blocked
	if ev := <-sub.Live; ev.Seq != 6 {
		t.Fatalf("live seq = %d", ev.Seq)
	}
	if _, ok := <-sub.Live; ok {
		t.Fatal("overflowed subscriber should be closed")
	}
	r.Close()
	if _, ok := <-r.Subscribe(0, 1).Live; ok {
		t.Fatal("subscribe after close should yield a closed channel")
	}
}

// countingDriver wraps fakeHermesDriver's command with a spawn counter and an
// optional gate: while gate is non-nil every spawn blocks in Command until it
// is closed, which holds a Create inside its load deterministically.
func countingDriver(t *testing.T, gate chan struct{}, mut func(*HermesACPDriverConfig)) (*HermesACPDriver, *atomic.Int32) {
	t.Helper()
	var spawns atomic.Int32
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.Command = func(string) []string {
			spawns.Add(1)
			if gate != nil {
				<-gate
			}
			return []string{os.Args[0], "-test.run=^$"}
		}
		if mut != nil {
			mut(c)
		}
	})
	return d, &spawns
}

func waitState(t *testing.T, s *HermesACPSession, want ManagedSessionState) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for s.State() != want {
		if time.Now().After(deadline) {
			t.Fatalf("session %s: state %s, want %s", s.ID(), s.State(), want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Review #666 finding 1a: a resume that arrives while the driver's own
// crash-restart is in flight (state Starting) must attach to that session,
// not spawn a second process and load the same id into it.
func TestHermesACPDriver_ResumeDuringRestartAttaches(t *testing.T) {
	d, spawns := countingDriver(t, nil, func(c *HermesACPDriverConfig) {
		c.CrashWindow = time.Nanosecond
		c.RestartBackoff = 1500 * time.Millisecond // holds the Starting window open
	})
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	waitState(t, s, StateStarting)

	got, err := d.Create(context.Background(), HermesACPOpts{Load: s.ID(), Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if got != s {
		t.Fatal("resume during restart returned a different session object (split brain)")
	}
	waitState(t, s, StateLive)
	if n := spawns.Load(); n != 2 { // the original process + the driver's own restart; the resume spawns none
		t.Fatalf("spawned %d hermes processes, want 2 (original + restart)", n)
	}
}

// Review #666 finding 1b: two overlapping resumes of one unregistered id
// share one process and one session.
func TestHermesACPDriver_ConcurrentLoadsShareOneProcess(t *testing.T) {
	gate := make(chan struct{})
	d, spawns := countingDriver(t, gate, nil)
	type res struct {
		s   *HermesACPSession
		err error
	}
	out := make(chan res, 2)
	load := func() {
		s, err := d.Create(context.Background(), HermesACPOpts{Load: "ext-1", Cwd: t.TempDir()})
		out <- res{s, err}
	}
	go load()
	for spawns.Load() < 1 { // first load is now parked inside Command, holding the claim
		time.Sleep(time.Millisecond)
	}
	go load()
	time.Sleep(200 * time.Millisecond) // a second spawn, if the bug is present, happens within microseconds
	if n := spawns.Load(); n != 1 {
		close(gate)
		t.Fatalf("second concurrent load spawned its own process (spawns=%d)", n)
	}
	close(gate)
	a, b := <-out, <-out
	if a.err != nil || b.err != nil {
		t.Fatalf("loads failed: %v / %v", a.err, b.err)
	}
	if a.s != b.s {
		t.Fatal("concurrent loads of one id returned different session objects")
	}
	if n := spawns.Load(); n != 1 {
		t.Fatalf("spawns = %d, want 1", n)
	}
}

// Review #666 finding 2: a session deleted while its process is in the
// crash-restart backoff must stay Detached and must not be session/load'ed.
func TestHermesACPDriver_DeleteDuringRestartStaysDeleted(t *testing.T) {
	d, _ := countingDriver(t, nil, func(c *HermesACPDriverConfig) {
		c.CrashWindow = time.Nanosecond
		c.RestartBackoff = 1500 * time.Millisecond
	})
	a, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.Create(context.Background(), HermesACPOpts{ForkOf: a.ID()}) // shares a's process
	if err != nil {
		t.Fatal(err)
	}
	_, _ = a.Prompt(json.RawMessage(`"crash now"`))
	waitState(t, a, StateStarting)
	waitState(t, b, StateStarting)
	if err := d.Delete(b.ID()); err != nil {
		t.Fatal(err)
	}
	waitState(t, a, StateLive) // the restart finished and loaded a
	if st := b.State(); st != StateDetached {
		t.Fatalf("deleted session resurrected: state %s", st)
	}
	evs, _ := b.Events().Since(0)
	for _, ev := range evs {
		if ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "replayed history for "+b.ID()) {
			t.Fatal("restart session/load'ed a session that was deleted during the backoff")
		}
	}
}

// A Delete that lands while a load of that id is still in flight is honoured
// when the load finishes: the id is not resurrected and the process is not leaked.
func TestHermesACPDriver_DeleteDuringLoadIsHonoured(t *testing.T) {
	gate := make(chan struct{})
	d, _ := countingDriver(t, gate, nil)
	done := make(chan error, 1)
	go func() {
		_, err := d.Create(context.Background(), HermesACPOpts{Load: "ext-2", Cwd: t.TempDir()})
		done <- err
	}()
	for {
		d.mu.Lock()
		_, inflight := d.loads["ext-2"]
		d.mu.Unlock()
		if inflight {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := d.Delete("ext-2"); err != nil {
		t.Fatal(err)
	}
	close(gate)
	if err := <-done; !errors.Is(err, ErrManagedSessionNotFound) {
		t.Fatalf("load of a deleted id returned %v, want ErrManagedSessionNotFound", err)
	}
	if d.Get("ext-2") != nil {
		t.Fatal("deleted id is registered after its load finished")
	}
}

// Sibling of review #666 finding 1: resuming a Crashed session replaces its
// registry entry, so the old one must be detached (not left behind with a
// live process and a ring nobody serves).
func TestHermesACPDriver_ResumeOfCrashedDetachesOld(t *testing.T) {
	d := fakeHermesDriver(t, nil) // CrashWindow = 1h: an early crash is terminal
	old, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = old.Prompt(json.RawMessage(`"crash now"`))
	waitState(t, old, StateCrashed)

	fresh, err := d.Create(context.Background(), HermesACPOpts{Load: old.ID(), Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if fresh == old {
		t.Fatal("resume of a crashed session returned the crashed object")
	}
	if st := old.State(); st != StateDetached {
		t.Fatalf("old crashed session state = %s, want detached (orphaned otherwise)", st)
	}
	if d.Get(old.ID()) != fresh || fresh.State() != StateLive {
		t.Fatalf("registry does not point at the live replacement (state %s)", fresh.State())
	}
}
