package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
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

// A crash-restart's session/load rebuilds the session with the agent's
// defaults; the client's chosen model and mode must be put back, or a work
// thread pinned to one model silently continues on another.
func TestHermesACPDriver_SetModelSurvivesCrashRestart(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.CrashWindow = time.Nanosecond })
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetModel(context.Background(), "custom:cogos:sentinel-model-777"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMode(context.Background(), "accept_edits"); err != nil {
		t.Fatal(err)
	}
	if got := s.Info().Model; got != "custom:cogos:sentinel-model-777" {
		t.Fatalf("info model = %q", got)
	}
	if err := s.SetModel(context.Background(), ""); err == nil {
		t.Fatal("empty model id should be refused by the agent")
	}
	if got := s.Info().Model; got != "custom:cogos:sentinel-model-777" {
		t.Fatalf("a refused set_model must not change the recorded model: %q", got)
	}

	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	ev := waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "replayed history for "+s.ID())
	})
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventState && strings.Contains(string(ev.Data), `"live"`)
	})
	if _, err := s.Prompt(json.RawMessage(`"settings?"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate &&
			strings.Contains(string(ev.Data), "settings:mode=accept_edits:model=custom:cogos:sentinel-model-777")
	})
}

// An agent without session/set_model (opencode's ACP agent, runtime class
// "opencode") changes models through session/set_config_option. SetModel falls
// back to it on method-not-found, records the model, and a crash-restart puts
// it back the same way.
func TestHermesACPDriver_SetModelFallsBackToConfigOption(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.CrashWindow = time.Nanosecond })
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	const model = acpfake.ConfigOnlyPrefix + "openrouter/deepseek/deepseek-v4.1-flash-1017"
	if err := s.SetModel(context.Background(), model); err != nil {
		t.Fatalf("set_model with config-option fallback: %v", err)
	}
	if got := s.Info().Model; got != model {
		t.Fatalf("info model = %q", got)
	}
	if _, err := s.Prompt(json.RawMessage(`"modelinfo"`)); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "modelinfo:model="+model+":")
	})
	// A refusal that is not method-not-found is not retried as a config option.
	if err := s.SetModel(context.Background(), "reject-me"); err == nil {
		t.Fatal("a refused set_model must stay refused")
	}
	if got := s.Info().Model; got != model {
		t.Fatalf("a refused set_model must not change the recorded model: %q", got)
	}

	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	ev = waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "replayed history for "+s.ID())
	})
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventState && strings.Contains(string(ev.Data), `"live"`)
	})
	if _, err := s.Prompt(json.RawMessage(`"settings?"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, ev.Seq, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), ":model="+model)
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

// Review #666 round 2: the old generation's watch() goroutine ends its
// restart with setState(Live) for each session it re-loaded. If the NEW
// process crashes inside CrashWindow first, its own watcher has already
// decided Crashed (terminal, no retry); a stale Live landing after that
// would leave the session "live" over a dead connection forever.
func TestHermesACPDriver_StaleRestartCannotOverwriteNewCrash(t *testing.T) {
	loaded, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.CrashWindow = 400 * time.Millisecond
		c.RestartBackoff = time.Millisecond
		c.afterRestartLoad = func(*HermesACPSession) {
			once.Do(func() { close(loaded) })
			<-release // old generation parked between its session/load and its state write
		}
	})
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // outlive CrashWindow so the first crash is "late" and restarts
	_, _ = s.Prompt(json.RawMessage(`"crash now"`))
	<-loaded

	// The new process dies at once: inside CrashWindow, so its watcher marks Crashed.
	s.proc.mu.Lock()
	cmd := s.proc.cmd
	s.proc.mu.Unlock()
	_ = cmd.Process.Kill()
	waitState(t, s, StateCrashed)

	close(release) // the stale old-generation goroutine now tries setState(Live)
	time.Sleep(300 * time.Millisecond)
	if st := s.State(); st != StateCrashed {
		t.Fatalf("stale restart overwrote the new generation's verdict: state %s, want crashed", st)
	}
}

func TestHermesACPDriver_CreateRejectsForkWithLoad(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	a, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Create(context.Background(), HermesACPOpts{ForkOf: a.ID(), Load: "other"})
	if !errors.Is(err, ErrInvalidManagedSession) {
		t.Fatalf("fork_of+load returned %v, want ErrInvalidManagedSession", err)
	}
	if n := len(d.List()); n != 1 {
		t.Fatalf("a rejected create registered a session (%d sessions)", n)
	}
}

// Sibling of review #666 round 2: Create's own Live write is subject to the
// same stale-write hazard. If the process dies after session/new returned but
// before registration, Create must fail, not register a "live" session over a
// dead connection.
func TestHermesACPDriver_CreateOverDeadProcessFails(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.afterCreateCall = func(p *hermesProcess) {
			p.mu.Lock()
			cmd, conn := p.cmd, p.conn
			p.mu.Unlock()
			_ = cmd.Process.Kill()
			<-conn.Done()
		}
	})
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err == nil {
		t.Fatalf("Create over a dead process returned a session in state %s", s.State())
	}
	if n := len(d.List()); n != 0 {
		t.Fatalf("a failed create left %d session(s) registered", n)
	}
}

func TestHermesACPSession_RedirectFoldsIntoRunningTurn(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Prompt(json.RawMessage(`"redir start"`))
	if err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "working")
	})
	got, err := s.Redirect(json.RawMessage(`"actually do Y"`))
	if err != nil {
		t.Fatalf("redirect: %v", err)
	}
	if got != turn {
		t.Fatalf("redirect joined %q, want the running turn %q", got, turn)
	}
	// The running turn finishes by itself, carrying the redirect text: no cancel was sent.
	end := waitEvent(t, s, 0, kind(MSEventTurnEnd))
	if !strings.Contains(string(end.Data), `"end_turn"`) || !strings.Contains(string(end.Data), turn) {
		t.Fatalf("turn_end = %s", end.Data)
	}
	fwd := waitEvent(t, s, 0, kind(MSEventPromptForwarded))
	if !strings.Contains(string(fwd.Data), `"confirmed":true`) {
		t.Fatalf("a real fold-in must be confirmed: %s", fwd.Data)
	}
	if !strings.Contains(string(fwd.Data), "actually do Y") || !strings.Contains(string(fwd.Data), turn) {
		t.Fatalf("prompt_forwarded = %s", fwd.Data)
	}
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "continued:actually do Y")
	})
	// Exactly one turn_start and one turn_end: the forward opened no second turn.
	starts, ends := 0, 0
	for _, ev := range s.Events().Subscribe(0, 512).Replay {
		switch ev.Kind {
		case MSEventTurnStart:
			starts++
		case MSEventTurnEnd:
			ends++
		}
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("turn_start=%d turn_end=%d, want 1 and 1", starts, ends)
	}
	if s.Info().TurnInFlight != "" {
		t.Fatalf("turn still in flight: %q", s.Info().TurnInFlight)
	}
}

func TestHermesACPSession_RedirectRefusals(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Redirect(json.RawMessage(`"idle"`)); err != ErrNoTurnInFlight {
		t.Fatalf("idle redirect: %v, want ErrNoTurnInFlight", err)
	}
	if _, err := s.Prompt(json.RawMessage(`"redir start"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	img := json.RawMessage(`[{"type":"text","text":"x"},{"type":"image","data":"AA==","mimeType":"image/png"}]`)
	if _, err := s.Redirect(img); err != ErrTurnInFlight {
		t.Fatalf("rich-media redirect: %v, want ErrTurnInFlight", err)
	}
	if err := s.Cancel(); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnEnd))
}

// An agent that does NOT fold the prompt in starts a second turn. If the first
// turn has ended by the time the grace passes, the kernel must adopt the
// second as the session's turn: a running turn is never left untracked.
func TestHermesACPSession_RedirectIntoNonRedirectingAgentIsAdopted(t *testing.T) {
	old := forwardGrace
	forwardGrace = 300 * time.Millisecond
	defer func() { forwardGrace = old }()
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"sleep150 first"`)); err != nil { // ends before the grace
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	// The fake treats "sleep" prompts as independent turns, so this one runs now,
	// for 1.2 s, and outlives both the first turn and the grace.
	if _, err := s.Redirect(json.RawMessage(`"sleep1200 second"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnEnd)) // the first turn's end
	time.Sleep(forwardGrace + 150*time.Millisecond)
	if s.Info().TurnInFlight == "" {
		t.Fatalf("second turn is running but the session tracks none: %+v", s.Info())
	}
	starts := 0
	for _, ev := range s.Events().Subscribe(0, 512).Replay {
		if ev.Kind == MSEventTurnStart {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("turn_start events = %d, want 2 (the adopted turn is announced)", starts)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Info().TurnInFlight != "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if s.Info().TurnInFlight != "" {
		t.Fatalf("adopted turn never closed: %+v", s.Info())
	}
	ends := 0
	for _, ev := range s.Events().Subscribe(0, 512).Replay {
		if ev.Kind == MSEventTurnEnd {
			ends++
		}
	}
	if ends != 2 {
		t.Fatalf("turn_end events = %d, want 2", ends)
	}
}

// cog-review (PR #712): a normal Prompt() must not take the turn slot while a forward is unresolved,
// or the forward runs on as an untracked second turn.
func TestHermesACPSession_PromptRefusedWhileForwardUnresolved(t *testing.T) {
	old := forwardGrace
	forwardGrace = 400 * time.Millisecond
	defer func() { forwardGrace = old }()
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"sleep100 first"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	if _, err := s.Redirect(json.RawMessage(`"sleep1500 second"`)); err != nil { // a non-redirecting agent: runs as its own turn
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnEnd)) // the first turn is over; the grace window is still open
	if _, err := s.Prompt(json.RawMessage(`"B"`)); err != ErrTurnInFlight {
		t.Fatalf("Prompt inside the grace window = %v, want ErrTurnInFlight (the forward may still be a turn)", err)
	}
	time.Sleep(forwardGrace + 200*time.Millisecond) // past the grace: the forward is adopted
	if s.Info().TurnInFlight == "" {
		t.Fatalf("forward not adopted after the grace: %+v", s.Info())
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Info().TurnInFlight != "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := s.Prompt(json.RawMessage(`"C"`)); err != nil {
		t.Fatalf("Prompt after the adopted turn ended: %v", err)
	}
}

// cog-review (PR #712, round 2): an agent that does not redirect but answers the forwarded prompt as a quick
// turn of its own must not be reported as a fold-in; the turn that ran has to be visible.
func TestHermesACPSession_FastNonRedirectingAnswerIsNotAFoldIn(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow job"`)); err != nil { // runs until cancelled; does not redirect
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	running := s.Info().TurnInFlight
	if _, err := s.Redirect(json.RawMessage(`"quick question"`)); err != nil { // the fake answers: echo, end_turn, at once
		t.Fatal(err)
	}
	waitEvent(t, s, 0, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "echo:quick question")
	})
	deadline := time.Now().Add(3 * time.Second)
	var starts []ManagedSessionEvent
	for time.Now().Before(deadline) && len(starts) < 2 {
		starts = starts[:0]
		for _, ev := range s.Events().Subscribe(0, 512).Replay {
			if ev.Kind == MSEventTurnStart {
				starts = append(starts, ev)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, ev := range s.Events().Subscribe(0, 512).Replay {
		if ev.Kind == MSEventPromptForwarded && strings.Contains(string(ev.Data), `"confirmed":true`) {
			t.Fatalf("a quick non-redirecting answer was reported as a confirmed fold-in: %s", ev.Data)
		}
	}
	if len(starts) != 2 {
		t.Fatalf("turn_start events = %d, want 2: the turn that actually ran must be announced", len(starts))
	}
	if got := s.Info().TurnInFlight; got != running {
		t.Fatalf("the original turn must still be the tracked one: have %q want %q", got, running)
	}
	if err := s.Cancel(); err != nil {
		t.Fatal(err)
	}
}

// Only one forward may be unresolved at a time.
func TestHermesACPSession_SecondForwardWhileOnePending(t *testing.T) {
	old := forwardGrace
	forwardGrace = 5 * time.Second
	defer func() { forwardGrace = old }()
	d := fakeHermesDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow job"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate))
	if _, err := s.Redirect(json.RawMessage(`"sleep800 first forward"`)); err != nil { // pending for 800 ms
		t.Fatal(err)
	}
	if _, err := s.Redirect(json.RawMessage(`"second forward"`)); err != ErrTurnInFlight {
		t.Fatalf("second forward while one is pending = %v, want ErrTurnInFlight", err)
	}
	if err := s.Cancel(); err != nil {
		t.Fatal(err)
	}
	// Let the first forward resolve before the test returns: its goroutine must not outlive the test.
	waitForwardsResolved(t, s)
}

// waitForwardsResolved blocks until no forwarded prompt is unresolved.
func waitForwardsResolved(t *testing.T, s *HermesACPSession) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := s.fwdPending
		s.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a forwarded prompt never resolved")
}
