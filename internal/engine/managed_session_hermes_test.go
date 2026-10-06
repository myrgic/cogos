package engine

import (
	"context"
	"encoding/json"
	"os"
	"strings"
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
