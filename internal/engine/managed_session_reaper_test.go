package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// reapDriver is fakeHermesDriver with durable records (a reaped session is
// resumed from its record) and the argv profile of every process it starts.
func reapDriver(t *testing.T, mut func(*HermesACPDriverConfig)) (*HermesACPDriver, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var profiles []string
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.RecordDir = t.TempDir()
		c.Command = func(profile string) []string {
			mu.Lock()
			profiles = append(profiles, profile)
			mu.Unlock()
			return []string{os.Args[0], "-test.run=^$"}
		}
		if mut != nil {
			mut(c)
		}
	})
	return d, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), profiles...)
	}
}

// agentText is the concatenated agent text of the ring's session updates.
func agentText(s *HermesACPSession) string {
	evs, _ := s.Events().Since(0)
	var b strings.Builder
	for _, ev := range evs {
		if ev.Kind == MSEventSessionUpdate {
			b.WriteString(agentMessageText(ev.Data))
		}
	}
	return b.String()
}

func promptAndWait(t *testing.T, s *HermesACPSession, text string) {
	t.Helper()
	since := s.Events().LastSeq()
	turn, err := s.Prompt(json.RawMessage(`"` + text + `"`))
	if err != nil {
		t.Fatalf("prompt %q: %v", text, err)
	}
	waitEvent(t, s, since, func(ev ManagedSessionEvent) bool {
		return ev.Kind == MSEventTurnEnd && strings.Contains(string(ev.Data), `"`+turn+`"`)
	})
}

func TestReap_IdleSessionIsDetachedKeepsItsRecordAndResumesWithItsSettings(t *testing.T) {
	d, profiles := reapDriver(t, nil)
	ctx := context.Background()
	s, err := d.Create(ctx, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	if err := s.SetModel(ctx, "model-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMode(ctx, "dont_ask"); err != nil {
		t.Fatal(err)
	}
	promptAndWait(t, s, "hello")

	// Inside idle_timeout (default 1 h): kept.
	if got := d.Reap(time.Now().Add(30 * time.Minute)); len(got) != 0 {
		t.Fatalf("reaped at 30 min idle: %+v", got)
	}
	// Past it: detached, out of the registry, process stopped, record kept.
	got := d.Reap(time.Now().Add(2 * time.Hour))
	if len(got) != 1 || got[0].SessionID != id || got[0].Reason != ReapReasonIdle || got[0].IdleSecs < 3600 {
		t.Fatalf("reap = %+v", got)
	}
	if d.Get(id) != nil {
		t.Fatal("reaped session still registered")
	}
	if st := s.State(); st != StateDetached {
		t.Fatalf("state after reap = %s", st)
	}
	select {
	case <-s.proc.currentConn().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("agent process still running after its only session was reaped")
	}
	evs, _ := s.Events().Since(0)
	last := evs[len(evs)-1]
	if last.Kind != MSEventState || !strings.Contains(string(last.Data), `"detached"`) || !strings.Contains(string(last.Data), "reaped: idle") {
		t.Fatalf("last event = %s %s", last.Kind, last.Data)
	}
	if _, err := s.Prompt(json.RawMessage(`"late"`)); !errors.Is(err, ErrManagedSessionNotFound) {
		t.Fatalf("prompt on a reaped session = %v, want not found (the client resumes)", err)
	}
	rec, err := d.loadRecord(id)
	if err != nil || rec.Profile != "cog" || rec.Mode != "dont_ask" || rec.Model != "model-a" {
		t.Fatalf("record after reap = %+v, %v", rec, err)
	}
	total, recent := d.reaps.snapshot()
	if total != 1 || len(recent) != 1 || recent[0].SessionID != id {
		t.Fatalf("reap log = %d %+v", total, recent)
	}

	// Resume with nothing but the id (what the stage's reattach sends): a new
	// process under the recorded profile, the conversation loaded, and the
	// agent back on the mode and model the session had.
	r, err := d.Create(ctx, HermesACPOpts{Load: id})
	if err != nil {
		t.Fatal(err)
	}
	if r == s || r.State() != StateLive {
		t.Fatalf("resume returned %p state %s", r, r.State())
	}
	if p := profiles(); len(p) != 2 || p[1] != "cog" {
		t.Fatalf("process profiles = %q, want the recorded profile on resume", p)
	}
	if info := r.Info(); info.Profile != "cog" || info.Mode != "dont_ask" || info.Model != "model-a" {
		t.Fatalf("resumed info = %+v", info)
	}
	promptAndWait(t, r, "settings")
	if txt := agentText(r); !strings.Contains(txt, "replayed history for "+id) || !strings.Contains(txt, "settings:mode=dont_ask:model=model-a") {
		t.Fatalf("resumed agent text = %q", txt)
	}
}

func TestReap_ABusySessionIsNeverReaped(t *testing.T) {
	d, _ := reapDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir(), Owner: "test", LeaseTTLSecs: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow running"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventSessionUpdate)) // the fake registered the turn (a cancel before that is dropped)
	// Idle past the timeout AND its lease long expired: a running turn keeps it.
	if got := d.Reap(time.Now().Add(3 * time.Hour)); len(got) != 0 {
		t.Fatalf("reaped a session with a turn in flight: %+v", got)
	}
	if info := s.Info(); !info.TurnRunning || d.Get(s.ID()) != s {
		t.Fatalf("info = %+v", info)
	}
	since := s.Events().LastSeq()
	if err := s.Cancel(); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, since, kind(MSEventTurnEnd))
	// The first tick after the turn ends takes it.
	if got := d.Reap(time.Now().Add(3 * time.Hour)); len(got) != 1 {
		t.Fatalf("not reaped once idle: %+v", got)
	}
}

func TestReap_APermissionParkedSessionIsNeverReaped(t *testing.T) {
	d, _ := reapDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"permission please"`)); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, s, 0, kind(MSEventPermissionRequest))
	var req struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(ev.Data, &req)
	if got := d.Reap(time.Now().Add(3 * time.Hour)); len(got) != 0 {
		t.Fatalf("reaped a session waiting on a person: %+v", got)
	}
	if len(s.Info().PendingPermissions) != 1 {
		t.Fatalf("permission lost: %+v", s.Info())
	}
	since := s.Events().LastSeq()
	if err := s.ResolvePermission(req.RequestID, "allow"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, since, kind(MSEventTurnEnd))
	if got := d.Reap(time.Now().Add(3 * time.Hour)); len(got) != 1 {
		t.Fatalf("not reaped once answered and idle: %+v", got)
	}
}

func TestReap_LeaseExpiryRenewalAndTheIdleBound(t *testing.T) {
	d, _ := reapDriver(t, nil)
	ctx := context.Background()
	s, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir(), Owner: "stage:room:r1", LeaseTTLSecs: 60})
	if err != nil {
		t.Fatal(err)
	}
	info := s.Info()
	if info.Owner != "stage:room:r1" || info.LeaseTTLSecs != 60 || info.LeaseExpiresAt == nil {
		t.Fatalf("lease not recorded: %+v", info)
	}
	if got := d.Reap(time.Now().Add(30 * time.Second)); len(got) != 0 {
		t.Fatalf("reaped inside its lease: %+v", got)
	}

	// An explicit renewal moves the lease, and is not activity.
	lastBefore := s.Info().LastActivityAt
	if err := s.RenewLease("", 10*time.Minute, false, false); err != nil {
		t.Fatal(err)
	}
	info = s.Info()
	if !info.LastActivityAt.Equal(lastBefore) || info.LeaseTTLSecs != 600 {
		t.Fatalf("renewal touched the idle clock or kept the old ttl: %+v (before %v)", info, lastBefore)
	}
	if got := d.Reap(time.Now().Add(5 * time.Minute)); len(got) != 0 {
		t.Fatalf("reaped inside a renewed lease: %+v", got)
	}

	// A write renews it at its own length.
	exp := *s.Info().LeaseExpiresAt
	time.Sleep(20 * time.Millisecond)
	if err := s.SetMode(ctx, "accept_edits"); err != nil {
		t.Fatal(err)
	}
	if !s.Info().LeaseExpiresAt.After(exp) {
		t.Fatal("a write on the session did not renew its lease")
	}

	// Expired and unrenewed: reaped, even though it is nowhere near idle_timeout.
	got := d.Reap(time.Now().Add(11 * time.Minute))
	if len(got) != 1 || got[0].Reason != ReapReasonLeaseExpired || got[0].Owner != "stage:room:r1" {
		t.Fatalf("reap = %+v", got)
	}
	if err := s.RenewLease("", 0, false, false); !errors.Is(err, ErrManagedSessionNotFound) {
		t.Fatalf("renewing a reaped session = %v, want not found", err)
	}

	// A held, valid lease never stretches a session past idle_timeout.
	s2, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir(), Owner: "o", LeaseTTLSecs: 48 * 3600})
	if err != nil {
		t.Fatal(err)
	}
	got = d.Reap(time.Now().Add(2 * time.Hour))
	if len(got) != 1 || got[0].SessionID != s2.ID() || got[0].Reason != ReapReasonIdle {
		t.Fatalf("idle session with a long lease: reap = %+v", got)
	}
}

func TestReap_AttachIsActivityAndAResumeOfAResidentSessionSetsTheLease(t *testing.T) {
	d, _ := reapDriver(t, func(c *HermesACPDriverConfig) { c.IdleTimeout = time.Minute })
	ctx := context.Background()
	s, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	before := s.Info().LastActivityAt
	time.Sleep(20 * time.Millisecond)
	same, err := d.Create(ctx, HermesACPOpts{Load: s.ID(), Owner: "stage:room:r2", LeaseTTLSecs: 300})
	if err != nil || same != s {
		t.Fatalf("resume of a live session = %p, %v", same, err)
	}
	info := s.Info()
	if !info.LastActivityAt.After(before) || info.Owner != "stage:room:r2" || info.LeaseTTLSecs != 300 {
		t.Fatalf("after resume: %+v", info)
	}
	before = info.LastActivityAt
	time.Sleep(20 * time.Millisecond)
	s.touch() // a new event subscription
	if !s.Info().LastActivityAt.After(before) {
		t.Fatal("attach did not reset the idle clock")
	}
}

func TestReap_ParentAndForkShareAProcessUntilBothAreGone(t *testing.T) {
	d, _ := reapDriver(t, nil)
	ctx := context.Background()
	front, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := d.Create(ctx, HermesACPOpts{ForkOf: front.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fork.Prompt(json.RawMessage(`"slow work"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, fork, 0, kind(MSEventSessionUpdate)) // the fake registered the turn
	got := d.Reap(time.Now().Add(2 * time.Hour))
	if len(got) != 1 || got[0].SessionID != front.ID() {
		t.Fatalf("reap = %+v, want only the idle parent", got)
	}
	select {
	case <-front.proc.currentConn().Done():
		t.Fatal("the shared process stopped while the fork still runs in it")
	default:
	}
	since := fork.Events().LastSeq()
	if err := fork.Cancel(); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, fork, since, kind(MSEventTurnEnd))
	if got := d.Reap(time.Now().Add(2 * time.Hour)); len(got) != 1 || got[0].SessionID != fork.ID() {
		t.Fatalf("second reap = %+v", got)
	}
	select {
	case <-fork.proc.currentConn().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("process kept after its last session was reaped")
	}
}

func TestReap_IdleTimeoutOffStillHonoursLeases(t *testing.T) {
	d, _ := reapDriver(t, func(c *HermesACPDriverConfig) { c.IdleTimeout = -1 })
	ctx := context.Background()
	plain, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir(), LeaseTTLSecs: 60})
	if err != nil {
		t.Fatal(err)
	}
	got := d.Reap(time.Now().Add(72 * time.Hour))
	if len(got) != 1 || got[0].SessionID != leased.ID() {
		t.Fatalf("reap with idle reaping off = %+v", got)
	}
	if d.Get(plain.ID()) == nil {
		t.Fatal("idle reaping is off, yet an unleased session was reaped")
	}
}

func TestReap_NegativeLeaseIsRefused(t *testing.T) {
	d, _ := reapDriver(t, nil)
	if _, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir(), LeaseTTLSecs: -5}); !errors.Is(err, ErrInvalidManagedSession) {
		t.Fatalf("negative lease = %v", err)
	}
}

func TestRunReaper_TicksUntilCancelled(t *testing.T) {
	d, _ := reapDriver(t, func(c *HermesACPDriverConfig) {
		c.ReapInterval = 20 * time.Millisecond
	})
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir(), LeaseTTLSecs: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.RunReaper(ctx); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for d.Get(s.ID()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("the reaper loop never reaped the expired lease")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunReaper did not stop on cancel")
	}
}

func TestDetach_StopsTheProcessKeepsTheRecordAndResumes(t *testing.T) {
	d, profiles := reapDriver(t, nil)
	ctx := context.Background()
	s, err := d.Create(ctx, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetModel(ctx, "model-b"); err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	if err := d.DetachAuthorized(id, func(string) bool { return false }); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("detach without class authority = %v", err)
	}
	if err := d.DetachAuthorized(id, nil); err != nil {
		t.Fatal(err)
	}
	if d.Get(id) != nil || s.State() != StateDetached {
		t.Fatalf("after detach: registered=%v state=%s", d.Get(id) != nil, s.State())
	}
	select {
	case <-s.proc.currentConn().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("process kept after detach")
	}
	if _, err := s.Prompt(json.RawMessage(`"x"`)); !errors.Is(err, ErrManagedSessionNotFound) {
		t.Fatalf("prompt after detach = %v", err)
	}
	if rec, err := d.loadRecord(id); err != nil || rec.Model != "model-b" || rec.Profile != "cog" {
		t.Fatalf("record after detach = %+v %v", rec, err)
	}
	if err := d.DetachAuthorized(id, nil); err != nil { // idempotent
		t.Fatal(err)
	}
	r, err := d.Create(ctx, HermesACPOpts{Load: id})
	if err != nil {
		t.Fatal(err)
	}
	promptAndWait(t, r, "settings")
	if !strings.Contains(agentText(r), "model=model-b") || profiles()[len(profiles())-1] != "cog" {
		t.Fatalf("resumed after detach: %q profiles %q", agentText(r), profiles())
	}
}

func TestRenewLease_ActiveIsActivityAPlainRenewalIsNot(t *testing.T) {
	d, _ := reapDriver(t, nil)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir(), Owner: "o", LeaseTTLSecs: 600})
	if err != nil {
		t.Fatal(err)
	}
	before := s.Info().LastActivityAt
	time.Sleep(20 * time.Millisecond)
	if err := s.RenewLease("", 0, false, false); err != nil {
		t.Fatal(err)
	}
	if !s.Info().LastActivityAt.Equal(before) {
		t.Fatal("a plain renewal moved the idle clock")
	}
	if err := s.RenewLease("", 0, false, true); err != nil {
		t.Fatal(err)
	}
	if !s.Info().LastActivityAt.After(before) {
		t.Fatal("an active renewal did not move the idle clock")
	}
}
