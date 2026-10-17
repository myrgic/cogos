// run_exec_hermes_test.go - P5 slice W2: the Hermes-fork run executor.
//
// These drive the real executor over the fake ACP agent. TestMain (this test
// binary doubles as the agent, ACPFAKE_AGENT=1) and the fakeHermesDriver
// helper live in managed_session_hermes_test.go and are reused here. Every
// test asserts real behavior: no t.Skip, no empty bodies.

package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// openHermesRun opens a hermes-fork run on the board through the kernel's own
// open path (OpenKernelRun; the run API's own hermes-fork path is exercised by
// serve_runs_exec_test.go).
func openHermesRun(t *testing.T, st *RunBoardStore, title, brief string) *Run {
	t.Helper()
	r, err := st.OpenKernelRun(RunOpen{
		Title:    title,
		Brief:    brief,
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorHermesFork},
		Origin:   RunOrigin{By: "node-a"},
	}, "node:node-a")
	if err != nil {
		t.Fatalf("open hermes-fork run: %v", err)
	}
	return r
}

// getRun returns the run or fails the test.
func getRun(t *testing.T, st *RunBoardStore, id string) *Run {
	t.Helper()
	r, ok := st.Get(id)
	if !ok {
		t.Fatalf("run %s is not on the board", id)
	}
	return r
}

// runEndOf returns the run's run.end, failing when there is none.
func runEndOf(t *testing.T, st *RunBoardStore, id string) RunEnd {
	t.Helper()
	r := getRun(t, st, id)
	if r.End == nil {
		t.Fatalf("run %s has no run.end (state %s)", id, r.State)
	}
	return *r.End
}

// endEntries counts the folded run.end entries of one run: the board's one
// run.end invariant (I1) is a count over the ledger, not just the fold.
func endEntries(t *testing.T, st *RunBoardStore, id string) int {
	t.Helper()
	st.mu.RLock()
	defer st.mu.RUnlock()
	n := 0
	for _, e := range st.entries {
		if e.itemID() == id && e.logicalKind() == "run.end" {
			n++
		}
	}
	return n
}

// captureHermesSession records the session the executor creates, through the
// driver's afterCreateCall test seam (managed_session_hermes.go). The ring is
// retained after the session is closed, so the test can count its events once
// Start has returned.
func captureHermesSession(t *testing.T) (*HermesACPDriver, func() *HermesACPSession) {
	t.Helper()
	var created *HermesACPSession
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.afterCreateCall = func(p *hermesProcess) {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, s := range p.sessions {
				created = s
			}
		}
	})
	return d, func() *HermesACPSession { return created }
}

// turnEnds returns the turn_end events retained on a session's ring.
func turnEnds(t *testing.T, s *HermesACPSession) []ManagedSessionEvent {
	t.Helper()
	evs, gap := s.Events().Since(0)
	if gap {
		t.Fatal("event ring reported a gap")
	}
	out := []ManagedSessionEvent{}
	for _, ev := range evs {
		if ev.Kind == MSEventTurnEnd {
			out = append(out, ev)
		}
	}
	return out
}

// testRunExecutor builds the executor with short cancel bounds. The grace and
// its retry are policy (runCancelGrace / runCancelRetry in the kernel); a test
// runs them small so a cancel that has to be forced is still bounded by
// seconds, not tens of them.
func testRunExecutor(t *testing.T, board *RunBoardStore, d *HermesACPDriver) *HermesRunExecutor {
	t.Helper()
	e := NewHermesRunExecutor(board, d)
	e.cancelGrace = 500 * time.Millisecond
	e.cancelRetry = 2 * time.Millisecond
	return e
}

// runCtx bounds a run's host loop. A turn that never ends fails the test at
// the deadline instead of hanging the test binary, and a run that ends early
// is unaffected.
func runCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// startRun hosts a run on its own goroutine and returns the channel Start's
// error arrives on. The host loop's context is bounded (runCtx), so a test
// that stops driving the run still sees Start return.
func startRun(t *testing.T, e *HermesRunExecutor, runID string, opts HermesACPOpts) <-chan error {
	t.Helper()
	ctx := runCtx(t)
	out := make(chan error, 1)
	go func() { out <- e.Start(ctx, runID, opts) }()
	return out
}

// turnRunningOnAgent reports whether the session's ring shows the agent
// executing its current turn: an update from the agent after the turn's
// turn_start. It is the evidence that the agent has taken the prompt in hand,
// without which a cancel can race the agent's own registration of the turn.
func turnRunningOnAgent(sess *HermesACPSession) bool {
	evs, _ := sess.Events().Since(0)
	start := uint64(0)
	for _, ev := range evs {
		if ev.Kind == MSEventTurnStart {
			start = ev.Seq
		}
	}
	if start == 0 {
		return false
	}
	for _, ev := range evs {
		if ev.Seq > start && ev.Kind == MSEventSessionUpdate {
			return true
		}
	}
	return false
}

// waitAgentTurn blocks until the executor hosts runID, its session has a turn
// in flight, and the agent has begun that turn. Only then is a cancel safe to
// send: ACP lets an agent drop a session/cancel that arrives for a turn it has
// not started, so a cancel sent before the agent reports the turn is a race
// the test must not rely on. Every wait is deadline-bounded, and a run that
// ends while its turn is starting fails the test at once: that is a run
// closed by something other than its turn.
func waitAgentTurn(t *testing.T, board *RunBoardStore, e *HermesRunExecutor, runID string) *HermesACPSession {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok := board.Get(runID); ok && r.End != nil {
			t.Fatalf("run %s ended %s (%q) while its turn was starting", runID, r.End.Outcome, r.End.Reason)
		}
		e.mu.Lock()
		a := e.active[runID]
		e.mu.Unlock()
		if a != nil && a.sess != nil && a.sess.Info().TurnInFlight != "" && turnRunningOnAgent(a.sess) {
			return a.sess
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("run %s never had a turn running on the agent", runID)
	return nil
}

// TestRunHermesOKWritesOneRunEnd: a turn that completes normally ends the run
// ok, exactly once, and the session it ran in is closed afterwards.
func TestRunHermesOKWritesOneRunEnd(t *testing.T) {
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "answer the brief", "hello from node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeOK || end.ObservedBy != "kernel" {
		t.Fatalf("run.end = outcome %q observed_by %q; want ok/kernel", end.Outcome, end.ObservedBy)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	// The run knows the session that hosted it.
	if r := getRun(t, board, run.ID); r.Executor.Ref == nil || *r.Executor.Ref != sess.ID() {
		t.Fatalf("run executor ref = %v, want %s", r.Executor.Ref, sess.ID())
	}
	ends := turnEnds(t, sess)
	if len(ends) != 1 {
		t.Fatalf("turn_end events = %d, want exactly 1", len(ends))
	}
	if strings.Contains(string(ends[0].Data), `"error"`) {
		t.Fatalf("turn_end = %s; want no error", ends[0].Data)
	}
	// The run ended: its session is closed and no longer registered.
	if sess.State() != StateDetached {
		t.Fatalf("session state = %s, want detached after the run ended", sess.State())
	}
	if d.Get(sess.ID()) != nil {
		t.Fatal("the session is still registered after the run ended")
	}
}

// TestRunHermesCancelKills: cancelling an in-flight turn ends the run killed,
// exactly once.
func TestRunHermesCancelKills(t *testing.T) {
	board := NewRunBoardStore("")
	d := fakeHermesDriver(t, nil)
	e := testRunExecutor(t, board, d)
	run := openHermesRun(t, board, "cancel me", "slow work for node-b")

	started := startRun(t, e, run.ID, HermesACPOpts{Cwd: t.TempDir()})
	waitAgentTurn(t, board, e, run.ID)

	if err := e.Cancel(run.ID, "cancelled by node-b", "node:node-b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeKilled {
		t.Fatalf("run.end outcome = %q, want %q", end.Outcome, RunOutcomeKilled)
	}
	if end.Reason != "cancelled by node-b" {
		t.Fatalf("run.end reason = %q, want the cancel reason", end.Reason)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}

// TestRunHermesCancelSurvivesLostNotification: an agent that ignores
// session/cancel must not wedge the run. The kernel re-sends the cancel and,
// once its grace has elapsed, ends the turn itself, so a cancel always
// produces exactly one killed run.end.
func TestRunHermesCancelSurvivesLostNotification(t *testing.T) {
	board := NewRunBoardStore("")
	d := fakeHermesDriver(t, nil)
	e := testRunExecutor(t, board, d)
	run := openHermesRun(t, board, "agent that ignores cancel", "ignore_cancel work for node-b")

	started := startRun(t, e, run.ID, HermesACPOpts{Cwd: t.TempDir()})
	waitAgentTurn(t, board, e, run.ID)

	// The cancel is the code's own wait to bound: give it the grace plus the
	// time an ended turn needs to fold, and fail the test (not the binary) if
	// it neither returns nor ends the run.
	cancelled := make(chan error, 1)
	go func() { cancelled <- e.Cancel(run.ID, "cancelled by node-b", "node:node-b") }()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("cancel did not return: the agent ignored the cancel and the grace did not end the turn")
	}
	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeKilled {
		t.Fatalf("run.end outcome = %q, want %q (reason %q)", end.Outcome, RunOutcomeKilled, end.Reason)
	}
	if end.Reason != "cancelled by node-b" {
		t.Fatalf("run.end reason = %q, want the cancel reason", end.Reason)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}

// TestRunHermesDroppedSubscriberStillEndsRun: a subscriber the ring drops
// for falling behind (buffer overflow) is not a session that ended. The turn
// runs on, the run stays open, and it ends on the turn's real outcome — not
// crash, which is what a single immediate re-read of the ring would have
// decided while the turn was still in flight.
func TestRunHermesDroppedSubscriberStillEndsRun(t *testing.T) {
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := testRunExecutor(t, board, d)
	// Overflow the subscriber the moment Start has it: 512 events fill its
	// buffer, the next one drops it, before the brief is even sent.
	e.afterSubscribe = func(s *HermesACPSession) {
		for i := 0; i <= runEventBuffer; i++ {
			s.Events().Append(MSEventSessionUpdate, map[string]any{"update": "filler", "n": i})
		}
	}
	run := openHermesRun(t, board, "dropped subscriber", "slow work for node-b")

	started := startRun(t, e, run.ID, HermesACPOpts{Cwd: t.TempDir()})
	waitAgentTurn(t, board, e, run.ID)

	// The session is live and the turn is in flight: the dropped subscriber
	// must not have read as "the session vanished before its turn_end".
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	if d.Get(sess.ID()) != sess || sess.State() != StateLive {
		t.Fatalf("the session did not survive the drop: %v state %v", d.Get(sess.ID()), sess.State())
	}
	if end := getRun(t, board, run.ID).End; end != nil {
		t.Fatalf("run ended %s while its turn was running: a dropped subscriber was read as a gone session", end.Outcome)
	}

	// End the turn for real: the run must fold that turn's outcome.
	if err := e.Cancel(run.ID, "cancelled by node-b", "node:node-b"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}
	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeKilled {
		t.Fatalf("run.end outcome = %q, want %q (reason %q)", end.Outcome, RunOutcomeKilled, end.Reason)
	}
	if end.Reason != "cancelled by node-b" {
		t.Fatalf("run.end reason = %q, want the cancel reason", end.Reason)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
	if sess := session(); sess.State() != StateDetached {
		t.Fatalf("session state = %s, want detached after the run ended", sess.State())
	}
}

// TestRunHermesVanishedSessionEndsRunCrash: the drop fix must not swing the
// other way: a session that is really gone (deleted out from under its run)
// has no turn_end left to report, so the run still ends — crash, exactly
// once, without waiting on an event that can never come. The agent here
// ignores cancels, so the teardown cannot put a turn_end on the ring that
// reads as the turn ending (a session whose agent does acknowledge the
// cancel is the killed case, runEndForTurn's existing mapping).
func TestRunHermesVanishedSessionEndsRunCrash(t *testing.T) {
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := testRunExecutor(t, board, d)
	run := openHermesRun(t, board, "session deleted under it", "ignore_cancel work for node-b")

	started := startRun(t, e, run.ID, HermesACPOpts{Cwd: t.TempDir()})
	sess := waitAgentTurn(t, board, e, run.ID)
	if got := session(); got != sess {
		t.Fatalf("captured session = %p, want the run's %p", got, sess)
	}

	// The session goes away with the turn still open (a client deleted it, or
	// its driver let it go): the kernel can no longer observe a turn_end for
	// the run, and the wait for one must not run on.
	if err := d.Delete(sess.ID()); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("start: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("start did not return: a session that is gone left the run's wait running")
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeCrash {
		t.Fatalf("run.end outcome = %q, want %q (reason %q)", end.Outcome, RunOutcomeCrash, end.Reason)
	}
	if end.Reason == "" {
		t.Fatal("a crash run.end carries no reason")
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}

// TestRunHermesAgentExitMidTurnCrashes: an agent process that exits during the
// turn ends the run crash, exactly once, with one turn_end carrying the
// driver's "agent process exited" error.
func TestRunHermesAgentExitMidTurnCrashes(t *testing.T) {
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "crash mid-turn", "crash now, node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeCrash {
		t.Fatalf("run.end outcome = %q, want %q (reason %q)", end.Outcome, RunOutcomeCrash, end.Reason)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	ends := turnEnds(t, sess)
	if len(ends) != 1 {
		t.Fatalf("turn_end events = %d, want exactly 1", len(ends))
	}
	if !strings.Contains(string(ends[0].Data), "agent process exited") {
		t.Fatalf("turn_end = %s; want error \"agent process exited\"", ends[0].Data)
	}
}

// TestRunHermesKernelRestartLost: a hermes-fork run that still carries a live
// record when the kernel restarts has no driver afterwards; the boot sweep
// closes it lost, exactly once, and a second sweep closes nothing.
func TestRunHermesKernelRestartLost(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()

	board := NewRunBoardStore(root)
	run := openHermesRun(t, board, "left running", "work for node-a")
	if _, err := board.SetLive(run.ID, &RunLive{PID: 4242, PIDStarted: "2026-10-16T00:00:00Z", TurnID: "t1"}, "sess-node-a-1", "node:kernel"); err != nil {
		t.Fatalf("record live: %v", err)
	}
	if r := getRun(t, board, run.ID); r.Live == nil || r.State != RunStateRunning {
		t.Fatalf("run after the live record = state %q live %+v; want running", r.State, r.Live)
	}

	// Simulated kernel restart: the board is rebuilt from the ledger and the
	// previous kernel's sessions are gone with it.
	resetLedgerCacheForTest()
	restarted, err := RebuildRunBoardStoreFromLedger(root)
	if err != nil {
		t.Fatalf("rebuild from ledger: %v", err)
	}
	if r := getRun(t, restarted, run.ID); r.End != nil {
		t.Fatalf("the rebuild closed a run that still had a live record: %+v", r.End)
	}

	e := NewHermesRunExecutor(restarted, fakeHermesDriver(t, nil))
	closed, err := e.BootSweep(context.Background())
	if err != nil {
		t.Fatalf("boot sweep: %v", err)
	}
	if len(closed) != 1 || closed[0] != run.ID {
		t.Fatalf("boot sweep closed %v, want [%s]", closed, run.ID)
	}
	end := runEndOf(t, restarted, run.ID)
	if end.Outcome != RunOutcomeLost || end.ObservedBy != "kernel" {
		t.Fatalf("boot sweep run.end = outcome %q observed_by %q; want lost/kernel", end.Outcome, end.ObservedBy)
	}
	if !strings.Contains(end.Reason, "kernel restarted with no live driver") {
		t.Fatalf("boot sweep reason = %q", end.Reason)
	}
	if n := endEntries(t, restarted, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}

	// Idempotent: the run has its run.end, so a second sweep closes nothing.
	again, err := e.BootSweep(context.Background())
	if err != nil {
		t.Fatalf("second boot sweep: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second boot sweep closed %v, want none", again)
	}
	if n := endEntries(t, restarted, run.ID); n != 1 {
		t.Fatalf("run.end entries after the second sweep = %d, want 1", n)
	}
}

// TestRunHermesSecondEndRefused: the executor's run.end is the run's only one.
// A second end attempt is a conflict (I1) and Start refuses an ended run.
func TestRunHermesSecondEndRefused(t *testing.T) {
	board := NewRunBoardStore("")
	d := fakeHermesDriver(t, nil)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "one end only", "hello node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}

	if _, err := board.End(run.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "node:kernel"); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("second run.end = %v, want a conflict", err)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries after the refused second end = %d, want 1", n)
	}
	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Cwd: t.TempDir()}); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("start of an ended run = %v, want a conflict", err)
	}
}

// TestRunHermesReconcileClosesVanishedSession: a run whose session is gone
// from the driver, with no run.end, is closed lost by the reconcile backstop,
// exactly once; the next tick plans nothing.
func TestRunHermesReconcileClosesVanishedSession(t *testing.T) {
	board := NewRunBoardStore("")
	d := fakeHermesDriver(t, nil)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "vanished session", "work for node-b")

	sess, err := d.Create(context.Background(), HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := board.SetLive(run.ID, &RunLive{PID: 4242, PIDStarted: "2026-10-16T00:00:00Z"}, sess.ID(), "node:kernel"); err != nil {
		t.Fatalf("record live: %v", err)
	}
	// The session goes away and the run stays open: exactly the case the
	// 30 s tick is the backstop for.
	if err := d.Delete(sess.ID()); err != nil {
		t.Fatalf("delete session: %v", err)
	}

	cfg, err := e.LoadConfig("")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	live, err := e.FetchLive(context.Background(), cfg)
	if err != nil {
		t.Fatalf("fetch live: %v", err)
	}
	plan, err := e.ComputePlan(cfg, live, nil)
	if err != nil {
		t.Fatalf("compute plan: %v", err)
	}
	if len(plan.Actions) != 1 || plan.Actions[0].Name != run.ID {
		t.Fatalf("plan actions = %+v, want one closing %s", plan.Actions, run.ID)
	}
	if got := plan.Actions[0].Details[runPlanOutcome]; got != RunOutcomeLost {
		t.Fatalf("plan outcome = %v, want %q", got, RunOutcomeLost)
	}
	results, err := e.ApplyPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("apply plan: %v", err)
	}
	if len(results) != 1 || results[0].Status != reconcile.ApplySucceeded {
		t.Fatalf("apply results = %+v, want one succeeded", results)
	}
	if end := runEndOf(t, board, run.ID); end.Outcome != RunOutcomeLost || end.ObservedBy != "kernel" {
		t.Fatalf("reconcile run.end = outcome %q observed_by %q; want lost/kernel", end.Outcome, end.ObservedBy)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}

	// The next tick is a no-op: the run has its run.end.
	live2, err := e.FetchLive(context.Background(), cfg)
	if err != nil {
		t.Fatalf("fetch live again: %v", err)
	}
	plan2, err := e.ComputePlan(cfg, live2, nil)
	if err != nil {
		t.Fatalf("compute plan again: %v", err)
	}
	if len(plan2.Actions) != 0 {
		t.Fatalf("second plan actions = %+v, want none", plan2.Actions)
	}

	state, err := e.BuildState(cfg, live2, nil)
	if err != nil {
		t.Fatalf("build state: %v", err)
	}
	if len(state.Resources) != 1 || state.Resources[0].ExternalID != run.ID {
		t.Fatalf("state resources = %+v, want the one run", state.Resources)
	}
	if st := e.Health(); !strings.Contains(st.Message, "runs=1") || st.Health != reconcile.HealthHealthy {
		t.Fatalf("health = %+v, want one run reported healthy", st)
	}
}
