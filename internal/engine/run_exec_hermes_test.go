// run_exec_hermes_test.go - P5 slice W2: the Hermes-fork run executor.
//
// These drive the real executor over the fake ACP agent. TestMain (this test
// binary doubles as the agent, ACPFAKE_AGENT=1) and the fakeHermesDriver
// helper live in managed_session_hermes_test.go and are reused here. Every
// test asserts real behavior: no t.Skip, no empty bodies.

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

// openHermesRunWithModel is openHermesRun plus the run's model (RunOpen.Model,
// run_board.go): the executor must apply it to the session before the prompt.
func openHermesRunWithModel(t *testing.T, st *RunBoardStore, title, brief, model string) *Run {
	t.Helper()
	r, err := st.OpenKernelRun(RunOpen{
		Title:    title,
		Brief:    brief,
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorHermesFork},
		Model:    model,
		Origin:   RunOrigin{By: "node-a"},
	}, "node:node-a")
	if err != nil {
		t.Fatalf("open hermes-fork run: %v", err)
	}
	return r
}

// rootedRunBoard returns a run board over a real workspace root, so the
// kernel's per-run artifacts under <root>/.cog/run/ are actually written. The
// ledger's process-global last-event cache is keyed by bucket, not root, so it
// is cleared around the test the way run_board_restart_test.go does.
func rootedRunBoard(t *testing.T) (*RunBoardStore, string) {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()
	return NewRunBoardStore(root), root
}

// reportPathFor is where the kernel writes a run's report: the per-run state
// directory (.cog/run) its other artifacts live under (turns/, bus/, ...).
func reportPathFor(root, runID string) string {
	return filepath.Join(root, ".cog", "run", "runs", runID, "report.md")
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

// ringAgentText joins every agent_message_chunk text retained on the
// session's ring, in seq order: the same ACP session/update events the run's
// report is built from, read test-side. It is the evidence of what the agent
// said, without depending on the report path at all.
func ringAgentText(t *testing.T, s *HermesACPSession) string {
	t.Helper()
	evs, gap := s.Events().Since(0)
	if gap {
		t.Fatal("event ring reported a gap")
	}
	var b strings.Builder
	for _, ev := range evs {
		if ev.Kind != MSEventSessionUpdate {
			continue
		}
		var u struct {
			Kind    string `json:"sessionUpdate"`
			Content struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal(ev.Data, &u) != nil || u.Kind != "agent_message_chunk" {
			continue
		}
		b.WriteString(u.Content.Text)
	}
	return b.String()
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

// ─── the run's model (gap 1) ───────────────────────────────────────────────
//
// RunOpen/Run carry a Model (run_board.go); Start must apply it to the session
// before the first prompt, or every run silently runs on the profile default.
// The fake agent reports, in the turn's own agent text, the model it holds and
// how many set_model requests it has received — so the assertion is made
// through the protocol, in the order the prompt sees it.

// TestRunHermesAppliesModelBeforePrompt: a run opened with a model has it
// applied to the session before the first prompt, exactly once. The agent's
// own report of what it held when the prompt reached it is the evidence.
func TestRunHermesAppliesModelBeforePrompt(t *testing.T) {
	const model = "custom:cogos:sentinel-model-777"
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRunWithModel(t, board, "model run", "modelinfo for node-a", model)

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if end := runEndOf(t, board, run.ID); end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok", end.Outcome, end.Reason)
	}
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	want := "modelinfo:model=" + model + ":set_model_calls=1"
	if got := ringAgentText(t, sess); !strings.Contains(got, want) {
		t.Fatalf("agent text = %q; want it to contain %q: the run's model must be applied to the session before the first prompt", got, want)
	}
	if got := sess.Info().Model; got != model {
		t.Fatalf("session model = %q, want the run's %q", got, model)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}

// TestRunHermesSetModelFailureEndsRunFail: a set_model the agent refuses ends
// the run exactly once, fail, with a reason naming the model error, and the
// session it was created in is closed. No turn is ever sent.
func TestRunHermesSetModelFailureEndsRunFail(t *testing.T) {
	const model = "reject:this-model"
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRunWithModel(t, board, "model refused", "modelinfo for node-a", model)

	err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
	if err == nil {
		t.Fatal("start returned nil: a refused set_model must end the run, not leave it running")
	}
	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeFail {
		t.Fatalf("run.end outcome = %q (%q); want fail", end.Outcome, end.Reason)
	}
	if !strings.Contains(end.Reason, model) {
		t.Fatalf("run.end reason = %q; want it to name the model error (the refused model id %q)", end.Reason, model)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	if sess.State() != StateDetached || d.Get(sess.ID()) != nil {
		t.Fatalf("session state = %s, registered = %v; want it closed with the run",
			sess.State(), d.Get(sess.ID()) != nil)
	}
	// The prompt never went out: the agent said nothing, so there is no turn
	// to fold and no text to report.
	if got := ringAgentText(t, sess); got != "" {
		t.Fatalf("agent text = %q; want none (no turn may start after a refused model)", got)
	}
}

// TestRunHermesNoModelNoSetModelCall: a run with no model behaves exactly as
// before — the executor sends no set_model at all, and the agent holds none.
func TestRunHermesNoModelNoSetModelCall(t *testing.T) {
	board := NewRunBoardStore("")
	d, session := captureHermesSession(t)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "no model", "modelinfo for node-a") // RunOpen.Model left empty

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if end := runEndOf(t, board, run.ID); end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok", end.Outcome, end.Reason)
	}
	sess := session()
	if sess == nil {
		t.Fatal("the executor created no session")
	}
	want := "modelinfo:model=:set_model_calls=0"
	if got := ringAgentText(t, sess); !strings.Contains(got, want) {
		t.Fatalf("agent text = %q; want it to contain %q: a run with no model must send no set_model", got, want)
	}
}

// ─── the run's report (gap 2) ──────────────────────────────────────────────
//
// run.end carries no report text, and the session is deleted right after it, so
// a client cannot read what the agent said. A run that ends with a turn writes
// this turn's agent text to <root>/.cog/run/runs/<run id>/report.md and points
// RunEnd.Report at it (sha256 of the file bytes).

// TestRunHermesWritesReportWithMatchingSHA: a run ending with a turn writes its
// agent text to the per-run report file, and RunEnd.Report carries that path
// and the sha256 of exactly those bytes.
func TestRunHermesWritesReportWithMatchingSHA(t *testing.T) {
	board, root := rootedRunBoard(t)
	d := fakeHermesDriver(t, nil)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "report", "hello from node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok", end.Outcome, end.Reason)
	}
	if end.Report == nil {
		t.Fatal("run.end carries no report: the session is deleted with the run, so the client has nothing to read")
	}
	if want := reportPathFor(root, run.ID); end.Report.Path != want {
		t.Fatalf("report path = %q, want %q", end.Report.Path, want)
	}
	body, err := os.ReadFile(end.Report.Path)
	if err != nil {
		t.Fatalf("read report %s: %v", end.Report.Path, err)
	}
	if got := string(body); got != "echo:hello from node-a" {
		t.Fatalf("report = %q, want this turn's agent text alone", got)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); end.Report.SHA256 != want {
		t.Fatalf("report sha256 = %q, want %q (of the file's bytes)", end.Report.SHA256, want)
	}
	// The per-run state directory is the kernel's own (0700), not world-writable.
	info, err := os.Stat(filepath.Dir(end.Report.Path))
	if err != nil {
		t.Fatalf("stat report dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("report dir mode = %o, want 700", got)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}

// TestRunHermesReportKeepsOnlyThisTurn: a resumed session's ring may still hold
// an earlier turn's agent text. The report is this turn's text, not the ring's.
func TestRunHermesReportKeepsOnlyThisTurn(t *testing.T) {
	board, _ := rootedRunBoard(t)
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.afterCreateCall = func(p *hermesProcess) {
			// Before Start takes its baseline: an earlier turn's agent text,
			// exactly the shape the driver stores for session/update.
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, s := range p.sessions {
				s.Events().Append(MSEventSessionUpdate, map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": "earlier turn text"},
				})
			}
		}
	})
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "resumed ring", "hello from node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	end := runEndOf(t, board, run.ID)
	if end.Report == nil {
		t.Fatal("run.end carries no report")
	}
	body, err := os.ReadFile(end.Report.Path)
	if err != nil {
		t.Fatalf("read report %s: %v", end.Report.Path, err)
	}
	got := string(body)
	if strings.Contains(got, "earlier turn text") {
		t.Fatalf("report = %q; it contains an earlier turn's text, not just this turn's", got)
	}
	if got != "echo:hello from node-a" {
		t.Fatalf("report = %q, want this turn's agent text alone", got)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); end.Report.SHA256 != want {
		t.Fatalf("report sha256 = %q, want %q", end.Report.SHA256, want)
	}
}

// TestRunHermesReportTruncatesAtCap: a turn whose agent text runs past 1 MiB is
// capped, and the cap is marked rather than silently swallowing the tail.
func TestRunHermesReportTruncatesAtCap(t *testing.T) {
	board, _ := rootedRunBoard(t)
	d := fakeHermesDriver(t, nil)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "huge report", "huge work for node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok", end.Outcome, end.Reason)
	}
	if end.Report == nil {
		t.Fatal("run.end carries no report for a turn that produced agent text")
	}
	body, err := os.ReadFile(end.Report.Path)
	if err != nil {
		t.Fatalf("read report %s: %v", end.Report.Path, err)
	}
	const cap = 1 << 20
	if len(body) < cap {
		t.Fatalf("report = %d bytes; the agent's >1 MiB turn was not written up to the cap", len(body))
	}
	if len(body) == cap {
		t.Fatal("report stopped exactly at the cap with no truncation marker")
	}
	if len(body) > cap+512 {
		t.Fatalf("report = %d bytes, over the 1 MiB cap by more than a marker", len(body))
	}
	for i := 0; i < cap; i++ {
		if body[i] != 'x' {
			t.Fatalf("report byte %d = %q, want the agent's text ('x') up to the cap", i, body[i])
		}
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); end.Report.SHA256 != want {
		t.Fatalf("report sha256 = %q, want %q (of the truncated file's bytes)", end.Report.SHA256, want)
	}
}

// TestRunHermesKilledCancelKeepsReport: a cancel that has to force the stop
// (the agent never answers it) can close the session's ring before the turn_end
// lands, so no turn_end is observed — but there was a turn, and the text it
// streamed is still this run's report: the run ends killed, with the text.
func TestRunHermesKilledCancelKeepsReport(t *testing.T) {
	board, _ := rootedRunBoard(t)
	d := fakeHermesDriver(t, nil)
	e := testRunExecutor(t, board, d)
	run := openHermesRun(t, board, "killed with report", "ignore_cancel work for node-b")

	started := startRun(t, e, run.ID, HermesACPOpts{Cwd: t.TempDir()})
	waitAgentTurn(t, board, e, run.ID)
	cancelled := make(chan error, 1)
	go func() { cancelled <- e.Cancel(run.ID, "cancelled by node-b", "node:node-b") }()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("cancel did not return: the grace did not end the turn")
	}
	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}

	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeKilled {
		t.Fatalf("run.end outcome = %q (%q); want killed", end.Outcome, end.Reason)
	}
	if end.Report == nil {
		t.Fatal("a killed run whose turn streamed text carries no report")
	}
	body, err := os.ReadFile(end.Report.Path)
	if err != nil {
		t.Fatalf("read report %s: %v", end.Report.Path, err)
	}
	if got := string(body); got != "working..." {
		t.Fatalf("report = %q, want the text the cancelled turn streamed", got)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); end.Report.SHA256 != want {
		t.Fatalf("report sha256 = %q, want %q", end.Report.SHA256, want)
	}
}

// TestRunHermesReportWriteFailureStillEndsRun: a report that cannot be written
// never costs the run its run.end — it is written without a report instead.
func TestRunHermesReportWriteFailureStillEndsRun(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()
	// A regular file where the report's parent chain must be a directory:
	// MkdirAll(<root>/.cog/run/runs/<run id>) cannot succeed, so the report
	// write fails after the turn ended. The ledger (under .cog/ledger/) is
	// unaffected.
	if err := os.MkdirAll(filepath.Join(root, ".cog"), 0o755); err != nil {
		t.Fatalf("mkdir .cog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".cog", "run"), []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("seed blocking file: %v", err)
	}

	board := NewRunBoardStore(root)
	d := fakeHermesDriver(t, nil)
	e := NewHermesRunExecutor(board, d)
	run := openHermesRun(t, board, "report write fails", "hello from node-a")

	if err := e.Start(runCtx(t), run.ID, HermesACPOpts{Profile: "cog", Cwd: t.TempDir()}); err != nil {
		t.Fatalf("start: %v", err)
	}
	end := runEndOf(t, board, run.ID)
	if end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok: a report write failure must never prevent run.end", end.Outcome, end.Reason)
	}
	if end.Report != nil {
		t.Fatalf("run.end report = %+v; want none (the write could not succeed)", end.Report)
	}
	if n := endEntries(t, board, run.ID); n != 1 {
		t.Fatalf("run.end entries = %d, want exactly 1", n)
	}
}
