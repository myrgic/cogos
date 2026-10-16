// run_exec_hermes.go - the Hermes-fork run executor (P5 slice W2).
//
// A run whose executor kind is hermes-fork is hosted by the kernel through
// HermesACPDriver.Create; its turn_end (read from the session EventRing)
// closes the run on the board with exactly one run.end. The reconcile daemon
// (type "run", 30 s tick) is the backstop that closes runs whose turn_end was
// missed.
//
// Ownership: the kernel owns the agent process (ADR-093), so Start is the
// run's host loop and does not return on the caller's context alone. A run
// ends when its turn ends (Start writes run.end), when a cancel ends the turn
// (Cancel -> killed), or when a backstop finds the session gone (BootSweep,
// the reconcile tick: lost/crash).
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// RunReconcileType is the reconcile-daemon provider type for kernel runs.
const RunReconcileType = "run"

// runExecActor writes the board entries the executor owns. The run board maps
// a "node:" actor to the node identity (runActor, run_board.go).
const runExecActor = "node:kernel"

// runExecObservedBy is RunEnd.ObservedBy for every end this kernel writes.
const runExecObservedBy = "kernel"

// runAgentExited is the driver's report of a lost in-flight turn: the agent
// process died while session/prompt was in flight (managed_session_hermes.go).
const runAgentExited = "agent process exited"

// runEventBuffer is the subscription buffer Start watches the ring with. A
// subscriber that falls behind is dropped by the ring (not blocked), so
// awaitTurnEnd falls back to the retained events.
const runEventBuffer = 512

// HermesRunExecutor drives hermes-fork runs on the run board.
type HermesRunExecutor struct {
	board  *RunBoardStore
	driver *HermesACPDriver

	mu     sync.Mutex
	active map[string]*runExecSession // run id -> hosting session
}

// runExecSession is one run this kernel is hosting. Everything but done is
// guarded by HermesRunExecutor.mu: the active set is the kernel's answer to
// "who is driving this run?".
type runExecSession struct {
	sess *HermesACPSession

	cancelled bool   // Cancel landed: the turn's end reads as killed
	reason    string // the cancel reason, recorded on run.end

	// done is closed once run.end has been written and the session let go.
	// Cancel waits on it, so a caller that cancels a run sees the run ended.
	done chan struct{}
}

// NewHermesRunExecutor builds the executor over a run board and a driver.
func NewHermesRunExecutor(board *RunBoardStore, driver *HermesACPDriver) *HermesRunExecutor {
	return &HermesRunExecutor{
		board:  board,
		driver: driver,
		active: map[string]*runExecSession{},
	}
}

// Start hosts an already-opened run: creates the Hermes session (opts), sends
// the brief as the first prompt, and watches the ring for turn_end. The turn
// result is folded into exactly one run.end (observed_by kernel), and the
// session is closed before Start returns.
func (e *HermesRunExecutor) Start(ctx context.Context, runID string, opts HermesACPOpts) error {
	run, ok := e.board.Get(runID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	if run.Executor.Kind != RunExecutorHermesFork {
		return fmt.Errorf("%w: run %s executor is %q, not %s", ErrRunInvalid, runID, run.Executor.Kind, RunExecutorHermesFork)
	}
	if run.End != nil {
		return fmt.Errorf("%w: run %s already has a run.end", ErrRunConflict, runID)
	}

	sess, err := e.driver.Create(ctx, opts)
	if err != nil {
		return fmt.Errorf("run %s: create session: %w", runID, err)
	}

	a := &runExecSession{sess: sess, done: make(chan struct{})}
	e.mu.Lock()
	if e.active[runID] != nil {
		e.mu.Unlock()
		_ = e.driver.Delete(sess.ID())
		return fmt.Errorf("%w: run %s is already hosted by this kernel", ErrRunConflict, runID)
	}
	e.active[runID] = a
	e.mu.Unlock()

	// Liveness first (U2): the session does not survive a kernel restart, so a
	// run left with a live record and no run.end is exactly what BootSweep
	// closes. The executor_ref is the session id the reconcile tick matches
	// against the driver's live sessions.
	pid, started := sess.procInfo()
	if _, err := e.board.SetLive(runID, &RunLive{PID: pid, PIDStarted: started}, sess.ID(), runExecActor); err != nil {
		e.closeRun(runID, a, RunEnd{
			Outcome: RunOutcomeFail, Reason: "record liveness: " + err.Error(), ObservedBy: runExecObservedBy,
		})
		return fmt.Errorf("run %s: record liveness: %w", runID, err)
	}

	// Watch from the session's current tail: a resumed session's ring may
	// still hold an earlier turn's turn_end, which is not this run's.
	baseline := sess.Events().LastSeq()
	sub := sess.Events().Subscribe(baseline, runEventBuffer)
	defer sess.Events().Unsubscribe(sub)

	brief, err := json.Marshal(run.Brief)
	if err != nil {
		e.closeRun(runID, a, RunEnd{
			Outcome: RunOutcomeFail, Reason: "encode brief: " + err.Error(), ObservedBy: runExecObservedBy,
		})
		return fmt.Errorf("run %s: encode brief: %w", runID, err)
	}
	if _, err := sess.Prompt(brief); err != nil {
		// The turn never started, so no turn_end will arrive: report the
		// prompt failure as the run's end (a session that was already dead
		// reads as the agent process being gone).
		end := RunEnd{Outcome: RunOutcomeFail, Reason: "first prompt: " + err.Error(), ObservedBy: runExecObservedBy}
		if errors.Is(err, ErrSessionNotLive) || sess.State() == StateCrashed {
			end.Outcome, end.Reason = RunOutcomeCrash, runAgentExited
		}
		e.closeRun(runID, a, end)
		return fmt.Errorf("run %s: first prompt: %w", runID, err)
	}

	turn, seen := awaitTurnEnd(sess, sub, baseline)
	e.mu.Lock()
	cancelled, reason := a.cancelled, a.reason
	e.mu.Unlock()
	e.closeRun(runID, a, runEndForTurn(cancelled, reason, turn, seen))
	return nil
}

// Cancel stops the run's in-flight turn; the run ends with outcome killed. It
// returns once Start has written the run.end, so a caller that gets nil knows
// the run is closed. A run this kernel is not hosting is a conflict: either it
// is not active (already ended, or another kernel's) or it has no driver here.
func (e *HermesRunExecutor) Cancel(runID, reason, actor string) error {
	e.mu.Lock()
	a := e.active[runID]
	if a == nil {
		e.mu.Unlock()
		return fmt.Errorf("%w: run %s is not hosted by this kernel", ErrRunConflict, runID)
	}
	a.cancelled = true
	if reason == "" {
		reason = "cancelled"
	}
	a.reason = reason
	e.mu.Unlock()

	// Not held across the session call: Cancel reaches the agent, and a
	// session that already died leaves the turn to end on its own.
	if err := a.sess.Cancel(); err != nil && !errors.Is(err, ErrSessionNotLive) {
		return fmt.Errorf("run %s: cancel turn: %w", runID, err)
	}
	<-a.done

	// Start writes the outcome the turn reached. If the turn completed before
	// the cancel landed, the cancel lost the race and is a conflict, not a
	// kill: the caller must not believe it stopped a running turn.
	if run, ok := e.board.Get(runID); ok && run.End != nil && run.End.Outcome != RunOutcomeKilled {
		return fmt.Errorf("%w: run %s ended %s before the cancel landed", ErrRunConflict, runID, run.End.Outcome)
	}
	return nil
}

// BootSweep closes every kernel-owned hermes-fork run left running by a
// previous kernel with run.end outcome lost (U2: sessions do not survive a
// restart). Returns the ids it closed. It is idempotent: a run with a run.end
// is not swept, so a second call closes nothing.
func (e *HermesRunExecutor) BootSweep(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	closed := []string{}
	for _, r := range e.board.List(RunFilter{Executor: RunExecutorHermesFork}) {
		if r.End != nil || r.Archived || e.hosted(r.ID) {
			continue
		}
		end := RunEnd{
			Outcome:    RunOutcomeLost,
			Reason:     "kernel restarted with no live driver",
			ObservedBy: runExecObservedBy,
			Evidence:   "boot sweep: hermes-fork run with no kernel session",
		}
		if _, err := e.board.End(r.ID, end, runExecActor); err != nil {
			return closed, fmt.Errorf("boot sweep: close run %s: %w", r.ID, err)
		}
		slog.Warn("run exec hermes: closed a leftover run as lost at boot", "run", r.ID)
		closed = append(closed, r.ID)
	}
	return closed, nil
}

// hosted reports whether this kernel is driving the run right now.
func (e *HermesRunExecutor) hosted(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active[runID] != nil
}

// closeRun writes the run's one run.end, drops the hosting session, and wakes
// a waiting Cancel. A refused end is not an error: the board's I1 conflict
// means another writer (the boot sweep, the reconcile tick) closed the run
// first, and that run.end stands.
func (e *HermesRunExecutor) closeRun(runID string, a *runExecSession, end RunEnd) {
	if _, err := e.board.End(runID, end, runExecActor); err != nil && !errors.Is(err, ErrRunConflict) {
		slog.Error("run exec hermes: could not end run", "run", runID, "outcome", end.Outcome, "error", err)
	}
	e.release(runID, a)
	close(a.done)
}

// release clears the run from the active set and closes its session. It is
// called after run.end: the run is over, so its session goes with it.
func (e *HermesRunExecutor) release(runID string, a *runExecSession) {
	e.mu.Lock()
	if e.active[runID] == a {
		delete(e.active, runID)
	}
	e.mu.Unlock()
	if a.sess != nil {
		_ = e.driver.Delete(a.sess.ID())
	}
}

// runTurnEnd is the body of a turn_end event (managed_session_events.go).
type runTurnEnd struct {
	TurnID     string `json:"turn_id"`
	StopReason string `json:"stop_reason"`
	Error      string `json:"error"`
}

// awaitTurnEnd reads the session's ring for the turn_end, from the
// subscription's replay, its live channel, and (when the subscription ends
// early: the ring closed, or this subscriber was dropped for falling behind)
// the events retained after baseline.
func awaitTurnEnd(sess *HermesACPSession, sub *RingSubscription, baseline uint64) (runTurnEnd, bool) {
	for _, ev := range sub.Replay {
		if ev.Kind == MSEventTurnEnd {
			return parseTurnEnd(ev), true
		}
	}
	for ev := range sub.Live {
		if ev.Kind == MSEventTurnEnd {
			return parseTurnEnd(ev), true
		}
	}
	evs, _ := sess.Events().Since(baseline)
	for _, ev := range evs {
		if ev.Kind == MSEventTurnEnd {
			return parseTurnEnd(ev), true
		}
	}
	return runTurnEnd{}, false
}

func parseTurnEnd(ev ManagedSessionEvent) runTurnEnd {
	var end runTurnEnd
	_ = json.Unmarshal(ev.Data, &end)
	return end
}

// runEndForTurn maps one turn result to the run's single run.end. A cancel
// (this kernel's, or one a client sent through the session) ends the run
// killed; a turn whose agent process died is a crash; any other turn error is
// a fail; a clean turn is ok.
func runEndForTurn(cancelled bool, reason string, turn runTurnEnd, seen bool) RunEnd {
	switch {
	case cancelled || turn.StopReason == "cancelled":
		if reason == "" {
			reason = "turn cancelled"
		}
		return RunEnd{Outcome: RunOutcomeKilled, Reason: reason, ObservedBy: runExecObservedBy}
	case !seen:
		return RunEnd{
			Outcome:    RunOutcomeCrash,
			Reason:     "session ended before its turn reported",
			ObservedBy: runExecObservedBy,
			Evidence:   "no turn_end on the session event ring",
		}
	case turn.Error == runAgentExited:
		return RunEnd{
			Outcome: RunOutcomeCrash, Reason: runAgentExited,
			ObservedBy: runExecObservedBy, StopReason: turn.StopReason,
			Evidence: "turn_end: " + turn.Error,
		}
	case turn.Error != "":
		return RunEnd{
			Outcome: RunOutcomeFail, Reason: turn.Error,
			ObservedBy: runExecObservedBy, StopReason: turn.StopReason,
		}
	}
	return RunEnd{Outcome: RunOutcomeOK, ObservedBy: runExecObservedBy, StopReason: turn.StopReason}
}

// procInfo reports the session's agent process id and start time for the run's
// live record (0 and "" when the process is not running).
func (s *HermesACPSession) procInfo() (int, string) {
	p := s.proc
	if p == nil {
		return 0, ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return 0, ""
	}
	return p.cmd.Process.Pid, p.startedAt.UTC().Format(time.RFC3339Nano)
}

// reconcile.Reconcilable (type "run"): the daemon tick is the 30 s backstop.
// It closes any hermes-fork run whose session is gone or has reached a
// terminal state and whose run.end was never written.

func (e *HermesRunExecutor) Type() string { return RunReconcileType }

// hermesRunConfig is the "run" provider's declared config. The run board is
// the declared state itself (it is ledger-backed and folded at boot), so the
// config carries only its type.
type hermesRunConfig struct {
	Type string `json:"type"`
}

// hermesRunSession is one session the driver hosts, as FetchLive sees it.
type hermesRunSession struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Turn  string `json:"turn,omitempty"`
}

// hermesRunLive is the observed live state: the sessions the driver hosts.
type hermesRunLive struct {
	Sessions []hermesRunSession `json:"sessions"`
}

// Plan Details keys: the outcome and reason ApplyPlan writes on the run's
// run.end.
const (
	runPlanOutcome = "outcome"
	runPlanReason  = "reason"
)

func (e *HermesRunExecutor) LoadConfig(root string) (any, error) {
	return &hermesRunConfig{Type: RunReconcileType}, nil
}

// FetchLive lists the sessions the driver hosts. A hermes-fork run's session
// that is not here is gone (a kernel restart, or a driver that dropped it):
// no turn can end it, so its run.end will never come.
func (e *HermesRunExecutor) FetchLive(ctx context.Context, config any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	infos := e.driver.List()
	live := &hermesRunLive{Sessions: make([]hermesRunSession, 0, len(infos))}
	for _, info := range infos {
		live.Sessions = append(live.Sessions, hermesRunSession{ID: info.SessionID, State: info.State, Turn: info.TurnInFlight})
	}
	return live, nil
}

// asHermesRunLive accepts live state as this provider produces it, and as it
// comes back from a JSON round trip (the daemon may carry it either way).
func asHermesRunLive(live any) *hermesRunLive {
	switch v := live.(type) {
	case nil:
		return &hermesRunLive{}
	case *hermesRunLive:
		return v
	case hermesRunLive:
		return &v
	}
	raw, err := json.Marshal(live)
	if err != nil {
		return &hermesRunLive{}
	}
	var out hermesRunLive
	if json.Unmarshal(raw, &out) != nil {
		return &hermesRunLive{}
	}
	return &out
}

// ComputePlan closes the hermes-fork runs whose session is gone or terminal
// and which have no run.end: lost when the session is missing (nothing can
// end it), crash when it is in state crashed. A run this kernel hosts, a run
// with no run.end that never went live (the boot sweep's business, not the
// tick's), and a run that already has its run.end are left alone.
func (e *HermesRunExecutor) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	sessions := map[string]hermesRunSession{}
	for _, s := range asHermesRunLive(live).Sessions {
		sessions[s.ID] = s
	}
	plan := &reconcile.Plan{
		ResourceType: RunReconcileType,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Actions:      []reconcile.Action{},
		Warnings:     []string{},
	}
	for _, r := range e.board.List(RunFilter{Executor: RunExecutorHermesFork}) {
		if r.End != nil || r.Archived || e.hosted(r.ID) {
			continue
		}
		ref := ""
		if r.Executor.Ref != nil {
			ref = *r.Executor.Ref
		}
		if ref == "" && r.Live == nil {
			continue
		}
		sess, ok := sessions[ref]
		switch {
		case ref == "" || !ok:
			plan.Actions = append(plan.Actions, reconcile.Action{
				Action: reconcile.ActionDelete, ResourceType: RunReconcileType, Name: r.ID,
				Details: map[string]any{
					runPlanOutcome: RunOutcomeLost,
					runPlanReason:  "no live session for the run",
				},
			})
		case sess.State == StateCrashed.String():
			plan.Actions = append(plan.Actions, reconcile.Action{
				Action: reconcile.ActionDelete, ResourceType: RunReconcileType, Name: r.ID,
				Details: map[string]any{
					runPlanOutcome: RunOutcomeCrash,
					runPlanReason:  "session crashed before the run ended",
				},
			})
		}
	}
	plan.Summary.Deletes = len(plan.Actions)
	return plan, nil
}

// ApplyPlan writes the run.end the plan calls for. A run that got its run.end
// between the plan and the apply is skipped, not failed: I1 refused a second
// end, which is the outcome the plan wanted.
func (e *HermesRunExecutor) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	if plan == nil {
		return []reconcile.Result{}, nil
	}
	results := make([]reconcile.Result, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		res := reconcile.Result{Phase: RunReconcileType, Action: string(action.Action), Name: action.Name}
		outcome, _ := action.Details[runPlanOutcome].(string)
		reason, _ := action.Details[runPlanReason].(string)
		if !validRunOutcome(outcome) {
			res.Status = reconcile.ApplyFailed
			res.Error = fmt.Sprintf("plan names outcome %q", outcome)
			results = append(results, res)
			continue
		}
		end := RunEnd{
			Outcome: outcome, Reason: reason, ObservedBy: runExecObservedBy,
			Evidence: "reconcile tick: " + reason,
		}
		_, err := e.board.End(action.Name, end, runExecActor)
		switch {
		case err == nil:
			res.Status = reconcile.ApplySucceeded
			res.CreatedID = action.Name
			slog.Warn("run exec hermes: closed an unended run", "run", action.Name, "outcome", outcome)
		case errors.Is(err, ErrRunConflict):
			res.Status = reconcile.ApplySkipped
			res.Error = err.Error()
		default:
			res.Status = reconcile.ApplyFailed
			res.Error = err.Error()
		}
		results = append(results, res)
	}
	return results, nil
}

// BuildState reports the hermes-fork runs as tracked resources, with the run
// counts in metadata.
func (e *HermesRunExecutor) BuildState(config any, live any, existing *reconcile.State) (*reconcile.State, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	runs := e.board.List(RunFilter{Executor: RunExecutorHermesFork})
	st := &reconcile.State{
		Version:      1,
		ResourceType: RunReconcileType,
		GeneratedAt:  now,
		Resources:    make([]reconcile.Resource, 0, len(runs)),
	}
	if existing != nil {
		st.Lineage, st.Serial = existing.Lineage, existing.Serial+1
	}
	open, ended := 0, 0
	for _, r := range runs {
		attrs := map[string]any{"state": r.State}
		if r.Executor.Ref != nil {
			attrs["session"] = *r.Executor.Ref
		}
		if r.End != nil {
			attrs["outcome"] = r.End.Outcome
			ended++
		} else {
			open++
		}
		st.Resources = append(st.Resources, reconcile.Resource{
			Address: "run." + r.ID, Type: RunReconcileType, Mode: reconcile.ModeManaged,
			ExternalID: r.ID, Name: r.Title, Attributes: attrs, LastRefreshed: now,
		})
	}
	st.Metadata = map[string]any{"runs": len(runs), "open": open, "ended": ended}
	return st, nil
}

// Health reports the run counts. A run that is open with no session this
// kernel hosts is drift the backstop has not closed yet, which reads as
// degraded until it is.
func (e *HermesRunExecutor) Health() reconcile.ResourceStatus {
	runs := e.board.List(RunFilter{Executor: RunExecutorHermesFork})
	open, ended := 0, 0
	for _, r := range runs {
		if r.End == nil {
			open++
		} else {
			ended++
		}
	}
	e.mu.Lock()
	hosted := len(e.active)
	e.mu.Unlock()

	st := reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
	if open > hosted {
		st.Sync = reconcile.SyncStatusOutOfSync
		st.Health = reconcile.HealthDegraded
	}
	st.Message = fmt.Sprintf("hermes-fork runs=%d open=%d ended=%d hosted=%d", len(runs), open, ended, hosted)
	return st
}

var _ reconcile.Reconcilable = (*HermesRunExecutor)(nil)

// RegisterHermesRunExecutor upserts the executor into the global reconcile
// registry as type "run" (polled by the daemon's 30 s tick).
func RegisterHermesRunExecutor(board *RunBoardStore, driver *HermesACPDriver) *HermesRunExecutor {
	e := NewHermesRunExecutor(board, driver)
	reconcile.UpsertProvider(e.Type(), e)
	return e
}
