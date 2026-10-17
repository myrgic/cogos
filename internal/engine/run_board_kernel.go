// run_board_kernel.go - the two run-board writers the kernel itself needs for
// the executor slices (W2): opening a run for an executor kind the kernel
// hosts (POST /v1/runs accepts manual only in W1), and recording a run's
// liveness.
//
// Both are thin writers over the same fold as run_board.go: they append one
// ledger entry through commitLocked and let applyLocked (run_board.go) do the
// folding, so a rebuilt board equals the live one.

package engine

import "fmt"

// kernelRunExecutor reports whether the kernel hosts runs of this executor
// kind itself (kernelOwnedRun, run_board.go). A gateway-owned kind is opened
// by its gateway, not here.
func kernelRunExecutor(kind string) bool {
	switch kind {
	case RunExecutorHermesFork, RunExecutorCCWorkflow:
		return true
	}
	return false
}

// OpenKernelRun opens a run for a kernel-hosted executor kind (hermes-fork,
// cc-workflow). It is Open's path, with the executor kind accepted instead of
// refused: the kernel's own dispatcher is the caller, and it knows which
// executor it is starting.
func (st *RunBoardStore) OpenKernelRun(in RunOpen, actor string) (*Run, error) {
	if !kernelRunExecutor(in.Executor.Kind) {
		return nil, fmt.Errorf("%w: executor %q is not hosted by the kernel", ErrRunInvalid, in.Executor.Kind)
	}
	return st.openRun(in, actor)
}

// SetLive records a run's liveness (RunLive: the hosting process, its start
// time and the turn in flight) as one item.set entry. It is the only writer of
// Run.Live: applyLocked folds body["live"] into it. ref, when non-empty, is
// the id of the session hosting the run and is recorded as the run's
// executor_ref, so the reconcile backstop (run_exec_hermes.go) can tell a run
// whose session is gone from one that is still hosted.
func (st *RunBoardStore) SetLive(id string, live *RunLive, ref, actor string) (*Run, error) {
	if live == nil {
		return nil, fmt.Errorf("%w: live is required", ErrRunInvalid)
	}
	if live.PID < 0 {
		return nil, fmt.Errorf("%w: live.pid must not be negative", ErrRunInvalid)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	if r.End != nil {
		return nil, fmt.Errorf("%w: run %s already has a run.end", ErrRunConflict, id)
	}
	body := map[string]any{"item": id, "live": structBody(live)}
	if ref != "" {
		body["executor_ref"] = ref
	}
	if _, err := st.commitLocked(r.Room, "item.set", body, actor); err != nil {
		return nil, err
	}
	return st.view(st.runs[id]), nil
}
