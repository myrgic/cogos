// run_exec_hermes.go - the Hermes-fork run executor (P5 slice W2, skeleton).
//
// A run whose executor kind is hermes-fork is hosted by the kernel through
// HermesACPDriver.Create; its turn_end (read from the session EventRing)
// closes the run on the board with exactly one run.end. The reconcile daemon
// (type "run", 30 s tick) is the backstop that closes runs whose turn_end was
// missed. Every method here is a stub; behavior lands in the next commit.

package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// RunReconcileType is the reconcile-daemon provider type for kernel runs.
const RunReconcileType = "run"

// ErrRunExecNotImplemented is returned by every stub method.
var ErrRunExecNotImplemented = errors.New("run exec hermes: not implemented")

// HermesRunExecutor drives hermes-fork runs on the run board.
type HermesRunExecutor struct {
	board  *RunBoardStore
	driver *HermesACPDriver

	mu     sync.Mutex
	active map[string]*HermesACPSession // run id -> hosting session
	health reconcile.ResourceStatus
}

// NewHermesRunExecutor builds the executor over a run board and a driver.
func NewHermesRunExecutor(board *RunBoardStore, driver *HermesACPDriver) *HermesRunExecutor {
	return &HermesRunExecutor{
		board:  board,
		driver: driver,
		active: map[string]*HermesACPSession{},
	}
}

// Start hosts an already-opened run: creates the Hermes session (opts), sends
// the brief as the first prompt, and watches the ring for turn_end.
func (e *HermesRunExecutor) Start(ctx context.Context, runID string, opts HermesACPOpts) error {
	return ErrRunExecNotImplemented
}

// Cancel stops the run's in-flight turn; the run ends with outcome killed.
func (e *HermesRunExecutor) Cancel(runID, reason, actor string) error {
	return ErrRunExecNotImplemented
}

// BootSweep closes every kernel-owned hermes-fork run left running by a
// previous kernel with run.end outcome lost (U2: sessions do not survive a
// restart). Returns the ids it closed.
func (e *HermesRunExecutor) BootSweep(ctx context.Context) ([]string, error) {
	return nil, ErrRunExecNotImplemented
}

// reconcile.Reconcilable (type "run"): the daemon tick is the 30 s backstop.

func (e *HermesRunExecutor) Type() string { return RunReconcileType }

func (e *HermesRunExecutor) LoadConfig(root string) (any, error) {
	return nil, ErrRunExecNotImplemented
}

func (e *HermesRunExecutor) FetchLive(ctx context.Context, config any) (any, error) {
	return nil, ErrRunExecNotImplemented
}

func (e *HermesRunExecutor) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	return nil, ErrRunExecNotImplemented
}

func (e *HermesRunExecutor) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, ErrRunExecNotImplemented
}

func (e *HermesRunExecutor) BuildState(config any, live any, existing *reconcile.State) (*reconcile.State, error) {
	return nil, ErrRunExecNotImplemented
}

func (e *HermesRunExecutor) Health() reconcile.ResourceStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.health
}

var _ reconcile.Reconcilable = (*HermesRunExecutor)(nil)

// RegisterHermesRunExecutor upserts the executor into the global reconcile
// registry as type "run" (polled by the daemon's 30 s tick).
func RegisterHermesRunExecutor(board *RunBoardStore, driver *HermesACPDriver) *HermesRunExecutor {
	e := NewHermesRunExecutor(board, driver)
	reconcile.UpsertProvider(e.Type(), e)
	return e
}
