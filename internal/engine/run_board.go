// run_board.go - the kernel run board (stage-into-kernel campaign, slice W1).
//
// A run is a board item (item.create section "tasks", kind "run") whose
// lifecycle is recorded as room-board entries in the node's `runs` ledger
// bucket. Entry shape and field names follow the rooms RFC section 6.9.1.
// Entries are unsigned in v1 (Confessed: pre-B1); P4 B1 adds signatures.
//
// This file is the W1 skeleton: types, store constructor, bucket name and
// method signatures. Every store method is a stub that returns
// ErrRunBoardNotImplemented until the W1 body slice lands.
package engine

import (
	"errors"
	"sync"
)

// RunsLedgerBucket is the ledger bucket (.cog/ledger/runs/) holding the
// node's run log.
const RunsLedgerBucket = "runs"

// ErrRunBoardNotImplemented is returned by every W1 skeleton store method.
var ErrRunBoardNotImplemented = errors.New("run board: not implemented")

// Run states, folded from the entries of one run item. Terminal-ward order:
// running, reported, processed, done. Archive requires a prior run.end (I2).
const (
	RunStateOpen      = "open"
	RunStateRunning   = "running"
	RunStateReported  = "reported"
	RunStateProcessed = "processed"
	RunStateDone      = "done"
)

// Run outcomes carried by run.end.
const (
	RunOutcomeOK     = "ok"
	RunOutcomeFail   = "fail"
	RunOutcomeCrash  = "crash"
	RunOutcomeKilled = "killed"
	RunOutcomeLost   = "lost"
)

// Executor kinds. Only RunExecutorManual is accepted by POST /v1/runs in W1.
const (
	RunExecutorManual     = "manual"
	RunExecutorHermesFork = "hermes-fork"
	RunExecutorCCWorkflow = "cc-workflow"
	RunExecutorGWSession  = "gw-session"
	RunExecutorGWCron     = "gw-cron"
	RunExecutorGWDeleg    = "gw-delegation"
	RunExecutorGWKanban   = "gw-kanban"
)

// RunExecutor names who runs the work. Ref is the session id, workflow run
// id or gateway id once known.
type RunExecutor struct {
	Kind string  `json:"kind"`
	Ref  *string `json:"ref"`
}

// RunOrigin records who caused the run.
type RunOrigin struct {
	By           string `json:"by"`
	ParentRun    string `json:"parent_run,omitempty"`
	RecycledFrom string `json:"recycled_from,omitempty"`
}

// RunLive is the liveness record set by item.set once the run is running.
type RunLive struct {
	PID        int    `json:"pid,omitempty"`
	PIDStarted string `json:"pid_started,omitempty"`
	TurnID     string `json:"turn_id,omitempty"`
}

// RunReport points at the written report of a finished run.
type RunReport struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// RunEnd is the body of the single run.end entry. It may be written by the
// {node} actor (rooms RFC amendment 2026-10-16).
type RunEnd struct {
	Outcome    string     `json:"outcome"` // ok|fail|crash|killed|lost
	Reason     string     `json:"reason,omitempty"`
	StopReason string     `json:"stop_reason,omitempty"`
	ExitCode   *int       `json:"exit_code,omitempty"`
	Report     *RunReport `json:"report,omitempty"`
	ObservedBy string     `json:"observed_by"` // kernel|gateway
	Evidence   string     `json:"evidence,omitempty"`
}

// RunDelivered is the body of run.delivered, the wake acknowledgement.
type RunDelivered struct {
	To string `json:"to"` // front session
	At string `json:"at"`
}

// RunOpen is the POST /v1/runs request: the item.create body for a run.
type RunOpen struct {
	Room        string      `json:"room,omitempty"` // empty means the node board node/<name>
	Title       string      `json:"title"`
	Brief       string      `json:"brief,omitempty"`
	Owner       string      `json:"owner"` // cog|work
	Executor    RunExecutor `json:"executor"`
	Workflow    string      `json:"workflow,omitempty"`
	Model       string      `json:"model,omitempty"`
	Worktree    string      `json:"worktree,omitempty"` // auto|none
	Origin      RunOrigin   `json:"origin"`
	ObserveOnly bool        `json:"observe_only,omitempty"`
}

// Run is the folded view of one run item.
type Run struct {
	ID          string        `json:"id"` // <log short>-<seq>
	Room        string        `json:"room"`
	Title       string        `json:"title"`
	Brief       string        `json:"brief,omitempty"`
	Owner       string        `json:"owner"`
	State       string        `json:"state"`
	Executor    RunExecutor   `json:"executor"`
	Workflow    string        `json:"workflow,omitempty"`
	Model       string        `json:"model,omitempty"`
	Worktree    string        `json:"worktree,omitempty"`
	Origin      RunOrigin     `json:"origin"`
	ObserveOnly bool          `json:"observe_only,omitempty"`
	Live        *RunLive      `json:"live,omitempty"`
	End         *RunEnd       `json:"end,omitempty"`
	Delivered   *RunDelivered `json:"delivered,omitempty"`
	Archived    bool          `json:"archived,omitempty"`
	Stalled     string        `json:"stalled,omitempty"`     // I3: wake-undelivered
	Observation string        `json:"observation,omitempty"` // "unknown" when a gateway is unreachable
	Seq         uint64        `json:"seq"`                   // last folded entry seq
}

// RunFilter is the GET /v1/runs query: room, executor, state.
type RunFilter struct {
	Room     string
	Executor string
	State    string
}

// RunEvent is one frame on GET /v1/runs/events (WebSocket).
type RunEvent struct {
	Seq  uint64 `json:"seq"`
	Kind string `json:"kind"` // entry kind, e.g. item.create, run.end
	Run  string `json:"run"`
	Data any    `json:"data,omitempty"`
}

// RunBoardStore is the ledger-backed run board. It follows the ShareLinkStore
// pattern: write-ahead AppendEvent into the `runs` bucket, fold on boot.
type RunBoardStore struct {
	mu            sync.RWMutex
	runs          map[string]*Run
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
}

// NewRunBoardStore returns an empty store. workspaceRoot "" means in-memory.
func NewRunBoardStore(workspaceRoot string) *RunBoardStore {
	return &RunBoardStore{
		runs:          make(map[string]*Run),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
	}
}

// RebuildRunBoardStoreFromLedger folds the `runs` bucket into a store.
func RebuildRunBoardStoreFromLedger(workspaceRoot string) (*RunBoardStore, error) {
	return nil, ErrRunBoardNotImplemented
}

// Open writes item.create for a new run (executor manual only in W1).
func (st *RunBoardStore) Open(in RunOpen, actor string) (*Run, error) {
	return nil, ErrRunBoardNotImplemented
}

// Get returns one run by item id.
func (st *RunBoardStore) Get(id string) (*Run, bool) { return nil, false }

// List returns the runs matching f.
func (st *RunBoardStore) List(f RunFilter) []*Run { return nil }

// End appends the single run.end (I1: a second end is a conflict).
func (st *RunBoardStore) End(id string, end RunEnd, actor string) (*Run, error) {
	return nil, ErrRunBoardNotImplemented
}

// Delivered appends run.delivered, the wake acknowledgement.
func (st *RunBoardStore) Delivered(id string, d RunDelivered, actor string) (*Run, error) {
	return nil, ErrRunBoardNotImplemented
}

// Archive appends item.archive (I2: refused before run.end).
func (st *RunBoardStore) Archive(id string, actor string) (*Run, error) {
	return nil, ErrRunBoardNotImplemented
}

// FoldHash returns a stable hash of the folded state, used to prove that a
// restart rebuilds an equal fold.
func (st *RunBoardStore) FoldHash() string { return "" }

// EventsSince returns events with Seq greater than since, for the WebSocket
// replay on GET /v1/runs/events?since=.
func (st *RunBoardStore) EventsSince(since uint64) []RunEvent { return nil }
