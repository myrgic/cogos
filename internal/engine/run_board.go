// run_board.go - the kernel run board (stage-into-kernel campaign, slice W1).
//
// A run is a board item (item.create section "tasks", kind "run") whose
// lifecycle is recorded as room-board entries in the node's `runs` ledger
// bucket. Entry shape and field names follow the rooms RFC section 6.9.1.
// Entries are unsigned in v1 (Confessed: pre-B1); P4 B1 adds signatures.
//
// Durability follows the ShareLinkStore pattern: every mutation appends a
// ledger event BEFORE touching memory (write-ahead), and
// RebuildRunBoardStoreFromLedger replays the bucket at boot. On rebuild, a
// run the kernel owns with no live driver and no run.end is closed `lost`
// by the kernel (W0 invariant I4).
package engine

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// RunsLedgerBucket is the ledger bucket (.cog/ledger/runs/) holding the
// node's run log.
const RunsLedgerBucket = "runs"

// Store errors. The HTTP layer maps them: invalid 400, not found 404,
// conflict 409, ledger 503.
var (
	ErrRunInvalid      = errors.New("run board: invalid request")
	ErrRunNotFound     = errors.New("run board: no such run")
	ErrRunConflict     = errors.New("run board: conflict")
	ErrRunLedgerFailed = errors.New("run board: ledger append failed")
)

// runLogShort is the log short of the node's one run log; item ids are
// <runLogShort>-<seq of the item.create entry>.
const runLogShort = "rl"

// runNodeBoardRoom is the room of a run that no room started (cron, CLI,
// Discord, tool). Runs on it have no front session to wake.
const runNodeBoardRoom = "node/local"

// runWakeStallAfter is I3: a room-origin run.end with no run.delivered after
// this long reads as stalled: wake-undelivered.
const runWakeStallAfter = 5 * time.Minute

// Size bounds, a safety net against an authorized caller looping.
const (
	maxRunTitleBytes = 512
	maxRunBriefBytes = 64 << 10
	maxRunsInStore   = 100000
)

// Run states, folded from the entries of one run item. Terminal-ward order:
// running, reported, processed, done. Archive requires a prior run.end (I2).
const (
	RunStateOpen      = "open"
	RunStateRunning   = "running"
	RunStateEnded     = "ended" // run.end written, wake not yet delivered
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

// Executor kinds. POST /v1/runs accepts RunExecutorManual and
// RunExecutorHermesFork (the kind the kernel hosts itself, run_exec_hermes.go);
// Open (below) is the manual-only writer, OpenKernelRun (run_board_kernel.go)
// the kernel-hosted one.
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

// RunCancel is the body of POST /v1/runs/{id}/cancel: why the caller is
// stopping the run. It is recorded on the run's run.end (outcome killed).
type RunCancel struct {
	Reason string `json:"reason,omitempty"`
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
	EndedAt     string        `json:"ended_at,omitempty"`    // time of run.end
	Seq         uint64        `json:"seq"`                   // last folded entry seq

	setState string // explicit item.set state (processed|done); honored only after run.end
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

// runEntry is one entry of the node's run log, in the rooms RFC section 6.9.1
// shape. Unsigned in v1: Confessed names the deviation (pre-B1).
type runEntry struct {
	Room      string         `json:"room"`
	Log       string         `json:"log"`
	Seq       uint64         `json:"seq"`
	Prev      string         `json:"prev"`
	Actor     map[string]any `json:"actor"`
	Kind      string         `json:"kind"` // item.create | item.set | item.append | item.archive
	Body      map[string]any `json:"body"`
	HLC       string         `json:"hlc"`
	Confessed string         `json:"confessed"`
	ID        string         `json:"id"` // sha256 of the entry without id
}

const runConfessedPreB1 = "pre-B1"

func (e *runEntry) computeID() string {
	cp := *e
	cp.ID = ""
	b, _ := json.Marshal(cp) // maps marshal with sorted keys: deterministic
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// logicalKind is the kind a client sees: the item.append sub-kind (run.end,
// run.delivered) or the entry kind itself.
func (e *runEntry) logicalKind() string {
	if e.Kind == "item.append" {
		if k, _ := e.Body["kind"].(string); k != "" {
			return k
		}
	}
	return e.Kind
}

// itemID is the run item the entry belongs to.
func (e *runEntry) itemID() string {
	if e.Kind == "item.create" {
		return fmt.Sprintf("%s-%d", runLogShort, e.Seq)
	}
	s, _ := e.Body["item"].(string)
	return s
}

// RunBoardStore is the ledger-backed run board. It follows the ShareLinkStore
// pattern: write-ahead AppendEvent into the `runs` bucket, fold on boot.
type RunBoardStore struct {
	mu            sync.RWMutex
	runs          map[string]*Run
	order         []string // item ids in creation order
	entries       []*runEntry
	notify        chan struct{} // closed and replaced on every append
	lastHLCms     int64
	hlcCounter    int
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
	now           func() time.Time
}

// NewRunBoardStore returns an empty store. workspaceRoot "" means in-memory.
func NewRunBoardStore(workspaceRoot string) *RunBoardStore {
	return &RunBoardStore{
		runs:          make(map[string]*Run),
		notify:        make(chan struct{}),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
		now:           func() time.Time { return time.Now().UTC() },
	}
}

func cloneRun(r *Run) *Run {
	cp := *r
	if r.Executor.Ref != nil {
		ref := *r.Executor.Ref
		cp.Executor.Ref = &ref
	}
	if r.Live != nil {
		l := *r.Live
		cp.Live = &l
	}
	if r.End != nil {
		e := *r.End
		if r.End.ExitCode != nil {
			c := *r.End.ExitCode
			e.ExitCode = &c
		}
		if r.End.Report != nil {
			rp := *r.End.Report
			e.Report = &rp
		}
		cp.End = &e
	}
	if r.Delivered != nil {
		d := *r.Delivered
		cp.Delivered = &d
	}
	return &cp
}

// view returns a copy with the time-derived fields filled in.
func (st *RunBoardStore) view(r *Run) *Run {
	cp := cloneRun(r)
	if cp.End != nil && cp.Delivered == nil && cp.Room != runNodeBoardRoom && !cp.Archived {
		if t, err := time.Parse(time.RFC3339Nano, cp.EndedAt); err == nil && st.now().Sub(t) > runWakeStallAfter {
			cp.Stalled = "wake-undelivered" // I3
		}
	}
	return cp
}

// hlcLocked returns the next hybrid-logical-clock stamp (display order only).
func (st *RunBoardStore) hlcLocked(now time.Time) string {
	ms := now.UnixMilli()
	if ms <= st.lastHLCms {
		st.hlcCounter++
		ms = st.lastHLCms
	} else {
		st.lastHLCms = ms
		st.hlcCounter = 0
	}
	return fmt.Sprintf("%s/%d", time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z"), st.hlcCounter)
}

func runActor(actor string) map[string]any {
	if n, ok := strings.CutPrefix(actor, "node:"); ok {
		return map[string]any{"node": n}
	}
	if actor == "" {
		actor = "unknown"
	}
	return map[string]any{"person": actor}
}

// commitLocked write-ahead appends the entry to the ledger, then folds it.
// Caller holds st.mu and has already validated the transition against the
// current fold, so a fold of the appended entry cannot fail.
func (st *RunBoardStore) commitLocked(room, kind string, body map[string]any, actor string) (*runEntry, error) {
	e := &runEntry{
		Room:      room,
		Log:       runLogShort,
		Seq:       uint64(len(st.entries)) + 1,
		Actor:     runActor(actor),
		Kind:      kind,
		Body:      body,
		HLC:       st.hlcLocked(st.now()),
		Confessed: runConfessedPreB1,
	}
	if n := len(st.entries); n > 0 {
		e.Prev = st.entries[n-1].ID
	}
	e.ID = e.computeID()
	if st.workspaceRoot != "" {
		raw, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRunLedgerFailed, err)
		}
		var data map[string]interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRunLedgerFailed, err)
		}
		env := &EventEnvelope{
			HashedPayload: EventPayload{
				Type:      "run.entry",
				Timestamp: st.now().Format(time.RFC3339),
				SessionID: RunsLedgerBucket,
				Data:      data,
			},
			Metadata: EventMetadata{Source: "run-board"},
		}
		if err := st.appendEvent(st.workspaceRoot, RunsLedgerBucket, env); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRunLedgerFailed, err)
		}
	}
	st.applyLocked(e)
	return e, nil
}

// bodyAs round-trips a map body into a typed struct.
func bodyAs(body map[string]any, out any) {
	b, _ := json.Marshal(body)
	_ = json.Unmarshal(b, out)
}

func structBody(v any) map[string]any {
	b, _ := json.Marshal(v)
	m := map[string]any{}
	_ = json.Unmarshal(b, &m)
	return m
}

func entryTime(e *runEntry) string {
	ts, _, _ := strings.Cut(e.HLC, "/")
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t.Format(time.RFC3339Nano)
	}
	return ts
}

// applyLocked folds one entry into memory. It is the only place that mutates
// runs, and it is shared by live writes and the boot replay, so a rebuilt
// fold equals the live one.
func (st *RunBoardStore) applyLocked(e *runEntry) {
	st.entries = append(st.entries, e)
	close(st.notify)
	st.notify = make(chan struct{})

	id := e.itemID()
	if e.Kind == "item.create" {
		var in RunOpen
		bodyAs(e.Body, &in)
		st.runs[id] = &Run{
			ID: id, Room: e.Room, Title: in.Title, Brief: in.Brief, Owner: in.Owner,
			State: RunStateOpen, Executor: in.Executor, Workflow: in.Workflow, Model: in.Model,
			Worktree: in.Worktree, Origin: in.Origin, ObserveOnly: in.ObserveOnly, Seq: e.Seq,
		}
		st.order = append(st.order, id)
		return
	}
	r := st.runs[id]
	if r == nil {
		return
	}
	r.Seq = e.Seq
	switch e.logicalKind() {
	case "item.set":
		if s, ok := e.Body["state"].(string); ok && (s == RunStateProcessed || s == RunStateDone) && r.End != nil {
			r.setState = s
		}
		if ref, ok := e.Body["executor_ref"].(string); ok {
			r.Executor.Ref = &ref
		}
		if lv, ok := e.Body["live"].(map[string]any); ok {
			var l RunLive
			bodyAs(lv, &l)
			r.Live = &l
		}
	case "run.end":
		var end RunEnd
		bodyAs(e.Body, &end)
		if r.End == nil {
			r.End = &end
			r.EndedAt = entryTime(e)
		}
	case "run.delivered":
		var d RunDelivered
		bodyAs(e.Body, &d)
		if r.Delivered == nil && r.End != nil {
			r.Delivered = &d
		}
	case "item.archive":
		if r.End != nil {
			r.Archived = true
		}
	}
	r.State = foldRunState(r)
}

func foldRunState(r *Run) string {
	switch {
	case r.Archived:
		return RunStateDone
	case r.End != nil && r.setState != "":
		return r.setState
	case r.End != nil && r.Delivered != nil:
		return RunStateReported
	case r.End != nil:
		return RunStateEnded
	case r.Live != nil:
		return RunStateRunning
	}
	return RunStateOpen
}

func validRunOutcome(o string) bool {
	switch o {
	case RunOutcomeOK, RunOutcomeFail, RunOutcomeCrash, RunOutcomeKilled, RunOutcomeLost:
		return true
	}
	return false
}

func validRunOwner(o string) bool { return o == "cog" || o == "work" }

// Open writes item.create for a new run, for the one executor kind an outside
// caller may name: manual (POST /v1/runs routes hermes-fork to OpenKernelRun
// and the kernel's executor). Any other kind is refused here.
func (st *RunBoardStore) Open(in RunOpen, actor string) (*Run, error) {
	if in.Executor.Kind != RunExecutorManual {
		return nil, fmt.Errorf("%w: executor %q is not accepted here (manual only)", ErrRunInvalid, in.Executor.Kind)
	}
	return st.openRun(in, actor)
}

// openRun writes item.create for a run. Behavior and validation are Open's:
// callers have already accepted the executor kind.
func (st *RunBoardStore) openRun(in RunOpen, actor string) (*Run, error) {
	if strings.TrimSpace(in.Title) == "" || len(in.Title) > maxRunTitleBytes {
		return nil, fmt.Errorf("%w: title must be 1 to %d bytes", ErrRunInvalid, maxRunTitleBytes)
	}
	if len(in.Brief) > maxRunBriefBytes {
		return nil, fmt.Errorf("%w: brief over %d bytes", ErrRunInvalid, maxRunBriefBytes)
	}
	if !validRunOwner(in.Owner) {
		return nil, fmt.Errorf("%w: owner must be cog or work", ErrRunInvalid)
	}
	if in.Origin.By == "" {
		return nil, fmt.Errorf("%w: origin.by is required", ErrRunInvalid)
	}
	if in.Worktree != "" && in.Worktree != "auto" && in.Worktree != "none" {
		return nil, fmt.Errorf("%w: worktree must be auto or none", ErrRunInvalid)
	}
	room := in.Room
	if room == "" {
		room = runNodeBoardRoom
	}
	in.Room = ""
	body := structBody(in)
	body["section"] = "tasks"
	body["kind"] = "run"

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.runs) >= maxRunsInStore {
		return nil, fmt.Errorf("%w: run board at capacity", ErrRunConflict)
	}
	e, err := st.commitLocked(room, "item.create", body, actor)
	if err != nil {
		return nil, err
	}
	return st.view(st.runs[e.itemID()]), nil
}

// Get returns one run by item id.
func (st *RunBoardStore) Get(id string) (*Run, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	r, ok := st.runs[id]
	if !ok {
		return nil, false
	}
	return st.view(r), true
}

// List returns the runs matching f, in creation order.
func (st *RunBoardStore) List(f RunFilter) []*Run {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := []*Run{}
	for _, id := range st.order {
		r := st.runs[id]
		if f.Room != "" && r.Room != f.Room {
			continue
		}
		if f.Executor != "" && r.Executor.Kind != f.Executor {
			continue
		}
		if f.State != "" && r.State != f.State {
			continue
		}
		out = append(out, st.view(r))
	}
	return out
}

func (st *RunBoardStore) lookupLocked(id string) (*Run, error) {
	r, ok := st.runs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRunNotFound, id)
	}
	return r, nil
}

// End appends the single run.end (I1: a second end is a conflict).
func (st *RunBoardStore) End(id string, end RunEnd, actor string) (*Run, error) {
	if !validRunOutcome(end.Outcome) {
		return nil, fmt.Errorf("%w: outcome must be ok, fail, crash, killed or lost", ErrRunInvalid)
	}
	if end.ObservedBy != "kernel" && end.ObservedBy != "gateway" {
		return nil, fmt.Errorf("%w: observed_by must be kernel or gateway", ErrRunInvalid)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	if r.End != nil {
		return nil, fmt.Errorf("%w: run %s already has a run.end (I1)", ErrRunConflict, id)
	}
	body := structBody(end)
	body["item"] = id
	body["kind"] = "run.end"
	if _, err := st.commitLocked(r.Room, "item.append", body, actor); err != nil {
		return nil, err
	}
	return st.view(st.runs[id]), nil
}

// Delivered appends run.delivered, the wake acknowledgement.
func (st *RunBoardStore) Delivered(id string, d RunDelivered, actor string) (*Run, error) {
	if d.To == "" {
		return nil, fmt.Errorf("%w: to is required", ErrRunInvalid)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	if d.At == "" {
		d.At = st.now().Format(time.RFC3339)
	} else if _, err := time.Parse(time.RFC3339, d.At); err != nil {
		return nil, fmt.Errorf("%w: at must be RFC 3339", ErrRunInvalid)
	}
	if r.End == nil {
		return nil, fmt.Errorf("%w: run %s has no run.end to acknowledge", ErrRunConflict, id)
	}
	if r.Delivered != nil {
		return nil, fmt.Errorf("%w: run %s already delivered", ErrRunConflict, id)
	}
	body := structBody(d)
	body["item"] = id
	body["kind"] = "run.delivered"
	if _, err := st.commitLocked(r.Room, "item.append", body, actor); err != nil {
		return nil, err
	}
	return st.view(st.runs[id]), nil
}

// Archive appends item.archive (I2: refused before run.end).
func (st *RunBoardStore) Archive(id string, actor string) (*Run, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	if r.End == nil {
		return nil, fmt.Errorf("%w: run %s has no run.end (I2)", ErrRunConflict, id)
	}
	if r.Archived {
		return nil, fmt.Errorf("%w: run %s already archived", ErrRunConflict, id)
	}
	body := map[string]any{"item": id, "closure": st.endEntryIDLocked(id)}
	if _, err := st.commitLocked(r.Room, "item.archive", body, actor); err != nil {
		return nil, err
	}
	return st.view(st.runs[id]), nil
}

func (st *RunBoardStore) endEntryIDLocked(id string) string {
	for _, e := range st.entries {
		if e.itemID() == id && e.logicalKind() == "run.end" {
			return e.ID
		}
	}
	return ""
}

// FoldHash returns a stable hash of the folded state, used to prove that a
// restart rebuilds an equal fold. Time-derived fields (Stalled) are excluded.
func (st *RunBoardStore) FoldHash() string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	ids := append([]string(nil), st.order...)
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		b, _ := json.Marshal(st.runs[id])
		h.Write(b)
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// EventsSince returns events with Seq greater than since, for the WebSocket
// replay on GET /v1/runs/events?since=.
func (st *RunBoardStore) EventsSince(since uint64) []RunEvent {
	evs, _ := st.eventsAndWait(since)
	return evs
}

// eventsAndWait returns the events after since and a channel that closes at
// the next append, read under one lock so no append is missed in between.
func (st *RunBoardStore) eventsAndWait(since uint64) ([]RunEvent, <-chan struct{}) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := []RunEvent{}
	for _, e := range st.entries {
		if e.Seq > since {
			out = append(out, RunEvent{Seq: e.Seq, Kind: e.logicalKind(), Run: e.itemID(), Data: e.Body})
		}
	}
	return out, st.notify
}

// RebuildRunBoardStoreFromLedger folds the `runs` bucket into a store. A
// missing file is an empty store; malformed lines are skipped. After the
// replay, every run the kernel owns with no live driver and no run.end is
// closed `lost` (W0 invariant I4), and marked done: it has no front session
// to wake.
func RebuildRunBoardStoreFromLedger(workspaceRoot string) (*RunBoardStore, error) {
	st := NewRunBoardStore(workspaceRoot)
	if workspaceRoot == "" {
		return st, nil
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", RunsLedgerBucket, "events.jsonl")
	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		return st, fmt.Errorf("open run ledger: %w", err)
	}
	if err == nil {
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var env EventEnvelope
			if json.Unmarshal([]byte(line), &env) != nil || env.HashedPayload.Type != "run.entry" {
				continue
			}
			var e runEntry
			b, _ := json.Marshal(env.HashedPayload.Data)
			if json.Unmarshal(b, &e) != nil || e.Seq != uint64(len(st.entries))+1 {
				continue // malformed, or a gap/duplicate: keep the log gapless
			}
			if e.ID != e.computeID() {
				continue
			}
			if n := len(st.entries); (n > 0 && e.Prev != st.entries[n-1].ID) || (n == 0 && e.Prev != "") {
				continue
			}
			st.lastHLCms = hlcMillis(e.HLC, st.lastHLCms)
			st.applyLocked(&e)
		}
		if err := sc.Err(); err != nil {
			return st, fmt.Errorf("scan run ledger: %w", err)
		}
	}
	if err := st.closeOrphansLocked(); err != nil {
		return st, err
	}
	return st, nil
}

func hlcMillis(hlc string, cur int64) int64 {
	ts, _, _ := strings.Cut(hlc, "/")
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil && t.UnixMilli() > cur {
		return t.UnixMilli()
	}
	return cur
}

// kernelOwned reports whether the kernel, not a gateway, is responsible for
// the run's liveness (I4). Adopted gateway runs are never closed on silence.
func kernelOwnedRun(r *Run) bool {
	if r.ObserveOnly {
		return false
	}
	switch r.Executor.Kind {
	case RunExecutorManual, RunExecutorHermesFork, RunExecutorCCWorkflow:
		return true
	}
	return false
}

func (st *RunBoardStore) closeOrphansLocked() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, id := range append([]string(nil), st.order...) {
		r := st.runs[id]
		if r.End != nil || r.Live != nil || !kernelOwnedRun(r) {
			continue
		}
		end := RunEnd{
			Outcome:    RunOutcomeLost,
			Reason:     "kernel restarted with no live driver",
			ObservedBy: "kernel",
			Evidence:   "boot reconcile: no live record and no run.end",
		}
		body := structBody(end)
		body["item"] = id
		body["kind"] = "run.end"
		if _, err := st.commitLocked(r.Room, "item.append", body, "node:kernel"); err != nil {
			return fmt.Errorf("close orphan run %s: %w", id, err)
		}
		if _, err := st.commitLocked(r.Room, "item.set", map[string]any{"item": id, "state": RunStateDone}, "node:kernel"); err != nil {
			return fmt.Errorf("close orphan run %s: %w", id, err)
		}
		slog.Warn("run board: closed orphan run as lost at boot", "run", id)
	}
	return nil
}
