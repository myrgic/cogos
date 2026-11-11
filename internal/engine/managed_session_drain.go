// managed_session_drain.go — kernel drain for managed sessions (cancel-and-record).
//
// A SIGTERM used to cancel every live turn and exit, leaving nothing behind but
// a log line and a stage-side "lost" sweep that could say a turn died but not
// what it was doing. Drain makes that cancel re-dispatchable (Chaz 2026-11-11,
// "a log line that says a thread died is yesterday's K2 loss again"):
//
//  1. BeginDrain sets the draining flag (no new session, no new prompt, no new
//     redirect from then on) and, for every session with a turn in flight,
//     writes a DrainCancelRecord BEFORE anything is cancelled.
//  2. Close (the existing teardown) then cancels as it always did. It also
//     records any in-flight turn BeginDrain did not see (Close called on its
//     own), once per turn.
//
// The record is a durable JSON file in DrainDir (<run>/drain-cancelled/), NOT in
// RecordDir: the retention sweep deletes RecordDir files of sessions the driver
// no longer holds, and a drain record must outlive exactly that. It carries what
// a NEW dispatch needs to pick the work back up without a human reconstructing
// it: the brief the turn was running (and any corrections forwarded into it),
// the worktree (the session's cwd), the branch, and the last commit SHA at the
// moment of cancellation, plus the stage-pickup-shaped checkpoint (elapsed,
// last tool, files touched). It is marked outcome "drain-cancelled", crashed
// false: it must never read as mysterious data loss.
//
// Wait-vs-cancel was ruled on: cancel-and-record, no bounded wait.
package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ErrDraining refuses new sessions and prompts once the kernel is draining.
var ErrDraining = errors.New("managed session: kernel is draining for shutdown")

// Drain record vocabulary. DrainOutcome is the marker a reader keys on.
const (
	DrainSchema  = "cogos.drain-cancel/v1"
	DrainOutcome = "drain-cancelled"
	drainNote    = "Cancelled by kernel drain on shutdown (SIGTERM/restart). NOT a crash and NOT a failure: " +
		"the work is intact in the worktree at last_commit (plus any dirty_files). Re-dispatch brief_text " +
		"(then forwards, in order) as a NEW turn in the same worktree/branch to resume."
	drainGitTimeout = 3 * time.Second
	drainTailBytes  = 4000
	drainMaxDirty   = 200
)

// DrainCancelRecord is the durable, re-dispatchable record of one turn the
// drain cancelled.
type DrainCancelRecord struct {
	Schema    string `json:"schema"`
	Outcome   string `json:"outcome"` // always DrainOutcome
	Crashed   bool   `json:"crashed"` // always false: drain-cancelled, not crashed
	Note      string `json:"note"`
	SessionID string `json:"session_id"`
	TurnID    string `json:"turn_id"`
	Room      string `json:"room,omitempty"`
	Class     string `json:"class,omitempty"`
	Profile   string `json:"profile,omitempty"`
	Actor     string `json:"actor,omitempty"`
	Model     string `json:"model,omitempty"`
	Mode      string `json:"mode,omitempty"`

	CancelledAt   time.Time `json:"cancelled_at"`
	TurnStartedAt time.Time `json:"turn_started_at"`
	ElapsedSecs   int64     `json:"elapsed_secs"`

	// What a new dispatch needs (the four required fields).
	BriefText  string            `json:"brief_text"`         // the task the turn was running
	Brief      json.RawMessage   `json:"brief,omitempty"`    // the same, as the ACP content blocks (non-text blocks survive)
	Forwards   []json.RawMessage `json:"forwards,omitempty"` // prompts forwarded into the running turn (Redirect), delivery unconfirmed
	Worktree   string            `json:"worktree"`           // the session's cwd
	Branch     string            `json:"branch,omitempty"`   // "HEAD" = detached
	LastCommit string            `json:"last_commit,omitempty"`
	GitError   string            `json:"git_error,omitempty"` // why branch/last_commit are empty, if they are

	// Checkpoint, mirroring stage pickup.py: last tool, files touched.
	LastTool   *DrainLastTool `json:"last_tool,omitempty"`
	DirtyFiles []string       `json:"dirty_files,omitempty"` // uncommitted paths in the worktree (git status --porcelain)
	AgentTail  string         `json:"agent_tail,omitempty"`  // end of what the agent had said this turn
}

// DrainLastTool is the last tool call the agent made in the cancelled turn.
type DrainLastTool struct {
	ID     string `json:"id,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Title  string `json:"title,omitempty"`
	Status string `json:"status,omitempty"`
}

// promptText joins the text blocks of an ACP content-block array.
func promptText(blocks json.RawMessage) string {
	var bs []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(blocks, &bs) != nil {
		return ""
	}
	var parts []string
	for _, b := range bs {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// Draining reports whether BeginDrain (or Close) has run.
func (d *HermesACPDriver) Draining() bool {
	return d.draining.Load()
}

// drainDir is where drain records go: the configured DrainDir, else a sibling
// of RecordDir ("" = nowhere durable to write).
func (d *HermesACPDriver) drainDir() string {
	if d.cfg.DrainDir != "" {
		return d.cfg.DrainDir
	}
	if d.cfg.RecordDir != "" {
		return filepath.Join(filepath.Dir(d.cfg.RecordDir), "drain-cancelled")
	}
	return ""
}

// BeginDrain refuses new work and records every in-flight turn, durably,
// before anything is cancelled. Idempotent: a turn is recorded once. It returns
// the record files written by this call.
func (d *HermesACPDriver) BeginDrain() []string {
	d.draining.Store(true)
	d.mu.Lock()
	all := make([]*HermesACPSession, 0, len(d.sessions))
	for _, s := range d.sessions {
		all = append(all, s)
	}
	d.mu.Unlock()
	return d.recordDrainCancels(all)
}

func (d *HermesACPDriver) recordDrainCancels(all []*HermesACPSession) []string {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		paths []string
		sem   = make(chan struct{}, reapConcurrency)
	)
	for _, s := range all {
		wg.Add(1)
		sem <- struct{}{}
		go func(s *HermesACPSession) {
			defer func() { <-sem; wg.Done() }()
			if p := d.recordDrainCancel(s); p != "" {
				mu.Lock()
				paths = append(paths, p)
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	return paths
}

// recordDrainCancel writes the record for s's in-flight turn, once. It returns
// the file path, or "" when there is nothing to record (no turn in flight, or
// already recorded) or the write failed (logged loudly: the cancel proceeds
// either way, a stuck shutdown helps nobody).
func (d *HermesACPDriver) recordDrainCancel(s *HermesACPSession) string {
	s.mu.Lock()
	turn := s.turn
	var rec DrainCancelRecord
	if turn != nil && s.state == StateLive {
		rec = DrainCancelRecord{
			Schema: DrainSchema, Outcome: DrainOutcome, Crashed: false, Note: drainNote,
			SessionID: s.id, TurnID: turn.id, Room: s.room, Class: s.class, Profile: s.profile,
			Actor: s.actor, Model: s.model, Mode: s.mode,
			TurnStartedAt: turn.startedAt, Worktree: s.cwd,
			Brief: turn.prompt, BriefText: promptText(turn.prompt),
			Forwards: append([]json.RawMessage(nil), turn.forwards...),
		}
	} else {
		turn = nil
	}
	s.mu.Unlock()
	if turn == nil {
		return ""
	}

	key := s.id + "/" + turn.id
	d.mu.Lock()
	if d.drainRecorded == nil {
		d.drainRecorded = map[string]string{}
	}
	if _, done := d.drainRecorded[key]; done {
		d.mu.Unlock()
		return ""
	}
	d.drainRecorded[key] = "" // claimed; path filled in below
	d.mu.Unlock()

	now := d.clockNow()
	rec.CancelledAt = now
	rec.ElapsedSecs = int64(now.Sub(turn.startedAt).Seconds())
	rec.LastTool, rec.AgentTail = drainRingCheckpoint(s, turn)
	d.drainGit(&rec)

	dir := d.drainDir()
	if dir == "" {
		slog.Error("managed-sessions: DRAIN RECORD NOT WRITTEN (no DrainDir/RecordDir configured); turn is cancelled and its work is not re-dispatchable from a record",
			"session", s.id, "turn", turn.id, "worktree", rec.Worktree)
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	name := fmt.Sprintf("%s-%s.json", now.UTC().Format("20060102T150405Z"), hex.EncodeToString(sum[:6]))
	path := filepath.Join(dir, name)
	b, err := json.MarshalIndent(rec, "", "  ")
	if err == nil {
		err = atomicWriteConfigFile(path, append(b, '\n'))
	}
	if err != nil {
		slog.Error("managed-sessions: DRAIN RECORD WRITE FAILED; turn will still be cancelled",
			"session", s.id, "turn", turn.id, "path", path, "err", err)
		return ""
	}
	d.mu.Lock()
	d.drainRecorded[key] = path
	d.mu.Unlock()
	slog.Warn("managed-sessions: drain-cancelled turn recorded (re-dispatchable)",
		"session", s.id, "turn", turn.id, "worktree", rec.Worktree, "branch", rec.Branch,
		"last_commit", rec.LastCommit, "record", path)
	return path
}

// drainRingCheckpoint reads the last tool call and the tail of the agent's text
// for this turn off the session's event ring.
func drainRingCheckpoint(s *HermesACPSession, turn *hermesTurn) (*DrainLastTool, string) {
	evs, _ := s.Events().Since(0)
	// Only this turn's events: from its turn_start on.
	start := 0
	for i, ev := range evs {
		if ev.Kind == MSEventTurnStart && bytes.Contains(ev.Data, []byte(`"`+turn.id+`"`)) {
			start = i
		}
	}
	var last *DrainLastTool
	var text strings.Builder
	byID := map[string]*DrainLastTool{}
	for _, ev := range evs[start:] {
		if ev.Kind != MSEventSessionUpdate {
			continue
		}
		var u struct {
			Kind   string `json:"sessionUpdate"`
			ID     string `json:"toolCallId"`
			Title  string `json:"title"`
			TKind  string `json:"kind"`
			Status string `json:"status"`
		}
		if json.Unmarshal(ev.Data, &u) != nil {
			continue
		}
		switch u.Kind {
		case "agent_message_chunk":
			text.WriteString(agentMessageText(ev.Data))
		case "tool_call", "tool_call_update":
			t := byID[u.ID]
			if t == nil {
				t = &DrainLastTool{ID: u.ID}
				byID[u.ID] = t
			}
			if u.Title != "" {
				t.Title = u.Title
			}
			if u.TKind != "" {
				t.Kind = u.TKind
			}
			if u.Status != "" {
				t.Status = u.Status
			}
			last = t
		}
	}
	tail := text.String()
	if len(tail) > drainTailBytes {
		tail = tail[len(tail)-drainTailBytes:]
	}
	if last != nil {
		c := *last
		last = &c
	}
	return last, tail
}

// drainGit fills branch, last commit and dirty files from the worktree. A git
// failure is recorded in GitError, never fatal: the other fields still carry.
func (d *HermesACPDriver) drainGit(rec *DrainCancelRecord) {
	if rec.Worktree == "" {
		rec.GitError = "session has no cwd"
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainGitTimeout)
	defer cancel()
	git := func(args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "git", append([]string{"-C", rec.Worktree}, args...)...).Output()
		return strings.TrimRight(string(out), "\n"), err
	}
	var errs []string
	if sha, err := git("rev-parse", "HEAD"); err != nil {
		errs = append(errs, "rev-parse HEAD: "+err.Error())
	} else {
		rec.LastCommit = sha
	}
	if br, err := git("rev-parse", "--abbrev-ref", "HEAD"); err != nil {
		errs = append(errs, "branch: "+err.Error())
	} else {
		rec.Branch = br
	}
	if st, err := git("status", "--porcelain"); err != nil {
		errs = append(errs, "status: "+err.Error())
	} else if st != "" {
		for _, line := range strings.Split(st, "\n") {
			if len(rec.DirtyFiles) >= drainMaxDirty {
				rec.DirtyFiles = append(rec.DirtyFiles, "...(truncated)")
				break
			}
			rec.DirtyFiles = append(rec.DirtyFiles, line)
		}
	}
	rec.GitError = strings.Join(errs, "; ")
}
