package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// drainRepo makes a git worktree with one commit on branch "feat/drain-me" and
// returns its path and the commit SHA.
func drainRepo(t *testing.T) (dir, sha string) {
	t.Helper()
	dir = t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q")
	run("checkout", "-q", "-b", "feat/drain-me")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "a.txt")
	run("commit", "-q", "-m", "first")
	return dir, run("rev-parse", "HEAD")
}

func drainTestDriver(t *testing.T) (*HermesACPDriver, string) {
	t.Helper()
	drainDir := t.TempDir()
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.RecordDir = t.TempDir()
		c.DrainDir = drainDir
	})
	return d, drainDir
}

func readDrainRecords(t *testing.T, dir string) []DrainCancelRecord {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var out []DrainCancelRecord
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			t.Fatalf("unexpected file in drain dir: %s", e.Name())
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var r DrainCancelRecord
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatalf("%s: %v\n%s", e.Name(), err, b)
		}
		out = append(out, r)
	}
	return out
}

// A session mid-turn when the drain starts is cancelled, and its record carries
// all four required fields and is marked drain-cancelled, not crashed.
func TestDrain_MidTurnIsCancelledAndRecordedRedispatchable(t *testing.T) {
	d, drainDir := drainTestDriver(t)
	wt, sha := drainRepo(t)
	if err := os.WriteFile(filepath.Join(wt, "wip.go"), []byte("package x"), 0o644); err != nil { // uncommitted work
		t.Fatal(err)
	}
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: wt, Room: "485ee7"})
	if err != nil {
		t.Fatal(err)
	}
	const brief = "slow: build the thing in this worktree, then report" // "slow" prefix: the fake agent holds the turn until cancelled
	turn, err := s.Prompt(json.RawMessage(`"` + brief + `"`))
	if err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnStart))
	// The agent has done a tool call by now.
	s.Events().Append(MSEventSessionUpdate, map[string]any{"sessionUpdate": "tool_call", "toolCallId": "c1", "title": "edit wip.go", "kind": "edit", "status": "pending"})
	s.Events().Append(MSEventSessionUpdate, map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "c1", "status": "completed"})

	paths := d.BeginDrain()
	if len(paths) != 1 {
		t.Fatalf("BeginDrain wrote %d records, want 1", len(paths))
	}
	// Record exists BEFORE the cancel: the turn is still in flight.
	if s.Info().TurnInFlight == "" {
		t.Fatal("BeginDrain must record before cancelling: turn already gone")
	}
	d.Close()

	// Close cancels the turn and then stops the process, and which of the two
	// the turn_end reports is a race that predates drain: "cancelled" if the
	// agent answered the cancel first, "agent process exited" if the stop won.
	// Either way the turn is over, and ended by the drain rather than finishing.
	// Read the ring directly: Close also closes it, so a subscriber may miss it.
	var endData string
	for i := 0; i < 200 && endData == ""; i++ {
		evs, _ := s.Events().Since(0)
		for _, ev := range evs {
			if ev.Kind == MSEventTurnEnd {
				endData = string(ev.Data)
			}
		}
		if endData == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !strings.Contains(endData, `"stop_reason":"cancelled"`) && !strings.Contains(endData, "agent process exited") {
		t.Fatalf("turn_end = %q, want cancelled (or the process exiting under the cancel)", endData)
	}
	if s.Info().TurnInFlight != "" {
		t.Fatal("turn still in flight after drain Close")
	}

	recs := readDrainRecords(t, drainDir)
	if len(recs) != 1 {
		t.Fatalf("%d records after BeginDrain+Close, want exactly 1 (record once)", len(recs))
	}
	r := recs[0]
	// The four required fields.
	if !strings.Contains(r.BriefText, brief) {
		t.Errorf("brief_text = %q, want it to contain the task", r.BriefText)
	}
	if r.Worktree != wt {
		t.Errorf("worktree = %q, want %q", r.Worktree, wt)
	}
	if r.Branch != "feat/drain-me" {
		t.Errorf("branch = %q", r.Branch)
	}
	if r.LastCommit != sha {
		t.Errorf("last_commit = %q, want %q", r.LastCommit, sha)
	}
	// Marked drain-cancelled, not crashed.
	if r.Outcome != "drain-cancelled" || r.Crashed {
		t.Errorf("outcome=%q crashed=%v, want drain-cancelled / false", r.Outcome, r.Crashed)
	}
	if r.Schema != DrainSchema || !strings.Contains(r.Note, "NOT a crash") {
		t.Errorf("schema=%q note=%q", r.Schema, r.Note)
	}
	// Checkpoint, mirroring pickup.py.
	if r.SessionID != s.ID() || r.TurnID != turn || r.Room != "485ee7" || r.CancelledAt.IsZero() || r.TurnStartedAt.IsZero() {
		t.Errorf("identity/timing fields wrong: %+v", r)
	}
	if r.LastTool == nil || r.LastTool.ID != "c1" || r.LastTool.Title != "edit wip.go" || r.LastTool.Status != "completed" {
		t.Errorf("last_tool = %+v", r.LastTool)
	}
	if len(r.DirtyFiles) != 1 || !strings.Contains(r.DirtyFiles[0], "wip.go") {
		t.Errorf("dirty_files = %v", r.DirtyFiles)
	}
	if len(r.Brief) == 0 {
		t.Error("brief (content blocks) missing")
	}
}

// After BeginDrain nothing new starts: no session, no prompt, no redirect.
func TestDrain_RefusesNewWork(t *testing.T) {
	d, _ := drainTestDriver(t)
	idle, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	busy, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Prompt(json.RawMessage(`"slow"`)); err != nil {
		t.Fatal(err)
	}
	d.BeginDrain()
	if !d.Draining() {
		t.Fatal("Draining() = false after BeginDrain")
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()}); !errors.Is(err, ErrDraining) {
		t.Errorf("Create while draining: %v, want ErrDraining", err)
	}
	if _, err := idle.Prompt(json.RawMessage(`"hi"`)); !errors.Is(err, ErrDraining) {
		t.Errorf("Prompt while draining: %v, want ErrDraining", err)
	}
	if _, err := busy.Redirect(json.RawMessage(`"fold this in"`)); !errors.Is(err, ErrDraining) {
		t.Errorf("Redirect while draining: %v, want ErrDraining", err)
	}
}

// Draining sessions with no turn running leave no record: nothing was lost.
func TestDrain_IdleSessionWritesNoRecord(t *testing.T) {
	d, drainDir := drainTestDriver(t)
	if _, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if paths := d.BeginDrain(); len(paths) != 0 {
		t.Fatalf("idle drain wrote %v", paths)
	}
	d.Close()
	if recs := readDrainRecords(t, drainDir); len(recs) != 0 {
		t.Fatalf("idle drain left %d records", len(recs))
	}
}

// Close on its own (no BeginDrain) still records an in-flight turn: the record
// does not depend on the caller remembering to drain first.
func TestDrain_CloseAloneRecords(t *testing.T) {
	d, drainDir := drainTestDriver(t)
	wt, sha := drainRepo(t)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: wt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow work"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnStart))
	d.Close()
	recs := readDrainRecords(t, drainDir)
	if len(recs) != 1 || recs[0].LastCommit != sha || recs[0].Outcome != DrainOutcome {
		t.Fatalf("records = %+v", recs)
	}
}

// A worktree git cannot read still gets a record, with the reason; the brief
// and worktree path (the fields git does not supply) are intact.
func TestDrain_GitFailureStillRecords(t *testing.T) {
	d, drainDir := drainTestDriver(t)
	notRepo := t.TempDir()
	t.Setenv("GIT_CEILING_DIRECTORIES", filepath.Dir(notRepo)) // TMPDIR may itself sit inside a repo
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: notRepo})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow task"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnStart))
	d.BeginDrain()
	d.Close()
	recs := readDrainRecords(t, drainDir)
	if len(recs) != 1 {
		t.Fatalf("records = %d", len(recs))
	}
	r := recs[0]
	if r.GitError == "" || r.LastCommit != "" || r.Worktree != notRepo || r.BriefText != "slow task" || r.Crashed {
		t.Errorf("record = %+v", r)
	}
}

// Forwarded prompts (Redirect) ride along in the record, in order.
func TestDrain_ForwardsAreCarried(t *testing.T) {
	d, drainDir := drainTestDriver(t)
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prompt(json.RawMessage(`"slow task"`)); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnStart))
	s.mu.Lock()
	s.turn.forwards = append(s.turn.forwards, json.RawMessage(`[{"type":"text","text":"also do X"}]`))
	s.mu.Unlock()
	d.BeginDrain()
	d.Close()
	recs := readDrainRecords(t, drainDir)
	if len(recs) != 1 || len(recs[0].Forwards) != 1 || !strings.Contains(string(recs[0].Forwards[0]), "also do X") {
		t.Fatalf("records = %+v", recs)
	}
}

// With no DrainDir the records land beside RecordDir, not in it: the retention
// sweep deletes RecordDir files of sessions the driver no longer holds.
func TestDrain_DefaultDirIsSiblingOfRecordDir(t *testing.T) {
	root := t.TempDir()
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.RecordDir = filepath.Join(root, "managed-sessions") })
	if got, want := d.drainDir(), filepath.Join(root, "drain-cancelled"); got != want {
		t.Fatalf("drainDir = %q, want %q", got, want)
	}
	bare := fakeHermesDriver(t, nil)
	if bare.drainDir() != "" {
		t.Fatal("no dirs configured: drainDir must be empty")
	}
}
