package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSessionJournal_MessageAndToolCallTwoLines unit-tests the event→line
// mapping by attaching the journal to a ring directly (one message, one tool
// call) and asserts exactly two lines, each carrying session, room and kind,
// with redaction. It intentionally does NOT cover the create-path wiring; that
// is TestSessionJournal_DriverCreateWiring below.
func TestSessionJournal_MessageAndToolCallTwoLines(t *testing.T) {
	dir := t.TempDir()
	ring := NewEventRing("sess-1", 16)
	j, err := NewSessionJournal(dir, "sess-1", "room-a", "hermes")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)

	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": "hello there"},
	})
	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "call-1",
		"title":         "read_file",
		"kind":          "read",
		"rawInput":      map[string]any{"path": "/tmp/x", "api_key": "sk-should-not-appear"},
	})
	j.Close()

	b, err := os.ReadFile(filepath.Join(dir, "sess-1.jsonl"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	lines := nonEmptyLines(string(b))
	if len(lines) != 2 {
		t.Fatalf("want exactly 2 journal lines, got %d:\n%s", len(lines), string(b))
	}
	var kinds []string
	for i, ln := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(ln), &rec); err != nil {
			t.Fatalf("line %d not JSON: %v (%s)", i, err, ln)
		}
		if rec["session"] != "sess-1" {
			t.Fatalf("line %d missing session: %s", i, ln)
		}
		k, _ := rec["kind"].(string)
		if k == "" {
			t.Fatalf("line %d missing kind: %s", i, ln)
		}
		if rec["room"] != "room-a" {
			t.Fatalf("line %d missing room: %s", i, ln)
		}
		kinds = append(kinds, k)
	}
	if kinds[0] != "message" || kinds[1] != "tool" {
		t.Fatalf("want kinds [message tool], got %v", kinds)
	}
	if strings.Contains(string(b), "sk-should-not-appear") {
		t.Fatalf("journal leaked a secret: %s", string(b))
	}
}

// TestSessionJournal_WriteAfterCloseIsSafe proves the write path never blocks
// or panics a turn, even after the journal is finished.
func TestSessionJournal_WriteAfterCloseIsSafe(t *testing.T) {
	dir := t.TempDir()
	ring := NewEventRing("sess-2", 4)
	j, err := NewSessionJournal(dir, "sess-2", "", "")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)
	j.Close()
	for i := 0; i < 256; i++ {
		ring.Append(MSEventState, map[string]any{"state": "ready"})
	}
}

// TestSessionJournal_DriverCreateWiring drives the driver's own create path
// (HermesACPDriver.create) rather than attaching the journal by hand, so that
// deleting either wiring hunk in managed_session_hermes.go FAILS it:
//
//   - remove the SetJournal attach in the opts.Load branch (the block right
//     after `sess.id, sess.ring = sid, NewEventRing(...)`) and the loaded
//     session writes no old-session.jsonl — the load file read fails;
//   - remove the SetJournal attach in the session/new branch (the
//     `if sess.ring == nil` block, lines ~975-977) and the created session
//     writes no <id>.jsonl — the create file read fails.
//
// COGOS_SESSION_JOURNAL_DIR points the journal at a temp dir so the run is
// isolated. Sessions are deleted first: that closes each ring and drains its
// journal to disk before the file is read.
func TestSessionJournal_DriverCreateWiring(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(SessionJournalDirEnv, dir)

	d := fakeHermesDriver(t, nil)

	// session/new — the default-branch wiring.
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Prompt(json.RawMessage(`"hello"`)); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	waitEvent(t, s, 0, kind(MSEventTurnEnd))

	// session/load — the load-branch wiring. The id is cold, so this drives
	// the opts.Load case of create.
	loaded, err := d.Create(context.Background(), HermesACPOpts{Load: "old-session", Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	waitEvent(t, loaded, 0, kind(MSEventSessionUpdate))

	for _, id := range []string{s.ID(), loaded.ID()} {
		if err := d.Delete(id); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
	}
	for _, id := range []string{s.ID(), loaded.ID()} {
		b, err := os.ReadFile(filepath.Join(dir, journalFileName(id)+".jsonl"))
		if err != nil {
			t.Fatalf("create path wrote no journal for %s: %v", id, err)
		}
		if got := len(nonEmptyLines(string(b))); got == 0 {
			t.Fatalf("journal for %s has no lines", id)
		}
	}
}

// TestSessionJournal_UnboundRoomIsExplicitBlank asserts an unbound session's
// record still carries the room key, blank: a blank room is deliberate and
// visible, never an omitted key indistinguishable from an older writer's line.
func TestSessionJournal_UnboundRoomIsExplicitBlank(t *testing.T) {
	dir := t.TempDir()
	j, err := NewSessionJournal(dir, "sess-unbound", "", "")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring := NewEventRing("sess-unbound", 4)
	ring.SetJournal(j)
	ring.Append(MSEventState, map[string]any{"state": "ready"})
	j.Close()

	b, err := os.ReadFile(filepath.Join(dir, "sess-unbound.jsonl"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	line := nonEmptyLines(string(b))[0]
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("line not JSON: %v (%s)", err, line)
	}
	room, ok := rec["room"]
	if !ok {
		t.Fatalf("unbound record omits the room key entirely: %s", line)
	}
	if room != "" {
		t.Fatalf("unbound room = %v, want blank string: %s", room, line)
	}
}

// TestSessionJournal_FullQueueLeavesGapMarker proves a full queue is not
// silent: the dropped count is carried on the next record, and a gap at the
// tail (nothing follows) is announced by its own record — the way the ring
// reports its own eviction gap. The first journal is built directly with no
// drain goroutine, so Write can only take the full-queue branch, making the
// drop deterministic.
func TestSessionJournal_FullQueueLeavesGapMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sess-gap.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	j := &SessionJournal{
		sessionID: "sess-gap",
		ch:        make(chan ManagedSessionEvent), // no receiver: every Write drops
		done:      make(chan struct{}),
		file:      f,
		path:      path,
	}
	for i := 0; i < 3; i++ {
		j.Write(ManagedSessionEvent{Kind: MSEventState, Data: json.RawMessage(`{"state":"ready"}`)})
	}
	// The next persisted record absorbs the gap count.
	j.writeLine(ManagedSessionEvent{Time: time.Now(), Kind: MSEventState, Data: json.RawMessage(`{"state":"ready"}`)})
	j.Close()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	lines := nonEmptyLines(string(b))
	if len(lines) != 1 {
		t.Fatalf("want 1 record, got %d:\n%s", len(lines), string(b))
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("line not JSON: %v (%s)", err, lines[0])
	}
	if got, _ := rec["gap"].(float64); got != 3 {
		t.Fatalf("gap = %v, want 3: %s", rec["gap"], lines[0])
	}

	// A tail gap, with no later event to carry it, still becomes visible.
	tail, err := NewSessionJournal(dir, "sess-tail", "", "")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	tail.mu.Lock()
	tail.dropped = 2
	tail.mu.Unlock()
	tail.Close()

	tb, err := os.ReadFile(filepath.Join(dir, "sess-tail.jsonl"))
	if err != nil {
		t.Fatalf("read tail journal: %v", err)
	}
	tline := nonEmptyLines(string(tb))[0]
	var trec map[string]any
	if err := json.Unmarshal([]byte(tline), &trec); err != nil {
		t.Fatalf("tail line not JSON: %v (%s)", err, tline)
	}
	if trec["kind"] != "gap" {
		t.Fatalf("tail gap record kind = %v, want gap: %s", trec["kind"], tline)
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}
