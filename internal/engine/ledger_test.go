package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// ── Hash tests ─────────────────────────────────────────────────────────────

func TestHashEvent(t *testing.T) {
	t.Parallel()
	data := []byte(`{"session_id":"test","timestamp":"2026-01-01T00:00:00Z","type":"test.event"}`)

	sha256hash, err := HashEvent(data, "sha256")
	if err != nil {
		t.Fatalf("HashEvent sha256: %v", err)
	}
	if len(sha256hash) != 64 {
		t.Errorf("sha256 length = %d; want 64", len(sha256hash))
	}

	sha512hash, err := HashEvent(data, "sha512")
	if err != nil {
		t.Fatalf("HashEvent sha512: %v", err)
	}
	if len(sha512hash) != 128 {
		t.Errorf("sha512 length = %d; want 128", len(sha512hash))
	}

	// Deterministic: same input → same output.
	h2, _ := HashEvent(data, "sha256")
	if sha256hash != h2 {
		t.Error("HashEvent not deterministic")
	}

	// Default algorithm (empty string) = sha256.
	hDefault, err := HashEvent(data, "")
	if err != nil {
		t.Fatalf("HashEvent default: %v", err)
	}
	if hDefault != sha256hash {
		t.Error("HashEvent default != sha256")
	}

	// Unknown algorithm returns error.
	if _, err := HashEvent(data, "md5"); err == nil {
		t.Error("expected error for md5")
	}
}

// ── Canonicalize tests ─────────────────────────────────────────────────────

func TestCanonicalizeEvent(t *testing.T) {
	t.Parallel()
	payload := &EventPayload{
		Type:      "test.event",
		Timestamp: "2026-01-01T00:00:00Z",
		SessionID: "session-abc",
	}

	b, err := CanonicalizeEvent(payload)
	if err != nil {
		t.Fatalf("CanonicalizeEvent: %v", err)
	}

	// Keys sorted alphabetically: session_id < timestamp < type.
	want := `{"session_id":"session-abc","timestamp":"2026-01-01T00:00:00Z","type":"test.event"}`
	if string(b) != want {
		t.Errorf("canonical = %s; want %s", b, want)
	}

	// Deterministic.
	b2, _ := CanonicalizeEvent(payload)
	if string(b) != string(b2) {
		t.Error("CanonicalizeEvent not deterministic")
	}
}

func TestCanonicalizeEventWithPriorHash(t *testing.T) {
	t.Parallel()
	payload := &EventPayload{
		Type:      "test.event",
		Timestamp: "2026-01-01T00:00:01Z",
		SessionID: "session-abc",
		PriorHash: "abc123",
	}

	b, err := CanonicalizeEvent(payload)
	if err != nil {
		t.Fatalf("CanonicalizeEvent: %v", err)
	}

	// prior_hash < session_id < timestamp < type.
	want := `{"prior_hash":"abc123","session_id":"session-abc","timestamp":"2026-01-01T00:00:01Z","type":"test.event"}`
	if string(b) != want {
		t.Errorf("canonical = %s; want %s", b, want)
	}
}

func TestCanonicalizeEventWithData(t *testing.T) {
	t.Parallel()
	payload := &EventPayload{
		Type:      "process.start",
		Timestamp: "2026-01-01T00:00:00Z",
		SessionID: "s1",
		Data:      map[string]interface{}{"state": "active"},
	}

	b, err := CanonicalizeEvent(payload)
	if err != nil {
		t.Fatalf("CanonicalizeEvent with data: %v", err)
	}
	if len(b) == 0 {
		t.Error("empty canonical JSON")
	}
}

// ── Append + chain integrity tests ────────────────────────────────────────

func TestAppendEventChainIntegrity(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()
	sessionID := "chain-test-session"

	// Append three events in sequence.
	for i := 1; i <= 3; i++ {
		env := &EventEnvelope{
			HashedPayload: EventPayload{
				Type:      fmt.Sprintf("test.event.%d", i),
				Timestamp: nowISO(),
				SessionID: sessionID,
			},
			Metadata: EventMetadata{Source: "test"},
		}
		if err := AppendEvent(root, sessionID, env); err != nil {
			t.Fatalf("AppendEvent #%d: %v", i, err)
		}
	}

	// Read back all events and verify the chain.
	events := mustReadAllEvents(t, root, sessionID)
	if len(events) != 3 {
		t.Fatalf("event count = %d; want 3", len(events))
	}

	// Event 1: seq=1, no prior hash.
	if events[0].Metadata.Seq != 1 {
		t.Errorf("event[0].Seq = %d; want 1", events[0].Metadata.Seq)
	}
	if events[0].HashedPayload.PriorHash != "" {
		t.Errorf("event[0].PriorHash = %q; want empty", events[0].HashedPayload.PriorHash)
	}

	// Event 2: seq=2, prior_hash == event[0].hash.
	if events[1].Metadata.Seq != 2 {
		t.Errorf("event[1].Seq = %d; want 2", events[1].Metadata.Seq)
	}
	if events[1].HashedPayload.PriorHash != events[0].Metadata.Hash {
		t.Errorf("event[1].PriorHash = %q; want %q", events[1].HashedPayload.PriorHash, events[0].Metadata.Hash)
	}

	// Event 3: seq=3, prior_hash == event[1].hash.
	if events[2].Metadata.Seq != 3 {
		t.Errorf("event[2].Seq = %d; want 3", events[2].Metadata.Seq)
	}
	if events[2].HashedPayload.PriorHash != events[1].Metadata.Hash {
		t.Errorf("event[2].PriorHash = %q; want %q", events[2].HashedPayload.PriorHash, events[1].Metadata.Hash)
	}

	// Each hash must be non-empty and 64 chars (sha256).
	for i, ev := range events {
		if len(ev.Metadata.Hash) != 64 {
			t.Errorf("event[%d].Hash len = %d; want 64", i, len(ev.Metadata.Hash))
		}
	}
}

func TestAppendEventConcurrent(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()
	sessionID := "concurrent-session"

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := range workers {
		go func(n int) {
			defer wg.Done()
			env := &EventEnvelope{
				HashedPayload: EventPayload{
					Type:      fmt.Sprintf("concurrent.event.%d", n),
					Timestamp: nowISO(),
					SessionID: sessionID,
				},
				Metadata: EventMetadata{Source: "test"},
			}
			if err := AppendEvent(root, sessionID, env); err != nil {
				t.Errorf("worker %d AppendEvent: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	// All events must be present with monotonically increasing seq numbers.
	events := mustReadAllEvents(t, root, sessionID)
	if len(events) != workers {
		t.Fatalf("event count = %d; want %d", len(events), workers)
	}

	// Verify seq numbers are 1..N (order may vary between goroutines).
	seqs := make(map[int64]bool)
	for _, ev := range events {
		if seqs[ev.Metadata.Seq] {
			t.Errorf("duplicate seq %d", ev.Metadata.Seq)
		}
		seqs[ev.Metadata.Seq] = true
	}
	for seq := int64(1); seq <= workers; seq++ {
		if !seqs[seq] {
			t.Errorf("missing seq %d", seq)
		}
	}
}

func TestGetLastEventEmpty(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	last, err := GetLastEvent(root, "nonexistent-session")
	// Should return a file-not-found error and nil event.
	if err == nil {
		t.Error("expected error for missing ledger")
	}
	if last != nil {
		t.Errorf("expected nil last event; got %+v", last)
	}
}

func TestGetHashAlgorithmDefault(t *testing.T) {
	t.Parallel()
	// No genesis event → default sha256.
	alg := GetHashAlgorithm(t.TempDir())
	if alg != "sha256" {
		t.Errorf("default algorithm = %q; want sha256", alg)
	}
}

// ── Cross-session chain continuity ─────────────────────────────────────────

func TestGetLastGlobalEventNoPriorSession(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// No ledger directory at all → nil, no error.
	last, err := GetLastGlobalEvent(root, "new-session")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if last != nil {
		t.Errorf("expected nil; got %+v", last)
	}
}

func TestGetLastGlobalEventFindsNewest(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()

	// Write events into two prior sessions.
	for _, id := range []string{"session-a", "session-b"} {
		env := &EventEnvelope{
			HashedPayload: EventPayload{
				Type:      "process.start",
				Timestamp: nowISO(),
				SessionID: id,
			},
			Metadata: EventMetadata{Source: "test"},
		}
		if err := AppendEvent(root, id, env); err != nil {
			t.Fatalf("AppendEvent %s: %v", id, err)
		}
	}

	// With current session = "session-c", GetLastGlobalEvent should find one of the prior two.
	last, err := GetLastGlobalEvent(root, "session-c")
	if err != nil {
		t.Fatalf("GetLastGlobalEvent: %v", err)
	}
	if last == nil {
		t.Fatal("expected a prior event, got nil")
	}
	if last.Metadata.Hash == "" {
		t.Error("prior event has empty hash")
	}
}

func TestCrossSessionChain(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()

	// Session A: one event.
	sessionA := "session-alpha"
	envA := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "process.start",
			Timestamp: nowISO(),
			SessionID: sessionA,
		},
		Metadata: EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionA, envA); err != nil {
		t.Fatalf("AppendEvent session-a: %v", err)
	}
	lastA, _ := GetLastEvent(root, sessionA)
	hashA := lastA.Metadata.Hash

	// Session B (new session): its first event should chain from session A's last hash.
	sessionB := "session-beta"
	envB := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "process.start",
			Timestamp: nowISO(),
			SessionID: sessionB,
		},
		Metadata: EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionB, envB); err != nil {
		t.Fatalf("AppendEvent session-b: %v", err)
	}
	eventsB := mustReadAllEvents(t, root, sessionB)
	if len(eventsB) == 0 {
		t.Fatal("no events in session B")
	}

	// The genesis event of session B must reference session A's last hash.
	genesisB := eventsB[0]
	if genesisB.HashedPayload.PriorHash != hashA {
		t.Errorf("session B genesis PriorHash = %q; want session A last hash %q",
			genesisB.HashedPayload.PriorHash, hashA)
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

// mustReadAllEvents reads every event from a session ledger.
func mustReadAllEvents(t *testing.T, root, sessionID string) []EventEnvelope {
	t.Helper()
	// Sanitize to match the on-disk directory AppendEvent actually wrote
	// (see pathsafe.SanitizeComponent, #489); a no-op for the plain-slug
	// session IDs every existing caller of this helper uses.
	path := filepath.Join(root, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID), "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ledger %s: %v", path, err)
	}
	defer f.Close()

	var events []EventEnvelope
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		events = append(events, env)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan ledger: %v", err)
	}
	return events
}

// Silence "imported and not used" for fmt if the only use is in Errorf (always used).
var _ = fmt.Sprintf

// ── #489: colon-bearing session keys must not reach the filesystem raw ─────

// TestAppendEventSanitizesColonKey covers the concrete repro from #489: a
// session key of the "origin:agent" shape (e.g. "http:cog") must round-trip
// through AppendEvent/GetLastEvent while the on-disk directory name is
// NTFS-legal. The forward-slash-vs-backslash separator claim itself is
// covered at the unit level in pkg/pathsafe (SanitizeComponent's output is
// verified colon-free when stitched into both a "/"-joined and a
// simulated Windows "\"-joined path); this test proves the real engine
// call path (AppendEvent → GetLastEvent) actually uses that sanitized form
// on disk.
func TestAppendEventSanitizesColonKey(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()

	sessionID := "http:cog"
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "test.event",
			Timestamp: nowISO(),
			SessionID: sessionID,
		},
		Metadata: EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionID, env); err != nil {
		t.Fatalf("AppendEvent(%q): %v", sessionID, err)
	}

	ledgerRoot := filepath.Join(root, ".cog", "ledger")
	entries, err := os.ReadDir(ledgerRoot)
	if err != nil {
		t.Fatalf("read ledger root: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger root has %d entries, want 1: %+v", len(entries), entries)
	}
	if strings.Contains(entries[0].Name(), ":") {
		t.Fatalf("on-disk session dir %q still contains a colon", entries[0].Name())
	}
	want := pathsafe.SanitizeComponent(sessionID)
	if entries[0].Name() != want {
		t.Fatalf("on-disk session dir = %q, want %q", entries[0].Name(), want)
	}

	// GetLastEvent must find the same event back through the raw
	// (unsanitized) sessionID — callers never have to sanitize themselves.
	last, err := GetLastEvent(root, sessionID)
	if err != nil {
		t.Fatalf("GetLastEvent(%q): %v", sessionID, err)
	}
	if last == nil || last.HashedPayload.SessionID != sessionID {
		t.Fatalf("GetLastEvent round-trip failed: got %+v", last)
	}
}

// TestGetLastGlobalEventOwnColonSessionNotTreatedAsPrior guards the
// GetLastGlobalEvent comparison fixed alongside the path-construction sites:
// a colon-bearing CURRENT session must not appear to be its own "prior
// session" just because its on-disk directory name (sanitized) no longer
// string-matches the raw, unsanitized session key passed in.
func TestGetLastGlobalEventOwnColonSessionNotTreatedAsPrior(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()

	current := "http:cog"
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "process.start",
			Timestamp: nowISO(),
			SessionID: current,
		},
		Metadata: EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, current, env); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// No OTHER session exists yet — GetLastGlobalEvent(current) must not
	// mistake current's own (sanitized) ledger dir for a prior session.
	last, err := GetLastGlobalEvent(root, current)
	if err != nil {
		t.Fatalf("GetLastGlobalEvent: %v", err)
	}
	if last != nil {
		t.Fatalf("expected nil (no prior session besides current), got %+v", last)
	}
}

// TestListLedgerSessionsColonFilter covers the sibling path-construction
// site in ledger_query.go: filtering by a raw colon-bearing session key must
// resolve to the same sanitized directory AppendEvent actually wrote.
func TestListLedgerSessionsColonFilter(t *testing.T) {
	t.Parallel()
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()

	sessionID := "http:main"
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "test.event",
			Timestamp: nowISO(),
			SessionID: sessionID,
		},
		Metadata: EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionID, env); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	sessions, err := listLedgerSessions(root, sessionID)
	if err != nil {
		t.Fatalf("listLedgerSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("listLedgerSessions(%q) = %v, want exactly 1 match", sessionID, sessions)
	}
	want := pathsafe.SanitizeComponent(sessionID)
	if sessions[0] != want {
		t.Fatalf("listLedgerSessions(%q) = %q, want %q", sessionID, sessions[0], want)
	}

	// filepath.Join with the returned ID must resolve to a real file.
	path := filepath.Join(root, ".cog", "ledger", sessions[0], "events.jsonl")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("resolved session dir does not exist: %v", err)
	}
}

// TestAppendEvent_HealsTornTailAndTruncatesOnFailedWrite is k4-followups-review B1: a torn (no trailing
// newline) line fragment from an earlier crash/short-write must not corrupt the NEXT append, and a failed
// write must not itself leave a new torn fragment behind. Two scenarios in one test:
//  1. heal: the file already ends in a torn line (simulating an earlier failure, pre-fix) -- the next
//     successful AppendEvent must isolate its own complete line from that fragment, so a line-by-line JSONL
//     reader (which is what boot replay/GetLastEvent effectively is) can parse the NEW line cleanly even
//     though the OLD fragment before it is garbage.
//  2. truncate-on-error: AppendEvent asked to append to a file it cannot write to (made read-only) returns
//     an error and the file's size does not grow -- no fresh torn fragment is created by the failed attempt
//     itself.
func TestAppendEvent_HealsTornTailAndTruncatesOnFailedWrite(t *testing.T) {
	t.Cleanup(resetLedgerCacheForTest)
	root := t.TempDir()
	sessionID := "b1-heal-session"
	dir := filepath.Join(root, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	eventsFile := filepath.Join(dir, "events.jsonl")

	// Simulate a torn tail left by an earlier failure: a line fragment with NO trailing newline.
	torn := `{"metadata":{"seq":1,"source":"test"},"hashed_payload":{"type":"test.torn","session_id":"` + sessionID + `"`
	if err := os.WriteFile(eventsFile, []byte(torn), 0644); err != nil {
		t.Fatal(err)
	}
	sizeBefore, _ := os.Stat(eventsFile)

	env := &EventEnvelope{
		HashedPayload: EventPayload{Type: "test.after.torn", Timestamp: nowISO(), SessionID: sessionID},
		Metadata:      EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionID, env); err != nil {
		t.Fatalf("AppendEvent after a torn tail: %v", err)
	}

	raw, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected the healing newline to separate the torn fragment from the new line, got %d lines: %q", len(lines), raw)
	}
	last := lines[len(lines)-1]
	var parsed EventEnvelope
	if err := json.Unmarshal([]byte(last), &parsed); err != nil {
		t.Fatalf("the new line must parse cleanly on its own even though the file started with a torn fragment: %v\nline: %q", err, last)
	}
	if parsed.HashedPayload.Type != "test.after.torn" {
		t.Fatalf("parsed wrong event: %+v", parsed)
	}
	sizeAfterHeal, _ := os.Stat(eventsFile)
	if sizeAfterHeal.Size() <= sizeBefore.Size() {
		t.Fatalf("file did not grow after a successful append: before=%d after=%d", sizeBefore.Size(), sizeAfterHeal.Size())
	}

	// Scenario 2: a write that fails must not leave a fresh torn fragment. Make the file read-only so the
	// append's write fails; confirm the size is unchanged afterwards (the failed attempt added nothing).
	if err := os.Chmod(eventsFile, 0444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(eventsFile, 0644) })
	sizeBeforeFail := sizeAfterHeal.Size()
	env2 := &EventEnvelope{
		HashedPayload: EventPayload{Type: "test.should.fail", Timestamp: nowISO(), SessionID: sessionID},
		Metadata:      EventMetadata{Source: "test"},
	}
	if err := AppendEvent(root, sessionID, env2); err == nil {
		t.Skip("this environment lets root/the test user write a 0444 file (e.g. running as root); cannot exercise the write-failure path here")
	}
	sizeAfterFail, statErr := os.Stat(eventsFile)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if sizeAfterFail.Size() != sizeBeforeFail {
		t.Fatalf("a failed append grew the file: before=%d after=%d (should be unchanged, not left with a fresh torn fragment)",
			sizeBeforeFail, sizeAfterFail.Size())
	}
}
