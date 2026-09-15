package cogblock

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// === CANONICAL JSON TESTS ===

func TestCanonicalizeEvent_KeyOrdering(t *testing.T) {
	event1 := &EventPayload{
		Type:      "test.event",
		SessionID: "session-123",
		Timestamp: "2026-01-16T18:30:00Z",
		Data: map[string]interface{}{
			"zebra": "last",
			"alpha": "first",
			"mike":  "middle",
		},
	}

	event2 := &EventPayload{
		Type:      "test.event",
		SessionID: "session-123",
		Timestamp: "2026-01-16T18:30:00Z",
		Data: map[string]interface{}{
			"mike":  "middle",
			"alpha": "first",
			"zebra": "last",
		},
	}

	bytes1, err := CanonicalizeEvent(event1)
	if err != nil {
		t.Fatalf("Failed to canonicalize event1: %v", err)
	}

	bytes2, err := CanonicalizeEvent(event2)
	if err != nil {
		t.Fatalf("Failed to canonicalize event2: %v", err)
	}

	if string(bytes1) != string(bytes2) {
		t.Errorf("Canonical bytes differ:\nEvent1: %s\nEvent2: %s", bytes1, bytes2)
	}

	expected := `{"data":{"alpha":"first","mike":"middle","zebra":"last"},"session_id":"session-123","timestamp":"2026-01-16T18:30:00Z","type":"test.event"}`
	if string(bytes1) != expected {
		t.Errorf("Unexpected canonical form:\nGot:      %s\nExpected: %s", bytes1, expected)
	}
}

func TestCanonicalizeEvent_OptionalFields(t *testing.T) {
	// Without data
	event := &EventPayload{
		Type:      "test.event",
		SessionID: "session-123",
		Timestamp: "2026-01-16T18:30:00Z",
	}

	bytes, err := CanonicalizeEvent(event)
	if err != nil {
		t.Fatalf("Failed to canonicalize: %v", err)
	}

	expected := `{"session_id":"session-123","timestamp":"2026-01-16T18:30:00Z","type":"test.event"}`
	if string(bytes) != expected {
		t.Errorf("Got: %s\nWant: %s", bytes, expected)
	}

	// With prior_hash
	event.PriorHash = "abc123"
	bytes, err = CanonicalizeEvent(event)
	if err != nil {
		t.Fatalf("Failed to canonicalize: %v", err)
	}

	expected = `{"prior_hash":"abc123","session_id":"session-123","timestamp":"2026-01-16T18:30:00Z","type":"test.event"}`
	if string(bytes) != expected {
		t.Errorf("Got: %s\nWant: %s", bytes, expected)
	}
}

// === HASHING TESTS ===

func TestHashEvent_SHA256(t *testing.T) {
	payload := []byte(`{"session_id":"session-123","timestamp":"2026-01-16T18:30:00Z","type":"test.event"}`)

	hash, err := HashEvent(payload, "sha256")
	if err != nil {
		t.Fatalf("Failed to hash: %v", err)
	}

	if len(hash) != 64 {
		t.Errorf("Expected 64-char hash, got %d chars: %s", len(hash), hash)
	}

	// Deterministic
	hash2, _ := HashEvent(payload, "sha256")
	if hash != hash2 {
		t.Errorf("Non-deterministic hash: %s vs %s", hash, hash2)
	}
}

func TestHashEvent_SHA512(t *testing.T) {
	payload := []byte(`{"type":"test"}`)

	hash, err := HashEvent(payload, "sha512")
	if err != nil {
		t.Fatalf("Failed to hash: %v", err)
	}

	if len(hash) != 128 {
		t.Errorf("Expected 128-char hash, got %d chars", len(hash))
	}
}

func TestHashEvent_DefaultIsSHA256(t *testing.T) {
	payload := []byte(`{"type":"test"}`)

	h1, _ := HashEvent(payload, "")
	h2, _ := HashEvent(payload, "sha256")

	if h1 != h2 {
		t.Errorf("Default differs from sha256: %s vs %s", h1, h2)
	}
}

func TestHashEvent_UnsupportedAlgorithm(t *testing.T) {
	_, err := HashEvent([]byte("x"), "md5")
	if err == nil {
		t.Error("Expected error for unsupported algorithm")
	}
}

// === ENVELOPE CONSTRUCTOR TESTS ===

func TestNewEventEnvelope(t *testing.T) {
	before := time.Now().UTC()
	env := NewEventEnvelope("test.event", "session-42")
	after := time.Now().UTC()

	if env.HashedPayload.Type != "test.event" {
		t.Errorf("Type = %q; want test.event", env.HashedPayload.Type)
	}
	if env.HashedPayload.SessionID != "session-42" {
		t.Errorf("SessionID = %q; want session-42", env.HashedPayload.SessionID)
	}

	ts, err := time.Parse(time.RFC3339Nano, env.HashedPayload.Timestamp)
	if err != nil {
		t.Fatalf("Bad timestamp: %v", err)
	}
	if ts.Before(before) || ts.After(after) {
		t.Errorf("Timestamp %v not between %v and %v", ts, before, after)
	}

	if env.HashedPayload.Data == nil {
		t.Error("Data map should be initialized")
	}
}

func TestEventEnvelope_WithData(t *testing.T) {
	env := NewEventEnvelope("test", "s1")
	env.WithData("key1", "value1").WithData("key2", 42)

	if env.HashedPayload.Data["key1"] != "value1" {
		t.Errorf("key1 = %v; want value1", env.HashedPayload.Data["key1"])
	}
	if env.HashedPayload.Data["key2"] != 42 {
		t.Errorf("key2 = %v; want 42", env.HashedPayload.Data["key2"])
	}
}

func TestEventEnvelope_WithSource(t *testing.T) {
	env := NewEventEnvelope("test", "s1").WithSource("kernel")

	if env.Metadata.Source != "kernel" {
		t.Errorf("Source = %q; want kernel", env.Metadata.Source)
	}
}

// === APPEND & VERIFY INTEGRATION TESTS ===

func TestAppendEvent_FirstEvent(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "test-session-001"

	event := NewEventEnvelope("test.event", sessionID)
	event.WithData("message", "Hello, world!")

	err := AppendEvent(tmpDir, sessionID, event)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	eventsFile := filepath.Join(tmpDir, ".cog", "ledger", sessionID, "events.jsonl")
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatalf("Failed to read events file: %v", err)
	}

	var written EventEnvelope
	if err := json.Unmarshal(data[:len(data)-1], &written); err != nil { // trim trailing newline
		t.Fatalf("Failed to parse event: %v", err)
	}

	if written.Metadata.Seq != 1 {
		t.Errorf("Expected seq=1, got %d", written.Metadata.Seq)
	}
	if written.HashedPayload.PriorHash != "" {
		t.Errorf("First event should have empty prior_hash, got %s", written.HashedPayload.PriorHash)
	}
	if written.Metadata.Hash == "" {
		t.Error("Event hash was not computed")
	}
}

func TestAppendEvent_HashChaining(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "test-session-002"

	for i := 1; i <= 3; i++ {
		event := NewEventEnvelope("test.event", sessionID)
		event.WithData("number", i)
		if err := AppendEvent(tmpDir, sessionID, event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	eventsFile := filepath.Join(tmpDir, ".cog", "ledger", sessionID, "events.jsonl")
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	lines := splitNonEmpty(string(data))
	if len(lines) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(lines))
	}

	var events []*EventEnvelope
	for _, line := range lines {
		var e EventEnvelope
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("Failed to parse event: %v", err)
		}
		events = append(events, &e)
	}

	// Verify chain linkage
	if events[0].HashedPayload.PriorHash != "" {
		t.Errorf("Event 0: expected empty prior_hash")
	}
	if events[1].HashedPayload.PriorHash != events[0].Metadata.Hash {
		t.Errorf("Event 1: prior_hash mismatch")
	}
	if events[2].HashedPayload.PriorHash != events[1].Metadata.Hash {
		t.Errorf("Event 2: prior_hash mismatch")
	}
}

func TestVerifyLedger_ValidChain(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "test-session-003"

	genesis := NewEventEnvelope("workspace.genesis", sessionID)
	genesis.WithData("hash_algorithm", "sha256")
	if err := AppendEvent(tmpDir, sessionID, genesis); err != nil {
		t.Fatalf("Failed to append genesis: %v", err)
	}

	for i := 1; i <= 5; i++ {
		event := NewEventEnvelope("test.event", sessionID)
		event.WithData("index", i)
		if err := AppendEvent(tmpDir, sessionID, event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	if err := VerifyLedger(tmpDir, sessionID); err != nil {
		t.Errorf("Verification failed for valid chain: %v", err)
	}
}

func TestVerifyLedger_DetectsTampering(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "test-session-004"

	genesis := NewEventEnvelope("workspace.genesis", sessionID)
	genesis.WithData("hash_algorithm", "sha256")
	if err := AppendEvent(tmpDir, sessionID, genesis); err != nil {
		t.Fatal(err)
	}

	event := NewEventEnvelope("test.event", sessionID)
	event.WithData("value", "original")
	if err := AppendEvent(tmpDir, sessionID, event); err != nil {
		t.Fatal(err)
	}

	// Tamper with the second event
	eventsFile := filepath.Join(tmpDir, ".cog", "ledger", sessionID, "events.jsonl")
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatal(err)
	}

	lines := splitNonEmpty(string(data))
	var tampered EventEnvelope
	json.Unmarshal([]byte(lines[1]), &tampered)
	tampered.HashedPayload.Data["value"] = "TAMPERED"
	tamperedJSON, _ := json.Marshal(&tampered)
	lines[1] = string(tamperedJSON)

	os.WriteFile(eventsFile, []byte(strings.Join(lines, "\n")+"\n"), 0644)

	err = VerifyLedger(tmpDir, sessionID)
	if err == nil {
		t.Error("Verification should have detected tampering")
	}
}

// === HELPERS ===

func splitNonEmpty(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// === RFC-0003 REFINEMENT 4: CANON FORM TESTS ===

// TestCanonicalizeEvent_CanonForm_IncludedInHash verifies that when CanonForm is
// set, it appears in the canonical bytes and therefore changes the hash.
func TestCanonicalizeEvent_CanonForm_IncludedInHash(t *testing.T) {
	base := &EventPayload{
		Type:      "test.event",
		SessionID: "session-r4",
		Timestamp: "2026-05-19T10:00:00Z",
	}

	withCanonForm := &EventPayload{
		Type:      "test.event",
		SessionID: "session-r4",
		Timestamp: "2026-05-19T10:00:00Z",
		CanonForm: CanonFormRFC8785V1,
	}

	bytesBase, err := CanonicalizeEvent(base)
	if err != nil {
		t.Fatalf("CanonicalizeEvent(base): %v", err)
	}

	bytesWithCanon, err := CanonicalizeEvent(withCanonForm)
	if err != nil {
		t.Fatalf("CanonicalizeEvent(withCanonForm): %v", err)
	}

	// The canonical bytes must differ because canon_form is included.
	if string(bytesBase) == string(bytesWithCanon) {
		t.Errorf("Expected canonical bytes to differ when CanonForm is set, but they are identical:\n%s", bytesBase)
	}

	// The canon_form key must appear in the output.
	if !strings.Contains(string(bytesWithCanon), `"canon_form"`) {
		t.Errorf("canon_form not found in canonical bytes: %s", bytesWithCanon)
	}
	if !strings.Contains(string(bytesWithCanon), CanonFormRFC8785V1) {
		t.Errorf("canon_form value %q not found in canonical bytes: %s", CanonFormRFC8785V1, bytesWithCanon)
	}
}

// TestCanonicalizeEvent_CanonForm_AbsentOnLegacy verifies that EventPayload with
// empty CanonForm produces canonical bytes without the canon_form key — the
// backward-compatibility guarantee for pre-R4 ledger events.
func TestCanonicalizeEvent_CanonForm_AbsentOnLegacy(t *testing.T) {
	legacy := &EventPayload{
		Type:      "test.event",
		SessionID: "session-legacy",
		Timestamp: "2026-05-19T10:00:00Z",
	}

	bytes, err := CanonicalizeEvent(legacy)
	if err != nil {
		t.Fatalf("CanonicalizeEvent: %v", err)
	}

	if strings.Contains(string(bytes), "canon_form") {
		t.Errorf("canon_form must not appear in canonical bytes for legacy event (empty CanonForm): %s", bytes)
	}
}

// TestCanonicalizeEvent_CanonForm_ExactBytes verifies the exact canonical form
// of a new event with CanonForm set. This pins the expected output so that any
// future canonicalization change is immediately visible.
func TestCanonicalizeEvent_CanonForm_ExactBytes(t *testing.T) {
	event := &EventPayload{
		Type:      "test.event",
		SessionID: "session-r4",
		Timestamp: "2026-05-19T10:00:00Z",
		CanonForm: CanonFormRFC8785V1,
	}

	bytes, err := CanonicalizeEvent(event)
	if err != nil {
		t.Fatalf("CanonicalizeEvent: %v", err)
	}

	// Keys must be lexicographically sorted: canon_form < session_id < timestamp < type
	expected := `{"canon_form":"rfc8785-v1","session_id":"session-r4","timestamp":"2026-05-19T10:00:00Z","type":"test.event"}`
	if string(bytes) != expected {
		t.Errorf("Unexpected canonical form:\nGot:  %s\nWant: %s", bytes, expected)
	}
}

// TestNewEventEnvelope_DefaultsCanonForm verifies that NewEventEnvelope sets
// CanonForm to CanonFormRFC8785V1 on all new events.
func TestNewEventEnvelope_DefaultsCanonForm(t *testing.T) {
	env := NewEventEnvelope("test.event", "session-42")

	if env.HashedPayload.CanonForm != CanonFormRFC8785V1 {
		t.Errorf("CanonForm = %q; want %q", env.HashedPayload.CanonForm, CanonFormRFC8785V1)
	}
}

// TestCanonForm_NewEventHashesDifferFromLegacy verifies that a new event
// (CanonForm set) produces a different hash from a logically equivalent legacy
// event (CanonForm empty). This is intentional — the declared algorithm is part
// of the hash commitment.
func TestCanonForm_NewEventHashesDifferFromLegacy(t *testing.T) {
	newEvent := &EventPayload{
		Type:      "test.event",
		SessionID: "session-test",
		Timestamp: "2026-05-19T10:00:00Z",
		CanonForm: CanonFormRFC8785V1,
	}

	legacyEvent := &EventPayload{
		Type:      "test.event",
		SessionID: "session-test",
		Timestamp: "2026-05-19T10:00:00Z",
		// CanonForm intentionally empty — simulates a pre-R4 ledger event.
	}

	newBytes, err := CanonicalizeEvent(newEvent)
	if err != nil {
		t.Fatalf("CanonicalizeEvent(new): %v", err)
	}

	legacyBytes, err := CanonicalizeEvent(legacyEvent)
	if err != nil {
		t.Fatalf("CanonicalizeEvent(legacy): %v", err)
	}

	newHash, _ := HashEvent(newBytes, "sha256")
	legacyHash, _ := HashEvent(legacyBytes, "sha256")

	if newHash == legacyHash {
		t.Errorf("Expected new-event hash to differ from legacy-event hash (CanonForm is part of commitment)")
	}
}

// TestAppendEvent_CanonFormPersistedAndVerifiable verifies that a full
// ledger round-trip with the new default CanonForm passes VerifyLedger.
func TestAppendEvent_CanonFormPersistedAndVerifiable(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "test-session-r4"

	// Genesis event sets hash algorithm.
	genesis := NewEventEnvelope("workspace.genesis", sessionID)
	genesis.WithData("hash_algorithm", "sha256")
	if err := AppendEvent(tmpDir, sessionID, genesis); err != nil {
		t.Fatalf("AppendEvent genesis: %v", err)
	}

	// Subsequent events with CanonForm set by default.
	for i := 1; i <= 3; i++ {
		event := NewEventEnvelope("test.event", sessionID)
		event.WithData("index", i)
		if err := AppendEvent(tmpDir, sessionID, event); err != nil {
			t.Fatalf("AppendEvent %d: %v", i, err)
		}
	}

	// All events must have CanonForm in their payload.
	eventsFile := filepath.Join(tmpDir, ".cog", "ledger", sessionID, "events.jsonl")
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	lines := splitNonEmpty(string(data))
	for i, line := range lines {
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("Unmarshal line %d: %v", i, err)
		}
		if env.HashedPayload.CanonForm != CanonFormRFC8785V1 {
			t.Errorf("line %d: CanonForm = %q; want %q", i, env.HashedPayload.CanonForm, CanonFormRFC8785V1)
		}
	}

	// VerifyLedger must pass — hash chain must be intact with CanonForm included.
	if err := VerifyLedger(tmpDir, sessionID); err != nil {
		t.Errorf("VerifyLedger: %v", err)
	}
}

// TestCogBlock_CanonFormRoundTrip verifies that CanonForm survives JSON
// marshal/unmarshal on CogBlock and that omitempty works correctly.
func TestCogBlock_CanonFormRoundTrip(t *testing.T) {
	t.Run("with CanonForm", func(t *testing.T) {
		b := CogBlock{
			ID:        "block-r4",
			Kind:      BlockMessage,
			CanonForm: CanonFormRFC8785V1,
		}

		data, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}

		if !strings.Contains(string(data), `"canon_form"`) {
			t.Errorf("canon_form missing from JSON: %s", data)
		}

		var decoded CogBlock
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}

		if decoded.CanonForm != CanonFormRFC8785V1 {
			t.Errorf("CanonForm = %q; want %q", decoded.CanonForm, CanonFormRFC8785V1)
		}
	})

	t.Run("without CanonForm (omitempty)", func(t *testing.T) {
		b := CogBlock{
			ID:   "block-legacy",
			Kind: BlockMessage,
			// CanonForm intentionally omitted
		}

		data, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}

		if strings.Contains(string(data), "canon_form") {
			t.Errorf("canon_form should be omitted when empty: %s", data)
		}

		var decoded CogBlock
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}

		if decoded.CanonForm != "" {
			t.Errorf("CanonForm = %q; want empty string for legacy block", decoded.CanonForm)
		}
	})
}

// TestCanonFormRFC8785V1_ConstantValue pins the constant value so any
// accidental rename or edit fails loudly.
func TestCanonFormRFC8785V1_ConstantValue(t *testing.T) {
	if CanonFormRFC8785V1 != "rfc8785-v1" {
		t.Errorf("CanonFormRFC8785V1 = %q; want %q", CanonFormRFC8785V1, "rfc8785-v1")
	}
}

// TestAppendEvent_SanitizesColonSessionID covers myrgic/cogos#489 for this
// package's own AppendEvent/GetLastEvent/VerifyLedger — a sibling copy of
// the engine's ledger with the identical path-construction seam. A
// colon-bearing session key (the "origin:agent" shape used for
// channel-scoped sessions, e.g. "http:cog") must produce an NTFS-legal
// directory name and still round-trip correctly.
func TestAppendEvent_SanitizesColonSessionID(t *testing.T) {
	tmpDir := t.TempDir()
	sessionID := "http:cog"

	event := NewEventEnvelope("test.event", sessionID)
	event.WithData("message", "hello")
	if err := AppendEvent(tmpDir, sessionID, event); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	ledgerRoot := filepath.Join(tmpDir, ".cog", "ledger")
	entries, err := os.ReadDir(ledgerRoot)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger root has %d entries, want 1: %+v", len(entries), entries)
	}
	if strings.Contains(entries[0].Name(), ":") {
		t.Fatalf("on-disk session dir %q still contains a colon", entries[0].Name())
	}

	last, err := GetLastEvent(tmpDir, sessionID)
	if err != nil {
		t.Fatalf("GetLastEvent: %v", err)
	}
	if last == nil || last.HashedPayload.SessionID != sessionID {
		t.Fatalf("GetLastEvent round-trip failed: got %+v", last)
	}

	if err := VerifyLedger(tmpDir, sessionID); err != nil {
		t.Fatalf("VerifyLedger: %v", err)
	}
}

// === scanForGenesisAlgorithm ERROR PROPAGATION (cogos#541) ===

// TestScanForGenesisAlgorithm_PropagatesScanError verifies that a scan
// failure (e.g. a line exceeding bufio's max token size) is surfaced as an
// error instead of being silently treated as "no genesis event found".
func TestScanForGenesisAlgorithm_PropagatesScanError(t *testing.T) {
	// A single line longer than bufio.MaxScanTokenSize forces scanner.Scan()
	// to fail with bufio.ErrTooLong.
	oversized := "{\"pad\":\"" + strings.Repeat("x", bufio.MaxScanTokenSize+1) + "\"}\n"

	_, found, err := scanForGenesisAlgorithm(strings.NewReader(oversized))
	if err == nil {
		t.Fatalf("scanForGenesisAlgorithm: want non-nil error for oversized line, got nil (found=%v)", found)
	}
	if found {
		t.Fatalf("scanForGenesisAlgorithm: found=true on an aborted scan, want false")
	}
}

// TestScanForGenesisAlgorithm_CleanEOFNoError verifies the non-error path is
// unaffected: a stream with no genesis event still reports found=false, err=nil.
func TestScanForGenesisAlgorithm_CleanEOFNoError(t *testing.T) {
	alg, found, err := scanForGenesisAlgorithm(strings.NewReader(`{"hashed_payload":{"type":"tool.call"}}` + "\n"))
	if err != nil {
		t.Fatalf("scanForGenesisAlgorithm: unexpected error on clean EOF: %v", err)
	}
	if found {
		t.Fatalf("scanForGenesisAlgorithm: found=true, want false (no genesis event in stream); alg=%q", alg)
	}
}

// TestGetHashAlgorithm_PropagatesScanError verifies GetHashAlgorithm does not
// mask a scan abort as "no workspace.genesis event found" — it must return
// an error distinguishable from the clean not-found case.
func TestGetHashAlgorithm_PropagatesScanError(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, ".cog", "ledger", "sess-oversized")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	oversized := "{\"pad\":\"" + strings.Repeat("x", bufio.MaxScanTokenSize+1) + "\"}\n"
	eventsFile := filepath.Join(sessionDir, "events.jsonl")
	if err := os.WriteFile(eventsFile, []byte(oversized), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := GetHashAlgorithm(tmpDir)
	if err == nil {
		t.Fatalf("GetHashAlgorithm: want error when a session's ledger fails to scan, got nil")
	}
	if strings.Contains(err.Error(), "no workspace.genesis event found") {
		t.Fatalf("GetHashAlgorithm: scan-abort error was masked as not-found: %v", err)
	}
}
