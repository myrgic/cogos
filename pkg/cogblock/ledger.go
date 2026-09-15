package cogblock

import (
	"bufio"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdhash "hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// === EVENT ENVELOPE ===

// EventEnvelope represents the canonical event structure with hash chaining.
// This is the spine of the ledger — every event is hashed and chained.
type EventEnvelope struct {
	// HashedPayload contains the canonical JSON that gets hashed.
	HashedPayload EventPayload `json:"hashed_payload"`

	// Metadata is NOT included in the hash (for extensibility).
	Metadata EventMetadata `json:"metadata,omitempty"`
}

// EventPayload is the content that gets canonicalized and hashed.
type EventPayload struct {
	// Type of event (e.g., "workspace.genesis", "tool.call", "validation.success").
	Type string `json:"type"`

	// Timestamp when the event occurred (RFC3339 format).
	Timestamp string `json:"timestamp"`

	// SessionID that produced this event.
	SessionID string `json:"session_id"`

	// PriorHash chains to the previous event (empty for genesis).
	PriorHash string `json:"prior_hash,omitempty"`

	// Data contains type-specific payload (optional).
	Data map[string]interface{} `json:"data,omitempty"`

	// CanonForm identifies the canonicalization algorithm used to produce the
	// canonical bytes that are hashed for this event. RFC-0003 Refinement 4.
	//
	// New events default to CanonFormRFC8785V1 ("rfc8785-v1"). Existing events
	// without this field are treated as "rfc8785-v1" at read time — CanonForm
	// is omitempty so the wire format is unchanged for legacy events.
	//
	// When a future "rfc8785-v2" is introduced (e.g., to fix a Unicode
	// normalization edge case), events emitted under the new algorithm declare
	// "rfc8785-v2" here; old and new events coexist in the same ledger without
	// ambiguity. The CanonForm value is itself included in the canonical bytes,
	// so the declared algorithm is part of the hash commitment.
	CanonForm string `json:"canon_form,omitempty"`
}

// EventMetadata contains information NOT included in the hash.
type EventMetadata struct {
	// Hash of the canonical hashed_payload (computed during append).
	Hash string `json:"hash,omitempty"`

	// Seq is the sequence number within the session (computed during append).
	Seq int64 `json:"seq,omitempty"`

	// Source identifies what produced the event (e.g., "kernel", "hook", "cog-chat").
	Source string `json:"source,omitempty"`
}

// === CANONICALIZATION (RFC 8785) ===

// CanonicalizeEvent produces RFC 8785 canonical JSON for an event payload.
// This ensures deterministic hashing: same logical content -> same bytes.
//
// RFC 8785 rules:
//   - Object keys sorted lexicographically
//   - No insignificant whitespace
//   - Unicode normalized (NFC)
//   - Numbers in canonical form (no leading zeros, etc.)
//
// RFC-0003 Refinement 4: if payload.CanonForm is set (non-empty), it is
// included in the canonical map under the key "canon_form". This commits the
// declared canonicalization algorithm into the hash, making the hash dependent
// on which algorithm was used. For new events this is always "rfc8785-v1".
// Legacy events without CanonForm omit the field from the map (backward
// compatible — their hashes are unchanged).
func CanonicalizeEvent(payload *EventPayload) ([]byte, error) {
	// Convert to map for canonical JSON encoding
	data := map[string]interface{}{
		"type":       payload.Type,
		"timestamp":  payload.Timestamp,
		"session_id": payload.SessionID,
	}

	// Add optional fields only if present
	if payload.PriorHash != "" {
		data["prior_hash"] = payload.PriorHash
	}
	if len(payload.Data) > 0 {
		data["data"] = payload.Data
	}

	// RFC-0003 R4: include canon_form in hash when declared. Absent on legacy
	// events (omitempty) — their canonical bytes and hashes are unchanged.
	if payload.CanonForm != "" {
		data["canon_form"] = payload.CanonForm
	}

	return canonicalJSON(data)
}

// canonicalJSON implements RFC 8785 canonical JSON encoding.
func canonicalJSON(v interface{}) ([]byte, error) {
	switch value := v.(type) {
	case map[string]interface{}:
		// Sort keys lexicographically
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Build canonical object
		var parts []string
		for _, k := range keys {
			keyJSON, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			valJSON, err := canonicalJSON(value[k])
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(keyJSON)+":"+string(valJSON))
		}
		return []byte("{" + strings.Join(parts, ",") + "}"), nil

	case []interface{}:
		// Arrays preserve order
		var parts []string
		for _, item := range value {
			itemJSON, err := canonicalJSON(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(itemJSON))
		}
		return []byte("[" + strings.Join(parts, ",") + "]"), nil

	case json.RawMessage:
		// Raw JSON captured verbatim at write time (e.g. tool.call arguments)
		// decodes to map[string]interface{}/[]interface{} on read, which would
		// otherwise be re-sorted/reprocessed and hash differently than the
		// verbatim bytes hashed here. Decode and recurse so write-time and
		// read-time canonicalization agree.
		var decoded interface{}
		if err := json.Unmarshal(value, &decoded); err != nil {
			return nil, err
		}
		return canonicalJSON(decoded)

	default:
		// Primitives: use standard JSON encoding
		return json.Marshal(v)
	}
}

// === HASHING ===

// HashEvent computes the hash of canonical event bytes using the specified algorithm.
// Supported algorithms: "sha256" (default), "sha512".
func HashEvent(canonicalBytes []byte, algorithm string) (string, error) {
	var h stdhash.Hash

	switch algorithm {
	case "", "sha256":
		h = sha256.New()
	case "sha512":
		h = sha512.New()
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}

	h.Write(canonicalBytes)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// === LEDGER OPERATIONS ===

// AppendEvent appends an event to a session ledger with hash chaining.
// This is the canonical write path for all events.
//
// The ledger is stored at <workspaceRoot>/.cog/ledger/<sessionID>/events.jsonl.
func AppendEvent(workspaceRoot, sessionID string, envelope *EventEnvelope) error {
	ledgerDir := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID))
	eventsFile := filepath.Join(ledgerDir, "events.jsonl")

	// Ensure ledger directory exists
	if err := os.MkdirAll(ledgerDir, 0755); err != nil {
		return fmt.Errorf("failed to create ledger directory: %w", err)
	}

	// Get hash algorithm from workspace genesis event
	hashAlg, err := GetHashAlgorithm(workspaceRoot)
	if err != nil {
		// Default to sha256 if no genesis found
		hashAlg = "sha256"
	}

	// Compute sequence number and chain hash
	lastEvent, err := GetLastEvent(workspaceRoot, sessionID)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to get last event: %w", err)
	}

	// Set sequence number
	if lastEvent != nil {
		envelope.Metadata.Seq = lastEvent.Metadata.Seq + 1
	} else {
		envelope.Metadata.Seq = 1
	}

	// Set prior_hash from last event's hash
	if lastEvent != nil {
		envelope.HashedPayload.PriorHash = lastEvent.Metadata.Hash
	}

	// Canonicalize payload
	canonicalBytes, err := CanonicalizeEvent(&envelope.HashedPayload)
	if err != nil {
		return fmt.Errorf("failed to canonicalize event: %w", err)
	}

	// Compute hash
	eventHash, err := HashEvent(canonicalBytes, hashAlg)
	if err != nil {
		return fmt.Errorf("failed to hash event: %w", err)
	}
	envelope.Metadata.Hash = eventHash

	// Append to JSONL file
	f, err := os.OpenFile(eventsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open events file: %w", err)
	}
	defer f.Close()

	// Write event as JSON line
	eventJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := f.Write(append(eventJSON, '\n')); err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}

	return nil
}

// GetLastEvent retrieves the last event from a session ledger.
func GetLastEvent(workspaceRoot, sessionID string) (*EventEnvelope, error) {
	eventsFile := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID), "events.jsonl")

	f, err := os.Open(eventsFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lastEvent *EventEnvelope
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var event EventEnvelope
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue // Skip malformed events
		}
		lastEvent = &event
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if lastEvent == nil {
		return nil, os.ErrNotExist
	}

	return lastEvent, nil
}

// GetHashAlgorithm retrieves the hash algorithm from the workspace genesis event.
func GetHashAlgorithm(workspaceRoot string) (string, error) {
	// Look for workspace genesis event in .cog/ledger/
	ledgerDir := filepath.Join(workspaceRoot, ".cog", "ledger")

	entries, err := os.ReadDir(ledgerDir)
	if err != nil {
		return "", err
	}

	// Search for genesis event in any session
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		eventsFile := filepath.Join(ledgerDir, entry.Name(), "events.jsonl")
		f, err := os.Open(eventsFile)
		if err != nil {
			continue
		}

		alg, found := scanForGenesisAlgorithm(f)
		f.Close()
		if found {
			return alg, nil
		}
	}

	return "", fmt.Errorf("no workspace.genesis event found")
}

// scanForGenesisAlgorithm reads a JSONL stream looking for a workspace.genesis event
// and returns its hash_algorithm if found.
func scanForGenesisAlgorithm(r io.Reader) (string, bool) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var event EventEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}

		if event.HashedPayload.Type == "workspace.genesis" {
			if alg, ok := event.HashedPayload.Data["hash_algorithm"].(string); ok {
				return alg, true
			}
		}
	}
	return "", false
}

// === VERIFICATION ===

// VerifyLedger verifies the hash chain integrity for a session ledger.
// Returns error if any hash is invalid or chain is broken.
func VerifyLedger(workspaceRoot, sessionID string) error {
	eventsFile := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID), "events.jsonl")

	f, err := os.Open(eventsFile)
	if err != nil {
		return fmt.Errorf("failed to open events file: %w", err)
	}
	defer f.Close()

	// Get hash algorithm
	hashAlg, err := GetHashAlgorithm(workspaceRoot)
	if err != nil {
		hashAlg = "sha256" // Default
	}

	var prevHash string
	var lineNum int

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if strings.TrimSpace(line) == "" {
			continue
		}

		var event EventEnvelope
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return fmt.Errorf("line %d: malformed event: %w", lineNum, err)
		}

		// Verify hash chain
		if event.HashedPayload.PriorHash != prevHash {
			return fmt.Errorf("line %d: broken hash chain: expected prior_hash=%s, got %s",
				lineNum, prevHash, event.HashedPayload.PriorHash)
		}

		// Recompute hash and verify
		canonicalBytes, err := CanonicalizeEvent(&event.HashedPayload)
		if err != nil {
			return fmt.Errorf("line %d: failed to canonicalize: %w", lineNum, err)
		}

		computedHash, err := HashEvent(canonicalBytes, hashAlg)
		if err != nil {
			return fmt.Errorf("line %d: failed to hash: %w", lineNum, err)
		}

		if computedHash != event.Metadata.Hash {
			return fmt.Errorf("line %d: hash mismatch: expected %s, got %s",
				lineNum, event.Metadata.Hash, computedHash)
		}

		prevHash = event.Metadata.Hash
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading events: %w", err)
	}

	return nil
}

// === CONVENIENCE CONSTRUCTORS ===

// NewEventEnvelope creates a new event envelope with current timestamp.
// The CanonForm field defaults to CanonFormRFC8785V1 ("rfc8785-v1") per
// RFC-0003 Refinement 4 — all new events declare their canonicalization
// algorithm so future algorithm migrations are unambiguous.
func NewEventEnvelope(eventType, sessionID string) *EventEnvelope {
	return &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			SessionID: sessionID,
			Data:      make(map[string]interface{}),
			CanonForm: CanonFormRFC8785V1,
		},
		Metadata: EventMetadata{},
	}
}

// WithData adds data to the event payload.
func (e *EventEnvelope) WithData(key string, value interface{}) *EventEnvelope {
	if e.HashedPayload.Data == nil {
		e.HashedPayload.Data = make(map[string]interface{})
	}
	e.HashedPayload.Data[key] = value
	return e
}

// WithSource adds source metadata.
func (e *EventEnvelope) WithSource(source string) *EventEnvelope {
	e.Metadata.Source = source
	return e
}
