// ledger.go — CogOS v3 hash-chained event ledger
//
// Ported from apps/cogos/ledger_core.go (v2.4.0).
// CLI command functions removed; EventEnvelope, hash chain, and append logic preserved.
//
// Every significant cognitive event is recorded as an append-only JSONL entry.
// Entries are hash-chained (RFC 8785 canonical JSON + SHA-256) to provide
// tamper-evidence and causal ordering.
package engine

import (
	"bufio"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdhash "hash"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// appendMu serializes concurrent AppendEvent calls to the same session.
// A per-session mutex would be more granular but a global one is safe for stage 1.
var appendMu sync.Mutex

// lastEventCache caches the last event per session to avoid re-scanning the
// entire ledger file on every append. Populated on first access and updated
// after each successful append.
var lastEventCache = struct {
	mu        sync.RWMutex
	bySession map[string]*EventEnvelope
}{bySession: make(map[string]*EventEnvelope)}

// EventEnvelope is the canonical on-disk event shape.
type EventEnvelope struct {
	HashedPayload EventPayload  `json:"hashed_payload"`
	Metadata      EventMetadata `json:"metadata,omitempty"`
}

// EventPayload is the content that gets canonicalized and hashed.
type EventPayload struct {
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	SessionID string                 `json:"session_id"`
	PriorHash string                 `json:"prior_hash,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// EventMetadata is NOT included in the hash (for extensibility).
type EventMetadata struct {
	Hash   string `json:"hash,omitempty"`
	Seq    int64  `json:"seq,omitempty"`
	Source string `json:"source,omitempty"`
}

// nowISO returns the current time as an RFC3339 string.
func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// CanonicalizeEvent produces RFC 8785 canonical JSON for an event payload.
// Same logical content always produces the same bytes.
func CanonicalizeEvent(payload *EventPayload) ([]byte, error) {
	data := map[string]interface{}{
		"type":       payload.Type,
		"timestamp":  payload.Timestamp,
		"session_id": payload.SessionID,
	}
	if payload.PriorHash != "" {
		data["prior_hash"] = payload.PriorHash
	}
	if len(payload.Data) > 0 {
		data["data"] = payload.Data
	}
	return canonicalJSON(data)
}

// canonicalJSON is a minimal RFC 8785 implementation (sorted keys, no whitespace).
func canonicalJSON(v interface{}) ([]byte, error) {
	switch value := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			kj, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			vj, err := canonicalJSON(value[k])
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(kj)+":"+string(vj))
		}
		return []byte("{" + strings.Join(parts, ",") + "}"), nil
	case []interface{}:
		var parts []string
		for _, item := range value {
			ij, err := canonicalJSON(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(ij))
		}
		return []byte("[" + strings.Join(parts, ",") + "]"), nil
	default:
		return json.Marshal(v)
	}
}

// HashEvent computes the hash of canonical bytes using the given algorithm.
// Supported: "sha256" (default), "sha512".
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

// AppendEvent appends an event to the process ledger with hash chaining.
// The ledger lives at .cog/ledger/{sessionID}/events.jsonl.
// Safe for concurrent callers (serialized via appendMu).
//
// Uses an in-memory cache for the last event per session, turning the previous
// O(N) file scan per append into O(1) after the first access.
func AppendEvent(workspaceRoot, sessionID string, envelope *EventEnvelope) error {
	appendMu.Lock()
	defer appendMu.Unlock()
	ledgerDir := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID))
	eventsFile := filepath.Join(ledgerDir, "events.jsonl")

	if err := os.MkdirAll(ledgerDir, 0755); err != nil {
		return fmt.Errorf("create ledger dir: %w", err)
	}

	hashAlg := GetHashAlgorithm(workspaceRoot)

	// Try cache first, fall back to file scan only on first access.
	lastEvent := getCachedLastEvent(sessionID)
	if lastEvent == nil {
		var err error
		lastEvent, err = GetLastEvent(workspaceRoot, sessionID)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("get last event: %w", err)
		}
	}

	if lastEvent != nil {
		envelope.Metadata.Seq = lastEvent.Metadata.Seq + 1
		envelope.HashedPayload.PriorHash = lastEvent.Metadata.Hash
	} else {
		envelope.Metadata.Seq = 1
		if prior, _ := GetLastGlobalEvent(workspaceRoot, sessionID); prior != nil {
			envelope.HashedPayload.PriorHash = prior.Metadata.Hash
		}
	}

	canonical, err := CanonicalizeEvent(&envelope.HashedPayload)
	if err != nil {
		return fmt.Errorf("canonicalize: %w", err)
	}

	eventHash, err := HashEvent(canonical, hashAlg)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	envelope.Metadata.Hash = eventHash

	line, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	f, err := os.OpenFile(eventsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer f.Close()

	if _, err = fmt.Fprintf(f, "%s\n", line); err != nil {
		return err
	}

	// Update cache after successful write.
	setCachedLastEvent(sessionID, envelope)

	// Publish to every registered broker so live subscribers see this
	// event without needing to tail the JSONL. Publish is fire-and-forget:
	// a slow consumer cannot hold up the ledger (source of truth). When no
	// broker is installed (unit tests), the snapshot is nil and we no-op.
	for _, broker := range brokerSnapshot() {
		broker.Publish(envelope)
	}
	return nil
}

func getCachedLastEvent(sessionID string) *EventEnvelope {
	lastEventCache.mu.RLock()
	defer lastEventCache.mu.RUnlock()
	return lastEventCache.bySession[sessionID]
}

// resetLedgerCacheForTest clears the package-level lastEventCache. Only intended
// for tests: when -count>=2 reuses the same sessionID across fresh workspace
// roots, the cache from the prior iteration's root leaks into the new one and
// computes a wrong prior_hash (breaking chain-integrity assertions). Callers
// should invoke this via t.Cleanup at the start of any test that pins a
// sessionID literal.
func resetLedgerCacheForTest() {
	lastEventCache.mu.Lock()
	defer lastEventCache.mu.Unlock()
	lastEventCache.bySession = make(map[string]*EventEnvelope)
}

func setCachedLastEvent(sessionID string, env *EventEnvelope) {
	lastEventCache.mu.Lock()
	defer lastEventCache.mu.Unlock()
	// Store a copy to prevent mutation.
	cp := *env
	lastEventCache.bySession[sessionID] = &cp
}

// GetLastEvent returns the last event in a session ledger, or nil if empty.
func GetLastEvent(workspaceRoot, sessionID string) (*EventEnvelope, error) {
	eventsFile := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID), "events.jsonl")

	f, err := os.Open(eventsFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var last *EventEnvelope
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			continue
		}
		last = &env
	}
	return last, scanner.Err()
}

// GetLastGlobalEvent returns the last event from the most-recently-modified
// session in .cog/ledger/ that is NOT currentSessionID.
// Used on process startup to chain the new session's genesis event to the
// previous session's final event, maintaining a continuous cross-session ledger.
// Returns nil (without error) if there is no prior session.
func GetLastGlobalEvent(workspaceRoot, currentSessionID string) (*EventEnvelope, error) {
	ledgerBase := filepath.Join(workspaceRoot, ".cog", "ledger")

	entries, err := os.ReadDir(ledgerBase)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Directory entries are always the sanitized (on-disk) form of a session
	// ID; sanitize currentSessionID the same way before comparing so the
	// current session's own ledger dir isn't mistaken for a "prior session"
	// when currentSessionID contains characters SanitizeComponent rewrites.
	currentSanitized := pathsafe.SanitizeComponent(currentSessionID)

	var newestTime int64
	var newestSession string

	for _, e := range entries {
		if !e.IsDir() || e.Name() == currentSanitized {
			continue
		}
		eventsPath := filepath.Join(ledgerBase, e.Name(), "events.jsonl")
		info, err := os.Stat(eventsPath)
		if err != nil {
			continue
		}
		if mt := info.ModTime().UnixNano(); mt > newestTime {
			newestTime = mt
			newestSession = e.Name()
		}
	}

	if newestSession == "" {
		return nil, nil
	}
	return GetLastEvent(workspaceRoot, newestSession)
}

// GetHashAlgorithm returns the hash algorithm configured for the workspace.
// Defaults to "sha256" if no genesis event is found.
func GetHashAlgorithm(workspaceRoot string) string {
	genesisFile := filepath.Join(workspaceRoot, ".cog", "ledger", "genesis", "events.jsonl")
	f, err := os.Open(genesisFile)
	if err != nil {
		return "sha256"
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		var env EventEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err == nil {
			if alg, ok := env.HashedPayload.Data["hash_algorithm"].(string); ok && alg != "" {
				return alg
			}
		}
	}
	return "sha256"
}
