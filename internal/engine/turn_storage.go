// turn_storage.go — Chat-history turn persistence (Agent R hybrid design).
//
// Closes Agent F gap #4 and cogos#20 (RecordBlock data-loss bug).
// RecordBlock at cogblock_ledger.go is persistence-theatre: it writes a
// metadata-only event and drops the full CogBlock payload on the floor.
// Instead of mutating RecordBlock (its call-site pattern is used elsewhere
// for metadata-only ingest events), we add a parallel RecordTurn path that
// captures the prompt/response pair — a first-class "turn" — via:
//
//  1. A sidecar JSONL file at .cog/run/turns/<sessionID>.jsonl carrying
//     the full turn (prompt, response, tool-call transcript, usage, model).
//  2. A hash-chained `turn.completed` ledger event with TRUNCATED previews
//     (defaults: 8 KB prompt / 16 KB response) + pointer to the sidecar.
//
// Readers (cog_read_conversation, GET /v1/conversation) hydrate from the
// sidecar. Agent N's broker, once it lands, will fan out turn.completed on
// the live event bus for free — no extra code in this file.
package engine

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// Default truncation caps for the ledger-event preview fields. The sidecar
// row carries the full text; these only apply to the hashed payload.
const (
	DefaultPromptPreviewCap   = 8 * 1024  // 8 KB — covers most user messages
	DefaultResponsePreviewCap = 16 * 1024 // 16 KB — covers most assistant replies
)

// TurnRecord is one complete prompt → response exchange.
// Stored in full in the session sidecar; stored as a truncated preview in
// the turn.completed ledger event.
type TurnRecord struct {
	TurnID     string           `json:"turn_id"`    // UUID minted at turn-start
	TurnIndex  int              `json:"turn_index"` // 1-based within session
	SessionID  string           `json:"session_id"`
	Timestamp  time.Time        `json:"timestamp"`             // turn-start, UTC
	DurationMs int64            `json:"duration_ms,omitempty"` // turn-end minus turn-start
	Prompt     string           `json:"prompt"`                // user message text (full)
	Response   string           `json:"response"`              // assistant message text (full)
	ToolCalls  []ToolCallRecord `json:"tool_calls,omitempty"`  // kernel-tool transcript
	Provider   string           `json:"provider,omitempty"`
	Model      string           `json:"model,omitempty"`
	Usage      TurnUsage        `json:"usage,omitempty"`
	BlockID    string           `json:"block_id,omitempty"`    // links to cogblock.ingest
	Status     string           `json:"status,omitempty"`      // "ok" | "error"
	Error      string           `json:"error,omitempty"`       // on status="error"
	LedgerHash string           `json:"ledger_hash,omitempty"` // turn.completed hash, filled after append

	// Speculative-output fields — populated when barge-in interrupted playback
	// mid-utterance (Slice 4). Response contains what was actually delivered to
	// the user; ResponseSpeculative is the suffix that was generated but never
	// played. BargeinPositionMs is the wall-time position into the utterance
	// where playback stopped; BargeinPositionTextOffset is the character offset
	// in Response (full text) marking the solidified/speculative boundary.
	ResponseSpeculative       string  `json:"response_speculative,omitempty"`
	BargeinPositionMs         float64 `json:"bargein_position_ms,omitempty"`
	BargeinPositionTextOffset int     `json:"bargein_position_text_offset,omitempty"`
}

// TurnUsage is a minimal (provider-neutral) token tally for a turn.
type TurnUsage struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
	TotalTokens  int `json:"total_tokens,omitempty"`
}

// ToolCallRecord is one kernel-owned tool invocation within a turn.
// Populated by the tool loop (tool_loop.go) and threaded back to the
// handler so it can be stored alongside the prompt/response in the turn.
type ToolCallRecord struct {
	ID           string `json:"id,omitempty"`
	Name         string `json:"name"`
	Arguments    string `json:"arguments,omitempty"`
	Result       string `json:"result,omitempty"`
	DurationMs   int64  `json:"duration_ms,omitempty"`
	Rejected     bool   `json:"rejected,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
}

// turnIndexCache tracks the next turn_index per session in-memory, seeded
// from the sidecar on first access. Matches the lastEventCache pattern in
// ledger.go — cheap O(1) increment after the first O(N) scan.
var turnIndexCache = struct {
	mu     sync.Mutex
	next   map[string]int
	primed map[string]bool
}{
	next:   make(map[string]int),
	primed: make(map[string]bool),
}

// NextTurnIndex returns the next 1-based turn index for sessionID,
// incrementing the in-memory counter. On first access it seeds the counter
// by scanning the sidecar file (if present) and taking the max turn_index
// plus one. This handles kernel restarts cleanly — a restarted session
// won't reset to turn 1.
func NextTurnIndex(workspaceRoot, sessionID string) int {
	turnIndexCache.mu.Lock()
	defer turnIndexCache.mu.Unlock()

	if !turnIndexCache.primed[sessionID] {
		// Seed from disk — idx = (max existing turn_index) + 1, or 1 if new.
		turnIndexCache.next[sessionID] = readSidecarMaxTurnIndex(workspaceRoot, sessionID) + 1
		turnIndexCache.primed[sessionID] = true
	}

	idx := turnIndexCache.next[sessionID]
	turnIndexCache.next[sessionID] = idx + 1
	return idx
}

// resetTurnIndexCacheForTests clears the in-memory counter so tests using
// the same sessionID across t.TempDir() workspaces don't see stale state.
func resetTurnIndexCacheForTests() {
	turnIndexCache.mu.Lock()
	defer turnIndexCache.mu.Unlock()
	turnIndexCache.next = make(map[string]int)
	turnIndexCache.primed = make(map[string]bool)
}

// readSidecarMaxTurnIndex scans the session's sidecar for the highest
// turn_index value. Returns 0 if the file doesn't exist or is empty.
func readSidecarMaxTurnIndex(workspaceRoot, sessionID string) int {
	path := turnSidecarPath(workspaceRoot, sessionID)
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	maxIdx := 0
	for dec.More() {
		var row TurnRecord
		if err := dec.Decode(&row); err != nil {
			break // give up on first parse error — counter will still be monotonic
		}
		if row.TurnIndex > maxIdx {
			maxIdx = row.TurnIndex
		}
	}
	return maxIdx
}

// turnSidecarPath returns the sidecar path for a session's turn JSONL.
func turnSidecarPath(workspaceRoot, sessionID string) string {
	return filepath.Join(workspaceRoot, ".cog", "run", "turns", pathsafe.SanitizeComponent(sessionID)+".jsonl")
}

// ReadLastTurn reads the most-recent TurnRecord from the session sidecar.
// Returns nil, nil when the sidecar does not exist or is empty. Errors only
// for genuine I/O failures (permissions, truncated JSON on last line).
func ReadLastTurn(workspaceRoot, sessionID string) (*TurnRecord, error) {
	path := turnSidecarPath(workspaceRoot, sessionID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open sidecar: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var last *TurnRecord
	for dec.More() {
		var row TurnRecord
		if err := dec.Decode(&row); err != nil {
			break // ignore trailing partial line
		}
		copy := row
		last = &copy
	}
	return last, nil
}

// PatchTurnSpeculative rewrites the most-recent TurnRecord in the session
// sidecar to include speculative-output fields. It reads all rows, updates
// the last one, and atomically replaces the file. This is intentionally
// O(N) in sidecar size — sidecars are bounded (one session) and patching
// is a rare post-turn path, not the hot inference path.
func PatchTurnSpeculative(workspaceRoot, sessionID string, turnID string, specText string, bargeinMs float64, textOffset int) error {
	path := turnSidecarPath(workspaceRoot, sessionID)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open sidecar: %w", err)
	}

	dec := json.NewDecoder(f)
	var rows []TurnRecord
	for dec.More() {
		var row TurnRecord
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	f.Close()

	// Find and patch the target turn.
	patched := false
	for i := range rows {
		if rows[i].TurnID == turnID {
			rows[i].ResponseSpeculative = specText
			rows[i].BargeinPositionMs = bargeinMs
			rows[i].BargeinPositionTextOffset = textOffset
			patched = true
			break
		}
	}
	if !patched {
		return fmt.Errorf("turn %s not found in sidecar", turnID)
	}

	// Atomic rewrite: write to .tmp then rename.
	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp sidecar: %w", err)
	}
	enc := json.NewEncoder(out)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			out.Close()
			os.Remove(tmp)
			return fmt.Errorf("encode sidecar row: %w", err)
		}
	}
	out.Close()
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename sidecar: %w", err)
	}
	return nil
}

// turnSidecarRelPath returns the workspace-relative sidecar path used in
// the ledger event's `sidecar_path` field (stable across absolute-path
// rewrites by downstream consumers).
func turnSidecarRelPath(sessionID string) string {
	return filepath.Join(".cog", "run", "turns", pathsafe.SanitizeComponent(sessionID)+".jsonl")
}

// extractTextFromToolCalls scans a slice of ToolCallRecords for text-bearing
// tool calls (speak, send_text, output) and concatenates their text payloads
// with a double-newline separator. Returns "" when no text-bearing calls exist.
//
// This is the fix for the audit-fidelity gap: when the model replies purely
// via tool calls (e.g. speak({text:"..."})), resp.Content is empty and
// turn.Response would otherwise persist as "". Callers apply this after
// setting turn.Response = resp.Content:
//
//	if turn.Response == "" {
//	    turn.Response = extractTextFromToolCalls(turn.ToolCalls)
//	}
func extractTextFromToolCalls(calls []ToolCallRecord) string {
	var parts []string
	for _, tc := range calls {
		if tc.Arguments == "" {
			continue
		}
		var args map[string]any
		if err := json.Unmarshal([]byte(tc.Arguments), &args); err != nil {
			continue
		}
		var text string
		switch tc.Name {
		case "speak", "output":
			if v, ok := args["text"]; ok {
				text, _ = v.(string)
			}
		case "send_text":
			// send_text uses "content" as its text field.
			if v, ok := args["content"]; ok {
				text, _ = v.(string)
			}
			// Fallback: some callers pass "text" even on send_text.
			if text == "" {
				if v, ok := args["text"]; ok {
					text, _ = v.(string)
				}
			}
		}
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// truncateUTF8Bytes returns a prefix of s containing at most capBytes bytes,
// cut on a UTF-8 boundary (never slices inside a multibyte rune). The
// returned bool is true if the cut happened (s exceeded the cap).
//
// capBytes <= 0 means "no truncation".
func truncateUTF8Bytes(s string, capBytes int) (string, bool) {
	if capBytes <= 0 || len(s) <= capBytes {
		return s, false
	}
	// Walk backwards from capBytes to find a valid UTF-8 boundary.
	cut := capBytes
	for cut > 0 {
		if utf8.RuneStart(s[cut]) {
			break
		}
		cut--
	}
	return s[:cut], true
}

// appendTurnSidecar appends one JSONL row to the session sidecar. Creates
// the parent directory and file as needed. Holds appendMu briefly during
// the write so concurrent RecordTurn calls for the same session don't
// interleave their lines.
var sidecarMu sync.Mutex

func appendTurnSidecar(workspaceRoot string, turn *TurnRecord) error {
	path := turnSidecarPath(workspaceRoot, turn.SessionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("turns dir: %w", err)
	}
	line, err := json.Marshal(turn)
	if err != nil {
		return fmt.Errorf("marshal turn: %w", err)
	}
	sidecarMu.Lock()
	defer sidecarMu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open sidecar: %w", err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%s\n", line); err != nil {
		return fmt.Errorf("write sidecar: %w", err)
	}
	return nil
}

// countRejectedToolCalls is a small utility used to populate the ledger
// preview's tool_rejected_count field.
func countRejectedToolCalls(calls []ToolCallRecord) int {
	n := 0
	for _, c := range calls {
		if c.Rejected {
			n++
		}
	}
	return n
}

// RecordTurn persists a completed turn: full text to the session sidecar,
// truncated preview to a hash-chained `turn.completed` ledger event.
//
// This is the fix for cogos#20 (RecordBlock data-loss): the existing
// cogblock.ingest event at cogblock_ledger.go:46 stays as-is (message_count
// metadata) for consumers that treat it as an ingest receipt, while the
// new turn.completed event is the authoritative persistence point for the
// conversation text. Chat handlers call BOTH — RecordBlock for the block
// envelope, RecordTurn for the turn content — so nothing is lost.
//
// If turn.Timestamp is zero it defaults to time.Now().UTC(). If turn.Status
// is "" it defaults to "ok".
func (p *Process) RecordTurn(turn *TurnRecord) error {
	if p == nil || p.cfg == nil {
		return fmt.Errorf("process or config nil")
	}
	if turn == nil {
		return fmt.Errorf("turn is nil")
	}
	if turn.SessionID == "" {
		turn.SessionID = p.sessionID
	}
	if turn.Timestamp.IsZero() {
		turn.Timestamp = time.Now().UTC()
	}
	if turn.Status == "" {
		turn.Status = "ok"
	}

	// 1. Write full turn to the per-session sidecar JSONL.
	if err := appendTurnSidecar(p.cfg.WorkspaceRoot, turn); err != nil {
		// Continue on — a missing sidecar row is recoverable, but we must
		// still try to emit the ledger event so downstream consumers at
		// least see that a turn happened.
		slog.Warn("RecordTurn: sidecar append failed", "err", fmt.Sprintf("%v", err), "session", turn.SessionID)
	}

	// 2. Build the ledger preview payload.
	promptCap := DefaultPromptPreviewCap
	respCap := DefaultResponsePreviewCap

	promptPreview, promptTrunc := truncateUTF8Bytes(turn.Prompt, promptCap)
	respPreview, respTrunc := truncateUTF8Bytes(turn.Response, respCap)

	data := map[string]interface{}{
		"turn_id":             turn.TurnID,
		"turn_index":          turn.TurnIndex,
		"prompt_preview":      promptPreview,
		"prompt_truncated":    promptTrunc,
		"response_preview":    respPreview,
		"response_truncated":  respTrunc,
		"sidecar_path":        turnSidecarRelPath(turn.SessionID),
		"provider":            turn.Provider,
		"model":               turn.Model,
		"duration_ms":         turn.DurationMs,
		"block_id":            turn.BlockID,
		"tool_call_count":     len(turn.ToolCalls),
		"tool_rejected_count": countRejectedToolCalls(turn.ToolCalls),
		"status":              turn.Status,
	}
	if turn.Usage != (TurnUsage{}) {
		data["usage"] = map[string]interface{}{
			"input_tokens":  turn.Usage.InputTokens,
			"output_tokens": turn.Usage.OutputTokens,
			"total_tokens":  turn.Usage.TotalTokens,
		}
	}
	if turn.Error != "" {
		data["error"] = turn.Error
	}

	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "turn.completed",
			Timestamp: turn.Timestamp.UTC().Format(time.RFC3339),
			SessionID: turn.SessionID,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "kernel-v3"},
	}

	// 3. Append to the hash-chained ledger. This is what Agent N's broker
	// will fan out; `cog_tail_events event_type=turn.completed` becomes
	// live turn replay the moment the broker lands.
	if err := AppendEvent(p.cfg.WorkspaceRoot, turn.SessionID, env); err != nil {
		return fmt.Errorf("turn.completed ledger append: %w", err)
	}
	turn.LedgerHash = env.Metadata.Hash
	return nil
}
