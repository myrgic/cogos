// conversation_query.go — Reader for the turn.completed ledger stream.
//
// Thin wrapper that scans .cog/ledger/<sid>/events.jsonl for
// turn.completed events and optionally hydrates each with the full text
// from the per-session sidecar .cog/run/turns/<sid>.jsonl.
//
// When Agent L's QueryLedger API lands this wrapper will collapse to a
// 5-line shim over QueryLedger(event_type="turn.completed"); until then
// we do the (small) ledger scan locally. The scan is O(events-in-session)
// — cheap for typical sessions of <100 turns.
package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// ConversationQuery parameterises a read over the turn history.
type ConversationQuery struct {
	SessionID    string
	AfterTurn    int       // turn_index > AfterTurn (0 means no lower bound)
	BeforeTurn   int       // turn_index < BeforeTurn (0 means no upper bound)
	Since        time.Time // zero means no since-filter
	Limit        int       // default 20, max 200 (turns are bigger than events)
	IncludeFull  bool      // default true — hydrate prompt/response from sidecar
	IncludeTools bool      // default true — include tool-call transcript
	Order        string    // "asc" (default) | "desc"
}

// ConversationTurn is the reader-side projection of a turn.
type ConversationTurn struct {
	TurnID            string           `json:"turn_id"`
	TurnIndex         int              `json:"turn_index"`
	SessionID         string           `json:"session_id"`
	Timestamp         string           `json:"timestamp"`
	DurationMs        int64            `json:"duration_ms,omitempty"`
	Prompt            string           `json:"prompt"`
	PromptTruncated   bool             `json:"prompt_truncated,omitempty"`
	Response          string           `json:"response"`
	ResponseTruncated bool             `json:"response_truncated,omitempty"`
	ToolCalls         []ToolCallRecord `json:"tool_calls,omitempty"`
	Provider          string           `json:"provider,omitempty"`
	Model             string           `json:"model,omitempty"`
	Usage             TurnUsage        `json:"usage,omitempty"`
	BlockID           string           `json:"block_id,omitempty"`
	Status            string           `json:"status,omitempty"`
	Error             string           `json:"error,omitempty"`
	LedgerHash        string           `json:"ledger_hash,omitempty"`
	SidecarMissing    bool             `json:"sidecar_missing,omitempty"`
}

// ConversationQueryResult is the envelope returned to MCP/HTTP callers.
type ConversationQueryResult struct {
	Count         int                `json:"count"`
	SessionID     string             `json:"session_id,omitempty"`
	Turns         []ConversationTurn `json:"turns"`
	Truncated     bool               `json:"truncated"`
	NextAfterTurn int                `json:"next_after_turn,omitempty"`
}

// QueryConversation reads turn.completed events for the given session and
// (optionally) hydrates each from the sidecar. Returns turns in ascending
// turn_index order by default.
//
// If q.SessionID is "" the result is empty — caller must scope the read;
// cross-session scanning is explicitly out of scope for v1 (matches
// Agent R §9.9: default to current session, scoped reads only).
func QueryConversation(workspaceRoot string, q ConversationQuery) (*ConversationQueryResult, error) {
	if q.AfterTurn != 0 && q.BeforeTurn != 0 && q.AfterTurn >= q.BeforeTurn {
		return nil, errors.New("after_turn must be < before_turn")
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	order := q.Order
	if order == "" {
		order = "asc"
	}
	if order != "asc" && order != "desc" {
		return nil, fmt.Errorf("order must be asc or desc; got %q", order)
	}

	res := &ConversationQueryResult{SessionID: q.SessionID, Turns: []ConversationTurn{}}
	if q.SessionID == "" {
		return res, nil
	}

	turnEvents, err := readTurnCompletedEvents(workspaceRoot, q.SessionID)
	if err != nil {
		return nil, err
	}

	// Optional sidecar hydration. Loaded lazily and only once.
	var sidecar map[string]TurnRecord
	if q.IncludeFull {
		sidecar, err = loadSidecarIndex(workspaceRoot, q.SessionID)
		if err != nil && !os.IsNotExist(err) {
			// Don't fail the read — mark each turn's SidecarMissing instead.
			sidecar = nil
		}
	}

	// Apply turn-index / since filters, then build projection.
	projected := make([]ConversationTurn, 0, len(turnEvents))
	for _, ev := range turnEvents {
		idx, _ := intField(ev.HashedPayload.Data, "turn_index")
		if q.AfterTurn > 0 && idx <= q.AfterTurn {
			continue
		}
		if q.BeforeTurn > 0 && idx >= q.BeforeTurn {
			continue
		}
		if !q.Since.IsZero() {
			t, err := time.Parse(time.RFC3339, ev.HashedPayload.Timestamp)
			if err == nil && t.Before(q.Since) {
				continue
			}
		}
		turn := projectTurnFromEvent(ev, sidecar, q.IncludeFull, q.IncludeTools)
		projected = append(projected, turn)
	}

	// Sort: natural reading order is ascending turn_index. Events may
	// already be in order but hash-chain may interleave cross-session writes
	// in future so we sort explicitly.
	sort.Slice(projected, func(i, j int) bool {
		if order == "desc" {
			return projected[i].TurnIndex > projected[j].TurnIndex
		}
		return projected[i].TurnIndex < projected[j].TurnIndex
	})

	// Apply limit.
	if len(projected) > limit {
		res.Truncated = true
		projected = projected[:limit]
	}

	// Pagination cursor: only meaningful in ascending order.
	if order == "asc" && len(projected) > 0 {
		res.NextAfterTurn = projected[len(projected)-1].TurnIndex
	}

	res.Turns = projected
	res.Count = len(projected)
	return res, nil
}

// readTurnCompletedEvents scans the session ledger for all turn.completed
// events. Ignores other event types. Safe on a missing ledger file.
func readTurnCompletedEvents(workspaceRoot, sessionID string) ([]EventEnvelope, error) {
	path := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(sessionID), "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	defer f.Close()

	var out []EventEnvelope
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20) // allow up to 4 MB per line (big preview)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal(line, &env); err != nil {
			continue
		}
		if env.HashedPayload.Type != "turn.completed" {
			continue
		}
		out = append(out, env)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan ledger: %w", err)
	}
	return out, nil
}

// loadSidecarIndex loads the per-session sidecar into a map keyed by
// turn_id for fast per-turn hydration. Returns os.IsNotExist error if the
// sidecar file is missing (caller distinguishes "no sidecar yet" from
// real parse errors).
func loadSidecarIndex(workspaceRoot, sessionID string) (map[string]TurnRecord, error) {
	path := turnSidecarPath(workspaceRoot, sessionID)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(map[string]TurnRecord)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20) // allow up to 64 MB per row (huge turns)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var row TurnRecord
		if err := json.Unmarshal(line, &row); err != nil {
			continue
		}
		if row.TurnID == "" {
			continue
		}
		out[row.TurnID] = row
	}
	return out, sc.Err()
}

// projectTurnFromEvent builds a ConversationTurn from a turn.completed
// ledger event + optional sidecar row. When IncludeFull is true and a
// sidecar row is present, the full Prompt/Response are returned.
// Otherwise the ledger previews (which may be truncated) are returned.
func projectTurnFromEvent(ev EventEnvelope, sidecar map[string]TurnRecord, includeFull, includeTools bool) ConversationTurn {
	d := ev.HashedPayload.Data
	turnID, _ := stringField(d, "turn_id")
	idx, _ := intField(d, "turn_index")
	promptPreview, _ := stringField(d, "prompt_preview")
	respPreview, _ := stringField(d, "response_preview")
	promptTrunc, _ := boolField(d, "prompt_truncated")
	respTrunc, _ := boolField(d, "response_truncated")
	provider, _ := stringField(d, "provider")
	model, _ := stringField(d, "model")
	status, _ := stringField(d, "status")
	errStr, _ := stringField(d, "error")
	durMs, _ := intField(d, "duration_ms")
	blockID, _ := stringField(d, "block_id")

	// Usage sub-map is optional.
	var usage TurnUsage
	if usageMap, ok := d["usage"].(map[string]interface{}); ok {
		usage.InputTokens, _ = intFieldFromMap(usageMap, "input_tokens")
		usage.OutputTokens, _ = intFieldFromMap(usageMap, "output_tokens")
		usage.TotalTokens, _ = intFieldFromMap(usageMap, "total_tokens")
	}

	ct := ConversationTurn{
		TurnID:            turnID,
		TurnIndex:         idx,
		SessionID:         ev.HashedPayload.SessionID,
		Timestamp:         ev.HashedPayload.Timestamp,
		DurationMs:        int64(durMs),
		Prompt:            promptPreview,
		PromptTruncated:   promptTrunc,
		Response:          respPreview,
		ResponseTruncated: respTrunc,
		Provider:          provider,
		Model:             model,
		Usage:             usage,
		BlockID:           blockID,
		Status:            status,
		Error:             errStr,
		LedgerHash:        ev.Metadata.Hash,
	}

	if includeFull {
		row, ok := sidecar[turnID]
		if ok {
			ct.Prompt = row.Prompt
			ct.Response = row.Response
			ct.PromptTruncated = false
			ct.ResponseTruncated = false
			if includeTools {
				ct.ToolCalls = row.ToolCalls
			}
			if row.DurationMs != 0 {
				ct.DurationMs = row.DurationMs
			}
		} else if sidecar != nil {
			// Sidecar file exists but this turn's row is missing — surface
			// the gap so callers can investigate without failing the read.
			ct.SidecarMissing = true
		}
	}

	return ct
}

// ── small helpers for untyped map access ───────────────────────────────────

func stringField(m map[string]interface{}, key string) (string, bool) {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}

func boolField(m map[string]interface{}, key string) (bool, bool) {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b, true
		}
	}
	return false, false
}

func intField(m map[string]interface{}, key string) (int, bool) {
	return intFieldFromMap(m, key)
}

func intFieldFromMap(m map[string]interface{}, key string) (int, bool) {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n), true
		case int:
			return n, true
		case int64:
			return int(n), true
		case json.Number:
			if i, err := n.Int64(); err == nil {
				return int(i), true
			}
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}
