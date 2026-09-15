// speculative_output.go — bargein-driven speculative-output helpers (Slice 4).
//
// When mod3 detects a barge-in (user speaks while TTS is playing), it emits a
// "bargein.event" to its chat_flow_log ring buffer. That event carries:
//   - text_actually_played   — what the user heard
//   - text_speculative       — what was generated but NOT played
//   - bargein_position_ms    — wall-time ms into the utterance
//   - bargein_position_text_offset — character offset marking the boundary
//
// The kernel fetches this event at the START of each new turn (via
// fetchRecentBargeinEvent) and uses it two ways:
//
//  1. Context injection: the speculative text is passed to AssembleContext via
//     WithPreviousTurnSpeculative so FormatForProvider can inject a
//     <previous-turn-speculative> block into the model's system prompt.
//
//  2. Turn-record backfill: after RecordTurn writes the previous turn, the
//     bargein data is written back via PatchTurnSpeculative so the sidecar
//     records what actually happened.
//
// Bargein events are matched to the previous turn by session_id and recency
// (event must be within BargeinMatchWindowS of the current request). This
// is a best-effort heuristic — if mod3 is down or the event is stale, the
// speculative block is silently omitted rather than blocking inference.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// BargeinMatchWindowS is the maximum age (seconds) of a bargein.event for it
// to be considered relevant to the current turn. Events older than this are
// assumed to belong to a prior conversation segment and are ignored.
const BargeinMatchWindowS = 30.0

// mod3BargeinEvent is the JSON shape of a bargein.event from
// GET /v1/logs/chat-flow?event_type=bargein.event&session_id=X&limit=1.
// Only fields needed for speculative-output backfill are decoded.
type mod3BargeinEvent struct {
	EventType                 string  `json:"event_type"`
	Timestamp                 string  `json:"ts"`
	SessionID                 string  `json:"session_id"`
	TextActuallyPlayed        string  `json:"text_actually_played"`
	TextSpeculative           string  `json:"text_speculative"`
	BargeinPositionMs         float64 `json:"bargein_position_ms"`
	BargeinPositionTextOffset int     `json:"bargein_position_text_offset"`
}

// fetchRecentBargeinEvent queries mod3's chat-flow log for the most recent
// bargein.event for the given session. Returns nil if mod3 is unreachable,
// the event is older than BargeinMatchWindowS, or no bargein occurred.
// Errors are logged at debug level and suppressed — this path must never
// block or panic the inference path.
func fetchRecentBargeinEvent(ctx context.Context, mod3URL, sessionID string) *mod3BargeinEvent {
	if mod3URL == "" || sessionID == "" {
		return nil
	}

	reqURL, err := url.Parse(mod3URL + "/v1/logs/chat-flow")
	if err != nil {
		slog.Debug("speculative_output: bad mod3URL", "err", err)
		return nil
	}
	q := reqURL.Query()
	q.Set("session_id", sessionID)
	q.Set("event_type", "bargein.event")
	q.Set("limit", "1")
	reqURL.RawQuery = q.Encode()

	fetchCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		slog.Debug("speculative_output: build request failed", "err", err)
		return nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Debug("speculative_output: mod3 fetch failed", "err", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug("speculative_output: mod3 returned non-200", "status", resp.StatusCode)
		return nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		slog.Debug("speculative_output: read body failed", "err", err)
		return nil
	}

	var result struct {
		Events []mod3BargeinEvent `json:"events"`
		Count  int                `json:"count"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		slog.Debug("speculative_output: unmarshal failed", "err", err)
		return nil
	}
	if len(result.Events) == 0 {
		return nil
	}

	ev := &result.Events[0]

	// Recency check: parse the event timestamp and reject if too old.
	if ev.Timestamp != "" {
		// mod3 emits ISO 8601 timestamps; try RFC3339 first then RFC3339Nano.
		var ts time.Time
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, ev.Timestamp); err == nil {
				ts = t
				break
			}
		}
		if !ts.IsZero() && time.Since(ts).Seconds() > BargeinMatchWindowS {
			slog.Debug("speculative_output: bargein event too old",
				"age_s", fmt.Sprintf("%.1f", time.Since(ts).Seconds()),
				"session", sessionID,
			)
			return nil
		}
	}

	if ev.TextSpeculative == "" {
		return nil
	}

	return ev
}
