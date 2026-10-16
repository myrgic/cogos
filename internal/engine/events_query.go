// events_query.go — observability-flavored wrapper around QueryLedger.
//
// The hash-chained ledger (ledger.go / ledger_query.go) is the source of
// truth. This file exists so the event-bus surface (MCP cog_read_events,
// HTTP GET /v1/events) can reuse that read path without dragging audit
// semantics (verify_chain, seq pagination) into a debugging tool.
//
// Differences vs. QueryLedger:
//   - Accepts a Source filter in addition to SessionID/EventType.
//   - Since/Until accept either RFC3339 or duration shorthand ("5m"),
//     resolved by the caller via ParseSinceDuration before construction.
//   - Order toggles asc/desc (QueryLedger is always asc within a session).
//     Default is desc (newest first — "what's happening right now?").
//   - No verify_chain, no NextAfterSeq. Paging is via next_before (time-
//     based) for the multi-session case.
package engine

import (
	"time"
)

// EventQuery is the input shape for QueryEvents.
type EventQuery struct {
	SessionID        string
	EventTypePattern string
	Source           string
	Room             string
	Actor            string
	Since            time.Time
	Until            time.Time
	Limit            int
	Order            string // "asc" or "desc" (default "desc")
}

// EventQueryResult is the QueryEvents output.
type EventQueryResult struct {
	Count      int           `json:"count"`
	Events     []LedgerEvent `json:"events"`
	Truncated  bool          `json:"truncated"`
	NextBefore string        `json:"next_before,omitempty"` // RFC3339 timestamp for pagination
}

// QueryEvents reads matching events from the hash-chained ledger. Internally
// this delegates to QueryLedger with verify_chain=false and then applies the
// extra filters (source, until, order). We pull enough rows from QueryLedger
// to honour the caller's limit after filtering.
func QueryEvents(workspaceRoot string, q EventQuery) (*EventQueryResult, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLedgerLimit
	}
	if limit > maxLedgerLimit {
		limit = maxLedgerLimit
	}

	// Build the ledger query. Pull more than the requested limit so we can
	// apply source/until filters after reading. Cap at the ledger max.
	ledgerLimit := limit * 2
	if ledgerLimit > maxLedgerLimit {
		ledgerLimit = maxLedgerLimit
	}

	lq := LedgerQuery{
		SessionID: q.SessionID,
		EventType: q.EventTypePattern,
		Room:      q.Room,
		Actor:     q.Actor,
		Limit:     ledgerLimit,
	}
	if !q.Since.IsZero() {
		lq.SinceTimestamp = q.Since.UTC().Format(time.RFC3339)
	}

	raw, err := QueryLedger(workspaceRoot, lq)
	if err != nil {
		return nil, err
	}

	// Apply source filter + until bound.
	filtered := make([]LedgerEvent, 0, len(raw.Events))
	for _, evt := range raw.Events {
		if q.Source != "" && evt.Source != q.Source {
			continue
		}
		if !q.Until.IsZero() {
			ts, err := time.Parse(time.RFC3339, evt.Timestamp)
			if err == nil && !ts.Before(q.Until) {
				continue
			}
		}
		filtered = append(filtered, evt)
	}

	// QueryLedger delivers ascending-within-session, descending-by-session-mtime.
	// Default "desc" here means newest-first globally. Implementers of the
	// observability tool expect that ordering.
	order := q.Order
	if order == "" {
		order = "desc"
	}
	if order == "desc" {
		reverseEvents(filtered)
	}

	truncated := raw.Truncated
	if len(filtered) > limit {
		filtered = filtered[:limit]
		truncated = true
	}

	result := &EventQueryResult{
		Count:     len(filtered),
		Events:    filtered,
		Truncated: truncated,
	}

	// next_before: for "give me everything before this timestamp" pagination.
	// Only meaningful when we truncated a desc-ordered multi-session query.
	if truncated && len(filtered) > 0 && order == "desc" {
		result.NextBefore = filtered[len(filtered)-1].Timestamp
	}

	return result, nil
}

func reverseEvents(xs []LedgerEvent) {
	for i, j := 0, len(xs)-1; i < j; i, j = i+1, j-1 {
		xs[i], xs[j] = xs[j], xs[i]
	}
}
