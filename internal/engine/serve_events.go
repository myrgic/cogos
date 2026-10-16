// serve_events.go — HTTP surface for the kernel event bus.
//
// Two endpoints:
//
//	GET  /v1/events         — historical read. Thin wrapper around QueryLedger
//	                          with observability-flavored defaults (no
//	                          verify_chain, "since" accepts durations).
//	GET  /v1/events/stream  — live SSE stream backed by the EventBroker. Emits
//	                          `event: ledger.appended` frames with `id: <hash>`
//	                          for standard Last-Event-ID resume.
//
// Internal invariant: both endpoints source from AppendEvent's ledger (the
// single write sink). There is no duplicate storage — QueryLedger reads
// disk, the SSE stream reads the broker's ring + live fan-out.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSSEMaxDuration = 1 * time.Hour
	maxSSEMaxDuration     = 2 * time.Hour
	sseHeartbeatInterval  = 30 * time.Second
	sseWriteDeadline      = 10 * time.Second
)

// registerEventBusRoutes attaches the real event bus endpoints. Called from
// NewServer AFTER registerCompatRoutes so that the live handlers shadow any
// stale stub registrations (the stubs have been deleted, but this keeps the
// ordering future-proof against re-introductions).
func (s *Server) registerEventBusRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/events", s.handleEvents)
	s.route(mux, "GET /v1/events/stream", s.handleEventsStream)
}

// handleEvents is the non-streaming historical query. Mirrors handleLedger
// but with observability-flavored semantics: `since` accepts durations
// ("5m"), verify_chain is always off (use /v1/ledger for audit).
//
//	GET /v1/events?session_id=…&event_type=…&source=…&room=…&actor=…&since=…
//	                &until=…&limit=…&order=…
//	200 → { count, events, truncated, next_before? }
//	400 → bad query
//	404 → session_id specified but no events on disk
//	500 → read failure
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	now := time.Now().UTC()

	eq := EventQuery{
		SessionID:        q.Get("session_id"),
		EventTypePattern: q.Get("event_type"),
		Source:           q.Get("source"),
		Room:             q.Get("room"),
		Actor:            q.Get("actor"),
		Order:            q.Get("order"),
	}

	if v := q.Get("since"); v != "" {
		ts, err := ParseSinceDuration(v, now)
		if err != nil {
			writeEventsError(w, http.StatusBadRequest, fmt.Sprintf("since: %v", err))
			return
		}
		eq.Since = ts
	}
	if v := q.Get("until"); v != "" {
		ts, err := ParseSinceDuration(v, now)
		if err != nil {
			writeEventsError(w, http.StatusBadRequest, fmt.Sprintf("until: %v", err))
			return
		}
		eq.Until = ts
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeEventsError(w, http.StatusBadRequest, fmt.Sprintf("limit: %v", err))
			return
		}
		if n < 0 {
			writeEventsError(w, http.StatusBadRequest, "limit must be non-negative")
			return
		}
		eq.Limit = n
	}

	result, err := QueryEvents(s.cfg.WorkspaceRoot, eq)
	if err != nil {
		switch {
		case errors.Is(err, ErrSessionNotFound):
			writeEventsError(w, http.StatusNotFound, err.Error())
		default:
			msg := err.Error()
			if strings.Contains(msg, "event_type") || strings.Contains(msg, "since") {
				writeEventsError(w, http.StatusBadRequest, msg)
				return
			}
			writeEventsError(w, http.StatusInternalServerError, msg)
		}
		return
	}

	_ = json.NewEncoder(w).Encode(result)
}

// handleEventsStream is the live SSE endpoint. Replaces the serve_compat.go
// stub that previously just emitted heartbeats. Subscribers receive:
//
//   - `event: connected` on open (one-shot)
//   - `event: ledger.appended` per envelope (id = event hash)
//   - `: heartbeat` every 30s (SSE comment — invisible to EventSource)
//
// Query params:
//
//	session_id     optional filter
//	event_type     optional filter (exact or "prefix.*")
//	source         optional filter
//	since          RFC3339 timestamp or duration shorthand ("5m"); replays
//	               matching ring/disk entries before going live
//	last_event_id  standard SSE resume. If the hash is still in the ring,
//	               replay from the event AFTER it; otherwise fall back to
//	               `since` (or emit nothing before live).
//	max_events     stop after N delivered events
//	max_duration   hard cap on connection lifetime (default 1h, cap 2h)
func (s *Server) handleEventsStream(w http.ResponseWriter, r *http.Request) {
	broker := s.process.Broker()
	if broker == nil {
		http.Error(w, "event broker not initialised", http.StatusInternalServerError)
		return
	}

	q := r.URL.Query()
	now := time.Now().UTC()

	filter := EventFilter{
		SessionID:        q.Get("session_id"),
		EventTypePattern: q.Get("event_type"),
		Source:           q.Get("source"),
	}

	var sinceTime time.Time
	if v := q.Get("since"); v != "" {
		ts, err := ParseSinceDuration(v, now)
		if err != nil {
			http.Error(w, fmt.Sprintf("since: %v", err), http.StatusBadRequest)
			return
		}
		sinceTime = ts
	}

	// Prefer Last-Event-ID over `since`. The standard header is the browser
	// EventSource resume mechanism; the query-param form is the manual fallback.
	lastEventID := r.Header.Get("Last-Event-ID")
	if lastEventID == "" {
		lastEventID = q.Get("last_event_id")
	}

	maxEvents := 0
	if v := q.Get("max_events"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, fmt.Sprintf("max_events: invalid %q", v), http.StatusBadRequest)
			return
		}
		maxEvents = n
	}

	maxDur := defaultSSEMaxDuration
	if v := q.Get("max_duration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			http.Error(w, fmt.Sprintf("max_duration: invalid %q", v), http.StatusBadRequest)
			return
		}
		if d > maxSSEMaxDuration {
			d = maxSSEMaxDuration
		}
		maxDur = d
	}

	// Compile filter type early so bad patterns return 400 before we
	// touch the writer.
	if _, err := compileEventTypeMatcher(filter.EventTypePattern); err != nil {
		http.Error(w, fmt.Sprintf("event_type: %v", err), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")

	// Subscribe BEFORE replaying so we don't miss events that arrive
	// between snapshot and live.
	sub, err := broker.Subscribe(r.Context(), filter)
	if err != nil {
		http.Error(w, fmt.Sprintf("subscribe: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer sub.Cancel()

	rc := http.NewResponseController(w)
	// `WriteTimeout=5m` on the server would otherwise kill long-lived
	// connections — extend per-write.
	_ = rc.SetWriteDeadline(time.Now().Add(sseWriteDeadline))

	// Initial handshake: retry hint + connected event.
	fmt.Fprintf(w, "retry: 3000\n")
	connectedPayload, _ := json.Marshal(map[string]string{
		"session_id": filter.SessionID,
		"at":         now.Format(time.RFC3339),
	})
	fmt.Fprintf(w, "event: connected\ndata: %s\n\n", connectedPayload)
	flusher.Flush()

	// Build replay. Priority: Last-Event-ID (ring hit) > since. If the
	// Last-Event-ID is in the ring we replay events strictly AFTER it;
	// otherwise we fall through to `since` (client's job to notice they
	// missed events — we can't reconstruct from disk when the ring has
	// rolled over).
	var replay []*EventEnvelope
	var skipUntilAfterHash string
	if lastEventID != "" && broker.RingContainsHash(lastEventID) {
		replay = broker.RingReplay(filter, time.Time{})
		skipUntilAfterHash = lastEventID
	} else if !sinceTime.IsZero() {
		replay = broker.RingReplay(filter, sinceTime)
	}

	// Deliver replay.
	delivered := 0
	emit := func(env *EventEnvelope) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteDeadline))
		evt := envelopeToLedgerEvent(env)
		b, err := json.Marshal(evt)
		if err != nil {
			slog.Debug("events: marshal failed", "err", err)
			return true // keep going
		}
		fmt.Fprintf(w, "id: %s\nevent: ledger.appended\ndata: %s\n\n", env.Metadata.Hash, b)
		flusher.Flush()
		delivered++
		return maxEvents == 0 || delivered < maxEvents
	}

	for _, env := range replay {
		if skipUntilAfterHash != "" {
			if env.Metadata.Hash == skipUntilAfterHash {
				skipUntilAfterHash = ""
				continue
			}
			// Haven't reached the resume point yet — skip.
			if skipUntilAfterHash != "" {
				continue
			}
		}
		if !emit(env) {
			return
		}
	}

	// Live loop.
	deadline := time.NewTimer(maxDur)
	defer deadline.Stop()
	hb := time.NewTicker(sseHeartbeatInterval)
	defer hb.Stop()

	for {
		select {
		case env, ok := <-sub.Events:
			if !ok {
				return
			}
			if !emit(env) {
				return
			}
		case <-hb.C:
			_ = rc.SetWriteDeadline(time.Now().Add(sseWriteDeadline))
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case <-deadline.C:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func writeEventsError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
