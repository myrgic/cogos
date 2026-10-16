// serve_runs.go - /v1/runs routes for the kernel run board (W1).
//
// Reads need a live grant (the route family is carved out of the blanket GET
// exemption); writes need runs:write. Opening a run accepts executor manual
// only; driven executors arrive in later slices.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
)

// runsRoutePrefix is the route family guarded by ScopeRunsWrite for writes
// and carved out of the blanket GET exemption (reads need a live grant).
const runsRoutePrefix = "/v1/runs"

func (s *Server) registerRunRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/runs", s.handleRunList)
	s.route(mux, "GET /v1/runs/events", s.handleRunEvents)
	s.route(mux, "GET /v1/runs/{id}", s.handleRunGet)
	s.route(mux, "POST /v1/runs", s.handleRunOpen)
	s.route(mux, "POST /v1/runs/{id}/end", s.handleRunEnd)
	s.route(mux, "POST /v1/runs/{id}/delivered", s.handleRunDelivered)
}

func (s *Server) runBoardOr503(w http.ResponseWriter) *RunBoardStore {
	if s.runBoard == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "run board not initialised")
		return nil
	}
	return s.runBoard
}

// runActorFromRequest names the writer: the verified grant's actor, else its
// surface. Entries are unsigned in v1 (pre-B1), so this is the attribution.
func runActorFromRequest(r *http.Request) string {
	if g, ok := grantFromContext(r.Context()); ok {
		if g.Actor != "" {
			return g.Actor
		}
		return g.Surface
	}
	return ""
}

func writeRunStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRunInvalid):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, ErrRunNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ErrRunConflict):
		writeJSONError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, ErrRunLedgerFailed):
		writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "run ledger append failed; nothing was recorded")
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal", "run board error")
	}
}

func writeRunJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeRunBody(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(into); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be a JSON object")
		return false
	}
	return true
}

// handleRunList serves GET /v1/runs?room=&executor=&state=.
func (s *Server) handleRunList(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	q := r.URL.Query()
	runs := st.List(RunFilter{Room: q.Get("room"), Executor: q.Get("executor"), State: q.Get("state")})
	writeRunJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// handleRunGet serves GET /v1/runs/{id}.
func (s *Server) handleRunGet(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	run, ok := st.Get(r.PathValue("id"))
	if !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such run")
		return
	}
	writeRunJSON(w, http.StatusOK, run)
}

// handleRunOpen serves POST /v1/runs (executor manual only in W1).
func (s *Server) handleRunOpen(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	var in RunOpen
	if !decodeRunBody(w, r, &in) {
		return
	}
	run, err := st.Open(in, runActorFromRequest(r))
	if err != nil {
		writeRunStoreError(w, err)
		return
	}
	writeRunJSON(w, http.StatusCreated, run)
}

// handleRunEnd serves POST /v1/runs/{id}/end. A second end is a 409 (I1).
func (s *Server) handleRunEnd(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	var end RunEnd
	if !decodeRunBody(w, r, &end) {
		return
	}
	run, err := st.End(r.PathValue("id"), end, runActorFromRequest(r))
	if err != nil {
		writeRunStoreError(w, err)
		return
	}
	writeRunJSON(w, http.StatusOK, run)
}

// handleRunDelivered serves POST /v1/runs/{id}/delivered.
func (s *Server) handleRunDelivered(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	var d RunDelivered
	if !decodeRunBody(w, r, &d) {
		return
	}
	run, err := st.Delivered(r.PathValue("id"), d, runActorFromRequest(r))
	if err != nil {
		writeRunStoreError(w, err)
		return
	}
	writeRunJSON(w, http.StatusOK, run)
}

// handleRunEvents serves GET /v1/runs/events (WebSocket, since=): replays
// every event after `since`, then streams new ones. Server to client only.
func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	var since uint64
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "since must be an unsigned integer")
			return
		}
		since = n
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already wrote the error response
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context())

	for {
		evs, wait := st.eventsAndWait(since)
		for _, ev := range evs {
			b, err := json.Marshal(ev)
			if err != nil {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
			since = ev.Seq
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return
		}
	}
}
