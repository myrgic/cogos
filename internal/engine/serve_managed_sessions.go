// serve_managed_sessions.go — ADR-093 §7 HTTP surface + event WebSocket for
// kernel-managed agent sessions (today: the hermes-acp driver).
//
//	GET    /v1/managed-sessions                    list
//	POST   /v1/managed-sessions                    create / fork / load  {driver, profile, cwd, fork_of, load, mcp_servers}
//	GET    /v1/managed-sessions/{id}               one session
//	DELETE /v1/managed-sessions/{id}               detach (idempotent)
//	POST   /v1/managed-sessions/{id}/resume        attach an on-disk session (session/load; idempotent)
//	POST   /v1/managed-sessions/{id}/prompt        {prompt: "text" | [ACP content blocks]} -> 202 {turn_id}
//	POST   /v1/managed-sessions/{id}/cancel        session/cancel
//	POST   /v1/managed-sessions/{id}/mode          {mode_id}
//	POST   /v1/managed-sessions/{id}/permission    {request_id, option_id}  ("" option_id = cancelled)
//	GET    /v1/managed-sessions/{id}/events?since=N
//	       WebSocket when the request is an upgrade: a "hello" frame, then
//	       every retained event with seq > N, then the live tail. A plain
//	       GET returns the same replay as JSON (no live tail).
//
// Writes go through the same grant-auth gate as every other POST/DELETE;
// GETs (including the WS upgrade) are reads and exempt, matching the rest
// of the kernel.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

func (s *Server) registerManagedSessionRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/managed-sessions", s.handleManagedSessionList)
	s.route(mux, "POST /v1/managed-sessions", s.handleManagedSessionCreate)
	s.route(mux, "GET /v1/managed-sessions/{id}", s.handleManagedSessionGet)
	s.route(mux, "DELETE /v1/managed-sessions/{id}", s.handleManagedSessionDelete)
	s.route(mux, "POST /v1/managed-sessions/{id}/resume", s.handleManagedSessionResume)
	s.route(mux, "POST /v1/managed-sessions/{id}/prompt", s.handleManagedSessionPrompt)
	s.route(mux, "POST /v1/managed-sessions/{id}/cancel", s.handleManagedSessionCancel)
	s.route(mux, "POST /v1/managed-sessions/{id}/mode", s.handleManagedSessionMode)
	s.route(mux, "POST /v1/managed-sessions/{id}/permission", s.handleManagedSessionPermission)
	s.route(mux, "GET /v1/managed-sessions/{id}/events", s.handleManagedSessionEvents)
}

// managedSessionStatus maps driver errors onto HTTP statuses.
func managedSessionStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrManagedSessionNotFound), errors.Is(err, ErrPermissionNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, ErrInvalidManagedSession):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, ErrTurnInFlight), errors.Is(err, ErrSessionNotLive):
		return http.StatusConflict, "conflict"
	default:
		return http.StatusBadGateway, "agent_error"
	}
}

func writeManagedSessionError(w http.ResponseWriter, err error) {
	status, kind := managedSessionStatus(err)
	writeJSONError(w, status, kind, err.Error())
}

func (s *Server) managedSession(w http.ResponseWriter, r *http.Request) *HermesACPSession {
	id := r.PathValue("id")
	sess := s.managedSessions.Get(id)
	if sess == nil {
		writeJSONError(w, http.StatusNotFound, "not_found", "no managed session "+id)
	}
	return sess
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(v); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func (s *Server) handleManagedSessionList(w http.ResponseWriter, r *http.Request) {
	writeJSONResp(w, http.StatusOK, map[string]any{"sessions": s.managedSessions.List()})
}

type managedSessionCreateRequest struct {
	Driver string `json:"driver"`
	HermesACPOpts
}

func (s *Server) handleManagedSessionCreate(w http.ResponseWriter, r *http.Request) {
	var req managedSessionCreateRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Driver != "" && req.Driver != DriverHermesACP {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "unsupported driver "+strconvQuote(req.Driver)+" (supported: hermes-acp)")
		return
	}
	if req.ForkOf != "" && req.Load != "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "fork_of and load are mutually exclusive")
		return
	}
	// The session outlives this request: only the handshake is bounded by it.
	sess, err := s.managedSessions.Create(r.Context(), req.HermesACPOpts)
	if err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusCreated, sess.Info())
}

func (s *Server) handleManagedSessionResume(w http.ResponseWriter, r *http.Request) {
	var req HermesACPOpts
	if r.ContentLength != 0 && !decodeBody(w, r, &req) {
		return
	}
	req.Load, req.ForkOf = r.PathValue("id"), ""
	sess, err := s.managedSessions.Create(r.Context(), req)
	if err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, sess.Info())
}

func (s *Server) handleManagedSessionGet(w http.ResponseWriter, r *http.Request) {
	if sess := s.managedSession(w, r); sess != nil {
		writeJSONResp(w, http.StatusOK, sess.Info())
	}
}

func (s *Server) handleManagedSessionDelete(w http.ResponseWriter, r *http.Request) {
	_ = s.managedSessions.Delete(r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleManagedSessionPrompt(w http.ResponseWriter, r *http.Request) {
	sess := s.managedSession(w, r)
	if sess == nil {
		return
	}
	var req struct {
		Prompt json.RawMessage `json:"prompt"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.Prompt) == 0 || string(req.Prompt) == "null" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "prompt is required")
		return
	}
	turn, err := sess.Prompt(req.Prompt)
	if err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusAccepted, map[string]any{"turn_id": turn, "session_id": sess.ID()})
}

func (s *Server) handleManagedSessionCancel(w http.ResponseWriter, r *http.Request) {
	sess := s.managedSession(w, r)
	if sess == nil {
		return
	}
	if err := sess.Cancel(); err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleManagedSessionMode(w http.ResponseWriter, r *http.Request) {
	sess := s.managedSession(w, r)
	if sess == nil {
		return
	}
	var req struct {
		ModeID string `json:"mode_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.ModeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "mode_id is required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := sess.SetMode(ctx, req.ModeID); err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "mode_id": req.ModeID})
}

func (s *Server) handleManagedSessionPermission(w http.ResponseWriter, r *http.Request) {
	sess := s.managedSession(w, r)
	if sess == nil {
		return
	}
	var req struct {
		RequestID string `json:"request_id"`
		OptionID  string `json:"option_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if err := sess.ResolvePermission(req.RequestID, req.OptionID); err != nil {
		writeManagedSessionError(w, err)
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}

// managedEventsHello is the first WS frame (and the plain-GET envelope).
type managedEventsHello struct {
	Kind      string `json:"kind"` // "hello"
	SessionID string `json:"session_id"`
	State     string `json:"state"`
	Since     uint64 `json:"since"`
	OldestSeq uint64 `json:"oldest_seq"`
	LastSeq   uint64 `json:"last_seq"`
	// Gap is true when events after `since` were already evicted from the
	// ring; the client should refetch history from the agent's own store.
	Gap bool `json:"gap"`
}

func (s *Server) handleManagedSessionEvents(w http.ResponseWriter, r *http.Request) {
	sess := s.managedSession(w, r)
	if sess == nil {
		return
	}
	var since uint64
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "since must be a non-negative integer")
			return
		}
		since = n
	}

	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		sub := sess.Events().Subscribe(since, 1)
		sess.Events().Unsubscribe(sub)
		writeJSONResp(w, http.StatusOK, map[string]any{
			"hello":  managedEventsHello{Kind: "hello", SessionID: sess.ID(), State: sess.State().String(), Since: since, OldestSeq: sub.OldestSeq, LastSeq: sub.LastSeq, Gap: sub.Gap},
			"events": sub.Replay,
		})
		return
	}

	// A WS stream outlives the server's Read/WriteTimeout; clear them before
	// the hijack (the deadlines would otherwise carry onto the raw conn).
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already wrote the error response
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context()) // the stream is server→client only

	sub := sess.Events().Subscribe(since, 1024)
	defer sess.Events().Unsubscribe(sub)
	send := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return c.Write(wctx, websocket.MessageText, b)
	}
	hello := managedEventsHello{Kind: "hello", SessionID: sess.ID(), State: sess.State().String(), Since: since, OldestSeq: sub.OldestSeq, LastSeq: sub.LastSeq, Gap: sub.Gap}
	if send(hello) != nil {
		return
	}
	for _, ev := range sub.Replay {
		if send(ev) != nil {
			return
		}
	}
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case ev, ok := <-sub.Live:
			if !ok {
				// Ring closed (session deleted) or this client fell behind;
				// either way the client reconnects with since=<last seq>.
				_ = c.Close(websocket.StatusGoingAway, "event stream ended; reconnect with since")
				return
			}
			if send(ev) != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func strconvQuote(s string) string { return strconv.Quote(s) }
