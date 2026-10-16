// serve_rooms.go - /v1/rooms routes for the room board store (P4-3).
//
// Reads and writes both require a member grant: the whole family is carved
// out of the blanket GET exemption (serve_grant_auth.go), and a request with
// no credential at all is refused 403 (member_grant_required) rather than the
// write routes' usual 401 — the room surface is a member-only API. A verified
// grant whose actor is not a member of the room is refused 403 by the
// handlers here; a share link is refused 403 by the middleware because a leaf
// credential never acts as a grant (ID-L3).
package engine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
)

func (s *Server) registerRoomRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/rooms", s.handleRoomList)
	s.route(mux, "GET /v1/rooms/{room}/entries", s.handleRoomEntries)
	s.route(mux, "POST /v1/rooms/{room}/entries", s.handleRoomPostEntry)
	s.route(mux, "GET /v1/rooms/{room}/board", s.handleRoomBoard)
	s.route(mux, "GET /v1/rooms/{room}/archive", s.handleRoomArchive)
}

func (s *Server) roomBoardOr503(w http.ResponseWriter) *RoomBoardStore {
	if s.roomBoard == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "room board not initialised")
		return nil
	}
	return s.roomBoard
}

// roomCallerFromRequest names the reader or writer. A verified grant's actor
// is the member id; the node-root scope (or a disabled gate, where no grant
// is ever attached) is the kernel's admin authority.
func (s *Server) roomCallerFromRequest(r *http.Request) RoomCaller {
	g, ok := grantFromContext(r.Context())
	if !ok {
		// No verified grant on the context: either the gate is disabled or
		// this server was built without the middleware. Treat the caller as
		// the node admin, matching Config.WriteRouteGrantAuthDisabled.
		return RoomCaller{Admin: true}
	}
	return RoomCaller{Actor: g.Actor, Admin: grantHasScope(g, nodeRootScope)}
}

func writeRoomStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRoomInvalid):
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, ErrRoomNotFound):
		writeJSONError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ErrRoomNotMember):
		writeJSONError(w, http.StatusForbidden, "member_grant_required", err.Error())
	case errors.Is(err, ErrRoomHeld):
		writeJSONError(w, http.StatusConflict, "held", err.Error())
	case errors.Is(err, ErrRoomConflict):
		writeJSONError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, ErrRoomLedgerFailed):
		writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "room ledger append failed; nothing was recorded")
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal", "room board error")
	}
}

func writeRoomJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// handleRoomList serves GET /v1/rooms.
func (s *Server) handleRoomList(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	writeRoomJSON(w, http.StatusOK, map[string]any{"rooms": st.List(s.roomCallerFromRequest(r))})
}

// handleRoomEntries serves GET /v1/rooms/{room}/entries.
func (s *Server) handleRoomEntries(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	roomID := r.PathValue("room")
	if _, ok := st.Board(roomID); !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such room")
		return
	}
	if !st.Authorize(roomID, s.roomCallerFromRequest(r)) {
		writeJSONError(w, http.StatusForbidden, "member_grant_required", "a member grant is required to read this room")
		return
	}
	writeRoomJSON(w, http.StatusOK, map[string]any{"entries": st.Entries(roomID)})
}

// roomPostBody is the POST /v1/rooms/{room}/entries request: one strict room
// entry and an optional base64 DER certificate delivered with it.
type roomPostBody struct {
	Entry json.RawMessage `json:"entry"`
	Cert  string          `json:"cert"`
}

// handleRoomPostEntry serves POST /v1/rooms/{room}/entries. The entry is
// validated through the provider's admission path inside the store; a
// rejected entry writes nothing.
func (s *Server) handleRoomPostEntry(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body roomPostBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be a JSON object with an entry")
		return
	}
	if len(body.Entry) == 0 || string(body.Entry) == "null" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "entry is required")
		return
	}
	var certDER []byte
	if body.Cert != "" {
		decoded, err := base64.StdEncoding.DecodeString(body.Cert)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "cert must be base64 DER")
			return
		}
		certDER = decoded
	}
	view, err := st.Append(r.PathValue("room"), body.Entry, certDER, s.roomCallerFromRequest(r))
	if err != nil {
		writeRoomStoreError(w, err)
		return
	}
	writeRoomJSON(w, http.StatusCreated, map[string]any{"entry": view})
}

// handleRoomBoard serves GET /v1/rooms/{room}/board.
func (s *Server) handleRoomBoard(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	roomID := r.PathValue("room")
	board, ok := st.Board(roomID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such room")
		return
	}
	if !st.Authorize(roomID, s.roomCallerFromRequest(r)) {
		writeJSONError(w, http.StatusForbidden, "member_grant_required", "a member grant is required to read this room")
		return
	}
	writeRoomJSON(w, http.StatusOK, board)
}

// handleRoomArchive serves GET /v1/rooms/{room}/archive.
func (s *Server) handleRoomArchive(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	roomID := r.PathValue("room")
	if _, ok := st.Board(roomID); !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such room")
		return
	}
	if !st.Authorize(roomID, s.roomCallerFromRequest(r)) {
		writeJSONError(w, http.StatusForbidden, "member_grant_required", "a member grant is required to read this room")
		return
	}
	writeRoomJSON(w, http.StatusOK, map[string]any{"items": st.Archive(roomID)})
}
