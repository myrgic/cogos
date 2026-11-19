// serve_rooms.go - /v1/rooms routes for the room board store (P4-3).
//
// Reads and client-signed writes require a member grant: the whole family is
// carved out of the blanket GET exemption (serve_grant_auth.go), and a request
// with no credential at all is refused 403 (member_grant_required) rather than
// the write routes' usual 401 — the room surface is a member-only API. A
// verified grant whose actor is not a member of the room is refused 403 by the
// handlers here; a share link is refused 403 by the middleware because a leaf
// credential never acts as a grant (ID-L3).
//
// Two routes are the exception: POST /v1/rooms (create a room) and
// POST /v1/rooms/{room}/ops (apply one board item op) are kernel-AUTHORED
// writes — a trusted local client holding a grant never signs anything itself;
// the kernel signs with its own BEP device key (see RoomBoardStore.CreateRoom
// and ApplyOp). Because those two are ordinary kernel write routes, a request
// carrying no credential at all gets the write routes' 401 missing_grant, not
// the room family's 403 (isRoomKernelWriteRequest in serve_grant_auth.go).
package engine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func (s *Server) registerRoomRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/rooms", s.handleRoomList)
	s.route(mux, "POST /v1/rooms", s.handleRoomCreate)
	s.route(mux, "GET /v1/rooms/{room}/entries", s.handleRoomEntries)
	s.route(mux, "POST /v1/rooms/{room}/entries", s.handleRoomPostEntry)
	s.route(mux, "POST /v1/rooms/{room}/ops", s.handleRoomOps)
	s.route(mux, "GET /v1/rooms/{room}/board", s.handleRoomBoard)
	s.route(mux, "GET /v1/rooms/{room}/archive", s.handleRoomArchive)
	s.route(mux, "GET /v1/rooms/{room}/wait", s.handleRoomWait)
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
	case errors.Is(err, ErrRoomUnavailable):
		// No node device key: the kernel cannot sign a room genesis or an
		// item op, so the kernel-authored write routes are unavailable.
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "this node has no device key to author room entries")
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

// roomCreateBody is the POST /v1/rooms request. Both fields are optional: a
// create with neither makes a room reachable only by its returned id.
type roomCreateBody struct {
	Alias string `json:"alias"`
	Title string `json:"title"`
}

// handleRoomCreate serves POST /v1/rooms — the kernel-authored create. The
// kernel signs the room's genesis (the gen-1 spec) on its own device log, with
// the caller's grant actor as the owner member. When alias names a room that
// already exists, that room is returned unchanged (200); a new room is 201.
func (s *Server) handleRoomCreate(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body roomCreateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be a JSON object")
		return
	}
	roomID, created, err := st.CreateRoom(body.Alias, body.Title, s.roomCallerFromRequest(r))
	if err != nil {
		writeRoomStoreError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeRoomJSON(w, status, map[string]any{"room": roomID, "alias": body.Alias, "created": created})
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

// roomOpBody is the POST /v1/rooms/{room}/ops request: one board op and its
// fields. `op` is create|set|append|archive; `item` names the target item
// (ignored for create, which derives its own id); `fields` is the entry body's
// payload (item.create's initial fields, or item.set's field/value, etc.).
type roomOpBody struct {
	Op     string         `json:"op"`
	Item   string         `json:"item"`
	Fields map[string]any `json:"fields"`
}

// handleRoomOps serves POST /v1/rooms/{room}/ops — the kernel-authored item
// op. The kernel builds the matching item entry, signs it on its own device
// log for the room, and appends it through the same admission path Append
// uses; the caller must be a member of the room (or the node admin). The
// response carries both the stored entry and the folded item.
func (s *Server) handleRoomOps(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body roomOpBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be a JSON object with an op")
		return
	}
	if body.Op == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "op is required")
		return
	}
	view, item, err := st.ApplyOp(r.PathValue("room"), body.Op, body.Item, body.Fields, s.roomCallerFromRequest(r))
	if err != nil {
		writeRoomStoreError(w, err)
		return
	}
	writeRoomJSON(w, http.StatusCreated, map[string]any{"entry": view, "item": item})
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

// roomWaitMaxTimeout caps the hold at ~50s, under the bridge's 55s idle-timeout ceiling (the same figure
// claude-web's own probe confirmed against the live cloudflared/claude.ai path). roomWaitDefaultTimeout is
// the hold used when the caller omits wait_s, matching the pairing poll's existing precedent.
const (
	roomWaitDefaultTimeout = 25 * time.Second
	roomWaitMaxTimeout     = 50 * time.Second
)

// writeStreamError answers a held read with the bus RFC's fallback-contract shape: {error, code, fallback}
// (see RFC-bus-stream-transport-polling-and-envelope's sse-gap-v3 example), so a caller already handling
// that contract for the bus needs no second error shape for this one. `fallback` is the plain poll the
// caller can retry with instead of waiting — for a held read, that is this same route with no wait_s.
func writeStreamError(w http.ResponseWriter, status int, code, msg, fallback string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg, "code": code, "fallback": fallback})
}

// roomHeadsVector parses a heads-vector query parameter ({"log":seq,...}, the exact RoomHeads cursor
// shape) from its JSON-encoded string form. An empty string is the empty vector (give everything).
func roomHeadsVector(raw string) (map[string]uint64, error) {
	out := map[string]uint64{}
	if raw == "" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// handleRoomWait serves GET /v1/rooms/{room}/wait?after=<heads-json>&wait_s=<n>: the held read over the
// room chain. `after` is a per-writer heads vector {log: seq} (room node RFC §6.9.1's RoomHeads shape,
// NOT a single counter -- a room can span more than one authoring node's log). `wait_s` caps the hold,
// clamped to roomWaitMaxTimeout; 0 means return immediately either way (a plain poll, no hold).
//
// Response is always {entries, heads}, timeout or not: entries is empty on a timeout with nothing new,
// but heads is ALWAYS the room's current heads vector at the moment of response -- never an empty success
// with a stale cursor. The caller resumes with after=heads on its next call, timeout or not; that IS the
// in-band heartbeat (room node RFC's own RoomHeads anti-entropy idea, long-poll shaped for one HTTP call).
//
// RR-1 note: this reads room-chain data P4-3 already writes to the kernel ledger today (board.entry
// events in the `rooms` bucket). It adds a wait on top of an existing read path and settles nothing about
// whether MORE entries move onto the kernel ledger -- that stays Chaz's ruling (§6.10.14 RR-1).
func (s *Server) handleRoomWait(w http.ResponseWriter, r *http.Request) {
	st := s.roomBoardOr503(w)
	if st == nil {
		return
	}
	roomID := r.PathValue("room")
	if _, ok := st.Board(roomID); !ok {
		// room_closed: a room this node once held but no longer does (e.g. closed) is NOT_FOUND at the
		// plain board route too, but the stream contract names it explicitly so a long-lived waiter knows
		// to stop polling this room rather than treat a 404 as a transient gap.
		writeStreamError(w, http.StatusNotFound, "room_closed", "no such room", "GET /v1/rooms")
		return
	}
	if !st.Authorize(roomID, s.roomCallerFromRequest(r)) {
		writeJSONError(w, http.StatusForbidden, "member_grant_required", "a member grant is required to read this room")
		return
	}
	after, err := roomHeadsVector(r.URL.Query().Get("after"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "after must be a JSON object of {log: seq}")
		return
	}
	// cursor_gone (resync): this room's chain has no eviction (every entry is retained in the ledger
	// forever), so the bus RFC's ring-buffer "the window moved past your cursor" case cannot happen here.
	// The structurally equivalent failure on an append-only log is a cursor claiming MORE than the room
	// actually has on some log -- impossible to honor, and silently holding for the timeout would just
	// hide a real caller bug (a stale snapshot from a different room, a hand-edited cursor, a bug in the
	// caller's own bookkeeping). Caught up front, with the real current heads so the caller can resync.
	if heads, ok := st.Heads(roomID); ok {
		for log, seq := range after {
			if seq > heads[log] {
				writeStreamError(w, http.StatusConflict, "cursor_gone",
					fmt.Sprintf("after[%s]=%d is ahead of this room's actual head %d; resync from the returned heads", log, seq, heads[log]),
					"GET /v1/rooms/{room}/entries")
				return
			}
		}
	}
	timeout := roomWaitDefaultTimeout
	if raw := r.URL.Query().Get("wait_s"); raw != "" {
		secs, perr := strconv.ParseFloat(raw, 64)
		if perr != nil || secs < 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "wait_s must be a non-negative number")
			return
		}
		timeout = time.Duration(secs * float64(time.Second))
	}
	if timeout > roomWaitMaxTimeout {
		timeout = roomWaitMaxTimeout
	}
	entries, heads, ok := st.WaitAfter(r.Context(), roomID, after, timeout)
	if !ok {
		// The room existed at the Board() check above but is gone now (a close raced this request): same
		// stream code as the up-front check, for the same reason.
		writeStreamError(w, http.StatusNotFound, "room_closed", "room closed during the wait", "GET /v1/rooms")
		return
	}
	writeRoomJSON(w, http.StatusOK, map[string]any{"entries": entries, "heads": heads})
}
