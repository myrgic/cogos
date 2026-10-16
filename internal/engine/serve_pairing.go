// serve_pairing.go — the kernel pairing flow (P3b slice S8, decision O11).
//
// Pairing is how a screen with no credential asks to become somebody's: it
// asks, shows a short code, and long-polls with a secret. A person who already
// holds authority approves the code, and the screen receives a share or device
// link exactly once. This is a kernel flow, not a Stage feature: the Stage's
// two pairing paths (trusted devices, room pairing) become clients of it in
// slice S9, and any other surface can use it the same way.
//
// Routes (all under /v1/pairing, exempt from the grant gate because they carry
// their own authentication, see isGrantExemptRequest):
//
//	POST /v1/pairing/requests              no auth, rate-limited, 50 open, 600 s
//	GET  /v1/pairing/requests/{id}?secret= the requester's long-poll (constant-time secret)
//	GET  /v1/pairing/requests              list open requests (pairing:approve)
//	POST /v1/pairing/requests/{code}/approve   (pairing:approve)
//	POST /v1/pairing/requests/{code}/deny      (pairing:approve)
//
// {id} and {code} accept either the request id or the code.
//
// Who may approve: a grant carrying pairing:approve, or a share link carrying
// pairing:approve (a trusted device). A share link without the scope gets 403,
// never 401, because the credential is genuine. A share-link approver cannot
// escalate: the new link keeps the approver's actor, never a higher role, and
// scopes outside the approver's own are refused.
//
// What approval does: it issues a link of the REQUESTED kind through the
// share-link store (serve_share_links.go) and hands the raw token to the
// requester's next poll, once. The token lives only in kernel memory between
// approval and that poll; it is never written to the ledger. If the kernel
// restarts first the requester asks again. Pending requests are likewise
// in-memory (the Stage's PAIRS behaved the same way).
//
// Ledger (.cog/ledger/identity-pairing/events.jsonl) holds hashes and ids
// only: sha256 of the code and of the poll secret, the key fingerprint, link
// ids. The one public value it carries is the requester's P-256 public key, on
// the approval event, because the later identity ceremony (B0) binds the new
// device to that key. A public key is not a secret.
package engine

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ScopePairingApprove authorizes approving or denying a pairing request.
const ScopePairingApprove = "pairing:approve"

// pairingRoutePrefix is exempt from the grant gate (handlers authenticate).
const pairingRoutePrefix = "/v1/pairing"

// pairingLedgerSession is the ledger bucket for pairing events.
const pairingLedgerSession = "identity-pairing"

const (
	// pairingCodeAlphabet is the Stage's alphabet (devices.CODE_ALPHABET and
	// stage.PAIR_ALPHABET, vector X01): no vowels, so no accidental words.
	pairingCodeAlphabet = "BCDFGHJKLMNPQRSTVWXZ"
	// Code length by kind: 6 for devices (devices.py), 4 for room-style share pairing (stage.py).
	pairingCodeLenDevice = 6
	pairingCodeLenShare  = 4

	pairingLifetime     = 600 * time.Second
	pairingMaxOpen      = 50
	pairingPollWait     = 25 * time.Second
	pairingNameMax      = 40
	pairingSecretBytes  = 18
	pairingTokenBytes   = 32
	pairingCreateLimit  = 60 // creations per window, across all callers
	pairingCreateWindow = time.Minute
)

// defaultDeviceLinkScope is what a device link carries when the approver names
// no scope (the plan's trusted-device row, section 3).
var defaultDeviceLinkScope = []string{"stage:device", "stage:packs", "stage:screens", ScopePairingApprove}

type pairingState int

const (
	pairingPending pairingState = iota
	pairingApproved
	pairingDenied
)

type pairingRequest struct {
	id        string
	code      string
	secretH   [sha256.Size]byte
	kind      string
	name      string
	pubDER    []byte // SPKI DER of the requester's P-256 key, nil if none
	created   time.Time
	state     pairingState
	token     string // raw link token, set on approval, cleared on collection
	link      *ShareLink
	done      chan struct{}
	approvedB string // approver id for the ledger
}

// pairingStore holds open requests. now is overridable for tests.
type pairingStore struct {
	mu            sync.Mutex
	byID          map[string]*pairingRequest
	byCode        map[string]*pairingRequest
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
	now           func() time.Time
}

func newPairingStore(workspaceRoot string) *pairingStore {
	return &pairingStore{
		byID:          make(map[string]*pairingRequest),
		byCode:        make(map[string]*pairingRequest),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
		now:           time.Now,
	}
}

func (ps *pairingStore) append(eventType string, data map[string]interface{}) error {
	if ps.workspaceRoot == "" {
		return nil
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: ps.now().UTC().Format(time.RFC3339),
			SessionID: pairingLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-pairing"},
	}
	return ps.appendEvent(ps.workspaceRoot, pairingLedgerSession, env)
}

func pairingHash(s string) string { return sha256Hex(s) }

// pruneLocked drops requests past their lifetime. An approved request nobody
// collected has its link revoked: the token was never delivered, so the link
// would be live with nobody able to use it.
func (ps *pairingStore) pruneLocked(links *ShareLinkStore) {
	now := ps.now()
	for _, r := range ps.byID {
		if now.Sub(r.created) <= pairingLifetime {
			continue
		}
		if r.state == pairingApproved && r.link != nil && links != nil {
			_, _ = links.Revoke(r.link.LinkID)
		}
		_ = ps.append("pairing.expired", map[string]interface{}{
			"request_id": r.id, "code_hash": pairingHash(r.code), "state": r.stateName(),
		})
		ps.dropLocked(r)
	}
}

func (r *pairingRequest) stateName() string {
	switch r.state {
	case pairingApproved:
		return "approved"
	case pairingDenied:
		return "denied"
	}
	return "pending"
}

func (ps *pairingStore) dropLocked(r *pairingRequest) {
	delete(ps.byID, r.id)
	if cur, ok := ps.byCode[r.code]; ok && cur == r {
		delete(ps.byCode, r.code)
	}
	r.token = ""
}

func (ps *pairingStore) lookupLocked(key string) *pairingRequest {
	if r, ok := ps.byID[key]; ok {
		return r
	}
	return ps.byCode[strings.ToUpper(key)]
}

func randomPairingCode(n int) (string, error) {
	out := make([]byte, n)
	buf := make([]byte, n)
	for i := 0; i < n; {
		if _, err := rand.Read(buf[:1]); err != nil {
			return "", err
		}
		// rejection sampling: 20 letters, 256 mod 20 != 0
		if int(buf[0]) >= 256-256%len(pairingCodeAlphabet) {
			continue
		}
		out[i] = pairingCodeAlphabet[int(buf[0])%len(pairingCodeAlphabet)]
		i++
	}
	return string(out), nil
}

func randomURLToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// parsePairingPublicKey accepts a P-256 public key as base64 (std or URL
// alphabet, padded or not) of either an uncompressed point (65 bytes, 0x04 X Y)
// or SPKI DER, and returns the canonical SPKI DER.
func parsePairingPublicKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	var raw []byte
	var err error
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if raw, err = enc.DecodeString(s); err == nil {
			break
		}
	}
	if err != nil {
		return nil, errors.New("public_key must be base64")
	}
	if len(raw) == 65 && raw[0] == 4 {
		pk, err := ecdh.P256().NewPublicKey(raw)
		if err != nil {
			return nil, errors.New("public_key is not a valid P-256 point")
		}
		return x509.MarshalPKIXPublicKey(pk)
	}
	pub, err := x509.ParsePKIXPublicKey(raw)
	if err != nil {
		return nil, errors.New("public_key must be a P-256 uncompressed point or SPKI DER")
	}
	ek, ok := pub.(*ecdsa.PublicKey)
	if !ok || ek.Curve.Params().Name != "P-256" {
		return nil, errors.New("public_key must be a P-256 key")
	}
	return x509.MarshalPKIXPublicKey(ek)
}

// ─── routes ──────────────────────────────────────────────────────────────────

func (s *Server) registerPairingRoutes(mux *http.ServeMux) {
	s.route(mux, "POST /v1/pairing/requests", s.handlePairingCreate)
	s.route(mux, "GET /v1/pairing/requests", s.handlePairingList)
	s.route(mux, "GET /v1/pairing/requests/{id}", s.handlePairingPoll)
	s.route(mux, "POST /v1/pairing/requests/{code}/approve", s.handlePairingApprove)
	s.route(mux, "POST /v1/pairing/requests/{code}/deny", s.handlePairingDeny)
}

func (s *Server) pairingOr503(w http.ResponseWriter) *pairingStore {
	if s.pairing == nil || s.shareLinks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "pairing not initialised")
		return nil
	}
	return s.pairing
}

type pairingCreateRequest struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
}

func (s *Server) handlePairingCreate(w http.ResponseWriter, r *http.Request) {
	ps := s.pairingOr503(w)
	if ps == nil {
		return
	}
	if s.pairingLimiter != nil && !s.pairingLimiter.Allow() {
		writeJSONError(w, http.StatusTooManyRequests, "rate_limited", "too many pairing requests; try again shortly")
		return
	}
	var req pairingCreateRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON")
			return
		}
	}
	kind := req.Kind
	if kind == "" {
		kind = ShareLinkKindShare
	}
	if kind != ShareLinkKindShare && kind != ShareLinkKindDevice {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", `kind must be "share" or "device"`)
		return
	}
	var pubDER []byte
	if req.PublicKey != "" {
		var err error
		if pubDER, err = parsePairingPublicKey(req.PublicKey); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "a screen"
	}
	if rs := []rune(name); len(rs) > pairingNameMax {
		name = string(rs[:pairingNameMax])
	}

	codeLen := pairingCodeLenShare
	if kind == ShareLinkKindDevice {
		codeLen = pairingCodeLenDevice
	}
	secret, err := randomURLToken(pairingSecretBytes)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "internal", "entropy unavailable")
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pruneLocked(s.shareLinks)
	if len(ps.byID) >= pairingMaxOpen {
		writeJSONError(w, http.StatusTooManyRequests, "busy", "busy")
		return
	}
	var code string
	for {
		if code, err = randomPairingCode(codeLen); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "internal", "entropy unavailable")
			return
		}
		if _, taken := ps.byCode[code]; !taken {
			break
		}
	}
	now := ps.now().UTC()
	pr := &pairingRequest{
		id: "pr-" + mustHex(8), code: code, secretH: sha256.Sum256([]byte(secret)),
		kind: kind, name: name, pubDER: pubDER, created: now, done: make(chan struct{}),
	}
	ev := map[string]interface{}{
		"request_id": pr.id, "code_hash": pairingHash(code), "secret_hash": pairingHash(secret),
		"kind": kind, "name": name,
	}
	if pubDER != nil {
		ev["key_fingerprint"] = hex.EncodeToString(fingerprint(pubDER))
	}
	if err := ps.append("pairing.requested", ev); err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "pairing request NOT recorded: "+err.Error())
		return
	}
	ps.byID[pr.id] = pr
	ps.byCode[pr.code] = pr
	writeJSONResp(w, http.StatusOK, map[string]any{
		"id": pr.id, "code": pr.code, "secret": secret, "kind": kind,
		"expires_s":  int(pairingLifetime / time.Second),
		"expires_at": now.Add(pairingLifetime).Format(time.RFC3339),
	})
}

func fingerprint(der []byte) []byte {
	h := sha256.Sum256(der)
	return h[:]
}

// handlePairingPoll is the requester's long-poll. Unknown id, wrong secret and
// expired all answer the same 404, so a caller learns nothing about which.
func (s *Server) handlePairingPoll(w http.ResponseWriter, r *http.Request) {
	ps := s.pairingOr503(w)
	if ps == nil {
		return
	}
	key := r.PathValue("id")
	secretH := sha256.Sum256([]byte(r.URL.Query().Get("secret")))

	ps.mu.Lock()
	ps.pruneLocked(s.shareLinks)
	pr := ps.lookupLocked(key)
	if pr == nil || subtle.ConstantTimeCompare(pr.secretH[:], secretH[:]) != 1 {
		ps.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown code")
		return
	}
	done := pr.done
	pending := pr.state == pairingPending
	ps.mu.Unlock()

	if pending {
		wait := s.pairingPollWait
		if wait <= 0 {
			wait = pairingPollWait
		}
		ctx, cancel := context.WithTimeout(r.Context(), wait)
		defer cancel()
		select {
		case <-done:
		case <-ctx.Done():
			if r.Context().Err() != nil {
				return
			}
			writeJSONResp(w, http.StatusAccepted, map[string]any{"pending": true})
			return
		}
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pruneLocked(s.shareLinks)
	cur := ps.lookupLocked(key)
	if cur != pr { // expired or collected by a concurrent poll
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown code")
		return
	}
	switch pr.state {
	case pairingDenied:
		_ = ps.append("pairing.collected", map[string]interface{}{"request_id": pr.id, "outcome": "denied"})
		ps.dropLocked(pr)
		writeJSONError(w, http.StatusForbidden, "denied", "denied")
	case pairingApproved:
		token, link := pr.token, pr.link
		_ = ps.append("pairing.collected", map[string]interface{}{
			"request_id": pr.id, "outcome": "approved", "link_id": link.LinkID,
		})
		ps.dropLocked(pr)
		writeJSONResp(w, http.StatusOK, map[string]any{
			"status": "approved", "kind": link.Kind, "token": token, "link": viewOfShareLink(link),
		})
	default:
		writeJSONResp(w, http.StatusAccepted, map[string]any{"pending": true})
	}
}

// pairingApprover is who approved: a grant or a share link.
type pairingApprover struct {
	id   string
	link *ShareLink // nil when the approver is a grant
}

// authorizePairingApprover resolves the caller to an approver holding
// pairing:approve, or writes the refusal and returns false.
func (s *Server) authorizePairingApprover(w http.ResponseWriter, r *http.Request) (pairingApprover, bool) {
	token := grantTokenFromRequest(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing_grant",
			GrantHeaderName+" (a grant, or a share link carrying "+ScopePairingApprove+") required")
		return pairingApprover{}, false
	}
	if l, ok := s.shareLinks.Resolve(token); ok {
		for _, sc := range l.Scope {
			if sc == ScopePairingApprove {
				return pairingApprover{id: l.LinkID, link: l}, true
			}
		}
		writeJSONError(w, http.StatusForbidden, "insufficient_scope",
			"this share link does not carry the '"+ScopePairingApprove+"' scope")
		return pairingApprover{}, false
	}
	if s.identityGrants == nil {
		writeJSONError(w, http.StatusUnauthorized, "invalid_grant", "no identity grant registry available")
		return pairingApprover{}, false
	}
	g, ok := s.identityGrants.VerifyAny(token)
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "invalid_grant", "credential is missing, expired, or revoked")
		return pairingApprover{}, false
	}
	if !grantHasScope(g, ScopePairingApprove) {
		writeJSONError(w, http.StatusForbidden, "insufficient_scope",
			"grant does not carry the '"+ScopePairingApprove+"' scope")
		return pairingApprover{}, false
	}
	return pairingApprover{id: g.GrantID}, true
}

type pairingOpenView struct {
	ID        string `json:"id"`
	Code      string `json:"code"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	HasKey    bool   `json:"has_public_key"`
}

func (s *Server) handlePairingList(w http.ResponseWriter, r *http.Request) {
	ps := s.pairingOr503(w)
	if ps == nil {
		return
	}
	if _, ok := s.authorizePairingApprover(w, r); !ok {
		return
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pruneLocked(s.shareLinks)
	out := make([]pairingOpenView, 0, len(ps.byID))
	for _, pr := range ps.byID {
		if pr.state != pairingPending {
			continue
		}
		out = append(out, pairingOpenView{
			ID: pr.id, Code: pr.code, Kind: pr.kind, Name: pr.name,
			CreatedAt: pr.created.UTC().Format(time.RFC3339), HasKey: pr.pubDER != nil,
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"requests": out})
}

type pairingApproveRequest struct {
	Kind     string   `json:"kind,omitempty"` // optional; must match the request's
	Surface  string   `json:"surface,omitempty"`
	Actor    string   `json:"actor,omitempty"`
	Role     string   `json:"role,omitempty"`
	Room     string   `json:"room,omitempty"`
	Scope    []string `json:"scope,omitempty"`
	TTLHours float64  `json:"ttl_hours,omitempty"`
}

var roleRank = map[string]int{"guest": 1, "collaborator": 2, "owner": 3}

func (s *Server) handlePairingApprove(w http.ResponseWriter, r *http.Request) {
	ps := s.pairingOr503(w)
	if ps == nil {
		return
	}
	appr, ok := s.authorizePairingApprover(w, r)
	if !ok {
		return
	}
	var body pairingApproveRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON")
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pruneLocked(s.shareLinks)
	pr := ps.lookupLocked(r.PathValue("code"))
	if pr == nil || pr.state != pairingPending {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such code (expired?)")
		return
	}
	if body.Kind != "" && body.Kind != pr.kind {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "kind must match the request's kind ("+pr.kind+")")
		return
	}

	in := ShareLinkImport{
		Kind: pr.kind, Surface: body.Surface, Actor: body.Actor, Role: body.Role,
		Room: body.Room, Scope: body.Scope, TTLHours: body.TTLHours,
	}
	if appr.link != nil { // a device approving: defaults come from the approver
		if in.Actor == "" {
			in.Actor = appr.link.Actor
		}
		if in.Role == "" {
			in.Role = appr.link.Role
		}
	}
	if in.Surface == "" && pr.kind == ShareLinkKindDevice {
		in.Surface = shareLinkSurfacePrefix + "device/dev-" + mustHex(3)
	}
	if len(in.Scope) == 0 && pr.kind == ShareLinkKindDevice {
		in.Scope = append([]string(nil), defaultDeviceLinkScope...)
	}

	token, err := randomURLToken(pairingTokenBytes)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "internal", "entropy unavailable")
		return
	}
	in.IntegrityHash = sha256Hex(token)
	if err := in.validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if appr.link != nil {
		if err := checkNoEscalation(appr.link, in); err != nil {
			writeJSONError(w, http.StatusForbidden, "insufficient_scope", err.Error())
			return
		}
	}

	link, err := s.shareLinks.Import(in, appr.id)
	if err != nil {
		status, kind := http.StatusBadRequest, "invalid_request"
		switch {
		case errors.Is(err, ErrShareLinkStoreAtCapacity):
			status, kind = http.StatusTooManyRequests, "capacity_exceeded"
		case errors.Is(err, ErrShareLinkLedgerAppendFailed):
			status, kind = http.StatusServiceUnavailable, "ledger_unavailable"
		}
		writeJSONError(w, status, kind, err.Error())
		return
	}
	ev := map[string]interface{}{
		"request_id": pr.id, "code_hash": pairingHash(pr.code), "kind": pr.kind,
		"link_id": link.LinkID, "approver": appr.id, "name": pr.name,
	}
	if pr.pubDER != nil {
		ev["public_key"] = base64.StdEncoding.EncodeToString(pr.pubDER)
		ev["key_fingerprint"] = hex.EncodeToString(fingerprint(pr.pubDER))
	}
	if err := ps.append("pairing.approved", ev); err != nil {
		_, _ = s.shareLinks.Revoke(link.LinkID) // keep the two ledgers in step: no approval event, no link
		writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "approval NOT applied: "+err.Error())
		return
	}
	pr.token, pr.link, pr.state, pr.approvedB = token, link, pairingApproved, appr.id
	close(pr.done)
	writeJSONResp(w, http.StatusOK, map[string]any{
		"approved": true, "request_id": pr.id, "name": pr.name, "link": viewOfShareLink(link),
	})
}

// checkNoEscalation: a share link approving may mint only what it could itself
// act as. Same actor, role no higher, scopes inside its own.
func checkNoEscalation(approver *ShareLink, in ShareLinkImport) error {
	if in.Actor != approver.Actor {
		return fmt.Errorf("a share link may approve only for its own actor")
	}
	if roleRank[in.Role] > roleRank[approver.Role] {
		return fmt.Errorf("a share link cannot approve a role above its own")
	}
	have := make(map[string]struct{}, len(approver.Scope))
	for _, sc := range approver.Scope {
		have[sc] = struct{}{}
	}
	for _, sc := range in.Scope {
		if _, ok := have[sc]; !ok {
			return fmt.Errorf("scope %q is outside the approving link's own scope", sc)
		}
	}
	return nil
}

func (s *Server) handlePairingDeny(w http.ResponseWriter, r *http.Request) {
	ps := s.pairingOr503(w)
	if ps == nil {
		return
	}
	appr, ok := s.authorizePairingApprover(w, r)
	if !ok {
		return
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pruneLocked(s.shareLinks)
	pr := ps.lookupLocked(r.PathValue("code"))
	if pr == nil || pr.state != pairingPending {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such code (expired?)")
		return
	}
	if err := ps.append("pairing.denied", map[string]interface{}{
		"request_id": pr.id, "code_hash": pairingHash(pr.code), "approver": appr.id,
	}); err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "denial NOT applied: "+err.Error())
		return
	}
	pr.state = pairingDenied
	close(pr.done)
	writeJSONResp(w, http.StatusOK, map[string]any{"denied": true, "request_id": pr.id})
}
