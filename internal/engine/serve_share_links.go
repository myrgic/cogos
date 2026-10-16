// serve_share_links.go — the share-link store (P3b slice S2).
//
// A share link is a bearer credential that a surface hands to a person or a
// device so they can reach that surface's own pages: a Stage room key, a home
// key, a trusted-device cookie. It is NOT an identity grant, and it lives in a
// separate store on purpose (declared-identity-federation ID-L3: bearer links
// are leaves; they mint nothing and never act at a core).
//
// Why separate from IdentityGrantRegistry:
//
//   - A grant is a kernel API credential. requiredScopeForRequest returns ""
//     for most POST routes, and "" passes any live grant, so a share link
//     stored as a grant would pass every unscoped POST on the kernel API.
//   - The grant registry holds one live record per surface and caps at
//     maxLiveGrantSurfaces (256). A deployment has hundreds of keys; they must
//     not compete with surface grants for slots.
//
// The store therefore resolves links only inside the Stage UI listener and the
// pairing routes (later slices). grantAuthMiddleware refuses any token that
// resolves here with 403 on every non-exempt route, so a share link can never
// be presented as X-Cogos-Grant, Authorization: Bearer, or x-api-key.
//
// Durability follows the grant registry: every mutation appends a ledger event
// BEFORE touching memory (write-ahead), the ledger holds the sha256 hash and
// never the raw token, and RebuildShareLinkStoreFromLedger replays the ledger
// at boot. Links import by hash (the Stage's existing keys are already stored
// hashed or can be hashed there), so nobody re-pairs. Links do not expire by
// default; revoking one is how it ends.
package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// shareLinksLedgerSession is the ledger bucket for share-link events, at
// .cog/ledger/identity-share-links/events.jsonl.
const shareLinksLedgerSession = "identity-share-links"

// ScopeStageKeys authorizes importing, resolving, listing and revoking share
// links under the "stage/" surface prefix. The Stage service's own grant
// (stage-studio) carries it; no share link can.
const ScopeStageKeys = "stage:keys"

// shareLinkRoutePrefix is gated on EVERY method by requiredScopeForRequest and
// excluded from the blanket GET exemption (isGrantExemptRequest).
const shareLinkRoutePrefix = "/v1/stage/keys"

// shareLinkSurfacePrefix is the only namespace the stage:keys scope reaches.
const shareLinkSurfacePrefix = "stage/"

// maxShareLinks bounds the store. The Stage holds tens of keys today; this is
// a safety net against an authorized caller looping, not a working limit.
const maxShareLinks = 10000

// maxShareLinkImportBatch bounds one import request.
const maxShareLinkImportBatch = 2000

// Link kinds. "share" is a room/home key, "device" a trusted-device cookie.
const (
	ShareLinkKindShare  = "share"
	ShareLinkKindDevice = "device"
)

var (
	// ErrShareLinkStoreAtCapacity maps to 429.
	ErrShareLinkStoreAtCapacity = errors.New("share-link store at capacity")
	// ErrShareLinkNotFound maps to 404.
	ErrShareLinkNotFound = errors.New("no live share link with that id")
	// ErrShareLinkLedgerAppendFailed maps to 503; memory is untouched.
	ErrShareLinkLedgerAppendFailed = errors.New("identity-share-links ledger append failed")
)

var (
	shareLinkHashRe    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	shareLinkRoomRe    = regexp.MustCompile(`^[A-Za-z0-9_-]{0,64}$`)
	shareLinkSurfaceRe = regexp.MustCompile(`^` + regexp.QuoteMeta(shareLinkSurfacePrefix) + `[A-Za-z0-9._/-]{1,200}$`)
	shareLinkScopeRe   = regexp.MustCompile(`^stage:[A-Za-z0-9._:-]{1,100}$`)
)

// ShareLink is the record. IntegrityHash is the sha256 hex of the raw token
// (the same primitive as IdentityGrant, sha256Hex in process.go). It is never
// serialized to API responses. A ShareLink is immutable after publication in
// the store: replace the pointer, never mutate fields.
type ShareLink struct {
	LinkID        string
	Kind          string
	Surface       string
	Actor         string
	Role          string
	Room          string
	Scope         []string
	IntegrityHash string
	Parent        string // grant id of the surface that issued the link
	IssuedAt      time.Time
	ExpiresAt     time.Time // zero = does not expire
	Confessed     string    // e.g. "pre-B0": a recorded deviation, empty if none
}

func (l *ShareLink) expired(now time.Time) bool {
	return !l.ExpiresAt.IsZero() && now.After(l.ExpiresAt)
}

// ShareLinkImport is one record to import. The raw token never appears.
type ShareLinkImport struct {
	Kind          string   `json:"kind"`
	Surface       string   `json:"surface"`
	Actor         string   `json:"actor"`
	Role          string   `json:"role"`
	Room          string   `json:"room,omitempty"`
	Scope         []string `json:"scope"`
	IntegrityHash string   `json:"integrity_hash"`
	TTLHours      float64  `json:"ttl_hours,omitempty"`
	Confessed     string   `json:"confessed,omitempty"`
}

func (in *ShareLinkImport) validate() error {
	if in.Kind != ShareLinkKindShare && in.Kind != ShareLinkKindDevice {
		return fmt.Errorf("kind must be %q or %q", ShareLinkKindShare, ShareLinkKindDevice)
	}
	if !shareLinkSurfaceRe.MatchString(in.Surface) {
		return fmt.Errorf("surface must match %s", shareLinkSurfaceRe)
	}
	if !validActorID(in.Actor) {
		return fmt.Errorf("actor must match %s", actorIDPattern)
	}
	switch in.Role {
	case "owner", "collaborator", "guest":
	default:
		return fmt.Errorf("role must be owner, collaborator or guest")
	}
	if !shareLinkRoomRe.MatchString(in.Room) {
		return fmt.Errorf("room must match %s", shareLinkRoomRe)
	}
	if len(in.Scope) == 0 || len(in.Scope) > 16 {
		return fmt.Errorf("scope must hold 1 to 16 entries")
	}
	for _, sc := range in.Scope {
		if !shareLinkScopeRe.MatchString(sc) {
			return fmt.Errorf("scope entry %q must match %s", sc, shareLinkScopeRe)
		}
	}
	if !shareLinkHashRe.MatchString(in.IntegrityHash) {
		return fmt.Errorf("integrity_hash must be 64 lowercase hex characters (sha256 of the token)")
	}
	if in.TTLHours < 0 {
		return fmt.Errorf("ttl_hours must not be negative")
	}
	if len(in.Confessed) > 64 {
		return fmt.Errorf("confessed must be at most 64 characters")
	}
	return nil
}

// ShareLinkStore is the in-memory, ledger-backed store. One live link per
// Surface: importing a different hash for a surface that already has a live
// link supersedes it atomically (one ledger event).
type ShareLinkStore struct {
	mu            sync.RWMutex
	byID          map[string]*ShareLink
	bySurface     map[string]*ShareLink
	byHash        map[string]*ShareLink
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
}

// NewShareLinkStoreWithLedger returns an empty store. workspaceRoot "" means
// in-memory only.
func NewShareLinkStoreWithLedger(workspaceRoot string) *ShareLinkStore {
	return &ShareLinkStore{
		byID:          make(map[string]*ShareLink),
		bySurface:     make(map[string]*ShareLink),
		byHash:        make(map[string]*ShareLink),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
	}
}

// Import adds a link by hash. Idempotent: importing a hash that is already
// live for the same surface returns the existing link and writes nothing.
// A hash already live under a different surface is refused (one token, one
// meaning). parent is the issuing grant id (may be "" for in-process callers).
func (st *ShareLinkStore) Import(in ShareLinkImport, parent string) (*ShareLink, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()

	st.mu.Lock()
	defer st.mu.Unlock()

	if existing, ok := st.byHash[in.IntegrityHash]; ok && !existing.expired(now) {
		if existing.Surface != in.Surface {
			return nil, fmt.Errorf("integrity_hash is already live for another surface")
		}
		return existing, nil
	}

	// The surface's current link is superseded whether or not it has expired:
	// an expired predecessor must still be dropped and named in the ledger, or
	// it stays indexed (and counted against maxShareLinks) here and on rebuild.
	old := st.bySurface[in.Surface]
	if old == nil {
		if len(st.byID) >= maxShareLinks {
			st.evictExpiredLocked(now)
		}
		if len(st.byID) >= maxShareLinks {
			return nil, fmt.Errorf("%w: %d live links", ErrShareLinkStoreAtCapacity, maxShareLinks)
		}
	}

	link := &ShareLink{
		LinkID:        "link-" + mustHex(8),
		Kind:          in.Kind,
		Surface:       in.Surface,
		Actor:         in.Actor,
		Role:          in.Role,
		Room:          in.Room,
		Scope:         append([]string(nil), in.Scope...),
		IntegrityHash: in.IntegrityHash,
		Parent:        parent,
		IssuedAt:      now,
		Confessed:     in.Confessed,
	}
	if in.TTLHours > 0 {
		link.ExpiresAt = now.Add(time.Duration(in.TTLHours * float64(time.Hour)))
	}
	superseded := ""
	if old != nil {
		superseded = old.LinkID
	}
	if err := st.appendIssuedLocked(link, superseded, now); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrShareLinkLedgerAppendFailed, err)
	}
	if old != nil {
		st.dropLocked(old)
	}
	st.insertLocked(link)
	return link, nil
}

// Resolve returns the live link whose token hashes to sha256Hex(token).
// Lookup is by hash in a map; the hash is a one-way function of the secret, so
// the lookup timing reveals nothing usable about the token.
func (st *ShareLinkStore) Resolve(token string) (*ShareLink, bool) {
	if token == "" {
		return nil, false
	}
	return st.ResolveHash(sha256Hex(token))
}

// ResolveHash is Resolve for a caller that already holds the hash.
func (st *ShareLinkStore) ResolveHash(hash string) (*ShareLink, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	l, ok := st.byHash[hash]
	if !ok || l.expired(time.Now().UTC()) {
		return nil, false
	}
	return l, true
}

// IsShareToken reports whether token is a live share link. The grant gate uses
// it to refuse share links outright.
func (st *ShareLinkStore) IsShareToken(token string) bool {
	_, ok := st.Resolve(token)
	return ok
}

// Revoke ends a link. The ledger append happens first; on append failure the
// link stays live and the error wraps ErrShareLinkLedgerAppendFailed.
func (st *ShareLinkStore) Revoke(linkID string) (*ShareLink, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	l, ok := st.byID[linkID]
	if !ok {
		return nil, ErrShareLinkNotFound
	}
	if err := st.appendRevokedLocked(l, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrShareLinkLedgerAppendFailed, err)
	}
	st.dropLocked(l)
	return l, nil
}

// Snapshot returns the live links (no tokens exist in the store to leak).
func (st *ShareLinkStore) Snapshot() []*ShareLink {
	st.mu.RLock()
	defer st.mu.RUnlock()
	now := time.Now().UTC()
	out := make([]*ShareLink, 0, len(st.byID))
	for _, l := range st.byID {
		if !l.expired(now) {
			out = append(out, l)
		}
	}
	return out
}

// Len returns the number of tracked links.
func (st *ShareLinkStore) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.byID)
}

// insertLocked indexes l, first dropping whatever already holds its hash or
// its surface. A surface has one link and a hash has one meaning, so a
// predecessor on either index (live or expired, same surface or another) is
// gone once l is in, both live and on ledger replay (cog-review on #688).
func (st *ShareLinkStore) insertLocked(l *ShareLink) {
	if prev, ok := st.byHash[l.IntegrityHash]; ok && prev.LinkID != l.LinkID {
		st.dropLocked(prev)
	}
	if prev, ok := st.bySurface[l.Surface]; ok && prev.LinkID != l.LinkID {
		st.dropLocked(prev)
	}
	st.byID[l.LinkID] = l
	st.bySurface[l.Surface] = l
	st.byHash[l.IntegrityHash] = l
}

func (st *ShareLinkStore) dropLocked(l *ShareLink) {
	delete(st.byID, l.LinkID)
	if cur, ok := st.bySurface[l.Surface]; ok && cur.LinkID == l.LinkID {
		delete(st.bySurface, l.Surface)
	}
	if cur, ok := st.byHash[l.IntegrityHash]; ok && cur.LinkID == l.LinkID {
		delete(st.byHash, l.IntegrityHash)
	}
}

func (st *ShareLinkStore) evictExpiredLocked(now time.Time) {
	for _, l := range st.byID {
		if l.expired(now) {
			st.dropLocked(l)
		}
	}
}

// ─── ledger ──────────────────────────────────────────────────────────────────

func (st *ShareLinkStore) linkData(l *ShareLink) map[string]interface{} {
	d := map[string]interface{}{
		"link_id":        l.LinkID,
		"kind":           l.Kind,
		"surface":        l.Surface,
		"actor":          l.Actor,
		"role":           l.Role,
		"room":           l.Room,
		"scope":          l.Scope,
		"integrity_hash": l.IntegrityHash,
		"parent":         l.Parent,
		"issued_at":      l.IssuedAt.Format(time.RFC3339),
		"confessed":      l.Confessed,
	}
	if !l.ExpiresAt.IsZero() {
		d["expires_at"] = l.ExpiresAt.Format(time.RFC3339)
	}
	return d
}

func (st *ShareLinkStore) appendLocked(eventType string, data map[string]interface{}, now time.Time) error {
	if st.workspaceRoot == "" {
		return nil
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: now.Format(time.RFC3339),
			SessionID: shareLinksLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-share-links"},
	}
	return st.appendEvent(st.workspaceRoot, shareLinksLedgerSession, env)
}

func (st *ShareLinkStore) appendIssuedLocked(l *ShareLink, supersededLinkID string, now time.Time) error {
	d := st.linkData(l)
	if supersededLinkID != "" {
		d["superseded_link_id"] = supersededLinkID
	}
	return st.appendLocked("identity.share.issued", d, now)
}

func (st *ShareLinkStore) appendRevokedLocked(l *ShareLink, now time.Time) error {
	d := st.linkData(l)
	d["revoked_at"] = now.Format(time.RFC3339)
	return st.appendLocked("identity.share.revoked", d, now)
}

// RebuildShareLinkStoreFromLedger replays the share-link ledger. A missing file
// is an empty store; malformed lines are skipped.
func RebuildShareLinkStoreFromLedger(workspaceRoot string) (*ShareLinkStore, error) {
	st := NewShareLinkStoreWithLedger(workspaceRoot)
	if workspaceRoot == "" {
		return st, nil
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", shareLinksLedgerSession, "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, fmt.Errorf("open share-link ledger: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			continue
		}
		st.applyLedgerEvent(&env)
	}
	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("scan share-link ledger: %w", err)
	}
	return st, nil
}

// applyLedgerEvent runs only during rebuild, before the store is published.
func (st *ShareLinkStore) applyLedgerEvent(env *EventEnvelope) {
	d := env.HashedPayload.Data
	id, _ := d["link_id"].(string)
	if id == "" {
		return
	}
	switch env.HashedPayload.Type {
	case "identity.share.issued":
		str := func(k string) string { s, _ := d[k].(string); return s }
		l := &ShareLink{
			LinkID:        id,
			Kind:          str("kind"),
			Surface:       str("surface"),
			Actor:         str("actor"),
			Role:          str("role"),
			Room:          str("room"),
			Scope:         stringSliceFromAny(d["scope"]),
			IntegrityHash: str("integrity_hash"),
			Parent:        str("parent"),
			IssuedAt:      parseRFC3339Lenient(d["issued_at"]),
			Confessed:     str("confessed"),
		}
		if _, has := d["expires_at"]; has {
			l.ExpiresAt = parseRFC3339Lenient(d["expires_at"])
		}
		if l.Surface == "" || l.IntegrityHash == "" {
			return
		}
		if sup, _ := d["superseded_link_id"].(string); sup != "" {
			if old, ok := st.byID[sup]; ok {
				st.dropLocked(old)
			}
		}
		st.insertLocked(l)
	case "identity.share.revoked":
		if l, ok := st.byID[id]; ok {
			st.dropLocked(l)
		}
	}
}

// ─── routes ──────────────────────────────────────────────────────────────────
//
// Every route is POST (the list too), carries a body, and sits under
// /v1/stage/keys, which requiredScopeForRequest maps to ScopeStageKeys on all
// methods. Responses never contain a token or a hash.

func (s *Server) registerShareLinkRoutes(mux *http.ServeMux) {
	s.route(mux, "POST /v1/stage/keys/import", s.handleShareLinkImport)
	s.route(mux, "POST /v1/stage/keys/resolve", s.handleShareLinkResolve)
	s.route(mux, "POST /v1/stage/keys/list", s.handleShareLinkList)
	s.route(mux, "POST /v1/stage/keys/{id}/revoke", s.handleShareLinkRevoke)
}

type shareLinkView struct {
	LinkID    string   `json:"link_id"`
	Kind      string   `json:"kind"`
	Surface   string   `json:"surface"`
	Actor     string   `json:"actor"`
	Role      string   `json:"role"`
	Room      string   `json:"room,omitempty"`
	Scope     []string `json:"scope"`
	Parent    string   `json:"parent,omitempty"`
	IssuedAt  string   `json:"issued_at"`
	ExpiresAt string   `json:"expires_at,omitempty"`
	Confessed string   `json:"confessed,omitempty"`
}

func viewOfShareLink(l *ShareLink) shareLinkView {
	v := shareLinkView{
		LinkID: l.LinkID, Kind: l.Kind, Surface: l.Surface, Actor: l.Actor, Role: l.Role,
		Room: l.Room, Scope: l.Scope, Parent: l.Parent,
		IssuedAt: l.IssuedAt.Format(time.RFC3339), Confessed: l.Confessed,
	}
	if !l.ExpiresAt.IsZero() {
		v.ExpiresAt = l.ExpiresAt.Format(time.RFC3339)
	}
	return v
}

func (s *Server) shareLinkStoreOr503(w http.ResponseWriter) *ShareLinkStore {
	if s.shareLinks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "share-link store not initialised")
		return nil
	}
	return s.shareLinks
}

type shareLinkImportRequest struct {
	Links []ShareLinkImport `json:"links"`
}

// handleShareLinkImport imports one or many links by hash. The batch is
// validated whole before anything is written, so a bad record imports nothing.
func (s *Server) handleShareLinkImport(w http.ResponseWriter, r *http.Request) {
	st := s.shareLinkStoreOr503(w)
	if st == nil {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	var req shareLinkImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON: {\"links\":[...]}")
		return
	}
	if len(req.Links) == 0 || len(req.Links) > maxShareLinkImportBatch {
		writeJSONError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("links must hold 1 to %d records", maxShareLinkImportBatch))
		return
	}
	for i := range req.Links {
		if err := req.Links[i].validate(); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("links[%d]: %v", i, err))
			return
		}
	}
	parent := ""
	if g, ok := grantFromContext(r.Context()); ok {
		parent = g.GrantID
	}
	out := make([]shareLinkView, 0, len(req.Links))
	for i := range req.Links {
		l, err := st.Import(req.Links[i], parent)
		if err != nil {
			status, kind := http.StatusBadRequest, "invalid_request"
			switch {
			case errors.Is(err, ErrShareLinkStoreAtCapacity):
				status, kind = http.StatusTooManyRequests, "capacity_exceeded"
			case errors.Is(err, ErrShareLinkLedgerAppendFailed):
				status, kind = http.StatusServiceUnavailable, "ledger_unavailable"
			}
			writeJSONError(w, status, kind, fmt.Sprintf("links[%d] (%d imported before it): %v", i, len(out), err))
			return
		}
		out = append(out, viewOfShareLink(l))
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"links": out})
}

type shareLinkResolveRequest struct {
	Token string `json:"token"`
}

// handleShareLinkResolve answers {valid:false} for an unknown, revoked or
// expired token, and the record for a live one. Always 200, like
// /v1/identity/verify.
func (s *Server) handleShareLinkResolve(w http.ResponseWriter, r *http.Request) {
	st := s.shareLinkStoreOr503(w)
	if st == nil {
		return
	}
	var req shareLinkResolveRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON")
		return
	}
	l, ok := st.Resolve(req.Token)
	if !ok {
		writeJSONResp(w, http.StatusOK, map[string]any{"valid": false})
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"valid": true, "link": viewOfShareLink(l)})
}

func (s *Server) handleShareLinkList(w http.ResponseWriter, r *http.Request) {
	st := s.shareLinkStoreOr503(w)
	if st == nil {
		return
	}
	snap := st.Snapshot()
	out := make([]shareLinkView, 0, len(snap))
	for _, l := range snap {
		out = append(out, viewOfShareLink(l))
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"links": out})
}

func (s *Server) handleShareLinkRevoke(w http.ResponseWriter, r *http.Request) {
	st := s.shareLinkStoreOr503(w)
	if st == nil {
		return
	}
	id := r.PathValue("id")
	l, err := st.Revoke(id)
	if err != nil {
		if errors.Is(err, ErrShareLinkLedgerAppendFailed) {
			writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable",
				"revoke NOT applied; link "+id+" is still live: "+err.Error())
			return
		}
		writeJSONError(w, http.StatusNotFound, "not_found", "no live share link with id "+id)
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"revoked": true, "link_id": l.LinkID, "surface": l.Surface})
}
