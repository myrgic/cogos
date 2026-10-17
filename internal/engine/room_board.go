// room_board.go - the ledger-backed room board store (stage-into-kernel
// campaign, slice P4-3).
//
// A room's board is the union of per-node append-only logs of room entries
// (RFC rooms-as-declared-resources section 6.9.1). This file persists one
// accepted entry per ledger event in the node's `rooms` bucket and folds the
// accepted set with the provider's own admission and fold code
// (internal/providers/room) — no fold logic is duplicated here. The store is
// the durability and verification seam; the provider is the semantics.
//
// Durability follows the ShareLinkStore pattern (the same one run_board.go
// uses): every mutation appends a ledger event BEFORE touching memory
// (write-ahead), and RebuildRoomBoardStoreFromLedger replays the bucket at
// boot. Rebuilding re-evaluates every entry through room.Evaluate, so a
// restart yields the same fold hash as the process that wrote it.
//
// Node identity: a room entry is signed by the authoring node's BEP P-256
// device key. NodeDeviceKey loads that key when one exists on disk — it never
// mints one and never requires BEP transport to be enabled. Only the key's
// EXISTENCE and its derived DeviceID are ever inspected; no key material is
// logged, and the DER certificate (public) is the only thing carried into the
// ledger so a restart can re-verify the node's own log.
package engine

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myrgic/cogos/internal/providers/room"
	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// RoomsLedgerBucket is the ledger bucket (.cog/ledger/rooms/) holding the
// node's accepted room board entries.
const RoomsLedgerBucket = "rooms"

// RoomEntryEventType is the ledger event type of one accepted room entry. It
// lives in the `board.*` family so /v1/events?event_type=board.* selects room
// board traffic; the event's `room` and `actor` data fields are what the
// room=/actor= filters match.
const RoomEntryEventType = "board.entry"

// Store errors. The HTTP layer maps them: invalid 400, not found 404, not
// member 403, held/conflict 409, ledger 503.
var (
	ErrRoomInvalid      = errors.New("room board: invalid entry")
	ErrRoomNotFound     = errors.New("room board: no such room")
	ErrRoomNotMember    = errors.New("room board: caller is not a member of the room")
	ErrRoomHeld         = errors.New("room board: entry is held")
	ErrRoomConflict     = errors.New("room board: conflict")
	ErrRoomLedgerFailed = errors.New("room board: ledger append failed")
	// ErrRoomUnavailable is the kernel-authored write path with no node
	// device key: nothing can be signed, so the routes are 503.
	ErrRoomUnavailable = errors.New("room board: no node device key")
)

// roomNodeBinding is the Binding room.Evaluate consults at the genesis: the
// genesis' authoring log is the node this replica admits as the owner's
// authoring node. It is derived from the genesis entry's own log, never from
// the live node key, so a rebuild folds exactly as the original did even when
// no key is loaded.
type roomNodeBinding struct {
	device string
}

// NodeBoundTo implements room.Binding. An empty device binds no one (fails
// closed, like room.StubBinding with no entries).
func (b roomNodeBinding) NodeBoundTo(person, deviceID string) bool {
	return b.device != "" && deviceID == b.device
}

// RoomCaller names who is appending or reading. Actor is the member id the
// caller's verified grant acts for ("" for a legacy surface grant); Admin is
// set for the kernel's own node-root authority and for a server running with
// the grant gate disabled.
type RoomCaller struct {
	Actor string
	Admin bool
}

// RoomEntryView is the client-facing shape of one stored entry.
type RoomEntryView struct {
	ID    string `json:"id"`
	Room  string `json:"room"`
	Log   string `json:"log"`
	Seq   uint64 `json:"seq"`
	Kind  string `json:"kind"`
	Actor string `json:"actor"`
	HLC   string `json:"hlc"`
	Spec  string `json:"spec,omitempty"`
}

// RoomSummary is one row of GET /v1/rooms.
type RoomSummary struct {
	ID      string `json:"id"`
	Alias   string `json:"alias,omitempty"`
	Name    string `json:"name,omitempty"`
	Owner   string `json:"owner,omitempty"`
	Gen     int    `json:"gen"`
	Entries int    `json:"entries"`
}

// NodeDeviceKey is this node's BEP P-256 device identity in the form the room
// board needs: the private key to sign this node's log, the certificate DER
// the admission path verifies against, and the derived DeviceID. No method or
// log statement ever exposes priv or the key file's contents.
type NodeDeviceKey struct {
	deviceID string
	priv     *ecdsa.PrivateKey
	certDER  []byte
}

// DeviceID returns the canonical (formatted) DeviceID of this node's key.
func (k *NodeDeviceKey) DeviceID() string { return k.deviceID }

// LoadNodeDeviceKey loads the BEP device key at certDir without minting one.
// It requires BOTH bep-cert.pem and bep-key.pem to be present and usable; a
// directory with a cert but no key is rejected (the key's presence is the
// thing that is checked). It returns an error, never a partial key, when no
// usable identity exists.
func LoadNodeDeviceKey(certDir string) (*NodeDeviceKey, error) {
	if certDir == "" {
		return nil, errors.New("room board: no cert dir")
	}
	if _, err := os.Stat(filepath.Join(certDir, "bep-key.pem")); err != nil {
		return nil, fmt.Errorf("room board: node key not present: %w", err)
	}
	cert, err := bep.LoadBEPCert(certDir)
	if err != nil {
		return nil, fmt.Errorf("room board: load node key: %w", err)
	}
	priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("room board: node key is not an ECDSA private key")
	}
	if len(cert.Certificate) == 0 {
		return nil, errors.New("room board: node certificate chain is empty")
	}
	id, err := bep.DeviceIDFromTLSCert(&cert)
	if err != nil {
		return nil, fmt.Errorf("room board: node device id: %w", err)
	}
	return &NodeDeviceKey{
		deviceID: bep.FormatDeviceID(id),
		priv:     priv,
		certDER:  cert.Certificate[0],
	}, nil
}

// roomRec is the in-memory state of one room.
type roomRec struct {
	id         string
	alias      string // the client's label for this room ("" when none)
	entries    []*room.Entry
	certs      [][]byte
	genesisLog string
	spec       *room.Spec
	board      *room.Board
	hash       string
	dirty      bool // certs changed (e.g. ConfigureNodeKey): refold before read
}

// RoomBoardStore is the ledger-backed room board.
type RoomBoardStore struct {
	mu            sync.RWMutex
	rooms         map[string]*roomRec
	order         []string
	aliases       map[string]string // client alias -> room id
	appenders     map[string]*room.Appender
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
	key           *NodeDeviceKey
	now           func() time.Time
}

// NewRoomBoardStore returns an empty store. workspaceRoot "" means in-memory.
func NewRoomBoardStore(workspaceRoot string) *RoomBoardStore {
	return &RoomBoardStore{
		rooms:         make(map[string]*roomRec),
		aliases:       make(map[string]string),
		appenders:     make(map[string]*room.Appender),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
		now:           func() time.Time { return time.Now().UTC() },
	}
}

// ConfigureNodeKey loads this node's BEP device key from certDir (never
// minting one) so the node can author and verify its own log. A missing key
// is not fatal: the store still folds and serves rooms it can verify from
// certs already in the ledger. Rooms are marked dirty so the next read
// refolds with the node's certificate available.
func (st *RoomBoardStore) ConfigureNodeKey(certDir string) error {
	key, err := LoadNodeDeviceKey(certDir)
	if err != nil {
		return err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.key = key
	// Appenders hold the key that signed what is already on the log; a key
	// change invalidates them, so they are rebuilt from the stored entries.
	st.appenders = make(map[string]*room.Appender)
	for _, rec := range st.rooms {
		rec.dirty = true
	}
	return nil
}

// hasCertDER reports whether der is already in certs. Certificates are DER
// byte strings, so the test is byte equality.
func hasCertDER(certs [][]byte, der []byte) bool {
	for _, c := range certs {
		if bytes.Equal(c, der) {
			return true
		}
	}
	return false
}

// certListLocked returns the DER certificates available for a room: those
// carried in the ledger plus this node's own certificate once a key is loaded.
// It is the single place that assembles the cert list, and it never emits the
// same certificate twice — a certificate that reached the room both through
// the ledger and through this node's key is listed once. Caller holds st.mu.
func (st *RoomBoardStore) certListLocked(rec *roomRec) [][]byte {
	out := make([][]byte, 0, len(rec.certs)+1)
	for _, c := range rec.certs {
		if len(c) == 0 || hasCertDER(out, c) {
			continue
		}
		out = append(out, c)
	}
	if st.key != nil && len(st.key.certDER) > 0 && !hasCertDER(out, st.key.certDER) {
		out = append(out, st.key.certDER)
	}
	return out
}

// foldRoom evaluates and folds one room's entries with the provider's own
// admission and board code. bindingDevice is the log the genesis is bound to
// ("" binds no one: every genesis is refused, which is correct before the
// genesis is known).
func foldRoom(roomID string, entries []*room.Entry, certs [][]byte, bindingDevice string) (*room.Outcome, *room.Board, string, error) {
	p := room.Params{Room: roomID, Certs: certs}
	if bindingDevice != "" {
		p.Binding = roomNodeBinding{device: bindingDevice}
	}
	out := room.Evaluate(p, entries)
	board := room.FoldBoard(out, entries)
	hash, err := room.FoldHash(board)
	if err != nil {
		return out, board, "", err
	}
	return out, board, hash, nil
}

func lastSpec(out *room.Outcome) *room.Spec {
	if out == nil || len(out.Chain) == 0 {
		return nil
	}
	return out.Chain[len(out.Chain)-1]
}

// ensureFoldedLocked refolds a room marked dirty, swapping in the new fold
// only when the refold succeeds. Caller holds st.mu for writing.
func (st *RoomBoardStore) ensureFoldedLocked(rec *roomRec) {
	if !rec.dirty {
		return
	}
	out, board, hash, err := foldRoom(rec.id, rec.entries, st.certListLocked(rec), rec.genesisLog)
	if err != nil {
		return
	}
	rec.board, rec.spec, rec.hash = board, lastSpec(out), hash
	rec.dirty = false
}

// Append validates and persists one entry for a room, then folds it. raw is
// the strict JSON of a room.Entry (RFC section 6.9.1). certDER is an optional
// DER certificate delivered with the entry; when nil and the entry is on this
// node's own log, the node's own certificate is used. A certificate the room
// already holds is never recorded twice. The caller must be the genesis owner
// (genesis) or a member of the room's latest spec.
func (st *RoomBoardStore) Append(roomID string, raw []byte, certDER []byte, caller RoomCaller) (RoomEntryView, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.appendLocked(roomID, raw, certDER, caller, "")
}

// appendLocked is Append under st.mu. alias is optional: when non-empty it is
// recorded on the write-ahead event and registered as this room's idempotency
// handle (the create route's `alias`, see CreateRoom). It is "" for every
// client-signed append.
func (st *RoomBoardStore) appendLocked(roomID string, raw []byte, certDER []byte, caller RoomCaller, alias string) (RoomEntryView, error) {
	e, err := room.ParseEntry(raw)
	if err != nil {
		return RoomEntryView{}, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	id, err := e.ID()
	if err != nil {
		return RoomEntryView{}, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	if e.Kind == room.KindGenesis {
		if id != roomID {
			return RoomEntryView{}, fmt.Errorf("%w: genesis id %s does not name room %s", ErrRoomInvalid, id, roomID)
		}
		if st.key == nil || e.Log != st.key.deviceID {
			return RoomEntryView{}, fmt.Errorf("%w: genesis must be authored on this node's log", ErrRoomInvalid)
		}
	} else if e.Room != roomID {
		return RoomEntryView{}, fmt.Errorf("%w: entry names room %s, not %s", ErrRoomInvalid, e.Room, roomID)
	}

	rec := st.rooms[roomID]
	isNew := rec == nil
	if isNew {
		if e.Kind != room.KindGenesis {
			return RoomEntryView{}, fmt.Errorf("%w: %s", ErrRoomNotFound, roomID)
		}
		rec = &roomRec{id: roomID, genesisLog: e.Log}
	}
	st.ensureFoldedLocked(rec)

	// Membership: the genesis may only be written by the owner it names (on
	// this node's log); every later entry needs the caller to be a member of
	// the spec in force.
	if !caller.Admin {
		if e.Kind == room.KindGenesis {
			sp, serr := room.SpecFromEntry(e)
			owner := ""
			if serr == nil {
				owner = sp.Owner
			}
			if caller.Actor == "" || caller.Actor != owner {
				return RoomEntryView{}, fmt.Errorf("%w: genesis owner is %q", ErrRoomNotMember, owner)
			}
		} else if !rec.isMember(caller.Actor) {
			return RoomEntryView{}, fmt.Errorf("%w: actor %q", ErrRoomNotMember, caller.Actor)
		}
	}

	if certDER == nil && st.key != nil && e.Log == st.key.deviceID {
		certDER = st.key.certDER
	}

	entries := append(append([]*room.Entry(nil), rec.entries...), e)
	certs := st.certListLocked(rec)
	if len(certDER) > 0 && !hasCertDER(certs, certDER) {
		certs = append(certs, certDER)
	}
	binding := rec.genesisLog
	if e.Kind == room.KindGenesis {
		binding = e.Log
	}
	out, board, hash, err := foldRoom(roomID, entries, certs, binding)
	if err != nil {
		return RoomEntryView{}, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	v, ok := out.Verdict(id)
	switch {
	case !ok:
		return RoomEntryView{}, fmt.Errorf("%w: no verdict for entry", ErrRoomInvalid)
	case v.Status == room.StatusDuplicate:
		return RoomEntryView{}, fmt.Errorf("%w: entry %s already present", ErrRoomConflict, id)
	case v.Status == room.StatusHeld:
		return RoomEntryView{}, fmt.Errorf("%w: %s", ErrRoomHeld, v.Code)
	case v.Status != room.StatusAccepted:
		return RoomEntryView{}, fmt.Errorf("%w: %s", ErrRoomInvalid, v.Code)
	}

	// Write-ahead: the ledger event lands before memory changes, so a crash
	// between the two loses nothing (the replay reconstitutes the fold).
	if err := st.appendRoomEntryLocked(roomID, e, id, certDER, alias); err != nil {
		return RoomEntryView{}, err
	}

	if isNew {
		st.rooms[roomID] = rec
		st.order = append(st.order, roomID)
	}
	rec.entries = entries
	if len(certDER) > 0 && !hasCertDER(rec.certs, certDER) {
		rec.certs = append(rec.certs, certDER)
	}
	if e.Kind == room.KindGenesis {
		rec.genesisLog = e.Log
	}
	if alias != "" {
		rec.alias = alias
		st.aliases[alias] = roomID
	}
	rec.board, rec.spec, rec.hash, rec.dirty = board, lastSpec(out), hash, false

	// Keep this node's own appender for the room current: an accepted entry
	// on its log advances seq/prev, any other log becomes a dep head. The
	// appender is built from the stored entries the first time it is needed.
	if st.key != nil && st.key.priv != nil {
		if app := st.appenders[roomID]; app != nil {
			_ = app.Observe(e)
		} else {
			app = room.NewAppender(roomID, st.key.deviceID, st.key.priv)
			for _, x := range entries {
				_ = app.Observe(x)
			}
			st.appenders[roomID] = app
		}
	}
	return viewOfEntry(e, id), nil
}

// appendRoomEntryLocked writes the write-ahead ledger event for one accepted
// entry. The event's data carries `room` and `actor` (the R10c board-event
// fields the /v1/events filters match) plus the entry itself and, when known,
// the cert DER so a restart can re-verify without the key. alias, when
// non-empty, is the client's label for a kernel-created room, restored by the
// rebuild.
func (st *RoomBoardStore) appendRoomEntryLocked(roomID string, e *room.Entry, id string, certDER []byte, alias string) error {
	if st.workspaceRoot == "" {
		return nil
	}
	var entryMap map[string]any
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRoomLedgerFailed, err)
	}
	if err := json.Unmarshal(b, &entryMap); err != nil {
		return fmt.Errorf("%w: %v", ErrRoomLedgerFailed, err)
	}
	data := map[string]any{
		"room":  roomID,
		"actor": e.Actor.String(),
		"kind":  string(e.Kind),
		"log":   e.Log,
		"seq":   e.Seq,
		"id":    id,
		"spec":  e.Spec,
		"entry": entryMap,
	}
	if alias != "" {
		data["alias"] = alias
	}
	if len(certDER) > 0 {
		// A public certificate, never key material.
		data["cert"] = base64.StdEncoding.EncodeToString(certDER)
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      RoomEntryEventType,
			Timestamp: st.now().Format(time.RFC3339),
			SessionID: RoomsLedgerBucket,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "room-board"},
	}
	if err := st.appendEvent(st.workspaceRoot, RoomsLedgerBucket, env); err != nil {
		return fmt.Errorf("%w: %v", ErrRoomLedgerFailed, err)
	}
	return nil
}

func viewOfEntry(e *room.Entry, id string) RoomEntryView {
	return RoomEntryView{
		ID: id, Room: e.Room, Log: e.Log, Seq: e.Seq,
		Kind: string(e.Kind), Actor: e.Actor.String(), HLC: e.HLC, Spec: e.Spec,
	}
}

// ─── kernel-authored writes (POST /v1/rooms, POST /v1/rooms/{room}/ops) ──────
//
// A trusted local client holding a kernel grant never signs anything itself:
// the kernel authors a room's genesis and every item op on its own BEP device
// log (the key ConfigureNodeKey loaded) and appends them through the same
// admission path Append uses. One Appender per room tracks this node's log
// position; it is rebuilt from the stored entries, so a restarted node
// continues the log (seq/prev/deps) instead of colliding with it.

// roomOpKinds maps the {"op": ...} values of POST /v1/rooms/{room}/ops onto
// the board entry kinds it builds.
var roomOpKinds = map[string]room.Kind{
	"create":  room.KindItemCreate,
	"set":     room.KindItemSet,
	"append":  room.KindItemAppend,
	"archive": room.KindItemArchive,
}

// hlc stamps a kernel-authored entry. The counter is 0: entries written in one
// millisecond still order by (log, seq), which is their causal order.
func (st *RoomBoardStore) hlc() string {
	return st.now().UTC().Format("2006-01-02T15:04:05.000Z") + "/0"
}

// nodeName is this node's name in a room spec's nodes map. It is derived from
// the device key's own short form rather than any operator-supplied machine
// name, and is stable for the life of the key.
func (k *NodeDeviceKey) nodeName() string {
	name, err := room.LogShort(k.deviceID)
	if err != nil {
		return k.deviceID
	}
	return name
}

// genesisBody builds the gen-1 spec a kernel-created room starts from: the
// caller's grant actor as the sole owner member, homed on this node. title, if
// non-empty, is the spec's display name.
func (k *NodeDeviceKey) genesisBody(owner, title string) map[string]any {
	name := k.nodeName()
	body := map[string]any{
		"gen":   1,
		"owner": owner,
		"nodes": map[string]any{name: k.deviceID},
		"members": map[string]any{
			owner: map[string]any{
				"type":  room.TypePerson,
				"role":  room.RoleOwner,
				"homes": []any{name},
			},
		},
	}
	if title != "" {
		body["name"] = title
	}
	return body
}

// CreateRoom creates a room: a genesis entry (the gen-1 spec) signed on this
// node's log, with caller's grant actor as the owner member. When alias is
// given and a room already carries it, that room is returned unchanged
// (created false); otherwise created is true.
func (st *RoomBoardStore) CreateRoom(alias, title string, caller RoomCaller) (roomID string, created bool, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.key == nil || st.key.priv == nil {
		return "", false, ErrRoomUnavailable
	}
	if alias != "" {
		if id, ok := st.aliases[alias]; ok {
			if _, live := st.rooms[id]; live {
				return id, false, nil
			}
		}
	}
	owner := caller.Actor
	if owner == "" {
		// The owner must be a person member, so a grant that names nobody
		// cannot own a new room.
		return "", false, fmt.Errorf("%w: a grant actor is required to own a new room", ErrRoomInvalid)
	}
	app := room.NewAppender("", st.key.deviceID, st.key.priv)
	g, err := app.Genesis(room.Actor{Person: owner}, st.key.genesisBody(owner, title), st.hlc())
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	id, err := g.ID()
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	raw, err := json.Marshal(g)
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	if _, err := st.appendLocked(id, raw, nil, caller, alias); err != nil {
		return "", false, err
	}
	return id, true, nil
}

// ApplyOp builds one board item entry for op, signs it on this node's log for
// the room, and appends it through the same admission path Append uses. It
// returns the stored entry and the folded item after the write. item is the
// target item id; create derives its own id (<log short>-<seq> of the entry
// being written) and ignores any supplied value.
func (st *RoomBoardStore) ApplyOp(roomID, op, item string, fields map[string]any, caller RoomCaller) (RoomEntryView, *room.Item, error) {
	kind, ok := roomOpKinds[op]
	if !ok {
		return RoomEntryView{}, nil, fmt.Errorf("%w: unknown op %q", ErrRoomInvalid, op)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.key == nil || st.key.priv == nil {
		return RoomEntryView{}, nil, ErrRoomUnavailable
	}
	rec := st.rooms[roomID]
	if rec == nil {
		return RoomEntryView{}, nil, fmt.Errorf("%w: %s", ErrRoomNotFound, roomID)
	}
	st.ensureFoldedLocked(rec)
	if !caller.Admin && !rec.isMember(caller.Actor) {
		return RoomEntryView{}, nil, fmt.Errorf("%w: actor %q", ErrRoomNotMember, caller.Actor)
	}
	if rec.spec == nil {
		return RoomEntryView{}, nil, fmt.Errorf("%w: room %s has no spec", ErrRoomInvalid, roomID)
	}
	member := caller.Actor
	if caller.Admin && !rec.isMember(member) {
		// Node admin with no member identity of its own acts as the owner.
		member = rec.spec.Owner
	}
	actor, err := roomActorFor(rec, member)
	if err != nil {
		return RoomEntryView{}, nil, err
	}

	app := st.appenderLocked(rec)
	body := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		body[k] = v
	}
	if op == "create" {
		id, err := room.ItemID(st.key.deviceID, app.NextSeq())
		if err != nil {
			return RoomEntryView{}, nil, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
		}
		body["id"] = id
		item = id
	} else {
		if item == "" {
			return RoomEntryView{}, nil, fmt.Errorf("%w: item is required for op %q", ErrRoomInvalid, op)
		}
		body["item"] = item
	}

	e, err := app.Next(kind, actor, rec.spec.ID, body, st.hlc())
	if err != nil {
		st.appenders[roomID] = nil
		return RoomEntryView{}, nil, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		st.appenders[roomID] = nil
		return RoomEntryView{}, nil, fmt.Errorf("%w: %v", ErrRoomInvalid, err)
	}
	view, err := st.appendLocked(roomID, raw, nil, caller, "")
	if err != nil {
		// The appender advanced for an entry that was never stored: drop it
		// so the next op rebuilds its position from the entries that landed.
		st.appenders[roomID] = nil
		return RoomEntryView{}, nil, err
	}
	return view, rec.board.Items[item], nil
}

// roomActorFor names the actor of a kernel-authored entry from a member id.
// The room's spec says whether that id is a person or an agent.
func roomActorFor(rec *roomRec, memberID string) (room.Actor, error) {
	m, ok := rec.spec.Members[memberID]
	if !ok {
		return room.Actor{}, fmt.Errorf("%w: actor %q", ErrRoomNotMember, memberID)
	}
	if m.Type == room.TypeAgent {
		return room.Actor{Agent: memberID}, nil
	}
	return room.Actor{Person: memberID}, nil
}

// appenderLocked returns this node's appender for a room, building it from the
// stored entries the first time. Caller holds st.mu and st.key is set.
func (st *RoomBoardStore) appenderLocked(rec *roomRec) *room.Appender {
	if app := st.appenders[rec.id]; app != nil {
		return app
	}
	app := room.NewAppender(rec.id, st.key.deviceID, st.key.priv)
	for _, e := range rec.entries {
		_ = app.Observe(e)
	}
	st.appenders[rec.id] = app
	return app
}

// isMember reports whether actor names a member of the room's latest spec.
// Caller holds st.mu.
func (rec *roomRec) isMember(actor string) bool {
	if actor == "" || rec.spec == nil {
		return false
	}
	m, ok := rec.spec.Members[actor]
	return ok && m.Type != ""
}

// Entries returns the stored entries for a room, in log order.
func (st *RoomBoardStore) Entries(roomID string) []RoomEntryView {
	st.mu.RLock()
	defer st.mu.RUnlock()
	rec := st.rooms[roomID]
	if rec == nil {
		return nil
	}
	out := make([]RoomEntryView, 0, len(rec.entries))
	for _, e := range rec.entries {
		id, _ := e.ID()
		out = append(out, viewOfEntry(e, id))
	}
	return out
}

// Board returns the folded board for a room.
func (st *RoomBoardStore) Board(roomID string) (*room.Board, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec := st.rooms[roomID]
	if rec == nil {
		return nil, false
	}
	st.ensureFoldedLocked(rec)
	return rec.board, true
}

// FoldHash returns the stable hash of a room's folded board, used to prove a
// restart rebuilds an equal fold.
func (st *RoomBoardStore) FoldHash(roomID string) (string, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec := st.rooms[roomID]
	if rec == nil {
		return "", fmt.Errorf("%w: %s", ErrRoomNotFound, roomID)
	}
	st.ensureFoldedLocked(rec)
	return rec.hash, nil
}

// IsMember reports whether actor is a member of the room's latest spec.
func (st *RoomBoardStore) IsMember(roomID, actor string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec := st.rooms[roomID]
	if rec == nil {
		return false
	}
	st.ensureFoldedLocked(rec)
	return rec.isMember(actor)
}

// Authorize reports whether caller may read or write a room.
func (st *RoomBoardStore) Authorize(roomID string, caller RoomCaller) bool {
	if caller.Admin {
		return true
	}
	return st.IsMember(roomID, caller.Actor)
}

// Archive returns the archived items of a room's folded board.
func (st *RoomBoardStore) Archive(roomID string) []*room.Item {
	st.mu.Lock()
	defer st.mu.Unlock()
	rec := st.rooms[roomID]
	if rec == nil {
		return nil
	}
	st.ensureFoldedLocked(rec)
	var out []*room.Item
	for _, it := range rec.board.Items {
		if it.Archived {
			out = append(out, it)
		}
	}
	return out
}

// Spec returns the room's latest accepted spec.
func (st *RoomBoardStore) Spec(roomID string) (*room.Spec, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	rec := st.rooms[roomID]
	if rec == nil || rec.spec == nil {
		return nil, false
	}
	return rec.spec, true
}

// List returns the rooms caller may see, in declaration order.
func (st *RoomBoardStore) List(caller RoomCaller) []RoomSummary {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := []RoomSummary{}
	for _, roomID := range st.order {
		rec := st.rooms[roomID]
		if rec == nil {
			continue
		}
		st.ensureFoldedLocked(rec)
		if !caller.Admin && !rec.isMember(caller.Actor) {
			continue
		}
		sum := RoomSummary{ID: roomID, Alias: rec.alias, Entries: len(rec.entries)}
		if rec.spec != nil {
			sum.Owner = rec.spec.Owner
			sum.Gen = rec.spec.Gen
			sum.Name = rec.spec.Name
		}
		out = append(out, sum)
	}
	return out
}

// Rooms returns every room id the store holds, in declaration order.
func (st *RoomBoardStore) Rooms() []string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return append([]string(nil), st.order...)
}

// RebuildRoomBoardStoreFromLedger folds the `rooms` bucket into a store. A
// missing file is an empty store; malformed lines are skipped. Each event's
// entry is re-parsed and re-evaluated, so the rebuilt fold equals the live
// one, and any cert carried in the event is restored so verification succeeds
// without the key.
func RebuildRoomBoardStoreFromLedger(workspaceRoot string) (*RoomBoardStore, error) {
	st := NewRoomBoardStore(workspaceRoot)
	if workspaceRoot == "" {
		return st, nil
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", RoomsLedgerBucket, "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, fmt.Errorf("room board: open ledger: %w", err)
	}
	defer f.Close()

	type pending struct {
		entries    []*room.Entry
		certs      [][]byte
		genesisLog string
		alias      string
	}
	recs := map[string]*pending{}
	var order []string

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env EventEnvelope
		if json.Unmarshal([]byte(line), &env) != nil || env.HashedPayload.Type != RoomEntryEventType {
			continue
		}
		d := env.HashedPayload.Data
		roomID, _ := d["room"].(string)
		if roomID == "" {
			continue
		}
		raw, err := json.Marshal(d["entry"])
		if err != nil {
			continue
		}
		e, err := room.ParseEntry(raw)
		if err != nil {
			continue
		}
		p := recs[roomID]
		if p == nil {
			p = &pending{}
			recs[roomID] = p
			order = append(order, roomID)
		}
		p.entries = append(p.entries, e)
		if e.Kind == room.KindGenesis {
			p.genesisLog = e.Log
		}
		if alias, _ := d["alias"].(string); alias != "" {
			p.alias = alias
		}
		if s, _ := d["cert"].(string); s != "" {
			if der, derr := base64.StdEncoding.DecodeString(s); derr == nil && !hasCertDER(p.certs, der) {
				p.certs = append(p.certs, der)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("room board: scan ledger: %w", err)
	}

	for _, roomID := range order {
		p := recs[roomID]
		out, board, hash, err := foldRoom(roomID, p.entries, p.certs, p.genesisLog)
		if err != nil {
			return st, fmt.Errorf("room board: fold %s: %w", roomID, err)
		}
		st.rooms[roomID] = &roomRec{
			id:         roomID,
			alias:      p.alias,
			entries:    p.entries,
			certs:      p.certs,
			genesisLog: p.genesisLog,
			spec:       lastSpec(out),
			board:      board,
			hash:       hash,
		}
		if p.alias != "" {
			st.aliases[p.alias] = roomID
		}
		st.order = append(st.order, roomID)
	}
	return st, nil
}
