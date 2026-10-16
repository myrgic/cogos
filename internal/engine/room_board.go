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
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
	key           *NodeDeviceKey
	now           func() time.Time
}

// NewRoomBoardStore returns an empty store. workspaceRoot "" means in-memory.
func NewRoomBoardStore(workspaceRoot string) *RoomBoardStore {
	return &RoomBoardStore{
		rooms:         make(map[string]*roomRec),
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
	for _, rec := range st.rooms {
		rec.dirty = true
	}
	return nil
}

// certListLocked returns the DER certificates available for a room: those
// carried in the ledger plus this node's own certificate once a key is loaded.
// Caller holds st.mu.
func (st *RoomBoardStore) certListLocked(rec *roomRec) [][]byte {
	out := make([][]byte, 0, len(rec.certs)+1)
	out = append(out, rec.certs...)
	if st.key != nil && len(st.key.certDER) > 0 {
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
// node's own log, the node's own certificate is used. The caller must be the
// genesis owner (genesis) or a member of the room's latest spec.
func (st *RoomBoardStore) Append(roomID string, raw []byte, certDER []byte, caller RoomCaller) (RoomEntryView, error) {
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

	st.mu.Lock()
	defer st.mu.Unlock()

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
	if len(certDER) > 0 {
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
	if err := st.appendRoomEntryLocked(roomID, e, id, certDER); err != nil {
		return RoomEntryView{}, err
	}

	if isNew {
		st.rooms[roomID] = rec
		st.order = append(st.order, roomID)
	}
	rec.entries = entries
	if len(certDER) > 0 {
		rec.certs = append(rec.certs, certDER)
	}
	if e.Kind == room.KindGenesis {
		rec.genesisLog = e.Log
	}
	rec.board, rec.spec, rec.hash, rec.dirty = board, lastSpec(out), hash, false
	return viewOfEntry(e, id), nil
}

// appendRoomEntryLocked writes the write-ahead ledger event for one accepted
// entry. The event's data carries `room` and `actor` (the R10c board-event
// fields the /v1/events filters match) plus the entry itself and, when known,
// the cert DER so a restart can re-verify without the key.
func (st *RoomBoardStore) appendRoomEntryLocked(roomID string, e *room.Entry, id string, certDER []byte) error {
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
		sum := RoomSummary{ID: roomID, Entries: len(rec.entries)}
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
		if s, _ := d["cert"].(string); s != "" {
			if der, derr := base64.StdEncoding.DecodeString(s); derr == nil {
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
			entries:    p.entries,
			certs:      p.certs,
			genesisLog: p.genesisLog,
			spec:       lastSpec(out),
			board:      board,
			hash:       hash,
		}
		st.order = append(st.order, roomID)
	}
	return st, nil
}
