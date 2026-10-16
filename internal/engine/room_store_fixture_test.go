package engine

// room_store_fixture_test.go — fixtures for the P4-3 room-board store tests.
//
// The world is two nodes: node-a is THIS node (its BEP device key is a fixture
// key generated inside the test's temp dir, never a real key on disk), and
// node-b is a collaborator's node that holds a key and a spec.nodes entry but
// authors nothing here. Entries are built with the provider's own Appender, so
// the tests exercise the real signing and canonical-id path.
//
// The fixture deliberately speaks only the provider's EXPORTED API
// (internal/providers/room): the store under test folds the same entries
// through the same admission code the provider's own tests use.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/myrgic/cogos/internal/providers/room"
	"github.com/myrgic/cogos/pkg/substrate/bep"
)

type roomStoreFixture struct {
	t       *testing.T
	certDir string // node-a: this node's cert dir (a temp fixture key)
	key     *NodeDeviceKey
	peerDev string // node-b's canonical DeviceID (never authors anything)

	apps  map[string]*room.Appender // room id -> this node's log appender
	gens  map[string]*room.Entry    // room id -> genesis entry
	seqs  map[string]uint64         // room id -> entries written so far
	rooms []string                  // room ids in declaration order
	n     int
}

// newRoomStoreFixture builds the two-node world and declares no room yet.
func newRoomStoreFixture(t *testing.T) *roomStoreFixture {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "node-a")
	peerDir := filepath.Join(base, "node-b")
	for _, d := range []string{dir, peerDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
		if err := bep.GenerateBEPCert(d); err != nil {
			t.Fatalf("generate fixture cert in %s: %v", d, err)
		}
	}
	key, err := LoadNodeDeviceKey(dir)
	if err != nil {
		t.Fatalf("LoadNodeDeviceKey: %v", err)
	}
	peerCert, err := bep.LoadBEPCert(peerDir)
	if err != nil {
		t.Fatalf("load node-b cert: %v", err)
	}
	peerID, err := bep.DeviceIDFromTLSCert(&peerCert)
	if err != nil {
		t.Fatalf("node-b device id: %v", err)
	}
	return &roomStoreFixture{
		t: t, certDir: dir, key: key, peerDev: bep.FormatDeviceID(peerID),
		apps: map[string]*room.Appender{}, gens: map[string]*room.Entry{}, seqs: map[string]uint64{},
	}
}

func (f *roomStoreFixture) hlc() string {
	f.n++
	return fmt.Sprintf("2026-10-16T09:%02d:%02d.000Z/0", (f.n/60)%60, f.n%60)
}

// roomID returns the i'th declared room id.
func (f *roomStoreFixture) roomID(i int) string { return f.rooms[i] }

// genesisBody declares node-a's owner (chaz), node-b's collaborator (vroku)
// and one agent homed on node-a.
func (f *roomStoreFixture) genesisBody(name string) map[string]any {
	return map[string]any{
		"name":  name,
		"gen":   1,
		"owner": "chaz",
		"nodes": map[string]any{
			"node-a": f.key.DeviceID(),
			"node-b": f.peerDev,
		},
		"members": map[string]any{
			"chaz":        map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a"}},
			"vroku":       map[string]any{"type": "person", "role": "collaborator", "homes": []any{"node-b"}},
			"cog@node-a":  map[string]any{"type": "agent", "role": "agent", "node": "node-a", "class": "full"},
			"sage@node-b": map[string]any{"type": "agent", "role": "agent", "node": "node-b", "class": "collab"},
		},
		"dispatch": map[string]any{"node": "node-a"},
		"floor":    map[string]any{"priority": []any{"cog@node-a", "sage@node-b"}},
		"scope":    map[string]any{"pipelines": []any{"review"}},
	}
}

// declare writes a room's genesis on this node's log and returns the room id.
// The genesis IS the gen-1 spec entry, so its id is the spec new entries cite.
func (f *roomStoreFixture) declare(name string) string {
	f.t.Helper()
	app := room.NewAppender("", f.key.DeviceID(), f.key.priv)
	g, err := app.Genesis(room.Actor{Person: "chaz"}, f.genesisBody(name), f.hlc())
	if err != nil {
		f.t.Fatalf("genesis %s: %v", name, err)
	}
	id, err := g.ID()
	if err != nil {
		f.t.Fatalf("genesis id: %v", err)
	}
	f.apps[id] = app
	f.gens[id] = g
	f.seqs[id] = 1
	f.rooms = append(f.rooms, id)
	return id
}

// mk appends one entry to a room's log and returns it.
func (f *roomStoreFixture) mk(roomID string, kind room.Kind, actor room.Actor, body map[string]any) *room.Entry {
	f.t.Helper()
	app := f.apps[roomID]
	if app == nil {
		f.t.Fatalf("no appender for room %s", roomID)
	}
	e, err := app.Next(kind, actor, roomID, body, f.hlc())
	if err != nil {
		f.t.Fatalf("%s: %v", kind, err)
	}
	f.seqs[roomID]++
	return e
}

// itemCreate creates an item and returns the entry and its item id.
func (f *roomStoreFixture) itemCreate(roomID string, actor room.Actor, extra map[string]any) (*room.Entry, string) {
	f.t.Helper()
	seq := f.seqs[roomID] + 1
	id, err := room.ItemID(f.key.DeviceID(), seq)
	if err != nil {
		f.t.Fatalf("item id: %v", err)
	}
	body := map[string]any{"id": id, "text": "an item", "section": "tasks", "state": "open"}
	for k, v := range extra {
		body[k] = v
	}
	return f.mk(roomID, room.KindItemCreate, actor, body), id
}

func (f *roomStoreFixture) genesisJSON(roomID string) []byte {
	f.t.Helper()
	return roomJSON(f.t, f.gens[roomID])
}

func roomJSON(t *testing.T, e *room.Entry) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	return b
}

func roomJSONMap(t *testing.T, e *room.Entry) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := json.Unmarshal(roomJSON(t, e), &m); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	return m
}

// seedRoom folds a genesis, one item.create and one item.set into a store
// through the store's own append path. Returns the item id.
func seedRoom(t *testing.T, st *RoomBoardStore, f *roomStoreFixture, roomID string) string {
	t.Helper()
	owner := RoomCaller{Actor: "chaz"}
	if _, err := st.Append(roomID, f.genesisJSON(roomID), nil, owner); err != nil {
		t.Fatalf("append genesis: %v", err)
	}
	create, itemID := f.itemCreate(roomID, room.Actor{Person: "chaz"}, nil)
	if _, err := st.Append(roomID, roomJSON(t, create), nil, owner); err != nil {
		t.Fatalf("append item.create: %v", err)
	}
	set := f.mk(roomID, room.KindItemSet, room.Actor{Person: "chaz"},
		map[string]any{"item": itemID, "field": "state", "value": "done"})
	if _, err := st.Append(roomID, roomJSON(t, set), nil, owner); err != nil {
		t.Fatalf("append item.set: %v", err)
	}
	return itemID
}
