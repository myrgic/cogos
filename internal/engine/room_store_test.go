package engine

// room_store_test.go — the P4-3 test lane for the room board store, the
// /v1/rooms routes and the room/actor event filters.
//
// Every test name starts with TestRoomStore, per the slice brief.

import (
	"bufio"
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myrgic/cogos/internal/providers/room"
	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// newTestServerAt builds the same Server newTestServer does, but on a root the
// test already holds, so the ledger the room board writes can be read back.
func newTestServerAt(t *testing.T, root string) *Server {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, ".cog", "ledger"), 0o755); err != nil {
		t.Fatalf("mkdir ledger: %v", err)
	}
	cfg := makeConfig(t, root)
	nucleus := makeNucleus("Test", "unit-tester")
	process := NewProcess(cfg, nucleus)
	return NewServer(cfg, nucleus, process)
}

// roomEntryRecord is the events-query view these tests decode.
type roomEventView struct {
	Type  string `json:"type"`
	Room  string `json:"room"`
	Actor string `json:"actor"`
	Hash  string `json:"hash"`
}

type roomEventsResponse struct {
	Count  int             `json:"count"`
	Events []roomEventView `json:"events"`
}

// roomPostEntry posts an entry body to POST /v1/rooms/{room}/entries.
func roomPostEntry(t *testing.T, ts *httptest.Server, token, roomID string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/rooms/"+roomID+"/entries", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST entries: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// roomGetEvents queries GET /v1/events with the given extra query string.
func roomGetEvents(t *testing.T, ts *httptest.Server, query string) roomEventsResponse {
	t.Helper()
	resp, err := http.Get(ts.URL + "/v1/events?" + query)
	if err != nil {
		t.Fatalf("GET /v1/events?%s: %v", query, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /v1/events?%s = %d: %s", query, resp.StatusCode, b)
	}
	var out roomEventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	return out
}

// seedRoomHTTP declares a room and adds one item through the HTTP routes,
// using a member grant for chaz.
func seedRoomHTTP(t *testing.T, ts *httptest.Server, token string, f *roomStoreFixture, roomID string) string {
	t.Helper()
	if code, body := roomPostEntry(t, ts, token, roomID,
		map[string]any{"entry": roomJSONMap(t, f.gens[roomID])}); code != http.StatusCreated {
		t.Fatalf("POST genesis = %d: %v", code, body)
	}
	create, itemID := f.itemCreate(roomID, room.Actor{Person: "chaz"}, nil)
	if code, body := roomPostEntry(t, ts, token, roomID,
		map[string]any{"entry": roomJSONMap(t, create)}); code != http.StatusCreated {
		t.Fatalf("POST item.create = %d: %v", code, body)
	}
	return itemID
}

// TestRoomStoreRestartRebuildsSameFoldHash: entries persisted through the
// store, then rebuilt from the ledger by a fresh store, fold to the same
// board hash.
func TestRoomStoreRestartRebuildsSameFoldHash(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	f := newRoomStoreFixture(t)
	st := NewRoomBoardStore(root)
	if err := st.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	itemID := seedRoom(t, st, f, f.declare("oracles"))

	want, err := st.FoldHash(f.roomID(0))
	if err != nil {
		t.Fatalf("FoldHash: %v", err)
	}
	if want == "" {
		t.Fatal("live fold hash is empty after writes")
	}
	// Guard against a vacuously equal fold: the board must actually hold the
	// item, and the fold must reflect the item.set.
	board, ok := st.Board(f.roomID(0))
	if !ok {
		t.Fatalf("no board for room %s", f.roomID(0))
	}
	it := board.Items[itemID]
	if it == nil {
		t.Fatalf("folded board has no item %s: %+v", itemID, board.Items)
	}
	if got := it.Fields["state"].Values; len(got) != 1 || got[0].Value != "done" {
		t.Fatalf("folded state = %+v; want the item.set value done", got)
	}
	if n := len(st.Entries(f.roomID(0))); n != 3 {
		t.Fatalf("stored entries = %d; want 3", n)
	}

	// Simulated restart: rebuild the store from the ledger bucket.
	resetLedgerCacheForTest()
	re, err := RebuildRoomBoardStoreFromLedger(root)
	if err != nil {
		t.Fatalf("rebuild from ledger: %v", err)
	}
	got, err := re.FoldHash(f.roomID(0))
	if err != nil {
		t.Fatalf("rebuilt FoldHash: %v", err)
	}
	if got != want {
		t.Fatalf("rebuilt fold hash = %q, want equal to live %q", got, want)
	}
	if n := len(re.Entries(f.roomID(0))); n != 3 {
		t.Fatalf("rebuilt store holds %d entries; want 3", n)
	}
}

// TestRoomStoreGetWithoutMemberGrantIs403: reads of a room's entries and board
// are refused for a caller presenting no credential, and for a live grant whose
// actor is not a member of the room.
func TestRoomStoreGetWithoutMemberGrantIs403(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := f.declare("oracles")
	seedRoom(t, srv.roomBoard, f, roomID)

	for _, path := range []string{
		"/v1/rooms/" + roomID + "/entries",
		"/v1/rooms/" + roomID + "/board",
		"/v1/rooms/" + roomID + "/archive",
	} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s with no member grant = %d; want 403", path, resp.StatusCode)
		}
	}

	// A live grant that names nobody is not a member either.
	stranger := mintScopedTestGrant(t, srv, "stage-dashboard", ScopeRoomsWrite)
	for _, path := range []string{
		"/v1/rooms/" + roomID + "/entries",
		"/v1/rooms/" + roomID + "/board",
	} {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		req.Header.Set(GrantHeaderName, stranger)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s with a non-member grant = %d; want 403", path, resp.StatusCode)
		}
	}

	// Control: a member grant reads the same routes.
	member := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)
	for _, path := range []string{
		"/v1/rooms/" + roomID + "/entries",
		"/v1/rooms/" + roomID + "/board",
	} {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		req.Header.Set(GrantHeaderName, member)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s with a member grant = %d; want 200", path, resp.StatusCode)
		}
	}
}

// TestRoomStoreShareLinkGrantIs403: a share link presented in x-cogos-grant is
// a leaf credential and never acts as a grant on the room routes.
func TestRoomStoreShareLinkGrantIs403(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := f.declare("oracles")
	seedRoom(t, srv.roomBoard, f, roomID)

	const linkToken = "tok-room-store-share"
	if _, err := srv.shareLinks.Import(shareImport(linkToken, "stage/room/r1/owner"), ""); err != nil {
		t.Fatalf("import share link: %v", err)
	}

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, ts.URL+"/v1/rooms/"+roomID+"/entries",
			bytes.NewBufferString(`{"entry":{}}`))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(GrantHeaderName, linkToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s /v1/rooms/{room}/entries: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s with a share-link token = %d; want 403", method, resp.StatusCode)
		}
	}
}

// TestRoomStoreEventsRoomFilterReturnsOnlyThatRoom: two rooms, one node;
// /v1/events?room=A returns only A's board.* events.
func TestRoomStoreEventsRoomFilterReturnsOnlyThatRoom(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	srv := newTestServerAt(t, root)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	roomA := f.declare("oracles")
	roomB := f.declare("lighthouse")
	seedRoomHTTP(t, ts, token, f, roomA)
	seedRoomHTTP(t, ts, token, f, roomB)

	gotA := roomGetEvents(t, ts, "room="+roomA+"&limit=100")
	if gotA.Count == 0 {
		t.Fatalf("GET /v1/events?room=%s returned no events", roomA)
	}
	seenB := map[string]bool{}
	gotB := roomGetEvents(t, ts, "room="+roomB+"&limit=100")
	if gotB.Count == 0 {
		t.Fatalf("GET /v1/events?room=%s returned no events", roomB)
	}
	for _, e := range gotB.Events {
		if e.Room != roomB {
			t.Errorf("room=%s query returned an event for room %q", roomB, e.Room)
		}
		seenB[e.Hash] = true
	}
	for _, e := range gotA.Events {
		if e.Room != roomA {
			t.Errorf("room=%s query returned an event for room %q", roomA, e.Room)
		}
		if !strings.HasPrefix(e.Type, "board.") {
			t.Errorf("room=%s query returned type %q; want a board.* event", roomA, e.Type)
		}
		if seenB[e.Hash] {
			t.Errorf("event %s (%s) appeared under both rooms", e.Hash, e.Type)
		}
	}
	// Without the filter, both rooms' events are visible.
	all := roomGetEvents(t, ts, "limit=100")
	if all.Count <= gotA.Count {
		t.Errorf("unfiltered count = %d; want more than room %s alone (%d)", all.Count, roomA, gotA.Count)
	}
}

// TestRoomStoreEventsActorFilter: actor= returns only events whose actor
// matches.
func TestRoomStoreEventsActorFilter(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	srv := newTestServerAt(t, root)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	roomID := f.declare("oracles")
	_ = seedRoomHTTP(t, ts, token, f, roomID)
	// The agent member cog@node-a creates an item of its own.
	agent := room.Actor{Agent: "cog@node-a"}
	ownCreate, _ := f.itemCreate(roomID, agent, nil)
	if code, body := roomPostEntry(t, ts, token, roomID,
		map[string]any{"entry": roomJSONMap(t, ownCreate)}); code != http.StatusCreated {
		t.Fatalf("POST agent item.create = %d: %v", code, body)
	}

	agentEvents := roomGetEvents(t, ts, "actor=agent:cog@node-a&limit=100")
	if agentEvents.Count == 0 {
		t.Fatal("actor=agent:cog@node-a returned no events")
	}
	for _, e := range agentEvents.Events {
		if e.Actor != "agent:cog@node-a" {
			t.Errorf("actor=agent:cog@node-a returned actor %q", e.Actor)
		}
	}
	chazEvents := roomGetEvents(t, ts, "actor=person:chaz&limit=100")
	if chazEvents.Count == 0 {
		t.Fatal("actor=person:chaz returned no events")
	}
	for _, e := range chazEvents.Events {
		if e.Actor != "person:chaz" {
			t.Errorf("actor=person:chaz returned actor %q", e.Actor)
		}
	}
	if chazEvents.Count == agentEvents.Count {
		t.Errorf("both actor filters returned %d events; the filter is not discriminating",
			chazEvents.Count)
	}
}

// TestRoomStoreSelfCertNotDuplicated: every entry this node authors already
// carries the node's own certificate through certListLocked, so appending
// several self-authored entries must not accumulate duplicate copies of that
// certificate in the room's cert list.
func TestRoomStoreSelfCertNotDuplicated(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	f := newRoomStoreFixture(t)
	st := NewRoomBoardStore(root)
	if err := st.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := f.declare("oracles")
	owner := RoomCaller{Actor: "chaz"}

	// One genesis plus several further entries, all authored on this node's
	// own log (the fixture signs with this node's key).
	if _, err := st.Append(roomID, f.genesisJSON(roomID), nil, owner); err != nil {
		t.Fatalf("append genesis: %v", err)
	}
	const extra = 5
	for i := 0; i < extra; i++ {
		e, _ := f.itemCreate(roomID, room.Actor{Person: "chaz"}, nil)
		if _, err := st.Append(roomID, roomJSON(t, e), nil, owner); err != nil {
			t.Fatalf("append self-authored item.create %d: %v", i, err)
		}
	}

	st.mu.Lock()
	rec := st.rooms[roomID]
	if rec == nil {
		st.mu.Unlock()
		t.Fatalf("no room record for %s", roomID)
	}
	stored := append([][]byte(nil), rec.certs...)
	effective := st.certListLocked(rec)
	st.mu.Unlock()

	// Non-vacuous: the node's own certificate must actually be in the list,
	// otherwise the entries were not self-authored and nothing is proven.
	if !roomCertListHas(effective, f.key.certDER) {
		t.Fatalf("room cert list does not carry this node's certificate after %d self-authored appends", extra+1)
	}
	for _, tc := range []struct {
		name  string
		certs [][]byte
	}{
		{"stored", stored},
		{"effective", effective},
	} {
		counts := map[string]int{}
		for _, c := range tc.certs {
			counts[string(c)]++
		}
		for _, n := range counts {
			if n != 1 {
				t.Errorf("%s cert list holds %d copies of one certificate; want each distinct cert once (%d entries, %d distinct, after %d self-authored appends)",
					tc.name, n, len(tc.certs), len(counts), extra+1)
			}
		}
		if len(counts) != len(tc.certs) {
			t.Errorf("%s cert list has %d entries but %d distinct certs; want no duplicates",
				tc.name, len(tc.certs), len(counts))
		}
	}
}

// roomCertListHas reports whether der appears in certs.
func roomCertListHas(certs [][]byte, der []byte) bool {
	for _, c := range certs {
		if bytes.Equal(c, der) {
			return true
		}
	}
	return false
}

// TestRoomStoreNodeKeyDeviceIdMatchesCert: the device id the store reports for
// this node's key equals the id the BEP cert tooling reports for the same cert.
// The key is a fixture generated inside the test — never a real key on disk.
func TestRoomStoreNodeKeyDeviceIdMatchesCert(t *testing.T) {
	dir := t.TempDir()
	if err := bep.GenerateBEPCert(dir); err != nil {
		t.Fatalf("generate fixture cert: %v", err)
	}

	key, err := LoadNodeDeviceKey(dir)
	if err != nil {
		t.Fatalf("LoadNodeDeviceKey: %v", err)
	}
	if key.DeviceID() == "" {
		t.Fatal("LoadNodeDeviceKey returned an empty device id")
	}

	// The cert tooling's own derivation: DeviceID = SHA-256 of the parsed DER.
	certPEM, err := os.ReadFile(filepath.Join(dir, "bep-cert.pem"))
	if err != nil {
		t.Fatalf("read cert pem: %v", err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("cert pem did not decode")
	}
	x509Cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	want := bep.FormatDeviceID(bep.DeviceIDFromCert(x509Cert))
	if got := key.DeviceID(); got != want {
		t.Fatalf("store device id = %q; cert tooling reports %q", got, want)
	}

	// The kernel's own node id anchors to the same derivation.
	prev := nodeIDCertDir
	nodeIDCertDir = func() string { return dir }
	t.Cleanup(func() { nodeIDCertDir = prev })
	if got := bepAnchoredNodeID(); got != want {
		t.Fatalf("bepAnchoredNodeID() = %q; want %q", got, want)
	}

	// Only the key file's presence is checked: once it is gone there is no
	// device identity to load.
	if err := os.Remove(filepath.Join(dir, "bep-key.pem")); err != nil {
		t.Fatalf("remove fixture key: %v", err)
	}
	if _, err := LoadNodeDeviceKey(dir); err == nil {
		t.Fatal("LoadNodeDeviceKey accepted a cert dir with no key file")
	}
}

// TestRoomStorePostRejectsMissingRequiredKey: an entry missing a key RFC
// section 6.9.1 requires is refused with a 4xx and nothing is written.
func TestRoomStorePostRejectsMissingRequiredKey(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	srv := newTestServerAt(t, root)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)
	roomID := f.declare("oracles")
	seedRoomHTTP(t, ts, token, f, roomID)

	before := len(srv.roomBoard.Entries(roomID))
	ledgerBefore := roomLedgerLines(t, root)

	create, _ := f.itemCreate(roomID, room.Actor{Person: "chaz"}, nil)
	broken := roomJSONMap(t, create)
	delete(broken, "sig")

	if code, body := roomPostEntry(t, ts, token, roomID, map[string]any{"entry": broken}); code < 400 || code > 499 {
		t.Fatalf("POST entry without sig = %d (%v); want 4xx", code, body)
	}
	// A body with no entry at all is refused too.
	if code, _ := roomPostEntry(t, ts, token, roomID, map[string]any{"nope": true}); code < 400 || code > 499 {
		t.Fatalf("POST with no entry = %d; want 4xx", code)
	}

	if after := len(srv.roomBoard.Entries(roomID)); after != before {
		t.Fatalf("entries after a refused POST = %d; want unchanged at %d", after, before)
	}
	if after := roomLedgerLines(t, root); after != ledgerBefore {
		t.Fatalf("ledger lines after a refused POST = %d; want unchanged at %d", after, ledgerBefore)
	}
}

// roomLedgerLines counts the record lines in the room board's ledger bucket.
func roomLedgerLines(t *testing.T, root string) int {
	t.Helper()
	f, err := os.Open(filepath.Join(root, ".cog", "ledger", RoomsLedgerBucket, "events.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("open rooms ledger: %v", err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan rooms ledger: %v", err)
	}
	return n
}
