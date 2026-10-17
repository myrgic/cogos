package engine

// serve_rooms_write_test.go — the kernel-authored room write routes:
//
//	POST /v1/rooms                (create a room, kernel-signed genesis)
//	POST /v1/rooms/{room}/ops     (apply one board item op, kernel-signed)
//
// A trusted local client holding a kernel grant never signs anything itself:
// the kernel authors the entry on its own BEP device log (RoomBoardStore's
// node key) and appends it through the same admission path Append uses.
//
// Every test name starts with TestRoomWrites, per the slice brief.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// roomCreate posts a body to POST /v1/rooms.
func roomCreate(t *testing.T, ts *httptest.Server, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/rooms", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/rooms: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// roomOp posts a body to POST /v1/rooms/{room}/ops.
func roomOp(t *testing.T, ts *httptest.Server, token, roomID string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/rooms/"+roomID+"/ops", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/rooms/{room}/ops: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// roomGetBoard fetches the folded board through GET /v1/rooms/{room}/board.
func roomGetBoard(t *testing.T, ts *httptest.Server, token, roomID string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/rooms/"+roomID+"/board", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET board: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET board = %d; want 200", resp.StatusCode)
	}
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode board: %v", err)
	}
	return out
}

// roomSummaries fetches GET /v1/rooms.
func roomSummaries(t *testing.T, ts *httptest.Server, token string) []map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/rooms", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/rooms: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/rooms = %d; want 200", resp.StatusCode)
	}
	var out struct {
		Rooms []map[string]any `json:"rooms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode rooms: %v", err)
	}
	return out.Rooms
}

// itemFieldValue returns the folded single value of an item field.
func itemFieldValue(t *testing.T, item map[string]any, field string) string {
	t.Helper()
	fields, _ := item["Fields"].(map[string]any)
	f, _ := fields[field].(map[string]any)
	vals, _ := f["Values"].([]any)
	if len(vals) == 0 {
		return ""
	}
	v0, _ := vals[0].(map[string]any)
	s, _ := v0["Value"].(string)
	return s
}

// boardItem returns the folded item map for id.
func boardItem(t *testing.T, board map[string]any, id string) map[string]any {
	t.Helper()
	items, _ := board["Items"].(map[string]any)
	it, _ := items[id].(map[string]any)
	return it
}

// TestRoomWritesCreateIsIdempotentOnAlias: a create with an alias makes room
// 201 the first time and returns the SAME room with 200 the second time.
func TestRoomWritesCreateIsIdempotentOnAlias(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	code, body := roomCreate(t, ts, token, map[string]any{"alias": "oracles", "title": "Oracles"})
	if code != http.StatusCreated {
		t.Fatalf("first create = %d (%v); want 201", code, body)
	}
	roomID, _ := body["room"].(string)
	if roomID == "" {
		t.Fatalf("first create returned no room id: %v", body)
	}
	if body["alias"] != "oracles" {
		t.Errorf("first create alias = %v; want oracles", body["alias"])
	}
	if body["created"] != true {
		t.Errorf("first create created = %v; want true", body["created"])
	}

	code2, body2 := roomCreate(t, ts, token, map[string]any{"alias": "oracles"})
	if code2 != http.StatusOK {
		t.Fatalf("second create with the same alias = %d (%v); want 200", code2, body2)
	}
	if body2["room"] != roomID {
		t.Fatalf("second create room = %v; want the same id %q", body2["room"], roomID)
	}
	if body2["created"] != false {
		t.Errorf("second create created = %v; want false", body2["created"])
	}

	// One room only, and GET /v1/rooms lists the alias.
	if n := len(srv.roomBoard.Rooms()); n != 1 {
		t.Fatalf("store holds %d rooms; want 1", n)
	}
	rooms := roomSummaries(t, ts, token)
	if len(rooms) != 1 {
		t.Fatalf("GET /v1/rooms returned %d rooms; want 1", len(rooms))
	}
	if rooms[0]["id"] != roomID || rooms[0]["alias"] != "oracles" {
		t.Errorf("listed room = %v; want id %q alias oracles", rooms[0], roomID)
	}
}

// TestRoomWritesOpsFoldOntoBoard: create then create/set/archive ops give the
// right folded board through GET /v1/rooms/{room}/board, and each op is
// kernel-signed on the node's own log with an increasing seq.
func TestRoomWritesOpsFoldOntoBoard(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	code, body := roomCreate(t, ts, token, map[string]any{"alias": "oracles"})
	if code != http.StatusCreated {
		t.Fatalf("create room = %d (%v); want 201", code, body)
	}
	roomID, _ := body["room"].(string)

	code, body = roomOp(t, ts, token, roomID, map[string]any{
		"op":     "create",
		"fields": map[string]any{"text": "an item", "section": "tasks", "state": "open"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op create = %d (%v); want 201", code, body)
	}
	item, _ := body["item"].(map[string]any)
	if item == nil {
		t.Fatalf("op create returned no folded item: %v", body)
	}
	itemID, _ := item["ID"].(string)
	if itemID == "" {
		t.Fatalf("op create returned an item with no id: %v", item)
	}
	if got := itemFieldValue(t, item, "state"); got != "open" {
		t.Errorf("created item state = %q; want open", got)
	}

	code, body = roomOp(t, ts, token, roomID, map[string]any{
		"op": "set", "item": itemID,
		"fields": map[string]any{"field": "state", "value": "done"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op set = %d (%v); want 201", code, body)
	}
	item, _ = body["item"].(map[string]any)
	if got := itemFieldValue(t, item, "state"); got != "done" {
		t.Errorf("item state after set = %q; want done", got)
	}

	code, body = roomOp(t, ts, token, roomID, map[string]any{
		"op": "archive", "item": itemID,
		"fields": map[string]any{"closure": "done"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op archive = %d (%v); want 201", code, body)
	}
	item, _ = body["item"].(map[string]any)
	if item["Archived"] != true {
		t.Errorf("item Archived after archive = %v; want true", item["Archived"])
	}
	closure, _ := item["Closure"].(map[string]any)
	if closure["Kind"] != "done" {
		t.Errorf("item Closure = %v; want kind done", closure)
	}

	// The board served over HTTP agrees with the op responses.
	board := roomGetBoard(t, ts, token, roomID)
	it := boardItem(t, board, itemID)
	if it == nil {
		t.Fatalf("board has no item %s: %v", itemID, board["Items"])
	}
	if it["Archived"] != true {
		t.Errorf("board item Archived = %v; want true", it["Archived"])
	}
	if got := itemFieldValue(t, it, "state"); got != "done" {
		t.Errorf("board item state = %q; want done", got)
	}

	// The whole chain lives on this node's own log, seq 1..4.
	entries := srv.roomBoard.Entries(roomID)
	if len(entries) != 4 {
		t.Fatalf("stored entries = %d; want 4 (genesis, create, set, archive)", len(entries))
	}
	for i, e := range entries {
		if e.Log != srv.roomBoard.key.DeviceID() {
			t.Errorf("entry %d log = %q; want this node's log", i, e.Log)
		}
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d seq = %d; want %d", i, e.Seq, i+1)
		}
	}
}

// TestRoomWritesRebuildKeepsFoldAndChains: the ledger rebuild folds to the
// same hash and a rebuilt store keeps authoring on the same log — its next
// entry continues seq/prev rather than conflicting with what was written.
func TestRoomWritesRebuildKeepsFoldAndChains(t *testing.T) {
	root := t.TempDir()
	resetLedgerCacheForTest()
	t.Cleanup(resetLedgerCacheForTest)

	srv := newTestServerAt(t, root)
	srv.cfg.WriteRouteGrantAuthDisabled = false
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	code, body := roomCreate(t, ts, token, map[string]any{"alias": "oracles"})
	if code != http.StatusCreated {
		t.Fatalf("create room = %d (%v); want 201", code, body)
	}
	roomID, _ := body["room"].(string)

	code, body = roomOp(t, ts, token, roomID, map[string]any{
		"op":     "create",
		"fields": map[string]any{"text": "an item", "state": "open"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op create = %d (%v); want 201", code, body)
	}
	item, _ := body["item"].(map[string]any)
	itemID, _ := item["ID"].(string)

	code, body = roomOp(t, ts, token, roomID, map[string]any{
		"op": "set", "item": itemID,
		"fields": map[string]any{"field": "state", "value": "done"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op set = %d (%v); want 201", code, body)
	}

	want, err := srv.roomBoard.FoldHash(roomID)
	if err != nil {
		t.Fatalf("FoldHash: %v", err)
	}
	if want == "" {
		t.Fatal("live fold hash is empty after writes")
	}

	// Simulated restart: rebuild the store from the rooms ledger bucket.
	resetLedgerCacheForTest()
	re, err := RebuildRoomBoardStoreFromLedger(root)
	if err != nil {
		t.Fatalf("rebuild from ledger: %v", err)
	}
	got, err := re.FoldHash(roomID)
	if err != nil {
		t.Fatalf("rebuilt FoldHash: %v", err)
	}
	if got != want {
		t.Fatalf("rebuilt fold hash = %q; want equal to live %q", got, want)
	}
	// The rebuilt store cannot sign until its key is loaded (as at boot).
	if _, _, err := re.ApplyOp(roomID, "set", itemID,
		map[string]any{"field": "state", "value": "open"}, RoomCaller{Actor: "chaz"}); err == nil {
		t.Fatal("rebuilt store without a node key accepted a write; want unavailable")
	}
	if err := re.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("rebuilt ConfigureNodeKey: %v", err)
	}

	// Further ops chain onto the stored log (no seq/prev conflict).
	view, itemAfter, err := re.ApplyOp(roomID, "set", itemID,
		map[string]any{"field": "state", "value": "open"}, RoomCaller{Actor: "chaz"})
	if err != nil {
		t.Fatalf("rebuilt ApplyOp: %v", err)
	}
	if view.Seq != 4 {
		t.Errorf("rebuilt next entry seq = %d; want 4 (continues the stored log)", view.Seq)
	}
	if itemAfter == nil {
		t.Fatal("rebuilt ApplyOp returned no folded item")
	}
	hash2, err := re.FoldHash(roomID)
	if err != nil {
		t.Fatalf("rebuilt FoldHash after op: %v", err)
	}
	if hash2 == got {
		t.Error("fold hash unchanged after a further accepted set")
	}
	entries := re.Entries(roomID)
	if len(entries) != 4 {
		t.Fatalf("rebuilt store holds %d entries after op; want 4", len(entries))
	}
	if entries[3].Seq != 4 {
		t.Errorf("rebuilt last entry seq = %d; want 4", entries[3].Seq)
	}
}

// TestRoomWritesOpsNonMemberIs403: a live grant whose actor is not a member of
// the room cannot write to its board.
func TestRoomWritesOpsNonMemberIs403(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	owner := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)
	stranger := mintActorTestGrant(t, srv, "discord-vroku", "vroku", ScopeRoomsWrite)

	code, body := roomCreate(t, ts, owner, map[string]any{"alias": "oracles"})
	if code != http.StatusCreated {
		t.Fatalf("create room = %d (%v); want 201", code, body)
	}
	roomID, _ := body["room"].(string)

	code, body = roomOp(t, ts, stranger, roomID, map[string]any{
		"op": "create", "fields": map[string]any{"text": "sneaky"},
	})
	if code != http.StatusForbidden {
		t.Fatalf("op as a non-member = %d (%v); want 403", code, body)
	}
	if len(srv.roomBoard.Entries(roomID)) != 1 {
		t.Fatalf("a refused op wrote an entry; want the genesis alone")
	}

	// Control: the member owner may write.
	code, body = roomOp(t, ts, owner, roomID, map[string]any{
		"op": "create", "fields": map[string]any{"text": "legit"},
	})
	if code != http.StatusCreated {
		t.Fatalf("op as the owner = %d (%v); want 201", code, body)
	}
}

// TestRoomWritesWithoutGrantIs401: both kernel-authored write routes are
// ordinary write routes — no grant at all is the write routes' 401.
func TestRoomWritesWithoutGrantIs401(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}

	if code, _ := roomCreate(t, ts, "", map[string]any{"alias": "oracles"}); code != http.StatusUnauthorized {
		t.Errorf("POST /v1/rooms without a grant = %d; want 401", code)
	}
	if code, _ := roomOp(t, ts, "", "some-room", map[string]any{"op": "create"}); code != http.StatusUnauthorized {
		t.Errorf("POST /v1/rooms/{room}/ops without a grant = %d; want 401", code)
	}
}

// TestRoomWritesWithoutNodeKeyIs503: with no device key the kernel cannot sign
// anything, so both write routes are unavailable.
func TestRoomWritesWithoutNodeKeyIs503(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	// Replace the boot-configured store with one that holds no node key.
	srv.roomBoard = NewRoomBoardStore(srv.cfg.WorkspaceRoot)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	code, body := roomCreate(t, ts, token, map[string]any{"alias": "oracles"})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("POST /v1/rooms with no node key = %d (%v); want 503", code, body)
	}
	if body["error"].(map[string]any)["type"] != "unavailable" {
		t.Errorf("error.type = %v; want unavailable", body["error"])
	}
	code, body = roomOp(t, ts, token, "some-room", map[string]any{"op": "create"})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("POST ops with no node key = %d (%v); want 503", code, body)
	}
}
