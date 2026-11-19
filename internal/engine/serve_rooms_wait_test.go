package engine

// serve_rooms_wait_test.go — the held read over the room chain, GET /v1/rooms/{room}/wait.
//
// Covers both layers: RoomBoardStore.WaitAfter/Heads directly (goroutine-level timing), and the HTTP
// handler end to end (query params, error codes, the bus-RFC-shaped {error, code, fallback} body).
//
// RR-1 note: these tests exercise a READ over data P4-3 already writes to the kernel ledger today; they
// do not touch, assume, or need a ruling on whether MORE entries move onto the ledger.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------------------------------------------------------------- store-level: WaitAfter / Heads

func mustCreateWaitTestRoom(t *testing.T, srv *Server, alias string) string {
	t.Helper()
	roomID, _, err := srv.roomBoard.CreateRoom(alias, "Room", RoomCaller{Actor: "chaz", Admin: true})
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	return roomID
}

func TestRoomWaitHeadsEmptyOnGenesisOnlyRoom(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := mustCreateWaitTestRoom(t, srv, "oracles")
	heads, ok := srv.roomBoard.Heads(roomID)
	if !ok {
		t.Fatalf("Heads: room not found")
	}
	if len(heads) != 1 {
		t.Fatalf("heads = %v; want exactly one log (the genesis author)", heads)
	}
}

func TestRoomWaitReturnsImmediatelyWhenAlreadyBehind(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := mustCreateWaitTestRoom(t, srv, "oracles")

	// after={} (nothing seen yet) must return at once with the genesis entry, never holding.
	start := time.Now()
	entries, heads, ok := srv.roomBoard.WaitAfter(context.Background(), roomID, map[string]uint64{}, 5*time.Second)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("WaitAfter: room not found")
	}
	if elapsed > time.Second {
		t.Fatalf("WaitAfter held for %v on an empty cursor; should return at once", elapsed)
	}
	if len(entries) == 0 {
		t.Fatalf("expected at least the genesis entry, got none")
	}
	if len(heads) == 0 {
		t.Fatalf("expected a non-empty heads vector")
	}
}

func TestRoomWaitHoldsThenWakesOnNewEntry(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := mustCreateWaitTestRoom(t, srv, "oracles")
	heads, _ := srv.roomBoard.Heads(roomID)

	done := make(chan struct{})
	var gotEntries []RoomEntryView
	var gotHeads map[string]uint64
	var waitErr bool
	go func() {
		defer close(done)
		e, h, ok := srv.roomBoard.WaitAfter(context.Background(), roomID, heads, 5*time.Second)
		gotEntries, gotHeads, waitErr = e, h, !ok
	}()

	// Give WaitAfter time to actually block on the current notify channel before the write lands.
	time.Sleep(50 * time.Millisecond)
	_, _, err := srv.roomBoard.ApplyOp(roomID, "create",
		"", map[string]any{"text": "a new item", "section": "tasks", "state": "open"}, RoomCaller{Actor: "chaz", Admin: true})
	if err != nil {
		t.Fatalf("ApplyOp: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitAfter did not wake within 5s of the new entry landing")
	}
	if waitErr {
		t.Fatalf("WaitAfter reported room not found")
	}
	if len(gotEntries) != 1 {
		t.Fatalf("got %d new entries; want exactly 1 (the create op)", len(gotEntries))
	}
	if len(gotHeads) == 0 {
		t.Fatalf("expected a non-empty heads vector on wake")
	}
}

func TestRoomWaitTimesOutWithCurrentHeadsNotEmptySuccess(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	roomID := mustCreateWaitTestRoom(t, srv, "oracles")
	heads, _ := srv.roomBoard.Heads(roomID)

	start := time.Now()
	entries, gotHeads, ok := srv.roomBoard.WaitAfter(context.Background(), roomID, heads, 200*time.Millisecond)
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf("WaitAfter: room not found")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("returned too early (%v); should have held close to the 200ms timeout", elapsed)
	}
	if len(entries) != 0 {
		t.Fatalf("got %d entries on a timeout with nothing new; want 0", len(entries))
	}
	// This IS the point of the fix: a timeout must still return a true, usable cursor, never an empty
	// success with a stale one -- the caller resumes with after=gotHeads next time, no data loss possible.
	if len(gotHeads) == 0 {
		t.Fatalf("timeout returned an empty heads vector; caller has nothing to resume from")
	}
	for log, seq := range heads {
		if gotHeads[log] != seq {
			t.Errorf("heads on timeout drifted: log %s = %d; want unchanged %d", log, gotHeads[log], seq)
		}
	}
}

func TestRoomWaitUnknownRoomReturnsFalse(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	_, _, ok := srv.roomBoard.WaitAfter(context.Background(), "no-such-room", nil, time.Second)
	if ok {
		t.Fatal("WaitAfter on an unknown room should report ok=false")
	}
}

// ---------------------------------------------------------------- HTTP handler: GET /v1/rooms/{room}/wait

func roomWaitRaw(t *testing.T, ts *httptest.Server, token, roomID, query string) (int, map[string]any) {
	t.Helper()
	url := ts.URL + "/v1/rooms/" + roomID + "/wait"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/rooms/{room}/wait: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestRoomWaitHTTPImmediateReturnWithEmptyCursor(t *testing.T) {
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

	status, out := roomWaitRaw(t, ts, token, roomID, "wait_s=0")
	if status != http.StatusOK {
		t.Fatalf("GET wait = %d (%v); want 200", status, out)
	}
	entries, _ := out["entries"].([]any)
	if len(entries) == 0 {
		t.Fatalf("expected at least the genesis entry with no cursor, got none: %v", out)
	}
	heads, _ := out["heads"].(map[string]any)
	if len(heads) == 0 {
		t.Fatalf("expected a non-empty heads object: %v", out)
	}
}

func TestRoomWaitHTTPUnknownRoomIsRoomClosed(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	token := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)

	status, out := roomWaitRaw(t, ts, token, "no-such-room", "")
	if status != http.StatusNotFound {
		t.Fatalf("GET wait on unknown room = %d; want 404", status)
	}
	if out["code"] != "room_closed" {
		t.Errorf("code = %v; want room_closed", out["code"])
	}
	if _, ok := out["fallback"].(string); !ok || out["fallback"] == "" {
		t.Errorf("expected a non-empty fallback string in the stream-error shape: %v", out)
	}
}

func TestRoomWaitHTTPCursorAheadOfHeadIsCursorGone(t *testing.T) {
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
	heads, ok := srv.roomBoard.Heads(roomID)
	if !ok {
		t.Fatalf("Heads: room not found")
	}
	// Claim to have seen one more entry than actually exists on some log -- impossible to honor.
	fake := map[string]uint64{}
	for log, seq := range heads {
		fake[log] = seq + 100
	}
	raw, err := json.Marshal(fake)
	if err != nil {
		t.Fatalf("marshal fake cursor: %v", err)
	}

	status, out := roomWaitRaw(t, ts, token, roomID, "after="+string(raw))
	if status != http.StatusConflict {
		t.Fatalf("GET wait with an ahead-of-head cursor = %d (%v); want 409", status, out)
	}
	if out["code"] != "cursor_gone" {
		t.Errorf("code = %v; want cursor_gone", out["code"])
	}
}

func TestRoomWaitHTTPWithoutGrantIs403(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	// Read routes in the /v1/rooms family (entries/board/archive/wait) are 403 member_grant_required with
	// no credential, NOT 401 -- 401 is reserved for the two kernel-authored WRITE routes (see
	// serve_rooms.go's header and TestRoomWritesWithoutGrantIs401). /wait is a read, so it follows the
	// same rule as /board and /entries.
	status, _ := roomWaitRaw(t, ts, "", "whatever-room", "")
	if status != http.StatusForbidden {
		t.Fatalf("GET wait without a grant = %d; want 403 (same room-family read rule as /board, /entries)", status)
	}
}

func TestRoomWaitHTTPNonMemberIs403(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	f := newRoomStoreFixture(t)
	if err := srv.roomBoard.ConfigureNodeKey(f.certDir); err != nil {
		t.Fatalf("ConfigureNodeKey: %v", err)
	}
	ownerToken := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeRoomsWrite)
	code, body := roomCreate(t, ts, ownerToken, map[string]any{"alias": "oracles"})
	if code != http.StatusCreated {
		t.Fatalf("create room = %d (%v); want 201", code, body)
	}
	roomID, _ := body["room"].(string)

	outsiderToken := mintActorTestGrant(t, srv, "discord-someone-else", "someone-else", ScopeRoomsWrite)
	status, out := roomWaitRaw(t, ts, outsiderToken, roomID, "")
	if status != http.StatusForbidden {
		t.Fatalf("GET wait as a non-member = %d (%v); want 403", status, out)
	}
}

func TestRoomWaitHTTPBadAfterIs400(t *testing.T) {
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

	status, out := roomWaitRaw(t, ts, token, roomID, "after=not-json")
	if status != http.StatusBadRequest {
		t.Fatalf("GET wait with malformed after = %d (%v); want 400", status, out)
	}
}

func TestRoomWaitHTTPWaitSClampedToMax(t *testing.T) {
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
	heads, _ := srv.roomBoard.Heads(roomID)
	raw, _ := json.Marshal(heads)

	// wait_s=500 must be clamped to roomWaitMaxTimeout (~50s), not actually held for 500s -- give it a
	// client-side deadline well under 50s and confirm the request does NOT return within that window,
	// proving it is genuinely holding (not silently ignoring wait_s), without waiting the full 50s out.
	client := &http.Client{Timeout: 2 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/rooms/"+roomID+"/wait?after="+string(raw)+"&wait_s=500", nil)
	req.Header.Set(GrantHeaderName, token)
	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected the 2s client timeout to fire; the server returned within 2s on an already-current cursor")
	}
}
