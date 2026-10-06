package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func managedSessionTestServer(t *testing.T) (*httptest.Server, *HermesACPDriver) {
	t.Helper()
	srv := newTestServer(t)
	srv.cfg.WriteRouteGrantAuthDisabled = true
	srv.managedSessions = fakeHermesDriver(t, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, srv.managedSessions
}

func msDo(t *testing.T, ts *httptest.Server, method, path string, body any, out any) int {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, ts.URL+path, rdr)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

type wsFrame struct {
	Kind      string          `json:"kind"`
	Seq       uint64          `json:"seq"`
	Data      json.RawMessage `json:"data"`
	Gap       bool            `json:"gap"`
	LastSeq   uint64          `json:"last_seq"`
	SessionID string          `json:"session_id"`
}

func wsDial(t *testing.T, ts *httptest.Server, id string, since uint64) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/managed-sessions/" + id + "/events?since=" + strconv.FormatUint(since, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func wsRead(t *testing.T, c *websocket.Conn) wsFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var f wsFrame
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("frame %s: %v", b, err)
	}
	return f
}

func wsUntil(t *testing.T, c *websocket.Conn, pred func(wsFrame) bool) wsFrame {
	t.Helper()
	for i := 0; i < 100; i++ {
		if f := wsRead(t, c); pred(f) {
			return f
		}
	}
	t.Fatal("no matching frame")
	return wsFrame{}
}

func TestManagedSessionHTTP_FullFlowWithReplay(t *testing.T) {
	ts, _ := managedSessionTestServer(t)

	var info ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions", map[string]any{"driver": "hermes-acp", "profile": "cog", "cwd": t.TempDir()}, &info); code != http.StatusCreated {
		t.Fatalf("create = %d", code)
	}
	id := info.SessionID
	if info.State != "live" || id == "" {
		t.Fatalf("info = %+v", info)
	}

	// Client 1 attaches, sees hello + the state event, then a turn that asks
	// for permission.
	c1 := wsDial(t, ts, id, 0)
	if h := wsRead(t, c1); h.Kind != "hello" || h.SessionID != id || h.Gap {
		t.Fatalf("hello = %+v", h)
	}
	var pr map[string]string
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/prompt", map[string]any{"prompt": "permission to write"}, &pr); code != http.StatusAccepted || pr["turn_id"] == "" {
		t.Fatalf("prompt = %d %v", code, pr)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/prompt", map[string]any{"prompt": "again"}, nil); code != http.StatusConflict {
		t.Fatalf("second prompt = %d, want 409", code)
	}
	permEv := wsUntil(t, c1, func(f wsFrame) bool { return f.Kind == MSEventPermissionRequest })
	lastSeen := permEv.Seq

	// Client 1 "restarts": drops the socket while the permission is parked.
	c1.CloseNow()
	var got ManagedSessionInfo
	msDo(t, ts, "GET", "/v1/managed-sessions/"+id, nil, &got)
	if len(got.PendingPermissions) != 1 || got.TurnInFlight == "" {
		t.Fatalf("pending after disconnect: %+v", got)
	}

	// Client 2 answers the parked request, then reconnects with since=.
	var reqID struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(permEv.Data, &reqID)
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/permission", map[string]any{"request_id": reqID.RequestID, "option_id": "allow"}, nil); code != http.StatusOK {
		t.Fatalf("permission = %d", code)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/permission", map[string]any{"request_id": reqID.RequestID, "option_id": "allow"}, nil); code != http.StatusNotFound {
		t.Fatalf("double answer = %d, want 404", code)
	}
	// Let the turn finish while nobody is attached.
	deadline := time.Now().Add(10 * time.Second)
	for {
		got = ManagedSessionInfo{} // omitempty: a stale TurnInFlight would survive decode
		msDo(t, ts, "GET", "/v1/managed-sessions/"+id, nil, &got)
		if got.TurnInFlight == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("turn never finished after permission was answered")
		}
		time.Sleep(20 * time.Millisecond)
	}

	c2 := wsDial(t, ts, id, lastSeen)
	h := wsRead(t, c2)
	if h.Kind != "hello" || h.Gap {
		t.Fatalf("hello2 = %+v", h)
	}
	var kinds []string
	next := lastSeen + 1
	for {
		f := wsRead(t, c2)
		if f.Seq != next {
			t.Fatalf("replay seq %d, want %d", f.Seq, next)
		}
		next++
		kinds = append(kinds, f.Kind)
		if f.Kind == MSEventTurnEnd {
			break
		}
	}
	want := []string{MSEventPermissionResolved, MSEventSessionUpdate, MSEventTurnEnd}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("replayed kinds = %v, want %v", kinds, want)
	}

	// Live tail on the same socket: mode change + cancel.
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/mode", map[string]any{"mode_id": "dont_ask"}, nil); code != http.StatusOK {
		t.Fatalf("mode = %d", code)
	}
	if f := wsRead(t, c2); f.Kind != MSEventSessionUpdate || !strings.Contains(string(f.Data), "dont_ask") || f.Seq != next {
		t.Fatalf("live frame = %+v", f)
	}
	msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/prompt", map[string]any{"prompt": []any{map[string]any{"type": "text", "text": "slow"}}}, nil)
	wsUntil(t, c2, func(f wsFrame) bool {
		return f.Kind == MSEventSessionUpdate && strings.Contains(string(f.Data), "working")
	})
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/cancel", nil, nil); code != http.StatusOK {
		t.Fatalf("cancel = %d", code)
	}
	end := wsUntil(t, c2, func(f wsFrame) bool { return f.Kind == MSEventTurnEnd })
	if !strings.Contains(string(end.Data), "cancelled") {
		t.Fatalf("turn_end = %s", end.Data)
	}

	// Plain GET of events returns the JSON replay.
	var replay struct {
		Hello  managedEventsHello    `json:"hello"`
		Events []ManagedSessionEvent `json:"events"`
	}
	msDo(t, ts, "GET", "/v1/managed-sessions/"+id+"/events?since=0", nil, &replay)
	if len(replay.Events) == 0 || replay.Events[0].Seq != 1 || replay.Hello.LastSeq != replay.Events[len(replay.Events)-1].Seq {
		t.Fatalf("json replay: %+v", replay.Hello)
	}

	// Fork, list, delete (idempotent).
	var fork ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions", map[string]any{"fork_of": id}, &fork); code != http.StatusCreated || fork.ForkOf != id {
		t.Fatalf("fork = %d %+v", code, fork)
	}
	var list struct{ Sessions []ManagedSessionInfo }
	msDo(t, ts, "GET", "/v1/managed-sessions", nil, &list)
	if len(list.Sessions) != 2 {
		t.Fatalf("list = %+v", list)
	}
	for i := 0; i < 2; i++ {
		if code := msDo(t, ts, "DELETE", "/v1/managed-sessions/"+id, nil, nil); code != http.StatusNoContent {
			t.Fatalf("delete = %d", code)
		}
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/"+id, nil, nil); code != http.StatusNotFound {
		t.Fatalf("get after delete = %d", code)
	}
	// The deleted session's live socket is told to reconnect.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, _, err := c2.Read(ctx); err != nil {
			if websocket.CloseStatus(err) != websocket.StatusGoingAway {
				t.Fatalf("close = %v", err)
			}
			break
		}
	}
}

func TestManagedSessionHTTP_Validation(t *testing.T) {
	ts, _ := managedSessionTestServer(t)
	cases := []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/v1/managed-sessions", map[string]any{"driver": "claude-code"}, http.StatusBadRequest},
		{"POST", "/v1/managed-sessions", map[string]any{"fork_of": "a", "load": "b"}, http.StatusBadRequest},
		{"POST", "/v1/managed-sessions", map[string]any{"fork_of": "missing"}, http.StatusNotFound},
		{"POST", "/v1/managed-sessions/missing/prompt", map[string]any{"prompt": "x"}, http.StatusNotFound},
		{"GET", "/v1/managed-sessions/missing/events", nil, http.StatusNotFound},
		{"DELETE", "/v1/managed-sessions/missing", nil, http.StatusNoContent},
	}
	for _, c := range cases {
		if got := msDo(t, ts, c.method, c.path, c.body, nil); got != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, got, c.want)
		}
	}
	// resume = session/load of an on-disk session, idempotent.
	var a, b ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/disk-1/resume", map[string]any{"cwd": t.TempDir()}, &a); code != http.StatusOK || a.SessionID != "disk-1" {
		t.Fatalf("resume = %d %+v", code, a)
	}
	msDo(t, ts, "POST", "/v1/managed-sessions/disk-1/resume", nil, &b)
	if b.CreatedAt != a.CreatedAt {
		t.Fatalf("resume not idempotent: %+v vs %+v", a, b)
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/disk-1/events?since=x", nil, nil); code != http.StatusBadRequest {
		t.Fatalf("bad since = %d", code)
	}
}

func TestManagedSessionHTTP_WritesNeedGrant(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.WriteRouteGrantAuthDisabled = false
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	if code := msDo(t, ts, "POST", "/v1/managed-sessions", map[string]any{}, nil); code != http.StatusUnauthorized {
		t.Fatalf("ungranted create = %d, want 401", code)
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions", nil, nil); code != http.StatusOK {
		t.Fatalf("list = %d, want 200", code)
	}
}
