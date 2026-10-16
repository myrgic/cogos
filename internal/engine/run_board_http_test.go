package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func runsPost(t *testing.T, url, token, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(GrantHeaderName, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestRunBoardHTTP_EndTwice409_Unknown404_ManualOnly(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	token := mintScopedTestGrant(t, srv, "run-board-http", ScopeRunsWrite)

	code, run := runsPost(t, ts.URL+"/v1/runs", token,
		`{"title":"t","owner":"cog","executor":{"kind":"manual"},"origin":{"by":"chaz"}}`)
	if code != http.StatusCreated {
		t.Fatalf("open = %d", code)
	}
	id, _ := run["id"].(string)
	end := `{"outcome":"ok","observed_by":"kernel"}`
	if code, _ := runsPost(t, ts.URL+"/v1/runs/"+id+"/end", token, end); code != http.StatusOK {
		t.Fatalf("first end = %d", code)
	}
	if code, _ := runsPost(t, ts.URL+"/v1/runs/"+id+"/end", token, end); code != http.StatusConflict {
		t.Fatalf("second end = %d; want 409", code)
	}
	if code, _ := runsPost(t, ts.URL+"/v1/runs/nope/end", token, end); code != http.StatusNotFound {
		t.Fatalf("end on unknown run = %d; want 404", code)
	}
	if code, _ := runsPost(t, ts.URL+"/v1/runs", token,
		`{"title":"t","owner":"cog","executor":{"kind":"hermes-fork"},"origin":{"by":"chaz"}}`); code != http.StatusBadRequest {
		t.Fatalf("non-manual executor = %d; want 400", code)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/runs?state=ended", nil)
	req.Header.Set(GrantHeaderName, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		Runs []Run `json:"runs"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&list)
	if resp.StatusCode != http.StatusOK || len(list.Runs) != 1 || list.Runs[0].ID != id {
		t.Fatalf("list state=ended = %d %+v", resp.StatusCode, list.Runs)
	}
}

func TestRunBoardHTTP_EventsReplaySince(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	// The events handler outlives its hijacked connection by a span emit onto
	// the bus dir; wait for every handler to return before TempDir cleanup.
	var inflight sync.WaitGroup
	h := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		defer inflight.Done()
		h.ServeHTTP(w, r)
	}))
	defer inflight.Wait()
	defer ts.Close()
	token := mintScopedTestGrant(t, srv, "run-board-ws", ScopeRunsWrite)

	_, run := runsPost(t, ts.URL+"/v1/runs", token,
		`{"title":"t","owner":"cog","executor":{"kind":"manual"},"origin":{"by":"chaz"}}`)
	id, _ := run["id"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/v1/runs/events?since=0",
		&websocket.DialOptions{HTTPHeader: http.Header{GrantHeaderName: {token}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.CloseNow()
	read := func() RunEvent {
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var ev RunEvent
		_ = json.Unmarshal(b, &ev)
		return ev
	}
	if ev := read(); ev.Kind != "item.create" || ev.Run != id {
		t.Fatalf("replayed event = %+v", ev)
	}
	// A live append after the replay arrives on the same socket.
	runsPost(t, ts.URL+"/v1/runs/"+id+"/end", token, `{"outcome":"ok","observed_by":"kernel"}`)
	if ev := read(); ev.Kind != "run.end" || ev.Run != id {
		t.Fatalf("live event = %+v", ev)
	}
}

func TestRunBoardStalledWakeUndelivered_I3(t *testing.T) {
	st := NewRunBoardStore("")
	base := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	st.now = func() time.Time { return base }
	room, err := st.Open(RunOpen{Room: "stage-r1", Title: "room run", Owner: "cog",
		Executor: RunExecutor{Kind: RunExecutorManual}, Origin: RunOrigin{By: "chaz"}}, "chaz")
	if err != nil {
		t.Fatal(err)
	}
	node := runBoardTestOpen(t, st, "node run")
	for _, id := range []string{room.ID, node.ID} {
		if _, err := st.End(id, runBoardTestEnd(RunOutcomeOK), "chaz"); err != nil {
			t.Fatal(err)
		}
	}
	st.now = func() time.Time { return base.Add(4 * time.Minute) }
	if r, _ := st.Get(room.ID); r.Stalled != "" {
		t.Fatalf("stalled at 4 min: %q", r.Stalled)
	}
	st.now = func() time.Time { return base.Add(6 * time.Minute) }
	if r, _ := st.Get(room.ID); r.Stalled != "wake-undelivered" {
		t.Fatalf("room run at 6 min stalled = %q; want wake-undelivered", r.Stalled)
	}
	if r, _ := st.Get(node.ID); r.Stalled != "" {
		t.Fatalf("node-board run has no front session to wake, stalled = %q", r.Stalled)
	}
}
