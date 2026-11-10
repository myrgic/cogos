package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestParsePSRowsAndTreeStats(t *testing.T) {
	rows := parsePSRows([]byte(`  100     1  2000
  101   100   500
  102   100   300
  103   102   200
  200     1  9999
garbage line
  104   103   x
`))
	if len(rows) != 5 {
		t.Fatalf("rows = %+v", rows)
	}
	tree := newProcTree(rows)
	if n, rss := tree.stats(100); n != 4 || rss != (2000+500+300+200)*1024 {
		t.Fatalf("stats(100) = %d, %d", n, rss)
	}
	if n, rss := tree.stats(102); n != 2 || rss != 500*1024 {
		t.Fatalf("stats(102) = %d, %d", n, rss)
	}
	if n, rss := tree.stats(4242); n != 0 || rss != 0 {
		t.Fatalf("stats of a gone pid = %d, %d", n, rss)
	}
}

// stubProcs makes the driver's process table: each live agent pid gets an
// agent row (1000 KiB) and one child (500 KiB), as an MCP server would be.
func stubProcs(d *HermesACPDriver) {
	d.procs.snap = func(context.Context) ([]procRow, error) {
		var rows []procRow
		child := 9_000_000
		seen := map[int]bool{}
		for _, s := range d.snapshotSessions() {
			pid, _ := s.proc.livePID()
			if pid == 0 || seen[pid] { // a fork shares its parent's process: one agent row
				continue
			}
			seen[pid] = true
			rows = append(rows, procRow{PID: pid, PPID: 1, RSSKB: 1000})
			child++
			rows = append(rows, procRow{PID: child, PPID: pid, RSSKB: 500})
		}
		return rows, nil
	}
}

func TestListDetailedAndSummary_ProcessCostAndASharedProcessCountedOnce(t *testing.T) {
	d := fakeHermesDriver(t, nil)
	ctx := context.Background()
	front, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir(), Owner: "stage:room:a", LeaseTTLSecs: 600})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := d.Create(ctx, HermesACPOpts{ForkOf: front.ID()})
	if err != nil {
		t.Fatal(err)
	}
	other, err := d.Create(ctx, HermesACPOpts{Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	stubProcs(d)
	byID := map[string]ManagedSessionInfo{}
	for _, info := range d.ListDetailed(ctx) {
		byID[info.SessionID] = info
	}
	f, k, o := byID[front.ID()], byID[fork.ID()], byID[other.ID()]
	if f.PID == 0 || f.PID != k.PID || f.SharedProcess != 2 || f.ProcCount != 2 || f.RSSBytes != 1500*1024 {
		t.Fatalf("front %+v fork %+v", f, k)
	}
	if o.PID == 0 || o.PID == f.PID || o.SharedProcess != 1 || o.ProcCount != 2 {
		t.Fatalf("other %+v", o)
	}
	if f.Owner != "stage:room:a" || f.LeaseTTLSecs != 600 || f.LastActivityAt.IsZero() || f.TurnRunning {
		t.Fatalf("front lifecycle fields %+v", f)
	}

	sum := d.Summary(ctx)
	if sum.Count != 3 || sum.ByState["live"] != 3 || sum.Processes != 2 || sum.ProcCount != 4 || sum.RSSBytes != 2*1500*1024 {
		t.Fatalf("summary = %+v", sum)
	}
	if sum.ByOwner["stage:room:a"] != 1 || sum.ByOwner["(none)"] != 2 || sum.ByClass["(default)"] != 3 {
		t.Fatalf("summary groups = %+v", sum)
	}
	if sum.IdleTimeoutSecs != 3600 || sum.ReapIntervalSec != 60 || sum.RecentReaps == nil {
		t.Fatalf("summary reaper settings = %+v", sum)
	}
	d.Reap(time.Now().Add(2 * time.Hour))
	sum = d.Summary(ctx)
	if sum.Count != 0 || sum.ReapedTotal != 3 || len(sum.RecentReaps) != 3 || sum.Processes != 0 {
		t.Fatalf("summary after reap = %+v", sum)
	}
}

func TestManagedSessionHTTP_LifecycleFieldsSummaryLeaseHealthAndResumeAfterReap(t *testing.T) {
	ts, d := managedSessionTestServer(t)
	stubProcs(d)
	var created ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions", map[string]any{
		"driver": "hermes-acp", "cwd": t.TempDir(), "owner": "stage:room:h", "lease_ttl_secs": 120,
	}, &created); code != http.StatusCreated {
		t.Fatalf("create = %d", code)
	}
	id := created.SessionID
	if created.Owner != "stage:room:h" || created.LeaseTTLSecs != 120 || created.LeaseExpiresAt == nil {
		t.Fatalf("created = %+v", created)
	}

	var list struct {
		Sessions []map[string]any `json:"sessions"`
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions", nil, &list); code != 200 || len(list.Sessions) != 1 {
		t.Fatalf("list = %d %+v", code, list)
	}
	row := list.Sessions[0]
	for _, k := range []string{"last_activity_at", "idle_secs", "owner", "lease_expires_at", "pid", "proc_count", "rss_bytes", "turn_running", "shared_process"} {
		if _, ok := row[k]; !ok {
			t.Fatalf("list row lacks %q: %v", k, row)
		}
	}

	var got ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/lease", map[string]any{"ttl_secs": 900}, &got); code != 200 || got.LeaseTTLSecs != 900 {
		t.Fatalf("lease = %d %+v", code, got)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/lease", nil, &got); code != 200 || got.LeaseTTLSecs != 900 {
		t.Fatalf("bare renew = %d %+v", code, got)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/lease", map[string]any{"ttl_secs": -1}, nil); code != http.StatusBadRequest {
		t.Fatalf("negative ttl = %d", code)
	}
	var cleared ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/lease", map[string]any{"clear": true}, &cleared); code != 200 || cleared.LeaseExpiresAt != nil || cleared.LeaseTTLSecs != 0 || cleared.Owner != "stage:room:h" {
		t.Fatalf("clear = %d %+v", code, cleared)
	}

	var sum ManagedSessionSummary
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/summary", nil, &sum); code != 200 || sum.Count != 1 || sum.ByOwner["stage:room:h"] != 1 || sum.Processes != 1 {
		t.Fatalf("summary = %d %+v", code, sum)
	}
	var health map[string]any
	if code := msDo(t, ts, "GET", "/health", nil, &health); code != 200 {
		t.Fatalf("health = %d", code)
	}
	block, ok := health["managed_sessions"].(map[string]any)
	if !ok || block["count"] != float64(1) || block["idle_timeout_secs"] != float64(3600) {
		t.Fatalf("health managed_sessions = %v", health["managed_sessions"])
	}

	// Reaped: gone from the surface (404 says "resume it"), and a resume brings it back.
	if r := d.Reap(time.Now().Add(2 * time.Hour)); len(r) != 1 {
		t.Fatalf("reap = %+v", r)
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/"+id, nil, nil); code != http.StatusNotFound {
		t.Fatalf("get after reap = %d", code)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/prompt", map[string]any{"prompt": "hi"}, nil); code != http.StatusNotFound {
		t.Fatalf("prompt after reap = %d", code)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/no-such-session/detach", nil, nil); code != http.StatusNoContent {
		t.Fatalf("detach of an unknown id = %d, want 204 (idempotent)", code)
	}
	var resumed ManagedSessionInfo
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/resume", map[string]any{"owner": "stage:room:h", "lease_ttl_secs": 60}, &resumed); code != 200 || resumed.State != "live" || resumed.Owner != "stage:room:h" {
		t.Fatalf("resume after reap = %d %+v", code, resumed)
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/summary", nil, &sum); code != 200 || sum.ReapedTotal != 1 || len(sum.RecentReaps) != 1 || sum.RecentReaps[0].SessionID != id {
		t.Fatalf("summary after reap+resume = %+v", sum)
	}
	if code := msDo(t, ts, "POST", "/v1/managed-sessions/"+id+"/detach", nil, nil); code != http.StatusNoContent {
		t.Fatalf("detach = %d", code)
	}
	if code := msDo(t, ts, "GET", "/v1/managed-sessions/"+id, nil, nil); code != http.StatusNotFound {
		t.Fatalf("get after detach = %d", code)
	}
	b, _ := json.Marshal(sum.RecentReaps[0])
	if !strings.Contains(string(b), `"reason":"idle"`) {
		t.Fatalf("recent reap = %s", b)
	}
}

func TestManagedSessionsConfig_ReaperSettings(t *testing.T) {
	cases := []struct {
		idle, every  string
		wantIdle     time.Duration
		wantInterval time.Duration
	}{
		{"", "", 0, 0},
		{"30m", "10s", 30 * time.Minute, 10 * time.Second},
		{"0", "", -1, 0},
		{"off", "", -1, 0},
		{"banana", "-3s", 0, 0}, // invalid: defaults, never "off"
		{"2h", "100ms", 2 * time.Hour, time.Second},
	}
	for _, c := range cases {
		idle, every := (&ManagedSessionsConfig{IdleTimeout: c.idle, ReapInterval: c.every}).reaperSettings()
		if idle != c.wantIdle || every != c.wantInterval {
			t.Errorf("%q/%q -> %v/%v, want %v/%v", c.idle, c.every, idle, every, c.wantIdle, c.wantInterval)
		}
	}
	if idle, every := (*ManagedSessionsConfig)(nil).reaperSettings(); idle != 0 || every != 0 {
		t.Errorf("nil config -> %v/%v", idle, every)
	}
}
