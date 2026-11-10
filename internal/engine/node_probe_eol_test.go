package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// cog session-eol.md L5: a stage that answered slowly read exactly like a
// stopped one ("down"), and stayed "down" for one to two probe intervals after
// it came back. The probe now says which it was.
func TestNodeProbe_RefusedIsDownSlowIsDegradedAndSaysWhy(t *testing.T) {
	t.Parallel()
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedPort := portFromURL(t, closed.URL)
	closed.Close()
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(200)
	}))
	defer slow.Close()
	defer close(release)
	unhappy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer unhappy.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer ok.Close()

	m := &NodeManifest{Services: map[string]ServiceDef{
		"stopped": {Port: closedPort, Health: "/h"},
		"slow":    {Port: portFromURL(t, slow.URL), Health: "/h"},
		"unhappy": {Port: portFromURL(t, unhappy.URL), Health: "/h"},
		"fine":    {Port: portFromURL(t, ok.URL), Health: "/h"},
	}}
	nh := NewNodeHealth()
	nh.Timeout = 150 * time.Millisecond
	nh.Probe(m, 0)
	snap := nh.Snapshot()
	if h := snap["stopped"]; h.Status != "down" || h.Reason != "connection refused" {
		t.Errorf("stopped = %+v", h)
	}
	if h := snap["slow"]; h.Status != "degraded" || !strings.HasPrefix(h.Reason, "no answer within") || h.LatencyMs < 100 {
		t.Errorf("slow = %+v, want degraded (it holds the port, it is not gone)", h)
	}
	if h := snap["unhappy"]; h.Status != "degraded" || h.Reason != "http 503" {
		t.Errorf("unhappy = %+v", h)
	}
	if h := snap["fine"]; h.Status != "healthy" || h.Reason != "" {
		t.Errorf("fine = %+v", h)
	}
	if NewNodeHealth().timeout() != DefaultNodeProbeTimeout || DefaultNodeProbeTimeout != 5*time.Second {
		t.Errorf("default probe timeout = %v", NewNodeHealth().timeout())
	}
}

func TestNodeWatch_ReportOnlyServiceRecoveryShowsWithinTheRecheck(t *testing.T) {
	t.Parallel()
	s := newSwitchable(t)
	s.ok.Store(false)
	log := &eventLog{}
	m := &NodeManifest{Services: map[string]ServiceDef{
		"stage": {Kind: ServiceKindObserved, Port: portFromURL(t, s.ts.URL), Health: "/h"},
	}}
	nh := NewNodeHealth()
	w := NewNodeWatcher(nh, m, 1, log.emit, nil)
	w.Interval = time.Hour // only the re-check can notice the recovery
	w.RecheckInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for log.count(EventNodeServiceDown) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no down event")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.ok.Store(true)
	for log.count(EventNodeServiceRecovered) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("recovery not seen within the re-check; events=%v status=%+v", log.ev, nh.Snapshot()["stage"])
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h := nh.Snapshot()["stage"]; h.Status != "healthy" {
		t.Fatalf("status after recovery = %+v", h)
	}
	if log.count(EventNodeServiceDown) != 1 {
		t.Fatalf("down events = %d, want 1 (re-checks of a still-down service are not new transitions)", log.count(EventNodeServiceDown))
	}
	cancel()
	<-done
}

func TestNodeWatch_RemediableServiceIsNotRechecked(t *testing.T) {
	t.Parallel()
	s := newSwitchable(t)
	s.ok.Store(false)
	w, _, now := clockWatcher(t, ServiceDef{Kind: ServiceKindManaged, Port: portFromURL(t, s.ts.URL),
		Health: "/health", Restart: "always", Launchd: "x"}, &fakeRestarter{})
	*now = now.Add(time.Minute)
	w.Tick(context.Background())
	if names := w.recheckNames(); len(names) != 0 {
		t.Fatalf("recheck names = %v: a remediated service keeps the tick cadence its RemediateAfter counts in", names)
	}
}
