package engine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRestarter records Restart calls; every other method is unused by the
// watcher.
type fakeRestarter struct {
	ObserverSupervisor
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeRestarter) Restart(_ context.Context, name string, _ ServiceDef) (*ServiceStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	return &ServiceStatus{}, f.err
}

func (f *fakeRestarter) n() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type evt struct {
	typ  string
	data map[string]interface{}
}

type eventLog struct {
	mu sync.Mutex
	ev []evt
}

func (l *eventLog) emit(t string, d map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ev = append(l.ev, evt{t, d})
}

func (l *eventLog) count(t string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.ev {
		if e.typ == t {
			n++
		}
	}
	return n
}

// switchable is a test service whose health answer can be flipped.
type switchable struct {
	ts *httptest.Server
	ok atomic.Bool
}

func newSwitchable(t *testing.T) *switchable {
	t.Helper()
	s := &switchable{}
	s.ok.Store(true)
	s.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.ok.Load() {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(503)
	}))
	t.Cleanup(s.ts.Close)
	return s
}

// clockWatcher returns a watcher over one service with a controllable clock.
func clockWatcher(t *testing.T, def ServiceDef, r ServiceSupervisor) (*NodeWatcher, *eventLog, *time.Time) {
	t.Helper()
	log := &eventLog{}
	m := &NodeManifest{Services: map[string]ServiceDef{"svc": def}}
	w := NewNodeWatcher(NewNodeHealth(), m, 1, log.emit, r)
	now := time.Date(2026, 10, 6, 18, 0, 0, 0, time.UTC)
	w.now = func() time.Time { return now }
	return w, log, &now
}

func TestNodeWatch_TransitionsEmitOncePerChange(t *testing.T) {
	t.Parallel()
	s := newSwitchable(t)
	w, log, now := clockWatcher(t, ServiceDef{Kind: ServiceKindObserved, Port: portFromURL(t, s.ts.URL), Health: "/health"}, nil)
	ctx := context.Background()

	w.Tick(ctx) // first sight, healthy: nothing to report
	s.ok.Store(false)
	for i := 0; i < 4; i++ { // a long outage is ONE down event, not one per probe
		*now = now.Add(time.Minute)
		w.Tick(ctx)
	}
	s.ok.Store(true)
	*now = now.Add(time.Minute)
	w.Tick(ctx)
	w.Tick(ctx)

	if got := log.count(EventNodeServiceDown); got != 1 {
		t.Fatalf("down events = %d, want 1 (%v)", got, log.ev)
	}
	if got := log.count(EventNodeServiceRecovered); got != 1 {
		t.Fatalf("recovered events = %d, want 1 (%v)", got, log.ev)
	}
	down := log.ev[0].data
	if down["status"] != "degraded" || down["remediation"] != "report" || down["kind"] != "observed" {
		t.Fatalf("down event = %v", down)
	}
	if rec := log.ev[1].data; rec["down_for_s"] != 240 {
		t.Fatalf("recovered down_for_s = %v, want 240", rec["down_for_s"])
	}
}

func TestNodeWatch_UnknownIsNeverDown(t *testing.T) {
	t.Parallel()
	// An observed service with no health path is unprobeable: "unknown", which is
	// an absence of evidence and must never become a down event or a restart.
	r := &fakeRestarter{}
	w, log, now := clockWatcher(t, ServiceDef{Kind: ServiceKindObserved, Port: 9}, r) // not selfPort (1): it must be evaluated
	for i := 0; i < 5; i++ {
		*now = now.Add(time.Minute)
		w.Tick(context.Background())
	}
	if st := w.health.Snapshot()["svc"]; st.Status != "unknown" {
		t.Fatalf("precondition: status = %q, want unknown (the case under test)", st.Status)
	}
	if len(log.ev) != 0 || r.n() != 0 {
		t.Fatalf("unknown produced events %v / restarts %d", log.ev, r.n())
	}
}

func TestNodeWatch_ObservedIsNeverRestarted(t *testing.T) {
	t.Parallel()
	// The stage is kind=observed: agents live in its process, so the kernel
	// reports it down and never kills it, even with restart: always + a label.
	s := newSwitchable(t)
	s.ok.Store(false)
	r := &fakeRestarter{}
	w, log, now := clockWatcher(t, ServiceDef{Kind: ServiceKindObserved, Port: portFromURL(t, s.ts.URL),
		Health: "/health", Restart: "always", Launchd: "com.cogos.stage"}, r)
	for i := 0; i < 10; i++ {
		*now = now.Add(time.Hour)
		w.Tick(context.Background())
	}
	if r.n() != 0 {
		t.Fatalf("observed service restarted %d times", r.n())
	}
	if log.count(EventNodeServiceDown) != 1 || log.count(EventNodeServiceRestart) != 0 {
		t.Fatalf("events = %v", log.ev)
	}
}

func TestNodeWatch_ManagedRestartsAfterThreeWithBackoffAndCap(t *testing.T) {
	t.Parallel()
	s := newSwitchable(t)
	s.ok.Store(false)
	r := &fakeRestarter{err: errors.New("still wedged")}
	w, log, now := clockWatcher(t, ServiceDef{Kind: ServiceKindManaged, Port: portFromURL(t, s.ts.URL),
		Health: "/health", Restart: "always", Launchd: "ai.example.svc"}, r)
	ctx := context.Background()
	tick := func(d time.Duration) { *now = now.Add(d); w.Tick(ctx) }

	tick(0)
	tick(time.Minute)
	if r.n() != 0 {
		t.Fatalf("restarted after 2 failed probes; want only after %d", DefaultRemediateAfter)
	}
	tick(time.Minute) // 3rd consecutive failure: first restart
	if r.n() != 1 {
		t.Fatalf("restarts after 3 failures = %d, want 1", r.n())
	}
	tick(time.Minute) // inside the 2 min backoff: no restart
	if r.n() != 1 {
		t.Fatalf("restarted inside backoff: %d", r.n())
	}
	tick(time.Minute) // 2 min after the first: second restart, next backoff 4 min
	tick(2 * time.Minute)
	if r.n() != 2 {
		t.Fatalf("restarts = %d, want 2 (backoff doubles)", r.n())
	}
	tick(2 * time.Minute) // 4 min after the second: third restart (the cap)
	if r.n() != 3 {
		t.Fatalf("restarts = %d, want 3", r.n())
	}
	for i := 0; i < 5; i++ { // capped: no more restarts within the hour, one capped event
		tick(5 * time.Minute)
	}
	if r.n() != 3 || log.count(EventNodeServiceRemediationCapped) != 1 {
		t.Fatalf("after cap: restarts %d (want 3), capped events %d (want 1)", r.n(), log.count(EventNodeServiceRemediationCapped))
	}
	if log.count(EventNodeServiceRestart) != 3 {
		t.Fatalf("restart events = %d, want 3", log.count(EventNodeServiceRestart))
	}
	for _, e := range log.ev {
		if e.typ == EventNodeServiceRestart && (e.data["ok"] != false || e.data["error"] != "still wedged") {
			t.Fatalf("failed restart not reported as failed: %v", e.data)
		}
	}
	// An hour on, the window has rolled: remediation may try again.
	tick(time.Hour)
	if r.n() != 4 {
		t.Fatalf("after the window rolled: restarts %d, want 4", r.n())
	}
}

func TestNodeWatch_RecoveryResetsTheCount(t *testing.T) {
	t.Parallel()
	s := newSwitchable(t)
	r := &fakeRestarter{}
	w, _, now := clockWatcher(t, ServiceDef{Kind: ServiceKindManaged, Port: portFromURL(t, s.ts.URL),
		Health: "/health", Restart: "always", Launchd: "ai.example.svc"}, r)
	ctx := context.Background()
	tick := func(d time.Duration) { *now = now.Add(d); w.Tick(ctx) }
	// two failures, a recovery, two failures: never 3 consecutive, never restarted
	for _, ok := range []bool{false, false, true, false, false, true} {
		s.ok.Store(ok)
		tick(time.Minute)
	}
	if r.n() != 0 {
		t.Fatalf("restarted on non-consecutive failures: %d", r.n())
	}
}

func TestNodeWatch_NotRemediableWithoutPolicy(t *testing.T) {
	t.Parallel()
	for name, def := range map[string]ServiceDef{
		"restart-manual": {Kind: ServiceKindManaged, Restart: "manual", Launchd: "x"},
		"no-label":       {Kind: ServiceKindManaged, Restart: "always"},
		"external":       {Kind: ServiceKindExternal, Restart: "always", Launchd: "x"},
		"observed":       {Kind: ServiceKindObserved, Restart: "always", Launchd: "x"},
	} {
		if remediable(def) {
			t.Errorf("%s: remediable = true, want false", name)
		}
	}
	if !remediable(ServiceDef{Restart: "always", Launchd: "x"}) { // kind unset = managed
		t.Error("managed + always + label: remediable = false, want true")
	}
}

func TestNodeWatch_NoRestarterMeansReportOnly(t *testing.T) {
	t.Parallel()
	// enable_node_remediation off (the default): the watcher has no restarter;
	// a managed service is still reported, never restarted.
	s := newSwitchable(t)
	s.ok.Store(false)
	w, log, now := clockWatcher(t, ServiceDef{Kind: ServiceKindManaged, Port: portFromURL(t, s.ts.URL),
		Health: "/health", Restart: "always", Launchd: "x"}, nil)
	for i := 0; i < 6; i++ {
		*now = now.Add(time.Minute)
		w.Tick(context.Background())
	}
	if log.count(EventNodeServiceDown) != 1 || log.count(EventNodeServiceRestart) != 0 {
		t.Fatalf("events = %v", log.ev)
	}
}

func TestNodeWatch_RunProbesWhatever(t *testing.T) {
	t.Parallel()
	// The cadence is the watcher's own: Run probes immediately and then every
	// Interval, with no reference to the process state (#429: the old probe only
	// ran from emitHeartbeat, which skips while StateActive).
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(200) }))
	defer ts.Close()
	m := &NodeManifest{Services: map[string]ServiceDef{"svc": {Port: portFromURL(t, ts.URL), Health: "/health"}}}
	w := NewNodeWatcher(NewNodeHealth(), m, 1, nil, nil)
	w.Interval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Millisecond)
	defer cancel()
	w.Run(ctx)
	if n := hits.Load(); n < 3 {
		t.Fatalf("probes in 130ms at 20ms cadence = %d, want >= 3", n)
	}
}

func TestProcess_NewNodeWatcherWiring(t *testing.T) {
	t.Parallel()
	m := &NodeManifest{Services: map[string]ServiceDef{}}
	p := &Process{cfg: &Config{Port: 6931, NodeProbeInterval: 15}, nodeHealth: NewNodeHealth(), nodeManifest: m}
	w := p.newNodeWatcher()
	if w == nil || w.Interval != 15*time.Second || w.restarter != nil {
		t.Fatalf("watcher = %+v; want 15s interval and no restarter (remediation off by default)", w)
	}
	p.cfg.EnableNodeRemediation = true
	if w := p.newNodeWatcher(); w.restarter == nil {
		t.Fatal("enable_node_remediation: watcher has no restarter")
	}
	if (&Process{cfg: &Config{}}).newNodeWatcher() != nil {
		t.Fatal("no manifest: want no watcher")
	}
}
