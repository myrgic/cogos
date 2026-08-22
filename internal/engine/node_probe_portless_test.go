package engine

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// listenPort extracts the port an httptest.Server bound to.
func listenPort(t *testing.T, srv *httptest.Server) int {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return p
}

// TestProbe_PortlessObservedServiceIsNotDown is the NEGATIVE CONTROL for the
// defect. Before the fix, a portless service was probed at
// http://localhost:0/... , failed to connect, and was reported permanently
// "down" while running perfectly. The assertion that matters is the one about
// what the status is NOT.
func TestProbe_PortlessObservedServiceIsNotDown(t *testing.T) {
	m := &NodeManifest{Services: map[string]ServiceDef{
		"osascript_thing": {Kind: ServiceKindObserved}, // Port 0, no Health
	}}

	nh := NewNodeHealth()
	nh.Probe(m, 9999)

	got := nh.Snapshot()["osascript_thing"]
	if got.Status == "down" {
		t.Fatalf("portless observed service reported %q; this is the exact defect — a running service painted permanently red", got.Status)
	}
	if got.Status != "unknown" {
		t.Errorf("status = %q; want %q", got.Status, "unknown")
	}
	if got.At.IsZero() {
		t.Error("probed_at is zero; an unknown result should still be timestamped")
	}
}

// TestProbe_PortlessKindsAllReportUnknown covers every unprobeable shape.
func TestProbe_PortlessKindsAllReportUnknown(t *testing.T) {
	cases := map[string]ServiceDef{
		"observed_no_port":        {Kind: ServiceKindObserved},
		"external_no_port":        {Kind: ServiceKindExternal},
		"managed_no_port":         {Kind: ServiceKindManaged, Health: "/healthz"},
		"unset_kind_no_port":      {Health: "/healthz"},
		"observed_port_no_health": {Kind: ServiceKindObserved, Port: 65535},
		"external_port_no_health": {Kind: ServiceKindExternal, Port: 65534},
	}
	m := &NodeManifest{Services: cases}

	nh := NewNodeHealth()
	nh.Probe(m, 1)

	snap := nh.Snapshot()
	for name := range cases {
		h, ok := snap[name]
		if !ok {
			t.Errorf("%s: missing from snapshot; an unprobeable service must still be reported", name)
			continue
		}
		if h.Status != "unknown" {
			t.Errorf("%s: status = %q; want %q", name, h.Status, "unknown")
		}
	}
}

// TestProbe_NormalHTTPServiceStillProbes is the REGRESSION GUARD: the ordinary
// path must be untouched.
func TestProbe_NormalHTTPServiceStillProbes(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer degraded.Close()

	// A port nothing is listening on: bind then immediately close.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadPort := listenPort(t, dead)
	dead.Close()

	m := &NodeManifest{Services: map[string]ServiceDef{
		"good":     {Port: listenPort(t, ok), Health: "/healthz"},
		"sick":     {Port: listenPort(t, degraded), Health: "/healthz"},
		"gone":     {Port: deadPort, Health: "/healthz"},
		"observed": {Kind: ServiceKindObserved, Port: listenPort(t, ok), Health: "/healthz"},
	}}

	nh := NewNodeHealth()
	nh.Probe(m, 1)
	snap := nh.Snapshot()

	want := map[string]string{
		"good":     "healthy",
		"sick":     "degraded",
		"gone":     "down",
		"observed": "healthy", // observed WITH a health path is still probed
	}
	for name, w := range want {
		if got := snap[name].Status; got != w {
			t.Errorf("%s: status = %q; want %q", name, got, w)
		}
	}

	// "gone" proves the fix did not blanket-suppress genuine failures.
	if snap["gone"].Status != "down" {
		t.Error("a real unreachable service must still report down; otherwise the fix hides real outages")
	}
}

// TestProjectService_UnknownIsNotRunning verifies (rather than assumes) that
// projectService's `running` predicate treats "unknown" as falsy.
func TestProjectService_UnknownIsNotRunning(t *testing.T) {
	view := projectService("osascript_thing",
		ServiceDef{Kind: ServiceKindObserved},
		ServiceHealth{Status: "unknown"})

	if view.Running {
		t.Error("running = true for status \"unknown\"; unknown is absence of evidence, not evidence of liveness")
	}
	if view.Health == nil {
		t.Fatal("health view is nil; an unknown status is a real status and must surface in /v1/services")
	}
	if view.Health.Status != "unknown" {
		t.Errorf("health.status = %q; want %q", view.Health.Status, "unknown")
	}
}

// TestNodeHealth_UnknownIsNotCountedHealthy guards the aggregate counters.
func TestNodeHealth_UnknownIsNotCountedHealthy(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	m := &NodeManifest{Services: map[string]ServiceDef{
		"good":     {Port: listenPort(t, ok), Health: "/healthz"},
		"portless": {Kind: ServiceKindObserved},
	}}

	nh := NewNodeHealth()
	nh.Probe(m, 1)

	healthy, total := nh.Counts()
	if healthy != 1 {
		t.Errorf("healthy = %d; want 1 (unknown must not inflate the healthy count)", healthy)
	}
	if total != 2 {
		t.Errorf("total = %d; want 2 (unknown services are still reported)", total)
	}
	if s := nh.Summary()["portless"]; s != "unknown" {
		t.Errorf("summary[portless] = %q; want %q", s, "unknown")
	}
}

// TestProbeService_PortlessReportsUnknown covers the CLI probe path.
func TestProbeService_PortlessReportsUnknown(t *testing.T) {
	health, _, _ := probeService(ServiceDef{Kind: ServiceKindObserved})
	if health == "down" {
		t.Fatal("cogos node status reported a portless observed service as down")
	}
	if health != "unknown" {
		t.Errorf("health = %q; want %q", health, "unknown")
	}
}

// TestProbeable_Table pins the predicate itself.
func TestProbeable_Table(t *testing.T) {
	cases := []struct {
		name string
		def  ServiceDef
		want bool
	}{
		{"managed with port", ServiceDef{Port: 8080, Health: "/h"}, true},
		{"managed no port", ServiceDef{Health: "/h"}, false},
		{"observed with port and health", ServiceDef{Kind: ServiceKindObserved, Port: 8080, Health: "/h"}, true},
		{"observed with port no health", ServiceDef{Kind: ServiceKindObserved, Port: 8080}, false},
		{"observed no port", ServiceDef{Kind: ServiceKindObserved}, false},
		{"external no port", ServiceDef{Kind: ServiceKindExternal}, false},
		{"unset kind with port", ServiceDef{Port: 8080}, true},
	}
	for _, c := range cases {
		if got := probeable(c.def); got != c.want {
			t.Errorf("%s: probeable = %v; want %v", c.name, got, c.want)
		}
	}
}
