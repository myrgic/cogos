package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"syscall"
	"time"
)

// NodeHealth holds the last-known health of all sibling services.
type NodeHealth struct {
	mu       sync.RWMutex
	services map[string]ServiceHealth
	// Timeout bounds each probe (0 = DefaultNodeProbeTimeout).
	Timeout time.Duration
}

// ServiceHealth is the probed state of a single service.
type ServiceHealth struct {
	Port   int       `json:"port"`
	Status string    `json:"status"` // healthy, degraded, down, unknown
	At     time.Time `json:"probed_at"`
	// Reason says why a probe was not healthy ("connection refused",
	// "no answer within 5s", "http 503"); LatencyMs is how long the probe
	// took. Both omitted when there is nothing to say.
	Reason    string `json:"reason,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}

// DefaultNodeProbeTimeout bounds one health probe. It is the stage
// watchdog's own bound (cog apps/stage/deploy/stage-watchdog.sh, curl -m 5):
// what the node already calls "answering". It was 2 s, and every time a
// loaded node's Python stage took longer than that (its event loop
// serializing a 13 MB rooms.json) the probe called it down while it served
// 200 (cog session-eol.md L5: 23 of 44 down transitions in a day had no
// restart near them).
const DefaultNodeProbeTimeout = 5 * time.Second

// probeable reports whether an HTTP health probe is meaningful for svc.
//
// The prober speaks exactly one dialect: GET http://localhost:<port><health>.
// A service that has no port, or an observed/external service that declares no
// health path, cannot answer in that dialect. Probing it anyway produces a
// guaranteed connection failure against http://localhost:0/..., which the
// prober would then report as "down" forever — a service that is running
// perfectly, painted permanently red.
//
// That is worse than reporting nothing. A tile that is always red trains the
// operator to stop reading the health surface, which is precisely the
// silent-dashboard failure this codebase has already been bitten by. So an
// unprobeable service is reported "unknown" — an honest absence of evidence —
// and never "down", which is a positive claim of failure we have not earned.
func probeable(svc ServiceDef) bool {
	if svc.Port == 0 {
		return false
	}
	switch svc.Kind.EffectiveKind() {
	case ServiceKindExternal:
		// External services are known to the kernel for dependency-graph
		// purposes only; per the ServiceKind doc comment above, the kernel
		// never probes them, regardless of whether Health is set.
		return false
	case ServiceKindObserved:
		return svc.Health != ""
	}
	return true
}

// NewNodeHealth returns an empty NodeHealth.
func NewNodeHealth() *NodeHealth {
	return &NodeHealth{services: make(map[string]ServiceHealth)}
}

// Probe checks all sibling services defined in the manifest concurrently.
// Skips the kernel itself (it knows its own health). Each probe is bounded by
// the probe timeout (DefaultNodeProbeTimeout); total wall time is about that
// regardless of service count.
func (nh *NodeHealth) Probe(manifest *NodeManifest, selfPort int) {
	nh.probe(manifest, selfPort, nil)
}

// ProbeOnly probes just the named services (the watcher's quick re-check of
// services that are not healthy).
func (nh *NodeHealth) ProbeOnly(manifest *NodeManifest, selfPort int, names map[string]bool) {
	if len(names) == 0 {
		return
	}
	nh.probe(manifest, selfPort, names)
}

func (nh *NodeHealth) timeout() time.Duration {
	if nh.Timeout > 0 {
		return nh.Timeout
	}
	return DefaultNodeProbeTimeout
}

// probeNodeService GETs one service's health path and says what the answer means:
//
//	200                      healthy
//	any other status         degraded ("http 503": it answered, unhappily)
//	no answer within timeout degraded ("no answer within 5s": something holds
//	                         the port but does not answer in time; a busy
//	                         process, not a missing one)
//	connection refused       down (nothing is listening)
//	anything else            down ("unreachable")
//
// A timeout used to read down, so a slow stage read exactly like a stopped one.
func probeNodeService(client *http.Client, svc ServiceDef, now time.Time) ServiceHealth {
	url := fmt.Sprintf("http://localhost:%d%s", svc.Port, svc.Health)
	t0 := time.Now()
	resp, err := client.Get(url)
	h := ServiceHealth{Port: svc.Port, At: now, LatencyMs: time.Since(t0).Milliseconds()}
	var nerr net.Error
	switch {
	case err == nil:
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			h.Status = "healthy"
		} else {
			h.Status, h.Reason = "degraded", fmt.Sprintf("http %d", resp.StatusCode)
		}
	case errors.Is(err, syscall.ECONNREFUSED):
		h.Status, h.Reason = "down", "connection refused"
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &nerr) && nerr.Timeout()):
		h.Status, h.Reason = "degraded", fmt.Sprintf("no answer within %s", client.Timeout)
	default:
		h.Status, h.Reason = "down", "unreachable"
	}
	return h
}

func (nh *NodeHealth) probe(manifest *NodeManifest, selfPort int, only map[string]bool) {
	client := &http.Client{Timeout: nh.timeout()}
	now := time.Now().UTC()

	type result struct {
		name   string
		health ServiceHealth
	}

	ch := make(chan result, len(manifest.Services))
	count := 0

	for name, svc := range manifest.Services {
		if svc.Port != 0 && svc.Port == selfPort {
			continue
		}
		if only != nil && !only[name] {
			continue
		}
		// Unprobeable services never get an HTTP attempt, so they can never
		// be reported "down" by a probe that was never going to succeed.
		if !probeable(svc) {
			nh.mu.Lock()
			nh.services[name] = ServiceHealth{Port: svc.Port, Status: "unknown", At: now}
			nh.mu.Unlock()
			continue
		}
		count++
		go func(name string, svc ServiceDef) {
			ch <- result{name, probeNodeService(client, svc, now)}
		}(name, svc)
	}

	for i := 0; i < count; i++ {
		r := <-ch
		nh.mu.Lock()
		nh.services[r.name] = r.health
		nh.mu.Unlock()
	}
}

// Snapshot returns a copy of the current service health map.
func (nh *NodeHealth) Snapshot() map[string]ServiceHealth {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	out := make(map[string]ServiceHealth, len(nh.services))
	for k, v := range nh.services {
		out[k] = v
	}
	return out
}

// Summary returns a compact status map (service → status string).
func (nh *NodeHealth) Summary() map[string]string {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	out := make(map[string]string, len(nh.services))
	for k, v := range nh.services {
		out[k] = v.Status
	}
	return out
}

// Counts returns (healthy, total) for quick reporting.
func (nh *NodeHealth) Counts() (int, int) {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	healthy := 0
	for _, v := range nh.services {
		if v.Status == "healthy" {
			healthy++
		}
	}
	return healthy, len(nh.services)
}

// Names returns sorted service names.
func (nh *NodeHealth) Names() []string {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	names := make([]string, 0, len(nh.services))
	for k := range nh.services {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
