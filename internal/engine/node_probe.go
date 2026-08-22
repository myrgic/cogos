package engine

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NodeHealth holds the last-known health of all sibling services.
type NodeHealth struct {
	mu       sync.RWMutex
	services map[string]ServiceHealth
}

// ServiceHealth is the probed state of a single service.
type ServiceHealth struct {
	Port   int       `json:"port"`
	Status string    `json:"status"` // healthy, degraded, down, unknown
	At     time.Time `json:"probed_at"`
}

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
	case ServiceKindObserved, ServiceKindExternal:
		return svc.Health != ""
	}
	return true
}

// NewNodeHealth returns an empty NodeHealth.
func NewNodeHealth() *NodeHealth {
	return &NodeHealth{services: make(map[string]ServiceHealth)}
}

// Probe checks all sibling services defined in the manifest concurrently.
// Skips the kernel itself (it knows its own health).
// Each probe has a 2s timeout; total wall time is ~2s regardless of service count.
func (nh *NodeHealth) Probe(manifest *NodeManifest, selfPort int) {
	client := &http.Client{Timeout: 2 * time.Second}
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
			status := "down"
			url := fmt.Sprintf("http://localhost:%d%s", svc.Port, svc.Health)
			resp, err := client.Get(url)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					status = "healthy"
				} else {
					status = "degraded"
				}
			}
			ch <- result{name, ServiceHealth{Port: svc.Port, Status: status, At: now}}
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
