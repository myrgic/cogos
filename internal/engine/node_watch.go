package engine

// node_watch.go — the node service watcher (#429): probe on its own cadence,
// report transitions as ledger events, and remediate declared-restartable
// services.
//
// Before this, NodeHealth.Probe ran only from emitHeartbeat, which returns early
// whenever the process is StateActive. On a busy node that meant the sibling
// services were looked at roughly once an hour (2026-10-06: probed_at 40+ min
// stale on a busy node), and a service that went down was recorded in /health and
// nothing else: "observation without reconciliation" (#429, mod3 wedged for ~20
// minutes while the kernel watched it).
//
// The watcher closes that loop:
//
//  1. Cadence. It runs on its own ticker (node_probe_interval, default 60 s),
//     independent of the process state machine, in its own goroutine so a slow
//     probe or a restart never blocks the process run loop.
//  2. Transitions. A probeable service that leaves "healthy" emits
//     node.service.down once; when it comes back it emits node.service.recovered
//     once. Ledger-first: the events go through EmitEvent (hash chain + broker),
//     so subscribers see them without polling /health. "unknown" (unprobeable:
//     no port, or an observed service with no health path) is an absence of
//     evidence and never produces a down event.
//  3. Remediation, by kind:
//       - managed + restart: always + a launchd label: after RemediateAfter
//         consecutive non-healthy probes, Restart via the ServiceSupervisor
//         (LaunchctlController on darwin). Attempts back off exponentially from
//         RestartBackoff and are capped at MaxRestartsPerHour over a rolling
//         hour; hitting the cap emits node.service.remediation_capped (once per
//         capping episode, numbered) and makes no attempt until the window has
//         room again, so a crash-looping service gets at most MaxRestartsPerHour
//         restarts an hour and every capping episode is reported.
//       - observed / external, or restart != always: never restarted, only
//         reported. (The stage is observed: agents live inside its process, so
//         the kernel must not kill it under live work.)
//
// The /health node map is unchanged in shape: it is still NodeHealth.Summary().

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// Node watcher defaults (Config.NodeProbeInterval overrides the cadence).
const (
	DefaultNodeProbeInterval  = 60 * time.Second
	DefaultRemediateAfter     = 3
	DefaultRestartBackoff     = 2 * time.Minute
	DefaultRestartBackoffMax  = 30 * time.Minute
	DefaultMaxRestartsPerHour = 3
	nodeRestartTimeout        = 30 * time.Second
)

// Event types emitted by the watcher.
const (
	EventNodeServiceDown              = "node.service.down"
	EventNodeServiceRecovered         = "node.service.recovered"
	EventNodeServiceRestart           = "node.service.restart"
	EventNodeServiceRemediationCapped = "node.service.remediation_capped"
)

// NodeWatcher probes the manifest's sibling services on a fixed cadence and acts
// on what it sees. Construct with NewNodeWatcher; drive with Run (production) or
// Tick (tests).
type NodeWatcher struct {
	health   *NodeHealth
	manifest *NodeManifest
	selfPort int

	// emit appends an event to the ledger (Process.EmitEvent in production).
	emit func(eventType string, data map[string]interface{})
	// restarter restarts a managed service (LaunchctlController on darwin). Nil
	// means observe-only: transitions are still reported, nothing is restarted.
	restarter ServiceSupervisor

	Interval           time.Duration
	RemediateAfter     int
	RestartBackoff     time.Duration
	RestartBackoffMax  time.Duration
	MaxRestartsPerHour int

	now func() time.Time

	mu    sync.Mutex
	state map[string]*watchState

	// inflight counts restarts running off the tick (see remediate).
	inflight sync.WaitGroup
}

// watchState is the watcher's memory of one service between ticks.
type watchState struct {
	status         string    // last probed status ("" = never seen)
	downSince      time.Time // first non-healthy probe of the current outage
	consecutive    int       // consecutive non-healthy probes
	restarts       []time.Time
	nextRestart    time.Time // earliest time the next restart may run (backoff)
	capped         bool      // remediation_capped emitted for the current capping episode
	capEpisodes    int       // capping episodes in the current outage (reset on recovery)
	restarting     bool      // a restart for this service is running (off the tick)
	outageRestarts int       // restarts started during the current outage (reset on recovery)
}

// NewNodeWatcher returns a watcher with the production defaults.
func NewNodeWatcher(health *NodeHealth, manifest *NodeManifest, selfPort int,
	emit func(string, map[string]interface{}), restarter ServiceSupervisor) *NodeWatcher {
	return &NodeWatcher{
		health:             health,
		manifest:           manifest,
		selfPort:           selfPort,
		emit:               emit,
		restarter:          restarter,
		Interval:           DefaultNodeProbeInterval,
		RemediateAfter:     DefaultRemediateAfter,
		RestartBackoff:     DefaultRestartBackoff,
		RestartBackoffMax:  DefaultRestartBackoffMax,
		MaxRestartsPerHour: DefaultMaxRestartsPerHour,
		now:                func() time.Time { return time.Now().UTC() },
		state:              make(map[string]*watchState),
	}
}

// Run probes once immediately, then every Interval until ctx is done.
func (w *NodeWatcher) Run(ctx context.Context) {
	if w == nil || w.manifest == nil {
		return
	}
	interval := w.Interval
	if interval <= 0 {
		interval = DefaultNodeProbeInterval
	}
	w.Tick(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Tick(ctx)
		}
	}
}

// Tick runs one probe pass and acts on it. Safe to call concurrently with
// readers of NodeHealth; Tick itself is serialized.
func (w *NodeWatcher) Tick(ctx context.Context) {
	if w == nil || w.manifest == nil {
		return
	}
	w.health.Probe(w.manifest, w.selfPort)
	snap := w.health.Snapshot()

	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	for name, svc := range w.manifest.Services {
		h, ok := snap[name]
		if !ok {
			continue // the kernel itself (selfPort) is never in the snapshot
		}
		w.observe(ctx, name, svc, h.Status, now)
	}
}

// observe folds one probe result into the service's state: transition events,
// then remediation. Caller holds w.mu.
func (w *NodeWatcher) observe(ctx context.Context, name string, svc ServiceDef, status string, now time.Time) {
	st := w.state[name]
	if st == nil {
		st = &watchState{}
		w.state[name] = st
	}
	prev := st.status
	st.status = status

	if status == "unknown" {
		// No evidence either way: never a transition, never a restart. An outage
		// in progress is left as it was (the next real probe decides).
		return
	}

	if status == "healthy" {
		if prev != "" && prev != "healthy" && prev != "unknown" {
			w.emitEvent(EventNodeServiceRecovered, map[string]interface{}{
				"service":    name,
				"from":       prev,
				"down_for_s": int(now.Sub(st.downSince).Seconds()),
				"restarts":   st.outageRestarts, // this outage only; the hourly cap uses st.restarts
				"kind":       string(svc.Kind.EffectiveKind()),
			})
		}
		st.consecutive, st.downSince, st.capped, st.capEpisodes, st.outageRestarts = 0, time.Time{}, false, 0, 0
		st.nextRestart = time.Time{}
		return
	}

	// down or degraded
	st.consecutive++
	if st.downSince.IsZero() {
		st.downSince = now
	}
	if prev != status && (prev == "" || prev == "healthy" || prev == "unknown") {
		w.emitEvent(EventNodeServiceDown, map[string]interface{}{
			"service":     name,
			"status":      status,
			"from":        prevOrNone(prev),
			"port":        svc.Port,
			"kind":        string(svc.Kind.EffectiveKind()),
			"remediation": w.remediationPolicy(svc),
		})
	}
	w.remediate(ctx, name, svc, st, now)
}

// remediable reports whether svc's declaration allows the kernel to restart it
// (kind managed, restart: always, a launchd label). Necessary, not sufficient:
// see willRestart.
func remediable(svc ServiceDef) bool {
	return svc.Kind.EffectiveKind() == ServiceKindManaged && svc.Restart == "always" && svc.Launchd != ""
}

// willRestart is the one predicate for "this watcher restarts svc when it stays
// down": the declaration allows it AND a restarter is wired (enable_node_remediation).
// Both the event payload and remediate read it, so what the event promises is
// what the watcher does.
func (w *NodeWatcher) willRestart(svc ServiceDef) bool {
	return w.restarter != nil && remediable(svc)
}

// remediationPolicy names what this watcher will do about svc when it is down.
func (w *NodeWatcher) remediationPolicy(svc ServiceDef) string {
	if w.willRestart(svc) {
		return "restart"
	}
	return "report"
}

func prevOrNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// remediate starts a restart of a remediable service once it has failed
// RemediateAfter consecutive probes, with exponential backoff and an hourly cap.
// Caller holds w.mu; the restart itself runs in its own goroutine, one per
// service at a time.
func (w *NodeWatcher) remediate(ctx context.Context, name string, svc ServiceDef, st *watchState, now time.Time) {
	if !w.willRestart(svc) {
		return
	}
	if st.consecutive < w.remediateAfter() || now.Before(st.nextRestart) || st.restarting {
		return
	}
	// Hourly cap over a rolling window.
	cut := now.Add(-time.Hour)
	kept := st.restarts[:0]
	for _, t := range st.restarts {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	st.restarts = kept
	if len(st.restarts) >= w.maxPerHour() {
		if !st.capped {
			st.capped = true
			st.capEpisodes++
			w.emitEvent(EventNodeServiceRemediationCapped, map[string]interface{}{
				"service":  name,
				"launchd":  svc.Launchd,
				"restarts": len(st.restarts),
				"window_s": 3600,
				"down_s":   int(now.Sub(st.downSince).Seconds()),
				"episode":  st.capEpisodes,
			})
		}
		return
	}
	// The rolling window has room again: this capping episode is over, so the
	// next time the cap is hit (same outage or not) it is reported again.
	st.capped = false

	attempt := len(st.restarts) + 1
	st.restarts = append(st.restarts, now)
	st.outageRestarts++
	st.nextRestart = now.Add(w.backoff(attempt))
	st.restarting = true
	data := map[string]interface{}{
		"service":     name,
		"launchd":     svc.Launchd,
		"attempt":     attempt,
		"consecutive": st.consecutive,
		"next_after":  st.nextRestart.Format(time.RFC3339),
	}
	// The restart itself runs outside Tick and outside w.mu (cog-review #667
	// round 3): launchctl stop/start can take up to nodeRestartTimeout, and a
	// synchronous call here would stall observation of every other service and
	// drop ticks. Decide under the lock, act off it; at most one restart per
	// service is in flight (st.restarting), and the backoff already set above
	// spaces the next one.
	w.inflight.Add(1)
	go func() {
		defer w.inflight.Done()
		rctx, cancel := context.WithTimeout(ctx, nodeRestartTimeout)
		_, err := w.restarter.Restart(rctx, name, svc)
		cancel()
		data["ok"] = err == nil
		if err != nil {
			data["error"] = err.Error()
			slog.Warn("node watch: restart failed", "service", name, "attempt", attempt, "err", err)
		} else {
			slog.Info("node watch: restarted", "service", name, "attempt", attempt)
		}
		w.emitEvent(EventNodeServiceRestart, data)
		w.mu.Lock()
		st.restarting = false
		w.mu.Unlock()
	}()
}

// waitRestarts blocks until every restart started so far has returned and
// emitted its event. Tests use it; production never needs to.
func (w *NodeWatcher) waitRestarts() { w.inflight.Wait() }

// backoff is the wait after the n-th restart (1-based): RestartBackoff doubling,
// capped at RestartBackoffMax.
func (w *NodeWatcher) backoff(n int) time.Duration {
	d := w.RestartBackoff
	if d <= 0 {
		d = DefaultRestartBackoff
	}
	max := w.RestartBackoffMax
	if max <= 0 {
		max = DefaultRestartBackoffMax
	}
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

func (w *NodeWatcher) remediateAfter() int {
	if w.RemediateAfter <= 0 {
		return DefaultRemediateAfter
	}
	return w.RemediateAfter
}

func (w *NodeWatcher) maxPerHour() int {
	if w.MaxRestartsPerHour <= 0 {
		return DefaultMaxRestartsPerHour
	}
	return w.MaxRestartsPerHour
}

// newNodeWatcher builds the process's node watcher from its manifest and config,
// or nil when there is no manifest. Remediation (a restarter) is wired only when
// enable_node_remediation is set; without it the watcher probes and reports.
func (p *Process) newNodeWatcher() *NodeWatcher {
	if p == nil || p.nodeManifest == nil {
		return nil
	}
	var restarter ServiceSupervisor
	switch {
	case p.cfg.EnableNodeRemediation && runtime.GOOS == "darwin":
		restarter = NewLaunchctlController()
	case p.cfg.EnableNodeRemediation:
		// No launchctl here (service_supervisor_stub.go: every Restart is
		// ErrNotControllable until #101's SystemdSupervisor). Wiring the stub
		// would announce "restart" and burn the hourly budget on guaranteed
		// failures, so the watcher stays report-only and says why.
		slog.Warn("node watch: enable_node_remediation has no effect on this platform; reporting only", "goos", runtime.GOOS)
	}
	w := NewNodeWatcher(p.nodeHealth, p.nodeManifest, p.cfg.Port, p.emitEvent, restarter)
	if p.cfg.NodeProbeInterval > 0 {
		w.Interval = time.Duration(p.cfg.NodeProbeInterval) * time.Second
	}
	return w
}

func (w *NodeWatcher) emitEvent(eventType string, data map[string]interface{}) {
	slog.Info("node watch: "+eventType, "service", data["service"])
	if w.emit != nil {
		w.emit(eventType, data)
	}
}
