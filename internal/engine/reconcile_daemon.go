// reconcile_daemon.go — daemon-resident continuous reconcile loop driver.
//
// ReconcileDaemon closes ADR-092 §2 step 4: "Reconcile loop start — periodic /
// on-demand reconciliation begins." It periodically iterates all registered
// Reconcilables and drives the full per-provider cycle:
//
//	LoadConfig → FetchLive → ComputePlan → ApplyPlan → BuildState → WriteState
//
// Each provider's cycle is error-isolated: a panic or error in one provider
// does not block or terminate the cycles of other providers (ADR-092 §3).
//
// Per-provider error isolation is non-negotiable per ADR-092 §3 at-least-once
// semantics: one bad Reconcilable must not block others.
//
// ADR-091 Layer: Kernel (requires a running process; consumes Substrate
// primitives pkg/reconcile.Reconcilable + process-local registry).
// ADR-095: this file implements the contract specified therein.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// ReconcileDaemonState represents the lifecycle state of the daemon.
type ReconcileDaemonState string

const (
	// ReconcileDaemonStarting is the initial state before the goroutine starts.
	ReconcileDaemonStarting ReconcileDaemonState = "Starting"
	// ReconcileDaemonLive means the goroutine is running; last tick had no errors.
	ReconcileDaemonLive ReconcileDaemonState = "Live"
	// ReconcileDaemonStalled means running but last tick had provider errors.
	// The daemon continues ticking.
	ReconcileDaemonStalled ReconcileDaemonState = "Stalled"
	// ReconcileDaemonShutdown means context was cancelled; goroutine has exited.
	ReconcileDaemonShutdown ReconcileDaemonState = "Shutdown"
)

// ReconcileDaemonConfig holds configuration for the ReconcileDaemon.
type ReconcileDaemonConfig struct {
	// WorkspaceRoot is the workspace root path, passed to LoadConfig and
	// reconcile.LoadState / reconcile.WriteState.
	WorkspaceRoot string

	// PollInterval is how often all registered providers are iterated.
	// Default: 30 seconds.
	PollInterval time.Duration

	// MaxConcurrent is the maximum number of providers reconciled concurrently
	// within a single tick. Default: 1 (serial) per ADR-095 §5 rationale:
	// serializes all providers to avoid ADR-092 §1 ledger chain-break risk until
	// the per-session mutex is added to pkg/cogblock.AppendEvent.
	MaxConcurrent int

	// ShutdownGracePeriod is how long the daemon waits for in-flight cycles to
	// complete after context cancel before returning. Default: 5 seconds.
	ShutdownGracePeriod time.Duration

	// Providers, when non-nil, is an explicit list of Reconcilables the daemon
	// iterates instead of the global registry. This is the ADR-101 Phase 2
	// registry-isolation mechanism: integration tests inject real providers here
	// without touching the global registry (which may contain daemon-binary stub
	// providers). When nil, the daemon uses reconcile.ListProviders() as before.
	// Production code leaves this nil; only testkernel sets it.
	Providers []reconcile.Reconcilable

	// Convergence tunes the per-provider anomaly thresholds (cost-over-budget
	// and persistent-degraded). Zero values fall back to sensible defaults.
	Convergence ConvergenceConfig

	// RetryMaxSkipTicks caps the per-provider retry skip window after
	// consecutive failed cycles. Default 32 (~16 min at the 30s PollInterval).
	//
	// Before this existed the daemon retried every registered provider on the
	// fixed PollInterval forever, healthy or permanently broken alike: a
	// provider whose ApplyPlan could not possibly succeed (missing actuator
	// binary) burned ~2,880 identical retries a day.
	RetryMaxSkipTicks int

	// RetryJitter is the fraction of the nominal skip window randomized away
	// in each direction. Zero selects the 0.25 default; pass a negative value
	// to disable jitter (tests that assert exact skip windows).
	RetryJitter float64

	// QuarantineAfter is the count of consecutive failed cycles after which
	// the daemon stops ACTUATING a provider. Default 12; negative disables
	// quarantine entirely.
	//
	// With exponential skip, 12 consecutive failures is ~255 ticks — roughly
	// two hours of real elapsed time, not six minutes. That threshold means
	// "this is not transient", which is the property that makes quarantine
	// safe: an overnight LM Studio outage backs off to 16-minute retries and
	// self-heals, while a missing actuator script quarantines.
	QuarantineAfter int
}

func (c *ReconcileDaemonConfig) withDefaults() ReconcileDaemonConfig {
	cfg := *c
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.ShutdownGracePeriod <= 0 {
		cfg.ShutdownGracePeriod = 5 * time.Second
	}
	if cfg.RetryMaxSkipTicks <= 0 {
		cfg.RetryMaxSkipTicks = 32
	}
	if cfg.RetryJitter == 0 {
		cfg.RetryJitter = 0.25
	} else if cfg.RetryJitter < 0 {
		cfg.RetryJitter = 0
	}
	if cfg.QuarantineAfter == 0 {
		cfg.QuarantineAfter = 12
	}
	return cfg
}

// quarantineRecord is the state captured when the daemon stops actuating a
// provider. Fingerprint is the provider's config fingerprint at the moment
// quarantine began: when a later cycle sees a DIFFERENT fingerprint, the
// operator has changed the config that was failing, and quarantine lifts
// automatically (see reviewQuarantine).
type quarantineRecord struct {
	Since       time.Time
	Failures    int
	Fingerprint string
}

// ReconcileDaemon is the daemon-resident goroutine that drives the full
// reconcile cycle for all registered Reconcilables on each periodic tick.
//
// Start the daemon with Start(ctx). Stop it by cancelling the context.
// Use Trigger to queue an early (out-of-band) reconcile for a specific provider.
type ReconcileDaemon struct {
	cfg ReconcileDaemonConfig

	mu    sync.Mutex
	state ReconcileDaemonState

	// triggerCh is a non-blocking channel for early trigger requests.
	// Keys are provider type strings. The map is drained at the start of
	// each tick and after each regular tick.
	triggerMu sync.Mutex
	triggered map[string]struct{}
	triggerCh chan struct{} // notifies the loop that triggers are queued

	// health self-reports per-provider reconcile anomalies (slow cycles,
	// persistent degraded health) as WARNs and a queryable snapshot.
	health *convergenceTracker

	// cycleSerials is a monotonic per-provider completion counter, bumped at
	// the END of each provider's runOneCycle (regardless of success/failure —
	// a cycle "completed" whether or not the provider errored). This is
	// telemetry, not kernel state (First Instruments §0/A3): it exists solely
	// so test harnesses can observe "has cycle N happened" without depending
	// on a fake provider's own internal counters (the prior test-local
	// waitForCycle pattern, which only worked because the fake tracked its own
	// fetchCount — a production provider under test has no such hook).
	cycleSerialsMu sync.Mutex
	cycleSerials   map[string]*atomic.Int64

	// lastSummaries caches the most recent reconcile.Summary per provider,
	// set right after each cycle's ComputePlan succeeds (First Instruments
	// B2/M1-B). This is telemetry, not kernel state (§0): the Summary is
	// already computed as part of the ordinary reconcile cycle; caching it
	// costs nothing extra at tick boundaries and adds no new git/network
	// round-trip. Used by LastCoherence to compute the graded reconcile-drift
	// score C_B without re-running ComputePlan out of band.
	lastSummariesMu sync.Mutex
	lastSummaries   map[string]reconcile.Summary

	// lastPhaseErr caches the last-logged error text per (providerType,
	// phase) key, used by warnPhaseFailureThrottled (issue #494, "unrelated
	// observation": a chronically unconfigured provider — e.g. discord with
	// no bot token — fails the identical phase every single tick forever,
	// and logging every one of those at Warn contributed to
	// ~/.cog/var/logs/serve.log growing unbounded). Telemetry, not kernel
	// state (First Instruments §0): purely a log-level decision, never
	// consulted for control flow.
	lastPhaseErrMu sync.Mutex
	lastPhaseErr   map[string]string

	// tickSeq is the daemon's monotonic tick counter, the clock the backoff
	// skip windows are measured in. Atomic because runProviders' concurrent
	// path (MaxConcurrent > 1) reads it from worker goroutines while runTick
	// advances it.
	tickSeq atomic.Int64

	// backoff widens the retry cadence for a provider whose cycles keep
	// failing. Instance-scoped: the autonomic self-heal ticker holds its own,
	// and the two schedulers tick at different intervals over overlapping
	// provider names, so sharing state would corrupt both.
	backoff *failureBackoff

	// quarantined records providers the daemon has stopped ACTUATING.
	// configFingerprints holds each provider's most recent config fingerprint,
	// the signal used to lift quarantine automatically once the operator
	// changes whatever was broken.
	quarantineMu       sync.Mutex
	quarantined        map[string]quarantineRecord
	configFingerprints map[string]string
}

// NewReconcileDaemon creates a ReconcileDaemon with the given config.
// Call Start(ctx) to begin the loop.
func NewReconcileDaemon(cfg ReconcileDaemonConfig) *ReconcileDaemon {
	resolved := cfg.withDefaults()
	return &ReconcileDaemon{
		cfg:                resolved,
		state:              ReconcileDaemonStarting,
		triggered:          make(map[string]struct{}),
		triggerCh:          make(chan struct{}, 1),
		health:             newConvergenceTracker(cfg.Convergence),
		cycleSerials:       make(map[string]*atomic.Int64),
		lastSummaries:      make(map[string]reconcile.Summary),
		lastPhaseErr:       make(map[string]string),
		backoff:            newFailureBackoff(resolved.RetryMaxSkipTicks, resolved.RetryJitter),
		quarantined:        make(map[string]quarantineRecord),
		configFingerprints: make(map[string]string),
	}
}

// warnPhaseFailureThrottled logs a cycle-aborting phase failure ("LoadConfig
// failed", "FetchLive failed", etc.) at Warn the first time this
// (providerType, phase) pair fails, or whenever the error text changes from
// the last time it failed, and at Debug for every subsequent occurrence of
// the byte-identical error. See the doc comment on lastPhaseErr for why this
// exists: a provider that fails the same phase for the same reason on every
// tick (the common shape for "not configured yet" rather than a transient
// fault) previously logged that at Warn forever. A fresh failure, or a
// change in the failure's text, is still new information and still gets a
// Warn — only the noise of repeating the identical message is suppressed.
func (d *ReconcileDaemon) warnPhaseFailureThrottled(providerType, phase string, err error) {
	msg := err.Error()
	key := providerType + "|" + phase

	d.lastPhaseErrMu.Lock()
	prev, seen := d.lastPhaseErr[key]
	changed := !seen || prev != msg
	d.lastPhaseErr[key] = msg
	d.lastPhaseErrMu.Unlock()

	logMsg := "reconcile-daemon: " + phase + " failed"
	if changed {
		slog.Warn(logMsg, "provider", providerType, "err", err)
		return
	}
	slog.Debug(logMsg, "provider", providerType, "err", err)
}

// clearPhaseFailureThrottle forgets any cached failure text for
// (providerType, phase), called right after that phase succeeds. Without
// this, a phase that fails, later succeeds, and then fails again with the
// exact same error text as its earlier failure would be treated as a
// continuation of the old streak (logged at Debug) rather than the fresh
// recurrence it actually is.
func (d *ReconcileDaemon) clearPhaseFailureThrottle(providerType, phase string) {
	key := providerType + "|" + phase
	d.lastPhaseErrMu.Lock()
	delete(d.lastPhaseErr, key)
	d.lastPhaseErrMu.Unlock()
}

// warnActionFailureThrottled is warnPhaseFailureThrottled's counterpart for
// per-action ApplyFailed results (issue #494, cog-review PR #496 second
// pass). It shares the same lastPhaseErr map/mutex but keys on
// (providerType, action, name) instead of (providerType, phase): a single
// provider can return many independent actions per ApplyPlan call, each
// able to fail for its own unrelated reason (e.g. one site CRD with a bad
// strategy while another site CRD deploys fine), so throttling at the
// coarser per-phase granularity would let one action's failure streak mask,
// or be masked by, a genuinely new failure on a different action. The
// logged message and field shape (provider/action/name/err as separate slog
// attributes, message text "reconcile-daemon: action failed") match exactly
// what this call site logged before throttling existed — only the decision
// of Warn-vs-Debug is new.
//
// The "action|" key prefix can never collide with a phase key: phase names
// (LoadConfig, FetchLive, ComputePlan, ...) never contain a literal "|",
// so providerType+"|"+phase and providerType+"|action|"+action+"|"+name
// occupy disjoint regions of the same map by construction.
func (d *ReconcileDaemon) warnActionFailureThrottled(providerType, action, name, errText string) {
	key := providerType + "|action|" + action + "|" + name

	d.lastPhaseErrMu.Lock()
	prev, seen := d.lastPhaseErr[key]
	changed := !seen || prev != errText
	d.lastPhaseErr[key] = errText
	d.lastPhaseErrMu.Unlock()

	args := []any{"provider", providerType, "action", action, "name", name, "err", errText}
	if changed {
		slog.Warn("reconcile-daemon: action failed", args...)
		return
	}
	slog.Debug("reconcile-daemon: action failed", args...)
}

// clearActionFailureThrottle is clearPhaseFailureThrottle's counterpart for
// warnActionFailureThrottled's (providerType, action, name) keys — called
// when that specific action succeeds, so a later recurrence of the same
// failure text after a period of health is treated as fresh, not a
// continuation of an old streak.
func (d *ReconcileDaemon) clearActionFailureThrottle(providerType, action, name string) {
	key := providerType + "|action|" + action + "|" + name
	d.lastPhaseErrMu.Lock()
	delete(d.lastPhaseErr, key)
	d.lastPhaseErrMu.Unlock()
}

// cycleOutcomeChanged reports whether providerType's cycle-outcome fingerprint
// differs from the last recorded one, updating the record.
//
// PR #496 threw the Warn-once/Debug-repeat net over every FAILURE site in
// runOneCycle — phases, per-action results — but the cycle-complete SUMMARY
// line, which escalates from Info to Warn whenever apply_failed > 0, sat
// outside that net. A provider stuck on a permanently-failing action
// therefore re-emitted it at Warn every 30s forever: 1,185 lines in a single
// day for one provider, 90.6% of all daemon WARNs. Empirically the throttle
// on its sibling worked exactly as designed over the same window — the
// detailed "action failed" line for that provider logged once, total — which
// is what makes the summary line's omission visible as an oversight rather
// than a policy.
//
// Fingerprinting on the outcome SHAPE rather than merely on "has failed
// before" keeps the level decision honest: a first failure, a change in the
// failure's shape, and a recovery are all still Warn. Only a byte-identical
// repeat of an already-reported outcome drops to Debug.
//
// Shares lastPhaseErr with a "|cycle|" key suffix. The three key spaces are
// disjoint by construction: phase keys are providerType+"|"+phase and phase
// names contain no "|", action keys are providerType+"|action|"+..., and
// "cycle" is not a phase name.
func (d *ReconcileDaemon) cycleOutcomeChanged(providerType, fingerprint string) bool {
	key := providerType + "|cycle|"
	d.lastPhaseErrMu.Lock()
	prev, seen := d.lastPhaseErr[key]
	changed := !seen || prev != fingerprint
	d.lastPhaseErr[key] = fingerprint
	d.lastPhaseErrMu.Unlock()
	return changed
}

// clearCycleOutcomeThrottle forgets providerType's cached cycle outcome, so a
// recurrence after a period of health is reported as the fresh event it is
// rather than as a continuation of the old streak. Called from runOneCycle's
// outcome defer on EVERY successful exit — including the "provider in sync"
// early return, which is the path a recovered provider actually takes.
func (d *ReconcileDaemon) clearCycleOutcomeThrottle(providerType string) {
	key := providerType + "|cycle|"
	d.lastPhaseErrMu.Lock()
	delete(d.lastPhaseErr, key)
	d.lastPhaseErrMu.Unlock()
}

// forgetProviderThrottles drops every throttle key belonging to providerType,
// called when the provider is no longer in the registry so the map does not
// grow without bound across a long-lived process.
func (d *ReconcileDaemon) forgetProviderThrottles(providerType string) {
	prefix := providerType + "|"
	d.lastPhaseErrMu.Lock()
	for k := range d.lastPhaseErr {
		if strings.HasPrefix(k, prefix) {
			delete(d.lastPhaseErr, k)
		}
	}
	d.lastPhaseErrMu.Unlock()
}

// ─── Retry backoff and quarantine ────────────────────────────────────────────

// configFingerprint reduces a provider's loaded config to a short stable
// string. Used only to notice that the operator CHANGED something, so a cheap
// structural hash is sufficient and a marshal failure is not an error —
// falling back to the Go-syntax rendering still changes when the config does.
func configFingerprint(config any) string {
	if config == nil {
		return "nil"
	}
	data, err := json.Marshal(config)
	if err != nil {
		data = []byte(fmt.Sprintf("%#v", config))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

// noteCycleFailure records a failed cycle: widen the retry window and, past
// the configured threshold, stop actuating the provider entirely.
func (d *ReconcileDaemon) noteCycleFailure(providerType string) {
	tick := int(d.tickSeq.Load())
	fails, skip := d.backoff.RecordFailure(providerType, tick)

	if d.cfg.QuarantineAfter <= 0 || fails < d.cfg.QuarantineAfter {
		return
	}

	d.quarantineMu.Lock()
	if _, already := d.quarantined[providerType]; already {
		d.quarantineMu.Unlock()
		return
	}
	d.quarantined[providerType] = quarantineRecord{
		Since:       time.Now(),
		Failures:    fails,
		Fingerprint: d.configFingerprints[providerType],
	}
	d.quarantineMu.Unlock()

	// Exactly one WARN, carrying the recovery path. From here the standing
	// condition lives in the pull surface (GET /v1/reconcile/convergence) and
	// in the still-open anomaly episode, not in repeated push.
	slog.Warn("reconcile-daemon: provider quarantined, actuation stopped",
		"provider", providerType,
		"consecutive_failures", fails,
		"skip_ticks", skip,
		"recovery", quarantineRecoveryHint(providerType),
	)
}

// quarantineRecoveryHint is the operator-facing string carried on the
// quarantine WARN and the convergence snapshot (ProviderConvergence.Recovery,
// served at GET /v1/reconcile/convergence).
//
// `cogos reconcile <type>` runs in its own independent process — it does its
// own LoadConfig→FetchLive→ComputePlan→ApplyPlan cycle and never touches this
// daemon's quarantine/backoff state, so it fixes drift directly but does NOT
// resume the live daemon. POST /v1/reconcile/{type}/resume
// (serve_reconcile_resume.go) is the real "resume immediately" lever: it
// calls this daemon's own Resume, in-process.
func quarantineRecoveryHint(providerType string) string {
	return "fix the underlying fault; quarantine lifts automatically when the provider's config changes or its next observation is in sync; " +
		"resume this daemon immediately with `POST /v1/reconcile/" + providerType + "/resume` " +
		"(requires enable_reconcile_control: true in kernel.yaml); " +
		"`cogos reconcile " + providerType + "` reconciles drift directly but runs in its own process and does NOT resume this daemon or lift its quarantine; " +
		"a kernel restart clears all quarantines"
}

// noteCycleSuccess clears a provider's failure streak and lifts quarantine.
//
// Reachable for a quarantined provider because quarantine stops ACTUATION,
// not observation: the read-only prefix still runs, so a condition that
// resolves on its own (the model gets loaded by hand, the peer comes back)
// produces an in-sync cycle, which lands here and lifts quarantine without
// operator involvement.
func (d *ReconcileDaemon) noteCycleSuccess(providerType string) {
	if n, recovered := d.backoff.RecordSuccess(providerType); recovered {
		slog.Info("reconcile-daemon: provider recovered, retry cadence restored",
			"provider", providerType, "was_fail_count", n)
	}
	d.liftQuarantine(providerType, "provider reconciled successfully")
}

// liftQuarantine removes providerType from quarantine, logging once if it was
// in fact quarantined.
func (d *ReconcileDaemon) liftQuarantine(providerType, why string) {
	d.quarantineMu.Lock()
	rec, was := d.quarantined[providerType]
	delete(d.quarantined, providerType)
	d.quarantineMu.Unlock()
	if !was {
		return
	}
	slog.Warn("reconcile-daemon: provider quarantine lifted, actuation resumed",
		"provider", providerType,
		"reason", why,
		"quarantined_for", time.Since(rec.Since).Round(time.Second).String(),
	)
}

// reviewQuarantine records providerType's current config fingerprint and lifts
// quarantine if it differs from the fingerprint captured when quarantine began.
//
// This is what makes a terminal state safe rather than a trap. The dominant
// chronic-failure shape in this daemon is "not configured yet" rather than a
// transient fault (see warnPhaseFailureThrottled's note: discord with no bot
// token). Without this, such a provider would quarantine after ~2h, the
// operator would add the missing token, and nothing would ever retry it —
// strictly worse than today, where it simply starts working on the next tick.
// LoadConfig has already run and is read-only, so noticing the change costs
// one hash.
func (d *ReconcileDaemon) reviewQuarantine(providerType, fingerprint string) {
	d.quarantineMu.Lock()
	d.configFingerprints[providerType] = fingerprint
	rec, quarantined := d.quarantined[providerType]
	d.quarantineMu.Unlock()

	if !quarantined || rec.Fingerprint == fingerprint {
		return
	}
	d.liftQuarantine(providerType, "config changed since quarantine")
	d.backoff.RecordSuccess(providerType)
}

// isQuarantined reports whether the daemon has stopped actuating providerType.
func (d *ReconcileDaemon) isQuarantined(providerType string) bool {
	d.quarantineMu.Lock()
	defer d.quarantineMu.Unlock()
	_, ok := d.quarantined[providerType]
	return ok
}

// Resume clears providerType's quarantine and retry backoff and queues an
// immediate cycle. This is the OPERATOR entrance (CLI / HTTP): an explicit
// "try now" that overrides the daemon's own pacing.
//
// Deliberately separate from Trigger. Trigger is wired to fsnotify projection
// watchers (see boot.go), which fire on file events the reconcilers themselves
// can cause; if Trigger reset the backoff, a batch of corpus writes would
// un-quarantine and restore the busy-loop this exists to stop. Machine
// triggers honour backoff; only operator intent overrides it.
func (d *ReconcileDaemon) Resume(providerType string) {
	if !d.hasProvider(providerType) {
		return
	}
	d.liftQuarantine(providerType, "operator resume")
	d.backoff.RecordSuccess(providerType)
	d.Trigger(providerType)
}

// LastCycleSerial returns the current monotonic cycle-completion counter for
// providerType and true if at least one cycle has completed for it. Returns
// (0, false) if no cycle for that provider type has completed yet.
//
// The counter is bumped once at the end of every runOneCycle call for that
// provider type (First Instruments A3) — a real completion signal, not a
// poll of provider-owned test state. Use testkernel.WaitForCycle to block
// until a target serial is reached.
func (d *ReconcileDaemon) LastCycleSerial(providerType string) (int, bool) {
	d.cycleSerialsMu.Lock()
	counter, ok := d.cycleSerials[providerType]
	d.cycleSerialsMu.Unlock()
	if !ok {
		return 0, false
	}
	return int(counter.Load()), true
}

// bumpCycleSerial increments the monotonic completion counter for
// providerType, creating it on first use. Called once at the end of every
// runOneCycle, after the provider's cycle (success or error) has fully
// completed.
func (d *ReconcileDaemon) bumpCycleSerial(providerType string) {
	d.cycleSerialsMu.Lock()
	counter, ok := d.cycleSerials[providerType]
	if !ok {
		counter = &atomic.Int64{}
		d.cycleSerials[providerType] = counter
	}
	d.cycleSerialsMu.Unlock()
	counter.Add(1)
}

// ProviderCoherence is the per-provider reconcile-drift detail behind
// LastCoherence's aggregate C_B (First Instruments M1-B).
type ProviderCoherence struct {
	ProviderType  string  `json:"provider_type"`
	DriftFraction float64 `json:"drift_fraction"`
	HasSummary    bool    `json:"has_summary"`
	Creates       int     `json:"creates"`
	Updates       int     `json:"updates"`
	Deletes       int     `json:"deletes"`
	Skipped       int     `json:"skipped"`
	Total         int     `json:"total"`
}

// LastCoherence computes the M1-B graded reconcile-drift score C_B (IMPL-SPEC
// B2) from the most recently cached per-provider Summary (populated at the
// end of every ComputePlan in runOneCycle; telemetry, not kernel state,
// §0). Side-effect-free: reads the cache only, runs no reconcile cycle.
//
//	drift_fraction_p := 0                              when Total()==0 (empty plan: nothing to
//	                                                     reconcile ⇒ fully in-sync, not maximally drifted)
//	                  := (Creates+Updates+Deletes)/Total()   otherwise  (== 1 - Skipped/Total())
//	C_B = 1 - (1/P) * Σ_p drift_fraction_p
//
// C_B is in [0,1]; 1.0 iff every provider is all-Skipped OR has an empty
// plan (Total()==0). Returns C_B==1.0 and an empty detail slice if no
// provider has completed a cycle yet (vacuously coherent — nothing has been
// observed to have drifted).
//
// Do NOT use Total()/(Skipped+Total()) — Summary.Total() already includes
// Skipped (pkg/substrate/reconcile/types.go Total()), so that form ranges
// [0.5,1], not [0,1] (verified degenerate per IMPL-SPEC B2).
func (d *ReconcileDaemon) LastCoherence() (cB float64, perProvider []ProviderCoherence) {
	d.lastSummariesMu.Lock()
	snapshot := make(map[string]reconcile.Summary, len(d.lastSummaries))
	for pt, s := range d.lastSummaries {
		snapshot[pt] = s
	}
	d.lastSummariesMu.Unlock()

	if len(snapshot) == 0 {
		return 1.0, nil
	}

	// Deterministic provider ordering for a stable detail slice.
	types := make([]string, 0, len(snapshot))
	for pt := range snapshot {
		types = append(types, pt)
	}
	sort.Strings(types)

	perProvider = make([]ProviderCoherence, 0, len(types))
	var driftSum float64
	for _, pt := range types {
		s := snapshot[pt]
		total := s.Total()
		var driftFraction float64
		if total == 0 {
			// Empty-plan boundary fix (IMPL-SPEC B2, blind-review-1): an idle
			// provider with nothing to reconcile is fully in-sync, not
			// maximally drifted. The naive (C+U+D)/Total() would be 0/0.
			driftFraction = 0
		} else {
			driftFraction = float64(s.Creates+s.Updates+s.Deletes) / float64(total)
		}
		driftSum += driftFraction
		perProvider = append(perProvider, ProviderCoherence{
			ProviderType:  pt,
			DriftFraction: driftFraction,
			HasSummary:    true,
			Creates:       s.Creates,
			Updates:       s.Updates,
			Deletes:       s.Deletes,
			Skipped:       s.Skipped,
			Total:         total,
		})
	}

	cB = 1 - (driftSum / float64(len(types)))
	return cB, perProvider
}

// ProviderConvergence returns the current per-provider reconcile anomaly
// snapshot (cost-over-budget / persistent-degraded). It is the queryable surface
// for operators and agents, and a test oracle: assert on observed runtime
// behaviour after running the daemon, not just on code paths.
// It merges the tracker's episode state with the daemon's own quarantine and
// failure-streak state, so one read answers "what is wrong, since when, how
// many distinct times, and is the daemon still trying" without grepping logs.
func (d *ReconcileDaemon) ProviderConvergence() []ProviderConvergence {
	snap := d.health.Snapshot()

	d.quarantineMu.Lock()
	quarantined := make(map[string]quarantineRecord, len(d.quarantined))
	for pt, rec := range d.quarantined {
		quarantined[pt] = rec
	}
	d.quarantineMu.Unlock()

	for i := range snap {
		pt := snap[i].Provider
		snap[i].ConsecutiveFailures = d.backoff.Failures(pt)
		if rec, ok := quarantined[pt]; ok {
			snap[i].Quarantined = true
			snap[i].QuarantinedSince = rec.Since.UTC().Format(time.RFC3339)
			snap[i].Recovery = quarantineRecoveryHint(pt)
		}
	}
	return snap
}

// observeConvergence feeds one completed cycle's cost, a fresh Health() read
// (for the degraded axis) and the provider's quarantine state to the anomaly
// tracker.
//
// Quarantine is fed in as its own anomaly axis on purpose. The condition that
// motivated this work reports Health()==Suspended, not Degraded, so the
// degraded axis never sees it and it contributes nothing to the anomaly
// counter — meaning the ~1,100 daily WARNs this change deletes were the only
// live indication it was broken. Raising quarantine as a counted, clearable
// episode is what turns removing the noise into keeping the signal rather
// than losing it.
//
// quarantined is a PARAMETER rather than a d.isQuarantined() read taken here,
// because on one of the three call sites that read would be taken too early.
// Quarantine is lifted by noteCycleSuccess, which runs from runOneCycle's
// outcome defer — and defers run after the return statement, with that one
// registered first so it runs last. A quarantined provider that self-heals
// therefore reaches the in-sync early return with d.quarantined still
// populated, and a live read there would feed the recovery cycle to the
// tracker as still-quarantined: healthyNow evaluates false, healthyStreak
// resets to 0 instead of advancing, and the episode takes ClearCycles+1
// healthy cycles to close instead of ClearCycles. Passing the value in makes
// each call site state what it means about the cycle it just finished, which
// is knowable at the call site and not always equal to the live map read.
func (d *ReconcileDaemon) observeConvergence(providerType string, provider reconcile.Reconcilable, cycleMs, fetchMs int64, quarantined bool) {
	d.health.Observe(providerType, convObservation{
		CycleMs:     cycleMs,
		FetchMs:     fetchMs,
		Degraded:    provider.Health().Health == reconcile.HealthDegraded,
		Quarantined: quarantined,
	})
}

// State returns the current lifecycle state of the daemon.
func (d *ReconcileDaemon) State() ReconcileDaemonState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

// PollInterval returns the daemon's resolved tick interval (First
// Instruments Module C, M3 — GET /v1/kernel/rates reads this so the live
// rate-ratio table reflects the daemon's ACTUAL PollInterval, not a static
// config guess). cfg is set once at construction via withDefaults(), so
// this is safe to read without the daemon's mu (immutable after
// NewReconcileDaemon).
func (d *ReconcileDaemon) PollInterval() time.Duration {
	return d.cfg.PollInterval
}

// ─── Isolated-registry helpers ───────────────────────────────────────────────
//
// When cfg.Providers is nil these helpers delegate to the global registry,
// preserving all pre-Phase-2 behaviour exactly. When cfg.Providers is set
// (testkernel isolation path) they operate solely on that list.

// hasProvider reports whether providerType is available in the active provider
// source (injected list or global registry).
func (d *ReconcileDaemon) hasProvider(pt string) bool {
	if d.cfg.Providers != nil {
		for _, p := range d.cfg.Providers {
			if p.Type() == pt {
				return true
			}
		}
		return false
	}
	return reconcile.HasProvider(pt)
}

// listProviderTypes returns the ordered type strings for all active providers.
func (d *ReconcileDaemon) listProviderTypes() []string {
	if d.cfg.Providers != nil {
		names := make([]string, len(d.cfg.Providers))
		for i, p := range d.cfg.Providers {
			names[i] = p.Type()
		}
		return names
	}
	return reconcile.ListProviders()
}

// getProvider returns the Reconcilable for the given type from the active
// provider source (injected list or global registry).
func (d *ReconcileDaemon) getProvider(pt string) (reconcile.Reconcilable, error) {
	if d.cfg.Providers != nil {
		for _, p := range d.cfg.Providers {
			if p.Type() == pt {
				return p, nil
			}
		}
		return nil, fmt.Errorf("provider %q not in injected list", pt)
	}
	return reconcile.GetProvider(pt)
}

// Trigger queues an immediate (out-of-band) reconcile for the named provider
// type. Non-blocking: if the provider is already queued, this is a no-op.
// If the provider is not registered (or not in the injected list when one is
// set), the trigger is silently dropped.
//
// This is the integration point for watch-based early-trigger mechanisms
// (e.g., ProjectionWatcher). See ADR-095 §4.
func (d *ReconcileDaemon) Trigger(providerType string) {
	if !d.hasProvider(providerType) {
		return
	}
	d.triggerMu.Lock()
	d.triggered[providerType] = struct{}{}
	d.triggerMu.Unlock()

	// Non-blocking notify: if the channel already has a notification queued,
	// the loop will drain triggers when it wakes. No need to send again.
	select {
	case d.triggerCh <- struct{}{}:
	default:
	}
}

// Start begins the reconcile loop in a background goroutine. The goroutine
// exits when ctx is cancelled. Start is safe to call only once.
func (d *ReconcileDaemon) Start(ctx context.Context) {
	d.mu.Lock()
	d.state = ReconcileDaemonLive
	d.mu.Unlock()

	slog.Info("reconcile-daemon: starting",
		"poll_interval", d.cfg.PollInterval,
		"max_concurrent", d.cfg.MaxConcurrent,
		"workspace", d.cfg.WorkspaceRoot,
	)

	go d.run(ctx)
}

// run is the main goroutine body. It runs until ctx is cancelled.
func (d *ReconcileDaemon) run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	defer func() {
		d.mu.Lock()
		d.state = ReconcileDaemonShutdown
		d.mu.Unlock()
		slog.Info("reconcile-daemon: stopped")
	}()

	for {
		select {
		case <-ctx.Done():
			// Drain any queued triggers with the grace period.
			d.drainTriggersOnShutdown(ctx)
			return

		case <-d.triggerCh:
			// Early trigger from a watcher or external caller.
			d.runTriggered(ctx)

		case <-ticker.C:
			// Periodic tick: reconcile all registered providers.
			d.runTick(ctx)
		}
	}
}

// runTick iterates all providers and runs their reconcile cycle.
// When cfg.Providers is set, iterates the injected list; otherwise uses the
// global registry. Also drains any pending triggers before iterating (so a
// trigger that fired between ticks is absorbed into this tick rather than
// generating a double run).
func (d *ReconcileDaemon) runTick(ctx context.Context) {
	// Absorb any outstanding triggers into this tick (they'll be covered by
	// the full provider scan).
	d.triggerMu.Lock()
	d.triggered = make(map[string]struct{})
	d.triggerMu.Unlock()

	providers := d.listProviderTypes()
	if len(providers) == 0 {
		return
	}

	// Advance the backoff clock and drop providers still inside a skip window.
	// A provider that has never failed is always ready, so the healthy path is
	// untouched.
	tick := int(d.tickSeq.Add(1))
	eligible := make([]string, 0, len(providers))
	for _, pt := range providers {
		if !d.backoff.Ready(pt, tick) {
			continue
		}
		eligible = append(eligible, pt)
	}

	slog.Debug("reconcile-daemon: tick",
		"provider_count", len(providers),
		"eligible_count", len(eligible),
	)
	if len(eligible) == 0 {
		return
	}

	errCount := d.runProviders(ctx, eligible)

	d.mu.Lock()
	if errCount > 0 {
		d.state = ReconcileDaemonStalled
	} else {
		d.state = ReconcileDaemonLive
	}
	d.mu.Unlock()
}

// runTriggered drains the pending trigger set and runs cycles for only the
// queued providers. Used for early (out-of-band) reconcile requests.
func (d *ReconcileDaemon) runTriggered(ctx context.Context) {
	d.triggerMu.Lock()
	queued := d.triggered
	d.triggered = make(map[string]struct{})
	d.triggerMu.Unlock()

	if len(queued) == 0 {
		return
	}

	// Machine triggers honour backoff. These come from fsnotify projection
	// watchers (boot.go), which fire on file events that the projection
	// reconcilers themselves can produce; letting them bypass the skip window
	// would hand every watcher a backoff-defeat lever and restore the
	// busy-loop. Operator intent goes through Resume, which clears the window
	// first.
	tick := int(d.tickSeq.Load())
	types := make([]string, 0, len(queued))
	for t := range queued {
		if !d.backoff.Ready(t, tick) {
			continue
		}
		types = append(types, t)
	}
	if len(types) == 0 {
		return
	}
	slog.Debug("reconcile-daemon: triggered", "providers", types)
	d.runProviders(ctx, types)
}

// drainTriggersOnShutdown runs any pending triggers within the shutdown grace period.
func (d *ReconcileDaemon) drainTriggersOnShutdown(ctx context.Context) {
	d.triggerMu.Lock()
	queued := d.triggered
	d.triggered = make(map[string]struct{})
	d.triggerMu.Unlock()

	if len(queued) == 0 {
		return
	}

	graceCtx, cancel := context.WithTimeout(context.Background(), d.cfg.ShutdownGracePeriod)
	defer cancel()

	types := make([]string, 0, len(queued))
	for t := range queued {
		types = append(types, t)
	}
	slog.Info("reconcile-daemon: draining triggers on shutdown", "providers", types)
	d.runProviders(graceCtx, types)
}

// runProviders runs the reconcile cycle for each named provider type.
// Providers are run with MaxConcurrent parallelism (default 1 = serial).
// Per-provider error isolation: panics and errors are recovered and logged;
// other providers in the same batch continue unaffected.
// Returns the number of providers that errored.
func (d *ReconcileDaemon) runProviders(ctx context.Context, providerTypes []string) int {
	if d.cfg.MaxConcurrent <= 1 {
		// Serial path: simple loop, no goroutines needed.
		errCount := 0
		for _, pt := range providerTypes {
			if err := d.runOneCycle(ctx, pt); err != nil {
				errCount++
			}
			// Bail early if context is done.
			if ctx.Err() != nil {
				break
			}
		}
		return errCount
	}

	// Concurrent path: semaphore-bounded goroutines.
	sem := make(chan struct{}, d.cfg.MaxConcurrent)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCount := 0

	for _, pt := range providerTypes {
		if ctx.Err() != nil {
			break
		}
		pt := pt
		sem <- struct{}{} // acquire
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }() // release
			if err := d.runOneCycle(ctx, pt); err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errCount
}

// runOneCycle runs the full reconcile cycle for a single provider type.
// The cycle is:
//
//	LoadConfig → FetchLive → ComputePlan → ApplyPlan → BuildState → WriteState
//
// Panics are recovered and returned as errors to preserve error isolation.
// Conforms to ADR-092 §4 Reconcilable contract order.
func (d *ReconcileDaemon) runOneCycle(ctx context.Context, providerType string) (retErr error) {
	// Retry-cadence accounting. Registered FIRST so it runs LAST (defers are
	// LIFO), i.e. after the panic-recover defer below has settled retErr — a
	// panicking provider must count as a failure, not a success.
	//
	// recordOutcome is cleared on the one path that is neither: a quarantined
	// provider's observe-only cycle, where the daemon deliberately did not
	// attempt the work and so has learned nothing about whether it would
	// succeed.
	recordOutcome := true
	defer func() {
		if !recordOutcome {
			return
		}
		if retErr != nil {
			d.noteCycleFailure(providerType)
			return
		}
		d.noteCycleSuccess(providerType)
		d.clearCycleOutcomeThrottle(providerType)
	}()

	// Recover from panics so one misbehaving provider can't take down the loop.
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic in provider %s: %v", providerType, r)
			slog.Error("reconcile-daemon: provider panicked",
				"provider", providerType,
				"panic", r,
			)
		}
	}()

	// Bump the monotonic cycle-completion counter at the END of this cycle,
	// regardless of outcome (success, error, or recovered panic above — this
	// defer runs after the panic-recover defer settles retErr, since defers
	// execute LIFO and this one is registered second). Telemetry, not kernel
	// state (First Instruments A3/§0): a test-observable "cycle N happened"
	// signal, no effect on control flow.
	defer d.bumpCycleSerial(providerType)

	tracer := otel.Tracer("cogos.reconcile-daemon")
	spanCtx, span := tracer.Start(ctx, "reconcile.daemon.cycle")
	span.SetAttributes(attribute.String("provider.type", providerType))
	defer span.End()

	start := time.Now()

	provider, err := d.getProvider(providerType)
	if err != nil {
		slog.Warn("reconcile-daemon: provider not found", "provider", providerType, "err", err)
		// The provider has left the registry: drop its throttle and backoff
		// keys rather than leaving them to accumulate for the process
		// lifetime, and do not count this against a retry streak it can no
		// longer work off.
		recordOutcome = false
		d.forgetProviderThrottles(providerType)
		d.backoff.Forget(providerType)
		d.liftQuarantine(providerType, "provider left the registry")
		return err
	}

	// Heap watermark guard (#716): a memory-heavy provider is skipped while the
	// live heap is above the watermark. A skip is neither success nor failure,
	// so recordOutcome is cleared. The bumpCycleSerial defer still bumps on a
	// skip; that is acceptable because the serial is telemetry, not control flow.
	if heapGuardSkip(provider, providerType) {
		recordOutcome = false
		return nil
	}

	// Per-phase timing. Each reconcile step is timed so a slow phase — e.g. an
	// O(corpus) FetchLive index reload, or a large WriteState — is visible in
	// the cycle-complete log and on the span, instead of only the opaque cycle
	// total. This makes "which phase is hot?" answerable from telemetry alone,
	// without attaching a profiler.
	var loadMs, fetchMs, stateMs, planMs, applyMs, writeMs int64

	// Step 1: LoadConfig — read-only disk operation.
	loadStart := time.Now()
	config, err := provider.LoadConfig(d.cfg.WorkspaceRoot)
	loadMs = time.Since(loadStart).Milliseconds()
	if err != nil {
		d.warnPhaseFailureThrottled(providerType, "LoadConfig", err)
		return fmt.Errorf("LoadConfig %s: %w", providerType, err)
	}
	d.clearPhaseFailureThrottle(providerType, "LoadConfig")

	// Fingerprint the freshly-loaded config and lift quarantine if the
	// operator has changed it since the provider was quarantined. Done here,
	// immediately after LoadConfig, so it also covers providers that fail in a
	// LATER phase (FetchLive against an unreachable peer) — those never reach
	// the actuation gate below, but their config can still be fixed.
	d.reviewQuarantine(providerType, configFingerprint(config))

	// Step 2: FetchLive — read-only observation of world state.
	fetchStart := time.Now()
	live, err := provider.FetchLive(spanCtx, config)
	fetchMs = time.Since(fetchStart).Milliseconds()
	if err != nil {
		// Issue #494 (unrelated observation): throttled rather than a plain
		// slog.Warn — a chronically unconfigured provider (e.g. discord with
		// no bot token) previously logged this exact failure at Warn on
		// every tick forever. See warnPhaseFailureThrottled's doc comment.
		d.warnPhaseFailureThrottled(providerType, "FetchLive", err)
		return fmt.Errorf("FetchLive %s: %w", providerType, err)
	}
	d.clearPhaseFailureThrottle(providerType, "FetchLive")

	// Acquire the cross-process state lock for the full
	// LoadState → ComputePlan → ApplyPlan → BuildState → WriteState cycle
	// (steps 3-7 below) so this daemon cycle can't race a CLI-invoked
	// `cogos reconcile <type>` run against the same providerType (same bug
	// class as issue #449's _meta.json race; see
	// pkg/substrate/reconcile/state.go doc comment). Released via defer so
	// every early-return path below (ComputePlan/ApplyPlan/BuildState
	// failures) still releases it. A lock-acquire failure (peer holds it
	// past StateLockTimeout) is treated like any other phase failure: warn
	// and skip this cycle, retried on the next tick.
	lock, lockErr := reconcile.AcquireStateLock(d.cfg.WorkspaceRoot, providerType)
	if lockErr != nil {
		d.warnPhaseFailureThrottled(providerType, "acquire state lock", lockErr)
		return fmt.Errorf("acquire state lock %s: %w", providerType, lockErr)
	}
	d.clearPhaseFailureThrottle(providerType, "acquire state lock")
	defer lock.Release()

	// Step 3: Load persisted state.
	stateStart := time.Now()
	state, stateErr := reconcile.LoadState(d.cfg.WorkspaceRoot, providerType)
	if stateErr != nil {
		// LoadState maps a missing file to (nil, nil); a non-nil error here is
		// corruption or a permission/read fault. Surface it instead of silently
		// resetting lineage serials, mirroring the WriteState warning below.
		// Continue with the nil state — providers already handle a nil state.
		//
		// Throttled like every other phase failure in this function
		// (cog-review, PR #496 first pass): a persistently corrupted or
		// unreadable state file fails with the same text on every tick
		// forever otherwise, reproducing this PR's own log-spam bug class.
		d.warnPhaseFailureThrottled(providerType, "LoadState", stateErr)
	} else {
		d.clearPhaseFailureThrottle(providerType, "LoadState")
	}
	stateMs = time.Since(stateStart).Milliseconds()

	// Step 4: ComputePlan — pure function, deterministic.
	planStart := time.Now()
	plan, err := provider.ComputePlan(config, live, state)
	planMs = time.Since(planStart).Milliseconds()
	if err != nil {
		d.warnPhaseFailureThrottled(providerType, "ComputePlan", err)
		return fmt.Errorf("ComputePlan %s: %w", providerType, err)
	}
	d.clearPhaseFailureThrottle(providerType, "ComputePlan")

	// Cache this cycle's Summary for LastCoherence (First Instruments B2/
	// M1-B). Telemetry, not kernel state (§0) — already computed above, so
	// this adds no marginal cost. Cached unconditionally (both the
	// early "in sync" exit and the full apply path below read the same
	// already-computed plan.Summary).
	d.lastSummariesMu.Lock()
	d.lastSummaries[providerType] = plan.Summary
	d.lastSummariesMu.Unlock()

	// phaseAttrs records the per-phase timings on the cycle span. Called at both
	// exits (early "in sync" return and full cycle) so traces always carry the
	// breakdown.
	phaseAttrs := func() []attribute.KeyValue {
		return []attribute.KeyValue{
			attribute.Int64("phase.load_config_ms", loadMs),
			attribute.Int64("phase.fetch_live_ms", fetchMs),
			attribute.Int64("phase.load_state_ms", stateMs),
			attribute.Int64("phase.compute_plan_ms", planMs),
			attribute.Int64("phase.apply_plan_ms", applyMs),
			attribute.Int64("phase.write_state_ms", writeMs),
		}
	}

	span.SetAttributes(
		attribute.Int("plan.creates", plan.Summary.Creates),
		attribute.Int("plan.updates", plan.Summary.Updates),
		attribute.Int("plan.deletes", plan.Summary.Deletes),
		attribute.Int("plan.skipped", plan.Summary.Skipped),
	)

	if !plan.Summary.HasChanges() {
		// No drift — log at debug level and exit early; no write needed.
		dur := time.Since(start)
		span.SetAttributes(attribute.Int64("cycle.duration_ms", dur.Milliseconds()))
		span.SetAttributes(phaseAttrs()...)
		slog.Debug("reconcile-daemon: provider in sync",
			"provider", providerType,
			"skipped", plan.Summary.Skipped,
			"fetch_ms", fetchMs,
			"plan_ms", planMs,
			"duration_ms", dur.Milliseconds(),
		)
		// Observed as NOT quarantined, deliberately. This return is the
		// success path: retErr is nil and recordOutcome is still true, so the
		// outcome defer runs noteCycleSuccess → liftQuarantine on the way out.
		// Reading d.isQuarantined() here instead would read the map before
		// that lift and hand the tracker a still-quarantined recovery cycle,
		// costing a self-healed provider one extra healthy cycle before its
		// episode could close. Nothing between here and the defer can
		// re-quarantine: quarantine is only entered from noteCycleFailure,
		// which this path does not reach.
		d.observeConvergence(providerType, provider, dur.Milliseconds(), fetchMs, false)
		return nil
	}

	// Actuation gate. A quarantined provider still runs everything above this
	// line — LoadConfig, FetchLive, ComputePlan, and the convergence
	// observation below — and stops only at ApplyPlan.
	//
	// "Stop actuating" rather than "stop looking" is what keeps the terminal
	// state honest. Skipping the whole cycle would freeze the anomaly tracker
	// for that provider, so the episode could never clear and the counter
	// would pin at 1 reporting a condition that may have resolved hours ago —
	// an always-on stale alarm, which is the same disease as the always-firing
	// one this change exists to cure. Keeping the read-only prefix alive
	// preserves drift detection, health observation, and episode-clearing by
	// construction.
	if d.isQuarantined(providerType) {
		dur := time.Since(start)
		span.SetAttributes(attribute.Int64("cycle.duration_ms", dur.Milliseconds()))
		span.SetAttributes(phaseAttrs()...)
		span.SetAttributes(attribute.Bool("cycle.quarantined", true))

		// Neither success nor failure: the work was not attempted. Hold the
		// retry window at full depth so observation continues at the widened
		// cadence instead of every tick.
		recordOutcome = false
		d.backoff.Hold(providerType, int(d.tickSeq.Load()))
		// Quarantined by construction — this branch is the isQuarantined()
		// gate itself, and recordOutcome=false means nothing lifts it on the
		// way out.
		d.observeConvergence(providerType, provider, dur.Milliseconds(), fetchMs, true)

		slog.Debug("reconcile-daemon: drift observed but provider quarantined",
			"provider", providerType,
			"creates", plan.Summary.Creates,
			"updates", plan.Summary.Updates,
			"deletes", plan.Summary.Deletes,
			"duration_ms", dur.Milliseconds(),
		)
		return nil
	}

	// Step 5: ApplyPlan — idempotent per ADR-092 §3.
	applyStart := time.Now()
	results, err := provider.ApplyPlan(spanCtx, plan)
	applyMs = time.Since(applyStart).Milliseconds()
	if err != nil {
		d.warnPhaseFailureThrottled(providerType, "ApplyPlan", err)
		return fmt.Errorf("ApplyPlan %s: %w", providerType, err)
	}
	d.clearPhaseFailureThrottle(providerType, "ApplyPlan")

	// Count apply failures. Per-action logging is throttled like every
	// phase-level failure above (issue #494, cog-review PR #496 second
	// pass): a single persistently-failing action (e.g. a site CRD with an
	// invalid strategy — ApplyPlan returns one ApplyFailed result with no
	// top-level error, so this loop is the ONLY place that error is ever
	// logged) would otherwise repeat the identical line every tick forever.
	// warnActionFailureThrottled keys on (providerType, action, name) —
	// finer than the phase-level helper's (providerType, phase) — so one
	// action's failure streak never suppresses, or is suppressed by, a
	// different action's genuinely new failure on the same provider. A
	// succeeded result clears that action's streak so a later recurrence
	// after recovery is treated as fresh.
	applyFailed := 0
	for _, r := range results {
		switch r.Status {
		case reconcile.ApplyFailed:
			applyFailed++
			d.warnActionFailureThrottled(providerType, r.Action, r.Name, r.Error)
		case reconcile.ApplySucceeded:
			d.clearActionFailureThrottle(providerType, r.Action, r.Name)
		}
	}

	// Steps 6-7: BuildState (pure) + WriteState (atomic tmp+rename), timed
	// together as the persist phase.
	writeStart := time.Now()
	// Both BuildState and WriteState failures are throttled the same way as
	// every other phase in this function (cog-review, PR #496 first pass:
	// these two were the remaining unthrottled sibling sites — a workspace
	// that loses write access to its state directory, or a provider whose
	// BuildState step is persistently broken, otherwise fails identically
	// on every tick forever, reproducing this PR's own log-spam bug class).
	newState, buildErr := provider.BuildState(config, live, state)
	if buildErr != nil {
		d.warnPhaseFailureThrottled(providerType, "BuildState", buildErr)
	} else {
		d.clearPhaseFailureThrottle(providerType, "BuildState")
		if newState != nil {
			// Step 7: WriteState — atomic tmp+rename.
			if writeErr := reconcile.WriteState(d.cfg.WorkspaceRoot, providerType, newState); writeErr != nil {
				d.warnPhaseFailureThrottled(providerType, "WriteState", writeErr)
			} else {
				d.clearPhaseFailureThrottle(providerType, "WriteState")
			}
		}
	}
	writeMs = time.Since(writeStart).Milliseconds()

	dur := time.Since(start)
	span.SetAttributes(attribute.Int64("cycle.duration_ms", dur.Milliseconds()))
	span.SetAttributes(phaseAttrs()...)

	// Level decision for the cycle summary. Warn on a new or changed failure
	// outcome, Debug on a byte-identical repeat of one already reported. The
	// message and fields below are unchanged — only which level they go out
	// at. See cycleOutcomeChanged.
	logLevel := slog.LevelInfo
	if applyFailed > 0 {
		fingerprint := fmt.Sprintf("c=%d,u=%d,d=%d,s=%d,f=%d",
			plan.Summary.Creates, plan.Summary.Updates, plan.Summary.Deletes,
			plan.Summary.Skipped, applyFailed)
		if d.cycleOutcomeChanged(providerType, fingerprint) {
			logLevel = slog.LevelWarn
		} else {
			logLevel = slog.LevelDebug
		}
	}
	slog.Log(ctx, logLevel, "reconcile-daemon: cycle complete",
		"provider", providerType,
		"creates", plan.Summary.Creates,
		"updates", plan.Summary.Updates,
		"deletes", plan.Summary.Deletes,
		"skipped", plan.Summary.Skipped,
		"apply_failed", applyFailed,
		"load_ms", loadMs,
		"fetch_ms", fetchMs,
		"state_ms", stateMs,
		"plan_ms", planMs,
		"apply_ms", applyMs,
		"write_ms", writeMs,
		"duration_ms", dur.Milliseconds(),
	)

	// The full-apply path only runs when the actuation gate above found the
	// provider un-quarantined, and quarantine is only entered from the outcome
	// defer of this same cycle. A live read is therefore equivalent to false
	// here; it is kept so the behaviour of this path is byte-for-byte what it
	// was before the parameter was introduced.
	d.observeConvergence(providerType, provider, dur.Milliseconds(), fetchMs, d.isQuarantined(providerType))

	if applyFailed > 0 {
		return fmt.Errorf("provider %s: %d action(s) failed during apply", providerType, applyFailed)
	}
	return nil
}
