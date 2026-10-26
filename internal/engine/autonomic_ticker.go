// autonomic_ticker.go — deterministic control-loop iteration for the kernel.
//
// The autonomic ticker is the "homeostasis is default; consciousness is the
// interrupt" implementation. Each tick:
//
//  1. Probes all registered Reconcilables via Health() — synchronous,
//     near-zero cost by Reconcilable contract.
//  2. Aggregates into a KernelHealthSnapshot.
//  3. Emits the snapshot to the bus_kernel_proprio channel.
//  4. Evaluates the escalation predicate:
//     - Any provider degraded / out-of-sync / operation-in-progress → escalate.
//     - Explicit trigger queued (TriggerAgent) → escalate.
//     - Idle re-checkin window elapsed → escalate.
//     - Otherwise → tick ends; no LLM call.
//
// Only the escalation path calls assessCycle / executeCycleTask.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// healBackoff tracks consecutive self-heal failures per provider so that a
// persistently-degraded provider (e.g. one whose config file has a parse
// error that survives across reconcile cycles) does not busyloop the CPU.
//
// Policy: after the Nth consecutive failure the provider is skipped for
// 2^(N-1) ticks, capped at healBackoffMaxSkip. A single WARN is emitted on
// the first skip; subsequent skips at the same backoff depth are silent to
// avoid log spam. Recovery (a successful reconcile) resets the counter.
//
// The skip-window arithmetic now lives in failureBackoff (failure_backoff.go),
// which preserves this curve exactly and adds jitter. Migrated together with
// ReconcileDaemon's adoption of the same type so the RFC-041 §T8 census of
// hand-rolled backoffs stays at four rather than growing to five.
var (
	healBackoffMu sync.Mutex
	healTickCount int // monotonic tick counter
	healBackoff   = newFailureBackoff(healBackoffMaxSkip, healBackoffJitter)
)

const (
	healBackoffMaxSkip = 64   // cap at ~64 ticks (~64 min at default interval)
	healBackoffWarnAt  = 3    // emit WARN once when consecutive failures reach this
	healBackoffJitter  = 0.25 // decorrelate providers that fail on the same tick
)

// --- Constants ---------------------------------------------------------------

// autonomicBusChannel is the bus channel where the kernel emits
// KernelHealthSnapshot events every tick. Mirrors the bus_tournament naming
// convention from internal/eval.
const autonomicBusChannel = "bus_kernel_proprio"

// autonomicEventFrom is the sender identity used in bus events emitted by the
// autonomic ticker.
const autonomicEventFrom = "kernel-autonomic"

// autonomicEventType is the event type written to the bus.
const autonomicEventType = "kernel.health.snapshot.v1"

// defaultIdleRecheckIn is how long the ticker waits before forcing an LLM
// escalation even when all providers are green. This ensures the maintenance
// agent checks in periodically rather than being silent indefinitely.
const defaultIdleRecheckIn = 1 * time.Hour

// --- Types -------------------------------------------------------------------

// KernelHealthSnapshot is the aggregate view produced each tick.
type KernelHealthSnapshot struct {
	Timestamp time.Time                           `json:"timestamp"`
	Providers map[string]reconcile.ResourceStatus `json:"providers"`
	Counts    HealthCounts                        `json:"counts"`
	// Anomalies is the count of abandoned/canceled internal inference calls
	// (#432) observed since the previous tick. Distinct from the per-provider
	// Health buckets below: a canceled inference is a control-loop event, not
	// a provider health state, but the incident this closes ran with vitals
	// reading 0anom for hours because nothing folded the WARN-logged
	// cancellations into a counted, tick-visible signal. Zero on a tick where
	// nothing was abandoned.
	Anomalies int `json:"anomalies"`
	// AnomaliesTotal is the cumulative count since process start, carried for
	// post-hoc audit alongside the per-tick delta above.
	AnomaliesTotal int64 `json:"anomalies_total"`
	// HostVitals carries RFC-040 S0 host gauges (disk/mem/load/uptime),
	// sampled in this same tick — no new loop, no new daemon (see
	// host_vitals.go). Gauges are OBSERVATIONS, not health: nothing in this
	// block feeds AllGreen() or any escalation decision (RFC-040 N5).
	// Threshold crossings, if ever wanted, route through the existing
	// anomaly machinery, exactly as the abandoned-inference count above
	// does — this field itself carries only the raw current reading.
	HostVitals HostVitals `json:"host_vitals"`
}

// HealthCounts is the four-bucket summary.
type HealthCounts struct {
	Healthy   int `json:"healthy"`
	Degraded  int `json:"degraded"`
	Missing   int `json:"missing"`
	Suspended int `json:"suspended"`
}

// AllGreen reports whether every provider is Synced/Healthy/(Idle or empty)
// AND no inference was abandoned/canceled this tick. A snapshot with zero
// providers is considered green on the provider axis — the ticker should
// only escalate when something is observably wrong, not when the registry
// is empty. Anomalies still gate green even with zero providers: #432's
// incident is exactly "provider health looked fine, requests were dying
// unnoticed."
func (s KernelHealthSnapshot) AllGreen() bool {
	return s.Counts.Degraded == 0 && s.Counts.Missing == 0 && s.Counts.Suspended == 0 && s.Anomalies == 0
}

// HasActionableDegradation reports whether any provider is in a health state
// the control loop can actually act on — Degraded or Missing. This
// deliberately mirrors healDegradedProviders' needsHeal exemption (see
// healDegradedProviders below): Suspended providers are excluded because
// self-heal treats them as intentionally paused or already converging, not
// as something an LLM cycle can fix by looking at them.
//
// This exists separately from AllGreen() because the two answer different
// questions. AllGreen() is a liveness-report predicate — "is everything
// nominal" — and legitimately treats any Suspended provider as not-green.
// shouldEscalate's degraded_health branch was using AllGreen() to answer an
// actionability question ("should the control loop wake an LLM"), which is
// wrong: a permanently-parked opt-in provider (no config, no declared
// dependency) is Suspended forever by design, and !AllGreen() fired an LLM
// escalation on every tick for as long as it stayed that way. Measured over
// a 43-day kernel log: 5,515 of 6,221 escalations (88.6%) were
// degraded_health, nearly all attributable to three permanently-Suspended
// providers (margin-bridge with no config yaml, mcp-tools (since
// removed) with its bridge URL unset, mlx-inference with nothing declared)
// that self-heal was already correctly ignoring. Use HasActionableDegradation() for
// escalation decisions; keep AllGreen() for reporting.
func (s KernelHealthSnapshot) HasActionableDegradation() bool {
	return s.Counts.Degraded > 0 || s.Counts.Missing > 0
}

// HasOperationInProgress reports whether any provider is currently running an
// apply operation (Syncing / Waiting). Operations in progress don't
// necessarily warrant LLM attention — the system is already converging. We
// escalate only on health degradation. This is exposed for test introspection.
func (s KernelHealthSnapshot) HasOperationInProgress() bool {
	for _, st := range s.Providers {
		if st.Operation == reconcile.OperationSyncing ||
			st.Operation == reconcile.OperationWaiting {
			return true
		}
	}
	return false
}

// HasOutOfSync reports whether any provider's Sync axis is not Synced.
func (s KernelHealthSnapshot) HasOutOfSync() bool {
	for _, st := range s.Providers {
		if st.Sync != reconcile.SyncStatusSynced && st.Sync != "" {
			return true
		}
	}
	return false
}

// --- AutonomicConfig ---------------------------------------------------------

// AutonomicConfig holds tunables for the autonomic ticker. Zero values are
// filled with defaults at runtime. Embedding in Config is deferred for v1;
// the controller reads from this struct directly.
type AutonomicConfig struct {
	// IdleRecheckIn is the maximum time the ticker will go without escalating
	// to an LLM cycle, even when all providers are green. Default 1h.
	IdleRecheckIn time.Duration
}

func (a AutonomicConfig) idleRecheckIn() time.Duration {
	if a.IdleRecheckIn > 0 {
		return a.IdleRecheckIn
	}
	return defaultIdleRecheckIn
}

// --- Snapshot builder --------------------------------------------------------

// buildKernelHealthSnapshot probes all registered Reconcilables and returns an
// aggregate snapshot. Uses the same probeAllProviders machinery as
// buildHealthBlock (context_blocks_health.go) so the two surfaces stay
// consistent.
//
// This is the CONSUMING form: it reads the abandoned-inference watermark via
// abandonedInferenceSnapshot, which swaps-and-resets the delta. The autonomic
// ticker (autonomicTick, below) is the sole production caller of this form —
// it relies on "delta since my last tick" to drive #432 escalation
// (shouldEscalate / escalateAbandonedInference). Any other production caller
// that reads kernel health for informational/display purposes MUST use
// buildKernelHealthSnapshotPeek instead, or it will silently steal the
// ticker's delta. See inference_inflight.go's abandonedInferencePeek doc.
func buildKernelHealthSnapshot(ctx context.Context) KernelHealthSnapshot {
	return buildKernelHealthSnapshotWith(ctx, abandonedInferenceSnapshot)
}

// buildKernelHealthSnapshotPeek is identical to buildKernelHealthSnapshot
// except it reads the abandoned-inference counter via the non-consuming
// abandonedInferencePeek, leaving the watermark the autonomic ticker depends
// on untouched. Use this from any concurrent, informational production
// caller (e.g. the ambient-state-of-self block for looped kernel-interior
// dispatch) so the ticker remains the only consumer of the delta.
func buildKernelHealthSnapshotPeek(ctx context.Context) KernelHealthSnapshot {
	return buildKernelHealthSnapshotWith(ctx, abandonedInferencePeek)
}

// buildKernelHealthSnapshotWith is the shared implementation behind both
// forms above; readAbandoned selects consuming vs. non-consuming semantics
// for the #432 abandoned-inference counter.
func buildKernelHealthSnapshotWith(ctx context.Context, readAbandoned func() (total int64, delta int64)) KernelHealthSnapshot {
	snap := KernelHealthSnapshot{
		Timestamp: time.Now().UTC(),
		Providers: make(map[string]reconcile.ResourceStatus),
	}

	// RFC-040 S0: sample host gauges in this same tick. sampleHostVitals is
	// best-effort per gauge (see host_vitals.go) and never blocks or panics,
	// so this cannot turn a cheap tick into a slow one nor fail the snapshot.
	snap.HostVitals = sampleHostVitals()

	// Fold abandoned/canceled inference (#432) into every snapshot,
	// regardless of provider registry state — this is a control-loop signal,
	// not a per-provider one, and must not be skipped by the empty-registry
	// early return below.
	total, delta := readAbandoned()
	snap.Anomalies = int(delta)
	snap.AnomaliesTotal = total

	names := reconcile.ListProviders()
	if len(names) == 0 {
		return snap
	}

	samples := probeAllProviders(ctx, names)
	for _, s := range samples {
		snap.Providers[s.Name] = s.Status
		switch {
		case s.Status.Health == reconcile.HealthDegraded:
			snap.Counts.Degraded++
		case s.Status.Health == reconcile.HealthMissing:
			snap.Counts.Missing++
		case s.Status.Health == reconcile.HealthSuspended:
			snap.Counts.Suspended++
		default:
			// Healthy or Progressing — count as healthy for escalation purposes.
			snap.Counts.Healthy++
		}
	}
	return snap
}

// --- Escalation predicate ----------------------------------------------------

// escalationReason describes why a tick escalated to an LLM call.
type escalationReason string

const (
	escalateDegradedHealth     escalationReason = "degraded_health"
	escalateAbandonedInference escalationReason = "abandoned_inference"
	escalateOutOfSync          escalationReason = "out_of_sync"
	escalateExplicitTrigger    escalationReason = "explicit_trigger"
	escalateIdleRecheckIn      escalationReason = "idle_recheckin"
)

// shouldEscalate returns a non-empty reason if the tick should route to the
// LLM assess/execute path, or "" if the tick should end after deterministic
// work. triggerPending is true when an external TriggerAgent call has been
// queued; lastLLMCycle is the wall-clock time the last LLM cycle ran.
func shouldEscalate(snap KernelHealthSnapshot, triggerPending bool, lastLLMCycle time.Time, cfg AutonomicConfig) escalationReason {
	// Abandoned/canceled inference (#432) is reported distinctly from
	// provider-health degradation even though both fail AllGreen() — it's a
	// control-loop event (a request the kernel gave up on), not a provider
	// reporting itself unhealthy, and the two want different operator/agent
	// framing in logs and dedupe fingerprints.
	if snap.Anomalies > 0 {
		return escalateAbandonedInference
	}
	// Health degradation is the next-highest-priority signal. Use
	// HasActionableDegradation rather than !AllGreen(): Suspended providers
	// are excluded to match healDegradedProviders' needsHeal exemption — a
	// provider that's intentionally paused (no config, opt-in dependency
	// unset) is not something an LLM cycle can act on, and !AllGreen() alone
	// was waking the LLM every tick for permanently-Suspended providers. See
	// HasActionableDegradation's doc comment for the measured impact.
	if snap.HasActionableDegradation() {
		return escalateDegradedHealth
	}
	// OutOfSync on any provider (without health degradation) still warrants
	// attention — the declared state and live state have diverged.
	if snap.HasOutOfSync() {
		return escalateOutOfSync
	}
	// Explicit trigger from TriggerAgent — bypass idle window.
	if triggerPending {
		return escalateExplicitTrigger
	}
	// Idle re-checkin: force at least one LLM cycle per hour so the agent
	// doesn't become completely silent on healthy workspaces.
	window := cfg.idleRecheckIn()
	if lastLLMCycle.IsZero() || time.Since(lastLLMCycle) >= window {
		return escalateIdleRecheckIn
	}
	return ""
}

// --- Bus emission ------------------------------------------------------------

// emitHealthSnapshot writes the snapshot to the bus_kernel_proprio channel.
// If the bus manager is nil (common in tests or stripped-down boots) the call
// is a no-op. Errors are logged but never returned — failing to emit
// telemetry should not affect the control loop.
func emitHealthSnapshot(ctx context.Context, mgr *BusSessionManager, snap KernelHealthSnapshot) {
	if mgr == nil {
		return
	}

	payload, err := snapshotToPayload(snap)
	if err != nil {
		slog.Warn("autonomic: failed to marshal health snapshot for bus emit", "err", err)
		return
	}

	if err := mgr.EnsureBus(autonomicBusChannel); err != nil {
		slog.Warn("autonomic: failed to ensure bus channel", "channel", autonomicBusChannel, "err", err)
		return
	}

	if _, err := mgr.AppendEvent(autonomicBusChannel, autonomicEventType, autonomicEventFrom, payload); err != nil {
		slog.Warn("autonomic: failed to emit health snapshot to bus", "channel", autonomicBusChannel, "err", err)
	}
}

// snapshotFingerprint returns a deterministic hash of the per-provider health
// shape (name, Health, Sync) sorted by name. Two snapshots with the same
// fingerprint represent the same provider population in the same buckets —
// the caller can treat them as the same event for the purpose of suppressing
// repeat LLM escalation. Operation and Message are intentionally excluded:
// Operation transitions are short-lived and Message often carries timestamps
// or counters that would defeat the dedupe.
//
// Returns "" when the snapshot has no providers (an empty registry has no
// degradation to dedupe).
func snapshotFingerprint(snap KernelHealthSnapshot) string {
	if len(snap.Providers) == 0 {
		return ""
	}
	names := make([]string, 0, len(snap.Providers))
	for n := range snap.Providers {
		names = append(names, n)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		st := snap.Providers[n]
		h.Write([]byte(n))
		h.Write([]byte{'|'})
		h.Write([]byte(st.Health))
		h.Write([]byte{'|'})
		h.Write([]byte(st.Sync))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// --- Self-healing reconcile loop ---------------------------------------------

// healDegradedProviders iterates all registered Reconcilables and, for any
// whose Health() is not Healthy, runs the full plan/apply cycle:
//
//  1. FetchLive — probe actual state.
//  2. ComputePlan — diff declared vs live.
//  3. ApplyPlan — execute any corrective actions (e.g. start a crashed process).
//
// This is the deterministic self-healing path. It runs on every tick before the
// LLM escalation predicate so that transient crashes (mlx_lm.server exited) are
// corrected autonomically without waking the LLM. Errors are logged but never
// propagate — a failed apply leaves Health() degraded, which will trigger the
// LLM escalation predicate on the same tick if needed.
func healDegradedProviders(ctx context.Context) {
	healBackoffMu.Lock()
	healTickCount++
	currentTick := healTickCount
	healBackoffMu.Unlock()

	names := reconcile.ListProviders()
	for _, name := range names {
		p, err := reconcile.GetProvider(name)
		if err != nil {
			continue
		}
		h := p.Health()
		// Only attempt self-heal when the provider is non-healthy (Degraded,
		// Missing, OutOfSync). Suspended and Progressing providers are either
		// intentionally paused or already converging — skip them.
		needsHeal := h.Health == reconcile.HealthDegraded ||
			h.Health == reconcile.HealthMissing ||
			h.Sync == reconcile.SyncStatusOutOfSync

		if !needsHeal {
			// Successful state: reset failure counter.
			if n, recovered := healBackoff.RecordSuccess(name); recovered {
				slog.Info("autonomic: self-heal: provider recovered, resetting backoff",
					"provider", name,
					"was_fail_count", n,
				)
			}
			continue
		}

		// Check backoff: if a previous failure scheduled a skip window, honour it.
		if !healBackoff.Ready(name, currentTick) {
			continue // silently skip — already warned when backoff was set
		}

		// Heap watermark guard (#716): skip memory-heavy providers while the
		// heap is above the watermark. No backoff penalty — a skip is not a failure.
		if heapGuardSkip(p, name) {
			continue
		}

		slog.Info("autonomic: self-heal: starting reconcile cycle",
			"provider", name,
			"health", string(h.Health),
			"sync", string(h.Sync),
		)

		// Load config (no-op for most providers that parse config at construction).
		cfg, err := p.LoadConfig("")
		if err != nil {
			slog.Warn("autonomic: self-heal: LoadConfig failed", "provider", name, "err", err)
			healRecordFailure(name, currentTick, "LoadConfig", err)
			continue
		}

		// Fetch live state.
		live, err := p.FetchLive(ctx, cfg)
		if err != nil {
			slog.Warn("autonomic: self-heal: FetchLive failed", "provider", name, "err", err)
			healRecordFailure(name, currentTick, "FetchLive", err)
			continue
		}

		// Compute plan.
		plan, err := p.ComputePlan(cfg, live, nil)
		if err != nil {
			slog.Warn("autonomic: self-heal: ComputePlan failed", "provider", name, "err", err)
			healRecordFailure(name, currentTick, "ComputePlan", err)
			continue
		}
		if plan == nil || !plan.Summary.HasChanges() {
			slog.Debug("autonomic: self-heal: no changes needed", "provider", name)
			continue
		}

		// Apply plan.
		results, err := p.ApplyPlan(ctx, plan)
		if err != nil {
			slog.Warn("autonomic: self-heal: ApplyPlan failed",
				"provider", name,
				"err", err,
				"results", len(results),
			)
			healRecordFailure(name, currentTick, "ApplyPlan", err)
			continue
		}

		// A nil top-level error is NOT sufficient evidence of success. Several
		// providers — LMSModelStateProvider among them — deliberately treat a
		// failed action as non-fatal: they append a Result{Status:
		// ApplyFailed} and return (results, nil) so one bad action does not
		// abandon the rest of the plan. This loop previously inspected only
		// err, so for those providers the backoff below was structurally
		// unreachable: every tick logged "apply complete" and reset the
		// counter while the action inside failed identically forever.
		applyFailed := 0
		for _, r := range results {
			if r.Status == reconcile.ApplyFailed {
				applyFailed++
			}
		}
		if applyFailed > 0 {
			slog.Warn("autonomic: self-heal: apply reported failed actions",
				"provider", name,
				"apply_failed", applyFailed,
				"results", len(results),
			)
			healRecordFailure(name, currentTick, "ApplyPlan",
				fmt.Errorf("%d action(s) failed during apply", applyFailed))
			continue
		}

		// Successful apply: reset backoff.
		if n, recovered := healBackoff.RecordSuccess(name); recovered {
			slog.Info("autonomic: self-heal: apply succeeded, resetting backoff",
				"provider", name,
				"was_fail_count", n,
			)
		}

		slog.Info("autonomic: self-heal: apply complete",
			"provider", name,
			"actions", len(plan.Actions),
			"results", len(results),
		)
	}
}

// healRecordFailure increments the consecutive failure counter for a provider
// and schedules a backoff skip window. The skip depth doubles each time,
// capped at healBackoffMaxSkip ticks. A WARN is emitted once when the
// failure count first reaches healBackoffWarnAt.
func healRecordFailure(name string, currentTick int, step string, err error) {
	n, skip := healBackoff.RecordFailure(name, currentTick)

	if n == healBackoffWarnAt {
		slog.Warn("autonomic: self-heal: provider repeatedly failing, entering backoff",
			"provider", name,
			"step", step,
			"err", err,
			"consecutive_failures", n,
			"skip_ticks", skip,
		)
	} else if n > healBackoffWarnAt {
		slog.Debug("autonomic: self-heal: provider still failing, extending backoff",
			"provider", name,
			"consecutive_failures", n,
			"skip_ticks", skip,
		)
	}
}

// snapshotToPayload serialises KernelHealthSnapshot into the map[string]any
// shape expected by BusSessionManager.AppendEvent.
func snapshotToPayload(snap KernelHealthSnapshot) (map[string]any, error) {
	// Round-trip through JSON to get a map[string]any cleanly.
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal to payload: %w", err)
	}
	return payload, nil
}
