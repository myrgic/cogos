// Package vitalsretention implements RFC-040 S2: the node-vitals retention
// provider.
//
// The kernel's autonomic ticker already emits a KernelHealthSnapshot every
// tick onto bus_kernel_proprio (internal/engine/autonomic_ticker.go,
// event type "kernel.health.snapshot.v1") — but that stream evaporates: the
// bus rotates events.jsonl to a timestamped archive at 64MB
// (internal/engine/bus_session.go) and the only read path,
// BusSessionManager.ReadEvents, scans the current file only. RFC-040 Open
// Question 1 established that raw bytes likely survive rotation but the
// query path does not — so S2 must be a RECORDER, not a compactor sitting
// over the bus. This package is that recorder.
//
// Design summary (see RFC-040 §"S2 — Retention provider" for the full spec):
//
//   - Recorder subscribes to bus_kernel_proprio via
//     BusSessionManager.AddEventHandler (wired in
//     internal/providers/all/all.go's engine.WireProviderRuntime hook,
//     which runs after the live bus manager exists — see ADR-085's
//     leaf-package discipline: this package never imports internal/engine).
//   - Every snapshot decomposes into named metrics (host vitals + provider
//     health counts + anomalies), each appended as one NDJSON line to
//     .cog/observatory/vitals/<node_key>/raw/<metric>/<YYYY-MM-DD>.ndjson.
//   - Compaction downsamples raw->5m after 48h and 5m->1h after 30d,
//     pruning the finer tier once its coarser replacement is written (N3).
//   - The only query surface is Window(metric, since, resolution) — no DSL
//     (N2). Exposed over GET /v1/vitals and a cog_ MCP tool by
//     internal/engine (see serve_vitals.go / mcp_tool_vitals.go there).
//   - File naming keys on node identity behind the NodeKeySource seam
//     (nodekey.go) — the concrete node_id is being reconciled in cogos
//     PR #474 (Seam B); this package does not depend on or block on it.
//
// This provider registers with pkg/substrate/reconcile as "vitals-retention"
// so its Health() folds into the kernel's proprioception snapshot like any
// other provider — but it manages no external declared-vs-live resource, so
// LoadConfig/FetchLive/ComputePlan/ApplyPlan/BuildState are honest no-ops in
// the same "stubMethods" idiom internal/providers/daemon/daemon.go already
// uses for agent/discord/eval/identity/etc.: Health() is the only
// meaningful axis for a provider whose job is "keep recording," not
// "reconcile declared state."
package vitalsretention

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/myrgic/cogos/internal/workspace"
	"github.com/myrgic/cogos/pkg/substrate/cogfield"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// Type is the resource type identifier registered with pkg/reconcile.
const Type = "vitals-retention"

// Block is the bus event wire type. Alias to the canonical
// pkg/substrate/cogfield.Block — identical to internal/engine's BusBlock
// alias, so BusSessionManager.AddEventHandler's func(busID string, block
// *BusBlock) signature is satisfied without this package importing
// internal/engine.
type Block = cogfield.Block

// ProprioBusID and ProprioEventType mirror the constants in
// internal/engine/autonomic_ticker.go. They are duplicated here (as literal
// string constants, not an import) deliberately: this package must not
// import internal/engine (that would create an import cycle, since engine
// already imports this package to wire the bus handler and MCP/HTTP
// surfaces). Any change to those constants must be mirrored here — a small,
// explicit coupling preferred over the alternative of collapsing the
// leaf-package boundary.
const (
	ProprioBusID     = "bus_kernel_proprio"
	ProprioEventType = "kernel.health.snapshot.v1"
)

func init() {
	reconcile.RegisterProvider(Type, &providerAdapter{})
}

// --- Workspace root injection -------------------------------------------

var (
	workspaceRootMu     sync.RWMutex
	daemonWorkspaceRoot string
)

// SetWorkspaceRoot injects the resolved workspace path, mirroring the
// pattern used by internal/providers/component and internal/providers/
// selfupdate. Called by internal/providers/all's engine.SetProvidersWorkspace
// composition immediately after LoadConfig resolves cfg.WorkspaceRoot, so
// both the daemon's autonomic tick and `cog reconcile`/`cog health` CLI
// invocations see a consistent root.
func SetWorkspaceRoot(root string) {
	workspaceRootMu.Lock()
	defer workspaceRootMu.Unlock()
	daemonWorkspaceRoot = root
}

// resolveWorkspaceRoot returns the injected root, falling back to
// workspace.ResolveWorkspace() when nothing has been injected yet (e.g. a
// standalone CLI invocation that never went through SetWorkspaceRoot).
func resolveWorkspaceRoot() (string, error) {
	workspaceRootMu.RLock()
	root := daemonWorkspaceRoot
	workspaceRootMu.RUnlock()
	if root != "" {
		return root, nil
	}
	root, _, err := workspace.ResolveWorkspace()
	if err != nil {
		return "", fmt.Errorf("vitals-retention: no workspace root injected and resolution failed: %w", err)
	}
	return root, nil
}

// --- Recorder --------------------------------------------------------------

// staleThreshold is how long the recorder may go without a successful append
// before Health() reports Degraded.
//
// Sizing: the recorder is driven by the autonomic ticker's
// bus_kernel_proprio dispatch (see recorder.go), which fires on the order of
// once per minute. 15 minutes is therefore many missed ticks — comfortably
// past transient scheduling jitter, a slow compaction pass, or a single
// dropped event, while still catching a genuine stall the same hour it
// starts rather than the next time a human happens to look.
//
// Deliberately NOT derived from the tick interval: this package is a leaf
// (ADR-085) and does not import internal/engine, and a monitor whose
// threshold silently follows the thing it monitors can be widened into
// uselessness by an unrelated config change.
const staleThreshold = 15 * time.Minute

// processStart is the fallback boot stamp for a Recorder that was created
// without one (the zero value). See Recorder.startedAt for why the grace
// period is per-instance rather than package-global.
var processStart = time.Now()

// Recorder is the live retention engine: it appends bus_kernel_proprio
// snapshots to per-metric-day NDJSON files and runs compaction/budget
// enforcement opportunistically on the same cadence (no new loop or daemon —
// RFC-040's "the ticker IS the scrape loop" doctrine, extended to S2: the
// bus-handler dispatch that already fires once per tick is the compaction
// cadence too).
type Recorder struct {
	mu sync.Mutex

	// last records outcome bookkeeping for Health().
	lastAppendErr  error
	lastAppendAt   time.Time
	lastCompactErr error
	lastCompactAt  time.Time

	// lastAppendOKAt is the time of the most recent SUCCESSFUL append.
	//
	// Distinct from lastAppendAt, which is stamped on every append attempt
	// including failures. Health() needs the success timestamp because the
	// failure this field exists to catch is not "appends are erroring" but
	// "appends have STOPPED HAPPENING AT ALL" — the handler no longer being
	// invoked, or being invoked and silently recording nothing.
	//
	// Found live 2026-08-27: this recorder reported Healthy in the kernel
	// snapshot while its newest on-disk row was 17 hours old. Health() only
	// checked lastAppendErr, so a recorder that simply stopped writing was
	// indistinguishable from one working perfectly. That is precisely the
	// "monitor proven only by silence" failure this workspace treats as a
	// defect class: absence of an error is not evidence of liveness.
	lastAppendOKAt time.Time

	// startedAt is this recorder's own boot stamp, used for the "never
	// appended yet" grace period in Health().
	//
	// Per-instance rather than package-global (cog-review note on PR #585):
	// a single shared processStart is harmless while production runs one
	// globalRecorder, but it silently couples every Recorder's grace period
	// to package init time. A test — or a future multi-node/multi-recorder
	// arrangement — that constructs a fresh Recorder after the package has
	// been loaded for a while would see it born already "stale", which is
	// wrong and confusing. Zero value falls back to processStart via
	// bootStamp() so existing construction sites need no change.
	startedAt time.Time

	// compacting is the single-flight guard: true while a compaction
	// goroutine is running for this recorder. Read and written only under
	// mu, alongside the lastCompactAt check-then-claim in
	// claimCompactSlot (compact.go) — see #497: this closes the
	// check-then-act race on lastCompactAt flagged non-blocking in #493's
	// final review, since claiming the slot and stamping lastCompactAt now
	// happen atomically in the same critical section instead of a
	// check-then-later-set split across the compaction pass.
	compacting bool

	// lastEventDay is the UTC day of block.Ts for the most recent event
	// HandleBusEvent has processed — the exact day appendRow last wrote
	// into (recorded by recordEventDay). enforceRawBudget's excludeDay
	// (compact.go) reads this via currentExcludeDay instead of calling
	// time.Now() itself — see #500: maybeCompact's goroutine computing
	// excludeDay from wall-clock at its own (possibly delayed) run time
	// could disagree with the day appendRow actually wrote to, narrowly
	// reopening the read-then-delete race #498 closed. Keying off the same
	// time source appendRow uses removes that independent clock read
	// entirely, for any recorder that has processed at least one event.
	lastEventDay time.Time

	// compactWG counts in-flight compaction goroutines spawned by
	// maybeCompact (Add(1) before the `go func`, Done() via defer inside
	// it). Production code never waits on it — per #497 the bus-handler
	// dispatch path must return without blocking on compaction I/O — but
	// tests that exercise the real (non-stubbed) compactHook need a way to
	// join the goroutine before their t.TempDir() cleanup fires; see
	// waitForCompactionIdle in compact_async_test.go and #515.
	compactWG sync.WaitGroup
}

// globalRecorder is the process-wide recorder instance. A package-level
// singleton (mirroring globalPinProvider/globalDiscordProvider in
// internal/providers/daemon/daemon.go) because pkg/substrate/reconcile's
// registry and BusSessionManager.AddEventHandler both need a stable handle
// to the same instance, wired from two different call sites
// (SetWorkspaceRoot via engine.SetProvidersWorkspace; HandleBusEvent via
// engine.WireProviderRuntime).
var globalRecorder = &Recorder{startedAt: time.Now()}

// GlobalRecorder returns the process-wide Recorder instance.
func GlobalRecorder() *Recorder { return globalRecorder }

// HandleBusEvent is the package-level entry point wired via
// BusSessionManager.AddEventHandler(name, HandleBusEvent). It delegates to
// the global recorder so engine.WireProviderRuntime's closure stays a
// one-liner.
func HandleBusEvent(busID string, block *Block) {
	globalRecorder.HandleBusEvent(busID, block)
}

// Window queries the global recorder. See window.go for the full contract.
func Window(metric string, since time.Time, resolution string) ([]Point, error) {
	return globalRecorder.Window(metric, since, resolution)
}

// baseDir returns the on-disk root for this node's vitals
// (.cog/observatory/vitals, or Config.BaseDir override for tests).
func (r *Recorder) baseDir() (string, error) {
	root, err := resolveWorkspaceRoot()
	if err != nil {
		return "", err
	}
	cfg := loadConfigCached(root)
	if cfg.BaseDir != "" {
		return cfg.BaseDir, nil
	}
	return vitalsBaseDir(root), nil
}

func vitalsBaseDir(workspaceRoot string) string {
	return filepath.Join(workspaceRoot, ".cog", "observatory", "vitals")
}

// --- Health ------------------------------------------------------------

// Health reports the provider's status for the kernel proprioception
// snapshot. Sync is always "Synced" — this provider has no declared-vs-live
// resource to drift (see package doc); Health reflects only whether the
// recorder is actually able to write and compact.
func (r *Recorder) Health() reconcile.ResourceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := resolveWorkspaceRoot(); err != nil {
		return reconcile.ResourceStatus{
			Sync: reconcile.SyncStatusUnknown, Health: reconcile.HealthMissing,
			Operation: reconcile.OperationIdle, Message: "no workspace root",
		}
	}

	// A recent append failure is Degraded, not Missing — the recorder is
	// present and wired, just failing writes (e.g. disk full, permissions).
	if r.lastAppendErr != nil && time.Since(r.lastAppendAt) < 10*time.Minute {
		return reconcile.ResourceStatus{
			Sync: reconcile.SyncStatusSynced, Health: reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "recent append failure: " + r.lastAppendErr.Error(),
		}
	}

	// STALENESS — the liveness check, added 2026-08-27 after this recorder
	// reported Healthy for 17 hours while writing nothing.
	//
	// The checks above can only fire when an append ATTEMPT recorded an
	// error. They are structurally blind to the worse failure: the handler
	// no longer being invoked at all. In that state lastAppendErr stays nil
	// forever and every probe reads green, which is the "monitor proven only
	// by silence" defect — a monitor whose passing signal is indistinguishable
	// from its own death.
	//
	// This recorder is driven synchronously by the autonomic ticker's
	// bus_kernel_proprio dispatch, so a successful append is expected roughly
	// once per tick. Going staleThreshold without one means the drive path is
	// broken upstream even though nothing here errored.
	//
	// Reported as Degraded rather than Missing: the provider is present and
	// wired, it is simply no longer being fed. Missing would imply it failed
	// to load.
	if r.lastAppendOKAt.IsZero() {
		// No successful append since this recorder started. Only meaningful
		// once it has been alive long enough for a tick to have plausibly
		// fired; before that, "nothing yet" is the correct and expected
		// state, not a fault.
		if time.Since(r.bootStamp()) > staleThreshold {
			return reconcile.ResourceStatus{
				Sync: reconcile.SyncStatusSynced, Health: reconcile.HealthDegraded,
				Operation: reconcile.OperationIdle,
				Message: "no successful append since process start " +
					time.Since(r.bootStamp()).Round(time.Second).String() +
					" ago — bus_kernel_proprio dispatch may not be wired",
			}
		}
	} else if age := time.Since(r.lastAppendOKAt); age > staleThreshold {
		return reconcile.ResourceStatus{
			Sync: reconcile.SyncStatusSynced, Health: reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message: "stale: last successful append " +
				age.Round(time.Second).String() +
				" ago (expected ~1/tick) — the pulse has stopped",
		}
	}

	if r.lastCompactErr != nil && time.Since(r.lastCompactAt) < 1*time.Hour {
		return reconcile.ResourceStatus{
			Sync: reconcile.SyncStatusSynced, Health: reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "recent compaction failure: " + r.lastCompactErr.Error(),
		}
	}
	return reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
}

// bootStamp returns this recorder's boot time, falling back to the package
// stamp when the field was never set (zero-value Recorder). Caller must hold
// r.mu, or call it only on a recorder not yet shared across goroutines.
func (r *Recorder) bootStamp() time.Time {
	if r.startedAt.IsZero() {
		return processStart
	}
	return r.startedAt
}

func (r *Recorder) recordAppendResult(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastAppendErr = err
	r.lastAppendAt = time.Now()
	if err == nil {
		r.lastAppendOKAt = r.lastAppendAt
	}
}

// recordCompactResult records the outcome of a finished compaction pass and
// releases the single-flight slot claimed by claimCompactSlot (compact.go)
// — the two are folded into one critical section so "compaction finished"
// and "a new one may be claimed" become visible to other goroutines
// atomically.
func (r *Recorder) recordCompactResult(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCompactErr = err
	r.lastCompactAt = time.Now()
	r.compacting = false
}

// recordEventDay records the UTC day of ts — block.Ts, already parsed and
// UTC-normalized by HandleBusEvent — as lastEventDay. Called once per event,
// before that event's row is appended, so a concurrently-running compaction
// goroutine's currentExcludeDay() call sees the day this call is about to
// write to (or has just written to) rather than an independently-read wall
// clock. See #500 and lastEventDay's doc.
func (r *Recorder) recordEventDay(ts time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastEventDay = ts.UTC().Truncate(24 * time.Hour)
}

// currentExcludeDay returns the day enforceRawBudget must never treat as a
// compaction candidate: the UTC day of the most recent event this recorder
// has actually processed (lastEventDay, set by recordEventDay) — the same
// time source appendRow keys off, per #500. Falls back to wall-clock
// time.Now() when no event has been recorded yet (a bare Recorder used
// directly, e.g. in tests or a standalone CLI invocation that never went
// through HandleBusEvent), which matches this package's pre-#500 behavior
// for those callers.
func (r *Recorder) currentExcludeDay() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lastEventDay.IsZero() {
		return r.lastEventDay
	}
	return time.Now().UTC().Truncate(24 * time.Hour)
}

// --- providerAdapter: pkg/substrate/reconcile.Reconcilable wiring -------

// providerAdapter satisfies reconcile.Reconcilable by delegating Health() to
// the global Recorder and giving every other lifecycle method an honest
// no-op body: this provider manages no external declared-vs-live resource
// (it is a recorder, not a reconciler of drift), so there is nothing for
// LoadConfig/FetchLive/ComputePlan/ApplyPlan/BuildState to meaningfully do.
// This mirrors internal/providers/daemon/daemon.go's stubMethods idiom,
// already used for agent/discord/eval/identity/pin/
// self-update/service — an accepted, documented pattern in this codebase for
// providers whose sole health signal is "is the background job running,"
// not "does declared config match live state."
type providerAdapter struct{}

func (providerAdapter) Type() string { return Type }

func (providerAdapter) LoadConfig(root string) (any, error) {
	return loadConfigCached(root), nil
}

func (providerAdapter) FetchLive(_ context.Context, cfg any) (any, error) {
	return cfg, nil
}

func (providerAdapter) ComputePlan(_ any, _ any, _ *reconcile.State) (*reconcile.Plan, error) {
	return &reconcile.Plan{ResourceType: Type}, nil
}

func (providerAdapter) ApplyPlan(_ context.Context, _ *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, nil
}

func (providerAdapter) BuildState(_ any, _ any, existing *reconcile.State) (*reconcile.State, error) {
	if existing != nil {
		return existing, nil
	}
	return reconcile.NewState(Type), nil
}

func (providerAdapter) Health() reconcile.ResourceStatus {
	return globalRecorder.Health()
}
