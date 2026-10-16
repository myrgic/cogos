// Package daemon registers all Reconcilable providers that the kernel daemon
// (cmd/cogos) needs to surface in proprioception health blocks.
//
// Background: The production providers (agent, component, discord, identity,
// service) were
// originally defined in the workspace-root package main of the cog CLI.
// Because package main cannot be imported, the daemon binary (built from
// cmd/cogos) could not reach those registrations and pkg/reconcile.ListProviders()
// always returned empty.
//
// This package provides daemon-safe provider structs whose Health()
// implementations replicate the workspace-root logic using only importable
// packages (os, filepath, pkg/reconcile).
// The non-health methods (LoadConfig, FetchLive, ComputePlan, ApplyPlan,
// BuildState) return errors — the daemon only exercises Health() through the
// proprioception block.
//
// Workspace context is injected at boot via SetWorkspaceRoot(), called by
// engine.SetProvidersWorkspace after LoadConfig resolves cfg.WorkspaceRoot.
// This avoids the workspace.ResolveWorkspace() dependency-injection seams
// (LoadConfig/GitRoot func vars) that are only wired in the cog CLI's main
// package, not in cmd/cogos.
//
// The component provider is already fully extracted to
// internal/providers/component and is wired here via blank import.
// The pin provider (internal/providers/pin) is fully extracted and registered
// here directly — its Health() delegates to the extracted package.
// The discord provider (Wave 4, ADR-121) is likewise fully extracted to
// internal/providers/discord and registered here directly (globalDiscordProvider)
// — its full LoadConfig/FetchLive/ComputePlan/ApplyPlan/BuildState/Health/
// ExportConfig cycle runs for real, re-homed from _legacy/wave4-providers/.
// The identity provider (Wave 6b) is registered here as a stub; the full
// plan/apply wiring lives in the workspace-root identity_wiring.go.
// The other four (agent, eval, identity, service) are implemented as minimal
// Health()-only structs below.
//
// cmd/cogos/providers_wire.go imports this package (triggering init()) and
// wires both engine.RegisterProviders and engine.SetProvidersWorkspace so
// the full seam is operational before the HTTP server starts serving requests.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/myrgic/cogos/internal/providers/discord"
	"github.com/myrgic/cogos/internal/providers/pin"
	"github.com/myrgic/cogos/internal/providers/selfupdate"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"

	// Trigger internal/providers/component's init() which registers "component".
	_ "github.com/myrgic/cogos/internal/providers/component"
)

// workspaceRoot is set at daemon boot by SetWorkspaceRoot (called from
// engine.SetProvidersWorkspace after LoadConfig resolves the workspace path).
// It is read by resolveRoot() on every Health() probe. Zero value means
// "not yet wired" — callers get a clear error rather than a silent bad result.
var (
	workspaceRootMu sync.RWMutex
	workspaceRoot   string
)

// SetWorkspaceRoot injects the resolved workspace path into this package so
// that all daemon-side provider Health() implementations can resolve their
// filesystem checks without depending on workspace.ResolveWorkspace(), whose
// dependency-injection seams (LoadConfig/GitRoot) are not wired in the
// cmd/cogos binary.
//
// Must be called before any provider Health() invocation — engine.runServe()
// calls it via engine.SetProvidersWorkspace immediately after LoadConfig
// resolves cfg.WorkspaceRoot.
func SetWorkspaceRoot(root string) {
	workspaceRootMu.Lock()
	workspaceRoot = root
	// Unlock before calling into selfupdate to avoid holding workspaceRootMu
	// across a cross-package call (selfupdate.SetWorkspaceRoot acquires its own
	// global.mu). Do NOT replace this with `defer workspaceRootMu.Unlock()`:
	// that would hold this lock across the cross-package acquisition and create a
	// lock-ordering hazard if selfupdate ever reaches back into this package.
	workspaceRootMu.Unlock()
	// Teach the self-update provider the workspace root so LoadConfig and the
	// updater log path resolve correctly. The full plan/apply cycle runs in the
	// daemon (unlike the Health()-only stubs above).
	selfupdate.SetWorkspaceRoot(root)
	// Teach the discord provider the workspace root so Health() can fall back
	// to checking .cog/config/discord/config.hcl presence even before a token
	// has been resolved (mirrors the pre-Wave-4 stub's check).
	globalDiscordProvider.SetWorkspaceRoot(root)
}

// globalPinProvider is the singleton pinProvider registered in init().
// Exposed so providers_wire.go can inject the workspace locator after the
// engine's URIRegistry is initialised.
var globalPinProvider = &pinProvider{stubMethods: stubMethods{name: "pin"}}

// globalDiscordProvider is the singleton *discord.DiscordProvider registered
// in init(). Wave 4 (ADR-121): replaces the former Health()-only discordProvider
// stub with the real LoadConfig/FetchLive/ComputePlan/ApplyPlan/BuildState cycle
// re-homed from _legacy/wave4-providers/. SetWorkspaceRoot (above) wires the
// workspace root into it at boot so Health()'s config.hcl fallback check keeps
// working exactly as the stub's did.
var globalDiscordProvider = &discord.DiscordProvider{}

// SetPinWorkspaceLocator wires a WorkspaceLocator into the pin provider so that
// FetchLive can consult the global workspace registry. Called from
// providers_wire.go after engine.URIRegistry is available.
func SetPinWorkspaceLocator(loc pin.WorkspaceLocator) {
	globalPinProvider.mu.Lock()
	defer globalPinProvider.mu.Unlock()
	if globalPinProvider.impl == nil {
		globalPinProvider.impl = pin.New(nil)
	}
	globalPinProvider.impl.SetWorkspaceLocator(loc)
}

// pinProvider is the daemon-side wrapper around the fully-extracted pin provider.
// It delegates Health() to pin.New() after running the read-only refresh cycle
// (LoadConfig → FetchLive → ComputePlan, no ApplyPlan) so that pinStates is
// populated before Health() is called. Without the read-only cycle, pinStates is
// always empty and Health() always reports green ("no pins declared") even when
// pin files declare drift.
//
// The refresh is time-bounded to 10 s to avoid blocking the proprioception tick.
// staleAfter controls how long a prior cycle result is reused to avoid hammering
// the filesystem on every foveated-context refresh.
//
// Concurrency: mu is held from the staleness check through the entire refresh
// so that concurrent Health() calls serialise on the lock. Only the first
// stale caller runs the refresh; subsequent callers reuse the result already
// being computed (they block on mu.Lock() and see a fresh lastProbe when they
// acquire it). This eliminates parallel RefreshState invocations and the
// associated parallel bumpPinFile race.
type pinProvider struct {
	stubMethods
	mu        sync.Mutex
	impl      *pin.Provider
	lastProbe time.Time
}

const pinProbeStaleAfter = 30 * time.Second

func (p *pinProvider) Type() string { return "pin" }

func (p *pinProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}

	// Hold mu for the entire probe so that concurrent Health() calls serialise
	// here. The first caller that finds a stale result runs RefreshState; any
	// other concurrent callers block on Lock() and re-check freshness once they
	// acquire it (finding the result already fresh).
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.impl == nil {
		p.impl = pin.New(nil)
	}

	if time.Since(p.lastProbe) >= pinProbeStaleAfter {
		// Run the read-only refresh cycle (no ApplyPlan, no filesystem writes).
		// A 10 s context prevents blocking the proprioception tick indefinitely.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.impl.RefreshState(ctx, root); err != nil {
			return reconcile.ResourceStatus{
				Sync:      reconcile.SyncStatusUnknown,
				Health:    reconcile.HealthDegraded,
				Operation: reconcile.OperationIdle,
				Message:   fmt.Sprintf("pin refresh: %v", err),
			}
		}
		p.lastProbe = time.Now()
	}

	return p.impl.Health()
}

func init() {
	reconcile.RegisterProvider("agent", &agentProvider{stubMethods: stubMethods{name: "agent"}})
	reconcile.RegisterProvider("discord", globalDiscordProvider)
	reconcile.RegisterProvider("eval", &evalProvider{stubMethods: stubMethods{name: "eval"}})
	reconcile.RegisterProvider("identity", &identityProvider{stubMethods: stubMethods{name: "identity"}})
	reconcile.RegisterProvider("pin", globalPinProvider)
	reconcile.RegisterProvider("self-update", selfupdate.Registered())
	reconcile.RegisterProvider("service", &serviceProvider{stubMethods: stubMethods{name: "service"}})
}

// resolveRoot returns the workspace root or an error status.
// It reads the package-level workspaceRoot set by SetWorkspaceRoot, bypassing
// workspace.ResolveWorkspace() whose DI seams are not wired in cmd/cogos.
func resolveRoot() (string, *reconcile.ResourceStatus) {
	workspaceRootMu.RLock()
	root := workspaceRoot
	workspaceRootMu.RUnlock()
	if root == "" {
		s := reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   "workspace not yet configured",
		}
		return "", &s
	}
	return root, nil
}

// stubMethods satisfies the non-Health parts of reconcile.Reconcilable.
// These daemon-side stubs are Health()-only: they expose proprioception state
// to the foveated context block but are not driven through the full
// LoadConfig→FetchLive→ComputePlan→ApplyPlan cycle.
//
// LoadConfig returns a sentinel struct so the reconcile daemon's cycle exits
// cleanly after BuildState (no WARN spam). FetchLive, ComputePlan, ApplyPlan,
// and BuildState return explicit "not available" errors so any unexpected
// caller gets a clear diagnostic rather than a silent nil.
type stubMethods struct{ name string }

// stubNoopConfig is the no-op sentinel returned by stubMethods.LoadConfig.
// The reconcile daemon checks for this type in ComputePlan and produces an
// empty plan, avoiding the WARN-per-tick log noise on fresh installs.
type stubNoopConfig struct{ name string }

func (s *stubMethods) LoadConfig(_ string) (any, error) {
	// Return a no-op config so the daemon cycle does not WARN on LoadConfig.
	// Health() is the only meaningful operation for daemon-side stub providers.
	return &stubNoopConfig{name: s.name}, nil
}
func (s *stubMethods) FetchLive(_ context.Context, cfg any) (any, error) {
	// Pass through the stub config so the cycle can proceed to ComputePlan.
	if _, ok := cfg.(*stubNoopConfig); ok {
		return cfg, nil
	}
	return nil, fmt.Errorf("daemon: FetchLive not available for %s provider", s.name)
}
func (s *stubMethods) ComputePlan(cfg any, _ any, _ *reconcile.State) (*reconcile.Plan, error) {
	if nc, ok := cfg.(*stubNoopConfig); ok {
		// Return an empty plan so the daemon sees "no drift" and exits quietly.
		return &reconcile.Plan{ResourceType: nc.name}, nil
	}
	return nil, fmt.Errorf("daemon: ComputePlan not available for %s provider", s.name)
}
func (s *stubMethods) ApplyPlan(_ context.Context, _ *reconcile.Plan) ([]reconcile.Result, error) {
	return nil, fmt.Errorf("daemon: ApplyPlan not available for %s provider", s.name)
}
func (s *stubMethods) BuildState(cfg any, _ any, _ *reconcile.State) (*reconcile.State, error) {
	if nc, ok := cfg.(*stubNoopConfig); ok {
		return reconcile.NewState(nc.name), nil
	}
	return nil, fmt.Errorf("daemon: BuildState not available for %s provider", s.name)
}

// ─── agent ────────────────────────────────────────────────────────────────────

type agentProvider struct{ stubMethods }

func (p *agentProvider) Type() string { return "agent" }

func (p *agentProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}
	agentsDir := filepath.Join(root, ".cog", "bin", "agents")
	info, err := os.Stat(agentsDir)
	if err != nil {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   fmt.Sprintf("agents directory missing: %v", err),
		}
	}
	if !info.IsDir() {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "agents path exists but is not a directory",
		}
	}
	registryPath := filepath.Join(agentsDir, "registry.yaml")
	if _, err := os.Stat(registryPath); err != nil {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   "registry.yaml missing",
		}
	}
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusUnknown,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   fmt.Sprintf("agents directory readable (%s)", agentsDir),
	}
}

// ─── discord ──────────────────────────────────────────────────────────────────
//
// Wave 4 (ADR-121): the real *discord.DiscordProvider is registered directly
// (see globalDiscordProvider above and its RegisterProvider("discord", ...)
// call in init()). There is no daemon-local stub type here anymore — the
// re-homed provider from _legacy/wave4-providers/ implements the full
// LoadConfig/FetchLive/ComputePlan/ApplyPlan/BuildState/Health/ExportConfig
// cycle itself.

// ─── eval ─────────────────────────────────────────────────────────────────────

// evalProvider surfaces eval harness health for the daemon's proprioception block.
//
// Health() reads two state files from .cog/state/ that the eval harness writes:
//   - eval-baselines.json        — which experiments have a pinned baseline
//   - eval-dispatch-triggers.json — pending on-demand run requests
//
// Full plan/apply lives in the workspace-root CLI binary (eval_wiring.go);
// the daemon only needs Health() to contribute to the foveated context block.
type evalProvider struct{ stubMethods }

func (p *evalProvider) Type() string { return "eval" }

func (p *evalProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}

	stateDir := filepath.Join(root, ".cog", "state")

	// Read baseline pins — presence indicates experiments are being tracked.
	pinsPath := filepath.Join(stateDir, "eval-baselines.json")
	pinnedCount := 0
	if data, err := os.ReadFile(pinsPath); err == nil {
		var pins map[string]string
		if json.Unmarshal(data, &pins) == nil {
			pinnedCount = len(pins)
		}
	}

	// Read pending dispatch triggers — non-empty means a run was requested but
	// the reconcile cycle hasn't consumed it yet.
	triggersPath := filepath.Join(stateDir, "eval-dispatch-triggers.json")
	pendingCount := 0
	if data, err := os.ReadFile(triggersPath); err == nil {
		var triggers map[string]bool
		if json.Unmarshal(data, &triggers) == nil {
			pendingCount = len(triggers)
		}
	}

	// Check for the experiments directory — its presence signals eval is configured.
	experimentsDir := filepath.Join(root, ".cog", "mem", "semantic", "architecture", "tournament", "experiments")
	_, expDirErr := os.Stat(experimentsDir)

	switch {
	case expDirErr != nil:
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   "no tournament experiments directory — eval not configured",
		}
	case pendingCount > 0:
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthProgressing,
			Operation: reconcile.OperationSyncing,
			Message:   fmt.Sprintf("%d pending trigger(s), %d pinned baseline(s)", pendingCount, pinnedCount),
		}
	default:
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthHealthy,
			Operation: reconcile.OperationIdle,
			Message:   fmt.Sprintf("%d pinned baseline(s); full plan/apply via cog CLI", pinnedCount),
		}
	}
}

// ─── identity ────────────────────────────────────────────────────────────────

// identityProvider surfaces identity CRD health for the daemon's proprioception
// block. Health() checks for the presence of the identity config directory
// (.cog/config/identities/) and counts declared CRDs.
//
// Full plan/apply lives in the workspace-root CLI binary (identity_wiring.go);
// the daemon only needs Health() to contribute to the foveated context block.
//
// Wave 6b: stub-only. Wave 6c will wire the real IdentityProvider once the
// Constellation DB layer is extractable from the workspace-root package.
type identityProvider struct{ stubMethods }

func (p *identityProvider) Type() string { return "identity" }

func (p *identityProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}
	identitiesDir := filepath.Join(root, ".cog", "config", "identities")
	entries, err := os.ReadDir(identitiesDir)
	if err != nil {
		// No identities directory — treat as healthy (no identities declared).
		// Presence of the directory is optional; the provider operates on an
		// empty set if it does not exist.
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusSynced,
			Health:    reconcile.HealthHealthy,
			Operation: reconcile.OperationIdle,
			Message:   "no identities directory — no identities declared",
		}
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".yaml" {
			count++
		}
	}
	if count == 0 {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusSynced,
			Health:    reconcile.HealthHealthy,
			Operation: reconcile.OperationIdle,
			Message:   "identities directory present, no CRDs declared",
		}
	}
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusUnknown,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   fmt.Sprintf("%d identity CRD(s) declared; runtime status requires CLI", count),
	}
}

// ─── service ─────────────────────────────────────────────────────────────────

// serviceProvider checks for service CRD yaml files under
// .cog/config/services/. Full Docker container-status checks require the
// workspace-root CLI (cog plan service) — the daemon reports structural
// presence only.
type serviceProvider struct{ stubMethods }

func (p *serviceProvider) Type() string { return "service" }

func (p *serviceProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}
	servicesDir := filepath.Join(root, ".cog", "config", "services")
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		// No services directory — treat as healthy (no services declared).
		return reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".yaml" {
			count++
		}
	}
	if count == 0 {
		return reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
	}
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusUnknown,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   fmt.Sprintf("%d service CRD(s) declared; runtime status requires CLI", count),
	}
}
