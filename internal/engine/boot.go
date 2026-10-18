// boot.go — engine.Boot: in-process kernel boot, factored out of runServe.
//
// ADR-101 Phase 1: factor the boot sequence from runServe into a function
// that accepts a context and returns a *Kernel handle. runServe becomes a
// thin wrapper that handles daemon-lifecycle concerns (planServeState,
// saveDaemonState, signal context) and then calls Boot.
//
// Boot does NOT:
//   - Install OS signal handlers (caller's responsibility).
//   - Write or clean up the daemon state file.
//   - Call os.Exit on error (returns error instead).
//   - Call RegisterProviders or SetProvidersWorkspace (already done by runServe
//     before Boot is called; these are intentionally nil in test paths).
//
// Boot DOES:
//   - Load the nucleus.
//   - Build Process, Router, Server, ReconcileDaemon.
//   - Wire telemetry (no-op if no collector).
//   - Wire LocalHarnessController if an MCP server is present.
//   - Start all long-lived goroutines (process loop, HTTP server, reconcile daemon,
//     projection watchers).
//   - Block until the HTTP server is ready (listening) before returning.
//   - Return a *Kernel handle that gives callers Endpoint(), WorkspaceRoot(), and Stop().
//
// Phase 2 adds WithIsolatedRegistry: callers can inject an explicit provider
// list so the ReconcileDaemon bypasses the global registry. This is the
// integration-test isolation mechanism (ADR-101 Decision 3).
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"time"

	bep "github.com/myrgic/cogos/pkg/substrate/bep"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// BootOption is a functional option passed to Boot.
//
// Phase 1 defines no options with observable effect; the type exists so
// that Boot's signature is stable. Phase 2 will add WithIsolatedRegistry,
// WithPollInterval, etc. without changing callers.
type BootOption func(*bootConfig)

// bootConfig holds the resolved configuration derived from BootOptions.
// All fields are optional; zero values mean "use the kernel default".
type bootConfig struct {
	// pollInterval overrides the ReconcileDaemon tick interval.
	// 0 means use the ReconcileDaemon default (30 s).
	pollInterval time.Duration

	// providers, when non-nil, is passed to ReconcileDaemonConfig.Providers so
	// the daemon iterates this list instead of the global registry.
	// Set via WithIsolatedRegistry. nil = use global registry (default).
	providers []reconcile.Reconcilable

	// withoutLocalHarness, when true, skips starting the LocalHarnessController
	// goroutine. Set via WithoutLocalHarness. Measurement boots that need to
	// isolate the ReconcileDaemon's cadence from an uncontrolled background
	// ticker should set this (First Instruments A4).
	withoutLocalHarness bool
}

func applyBootOptions(opts []BootOption) bootConfig {
	cfg := bootConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithIsolatedRegistry returns a BootOption that injects an explicit provider
// list into the ReconcileDaemon, bypassing the global registry.
//
// When this option is set the daemon iterates only the supplied providers;
// the global registry (which in daemon-binary code paths contains stub
// providers) is never consulted. This is the ADR-101 Phase 2 mechanism for
// writing integration tests that exercise real plan/apply without stub
// interference.
//
// Production callers should not use this option; pass nil or omit it to
// preserve the default global-registry behaviour.
func WithIsolatedRegistry(providers ...reconcile.Reconcilable) BootOption {
	return func(c *bootConfig) {
		c.providers = providers
	}
}

// WithPollInterval returns a BootOption that overrides the ReconcileDaemon's
// tick interval. d <= 0 leaves the daemon default (30 s, applied by
// ReconcileDaemonConfig.withDefaults) in effect.
//
// This wires the previously-dead bootConfig.pollInterval field into
// ReconcileDaemonConfig.PollInterval (First Instruments A1). Measurement
// harnesses use this to defeat the natural tick (set very high, e.g. 1h, so
// Trigger() is the sole cycle driver) or to force fast natural cadence in a
// unit test.
func WithPollInterval(d time.Duration) BootOption {
	return func(c *bootConfig) {
		c.pollInterval = d
	}
}

// WithoutLocalHarness returns a BootOption that skips starting the
// LocalHarnessController goroutine (its own ticker, independent of the
// ReconcileDaemon) during Boot.
//
// engine.Boot unconditionally starts LocalHarnessController whenever
// server.mcpServer is non-nil, which is true for every testkernel boot (the
// MCP server is always constructed). That ticker is an uncontrolled
// background noise source not accounted for by any prior cost model
// (First Instruments A4). Measurement boots that want to isolate the
// ReconcileDaemon's cadence should set this option.
func WithoutLocalHarness() BootOption {
	return func(c *bootConfig) {
		c.withoutLocalHarness = true
	}
}

// Kernel is an opaque handle to a running in-process kernel instance.
// Obtain via Boot; release via Stop.
type Kernel struct {
	cfg               *Config
	server            *Server
	process           *Process
	reconcileDaemon   *ReconcileDaemon
	cancel            context.CancelFunc
	httpEndpoint      string // resolved actual addr, e.g. "http://127.0.0.1:54321"
	serverDone        chan error
	processDone       chan error
	shutdownTelemetry func(context.Context)

	// bepEngine is non-nil only when cluster.enabled=true and the engine
	// started successfully. Dark by default: when cluster.enabled=false this
	// field is nil and no BEP goroutines, listeners, or port binds occur.
	bepEngine *BEPEngine

	// bepProvider is non-nil only when cluster.enabled=true and the provider
	// file watcher started successfully. Stopped alongside bepEngine in Stop().
	bepProvider *BEPProvider
}

// SessionReaperRunning reports whether the server's session TTL reaper is
// running. It must be true for any kernel that is serving (cogos#423).
func (k *Kernel) SessionReaperRunning() bool {
	return k.server != nil && k.server.backgroundRunning()
}

// Endpoint returns the base URL of the kernel's HTTP server,
// e.g. "http://127.0.0.1:54321".
func (k *Kernel) Endpoint() string {
	return k.httpEndpoint
}

// WorkspaceRoot returns the workspace root path in use.
func (k *Kernel) WorkspaceRoot() string {
	return k.cfg.WorkspaceRoot
}

// ReconcileDaemon returns the kernel's ReconcileDaemon, exposing Trigger and
// State for integration tests that need to drive or observe reconcile cycles.
// The daemon is already running when Boot returns.
func (k *Kernel) ReconcileDaemon() *ReconcileDaemon {
	return k.reconcileDaemon
}

// Process returns the kernel's continuous Process, exposing read-only
// observation surfaces (e.g. State()) to integration tests and measurement
// harnesses (First Instruments A7). No mutation surface is exposed here —
// callers get the same *Process the kernel itself runs, but this accessor
// is intended for read-only accessors such as State().
func (k *Kernel) Process() *Process {
	return k.process
}

// BEPEngine returns the running BEPEngine handle, or nil when
// cluster.enabled=false (the default). Callers that need to inspect peer
// status or inject test events can use this handle; nil means the cluster
// subsystem is dark and no BEP activity of any kind is occurring.
func (k *Kernel) BEPEngine() *BEPEngine {
	return k.bepEngine
}

// Stop cancels the kernel's context and waits for all goroutines to exit
// within a 10-second deadline. Safe to call multiple times (idempotent after
// first call; subsequent calls return nil immediately).
func (k *Kernel) Stop() error {
	k.cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := k.server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("kernel stop: server shutdown error", "err", err)
	}

	// Wait for the process goroutine.
	select {
	case <-k.processDone:
	case <-shutdownCtx.Done():
		slog.Warn("kernel stop: process did not stop in time")
	}

	if k.shutdownTelemetry != nil {
		k.shutdownTelemetry(context.Background())
	}

	// Shut down BEP subsystem if it was started (cluster.enabled=true path).
	// Stop provider first (halts file-watcher goroutines) then engine
	// (closes peer connections and drains message goroutines).
	if k.bepProvider != nil {
		k.bepProvider.Stop()
	}
	if k.bepEngine != nil {
		k.bepEngine.Stop()
	}

	return nil
}

// Boot starts a kernel in-process. It loads the nucleus, builds all kernel
// subsystems, starts the HTTP server on a pre-allocated listener, and returns
// a *Kernel handle once the server is accepting connections.
//
// The kernel runs until ctx is cancelled or Stop is called.
//
// cfg must be a fully resolved *Config (LoadConfig + any flag overrides already
// applied by the caller). Boot does not call os.Exit; any failure returns error.
//
// RegisterProviders / SetProvidersWorkspace are expected to have been called
// (or intentionally left nil) by the caller before Boot.
func Boot(ctx context.Context, cfg *Config, opts ...BootOption) (*Kernel, error) {
	bootCfg := applyBootOptions(opts)

	// L5-HTTP-AUTHZ: warn loudly (never block) if the HTTP surface is about
	// to bind non-loopback with no auth token configured. See
	// boot_bindaddr_warning.go for the rationale.
	warnIfUnauthenticatedNonLoopback(cfg)

	// Load nucleus (identity core).
	nucleus, err := LoadNucleus(cfg)
	if err != nil {
		return nil, fmt.Errorf("boot: nucleus load failed: %w", err)
	}
	slog.Info("nucleus loaded", "summary", nucleus.Summary())

	// Build the continuous process.
	process := NewProcess(cfg, nucleus)

	// Load TRM model and embedding index (optional — graceful degradation).
	if trm, embIdx := loadTRMAtStartup(cfg); trm != nil && embIdx != nil {
		process.SetTRM(trm, embIdx)
		slog.Info("trm: wired into context assembly pipeline")
	}

	// Build the inference router. Pass the kernel context so the router runs its
	// background availability maintainer (provider readiness is probed off the
	// request hot path) and stops cleanly on shutdown.
	router, err := BuildRouter(cfg, WithRouterContext(ctx))
	if err != nil {
		slog.Warn("router build failed; inference disabled", "err", err)
	}

	// Build the HTTP server.
	server := NewServer(cfg, nucleus, process)
	server.SetRouter(router)
	// Service control (serve_services.go): without this the mutation routes
	// fall back to ObserverSupervisor and every start/stop/restart 409s, so
	// enable_service_control had no effect on a real kernel. On non-darwin
	// NewLaunchctlController is the observer alias, so this is a no-op there.
	server.SetServiceSupervisor(NewLaunchctlController())

	// L5-HTTP-AUTHZ follow-up (board 75): the write-route grant-auth gate
	// (serve_grant_auth.go) is on by default; log loudly if the operator
	// turned it off, same "never block, always be visible in logs" posture
	// as warnIfUnauthenticatedNonLoopback above.
	if cfg.WriteRouteGrantAuthDisabled {
		slog.Warn("SECURITY: kernel HTTP write-route grant authentication is DISABLED " +
			"(disable_write_route_grant_auth: true) — every POST/PUT/PATCH/DELETE route " +
			"and /mcp (including cog_write_cogdoc and every other MCP tool call) accepts " +
			"requests with no X-Cogos-Grant token; this reopens the write-route CSRF gap " +
			"that gate exists to close and should only be set for local development")
	}

	// Mint (or recover) the kernel's own node-root identity grant so local
	// consumers have a zero-paste bootstrap credential for the gate above.
	// Best-effort: a failure here does not block boot — it degrades to "no
	// consumer can satisfy the gate until it mints its own grant via
	// POST /v1/identity/grants", which is the same bootstrap path every
	// other surface already uses.
	if nodeRootGrant, err := ensureNodeRootGrant(server); err != nil {
		slog.Warn("boot: node-root identity grant mint/recover failed; "+
			"local consumers of the write-route grant-auth gate must self-mint via "+
			"POST /v1/identity/grants", "err", err)
	} else {
		slog.Info("boot: node-root identity grant ready", "grant_id", nodeRootGrant.GrantID,
			"expires_at", nodeRootGrant.ExpiresAt.Format(time.RFC3339))
	}

	// G0(b): wire the RBAC harness-binding layer so cog_register_session can
	// create HarnessBindingCRDs for sessions that supply an optional "subject"
	// field. WireHarnessBackend is set by cmd/cogos/providers_wire.go; nil
	// (e.g. in tests or CLI-only builds) is safe — naked-by-default contract.
	if WireHarnessBackend != nil {
		WireHarnessBackend(server)
	}

	// Wire post-boot provider runtime handles (kernel Process for ledger
	// events, BusSessionManager/BusEventBroker for the SSE bus) into any
	// registered Reconcilable that needs them. Set by
	// internal/providers/all.Register; nil (e.g. in tests) is safe — the
	// affected provider(s) degrade to no-emit rather than failing boot.
	if WireProviderRuntime != nil {
		WireProviderRuntime(server)
	}

	// Wire the constellation indexer so CogDocService.WriteAndSync /
	// PatchAndSync perform an eager per-file FTS upsert, and so that the lazy
	// drift-repair path in searchMemoryFTSDriftRepair can call IndexFile without
	// importing sdk/constellation (package-boundary guard).
	// WireConstellationIndexer is set by cmd/cogos/providers_wire.go; nil means
	// eager upsert and drift repair are disabled (safe degraded mode for tests).
	if WireConstellationIndexer != nil {
		WireConstellationIndexer(server)
	}

	// Wire the session-activity publisher.
	if server.busSessions != nil {
		process.SetSessionActivityPublisher(server.busSessions.AppendEvent)
	}

	// Initialize telemetry (traces + metrics). No-op if no collector is available.
	shutdownTelemetry := initTelemetry(ctx)

	// Derived context the kernel owns; caller's ctx cancellation also stops it.
	kernelCtx, cancel := context.WithCancel(ctx)

	// Start the node-root grant's TTL renewal ticker (board-task-60 followup):
	// ensureNodeRootGrant above mints/recovers via the package-default 30-day
	// TTL and nothing renews it on its own, so a kernel with >30d uptime would
	// otherwise start 401ing every local consumer of the write-route
	// grant-auth gate until restart. Runs an immediate threshold-based check
	// before ever waiting on its own ticker, so a restart that recovers a
	// grant close to its existing expiry is caught right away instead of
	// waiting up to a full renewal interval (round 4 cold-start fix — see
	// startNodeRootGrantRenewal's doc comment). Owned by kernelCtx so it
	// stops with every other kernel goroutine on Stop(); started
	// unconditionally (not gated on the ensureNodeRootGrant error above)
	// since maybeRenewNodeRootGrant degrades safely to a fresh mint/recover
	// attempt on its own if no grant exists yet.
	startNodeRootGrantRenewal(kernelCtx, server)

	// Wire LocalHarnessController if an MCP server is present, unless the
	// caller opted out via WithoutLocalHarness (First Instruments A4 — this
	// controller runs its own ticker, independent of the ReconcileDaemon, and
	// measurement boots that need to isolate reconcile cadence skip it).
	if server.mcpServer != nil && !bootCfg.withoutLocalHarness {
		ctrl, err := NewLocalHarnessController(cfg, nucleus, process, server.mcpServer)
		if err != nil {
			slog.Warn("local harness disabled", "err", err)
		} else {
			ctrl.SetBusSessionManager(server.busSessions)
			server.SetAgentController(ctrl)
			ctrl.Start(kernelCtx)
			slog.Info("local harness started", "agent_id", DefaultAgentID, "interval", ctrl.interval.String())
		}
	}

	// ADR-092 §2 step 4: Reconcile loop start.
	// When WithIsolatedRegistry was passed (testkernel path), bootCfg.providers
	// is non-nil and the daemon bypasses the global registry.
	// bootCfg.pollInterval (set via WithPollInterval) is forwarded to
	// ReconcileDaemonConfig.PollInterval; zero leaves the daemon's own default
	// (30s, applied by withDefaults()) in effect (First Instruments A1).
	reconcileDaemon := NewReconcileDaemon(ReconcileDaemonConfig{
		WorkspaceRoot: cfg.WorkspaceRoot,
		Providers:     bootCfg.providers,
		PollInterval:  bootCfg.pollInterval,
	})
	reconcileDaemon.Start(kernelCtx)
	server.SetReconcileDaemon(reconcileDaemon)

	// Wire ProjectionWatcher as an early-trigger source.
	for _, kind := range AllProjectionKinds {
		kind := kind
		nodesDir := cfg.WorkspaceRoot + "/.cog/mem/semantic/lineage/nodes"
		providerType := "lineage-projection-" + string(kind)
		watcher := NewProjectionWatcher(nodesDir, func(watchCtx context.Context) error {
			reconcileDaemon.Trigger(providerType)
			return nil
		}, 0)
		if err := watcher.Start(kernelCtx); err != nil {
			slog.Debug("projection watcher skipped (nodes dir not present)",
				"kind", kind, "err", err)
		} else {
			slog.Info("projection watcher started", "kind", kind, "nodes_dir", nodesDir)
		}
	}

	// Wire the decision-lineage watcher. It reads a different corpus than the
	// theoretical-lineage projections (the ADR/RFC decision records), so it
	// watches the architecture dirs and re-projects the spine manifold on any
	// decision change.
	{
		decisionProviderType := "lineage-projection-" + string(ProjectionDecisionLineage)
		for _, corpusDir := range DecisionCorpusDirs(cfg.WorkspaceRoot) {
			watcher := NewProjectionWatcher(corpusDir, func(watchCtx context.Context) error {
				reconcileDaemon.Trigger(decisionProviderType)
				return nil
			}, 0)
			if err := watcher.Start(kernelCtx); err != nil {
				slog.Debug("decision-lineage watcher skipped (corpus dir not present)",
					"dir", corpusDir, "err", err)
			} else {
				slog.Info("decision-lineage watcher started", "corpus_dir", corpusDir)
			}
		}
	}

	// Start the mem-currency watcher so FTS stays current when .cog.md files
	// are written outside the MCP write path (e.g. by `cogos ingest`, direct
	// editor saves, or CI automation).  The watcher also adds newly created
	// subdirectories of .cog/mem/ to the fsnotify watch set so that new memory
	// sector dirs are watched immediately on macOS kqueue (which is
	// non-recursive).
	//
	// Only started when a ConstellationIndexer has been wired (i.e. after
	// WireConstellationIndexer ran above).  In tests and CLI paths where
	// pkgFTSRepairIndexer is nil, this block is a no-op.
	if pkgFTSRepairIndexer != nil {
		// watchDirs covers .cog/mem (always) plus any workspace-root cogdoc
		// directories declared via .cog/config/cogdocs.yaml requiredPaths
		// (e.g. architecture/) — the same roots IndexWorkspace's batch walk
		// now covers per the widened walkRoots(). Without also watching
		// these here, a live daemon's incremental indexing would fall out of
		// sync with what a full reindex sees: edits under a widened root
		// would be invisible to search/FTS until someone manually triggers a
		// rebuild.
		//
		// Resolved via ExtraCogdocRootsFunc rather than an sdk/constellation
		// import: Boot is on the long-lived daemon path, which must stay free
		// of that import per the package-boundary guard (cogdoc_service.go,
		// cli_reindex.go). Nil func means degraded mode (only .cog/mem is
		// live-watched); a manual `cogos reindex` still covers widened roots.
		watchDirs := []string{filepath.Join(cfg.WorkspaceRoot, ".cog", "mem")}
		if ExtraCogdocRootsFunc != nil {
			watchDirs = append(watchDirs, ExtraCogdocRootsFunc(cfg.WorkspaceRoot)...)
		}

		for _, dir := range watchDirs {
			mw := NewMemWatcher(dir, pkgFTSRepairIndexer)
			if err := mw.Start(); err != nil {
				slog.Debug("mem_watcher: not started (dir absent or fsnotify unavailable)",
					"dir", dir, "err", err)
				continue
			}
			slog.Info("mem_watcher: started", "dir", dir)
			// Stop the watcher when the kernel context is cancelled.
			go func(w *MemWatcher) {
				<-kernelCtx.Done()
				w.Stop()
			}(mw)
		}
	}

	// Pre-allocate the listener so we know the actual port before Start returns.
	// This is necessary when cfg.Port == 0 (OS-assigned ephemeral port) so
	// that Endpoint() can return a valid URL immediately after Boot.
	listenAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.Port)
	if cfg.BindAddr == "" {
		listenAddr = fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("boot: listen %s: %w", listenAddr, err)
	}

	// Derive the actual endpoint URL from the listener.
	actualAddr := ln.Addr().(*net.TCPAddr)
	bindHost := cfg.BindAddr
	if bindHost == "" {
		bindHost = "127.0.0.1"
	}
	httpEndpoint := fmt.Sprintf("http://%s:%d", bindHost, actualAddr.Port)

	// Start process goroutine.
	processDone := make(chan error, 1)
	go func() {
		processDone <- process.Run(kernelCtx)
	}()

	// Start HTTP server goroutine using the pre-allocated listener.
	serverDone := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); err != nil {
			serverDone <- err
		}
	}()

	// ── BEP cluster engine (Phase 2 S2) ─────────────────────────────────────
	// Dark by default: only started when cluster.enabled=true.
	// This mirrors the IdentityNakedDefault dark-flag pattern: with the
	// shipped default (enabled=false) there is ZERO observable difference
	// from the pre-S2 binary — no listener, no goroutines, no port bind.
	var bepEngineHandle *BEPEngine
	var bepProviderHandle *BEPProvider
	{
		provider := NewBEPProvider(cfg.WorkspaceRoot)
		clusterCfg, cfgErr := provider.LoadConfig()
		if cfgErr != nil {
			slog.Warn("cluster: failed to load cluster.yaml; BEP engine not started",
				"err", cfgErr)
		} else if clusterCfg.Enabled {
			eng, engErr := NewBEPEngine(cfg.WorkspaceRoot, clusterCfg, provider)
			if engErr != nil {
				// Missing cert or other construction failure: log clearly and
				// continue booting. The kernel is still fully functional without BEP.
				slog.Error("cluster: BEPEngine construction failed; cluster transport disabled",
					"err", engErr)
			} else {
				// Wire SyncEvents to the kernel's event bus so that peer
				// connect/disconnect and file-sync events land in the ledger.
				if server.busSessions != nil {
					eng.SetEventCallback(func(evt bep.SyncEvent) {
						_, _ = server.busSessions.AppendEvent(
							"bus_cluster",
							"cluster.sync."+evt.Type,
							"bep-engine",
							map[string]interface{}{
								"summary":   evt.Summary,
								"timestamp": evt.Timestamp,
							},
						)
					})
				}
				if startErr := eng.Start(); startErr != nil {
					// Start failure (e.g. port already in use): log and continue.
					slog.Error("cluster: BEPEngine.Start failed; cluster transport disabled",
						"err", startErr)
				} else {
					// Phase 2 S3: wire the provider's file-watcher so that local
					// CRD changes propagate to peers automatically.
					//
					// Order matters:
					//   1. Register the change handler first so no events are lost
					//      during the window between handler registration and watcher
					//      start (fsnotify buffers events; the provider flushes them
					//      after the watcher goroutine starts).
					//   2. Start the file watcher (creates the dir if absent,
					//      initialises fsnotify or falls back to polling).
					//
					// The provider is stopped in Kernel.Stop() via eng.Stop →
					// bepEngine reference; provider lifecycle is tied to the engine.
					provider.AddChangeHandler(eng.NotifyLocalChange)
					if watchErr := provider.Start(); watchErr != nil {
						slog.Warn("cluster: BEPProvider.Start failed; local CRD changes will not propagate",
							"err", watchErr)
					} else {
						bepProviderHandle = provider
						slog.Info("cluster: BEP provider watcher started",
							"watch_dir", provider.WatchDir())
					}

					bepEngineHandle = eng
					server.bepEngine = eng

					// Phase 2 S4: wire the cluster dispatch router into the
					// MCP surface so cog_dispatch_to_harness(target_node=...)
					// routes over BEP. Also wire the local dispatcher into the
					// engine so it can serve incoming MessageTypeDispatch from
					// remote peers.
					server.SetClusterRouter(eng)
					if ctrl, ok := server.agentController.(AgentDispatcher); ok {
						eng.SetDispatcher(ctrl)
					}

					slog.Info("cluster: BEP engine started",
						"listen_port", clusterCfg.ListenPort,
						"peers", len(clusterCfg.Peers),
					)
				}
			}
		} else {
			slog.Debug("cluster: cluster.enabled=false; BEP engine not started (dark by default)")
		}
	}

	k := &Kernel{
		cfg:               cfg,
		server:            server,
		process:           process,
		reconcileDaemon:   reconcileDaemon,
		cancel:            cancel,
		httpEndpoint:      httpEndpoint,
		serverDone:        serverDone,
		processDone:       processDone,
		shutdownTelemetry: shutdownTelemetry,
		bepEngine:         bepEngineHandle,
		bepProvider:       bepProviderHandle,
	}

	slog.Info("kernel booted", "endpoint", httpEndpoint, "workspace", cfg.WorkspaceRoot)
	return k, nil
}
