// Package testkernel provides an in-process kernel boot harness for
// daemon-level integration tests.
//
// ADR-101 Phase 1: thin wrapper over engine.Boot.
// ADR-101 Phase 2: WithIsolatedRegistry injects an explicit provider list so
// tests can exercise real plan/apply without touching the global registry.
// ADR-101 Phase 3: ListTools queries the live MCP surface via the wire
// protocol, enabling binary-assembly tests. CallTool is the Phase-3b follow-up.
// Phase 4 will add goroutine-leak detection.
//
// # Background goroutines started by a testkernel.Boot (First Instruments A5)
//
// engine.Boot starts every one of these unconditionally except where noted.
// Any measurement harness computing cadence/timing off a testkernel.Boot must
// account for ALL of them as potential noise/contention sources, not just the
// ReconcileDaemon:
//
//   - ReconcileDaemon.run — the periodic reconcile loop (this package's main
//     subject; PollInterval-controlled via WithPollInterval, First
//     Instruments A1).
//   - Process.Run — the process's own consolidation/heartbeat select-loop
//     (consolidationTicker + heartbeatTicker), driving emitHeartbeat and the
//     K12-gated consolidation Module E taps. Cadence controlled via
//     WithConsolidationInterval / WithHeartbeatInterval (First Instruments A2).
//   - LocalHarnessController — its own ticker (interval = cfg.HeartbeatInterval
//     seconds, default 1 minute), started whenever server.mcpServer is
//     non-nil (true for every testkernel boot). An uncontrolled background
//     source not accounted for by any prior cost model; skip it with
//     WithoutLocalHarness (First Instruments A4).
//   - ProjectionWatcher, one per AllProjectionKinds — fsnotify-based watchers
//     over .cog/mem/semantic/lineage/nodes that call
//     reconcileDaemon.Trigger(providerType) on change. In a fresh
//     makeMinimalWorkspace() boot the watched directory does not exist, so
//     these typically fail to start (logged at Debug) and are inert.
//   - Decision-lineage ProjectionWatcher(s), one per DecisionCorpusDirs —
//     same watch-then-Trigger shape as above, over the ADR/RFC decision
//     corpus dirs; also typically inert in a minimal test workspace.
//   - MemWatcher (mem-currency / FTS watcher) — only started when a
//     ConstellationIndexer has been wired (pkgFTSRepairIndexer != nil); nil
//     in standard testkernel boots, so this is normally NOT started.
//   - The inference router's background availability maintainer (probes
//     configured providers off the request hot path); started by
//     BuildRouter regardless of BootOption.
//   - OTel telemetry (initTelemetry) — no-op (no goroutine of consequence)
//     when no OTLP collector is configured, which is the default test
//     environment.
//
// See engine.Boot's own doc comment for the authoritative, code-level list;
// this summary exists so a measurement-harness author does not have to
// re-derive it from scratch.
//
// Typical usage:
//
//	func TestFoo(t *testing.T) {
//	    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	    defer cancel()
//
//	    k, err := testkernel.Boot(ctx, t)
//	    if err != nil {
//	        t.Fatalf("testkernel.Boot: %v", err)
//	    }
//	    t.Cleanup(func() {
//	        if err := k.Stop(); err != nil {
//	            t.Errorf("testkernel.Stop: %v", err)
//	        }
//	    })
//
//	    // Probe the HTTP surface.
//	    resp, err := http.Get(k.Endpoint() + "/health")
//	    // ...
//	}
package testkernel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/engine"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// Option is a functional option for Boot.
type Option func(*config)

// config holds resolved options derived from Option values.
type config struct {
	// workspace is the workspace root. Defaults to a t.TempDir() path.
	workspace string

	// port is the HTTP port. Defaults to 0 (OS-assigned ephemeral port).
	port int

	// pollInterval overrides the reconcile daemon tick. 0 = use daemon default.
	pollInterval time.Duration

	// consolidationIntervalSec, when > 0, overrides engine.Config.ConsolidationInterval
	// (seconds) before Boot. 0 = leave LoadConfig's resolved value in place.
	consolidationIntervalSec int
	consolidationIntervalSet bool

	// heartbeatIntervalSec, when > 0, overrides engine.Config.HeartbeatInterval
	// (seconds) before Boot. 0 = leave LoadConfig's resolved value in place.
	heartbeatIntervalSec int
	heartbeatIntervalSet bool

	// withoutLocalHarness, when true, forwards engine.WithoutLocalHarness so
	// the LocalHarnessController goroutine is not started for this boot.
	withoutLocalHarness bool

	// providers, when non-nil, is forwarded to engine.WithIsolatedRegistry so
	// the daemon bypasses the global registry. nil = use global registry.
	providers []reconcile.Reconcilable
}

// WithWorkspace sets the workspace root for the kernel under test.
// Defaults to a temporary directory created at Boot time.
func WithWorkspace(path string) Option {
	return func(c *config) { c.workspace = path }
}

// WithIsolatedRegistry injects an explicit set of Reconcilable providers into
// the kernel's ReconcileDaemon, bypassing the global registry.
//
// Use this in integration tests that need to exercise real plan/apply cycles
// with specific providers without interference from globally-registered stubs.
// Any provider type NOT in the supplied list will never be touched by the
// daemon for the lifetime of this kernel.
//
// This is the ADR-101 Phase 2 test-isolation mechanism.
func WithIsolatedRegistry(providers ...reconcile.Reconcilable) Option {
	return func(c *config) { c.providers = providers }
}

// WithPollInterval overrides the ReconcileDaemon's tick interval for this
// kernel. d <= 0 leaves the daemon default (30s) in effect.
//
// Forwards to engine.WithPollInterval (First Instruments A1). Measurement
// harnesses use a very high value (e.g. 1h) to defeat the natural tick so
// Trigger() is the sole cycle driver, or a very low value to force fast
// natural cadence in a unit test.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.pollInterval = d }
}

// WithConsolidationInterval overrides the consolidation cadence (seconds) for
// this kernel's Process before Boot. sec must be >= 0; 0 means "use the
// engine default" (LoadConfig already resolves ConsolidationInterval to 3600
// before Boot sees it, so 0 here is a no-op override rather than a literal
// zero interval, which would panic time.NewTicker). A value of 1 (or any
// second-scale value) is legal — this is what lets First Instruments realize
// its second-scale cell lattice (First Instruments A2/A6).
func WithConsolidationInterval(sec int) Option {
	return func(c *config) {
		c.consolidationIntervalSec = sec
		c.consolidationIntervalSet = true
	}
}

// WithHeartbeatInterval overrides the heartbeat cadence (seconds) for this
// kernel's Process before Boot. sec must be >= 0; 0 means "use the engine
// default" (60), same reasoning as WithConsolidationInterval (First
// Instruments A2).
func WithHeartbeatInterval(sec int) Option {
	return func(c *config) {
		c.heartbeatIntervalSec = sec
		c.heartbeatIntervalSet = true
	}
}

// WithoutLocalHarness skips starting the kernel's LocalHarnessController
// goroutine for this boot. Forwards to engine.WithoutLocalHarness (First
// Instruments A4) — see that option's doc comment for why this matters to
// cadence measurement: engine.Boot unconditionally starts the controller
// whenever an MCP server is present, which is true for every testkernel
// boot, making it an uncontrolled background noise source unless skipped.
func WithoutLocalHarness() Option {
	return func(c *config) { c.withoutLocalHarness = true }
}

// Kernel is an opaque handle to an in-process kernel instance started by Boot.
// Call Stop when the test finishes; the idiomatic pattern is t.Cleanup.
type Kernel struct {
	kernel   *engine.Kernel
	endpoint string
}

// Endpoint returns the base URL of the kernel's HTTP server,
// e.g. "http://127.0.0.1:54321".
func (k *Kernel) Endpoint() string {
	return k.endpoint
}

// WorkspaceRoot returns the workspace root path used by this kernel.
func (k *Kernel) WorkspaceRoot() string {
	return k.kernel.WorkspaceRoot()
}

// ReconcileDaemon returns the kernel's ReconcileDaemon so tests can call
// Trigger and inspect State without going through the HTTP surface.
func (k *Kernel) ReconcileDaemon() *engine.ReconcileDaemon {
	return k.kernel.ReconcileDaemon()
}

// State returns the current lifecycle state of this kernel's Process
// (StateActive / StateReceptive / StateDormant). Read-only (First
// Instruments A7) — used by measurement harnesses to assert a dormant
// measurement boot is non-Active (H6), since emitHeartbeat early-returns on
// StateActive, suppressing both the M12 heartbeat tap and the K12-gated
// consolidation.
func (k *Kernel) State() engine.ProcessState {
	return k.kernel.Process().State()
}

// LastCycleSerial returns the current monotonic cycle-completion counter for
// providerType (First Instruments A3). See engine.ReconcileDaemon.LastCycleSerial.
func (k *Kernel) LastCycleSerial(providerType string) (int, bool) {
	return k.kernel.ReconcileDaemon().LastCycleSerial(providerType)
}

// ConsolidationEvents returns a read-only snapshot of observed dormant-
// consolidation completions for this kernel's Process (First Instruments
// Module E, M11). See engine.Process.ConsolidationEvents.
func (k *Kernel) ConsolidationEvents() []engine.ConsolidationEvent {
	return k.kernel.Process().ConsolidationEvents()
}

// HeartbeatEvents returns a read-only snapshot of observed heartbeat events
// (past the StateActive gate) for this kernel's Process (First Instruments
// Module E, M12). See engine.Process.HeartbeatEvents.
func (k *Kernel) HeartbeatEvents() []engine.HeartbeatEvent {
	return k.kernel.Process().HeartbeatEvents()
}

// ActiveExpiryObservations returns a read-only snapshot of observed
// active-window-expiry events for this kernel's Process (First Instruments
// Module E, M13). See engine.Process.ActiveExpiryObservations.
func (k *Kernel) ActiveExpiryObservations() []engine.ActiveExpiryObservation {
	return k.kernel.Process().ActiveExpiryObservations()
}

// RecordActiveExpiryObservation lets a test-owned poller of a session's
// IsActive record an M13 observation on this kernel's Process (First
// Instruments Module E). See engine.Process.RecordActiveExpiryObservation.
func (k *Kernel) RecordActiveExpiryObservation(obs engine.ActiveExpiryObservation) {
	k.kernel.Process().RecordActiveExpiryObservation(obs)
}

// WaitForCycle blocks until providerType's cycle-completion counter reaches
// at least minSerial, or ctx is done. Polls LastCycleSerial rather than any
// provider-owned test state, so it works against a real production provider
// under test, not just a fake that tracks its own counters (First
// Instruments A3).
func WaitForCycle(ctx context.Context, k *Kernel, providerType string, minSerial int) error {
	const pollInterval = 5 * time.Millisecond
	for {
		if serial, ok := k.LastCycleSerial(providerType); ok && serial >= minSerial {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("testkernel.WaitForCycle: provider %q did not reach serial %d: %w", providerType, minSerial, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

// Stop cancels the kernel's context and waits for all goroutines to exit.
// Safe to call multiple times.
func (k *Kernel) Stop() error {
	return k.kernel.Stop()
}

// NodeRootGrantToken returns this kernel's boot-minted node-root identity
// grant token, read from the 0600 vault file ~/.cog/vault/node-root-grant
// that ensureNodeRootGrant (boot_node_root_grant.go) writes on every Boot.
// This is the same credential local consumers use to satisfy the write-route
// grant-auth gate (serve_grant_auth.go).
//
// It reads the vault file rather than calling the kernel over HTTP because
// the zero-paste primitive is no longer an unauthenticated GET (ledger L03):
// /v1/identity/* is gated on every method now, so a caller holding no grant
// cannot fetch one over HTTP — which is the point. The vault file is the
// designated bootstrap store for exactly this case: a same-user local
// process reads its first credential off a 0600 file instead of off a
// loopback socket that any process on the host can open.
//
// Returns "" if no vault file exists or it is empty (e.g. ensureNodeRootGrant
// failed at boot, which Boot logs a warning for but does not fail on) —
// callers should treat that the same as "grant-auth is effectively
// unreachable for this process", matching the gate's own fail-open posture
// rather than failing the test outright.
func (k *Kernel) NodeRootGrantToken(ctx context.Context, t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(home, ".cog", "vault", "node-root-grant"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// ListTools performs an MCP initialize→notifications/initialized→tools/list
// sequence over HTTP and returns the sorted list of tool names registered on
// this kernel's MCP server.
//
// This is a Phase-3 helper that exercises the actual MCP wire protocol rather
// than going through internal Go types, so it catches registration gaps that
// only show up on the live surface (the category-C gap that motivated ADR-101).
//
// Every request attaches X-Cogos-Grant using this kernel's own boot-minted
// node-root token (see NodeRootGrantToken) — /mcp is gated on every method by
// serve_grant_auth.go, so ListTools exercises the same bootstrap path a real
// local MCP consumer (Claude Code, THESEUS, ...) would use, rather than
// working around the gate.
//
// CallTool is the natural follow-up (ADR-101 Phase 3b); this minimal addition
// provides the assertion surface needed for TestDaemonWiring without wiring
// the full call path.
func (k *Kernel) ListTools(ctx context.Context, t *testing.T) ([]string, error) {
	t.Helper()

	mcpURL := k.endpoint + "/mcp"
	client := &http.Client{Timeout: 10 * time.Second}
	grantToken := k.NodeRootGrantToken(ctx, t)

	doPost := func(body string, extraHeaders map[string]string) ([]byte, http.Header, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, mcpURL, strings.NewReader(body))
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if grantToken != "" {
			req.Header.Set("X-Cogos-Grant", grantToken)
		}
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return b, resp.Header, err
	}

	// Step 1: initialize — acquire session ID.
	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"testkernel","version":"1"}}}`
	_, initHeaders, err := doPost(initBody, nil)
	if err != nil {
		return nil, fmt.Errorf("ListTools: initialize: %w", err)
	}
	sessionID := initHeaders.Get("Mcp-Session-Id")

	// Step 2: notifications/initialized (fire-and-forget).
	_, _, _ = doPost(`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		map[string]string{"Mcp-Session-Id": sessionID})

	// Step 3: tools/list.
	listBody := `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
	listResp, _, err := doPost(listBody, map[string]string{"Mcp-Session-Id": sessionID})
	if err != nil {
		return nil, fmt.Errorf("ListTools: tools/list: %w", err)
	}

	// The MCP Streamable HTTP transport may wrap the JSON-RPC response as an
	// SSE event (Content-Type: text/event-stream):
	//
	//   event: message\ndata: {...}\n\n
	//
	// Strip SSE framing so we parse the raw JSON-RPC payload regardless of
	// whether the server chose SSE or plain JSON encoding.
	jsonPayload := extractSSEData(listResp)

	// Parse the JSON-RPC response.
	var rpc struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(jsonPayload, &rpc); err != nil {
		return nil, fmt.Errorf("ListTools: decode response: %w (body: %s)", err, listResp)
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("ListTools: JSON-RPC error: %s", rpc.Error.Message)
	}

	names := make([]string, 0, len(rpc.Result.Tools))
	for _, tool := range rpc.Result.Tools {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	return names, nil
}

// extractSSEData returns the JSON payload from an SSE-framed body.
// If the body begins with "event:" or "data:" lines, it extracts and
// concatenates all "data: ..." lines.  If the body looks like plain JSON
// (starts with '{') it is returned unchanged.  This lets ListTools work
// whether the server chose SSE or plain JSON encoding.
func extractSSEData(body []byte) []byte {
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return body // already plain JSON
	}
	// SSE: scan for "data: ..." lines and concatenate.
	var dataLines []string
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		}
	}
	if len(dataLines) == 0 {
		return body // can't parse; return original for error reporting
	}
	return []byte(strings.Join(dataLines, ""))
}

// Boot starts a kernel in-process with the given options.
// It creates an isolated temporary workspace (unless WithWorkspace is given),
// boots the kernel via engine.Boot, and blocks until the HTTP /health endpoint
// responds 200 or ctx expires.
//
// Boot does NOT call RegisterProviders or SetProvidersWorkspace. Tests that
// need real providers should pass WithIsolatedRegistry (Phase 2) rather than
// touching the global registry.
//
// HOME isolation: engine.Boot mints/recovers a node-root identity grant at
// startup (boot_node_root_grant.go's ensureNodeRootGrant), which reads and
// writes ~/.cog/vault/node-root-grant via os.UserHomeDir(). Left
// unisolated, every test that boots a kernel overwrites the operator's real
// vault file with a test-minted token — desyncing it from whatever live
// kernel is running on the host (incident: 2026-09-24 23:02, every local
// kernel write 401'd until restart). Boot therefore points HOME (and
// USERPROFILE, for os.UserHomeDir's Windows path) at a fresh t.TempDir()
// before calling engine.Boot, via t.Setenv — which is process-global but
// test-scoped and auto-restored on test end. This is safe here because no
// caller of testkernel.Boot runs in parallel with another (see
// first_instruments_a_test.go's "no t.Parallel()" convention); t.Setenv
// itself panics if a parallel test tries this, which is exactly the signal
// a future parallel test would need to add its own isolation instead.
func Boot(ctx context.Context, t *testing.T, opts ...Option) (*Kernel, error) {
	t.Helper()

	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)

	cfg := &config{port: 0}
	for _, o := range opts {
		o(cfg)
	}

	// Create a minimal workspace if none was provided.
	workspace := cfg.workspace
	if workspace == "" {
		workspace = makeMinimalWorkspace(t)
	}

	engineCfg, err := engine.LoadConfig(workspace, cfg.port)
	if err != nil {
		return nil, fmt.Errorf("testkernel.Boot: load config: %w", err)
	}
	// In test paths, always use port 0 (OS-assigned ephemeral port) unless
	// the caller explicitly set a port via WithPort.
	// LoadConfig with port==0 leaves the default 6931; we override explicitly
	// here to avoid colliding with a running production daemon.
	if cfg.port == 0 {
		engineCfg.Port = 0
	}
	// Ensure loopback bind so the test can reach the server.
	if engineCfg.BindAddr == "" {
		engineCfg.BindAddr = "127.0.0.1"
	}
	// First Instruments A2: override the process's cadence config in seconds
	// before Boot, so second-scale cell-lattice values take effect. 0 means
	// "use the default" (LoadConfig already resolved ConsolidationInterval to
	// 3600 and HeartbeatInterval to 60 before we get here, so a literal 0
	// override would disable the ticker entirely, not select a default —
	// only apply the override when a positive value was explicitly set).
	if cfg.consolidationIntervalSet && cfg.consolidationIntervalSec > 0 {
		engineCfg.ConsolidationInterval = cfg.consolidationIntervalSec
	}
	if cfg.heartbeatIntervalSet && cfg.heartbeatIntervalSec > 0 {
		engineCfg.HeartbeatInterval = cfg.heartbeatIntervalSec
	}

	// Build engine BootOptions from testkernel config.
	var bootOpts []engine.BootOption
	if cfg.providers != nil {
		bootOpts = append(bootOpts, engine.WithIsolatedRegistry(cfg.providers...))
	}
	if cfg.pollInterval > 0 {
		bootOpts = append(bootOpts, engine.WithPollInterval(cfg.pollInterval))
	}
	if cfg.withoutLocalHarness {
		bootOpts = append(bootOpts, engine.WithoutLocalHarness())
	}

	k, err := engine.Boot(ctx, engineCfg, bootOpts...)
	if err != nil {
		return nil, fmt.Errorf("testkernel.Boot: engine.Boot: %w", err)
	}

	// Wait for the HTTP server to be accepting connections.
	if err := waitForHealth(ctx, k.Endpoint()); err != nil {
		_ = k.Stop()
		return nil, fmt.Errorf("testkernel.Boot: health check timeout: %w", err)
	}

	return &Kernel{kernel: k, endpoint: k.Endpoint()}, nil
}

// waitForHealth polls GET <endpoint>/health until a 200 is received or ctx
// expires. Uses a 250 ms poll interval. The server goroutine is already
// running inside engine.Boot, so the window is typically < 50 ms.
func waitForHealth(ctx context.Context, endpoint string) error {
	healthURL := endpoint + "/health"
	client := &http.Client{Timeout: 500 * time.Millisecond}

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// makeMinimalWorkspace creates the directory structure required by LoadNucleus.
// Mirrors the structure created by makeWorkspace in testhelper_test.go, but
// lives in an exported package so integration tests outside internal/engine can
// use it.
func makeMinimalWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	dirs := []string{
		filepath.Join(root, ".cog", "config"),
		filepath.Join(root, ".cog", "mem", "semantic"),
		filepath.Join(root, ".cog", "ledger"),
		filepath.Join(root, "projects", "cog_lab_package", "identities"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("testkernel: makeMinimalWorkspace: mkdir %s: %v", d, err)
		}
	}

	identity := "# Test\nRole: test-kernel\n"
	identityFile := filepath.Join(root, "projects", "cog_lab_package", "identities", "identity_test.md")
	if err := os.WriteFile(identityFile, []byte(identity), 0644); err != nil {
		t.Fatalf("testkernel: write identity: %v", err)
	}

	idCfg := "default_identity: test\nidentity_directory: projects/cog_lab_package/identities\n"
	if err := os.WriteFile(filepath.Join(root, ".cog", "config", "identity.yaml"), []byte(idCfg), 0644); err != nil {
		t.Fatalf("testkernel: write identity.yaml: %v", err)
	}

	return root
}
