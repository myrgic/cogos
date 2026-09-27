// Package all wires all production provider and MCP-extension registrations
// used by the kernel daemon (cmd/cogos).
//
// Motivation (ADR-101 test-gap C):
//
//	Before this package existed the wiring lived exclusively in cmd/cogos
//	(a package main, not importable by test helpers outside that directory).
//	Extracting it here allows testkernel-based tests to apply the same
//	production registration set that the daemon boots with, closing the
//	"cmd/cogos assembly" gap where a dropped registration goes undetected
//	until a live kernel is interrogated.
//
// Design rules:
//   - No package-level init().  The two functions below are pure setters;
//     callers (cmd/cogos init() blocks) decide when to invoke them.
//   - No package-level state beyond the helpers shared by the two functions.
//     The eval provider and harness registry are owned by the caller and
//     passed in; conversations provider is similarly owned by the caller.
//   - cmd/cogos stays as the owner of package-level provider vars so that
//     the existing cmd/cogos test (z_conversations_wire_test.go) can reach
//     them without indirection.
//
// Usage in cmd/cogos:
//
//	// providers_wire.go init():
//	all.Register(daemonEvalProvider, daemonHarnessRegistry)
//
//	// z_conversations_wire.go init():
//	all.RegisterConversations(daemonConversationsProvider)
//
// Usage in tests:
//
//	// Reset engine globals, apply production wiring, boot kernel.
//	all.Register(eval.New(nil, nil), subidentity.NewHarnessRegistry())
//	all.RegisterConversations(conversations.NewProvider())
//	k, _ := testkernel.Boot(ctx, t)
package all

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/myrgic/cogos/internal/conversations"
	"github.com/myrgic/cogos/internal/engine"
	"github.com/myrgic/cogos/internal/eval"
	"github.com/myrgic/cogos/internal/providers/component"
	"github.com/myrgic/cogos/internal/providers/daemon"
	"github.com/myrgic/cogos/internal/providers/marginbridge" // registers "margin-bridge" with pkg/reconcile
	_ "github.com/myrgic/cogos/internal/providers/site"       // registers "site" with pkg/reconcile
	"github.com/myrgic/cogos/internal/providers/vitalsretention"
	"github.com/myrgic/cogos/internal/workspace"
	subidentity "github.com/myrgic/cogos/pkg/substrate/identity"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
	"gopkg.in/yaml.v3"
)

// ── workspace helpers (duplicated from cmd/cogos per ADR-085 rule 7) ─────────

// globalConfig is the minimal shape of ~/.cog/node/global.yaml used by
// workspace resolution.
type globalConfig struct {
	CurrentWorkspace string                     `yaml:"current-workspace,omitempty"`
	Workspaces       map[string]*workspaceEntry `yaml:"workspaces,omitempty"`
}

type workspaceEntry struct {
	Path string `yaml:"path"`
}

type configProvider struct{ cfg *globalConfig }

func (p configProvider) CurrentWorkspace() string { return p.cfg.CurrentWorkspace }
func (p configProvider) WorkspacePath(name string) (string, bool) {
	e, ok := p.cfg.Workspaces[name]
	if !ok || e == nil {
		return "", false
	}
	return e.Path, ok
}

// loadGlobalConfig reads ~/.cog/node/global.yaml with the legacy fallback.
// Returns empty config on ENOENT.
func loadGlobalConfig() (workspace.ConfigProvider, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return configProvider{cfg: &globalConfig{Workspaces: make(map[string]*workspaceEntry)}}, nil
	}
	newPath := filepath.Join(home, ".cog", "node", "global.yaml")
	oldPath := filepath.Join(home, ".cog", "config")

	path := newPath
	if info, statErr := os.Stat(newPath); os.IsNotExist(statErr) || (statErr == nil && info.IsDir()) {
		if info2, statErr2 := os.Stat(oldPath); statErr2 == nil && !info2.IsDir() {
			path = oldPath
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return configProvider{cfg: &globalConfig{Workspaces: make(map[string]*workspaceEntry)}}, nil
		}
		return nil, fmt.Errorf("providers/all: read global config: %w", err)
	}

	var cfg globalConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("providers/all: parse global config: %w", err)
	}
	if cfg.Workspaces == nil {
		cfg.Workspaces = make(map[string]*workspaceEntry)
	}
	return configProvider{cfg: &cfg}, nil
}

// gitRoot returns the top-level git repository containing the process
// working directory.
func gitRoot() (string, error) {
	var out bytes.Buffer
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("not a git repository")
	}
	return strings.TrimSpace(out.String()), nil
}

// uriRegistryLocator adapts engine.ResolveWorkspacePath to the pin.WorkspaceLocator
// interface without exporting engine's internal resolver type.
type uriRegistryLocator struct{}

func (a *uriRegistryLocator) LocateWorkspace(ctx context.Context, name string) (string, error) {
	return engine.ResolveWorkspacePath(ctx, name)
}

// ── Registration ──────────────────────────────────────────────────────────────

// Register sets the four engine hook variables that the kernel daemon uses
// at boot:
//
//   - engine.RegisterProviders — reconcile provider registry + ProjectionCompiler
//   - engine.SetProvidersWorkspace — workspace root injection into providers
//   - engine.RegisterMCPExtensions — eval MCP tools (initial chain)
//   - engine.WireHarnessBackend — RBAC harness-binding layer
//
// Also wires workspace.LoadConfig and workspace.GitRoot so that
// workspace.ResolveWorkspace() can resolve via the global registry and git root
// detection in addition to COG_ROOT.
//
// Register replaces (not chains) any previously set values for these four
// hooks. Call RegisterConversations after Register to add the conversations
// layer on top.
//
// Callers own evalProvider and harnessRegistry. Pass eval.New(nil, nil) and
// subidentity.NewHarnessRegistry() for production defaults.
func Register(evalProvider *eval.EvalProvider, harnessRegistry *subidentity.HarnessRegistry) {
	// Wire workspace resolution DI seams.
	workspace.LoadConfig = func() (workspace.ConfigProvider, error) {
		return loadGlobalConfig()
	}
	workspace.GitRoot = gitRoot

	// The named import of internal/providers/daemon above triggered daemon.init()
	// (and component.init() via daemon's blank import), registering all 10
	// production providers with pkg/reconcile.
	engine.RegisterProviders = func() {
		// providers already registered by internal/providers/daemon init()
		engine.RegisterProjectionCompiler()
	}

	engine.SetProvidersWorkspace = func(workspaceRoot string) {
		daemon.SetWorkspaceRoot(workspaceRoot)
		component.SetWorkspaceRoot(workspaceRoot)
		// RFC-040 S2: give the vitals-retention recorder its workspace root
		// so Health()/Window() and the bus-handler wiring below (see
		// WireProviderRuntime) all resolve the same .cog/observatory/vitals
		// base, in both the daemon and `cog reconcile`/`cog health` CLI paths.
		vitalsretention.SetWorkspaceRoot(workspaceRoot)
		if workspaceRoot != "" {
			engine.RegisterWorktreeReconciler(workspaceRoot, nil, nil, nil)
		}
		daemon.SetPinWorkspaceLocator(&uriRegistryLocator{})
		if workspaceRoot != "" && evalProvider != nil {
			_, _ = evalProvider.LoadConfig(workspaceRoot)
		}
	}

	engine.RegisterMCPExtensions = func(srv *engine.MCPServer) {
		if evalProvider != nil {
			eval.RegisterEvalTools(srv.Server(), evalProvider)
		}
	}

	engine.WireHarnessBackend = func(s *engine.Server) {
		if harnessRegistry != nil {
			s.SetHarnessBackend(harnessRegistry)
		}
	}

	// Wire margin-bridge's kernel-native event delivery (ledger + SSE bus).
	// The provider itself stays decoupled from internal/engine (ADR-085 leaf
	// package discipline); this closure is the one place that bridges the
	// two, using accessor methods on *engine.Server since WireProviderRuntime
	// runs after NewServer, when Process/BusSessions/BusBroker all exist.
	engine.WireProviderRuntime = func(s *engine.Server) {
		p, err := reconcile.GetProvider("margin-bridge")
		if err == nil {
			if mb, ok := p.(*marginbridge.Provider); ok {
				mb.SetEventSink(&kernelEventSink{server: s})
			}
		}

		// RFC-040 S2: subscribe the vitals-retention recorder to
		// bus_kernel_proprio. AddEventHandler dispatches synchronously
		// inside BusSessionManager.AppendEvent, once per autonomic tick —
		// see internal/providers/vitalsretention/recorder.go for the
		// best-effort contract (never blocks, never panics, records
		// failures for Health() instead of returning them).
		if mgr := s.BusSessions(); mgr != nil {
			mgr.AddEventHandler("vitals-retention-recorder", vitalsretention.HandleBusEvent)
		}
	}
}

// kernelEventSink adapts *engine.Server's Process/BusSessionManager/
// BusEventBroker handles to marginbridge.EventSink.
type kernelEventSink struct {
	server *engine.Server
}

// EmitLedgerEvent appends a hash-chained ledger event via the kernel
// Process, visible to cog_read_events/cog_tail_events. Not gated by the
// cog_emit_event MCP tool's closed allowedEventTypes map — that allowlist
// only applies to the MCP-exposed tool, not this internal engine call.
func (k *kernelEventSink) EmitLedgerEvent(eventType string, data map[string]interface{}) error {
	proc := k.server.Process()
	if proc == nil {
		return fmt.Errorf("margin-bridge: no kernel process available")
	}
	return proc.EmitEvent(eventType, data, "margin-bridge")
}

// EmitBusEvent appends to the named bus and publishes it to live SSE
// subscribers. AppendEvent alone does not reach SSE subscribers — the HTTP
// bus-send handler calls busBroker.Publish itself after AppendEvent
// (internal/engine/serve_bus.go), so this adapter must do the same for any
// consumer attached over SSE (Mod3, dashboard) to see the event.
func (k *kernelEventSink) EmitBusEvent(busID, eventType, from string, payload map[string]interface{}) error {
	mgr := k.server.BusSessions()
	if mgr == nil {
		return fmt.Errorf("margin-bridge: no bus session manager available")
	}
	evt, err := mgr.AppendEvent(busID, eventType, from, payload)
	if err != nil {
		return err
	}
	if broker := k.server.BusBroker(); broker != nil {
		broker.Publish(busID, evt)
	}
	return nil
}

// RegisterConversations chains the conversations layer onto the already-set
// engine.RegisterMCPExtensions hook and calls engine.SetConversationsResolver.
//
// Must be called AFTER Register (or after any other code that sets
// engine.RegisterMCPExtensions) so the chain is: eval tools → conversations
// tools, matching the init() ordering in cmd/cogos ("p" before "z").
//
// Also registers the conversations provider with pkg/reconcile so the daemon's
// proprioception Health() block includes it, and wires workspace root injection
// so the provider's index is initialised on engine.SetProvidersWorkspace.
//
// provider must be non-nil; pass conversations.NewProvider() for production.
func RegisterConversations(provider *conversations.Provider) {
	if provider == nil {
		return
	}

	// Register provider with reconcile registry.
	reconcile.RegisterProvider("conversations", provider)

	// Chain workspace root injection.
	prevSetWorkspace := engine.SetProvidersWorkspace
	engine.SetProvidersWorkspace = func(workspaceRoot string) {
		if prevSetWorkspace != nil {
			prevSetWorkspace(workspaceRoot)
		}
		if workspaceRoot != "" {
			_, _ = provider.LoadConfig(workspaceRoot)
		}
	}

	// Chain MCP extension registration.
	prev := engine.RegisterMCPExtensions
	engine.RegisterMCPExtensions = func(srv *engine.MCPServer) {
		if prev != nil {
			prev(srv)
		}
		conversations.RegisterConversationTools(srv.Server(), srv.TrackTool, provider, srv.MaxToolOutputBytes())
	}

	// Wire conversations URI resolver.
	engine.SetConversationsResolver(&conversationsURIResolver{p: provider})

	// Wire the transcript half of GET /memory/search (myrgic/cogos#650).
	engine.SetTranscriptSearcher(&transcriptSearcher{p: provider})

	// Wire GET /v1/observatory/coverage (v0.2 coverage metric surface).
	prevHTTP := engine.RegisterHTTPExtensions
	engine.RegisterHTTPExtensions = func(s *engine.Server, mux *http.ServeMux) {
		if prevHTTP != nil {
			prevHTTP(s, mux)
		}
		s.Route(mux, "GET /v1/observatory/coverage",
			conversations.CoverageHTTPHandler(provider))
	}
}

// conversationsURIResolver adapts conversations.Provider to
// engine.ConversationsResolver.
type conversationsURIResolver struct {
	p *conversations.Provider
}

func (r *conversationsURIResolver) ResolveURI(_ context.Context, uri string) (any, error) {
	return r.p.ResolveURI(uri)
}

// transcriptSearcher adapts conversations.Provider to engine.TranscriptSearcher.
type transcriptSearcher struct {
	p *conversations.Provider
}

func (t *transcriptSearcher) SearchTranscripts(ctx context.Context, query string, limit int) ([]engine.TranscriptHit, error) {
	hits, err := t.p.SearchRecent(ctx, query, time.Time{}, limit)
	if err != nil {
		if errors.Is(err, conversations.ErrIndexNotReady) {
			return nil, fmt.Errorf("%w: %v", engine.ErrTranscriptsUnavailable, err)
		}
		return nil, err
	}
	out := make([]engine.TranscriptHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, engine.TranscriptHit{
			SessionID:    h.SessionID,
			TurnIndex:    h.TurnIndex,
			Source:       h.Source,
			Role:         string(h.Role),
			Timestamp:    h.Timestamp,
			Excerpt:      h.Excerpt,
			SessionTitle: h.SessionTitle,
			URI:          conversations.HitURI(h),
		})
	}
	return out, nil
}
