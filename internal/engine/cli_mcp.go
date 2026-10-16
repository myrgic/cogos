// cli_mcp.go — `cogos mcp serve` subcommand: run the engine MCPServer on stdio.
//
// Historical note: this implementation once existed alongside a root-package
// `cmdMCP` (mcp.go) with its own hand-rolled JSON-RPC loop and a 4-tool
// catalogue. That root package (and its serveServer type) was fully removed
// by the ADR-121 consolidation (myrgic/cogos#464, commit 6fcdd2a); cmd/cogos/
// has been the sole binary and build target since. Any comment or comparison
// referencing "root" describes deleted code, not a live alternate path.
//
// Transport: stdio, newline-delimited JSON-RPC 2.0, via the upstream
// modelcontextprotocol/go-sdk StdioTransport (server.RunStdio below).
//
// Tool catalogue parity (myrgic/cogos#422): the HTTP transport
// (registerMCPRoutes in serve_mcp.go) and this stdio entrypoint must expose
// the identical tool surface. Both build an MCPServer via NewMCPServer /
// NewMCPServerWithAgentController and then MUST call ApplyExtensions
// (providers_register.go) before serving — that single call registers the
// conversations/eval extension families and refreshes the derived
// schema/toolDefs caches. Before #422's fix this entrypoint skipped that
// call entirely, exposing 14 of the full 21 tools; see newStdioMCPServer.
//
// Note: root's old --bridge flag (external-tool proxy) has no equivalent
// here; passing --bridge to this subcommand fails flag parsing.
//
// Lifecycle:
//
//	cogos mcp serve [--workspace PATH]
//
// Workspace resolution mirrors other engine subcommands (auto-detected via
// findWorkspaceRoot when --workspace is absent). The server runs until the
// client closes stdin (EOF) or SIGINT/SIGTERM is received, then returns 0.
package engine

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// runMCPCmd dispatches `cogos mcp <subcommand>` arguments. Today only
// "serve" is supported; other subcommands return an explicit error so
// typos don't silently become no-ops.
//
// Returns an exit code (0 on clean shutdown). The caller in Main() is
// responsible for calling os.Exit.
func runMCPCmd(args []string, defaultWorkspace string) int {
	return runMCPCmdWithIO(args, defaultWorkspace, os.Stderr)
}

// runMCPCmdWithIO is the testable core. stderr defaults to os.Stderr when
// nil is passed. stdin/stdout are always the process streams because the
// SDK's StdioTransport binds to them directly; for unit tests we inject a
// transport via runMCPServeWithTransport below.
func runMCPCmdWithIO(args []string, defaultWorkspace string, stderr io.Writer) int {
	if stderr == nil {
		stderr = os.Stderr
	}

	if len(args) == 0 {
		fmt.Fprintln(stderr, "Usage: cogos mcp serve [--workspace PATH]")
		return 1
	}

	switch args[0] {
	case "serve":
		return runMCPServeEngine(args[1:], defaultWorkspace, stderr)
	default:
		fmt.Fprintf(stderr, "Unknown mcp subcommand: %s\n", args[0])
		return 1
	}
}

// newStdioMCPServer builds the MCPServer used by `cogos mcp serve`: session
// backends wired the same way the HTTP daemon wires them (serve_mcp.go's
// registerMCPRoutes), plus the shared ApplyExtensions step so stdio exposes
// the identical tool surface as HTTP (myrgic/cogos#422 — stdio previously
// skipped this, exposing 14 of 21 tools). Split out from runMCPServeEngine so
// tests can exercise construction + registration over an in-memory transport
// without binding to a real stdio pipe.
func newStdioMCPServer(cfg *Config, nucleus *Nucleus, process *Process) *MCPServer {
	server := NewMCPServer(cfg, nucleus, process)

	// Wire session-management backends so cog_register_session /
	// cog_list_sessions / cog_offer_handoff / cog_list_handoffs / etc. work
	// over stdio (same registries the HTTP path uses, just no live Server).
	//
	// forkRegistry mirrors serve.go's wiring (SetForkRegistry) so
	// cog_fork_session over stdio also gets a live lineage index — without
	// this, the stdio MCP path silently skipped fork-registry updates
	// entirely (m.forkRegistry stayed nil; see the nil-safe check in
	// mcp_fork_session.go). ReplaySessionRegistry's session.fork case and
	// ReplayForkRegistry both read the durable session.fork bus events so a
	// restart of this process reconstructs prior forks instead of starting
	// blank.
	busSessions := NewBusSessionManager(cfg.WorkspaceRoot)
	sessionRegistry := NewSessionRegistry()
	handoffRegistry := NewHandoffRegistry()
	forkRegistry := NewForkRegistry()
	_ = ReplaySessionRegistry(busSessions, sessionRegistry)
	_ = ReplayHandoffRegistry(busSessions, handoffRegistry)
	_ = ReplayForkRegistry(busSessions, forkRegistry)
	server.SetSessionsBackend(busSessions, sessionRegistry, handoffRegistry)
	server.SetForkRegistry(forkRegistry)

	// Apply any extension hooks registered by workspace-root wiring (e.g.
	// cmd/cogos/providers_wire.go's init() chaining eval + conversations onto
	// RegisterMCPExtensions) so stdio's tools/list matches HTTP's.
	server.ApplyExtensions()

	return server
}

// runMCPServeEngine parses `cogos mcp serve` flags and hands the mcp.Server
// off to StdioTransport. Intentionally does NOT call os.Exit so tests can
// assert on the returned code. On any initialization failure a short error
// goes to stderr and we return 1.
func runMCPServeEngine(args []string, defaultWorkspace string, stderr io.Writer) int {
	fs := flag.NewFlagSet("mcp serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	// Resolve workspace. Mirror the emit cmd's behavior — defer to
	// findWorkspaceRoot from cwd when the flag is empty.
	wsRoot := *workspace
	if wsRoot == "" {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(stderr, "error: getwd: %v\n", err)
			return 1
		}
		ws, err := findWorkspaceRoot(wd)
		if err != nil {
			fmt.Fprintf(stderr, "error: could not detect workspace: %v\n", err)
			return 1
		}
		wsRoot = ws
	}

	cfg, err := LoadConfig(wsRoot, 0)
	if err != nil {
		fmt.Fprintf(stderr, "error: load config: %v\n", err)
		return 1
	}

	nucleus, err := LoadNucleus(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "error: load nucleus: %v\n", err)
		return 1
	}

	process := NewProcess(cfg, nucleus)
	server := newStdioMCPServer(cfg, nucleus, process)

	// Wire a signal-aware context so shells (or hosts like Claude Desktop)
	// that send SIGINT/SIGTERM on shutdown get a clean exit.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := server.RunStdio(ctx); err != nil {
		// EOF-style termination from the client returns nil from Run; any
		// other error reaches here.
		fmt.Fprintf(stderr, "mcp serve: %v\n", err)
		return 1
	}
	return 0
}

// RunStdio runs the MCP server on the upstream SDK's StdioTransport, which
// reads/writes newline-delimited JSON-RPC 2.0 on os.Stdin / os.Stdout. This
// is the transport Claude Desktop, Cursor, Windsurf, etc. all speak.
//
// Blocks until ctx is cancelled or the client closes the stream. Returns
// nil on clean EOF; otherwise the underlying transport error.
func (m *MCPServer) RunStdio(ctx context.Context) error {
	return m.server.Run(ctx, &mcp.StdioTransport{})
}

// runServerOnTransport is a test hook: runs an already-built MCPServer on an
// arbitrary SDK transport. Not part of the public Engine API; exists so the
// unit tests can use NewInMemoryTransports to verify flag parsing and
// dispatch without touching real stdin/stdout.
func runServerOnTransport(ctx context.Context, m *MCPServer, t mcp.Transport) error {
	return m.server.Run(ctx, t)
}
