// main.go — CogOS v3 kernel entry point
//
// Starts the continuous process daemon. Three goroutines run concurrently:
//  1. process.Run(ctx)  — the cognitive loop (field updates, consolidation, heartbeat)
//  2. server.Start()    — the HTTP API
//
// Flags:
//
//	--port        API port (default 6931; v2 is 5100)
//	--workspace   path to workspace root (auto-detected from cwd if omitted)
//	--config      (reserved for future use)
package engine

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/myrgic/cogos/internal/providers/selfupdate"
)

var (
	// Version is injected at build time via -ldflags (e.g. "v0.1.0").
	Version = "dev"

	// BuildTime is injected at build time via -ldflags.
	BuildTime = "unknown"
)

// printUsage writes the top-level command listing to w and returns.
// It does NOT call os.Exit — callers decide the exit code.
func printUsage(w io.Writer) {
	fmt.Fprintf(w, "CogOS kernel — continuous-process daemon for AI agents.\n\n")
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  cogos <command> [flags]\n\n")
	fmt.Fprintf(w, "Commands:\n")
	fmt.Fprintf(w, "  init        Initialize a new workspace (.cog directory)\n")
	fmt.Fprintf(w, "  install     Post-install setup (e.g. --add-path adds ~/.cog/bin to PATH)\n")
	fmt.Fprintf(w, "  self-update Check for and apply cogos binary updates\n")
	fmt.Fprintf(w, "  serve       Start the kernel daemon in the foreground (--detach to background)\n")
	fmt.Fprintf(w, "  start       Launch the kernel daemon in a container\n")
	fmt.Fprintf(w, "  stop        Stop a running daemon\n")
	fmt.Fprintf(w, "  restart     Pull the latest image and restart the container daemon\n")
	fmt.Fprintf(w, "  status      Show daemon state and health\n")
	fmt.Fprintf(w, "  logs        Tail container daemon logs\n")
	fmt.Fprintf(w, "  health      Perform a quick health check (exits 0 = healthy)\n")
	fmt.Fprintf(w, "  version     Print build version and exit\n")
	fmt.Fprintf(w, "  node        Manage node configuration\n")
	fmt.Fprintf(w, "  reconcile   Run reconciliation loop diagnostics\n")
	fmt.Fprintf(w, "  coherence   Check workspace coherence (graded git-distance score)\n")
	fmt.Fprintf(w, "  rates       Print the kernel clock-constant rate-ratio table\n")
	fmt.Fprintf(w, "  reindex     Rebuild the FTS5 constellation index from scratch\n")
	fmt.Fprintf(w, "  doctor      Diagnose install/config/index/store health (OK/WARN/FAIL/UNKNOWN)\n")
	fmt.Fprintf(w, "  debug       Fetch a live pprof profile from the running daemon (heap, goroutines)\n")
	fmt.Fprintf(w, "  spine       Show the decision manifold (gravity/inertia field over ADRs/RFCs)\n")
	fmt.Fprintf(w, "  mcp         MCP server sub-commands (serve, ...)\n")
	fmt.Fprintf(w, "  emit        Emit an event onto the kernel bus\n")
	fmt.Fprintf(w, "  agents      List and query running agents\n")
	fmt.Fprintf(w, "  docs        Serve workspace documentation\n")
	fmt.Fprintf(w, "  blobs       Manage content-addressed blob store\n")
	fmt.Fprintf(w, "  experiment  Run kernel experiments\n")
	fmt.Fprintf(w, "  manifest    Print workspace manifest\n")
	fmt.Fprintf(w, "  chat        Start an interactive chat session\n")
	fmt.Fprintf(w, "  help        Show this help message\n")
	fmt.Fprintf(w, "\nGlobal flags:\n")
	fmt.Fprintf(w, "  --workspace PATH   Workspace root (auto-detected from cwd if omitted)\n")
	fmt.Fprintf(w, "  --port PORT        Daemon HTTP API port (default 6931)\n")
	fmt.Fprintf(w, "\nRun 'cogos <command> --help' for per-command flags.\n")
}

func Main() {
	port := flag.Int("port", 0, "HTTP API port (default 6931)")
	workspace := flag.String("workspace", "", "Workspace root path (auto-detected if empty)")

	// Override the default flag.Usage so `cogos --help` / `cogos -h` lists
	// all subcommands rather than only the two global flags.
	flag.Usage = func() {
		printUsage(os.Stderr)
	}
	flag.Parse()

	// Sub-commands.
	args := flag.Args()
	if len(args) > 0 {
		switch args[0] {
		case "help":
			printUsage(os.Stdout)
			return
		case "init":
			runInitCmd(args[1:], *workspace)
			return
		case "serve":
			runServeCmd(args[1:], *workspace, *port, "")
			return
		case "start":
			runStartCmd(args[1:], *workspace, *port)
			return
		case "stop":
			runStopCmd(args[1:], *workspace, *port)
			return
		case "restart":
			runRestartCmd(args[1:], *workspace, *port)
			return
		case "status":
			runStatusCmd(args[1:], *workspace, *port)
			return
		case "logs":
			runLogsCmd(args[1:], *workspace, *port)
			return
		case "version":
			fmt.Printf("cogos version=%s build=%s\n", Version, BuildTime)
			return
		case "health":
			runHealthCheckCmd(args[1:], *workspace, *port)
			return
		case "chat":
			runChat(args[1:], *workspace, *port)
			return
		case "bench":
			runBenchCmd(args[1:], *workspace, *port)
			return
		case "docs":
			runDocsCmd(args[1:], *workspace)
			return
		case "blobs":
			runBlobsCmd(args[1:], *workspace)
			return
		case "experiment":
			runExperimentCmd(args[1:], *workspace, *port)
			return
		case "manifest":
			runManifestCmd(args[1:], *workspace)
			return
		case "node":
			runNodeCmd(args[1:], *workspace)
			return
		case "reconcile":
			runReconcileCmd(args[1:], *workspace)
			return
		case "coherence":
			runCoherenceCmd(args[1:], *workspace)
			return
		case "rates":
			runRatesCmd(args[1:], *workspace)
			return
		case "spine":
			runSpineCmd(args[1:], *workspace)
			return
		case "emit":
			os.Exit(runEmitCmd(args[1:], *workspace))
		case "mcp":
			os.Exit(runMCPCmd(args[1:], *workspace))
		case "agents":
			runAgentsCmd(args[1:], *workspace, *port)
			return
		case "install":
			runInstallCmd(args[1:])
			return
		case "self-update":
			runSelfUpdateCmd(args[1:], *workspace, *port)
			return
		case "reindex":
			runReindexCmd(args[1:], *workspace)
			return
		case "doctor":
			runDoctorCmd(args[1:], *workspace)
			return
		case "debug":
			runDebugCmd(args[1:], *workspace, *port)
			return
		}
	}

	// No subcommand: print help instead of silently starting the daemon.
	// This fixes the Windows dogfood issue where bare `cogos` failed with a
	// config-walk error and gave no hint about available subcommands.
	// Users who want the foreground serve path should use `cogos serve`.
	if len(args) == 0 {
		printUsage(os.Stdout)
		return
	}

	// Unknown subcommand.
	fmt.Fprintf(os.Stderr, "cogos: unknown command %q\n\nRun 'cogos help' for usage.\n", args[0])
	os.Exit(1)
}

func runInitCmd(args []string, defaultWorkspace string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (default: current directory)")
	_ = fs.Parse(args)

	// Use positional arg if no --workspace flag.
	target := *workspace
	if target == "" && fs.NArg() > 0 {
		target = fs.Arg(0)
	}

	if err := RunInit(target); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func setupLogger() {
	// Configure structured logging.
	level := slog.LevelInfo
	if os.Getenv("COG_LOG_DEBUG") != "" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

func runServeCmd(args []string, defaultWorkspace string, defaultPort int, defaultBind string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "HTTP API port (default 6931)")
	bind := fs.String("bind", defaultBind, "HTTP server bind address (default 127.0.0.1; use 0.0.0.0 for LAN, requires trusted network)")
	detach := fs.Bool("detach", false, "Run daemon in the background (Unix: detaches into a new session; Windows: prints a background-run template and exits)")
	_ = fs.Parse(args)

	if *detach {
		// Resolve the workspace root up front so the parent always passes an
		// explicit --workspace to the child and can record daemon state at the
		// correct path (the child auto-detects from cwd otherwise).
		cfg, err := LoadConfig(*workspace, *port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
			os.Exit(1)
		}

		// Rebuild the child args without --detach so it runs in foreground.
		// Always pin --workspace to the resolved root.
		childArgs := []string{"serve", "--workspace", cfg.WorkspaceRoot}
		if cfg.Port != 0 {
			childArgs = append(childArgs, "--port", fmt.Sprintf("%d", cfg.Port))
		}
		if *bind != "" {
			childArgs = append(childArgs, "--bind", *bind)
		}

		pid, err := detachServeProcess(childArgs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

		// MUST-FIX: the child writes its own daemon-state file only after Boot
		// completes (hundreds of ms). Record state from the parent now, using
		// the child's PID, so `cogos stop` works immediately after --detach.
		// The child's planServeState may transiently rewrite this file once it
		// finishes booting; both writers use the same PID, so stop is correct
		// throughout the window.
		state := &DaemonState{
			Mode:      daemonModeBareMetal,
			Endpoint:  endpointForPort(cfg.Port),
			Workspace: cfg.WorkspaceRoot,
			StartedAt: time.Now().UTC().Format(time.RFC3339),
			PID:       &pid,
		}
		if err := saveDaemonState(state); err != nil {
			fmt.Fprintf(os.Stderr, "warning: daemon started (pid %d) but state file write failed: %v\n", pid, err)
		}
		return
	}
	runServe(*workspace, *port, *bind)
}

func runServe(workspace string, port int, bindAddr string) {
	// Register production Reconcilable providers before anything else.
	// RegisterProviders is set by cmd/cogos/providers_wire.go; nil in tests.
	if RegisterProviders != nil {
		RegisterProviders()
	}

	setupLogger()
	slog.Info("cogos: starting", "build", BuildTime)

	// Load configuration.
	cfg, err := LoadConfig(workspace, port)
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}
	// Flag override for bind address (takes precedence over YAML).
	// LoadConfig cannot accept it as a parameter without breaking many
	// unrelated call-sites, so we apply it here symmetrically with the
	// port flag.
	if bindAddr != "" {
		cfg.BindAddr = bindAddr
	}
	// Defensive fallback: if BindAddr is somehow still empty after load
	// (older YAML + empty-string flag + missing default), pin to loopback.
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1"
	}
	// Now that the workspace root is known, upgrade the stderr-only logger
	// to also fan records to <workspace>/.cog/run/kernel.log.jsonl (Agent U's
	// kernel-slog-api). Lines logged above go to stderr only; lines below
	// this call also land in the JSONL sink exposed by /v1/kernel-log and
	// the cog_tail_kernel_log MCP tool.
	upgradeLoggerWithFileSink(cfg)
	slog.Info("config loaded", "workspace", cfg.WorkspaceRoot, "port", cfg.Port, "bind", cfg.BindAddr)

	// Inject the resolved workspace root into daemon-side providers so their
	// Health() implementations can perform real filesystem checks.
	// SetProvidersWorkspace is set by cmd/cogos/providers_wire.go; nil in tests.
	if SetProvidersWorkspace != nil {
		SetProvidersWorkspace(cfg.WorkspaceRoot)
		slog.Info("providers workspace wired", "workspace", cfg.WorkspaceRoot)
	}

	// Inject the running build version and health port into the self-update
	// provider so it can compare against release tags and poll the right port.
	// The provider cannot import engine (import cycle), so the engine pushes
	// these in via setters at boot.
	selfupdate.SetRunningVersion(Version)
	selfupdate.SetPort(cfg.Port)

	// Daemon-already-running guard. Must stay in runServe, not Boot.
	if reuse, msg, err := planServeState(cfg, checkDaemonHealth); err != nil {
		slog.Error("daemon lifecycle failed", "err", err)
		os.Exit(1)
	} else if reuse {
		fmt.Fprintln(os.Stderr, msg)
		return
	}

	// Daemon state file management. Must stay in runServe, not Boot.
	state := buildDaemonState(cfg)
	if err := saveDaemonState(state); err != nil {
		slog.Error("daemon state write failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := removeDaemonState(cfg.WorkspaceRoot); err != nil {
			slog.Warn("daemon state cleanup failed", "err", err)
		}
	}()

	// Signal context for OS-level shutdown. Must stay in runServe, not Boot.
	ctx0 := context.Background()
	ctx, cancel := signal.NotifyContext(ctx0, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Boot the kernel. All subsystem construction, goroutine start, and
	// listener allocation happen inside Boot. Boot does not call os.Exit.
	kernel, err := Boot(ctx, cfg)
	if err != nil {
		slog.Error("kernel boot failed", "err", err)
		os.Exit(1)
	}

	// Wait for shutdown signal or fatal goroutine error.
	select {
	case <-ctx.Done():
		slog.Info("cogos: shutdown signal received")
	case err := <-kernel.serverDone:
		if err != nil {
			slog.Error("server error", "err", err)
			cancel()
		}
	case err := <-kernel.processDone:
		if err != nil {
			slog.Error("process error", "err", err)
			cancel()
		}
	}

	if err := kernel.Stop(); err != nil {
		slog.Warn("kernel stop error", "err", err)
	}

	slog.Info("cogos: stopped")
}

// runHealthCheck performs a quick health check and exits 0 (healthy) or 1 (unhealthy).
// Used by the Dockerfile HEALTHCHECK directive.
func runHealthCheckCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (used to resolve runtime state)")
	port := fs.Int("port", defaultPort, "Daemon port when no runtime state exists")
	_ = fs.Parse(args)

	baseURL := resolveClientEndpoint(*workspace, *port)
	url := baseURL + "/health"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unhealthy: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unhealthy: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "unhealthy: status %d\n", resp.StatusCode)
		os.Exit(1)
	}

	// A 200 means the daemon answered, not that it is capable. Ledger L01:
	// every non-Makefile build shipped without -tags fts5, so memory search
	// silently degraded to an unranked grep while /health stayed green for
	// weeks. build_tags.fts5 is the runtime probe, not the ldflags claim —
	// honour it here, or `cog health` keeps certifying a broken binary.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr == nil {
		var h struct {
			BuildTags struct {
				FTS5     *bool  `json:"fts5"`
				Declared string `json:"declared"`
				Mismatch bool   `json:"mismatch"`
			} `json:"build_tags"`
		}
		// A kernel that predates build_tags simply has no such field; absence
		// is not a failure, a present-and-false probe is.
		if json.Unmarshal(body, &h) == nil && h.BuildTags.FTS5 != nil && !*h.BuildTags.FTS5 {
			fmt.Fprintln(os.Stderr, "unhealthy: build_tags.fts5 is false — this binary was built "+
				"without -tags fts5 (or without CGO), so memory search silently falls back to an "+
				"unranked substring grep. Rebuild with the Makefile (BUILD_TAGS=fts5).")
			os.Exit(1)
		}
		if h.BuildTags.Mismatch {
			fmt.Fprintf(os.Stderr, "warning: build_tags mismatch — declared %q but the runtime probe disagrees\n",
				h.BuildTags.Declared)
		}
	}
	fmt.Println("healthy")
}

func runStartCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "Daemon port")
	image := fs.String("image", defaultDaemonImage, "OCI image to run")
	_ = fs.Parse(args)

	cfg, err := LoadConfig(*workspace, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}

	runtime, err := NewNerdctlRuntime()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	plan, err := planStart(cfg, runtime, checkDaemonHealth, *image)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	switch plan.Action {
	case startConflict:
		fmt.Fprintln(os.Stderr, plan.Message)
		os.Exit(1)
	case startReuse:
		fmt.Fprintln(os.Stderr, plan.Message)
		return
	case startAdopt:
		if plan.AdoptState != nil {
			if err := saveDaemonState(plan.AdoptState); err != nil {
				fmt.Fprintf(os.Stderr, "error: save daemon state: %v\n", err)
				os.Exit(1)
			}
		}
		fmt.Fprintln(os.Stderr, plan.Message)
		return
	case startFresh:
		if plan.RemoveStateFile {
			if err := removeDaemonState(cfg.WorkspaceRoot); err != nil {
				fmt.Fprintf(os.Stderr, "error: remove stale daemon state: %v\n", err)
				os.Exit(1)
			}
		}
	}

	containerName := plan.ContainerName
	if containerName == "" {
		containerName = containerNameForWorkspace(cfg.WorkspaceRoot)
	}
	containerID, err := runtime.Start(*image, ContainerConfig{
		Name:          containerName,
		WorkspaceRoot: cfg.WorkspaceRoot,
		Port:          cfg.Port,
		Command:       []string{"serve", "--workspace", cfg.WorkspaceRoot, "--port", fmt.Sprintf("%d", cfg.Port)},
		RestartPolicy: "unless-stopped",
		Env: map[string]string{
			"COG_DAEMON_MODE":      daemonModeContainer,
			"COG_DAEMON_CONTAINER": containerName,
			"COG_DAEMON_IMAGE":     *image,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if _, err := waitForDaemonHealthy(endpointForPort(cfg.Port), 12*time.Second, checkDaemonHealth); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if state, _ := loadDaemonState(cfg.WorkspaceRoot); state == nil {
		fallback := &DaemonState{
			Mode:      daemonModeContainer,
			Endpoint:  endpointForPort(cfg.Port),
			Container: containerName,
			Workspace: cfg.WorkspaceRoot,
			StartedAt: time.Now().UTC().Format(time.RFC3339),
			Image:     *image,
		}
		if err := saveDaemonState(fallback); err != nil {
			fmt.Fprintf(os.Stderr, "warning: daemon is healthy but state file was not written: %v\n", err)
		}
	}

	fmt.Fprintf(os.Stderr, "started container %s (%s)\n", containerName, strings.TrimSpace(containerID))
}

func runStopCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "Daemon port")
	_ = fs.Parse(args)

	cfg, err := LoadConfig(*workspace, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}

	state, err := loadDaemonState(cfg.WorkspaceRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if state == nil {
		fmt.Fprintf(os.Stderr, "no daemon state for workspace %s\n", cfg.WorkspaceRoot)
		return
	}

	switch state.Mode {
	case daemonModeContainer:
		runtime, err := NewNerdctlRuntime()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		containerID := state.Container
		if containerID == "" {
			containerID = containerNameForWorkspace(cfg.WorkspaceRoot)
		}
		if err := runtime.Stop(containerID); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case daemonModeBareMetal:
		if state.PID == nil {
			fmt.Fprintf(os.Stderr, "cannot stop unmanaged bare-metal daemon for %s\n", cfg.WorkspaceRoot)
			os.Exit(1)
		}
		if err := stopBareMetalPID(*state.PID); err != nil {
			fmt.Fprintf(os.Stderr, "error: stop pid %d: %v\n", *state.PID, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown daemon mode %q\n", state.Mode)
		os.Exit(1)
	}

	if err := waitForDaemonDown(state.Endpoint, 10*time.Second, checkDaemonHealth); err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}
	if err := removeDaemonState(cfg.WorkspaceRoot); err != nil {
		fmt.Fprintf(os.Stderr, "warning: cleanup state file: %v\n", err)
	}
	fmt.Fprintf(os.Stderr, "stopped daemon for workspace %s\n", cfg.WorkspaceRoot)
}

func runRestartCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("restart", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "Daemon port")
	image := fs.String("image", defaultDaemonImage, "OCI image to run")
	_ = fs.Parse(args)

	cfg, err := LoadConfig(*workspace, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}
	state, err := loadDaemonState(cfg.WorkspaceRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if state != nil && state.Mode == daemonModeBareMetal {
		fmt.Fprintln(os.Stderr, "restart is only supported for container-managed daemons; stop the foreground `serve` process and start again")
		os.Exit(1)
	}

	runtime, err := NewNerdctlRuntime()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if err := runtime.Pull(*image); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if state != nil {
		runStopCmd([]string{"--workspace", cfg.WorkspaceRoot, "--port", fmt.Sprintf("%d", cfg.Port)}, cfg.WorkspaceRoot, cfg.Port)
	}
	runStartCmd([]string{"--workspace", cfg.WorkspaceRoot, "--port", fmt.Sprintf("%d", cfg.Port), "--image", *image}, cfg.WorkspaceRoot, cfg.Port)
}

func runStatusCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "Daemon port")
	_ = fs.Parse(args)

	cfg, err := LoadConfig(*workspace, *port)
	if err != nil {
		// No workspace found: give a helpful prompt rather than a raw config error.
		// Goes to stderr + exit 1 so `if cogos status; then ...` scripts treat
		// "no workspace" as not-OK, matching conventional status-check semantics.
		fmt.Fprintln(os.Stderr, "no workspace found; run 'cogos init' to create one")
		os.Exit(1)
	}
	state, err := loadDaemonState(cfg.WorkspaceRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	endpoint := endpointForPort(cfg.Port)
	if state != nil && state.Endpoint != "" {
		endpoint = state.Endpoint
	}

	fmt.Fprintf(os.Stdout, "Workspace: %s\n", cfg.WorkspaceRoot)
	fmt.Fprintf(os.Stdout, "Endpoint:  %s\n", endpoint)
	if state == nil {
		fmt.Fprintln(os.Stdout, "State:     missing")
	} else {
		fmt.Fprintf(os.Stdout, "Mode:      %s\n", state.Mode)
		if state.Container != "" {
			fmt.Fprintf(os.Stdout, "Container: %s\n", state.Container)
		}
		if state.PID != nil {
			fmt.Fprintf(os.Stdout, "PID:       %d\n", *state.PID)
		}
	}

	health, err := checkDaemonHealth(endpoint, 2*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Health:    unreachable (%v)\n", err)
		if state == nil {
			return
		}
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Health:    %s\n", health.Status)
	if health.State != "" {
		fmt.Fprintf(os.Stdout, "Kernel:    %s\n", health.State)
	}
	if health.Identity != "" {
		fmt.Fprintf(os.Stdout, "Identity:  %s\n", health.Identity)
	}
}

func runLogsCmd(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected if empty)")
	follow := fs.Bool("f", true, "Follow logs")
	_ = fs.Parse(args)

	cfg, err := LoadConfig(*workspace, defaultPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}
	state, err := loadDaemonState(cfg.WorkspaceRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if state == nil {
		fmt.Fprintf(os.Stderr, "no daemon state for workspace %s\n", cfg.WorkspaceRoot)
		os.Exit(1)
	}
	if state.Mode != daemonModeContainer {
		fmt.Fprintln(os.Stderr, "logs is only supported for container-managed daemons")
		os.Exit(1)
	}

	runtime, err := NewNerdctlRuntime()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	containerID := state.Container
	if containerID == "" {
		containerID = containerNameForWorkspace(cfg.WorkspaceRoot)
	}
	reader, err := runtime.Logs(containerID, *follow)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()
	if _, err := io.Copy(os.Stdout, reader); err != nil {
		fmt.Fprintf(os.Stderr, "error: copy logs: %v\n", err)
		os.Exit(1)
	}
}
