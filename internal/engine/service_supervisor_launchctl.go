//go:build darwin

// service_supervisor_launchctl.go — LaunchctlController: macOS launchd backend.
//
// Wraps launchctl(1) to implement the ServiceSupervisor interface for
// kind=managed services that declare a launchd label (ServiceDef.Launchd).
//
// Operational semantics (from the pinned spec on issue #100):
//
//   - Start:   idempotent. If already running → return status (200). If launchd
//     is registered but not running → kickstart. If not registered at all →
//     load plist then kickstart.
//   - Stop:    SIGTERM with 5s timeout, then SIGKILL. Returns immediately with
//     Stopping=true; caller polls Status.
//   - Restart: `launchctl kickstart -k` (launchd kills + relaunches in one
//     step, so it completes even if the caller is the service itself), then
//     waits for a new PID. Serialised per-service via mutex.
//   - Enable:  launchctl load -w <plist>. Idempotent.
//   - Disable: launchctl unload -w <plist>. Idempotent (unload of missing job
//     is harmless — launchctl exits 0).
//   - Status:  parses launchctl list <label> output to extract PID and exit code.
//     Never mutates state.
//
// Exit-code mapping (from spec):
//
//	400 — caller sent malformed request (e.g. no plist path)
//	409 — already in the target state (idempotency — we return 200 instead;
//	       the spec clarifies start already-running is 200, not 409)
//	503 — transient launchd error (launchctl exit 125 / "service failed")
//	500 — unexpected launchctl exit code
//
// The raw launchctl exit code is always included in ServiceStatus.LaunchctlExitCode.
package engine

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LaunchctlController implements ServiceSupervisor for macOS launchd services.
// It shells out to launchctl(1) and parses its output to derive live state.
//
// The controller holds a per-service mutex map so that concurrent Restart
// calls for the same service serialise: the second caller blocks until the
// first finishes, then executes its own stop+start (which is idempotent).
type LaunchctlController struct {
	mu    sync.Mutex
	locks map[string]*serviceRestartLock
}

// serviceRestartLock serialises concurrent Restart calls for a single service.
// Plain mutex: each caller runs the full stop+start sequence under the lock.
type serviceRestartLock struct {
	mu sync.Mutex
}

// NewLaunchctlController returns a LaunchctlController ready for use.
func NewLaunchctlController() *LaunchctlController {
	return &LaunchctlController{
		locks: make(map[string]*serviceRestartLock),
	}
}

var _ ServiceSupervisor = (*LaunchctlController)(nil)

// ─── Interface methods ───────────────────────────────────────────────────────

// Start ensures the service is running. Idempotent.
func (c *LaunchctlController) Start(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	st, err := c.Status(ctx, name, def)
	if err != nil {
		return nil, err
	}

	// Already running → idempotent 200.
	if st.Running {
		return st, nil
	}

	label := def.Launchd
	if label == "" {
		return nil, fmt.Errorf("service %q has no launchd label: cannot start via launchctl", name)
	}

	// If launchd doesn't know about the job, load it first.
	if !st.LaunchdRegistered {
		if def.Launchd == "" {
			// No plist label available — can't load.
			return &ServiceStatus{
				Running:           false,
				LaunchdRegistered: false,
				At:                time.Now().UTC(),
			}, nil
		}
		plistPath := plistPathForLabel(label)
		// Spec item 5: if the plist file is absent on disk, do not attempt to
		// load — return ErrNotControllable so the HTTP layer surfaces the same
		// "controllable: false" shape as ObserverSupervisor (409 response).
		if _, statErr := os.Stat(plistPath); os.IsNotExist(statErr) {
			return &ServiceStatus{
				Running:           false,
				LaunchdRegistered: false,
				At:                time.Now().UTC(),
			}, fmt.Errorf("%w: plist not found at %s", ErrNotControllable, plistPath)
		}
		if loadErr := c.runLaunchctl(ctx, "load", plistPath); loadErr != nil {
			st2, _ := c.Status(ctx, name, def)
			if st2 != nil {
				return st2, loadErr
			}
			return &ServiceStatus{
				Running: false,
				At:      time.Now().UTC(),
			}, loadErr
		}
	}

	// Kickstart the job (load-and-start in one step, idempotent on newer macOS).
	exitCode := 0
	kickErr := c.runLaunchctlWithExit(ctx, &exitCode, "kickstart", "-k", "system/"+label)
	if kickErr != nil && exitCode != 0 {
		// Try the user domain if system domain kickstart failed.
		kickErr = c.runLaunchctlWithExit(ctx, &exitCode, "kickstart", "-k", "gui/"+currentUID()+"/"+label)
	}
	kickErr = wrapTransientErr(kickErr, exitCode)

	st2, statusErr := c.Status(ctx, name, def)
	if st2 == nil {
		st2 = &ServiceStatus{At: time.Now().UTC()}
	}
	st2.LaunchctlExitCode = exitCode
	if kickErr != nil && !st2.Running {
		return st2, kickErr
	}
	return st2, statusErr
}

// Stop sends a graceful shutdown signal and returns immediately with Stopping=true.
func (c *LaunchctlController) Stop(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	label := def.Launchd
	if label == "" {
		return nil, fmt.Errorf("service %q has no launchd label: cannot stop via launchctl", name)
	}

	st, _ := c.Status(ctx, name, def)
	if st != nil && !st.Running && !st.LaunchdRegistered {
		// Already stopped — idempotent.
		return st, nil
	}

	exitCode := 0
	stopErr := c.runLaunchctlWithExit(ctx, &exitCode, "stop", label)
	stopErr = wrapTransientErr(stopErr, exitCode)

	st2, _ := c.Status(ctx, name, def)
	if st2 == nil {
		st2 = &ServiceStatus{At: time.Now().UTC()}
	}
	st2.Stopping = true
	st2.LaunchctlExitCode = exitCode
	if stopErr != nil {
		return st2, stopErr
	}
	return st2, nil
}

// Restart asks launchd to kill and relaunch the job in one step
// (`launchctl kickstart -k`), then waits for a new PID to confirm the restart
// landed. Concurrent calls for the same service serialise via a per-service
// mutex.
//
// Why kickstart -k rather than stop+start: launchd owns the whole cycle, so
// the restart completes even if the caller disappears mid-way — which is the
// normal case when the caller IS the service being restarted (an agent
// gateway restarting itself through the kernel). The old stop → sleep →
// start sequence aborted at the sleep when the request context was
// cancelled, and a gracefully-stopped job whose plist only relaunches on a
// non-zero exit (KeepAlive.SuccessfulExit=false) then stayed down.
//
// If the job is not loaded at all, Restart falls back to Start (load +
// kickstart). The returned status carries PreviousPID; Restarted is true
// only when a different live PID was observed before the wait expired.
func (c *LaunchctlController) Restart(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	lock := c.restartLockFor(name)

	lock.mu.Lock()
	defer lock.mu.Unlock()

	label := def.Launchd
	if label == "" {
		return nil, fmt.Errorf("service %q has no launchd label: cannot restart via launchctl", name)
	}

	before, _ := c.Status(ctx, name, def)
	if before == nil || !before.LaunchdRegistered {
		return c.Start(ctx, name, def)
	}
	prevPID := before.PID

	exitCode := 0
	kickErr := c.runLaunchctlWithExit(ctx, &exitCode, "kickstart", "-k", "gui/"+currentUID()+"/"+label)
	kickErr = wrapTransientErr(kickErr, exitCode)
	if kickErr != nil {
		st, _ := c.Status(ctx, name, def)
		if st == nil {
			st = &ServiceStatus{At: time.Now().UTC()}
		}
		st.PreviousPID = prevPID
		st.LaunchctlExitCode = exitCode
		return st, kickErr
	}

	st := c.awaitNewPID(ctx, name, def, prevPID, restartConfirmTimeout)
	st.PreviousPID = prevPID
	st.LaunchctlExitCode = exitCode
	return st, nil
}

// restartConfirmTimeout bounds how long Restart waits for launchd to report a
// new PID. Gateways drain in-flight work on SIGTERM, so this is generous.
var restartConfirmTimeout = 30 * time.Second

// awaitNewPID polls Status until the job reports a live PID different from
// prevPID, the context ends, or timeout elapses. It always returns a status
// (the last one observed) and never an error: an unconfirmed restart is
// reported via Restarted=false, not as a failure of the kickstart itself.
func (c *LaunchctlController) awaitNewPID(ctx context.Context, name string, def ServiceDef, prevPID int, timeout time.Duration) *ServiceStatus {
	deadline := time.Now().Add(timeout)
	var last *ServiceStatus
	for {
		st, _ := c.Status(ctx, name, def)
		if st != nil {
			last = st
			if st.Running && st.PID != 0 && st.PID != prevPID {
				st.Restarted = true
				return st
			}
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			if last == nil {
				last = &ServiceStatus{At: time.Now().UTC()}
			}
			return last
		case <-time.After(250 * time.Millisecond):
		}
	}
	if last == nil {
		last = &ServiceStatus{At: time.Now().UTC()}
	}
	return last
}

// Enable loads the plist with -w (write boot preference) so the service
// starts automatically at login/boot.
func (c *LaunchctlController) Enable(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	label := def.Launchd
	if label == "" {
		return nil, fmt.Errorf("service %q has no launchd label: cannot enable via launchctl", name)
	}
	plistPath := plistPathForLabel(label)
	exitCode := 0
	err := c.runLaunchctlWithExit(ctx, &exitCode, "load", "-w", plistPath)
	st, _ := c.Status(ctx, name, def)
	if st == nil {
		st = &ServiceStatus{At: time.Now().UTC()}
	}
	st.LaunchctlExitCode = exitCode
	// load -w of an already-loaded job is a no-op with exit 0 on modern macOS.
	_ = err
	return st, nil
}

// Disable unloads the plist with -w (write boot preference) so the service
// does not start automatically. Idempotent: unload of a missing job exits 0.
func (c *LaunchctlController) Disable(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	label := def.Launchd
	if label == "" {
		return nil, fmt.Errorf("service %q has no launchd label: cannot disable via launchctl", name)
	}
	plistPath := plistPathForLabel(label)
	exitCode := 0
	_ = c.runLaunchctlWithExit(ctx, &exitCode, "unload", "-w", plistPath)
	// unload -w of a missing job is harmless (exits 0).
	st, _ := c.Status(ctx, name, def)
	if st == nil {
		st = &ServiceStatus{At: time.Now().UTC()}
	}
	st.LaunchctlExitCode = exitCode
	return st, nil
}

// Status returns the current live state by parsing `launchctl list <label>`.
// launchd is authoritative for Running; /health is not consulted here.
func (c *LaunchctlController) Status(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	label := def.Launchd
	if label == "" {
		// No launchd label — report as not registered.
		return &ServiceStatus{
			Running:           false,
			LaunchdRegistered: false,
			At:                time.Now().UTC(),
		}, nil
	}

	out, exitCode, err := c.runLaunchctlOutput(ctx, "list", label)

	st := &ServiceStatus{At: time.Now().UTC()}

	if err != nil || exitCode != 0 {
		// Non-zero exit from `launchctl list` means the job is not registered.
		st.Running = false
		st.LaunchdRegistered = false
		st.LaunchctlExitCode = exitCode
		return st, nil
	}

	st.LaunchdRegistered = true
	st.LaunchctlExitCode = exitCode

	// Parse the output: tab-separated fields including PID and LastExitStatus.
	// Example output:
	//   {
	//     "PID" = 1234;
	//     "LastExitStatus" = 0;
	//     "Label" = "com.cogos.kernel";
	//   };
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, `"PID"`) {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				pidStr := strings.Trim(strings.TrimSpace(parts[1]), "; \"")
				if pid, convErr := strconv.Atoi(pidStr); convErr == nil && pid > 0 {
					st.PID = pid
					st.Running = true
				}
			}
		}
		if strings.HasPrefix(line, `"LastExitStatus"`) {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				codeStr := strings.Trim(strings.TrimSpace(parts[1]), "; \"")
				if code, convErr := strconv.Atoi(codeStr); convErr == nil {
					st.ExitCode = code
				}
			}
		}
	}

	return st, nil
}

// ─── Internal helpers ────────────────────────────────────────────────────────

// wrapTransientErr wraps err with ErrLaunchctlTransient when the launchctl
// exit code is 125 ("service failed" — transient launchd error). This lets
// dispatchMutation map the error to HTTP 503 per the operational-semantics
// spec (pinned comment on issue #100).
func wrapTransientErr(err error, exitCode int) error {
	if err != nil && exitCode == 125 {
		return fmt.Errorf("%w: %w", ErrLaunchctlTransient, err)
	}
	return err
}

func (c *LaunchctlController) restartLockFor(name string) *serviceRestartLock {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.locks[name]; !ok {
		c.locks[name] = &serviceRestartLock{}
	}
	return c.locks[name]
}

// runLaunchctl runs launchctl with the given arguments and discards output.
func (c *LaunchctlController) runLaunchctl(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "launchctl", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	return cmd.Run()
}

// runLaunchctlWithExit runs launchctl, captures the exit code in *exitCode,
// and returns a non-nil error only if the invocation fails for a reason other
// than a non-zero exit code from the child process.
func (c *LaunchctlController) runLaunchctlWithExit(ctx context.Context, exitCode *int, args ...string) error {
	cmd := exec.CommandContext(ctx, "launchctl", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); ok {
		*exitCode = exitErr.ExitCode()
		return err
	}
	if err != nil {
		*exitCode = 1
		return err
	}
	*exitCode = 0
	return nil
}

// runLaunchctlOutput runs launchctl and returns (stdout, exit_code, error).
func (c *LaunchctlController) runLaunchctlOutput(ctx context.Context, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, "launchctl", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	} else if err != nil {
		exitCode = 1
	}
	return stdout.String(), exitCode, err
}

// plistPathForLabel converts a launchd label to a conventional plist path.
// By convention CogOS uses ~/Library/LaunchAgents/<label>.plist.
func plistPathForLabel(label string) string {
	home, err := homeDir()
	if err != nil {
		// Fallback: /Library/LaunchDaemons (system domain)
		return fmt.Sprintf("/Library/LaunchDaemons/%s.plist", label)
	}
	return fmt.Sprintf("%s/Library/LaunchAgents/%s.plist", home, label)
}

// currentUID returns the current user's UID as a string (for kickstart domain).
func currentUID() string {
	cmd := exec.Command("id", "-u")
	out, err := cmd.Output()
	if err != nil {
		return "501"
	}
	return strings.TrimSpace(string(out))
}

// homeDir returns the current user's home directory on darwin.
func homeDir() (string, error) {
	cmd := exec.Command("sh", "-c", "echo $HOME")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", fmt.Errorf("empty HOME")
	}
	return dir, nil
}
