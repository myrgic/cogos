// stage_ui_supervisor.go — optional supervisor for the Stage UI upstream.
//
// The kernel-owned Stage UI listener (serve_stage_ui.go) proxies to a Python
// Stage upstream that something else — launchd, a terminal, an operator —
// normally keeps alive. With a `supervise:` block the kernel takes that job
// over, so the tunnel can point at a kernel that is also the thing keeping the
// Stage up:
//
//	stage_ui:
//	  upstream: http://127.0.0.1:7872          # the process supervised below
//	  supervise:
//	    cmd: [python3, -m, stage]              # argv; an absent block = off
//	    cwd: /srv/stage
//	    env_file: /etc/cogos/stage.env         # KEY=VALUE lines, optional
//	    health_url: http://127.0.0.1:7872/healthz  # default: upstream + /healthz
//	    pidfile: /run/cogos/stage.pid
//	    log_file: /var/log/cogos/stage.log
//
// An absent block changes nothing: the listener proxies to whatever is already
// there, exactly as before (see the file header of serve_stage_ui.go).
//
// One check runs at start and every 10 s (DefaultStageUISupervisorInterval):
//
//   - A pidfile process that is alive AND answers 2xx on health_url within 3 s
//     is ADOPTED: the kernel does nothing to it at all and reports
//     adopted=true. This is the launchd handover path — the upstream never
//     blinks while the kernel takes responsibility for it.
//   - A process still inside its START GRACE
//     (DefaultStageUISupervisorStartGrace, 120 s) is booting, not broken: a
//     failed health check there is not a strike. The grace runs from the
//     moment the kernel started the child, or from the moment it first saw a
//     pidfile pid it did not start — a launchd upstream still coming up looks
//     exactly like that. Without it, an upstream that needs 40 s to answer
//     health is killed at the 30 s mark and restarted forever.
//   - Past its grace, a pidfile process that is alive but unhealthy for 3
//     consecutive checks (30 s) is replaced: a Stage that accepts connections
//     but cannot answer /healthz is worse than no Stage, and only a restart
//     clears it. The replacement feeds the restart backoff below, so a Stage
//     that cannot come up is not replaced in a hot loop.
//   - A pid the kernel cannot VOUCH FOR — one it did not start and has never
//     seen answer health — is never signalled, whatever its health says. A
//     stale pidfile can name a pid an unrelated process has since been given,
//     and a SIGTERM or SIGKILL aimed at that is an outage the kernel caused.
//     Such a pid is watched instead: after its grace and the same 3 checks the
//     PIDFILE is treated as stale — removed, with a child of this kernel's own
//     started in its place. If the old process really was a Stage holding the
//     port, the new child fails, exits, and the ordinary crash backoff takes
//     over from there (see mayTerminate, replaceStalePidFile).
//   - No live pidfile process means start one — nothing running yet, or the
//     upstream crashed — subject to the restart backoff below.
//
// The child is started in its own session (Setsid, see
// stage_ui_supervisor_unix.go) with stdout and stderr appended to log_file, so
// stopping or restarting the KERNEL never takes the upstream down: that is the
// whole reason for adopting the launchd-kept process rather than remaining its
// child. The kernel reaps it with a goroutine (so it is never a zombie while
// the kernel runs) and never waits for it in a way that ties its life to the
// kernel's.
//
// Restart backoff: a failed start, a crash, or a deliberate replacement of an
// unhealthy upstream makes the next attempt wait 2 s, doubling per consecutive
// failure to a 60 s cap. The backoff (and its attempt count) resets after 5 min
// continuously healthy — so a long-lived upstream that crashes once is retried
// in 2 s, not in whatever delay a burst last month left behind.
//
// Environment: the child gets the parent environment plus env_file's KEY=VALUE
// lines (last wins). Values are NEVER logged — only the key count and the
// paths (env_file, cwd, log_file, pidfile). Blank lines and # comments in the
// env file are skipped; a line without '=' is a config error, so a malformed
// file cannot silently start a Stage with the wrong environment.
//
// Status: GET /v1/stage-ui/supervisor is served by the listener itself, next
// to /v1/stage-ui/stats, and is never proxied (serve_stage_ui.go). Like the
// stats route it therefore exists while the listener does.
//
// Timing (interval, grace, start grace, health timeout, poll interval, clock)
// is injectable so the tests drive checks and backoff windows without
// sleeping.
package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultStageUISupervisorInterval is how often the supervisor checks the
	// upstream.
	DefaultStageUISupervisorInterval = 10 * time.Second

	// DefaultStageUISupervisorGrace is how long an unhealthy upstream has to
	// exit after SIGTERM before SIGKILL.
	DefaultStageUISupervisorGrace = 10 * time.Second

	// DefaultStageUISupervisorStartGrace is how long a starting upstream has
	// before a failed health check counts against it. It is a boot budget, not
	// a health threshold: a Stage that needs 40 s to load its model answers
	// nothing for 40 s, and without this it would be killed at the 30 s mark
	// and restarted forever.
	DefaultStageUISupervisorStartGrace = 120 * time.Second

	// DefaultStageUISupervisorHealthTimeout bounds one health probe. The
	// listener's own proxy has no such bound; this is the supervisor deciding,
	// not serving.
	DefaultStageUISupervisorHealthTimeout = 3 * time.Second

	// StageUISupervisorUnhealthyChecks is how many consecutive failed checks a
	// live-but-unhealthy upstream gets, once its start grace is over, before it
	// is replaced (3 × the 10 s interval = the 30 s the campaign specifies).
	// One failed check is a hiccup — a Stage restarting its own worker, a
	// moment of GC pressure — and killing on it would turn a hiccup into an
	// outage.
	StageUISupervisorUnhealthyChecks = 3

	// DefaultStageUISupervisorBackoff is the first restart delay after a
	// failed start, a crash, or a deliberate replacement of an unhealthy
	// upstream. It doubles per consecutive failure.
	DefaultStageUISupervisorBackoff = 2 * time.Second

	// MaxStageUISupervisorBackoff caps that doubling.
	MaxStageUISupervisorBackoff = 60 * time.Second

	// StageUISupervisorHealthyReset is how long the upstream must stay healthy
	// before the backoff and its attempt count reset.
	StageUISupervisorHealthyReset = 5 * time.Minute

	// stageUISupervisorPollInterval is how often the grace wait re-checks
	// whether the signalled process is gone.
	stageUISupervisorPollInterval = 50 * time.Millisecond
)

// stageUISupervisorStatus is the JSON body of GET /v1/stage-ui/supervisor.
//
// BackoffS is the restart delay currently in force. It keeps the value that
// was applied to the last restart (and would be doubled by the next failure)
// until StageUISupervisorHealthyReset of continuous health resets it to 0; it
// is not "a wait is happening right now".
type stageUISupervisorStatus struct {
	Enabled   bool    `json:"enabled"`
	PID       int     `json:"pid"`
	Healthy   bool    `json:"healthy"`
	Adopted   bool    `json:"adopted"`
	Restarts  int     `json:"restarts"`
	LastStart string  `json:"last_start"`
	LastError string  `json:"last_error"`
	BackoffS  float64 `json:"backoff_s"`
}

// stageUISupervisor keeps one Stage upstream process alive.
type stageUISupervisor struct {
	// Resolved from the stage_ui config; immutable after construction.
	cmd       []string
	cwd       string
	envFile   string
	pidFile   string
	logFile   string
	healthURL string

	// Injectable timing (tests set these; production gets the defaults).
	interval       time.Duration
	grace          time.Duration
	startGrace     time.Duration
	healthTimeout  time.Duration
	pollInterval   time.Duration
	unhealthyLimit int
	baseBackoff    time.Duration
	maxBackoff     time.Duration
	healthyReset   time.Duration
	now            func() time.Time

	client *http.Client

	// checkMu serializes checks. The loop and a test's own check must never
	// run two at once: two checks that both see "nothing alive" would start
	// two upstreams.
	checkMu sync.Mutex

	// mu guards the state below. It is never held across a probe, a signal or
	// an exec: only across field updates.
	mu           sync.Mutex
	pid          int           // last observed/started upstream process
	healthy      bool          // health_url answered 2xx on the last check
	adopted      bool          // the last healthy process was adopted, not started here
	restarts     int           // restarts of a previously running upstream (see incRestarts)
	lastStart    time.Time     // when the kernel last started a child
	lastErr      string        // last failure, cleared by the next successful start
	backoff      time.Duration // current restart delay (status-reported)
	backoffArmed bool          // a restart is waiting out backoff
	attempts     int           // consecutive failures feeding the doubling
	nextAttempt  time.Time     // when the armed backoff elapses
	unhealthy    int           // consecutive failed checks of a live upstream
	healthySince time.Time     // start of the current healthy window
	hadUpstream  bool          // an upstream has been seen alive at least once

	// pidTrust is what this kernel knows about the pid the pidfile names, and
	// therefore about whether it may signal it at all (see mayTerminate):
	// startedPid is a child this kernel started, healthyPid a pid it has seen
	// answer health (an adopted upstream), observedPid a live pidfile pid it
	// did neither for, first seen at observedSince — the anchor of that pid's
	// own start grace. warnedPid is the not-ours-to-signal pid already warned
	// about, so the warning is logged once per pid and not on every check.
	startedPid    int
	healthyPid    int
	observedPid   int
	observedSince time.Time
	warnedPid     int

	// stopMu guards the loop handle (start/stop).
	stopMu sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// newStageUISupervisor resolves a stage_ui config into a runnable supervisor.
// It reports a config error rather than running with a partial config: a
// supervisor that cannot find its pidfile or log, or that has no command, is a
// mistake, and starting something half-configured under the tunnel would hide
// it.
func newStageUISupervisor(c StageUIConfig) (*stageUISupervisor, error) {
	s := c.Supervise
	if len(s.Cmd) == 0 || strings.TrimSpace(s.Cmd[0]) == "" {
		return nil, fmt.Errorf("stage_ui.supervise.cmd: no command")
	}
	if s.PidFile == "" {
		return nil, fmt.Errorf("stage_ui.supervise.pidfile: required (the pid is how a running upstream is recognized and adopted)")
	}
	if s.LogFile == "" {
		return nil, fmt.Errorf("stage_ui.supervise.log_file: required (child stdout/stderr have nowhere to go)")
	}
	if s.EnvFile != "" {
		// Parse once, here, so a malformed env file is a startup error and not
		// a start failure loop.
		if _, _, err := loadEnvFile(s.EnvFile); err != nil {
			return nil, err
		}
	}
	return &stageUISupervisor{
		cmd:            append([]string(nil), s.Cmd...),
		cwd:            s.Cwd,
		envFile:        s.EnvFile,
		pidFile:        s.PidFile,
		logFile:        s.LogFile,
		healthURL:      stageUISupervisorHealthURL(c),
		interval:       DefaultStageUISupervisorInterval,
		grace:          DefaultStageUISupervisorGrace,
		startGrace:     DefaultStageUISupervisorStartGrace,
		healthTimeout:  DefaultStageUISupervisorHealthTimeout,
		pollInterval:   stageUISupervisorPollInterval,
		unhealthyLimit: StageUISupervisorUnhealthyChecks,
		baseBackoff:    DefaultStageUISupervisorBackoff,
		maxBackoff:     MaxStageUISupervisorBackoff,
		healthyReset:   StageUISupervisorHealthyReset,
		now:            time.Now,
		client:         &http.Client{Timeout: DefaultStageUISupervisorHealthTimeout},
	}, nil
}

// stageUISupervisorHealthURL is the health URL the supervisor probes: the
// explicit health_url when set, else the proxied upstream plus /healthz.
func stageUISupervisorHealthURL(c StageUIConfig) string {
	if s := strings.TrimSpace(c.Supervise.HealthURL); s != "" {
		return s
	}
	upstream := strings.TrimRight(strings.TrimSpace(c.Upstream), "/")
	if upstream == "" {
		upstream = DefaultStageUIUpstream
	}
	return upstream + "/healthz"
}

// start launches the check loop. The first check runs immediately, so a kernel
// that comes up with a dead upstream does not wait a full interval.
func (s *stageUISupervisor) start() {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.cancel, s.done = cancel, done
	go func() {
		defer close(done)
		s.loop(ctx)
	}()
}

// stop ends the check loop and waits for it. It never touches the child: the
// upstream outlives the kernel by design (Setsid), and a kernel shutdown is
// not a reason to take the Stage down.
func (s *stageUISupervisor) stop() {
	s.stopMu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.stopMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// loop runs the checks until ctx ends.
func (s *stageUISupervisor) loop(ctx context.Context) {
	s.runOnce(ctx) // on start
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.runOnce(ctx)
		}
	}
}

// runOnce is one check: adopt, replace, or start, in that order of preference.
func (s *stageUISupervisor) runOnce(ctx context.Context) {
	s.checkMu.Lock()
	defer s.checkMu.Unlock()

	now := s.now()
	pid := readPidFile(s.pidFile)
	alive := pid != 0 && processAlive(pid)
	healthy := alive && s.probeHealth(ctx)

	s.mu.Lock()
	prevPid, hadUpstream, wasAdopted := s.pid, s.hadUpstream, s.adopted
	s.mu.Unlock()

	if alive && healthy {
		s.noteHealthy(now, pid, prevPid, wasAdopted)
		return
	}

	if alive {
		// A live pidfile pid this kernel did not start is one it can only
		// watch: remember when it was first seen, because that instant is the
		// anchor of the pid's own start grace. A launchd upstream still booting
		// looks exactly like this, which is why it gets a grace at all.
		s.noteObserved(now, pid)
		if s.inStartGrace(now, pid) {
			// Boot is not a strike. Counting these is how an upstream that
			// needs 40 s to answer health gets killed every 30 s forever.
			s.setState(false, pid, prevPid != 0 && pid != prevPid)
			slog.Debug("stage-ui: supervisor upstream unhealthy inside its start grace",
				"pid", pid, "grace", s.startGrace.String(), "health_url", s.healthURL)
			return
		}
		strikes := s.noteUnhealthyCheck()
		s.setState(false, pid, prevPid != 0 && pid != prevPid)
		if strikes < s.unhealthyLimit {
			s.warnUnhealthy(pid, strikes)
			return
		}
		if !s.mayTerminate(pid) {
			// The pidfile names a live process this kernel cannot vouch for, so
			// it may not signal it — not for any number of failed checks. The
			// pidfile is what is stale here, and that is what gets replaced.
			s.replaceStalePidFile(now, pid, strikes)
			return
		}
		s.replaceUnhealthy(ctx, now, pid, strikes)
		return
	}

	// Nothing alive in the pidfile: nothing is running yet, or the upstream we
	// were watching is gone (a crash). A pid that is gone is one this kernel no
	// longer knows anything about — the number can already belong to something
	// else, so what vouched for it is dropped here.
	s.forgetTrust(pid)
	if hadUpstream && !s.armed() {
		s.incRestarts()
		s.armBackoff(now)
		s.setLastErr(fmt.Sprintf("upstream exited (pid %d)", prevPid))
		d, deadline := s.backoffWindow()
		slog.Warn("stage-ui: supervisor upstream exited",
			"pid", prevPid, "backoff", d.String(), "retry_at", deadline.UTC().Format(time.RFC3339))
	}
	if s.armed() && now.Before(s.retryAt()) {
		s.setState(false, 0, false)
		return
	}
	_ = s.spawn(now)
}

// replaceUnhealthy ends an unhealthy upstream and arms the restart backoff. The
// pid must already have been cleared as this kernel's to signal (mayTerminate):
// nothing here may end a process the kernel cannot vouch for.
//
// A deliberate replacement feeds the SAME doubling a crash does — 2 s, 4 s, ...
// to the 60 s cap — so a Stage that cannot come up is never replaced in a hot
// loop. The replacement itself starts when that window elapses, on the next
// check, exactly as a crash restart does.
func (s *stageUISupervisor) replaceUnhealthy(ctx context.Context, now time.Time, pid, strikes int) {
	slog.Warn("stage-ui: supervisor replacing unhealthy upstream",
		"pid", pid, "checks", strikes, "health_url", s.healthURL)
	s.setLastErr(fmt.Sprintf("upstream unhealthy (pid %d)", pid))
	s.terminate(ctx, pid)
	s.clearChild()
	s.incRestarts()
	d := s.armBackoff(now)
	slog.Warn("stage-ui: supervisor replacement waits out the backoff",
		"pid", pid, "backoff", d.String(), "retry_at", s.retryAt().UTC().Format(time.RFC3339))
}

// replaceStalePidFile treats a pidfile naming a live process this kernel cannot
// vouch for as stale. That pid is NEVER signalled — the number may belong to
// anything by now — so the pidfile is dropped and a child of this kernel's own
// started in its place. If the old process really was a Stage holding the port,
// the new child fails, exits, and the ordinary crash backoff takes over.
func (s *stageUISupervisor) replaceStalePidFile(now time.Time, pid, strikes int) {
	slog.Warn("stage-ui: supervisor treating the pidfile as stale",
		"pid", pid, "checks", strikes, "start_grace", s.startGrace.String(),
		"pidfile", s.pidFile, "health_url", s.healthURL)
	s.setLastErr(fmt.Sprintf("stale pidfile: pid %d unhealthy and not this kernel's to signal", pid))
	if err := removePidFile(s.pidFile); err != nil {
		slog.Warn("stage-ui: supervisor could not remove the stale pidfile",
			"pidfile", s.pidFile, "err", err)
	}
	s.clearChild()
	s.forgetTrust(pid)
	s.incRestarts()
	_ = s.spawn(now)
}

// warnUnhealthy logs one failed check of a live upstream. A pid this kernel can
// replace warns on every check, so whoever reads the log sees the strikes
// accumulate; a pid it cannot vouch for warns ONCE, because there is nothing
// the supervisor will do about it until the stale window is over and the
// pidfile, not the process, is replaced.
func (s *stageUISupervisor) warnUnhealthy(pid, strikes int) {
	if s.mayTerminate(pid) {
		slog.Warn("stage-ui: supervisor upstream unhealthy",
			"pid", pid, "check", strikes, "limit", s.unhealthyLimit, "health_url", s.healthURL)
		return
	}
	s.mu.Lock()
	first := s.warnedPid != pid
	s.warnedPid = pid
	s.mu.Unlock()
	if !first {
		return
	}
	slog.Warn("stage-ui: supervisor upstream unhealthy and not this kernel's to signal",
		"pid", pid, "check", strikes, "limit", s.unhealthyLimit, "health_url", s.healthURL)
}

// spawn starts the child and records it. A failed start arms the backoff, so a
// command that cannot exec does not spin.
func (s *stageUISupervisor) spawn(now time.Time) error {
	s.mu.Lock()
	restart := s.hadUpstream
	s.mu.Unlock()

	pid, err := s.startChild()
	if err != nil {
		s.setLastErr(err.Error())
		slog.Error("stage-ui: supervisor start failed", "cmd", s.cmd[0], "cwd", s.cwd, "err", err)
		s.armBackoff(now)
		return err
	}

	s.mu.Lock()
	s.pid = pid
	s.startedPid = pid // this pid is this kernel's to signal (mayTerminate)
	s.healthyPid = 0
	s.observedPid = 0
	s.observedSince = time.Time{}
	s.warnedPid = 0
	s.hadUpstream = true
	s.healthy = false
	s.adopted = false
	s.unhealthy = 0
	s.healthySince = time.Time{}
	s.backoffArmed = false
	s.nextAttempt = time.Time{}
	s.lastStart = now
	s.lastErr = ""
	s.mu.Unlock()

	slog.Info("stage-ui: supervisor started upstream",
		"pid", pid, "cmd", s.cmd[0], "cwd", s.cwd, "log_file", s.logFile, "pidfile", s.pidFile, "restart", restart)
	return nil
}

// startChild execs the upstream in its own session. Its stdout/stderr append to
// log_file; its pid goes to the pidfile before anything can observe it, so
// "started by the supervisor" and "in the pidfile" are never out of step.
func (s *stageUISupervisor) startChild() (int, error) {
	env, keys, err := loadEnvFile(s.envFile)
	if err != nil {
		return 0, err
	}
	if s.envFile != "" {
		// Key count and path only: the values are secrets far more often than
		// not, and nothing here needs them.
		slog.Info("stage-ui: supervisor env_file loaded", "path", s.envFile, "keys", keys)
	}
	if dir := filepath.Dir(s.logFile); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, fmt.Errorf("create log_dir %s: %w", dir, err)
		}
	}
	logf, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open log_file %s: %w", s.logFile, err)
	}
	defer logf.Close()

	cmd := exec.Command(s.cmd[0], s.cmd[1:]...)
	cmd.Dir = s.cwd
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = logf, logf
	supervisorDetach(cmd) // own session: the kernel's exit is not the child's
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start %s: %w", s.cmd[0], err)
	}
	pid := cmd.Process.Pid
	if err := writePidFile(s.pidFile, pid); err != nil {
		// Without the pidfile the child is untrackable: a later check would see
		// "nothing running" and start a second upstream beside it. Fail the
		// start instead of leaving an invisible process behind.
		_ = killProcess(pid)
		return 0, err
	}
	// Reap in the background: the child must not become a zombie while the
	// kernel runs, and the kernel must not wait on it in a way that ties its
	// life to the child's. Once it is gone the pid stops meaning anything —
	// nothing prevents the number being handed to an unrelated process — so
	// what vouched for it is dropped first.
	go func() {
		err := cmd.Wait()
		s.forgetTrust(pid)
		slog.Debug("stage-ui: supervisor child exited", "pid", pid, "err", err)
	}()
	return pid, nil
}

// terminate asks pid to exit, waits out the grace period, then ends it. The
// caller must have established that pid is this kernel's to signal at all
// (mayTerminate): this function signals unconditionally once it is called.
func (s *stageUISupervisor) terminate(ctx context.Context, pid int) {
	if err := terminateProcess(pid); err != nil {
		slog.Warn("stage-ui: supervisor SIGTERM failed", "pid", pid, "err", err)
	}
	if s.waitExit(ctx, pid, s.grace) {
		slog.Info("stage-ui: supervisor stopped unhealthy upstream", "pid", pid)
		return
	}
	if err := killProcess(pid); err != nil {
		slog.Warn("stage-ui: supervisor SIGKILL failed", "pid", pid, "err", err)
	}
	slog.Warn("stage-ui: supervisor SIGKILLed unhealthy upstream after the grace period",
		"pid", pid, "grace", s.grace.String())
	_ = s.waitExit(ctx, pid, s.grace)
}

// waitExit polls until pid is gone, the grace period expires, or ctx ends.
// grace is injectable but the wait itself is wall-clock: a process dying is a
// real-time event, not a simulated one.
func (s *stageUISupervisor) waitExit(ctx context.Context, pid int, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for {
		if !processAlive(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return !processAlive(pid)
		case <-time.After(s.pollInterval):
		}
	}
}

// probeHealth reports whether health_url answers 2xx within the health timeout.
func (s *stageUISupervisor) probeHealth(ctx context.Context) bool {
	cctx, cancel := context.WithTimeout(ctx, s.healthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, s.healthURL, nil)
	if err != nil {
		return false
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain, so keep-alive can reuse the conn
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// noteHealthy records a healthy check: adopt what is there, and reset the
// backoff once the upstream has been healthy long enough.
func (s *stageUISupervisor) noteHealthy(now time.Time, pid, prevPid int, wasAdopted bool) {
	adopted := prevPid == 0 || pid != prevPid

	s.mu.Lock()
	s.healthy = true
	s.pid = pid
	s.adopted = adopted
	s.healthyPid = pid // a health answer is what identifies the pid as the Stage
	s.observedPid = 0
	s.observedSince = time.Time{}
	s.warnedPid = 0
	s.unhealthy = 0
	s.hadUpstream = true
	s.backoffArmed = false
	s.nextAttempt = time.Time{}
	if s.healthySince.IsZero() {
		s.healthySince = now
	}
	reset := s.backoff != 0 && now.Sub(s.healthySince) >= s.healthyReset
	if reset {
		s.backoff = 0
		s.attempts = 0
	}
	s.mu.Unlock()

	if adopted && !wasAdopted {
		slog.Info("stage-ui: supervisor adopted the running upstream", "pid", pid, "health_url", s.healthURL)
	}
	if reset {
		slog.Info("stage-ui: supervisor backoff reset after a healthy window",
			"pid", pid, "healthy_for", now.Sub(s.healthySince).String())
	}
}

// mayTerminate reports whether this kernel may signal pid at all. It may signal
// only a process it started itself (in this kernel's lifetime), or one it has
// seen answer health at least once — an adopted upstream, which the health
// answer identified as the Stage the pidfile claims it is. Any other
// live-but-unhealthy pid is a NUMBER: the pidfile may be stale, and the number
// may have been recycled by an unrelated process, so the kernel watches it and
// never SIGTERMs or SIGKILLs it (see replaceStalePidFile).
func (s *stageUISupervisor) mayTerminate(pid int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return pid > 0 && (s.startedPid == pid || s.healthyPid == pid)
}

// inStartGrace reports whether pid is still inside the start grace: time
// measured from the moment this kernel started that child, or from the moment
// it first saw a pidfile pid it did not start. A failed health check inside
// that window is a boot, not a strike.
func (s *stageUISupervisor) inStartGrace(now time.Time, pid int) bool {
	s.mu.Lock()
	anchor := time.Time{}
	switch {
	case pid > 0 && pid == s.startedPid:
		anchor = s.lastStart
	case pid > 0 && pid == s.observedPid:
		anchor = s.observedSince
	default:
		s.mu.Unlock()
		return false
	}
	grace := s.startGrace
	s.mu.Unlock()
	if anchor.IsZero() {
		return false
	}
	return now.Sub(anchor) < grace
}

// noteObserved records the first time this kernel saw a live pidfile pid it did
// not start. That instant is the anchor of the pid's own start grace
// (inStartGrace) and the pid the not-ours-to-signal warning is about.
func (s *stageUISupervisor) noteObserved(now time.Time, pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pid <= 0 || pid == s.startedPid || pid == s.observedPid {
		return
	}
	s.observedPid, s.observedSince, s.warnedPid = pid, now, 0
}

// forgetTrust drops what this kernel knew about pid. Vouching for a pid is only
// meaningful while the process that justified it is the one the pidfile still
// names: once that process is gone the number can be recycled, and a recycled
// pid belongs to something this kernel has no business signalling (see
// mayTerminate). Only the pid's own entries are dropped, so a replacement that
// has already been started is untouched.
func (s *stageUISupervisor) forgetTrust(pid int) {
	if pid <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startedPid == pid {
		s.startedPid = 0
	}
	if s.healthyPid == pid {
		s.healthyPid = 0
	}
	if s.observedPid == pid {
		s.observedPid = 0
		s.observedSince = time.Time{}
	}
	if s.warnedPid == pid {
		s.warnedPid = 0
	}
}

// noteUnhealthyCheck counts one failed check of a live upstream.
func (s *stageUISupervisor) noteUnhealthyCheck() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unhealthy++
	s.healthySince = time.Time{}
	return s.unhealthy
}

// armBackoff advances the restart delay — 2 s, doubling to the cap, one step
// per consecutive failure — and returns the delay now in force.
func (s *stageUISupervisor) armBackoff(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	d := s.baseBackoff
	for i := 1; i < s.attempts && d < s.maxBackoff; i++ {
		d *= 2
	}
	if d > s.maxBackoff {
		d = s.maxBackoff
	}
	s.backoff = d
	s.backoffArmed = true
	s.nextAttempt = now.Add(d)
	return d
}

// armed reports whether a restart is waiting out a backoff window.
func (s *stageUISupervisor) armed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backoffArmed
}

// retryAt is when the armed backoff window ends.
func (s *stageUISupervisor) retryAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextAttempt
}

// backoffWindow reports the armed delay and its deadline (for one log line).
func (s *stageUISupervisor) backoffWindow() (time.Duration, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backoff, s.nextAttempt
}

// setState records the last check's verdict.
func (s *stageUISupervisor) setState(healthy bool, pid int, adopted bool) {
	s.mu.Lock()
	s.healthy, s.pid, s.adopted = healthy, pid, adopted
	s.mu.Unlock()
}

// setLastErr records the failure that explains the current state.
func (s *stageUISupervisor) setLastErr(msg string) {
	s.mu.Lock()
	s.lastErr = msg
	s.mu.Unlock()
}

// incRestarts counts one restart of a previously running upstream. It is
// recorded at the DECISION to restart — a detected crash, or a deliberate
// replacement of an unhealthy upstream — not at the spawn that follows, so the
// count is already correct while a restart waits out its backoff.
func (s *stageUISupervisor) incRestarts() {
	s.mu.Lock()
	s.restarts++
	s.mu.Unlock()
}

// clearChild forgets the child that was just stopped, so the next check starts
// a fresh one instead of re-examining a dead pid.
func (s *stageUISupervisor) clearChild() {
	s.mu.Lock()
	s.pid = 0
	s.healthy = false
	s.adopted = false
	s.unhealthy = 0
	s.healthySince = time.Time{}
	s.mu.Unlock()
}

// status is the snapshot served at GET /v1/stage-ui/supervisor.
func (s *stageUISupervisor) status() stageUISupervisorStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := stageUISupervisorStatus{
		Enabled:   true,
		PID:       s.pid,
		Healthy:   s.healthy,
		Adopted:   s.adopted,
		Restarts:  s.restarts,
		LastError: s.lastErr,
		BackoffS:  s.backoff.Seconds(),
	}
	if !s.lastStart.IsZero() {
		st.LastStart = s.lastStart.UTC().Format(time.RFC3339)
	}
	return st
}

// ─── pidfile and env_file ────────────────────────────────────────────────────

// readPidFile reads the pid the pidfile names, or 0 when it is absent,
// unparsable or names a nonsensical pid. An unparsable pidfile is not an error
// the caller can act on: it reads as "no known process", and the supervisor
// then decides from health and from what it can see.
func readPidFile(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// writePidFile records the pid the supervisor started.
func writePidFile(path string, pid int) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create pidfile_dir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write pidfile %s: %w", path, err)
	}
	return nil
}

// removePidFile drops the pidfile. Only the stale-pidfile path uses it: a
// pidfile naming a process this kernel cannot vouch for is worse than no
// pidfile at all, because it is what every later check would act on. A pidfile
// that is already gone is the state this wants, not an error.
func removePidFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove pidfile %s: %w", path, err)
	}
	return nil
}

// loadEnvFile reads KEY=VALUE lines from path, skipping blank lines and #
// comments, and returns them plus the key count. It reports the path and the
// line NUMBER of a bad line, never its text: a line that is not KEY=VALUE may
// still be a mistyped value, and values must not reach the log.
func loadEnvFile(path string) ([]string, int, error) {
	if path == "" {
		return nil, 0, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("stage_ui.supervise.env_file %s: %w", path, err)
	}
	var out []string
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "=") {
			return nil, 0, fmt.Errorf("stage_ui.supervise.env_file %s line %d: want KEY=VALUE", path, i+1)
		}
		out = append(out, line)
	}
	return out, len(out), nil
}

// ─── the status route ────────────────────────────────────────────────────────

// stageUISupervisorPath is served by the listener itself, never proxied.
const stageUISupervisorPath = "/v1/stage-ui/supervisor"

// serveStageUISupervisorStatus answers GET /v1/stage-ui/supervisor. A nil
// supervisor — no stage_ui.supervise block — reports enabled=false rather than
// 404: the route's answer is "the kernel is not supervising anything", which is
// a state, not a missing page.
func serveStageUISupervisorStatus(w http.ResponseWriter, r *http.Request, sup *stageUISupervisor) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "stage-ui: supervisor is GET only", http.StatusMethodNotAllowed)
		return
	}
	if sup == nil {
		writeJSONResp(w, http.StatusOK, stageUISupervisorStatus{})
		return
	}
	writeJSONResp(w, http.StatusOK, sup.status())
}
