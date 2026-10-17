// stage_ui_supervisor_test.go — tests for the optional Stage upstream
// supervisor (stage_ui_supervisor.go): first start, adoption of a healthy
// pidfile process, crash restart with backoff, replacement of an unhealthy but
// alive upstream, environment delivery, the status route, the YAML block and
// the server wiring.
//
// The stub upstream is THIS test binary re-executed (the
// GO_WANT_HELPER_PROCESS pattern): TestStageUIStubChild sees
// STAGE_UI_STUB_MODE and acts as the child — it binds 127.0.0.1:0, writes
// "pid addr" to STAGE_UI_STUB_ADDR_FILE, counts its own starts in
// STAGE_UI_STUB_COUNT_FILE, and serves /healthz with the health its mode asks
// for. Without the env var the test skips, so an ordinary `go test` ignores it.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// ─── stub child environment ──────────────────────────────────────────────────

const (
	// stubChildRun is the -test.run pattern that makes this binary the stub.
	stubChildRun = "^TestStageUIStubChild$"

	// stubModeEnv marks a process as the stub and names its health: "healthy"
	// (default) or "unhealthy".
	stubModeEnv = "STAGE_UI_STUB_MODE"
	// stubAddrEnv is a file the stub writes "<its pid> <bound addr>" to.
	stubAddrEnv = "STAGE_UI_STUB_ADDR_FILE"
	// stubCountEnv is a file the stub increments on every start.
	stubCountEnv = "STAGE_UI_STUB_COUNT_FILE"
	// stubDumpEnv is a file the stub writes its own environment to.
	stubDumpEnv = "STAGE_UI_STUB_ENV_DUMP"
	// stubCrashEnv="1" makes the FIRST start exit(3) and later starts serve.
	stubCrashEnv = "STAGE_UI_STUB_CRASH_FIRST"
	// stubUnhealthyEnv="1" makes the FIRST start serve 503, later starts 200.
	stubUnhealthyEnv = "STAGE_UI_STUB_UNHEALTHY_FIRST"
	// stubIgnoreTermEnv="1" makes the stub ignore SIGTERM, so only SIGKILL
	// ends it (the supervisor's grace-then-kill path).
	stubIgnoreTermEnv = "STAGE_UI_STUB_IGNORE_SIGTERM"
	// stubHealthDelayEnv is a millisecond "slow boot": the stub listens
	// immediately but answers 503 on /healthz until that long after it started.
	stubHealthDelayEnv = "STAGE_UI_STUB_HEALTH_DELAY_MS"
)

// TestStageUIStubChild is not a test in the ordinary sense: with
// STAGE_UI_STUB_MODE set, this test binary IS the stub Stage upstream the
// supervisor tests spawn.
func TestStageUIStubChild(t *testing.T) {
	mode := os.Getenv(stubModeEnv)
	if mode == "" {
		t.Skip("stub Stage upstream child; only runs under the supervisor tests")
	}
	starts := stubBumpStartCount(os.Getenv(stubCountEnv))
	if dump := os.Getenv(stubDumpEnv); dump != "" {
		body := strings.Join(os.Environ(), "\n") + "\n"
		if err := os.WriteFile(dump, []byte(body), 0o644); err != nil {
			t.Fatalf("stub: write env dump: %v", err)
		}
	}
	if os.Getenv(stubIgnoreTermEnv) == "1" {
		signal.Ignore(syscall.SIGTERM) // only SIGKILL ends this stub
	}
	if os.Getenv(stubCrashEnv) == "1" && starts == 1 {
		os.Exit(3) // crash: the supervisor must bring the upstream back
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("stub: listen: %v", err)
	}
	if f := os.Getenv(stubAddrEnv); f != "" {
		line := fmt.Sprintf("%d %s\n", os.Getpid(), ln.Addr().String())
		if err := os.WriteFile(f, []byte(line), 0o644); err != nil {
			t.Fatalf("stub: write addr file: %v", err)
		}
	}
	healthy := mode != "unhealthy"
	if os.Getenv(stubUnhealthyEnv) == "1" && starts == 1 {
		healthy = false
	}
	// A slow boot: up and listening, but answering 503 until this instant.
	// Covering it is the supervisor's start grace (defect: a child killed after
	// three failed checks never gets to serve).
	var healthyAt time.Time
	if ms, err := strconv.Atoi(os.Getenv(stubHealthDelayEnv)); err == nil && ms > 0 {
		healthyAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !healthy || (!healthyAt.IsZero() && time.Now().Before(healthyAt)) {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	_ = (&http.Server{Handler: mux}).Serve(ln) // until the supervisor stops it
}

// stubBumpStartCount increments the start counter file and returns the new
// value (0 when no counter file is configured).
func stubBumpStartCount(path string) int {
	if path == "" {
		return 0
	}
	n := 0
	if b, err := os.ReadFile(path); err == nil {
		n, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	n++
	if err := os.WriteFile(path, []byte(strconv.Itoa(n)+"\n"), 0o644); err != nil {
		return n
	}
	return n
}

// ─── stub child harness ──────────────────────────────────────────────────────

// stubChildParams configures one stub child.
type stubChildParams struct {
	mode           string   // "" = healthy, else "unhealthy"
	crashFirst     bool     // the first start exits 3
	unhealthyFirst bool     // the first start serves 503
	ignoreSigterm  bool     // survives SIGTERM (SIGKILL is required)
	envDump        bool     // dump the child's environment to a file
	healthDelayMS  int      // serves 503 for this long after starting (slow boot)
	extraEnv       []string // extra env_file lines (markers, secrets)
}

// stubChild is one stub upstream child plus the files the test watches.
type stubChild struct {
	dir       string
	argv      []string
	pidFile   string
	logFile   string
	addrFile  string
	countFile string
	envFile   string
	dumpFile  string
	// envLines are the KEY=VALUE lines written to env_file; they are ALSO the
	// child's configuration, so the supervisor tests exercise env delivery for
	// real rather than through a test-only channel.
	envLines []string
}

// newStubChild lays out the stub's files under a fresh temp dir.
func newStubChild(t *testing.T, p stubChildParams) *stubChild {
	t.Helper()
	dir := t.TempDir()
	c := &stubChild{
		dir:       dir,
		argv:      []string{os.Args[0], "-test.run=" + stubChildRun},
		pidFile:   filepath.Join(dir, "stage.pid"),
		logFile:   filepath.Join(dir, "stage.log"),
		addrFile:  filepath.Join(dir, "stage.addr"),
		countFile: filepath.Join(dir, "stage.count"),
		envFile:   filepath.Join(dir, "stage.env"),
		dumpFile:  filepath.Join(dir, "stage.env.dump"),
	}
	mode := p.mode
	if mode == "" {
		mode = "healthy"
	}
	c.envLines = []string{
		stubModeEnv + "=" + mode,
		stubAddrEnv + "=" + c.addrFile,
		stubCountEnv + "=" + c.countFile,
	}
	if p.crashFirst {
		c.envLines = append(c.envLines, stubCrashEnv+"=1")
	}
	if p.unhealthyFirst {
		c.envLines = append(c.envLines, stubUnhealthyEnv+"=1")
	}
	if p.ignoreSigterm {
		c.envLines = append(c.envLines, stubIgnoreTermEnv+"=1")
	}
	if p.envDump {
		c.envLines = append(c.envLines, stubDumpEnv+"="+c.dumpFile)
	}
	if p.healthDelayMS > 0 {
		c.envLines = append(c.envLines, stubHealthDelayEnv+"="+strconv.Itoa(p.healthDelayMS))
	}
	c.envLines = append(c.envLines, p.extraEnv...)
	return c
}

// supervisorConfig is the stage_ui.supervise config that starts this stub.
func (c *stubChild) supervisorConfig(t *testing.T) StageUISupervisorConfig {
	t.Helper()
	body := strings.Join(c.envLines, "\n") + "\n"
	if err := os.WriteFile(c.envFile, []byte(body), 0o644); err != nil {
		t.Fatalf("write env_file: %v", err)
	}
	return StageUISupervisorConfig{
		Cmd:     append([]string(nil), c.argv...),
		Cwd:     c.dir,
		EnvFile: c.envFile,
		PidFile: c.pidFile,
		LogFile: c.logFile,
	}
}

// stubSpawn starts the stub under the test's own control (for the adoption
// test: the process the supervisor is meant to adopt) and returns its pid.
func (c *stubChild) stubSpawn(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(c.argv[0], c.argv[1:]...)
	cmd.Dir = c.dir
	cmd.Env = append(os.Environ(), c.envLines...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn stub: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	return cmd.Process.Pid
}

// pidFromFile reads the pid the pidfile names (0 when absent or unparsable).
func (c *stubChild) pidFromFile(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile(c.pidFile)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	return n
}

// waitPidFromFile waits for the pidfile to name a pid.
func (c *stubChild) waitPidFromFile(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if pid := c.pidFromFile(t); pid > 0 {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("pidfile %s never named a pid", c.pidFile)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitPidFileChange waits for the pidfile to name a pid other than prev.
func (c *stubChild) waitPidFileChange(t *testing.T, prev int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if pid := c.pidFromFile(t); pid > 0 && pid != prev {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("pidfile %s still names %d", c.pidFile, prev)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitAddr waits for the child named by pid to publish the address it bound.
func (c *stubChild) waitAddr(t *testing.T, pid int) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for {
		if b, err := os.ReadFile(c.addrFile); err == nil {
			fields := strings.Fields(string(b))
			if len(fields) == 2 {
				last = string(b)
				if n, _ := strconv.Atoi(fields[0]); n == pid {
					return fields[1]
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d never published an address (addr file %q)", pid, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// healthURL is the /healthz URL of the child named by pid.
func (c *stubChild) healthURL(t *testing.T, pid int) string {
	t.Helper()
	return "http://" + c.waitAddr(t, pid) + "/healthz"
}

// starts reports how many child processes the stub has started.
func (c *stubChild) starts(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile(c.countFile)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	return n
}

// waitStarts waits until the stub has started at least want times and returns
// the count. The child bumps its counter a moment AFTER cmd.Start returns (it
// must be exec'd first), so a bare starts() read right after a start races; a
// positive expectation waits, a "did not restart" expectation reads directly.
func (c *stubChild) waitStarts(t *testing.T, want int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := c.starts(t); got >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("stub started %d times, want %d", c.starts(t), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitAlive waits until pid names a live process.
func waitAlive(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("process %d never became alive", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitDead waits until pid no longer names a live process.
func waitDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("process %d is still alive", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// killAndWait ends a stub child so no process outlives its test.
func killAndWait(t *testing.T, pid int) {
	t.Helper()
	if pid <= 0 || !processAlive(pid) {
		return
	}
	_ = terminateProcess(pid)
	for i := 0; i < 200 && processAlive(pid); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if processAlive(pid) {
		_ = killProcess(pid)
		for i := 0; i < 400 && processAlive(pid); i++ {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if processAlive(pid) {
		t.Errorf("stub child %d survived SIGTERM and SIGKILL", pid)
	}
}

// testSupervisor builds a supervisor over c with a test-driven clock: advance
// moves it forward, so restart-backoff windows can be crossed without
// sleeping. The clock is only safe for tests that drive checks themselves —
// the background loop (start) must keep the real clock.
func testSupervisor(t *testing.T, c StageUISupervisorConfig) (*stageUISupervisor, func(time.Duration)) {
	t.Helper()
	sup, err := newStageUISupervisor(StageUIConfig{Upstream: "http://127.0.0.1:1", Supervise: c})
	if err != nil {
		t.Fatalf("newStageUISupervisor: %v", err)
	}
	base := time.Now()
	offset := time.Duration(0)
	sup.now = func() time.Time { return base.Add(offset) }
	sup.healthTimeout = 2 * time.Second
	sup.pollInterval = 5 * time.Millisecond
	return sup, func(d time.Duration) { offset += d }
}

// check runs one supervisor check the way the loop does.
func (s *stageUISupervisor) check(t *testing.T) {
	t.Helper()
	s.runOnce(context.Background())
}

// waitHealthy waits until url answers 200 (a slow boot finishing).
func waitHealthy(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never became healthy", url)
}

// requireDefaultStartGrace fails unless the supervisor runs with the documented
// start grace. The grace tests derive their own window from that value, so a
// silently smaller (or absent) default would let them pass for the wrong reason.
func requireDefaultStartGrace(t *testing.T, sup *stageUISupervisor) {
	t.Helper()
	if sup.startGrace != DefaultStageUISupervisorStartGrace {
		t.Fatalf("start grace = %v, want the documented default %v", sup.startGrace, DefaultStageUISupervisorStartGrace)
	}
}

// walkToStrikeLimit runs one check per interval from the start of pid's start
// grace up to — but not including — the strike that decides a replacement, and
// asserts that nothing in that window touches pid. wantRestarts is the restart
// count the window opens with: a replacement inside the grace is exactly what
// defect 1 was, so any change to a pid there fails the test.
func walkToStrikeLimit(t *testing.T, sup *stageUISupervisor, advance func(time.Duration), child *stubChild, pid, wantRestarts int) {
	t.Helper()
	n := int(sup.startGrace/sup.interval) + sup.unhealthyLimit - 1
	for i := 0; i < n; i++ {
		sup.check(t)
		st := sup.status()
		if st.Restarts != wantRestarts || st.PID != pid || st.Healthy || st.Adopted {
			t.Fatalf("check %d of the grace window: %+v (want pid %d, restarts %d)", i+1, st, pid, wantRestarts)
		}
		if !processAlive(pid) {
			t.Fatalf("check %d of the grace window: pid %d was signalled", i+1, pid)
		}
		if got := child.pidFromFile(t); got != pid {
			t.Fatalf("check %d of the grace window: pidfile names %d, want %d", i+1, got, pid)
		}
		advance(sup.interval)
	}
}

// ─── behaviour ───────────────────────────────────────────────────────────────

func TestStageUISupervisor_StartsWhenNothingRuns(t *testing.T) {
	child := newStubChild(t, stubChildParams{})
	sup, _ := testSupervisor(t, child.supervisorConfig(t))

	sup.check(t) // nothing in the pidfile: the supervisor starts the upstream

	st := sup.status()
	if !st.Enabled || st.PID == 0 || st.Adopted || st.Restarts != 0 {
		t.Fatalf("status after the first start: %+v", st)
	}
	if st.LastStart == "" {
		t.Errorf("last_start empty after a start")
	}
	if st.LastError != "" {
		t.Errorf("last_error = %q after a clean start", st.LastError)
	}
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	if pid := child.waitPidFromFile(t); pid != st.PID {
		t.Errorf("pidfile names %d, status pid %d", pid, st.PID)
	}
	if got := child.waitStarts(t, 1); got != 1 {
		t.Fatalf("stub started %d times, want 1", got)
	}
	if !processAlive(st.PID) {
		t.Fatalf("child %d is not alive", st.PID)
	}
	if _, err := os.Stat(child.logFile); err != nil {
		t.Errorf("log_file not created: %v", err)
	}

	// The next check probes: the child is up and healthy, and stays unadopted
	// because the supervisor started it.
	sup.healthURL = child.healthURL(t, st.PID)
	sup.check(t)
	if st := sup.status(); !st.Healthy || st.Adopted || st.PID == 0 {
		t.Errorf("status after the healthy probe: %+v", st)
	}
	if got := child.starts(t); got != 1 {
		t.Fatalf("stub restarted while healthy: %d starts", got)
	}
}

func TestStageUISupervisor_AdoptsHealthyPidfileProcess(t *testing.T) {
	child := newStubChild(t, stubChildParams{})
	cfg := child.supervisorConfig(t)

	// The "launchd-kept" upstream: started by someone else, pid recorded.
	pid := child.stubSpawn(t)
	if err := os.WriteFile(cfg.PidFile, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		t.Fatalf("write pidfile: %v", err)
	}
	sup, _ := testSupervisor(t, cfg)
	sup.healthURL = child.healthURL(t, pid)

	sup.check(t)

	st := sup.status()
	if !st.Enabled || !st.Healthy || !st.Adopted {
		t.Fatalf("status on adoption: %+v", st)
	}
	if st.PID != pid {
		t.Errorf("adopted pid %d, want %d", st.PID, pid)
	}
	if st.Restarts != 0 {
		t.Errorf("restarts = %d on adoption, want 0", st.Restarts)
	}
	if got := child.waitStarts(t, 1); got != 1 {
		t.Fatalf("a second upstream was started: %d starts", got)
	}
	if got := child.pidFromFile(t); got != pid {
		t.Errorf("pidfile rewritten to %d, want %d", got, pid)
	}
	if !processAlive(pid) {
		t.Fatalf("adopted process %d is gone", pid)
	}
}

func TestStageUISupervisor_CrashRestartsWithBackoff(t *testing.T) {
	child := newStubChild(t, stubChildParams{crashFirst: true})
	sup, advance := testSupervisor(t, child.supervisorConfig(t))

	sup.check(t) // first start: the stub exits 3 immediately
	pid1 := child.waitPidFromFile(t)
	waitDead(t, pid1)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })

	sup.check(t) // crash seen: the restart waits out the backoff
	st := sup.status()
	if st.Restarts != 1 {
		t.Fatalf("restarts = %d after the crash, want 1 (%+v)", st.Restarts, st)
	}
	if st.BackoffS != 2 {
		t.Errorf("backoff_s = %v after the first crash, want 2", st.BackoffS)
	}
	if st.LastError == "" {
		t.Errorf("last_error empty after a crash")
	}
	if got := child.waitStarts(t, 1); got != 1 {
		t.Fatalf("restarted before the backoff elapsed: %d starts", got)
	}

	advance(DefaultStageUISupervisorBackoff + time.Second)
	sup.check(t) // the backoff elapsed: start again
	pid2 := child.waitPidFileChange(t, pid1)
	if got := child.waitStarts(t, 2); got != 2 {
		t.Fatalf("stub started %d times, want 2", got)
	}
	waitAlive(t, pid2)
	sup.healthURL = child.healthURL(t, pid2)

	sup.check(t) // healthy again; the backoff still stands until the window
	if st := sup.status(); !st.Healthy {
		t.Fatalf("restarted child is not healthy: %+v", st)
	}
	advance(StageUISupervisorHealthyReset + time.Second)
	sup.check(t)
	st = sup.status()
	if st.BackoffS != 0 {
		t.Errorf("backoff_s = %v after a healthy window, want 0", st.BackoffS)
	}
	if !st.Healthy {
		t.Errorf("upstream lost health during the reset window: %+v", st)
	}
}

func TestStageUISupervisor_UnhealthyAliveIsReplaced(t *testing.T) {
	child := newStubChild(t, stubChildParams{unhealthyFirst: true})
	sup, advance := testSupervisor(t, child.supervisorConfig(t))
	sup.startGrace = 0 // this test is about the strike limit, not about booting

	sup.check(t) // start the first (unhealthy) child
	pid1 := child.waitPidFromFile(t)
	waitAlive(t, pid1)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	sup.healthURL = child.healthURL(t, pid1)

	for i := 1; i < StageUISupervisorUnhealthyChecks; i++ {
		sup.check(t) // strikes short of the limit replace nothing
		if st := sup.status(); st.Restarts != 0 || st.PID != pid1 || st.Healthy || st.Adopted {
			t.Fatalf("strike %d: %+v", i, st)
		}
		if !processAlive(pid1) {
			t.Fatalf("child %d killed at strike %d, want %d strikes", pid1, i, StageUISupervisorUnhealthyChecks)
		}
	}

	sup.check(t) // the third strike: SIGTERM, then the replacement waits out the backoff
	waitDead(t, pid1)
	st := sup.status()
	if st.Restarts != 1 || st.PID != 0 {
		t.Fatalf("status at the replacement decision: %+v", st)
	}
	if want := DefaultStageUISupervisorBackoff.Seconds(); st.BackoffS != want {
		t.Fatalf("backoff_s = %v at the replacement decision, want %v", st.BackoffS, want)
	}
	if got := child.pidFromFile(t); got != pid1 {
		t.Fatalf("pidfile names %d during the backoff, want the dead %d", got, pid1)
	}

	advance(DefaultStageUISupervisorBackoff + time.Second)
	sup.check(t) // the backoff elapsed: the replacement starts
	pid2 := child.waitPidFileChange(t, pid1)
	waitAlive(t, pid2)
	st = sup.status()
	if st.PID != pid2 || st.Restarts != 1 {
		t.Fatalf("status after the replacement: %+v (pid1 %d pid2 %d)", st, pid1, pid2)
	}
	if got := child.waitStarts(t, 2); got != 2 {
		t.Fatalf("stub started %d times, want 2", got)
	}

	// The replacement is healthy (the stub is unhealthy only on its first
	// start), so the next check settles.
	sup.healthURL = child.healthURL(t, pid2)
	sup.check(t)
	if st := sup.status(); !st.Healthy || st.PID != pid2 {
		t.Errorf("replacement not healthy: %+v", st)
	}
}

func TestStageUISupervisor_UnhealthyIgnoringSigtermIsKilled(t *testing.T) {
	child := newStubChild(t, stubChildParams{unhealthyFirst: true, ignoreSigterm: true})
	sup, advance := testSupervisor(t, child.supervisorConfig(t))
	sup.grace = 100 * time.Millisecond
	sup.startGrace = 0 // this test is about SIGKILL, not about booting

	sup.check(t)
	pid1 := child.waitPidFromFile(t)
	waitAlive(t, pid1)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	sup.healthURL = child.healthURL(t, pid1)

	for i := 0; i < StageUISupervisorUnhealthyChecks; i++ {
		sup.check(t)
	}

	// The stub ignored SIGTERM, so only SIGKILL could end it, and the
	// replacement then waits out the backoff before it starts.
	waitDead(t, pid1)
	advance(DefaultStageUISupervisorBackoff + time.Second)
	sup.check(t)
	pid2 := child.waitPidFileChange(t, pid1)
	if pid2 == pid1 {
		t.Fatalf("upstream not replaced (pid %d)", pid1)
	}
	waitAlive(t, pid2)
	if st := sup.status(); st.Restarts != 1 {
		t.Errorf("restarts = %d, want 1", st.Restarts)
	}
}

// TestStageUISupervisor_SlowStartSurvivesTheStartGrace pins the first half of
// defect 1: a child the supervisor just started is not struck out while it is
// still booting. Three failed checks used to be fatal at the 30 s mark, so a
// child that answers health a little later than that was killed and restarted
// forever.
func TestStageUISupervisor_SlowStartSurvivesTheStartGrace(t *testing.T) {
	// The delay is wall-clock (the stub is a real process) while the checks
	// below advance the test clock by a full interval each: the supervisor sees
	// a child that is still booting after four checks, which is one more than
	// the three strikes that used to be fatal.
	child := newStubChild(t, stubChildParams{healthDelayMS: 2000})
	sup, advance := testSupervisor(t, child.supervisorConfig(t))
	requireDefaultStartGrace(t, sup)

	sup.check(t) // start the slow child
	pid := child.waitPidFromFile(t)
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	sup.healthURL = child.healthURL(t, pid)

	// The child is listening but answers 503 while it boots: more failed checks
	// than the strike limit, all of them inside the grace and none of them a
	// strike.
	for i := 1; i <= StageUISupervisorUnhealthyChecks+1; i++ {
		sup.check(t)
		st := sup.status()
		if st.Healthy {
			t.Fatalf("check %d: the booting child read as healthy (%+v)", i, st)
		}
		if st.Restarts != 0 || st.PID != pid || st.Adopted {
			t.Fatalf("check %d: the booting child was struck out: %+v", i, st)
		}
		if !processAlive(pid) {
			t.Fatalf("check %d: the booting child was signalled", i)
		}
		advance(sup.interval)
	}
	if got := child.starts(t); got != 1 {
		t.Fatalf("the slow child was started %d times, want 1", got)
	}

	// It is up for real now: the next check sees health, and the child is still
	// the one this supervisor started.
	waitHealthy(t, sup.healthURL)
	sup.check(t)
	st := sup.status()
	if !st.Healthy || st.Adopted || st.PID != pid || st.Restarts != 0 {
		t.Fatalf("status once the slow child came up: %+v (pid %d)", st, pid)
	}
	if got := child.starts(t); got != 1 {
		t.Errorf("stub started %d times, want 1", got)
	}
}

// TestStageUISupervisor_NeverHealthyIsReplacedOnlyAfterTheGrace pins the other
// half of defect 1: a child that never answers health is replaced only once its
// start grace is over, and every replacement after that is spaced by the same
// growing backoff a crash feeds — 2 s, 4 s, ... — so a Stage that cannot come
// up never loops hot.
func TestStageUISupervisor_NeverHealthyIsReplacedOnlyAfterTheGrace(t *testing.T) {
	child := newStubChild(t, stubChildParams{mode: "unhealthy"})
	sup, advance := testSupervisor(t, child.supervisorConfig(t))
	requireDefaultStartGrace(t, sup)

	sup.check(t) // start the child that will never answer health
	pid1 := child.waitPidFromFile(t)
	waitAlive(t, pid1)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	sup.healthURL = child.healthURL(t, pid1)

	// The whole grace, and the strikes that follow it up to the deciding one:
	// nothing here replaces the child. Without a grace the third check (30 s)
	// would already have killed it.
	walkToStrikeLimit(t, sup, advance, child, pid1, 0)

	// The strike limit past the grace: the child goes, and the replacement has
	// to wait out the first backoff step.
	sup.check(t)
	waitDead(t, pid1)
	st := sup.status()
	if st.Restarts != 1 || st.PID != 0 {
		t.Fatalf("status at the first replacement: %+v", st)
	}
	if want := DefaultStageUISupervisorBackoff.Seconds(); st.BackoffS != want {
		t.Fatalf("backoff_s = %v at the first replacement, want %v", st.BackoffS, want)
	}

	advance(DefaultStageUISupervisorBackoff - time.Second)
	sup.check(t) // still inside the 2 s window: nothing starts
	if got := child.pidFromFile(t); got != pid1 {
		t.Fatalf("a replacement started inside the 2 s backoff (pidfile names %d)", got)
	}
	if got := child.starts(t); got != 1 {
		t.Fatalf("stub started %d times inside the backoff, want 1", got)
	}

	advance(2 * time.Second) // past the window: the replacement starts
	sup.check(t)
	pid2 := child.waitPidFileChange(t, pid1)
	waitAlive(t, pid2)
	if got := child.waitStarts(t, 2); got != 2 {
		t.Fatalf("stub started %d times, want 2", got)
	}
	sup.healthURL = child.healthURL(t, pid2)

	// The second replacement is spaced twice as far out: same walk, same strike
	// limit, and 4 s — not 2 — before the next child may start.
	walkToStrikeLimit(t, sup, advance, child, pid2, 1)
	sup.check(t)
	waitDead(t, pid2)
	st = sup.status()
	if st.Restarts != 2 || st.PID != 0 {
		t.Fatalf("status at the second replacement: %+v", st)
	}
	if want := (2 * DefaultStageUISupervisorBackoff).Seconds(); st.BackoffS != want {
		t.Fatalf("backoff_s = %v at the second replacement, want %v", st.BackoffS, want)
	}

	advance(3 * time.Second)
	sup.check(t) // less than 4 s: still nothing
	if got := child.pidFromFile(t); got != pid2 {
		t.Fatalf("a replacement started inside the 4 s backoff (pidfile names %d)", got)
	}
	advance(2 * time.Second) // 5 s: past the window
	sup.check(t)
	pid3 := child.waitPidFileChange(t, pid2)
	waitAlive(t, pid3)
	if got := child.waitStarts(t, 3); got != 3 {
		t.Fatalf("stub started %d times, want 3", got)
	}
}

// TestStageUISupervisor_ForeignPidfileProcessIsNeverSignalled pins defect 2: a
// stale pidfile can name a pid that by now belongs to an unrelated process, and
// the supervisor must never SIGTERM or SIGKILL a pid it did not start and has
// never seen answer health. It watches the pid, warns once, and after the grace
// plus the strike limit treats the pidfile itself as stale.
func TestStageUISupervisor_ForeignPidfileProcessIsNeverSignalled(t *testing.T) {
	logs := captureStageLogs(t)

	// A process this kernel did not start, and that never answers health: from
	// the outside, exactly what a recycled pid looks like.
	child := newStubChild(t, stubChildParams{mode: "unhealthy"})
	stub := child.stubSpawn(t)
	// The stub is this test's own doing, not the supervisor's, so the test ends
	// it (stubSpawn's own cleanup reaps it, and only a reap makes the pid stop
	// reading as alive).
	t.Cleanup(func() { _ = killProcess(stub) })
	if err := os.WriteFile(child.pidFile, []byte(strconv.Itoa(stub)+"\n"), 0o644); err != nil {
		t.Fatalf("write pidfile: %v", err)
	}
	sup, advance := testSupervisor(t, child.supervisorConfig(t))
	requireDefaultStartGrace(t, sup)
	sup.healthURL = child.healthURL(t, stub)

	// Its own grace, then the strikes up to the deciding one: the stub is alive
	// and unsignalled throughout, and the pidfile still names it.
	walkToStrikeLimit(t, sup, advance, child, stub, 0)

	// The strike limit: the pidfile is stale, so the pidfile goes and a child of
	// the supervisor's own takes its place — the stub is left alone.
	sup.check(t)
	if !processAlive(stub) {
		t.Fatalf("the supervisor signalled a pid it never started and never saw answer health")
	}
	st := sup.status()
	if st.Restarts != 1 || st.PID == 0 || st.PID == stub {
		t.Fatalf("status after the stale pidfile: %+v (stub %d)", st, stub)
	}
	if st.BackoffS != 0 {
		t.Errorf("backoff_s = %v after a stale pidfile, want 0 (the new child starts at once)", st.BackoffS)
	}
	pid := child.waitPidFileChange(t, stub)
	if pid != st.PID {
		t.Fatalf("pidfile names %d, status pid %d", pid, st.PID)
	}
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, child.pidFromFile(t)) })
	if !processAlive(stub) {
		t.Fatalf("stub %d was signalled during the stale-pidfile replacement", stub)
	}
	if got := child.waitStarts(t, 2); got != 2 {
		t.Fatalf("stubs started %d times, want the stub plus exactly one supervisor child", got)
	}
	// "Never signal it" comes with one warning, not one per check.
	if n := strings.Count(logs.String(), "not this kernel's to signal"); n != 1 {
		t.Errorf("the not-ours-to-signal warning appears %d times, want 1:\n%s", n, logs.String())
	}
}

func TestStageUISupervisor_FailedStartBacksOffAndCaps(t *testing.T) {
	dir := t.TempDir()
	cfg := StageUISupervisorConfig{
		Cmd:     []string{filepath.Join(dir, "no-such-upstream-binary")},
		Cwd:     dir,
		PidFile: filepath.Join(dir, "stage.pid"),
		LogFile: filepath.Join(dir, "stage.log"),
	}
	sup, advance := testSupervisor(t, cfg)

	// 2 s doubling to the 60 s cap.
	for i, want := range []float64{2, 4, 8, 16, 32, 60, 60} {
		sup.check(t)
		st := sup.status()
		if st.BackoffS != want {
			t.Fatalf("attempt %d: backoff_s = %v, want %v", i+1, st.BackoffS, want)
		}
		if st.LastError == "" {
			t.Fatalf("attempt %d: last_error empty after a failed start", i+1)
		}
		if st.PID != 0 {
			t.Fatalf("attempt %d: pid = %d after a failed start", i+1, st.PID)
		}
		advance(time.Duration(want)*time.Second + time.Second)
	}
	if got := fileSize(t, cfg.LogFile); got != 0 {
		t.Errorf("log_file has %d bytes after failed starts, want 0", got)
	}
}

func TestStageUISupervisor_StopLeavesChildRunning(t *testing.T) {
	child := newStubChild(t, stubChildParams{})
	sup, err := newStageUISupervisor(StageUIConfig{Supervise: child.supervisorConfig(t)})
	if err != nil {
		t.Fatalf("newStageUISupervisor: %v", err)
	}
	sup.start() // the loop's first check is immediate
	pid := child.waitPidFromFile(t)
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, pid) })

	sup.stop()

	if !processAlive(pid) {
		t.Fatalf("Stop killed the child (pid %d); stopping the kernel must not", pid)
	}
	st := sup.status()
	if st.PID != pid || !st.Enabled {
		t.Errorf("status after Stop: %+v", st)
	}
	// The loop is gone: another Stop is a no-op, not a panic.
	sup.stop()
}

func TestStageUISupervisor_EnvFileReachesChild(t *testing.T) {
	child := newStubChild(t, stubChildParams{
		envDump: true,
		extraEnv: []string{
			"STAGE_UI_SCOPE_MARKER=from-env-file",
			"STAGE_UI_SECRET=correct-horse-battery-staple",
		},
	})
	t.Setenv("STAGE_UI_PARENT_MARKER", "from-parent-env")
	logs := captureStageLogs(t)

	sup, _ := testSupervisor(t, child.supervisorConfig(t))
	sup.check(t)
	pid := child.waitPidFromFile(t)
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, pid) })

	deadline := time.Now().Add(10 * time.Second)
	var dump string
	for {
		if b, err := os.ReadFile(child.dumpFile); err == nil && len(b) > 0 {
			dump = string(b)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stub never dumped its environment")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, want := range []string{
		"STAGE_UI_SCOPE_MARKER=from-env-file",    // from env_file
		"STAGE_UI_PARENT_MARKER=from-parent-env", // from the parent environment
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("child environment is missing %q", want)
		}
	}
	// The env_file's VALUES must never reach the log: only key names, their
	// count and paths may be logged.
	logged := logs.String()
	if strings.Contains(logged, "correct-horse-battery-staple") {
		t.Errorf("env_file value leaked into the supervisor log:\n%s", logged)
	}
	if !strings.Contains(logged, child.envFile) {
		t.Errorf("expected the env_file path (not values) in the log:\n%s", logged)
	}
	if !strings.Contains(logged, "keys=") {
		t.Errorf("expected the env_file key count in the log:\n%s", logged)
	}
}

// ─── the status route ────────────────────────────────────────────────────────

func TestStageUISupervisor_StatusRouteFields(t *testing.T) {
	up, hits := ownedUpstream(t)
	child := newStubChild(t, stubChildParams{})
	sup, _ := testSupervisor(t, child.supervisorConfig(t))

	withSup := ownedHandler(t, up.URL, StageUIConfig{supervisor: func() *stageUISupervisor { return sup }})
	off := ownedHandler(t, up.URL, StageUIConfig{})

	// Absent supervise block: the route still answers, with enabled=false.
	var st stageUISupervisorStatus
	decodeStatus(t, ownedRequest(t, off, http.MethodGet, stageUISupervisorPath, nil), &st)
	if st.Enabled {
		t.Errorf("enabled = true with no supervise block: %+v", st)
	}
	if st.PID != 0 || st.Healthy || st.Adopted || st.Restarts != 0 || st.LastStart != "" || st.LastError != "" || st.BackoffS != 0 {
		t.Errorf("disabled status is not the zero value: %+v", st)
	}

	// Configured but not yet started: enabled, nothing else.
	decodeStatus(t, ownedRequest(t, withSup, http.MethodGet, stageUISupervisorPath, nil), &st)
	if !st.Enabled || st.PID != 0 || st.Healthy || st.Adopted || st.BackoffS != 0 {
		t.Errorf("pre-start status: %+v", st)
	}

	// After a start and a healthy check the whole record is populated.
	sup.check(t)
	pid := child.waitPidFromFile(t)
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, pid) })
	sup.healthURL = child.healthURL(t, pid)
	sup.check(t)

	rec := ownedRequest(t, withSup, http.MethodGet, stageUISupervisorPath, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status route: %d %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	decodeStatus(t, rec, &st)
	if !st.Enabled || !st.Healthy || st.Adopted || st.PID != pid || st.Restarts != 0 || st.BackoffS != 0 {
		t.Errorf("live status: %+v (pid %d)", st, pid)
	}
	if _, err := time.Parse(time.RFC3339, st.LastStart); err != nil {
		t.Errorf("last_start %q is not RFC3339: %v", st.LastStart, err)
	}

	// GET only, and never proxied to the upstream.
	if rec := ownedRequest(t, withSup, http.MethodPost, stageUISupervisorPath, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status route: %d, want 405", rec.Code)
	} else if allow := rec.Header().Get("Allow"); allow != http.MethodGet {
		t.Errorf("Allow = %q, want GET", allow)
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("the status route reached the upstream %d times", n)
	}
}

func decodeStatus(t *testing.T, rec *httptest.ResponseRecorder, st *stageUISupervisorStatus) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status route: %d %q", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), st); err != nil {
		t.Fatalf("status decode: %v (body %q)", err, rec.Body.String())
	}
}

// ─── config, YAML and server wiring ──────────────────────────────────────────

func TestStageUISupervisor_ConfigYAML(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, ".cog", "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StageUI.Supervise.Cmd != nil || cfg.StageUI.Supervise.enabled() {
		t.Fatalf("supervisor must default off: %+v", cfg.StageUI.Supervise)
	}

	y := "stage_ui:\n" +
		"  addr: 127.0.0.1:7873\n" +
		"  upstream: http://127.0.0.1:7872\n" +
		"  supervise:\n" +
		"    cmd: [/usr/bin/python3, -m, stage]\n" +
		"    cwd: /srv/stage\n" +
		"    env_file: /etc/cogos/stage.env\n" +
		"    health_url: http://127.0.0.1:7999/healthz\n" +
		"    pidfile: /run/cogos/stage.pid\n" +
		"    log_file: /var/log/cogos/stage.log\n"
	if err := os.WriteFile(filepath.Join(ws, ".cog", "config", "kernel.yaml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.StageUI.Supervise
	if !s.enabled() || len(s.Cmd) != 3 || s.Cmd[0] != "/usr/bin/python3" || s.Cmd[2] != "stage" {
		t.Fatalf("supervise.cmd not applied: %+v", s)
	}
	if s.Cwd != "/srv/stage" || s.EnvFile != "/etc/cogos/stage.env" || s.PidFile != "/run/cogos/stage.pid" || s.LogFile != "/var/log/cogos/stage.log" {
		t.Fatalf("supervise paths not applied: %+v", s)
	}
	if got := stageUISupervisorHealthURL(cfg.StageUI); got != "http://127.0.0.1:7999/healthz" {
		t.Errorf("health url = %q, want the configured one", got)
	}

	// An explicit health_url is optional: the default is upstream + /healthz,
	// and the built-in upstream is used when none is configured.
	y = "stage_ui:\n  supervise:\n    cmd: [/bin/stage]\n    pidfile: /run/stage.pid\n    log_file: /var/log/stage.log\n"
	if err := os.WriteFile(filepath.Join(ws, ".cog", "config", "kernel.yaml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stageUISupervisorHealthURL(cfg.StageUI), DefaultStageUIUpstream+"/healthz"; got != want {
		t.Errorf("default health url = %q, want %q", got, want)
	}
	cfg.StageUI.Upstream = "http://127.0.0.1:7872/"
	if got, want := stageUISupervisorHealthURL(cfg.StageUI), "http://127.0.0.1:7872/healthz"; got != want {
		t.Errorf("health url with a trailing slash = %q, want %q", got, want)
	}

	// A block that cannot supervise is a config error, never a silent start.
	for _, bad := range []StageUISupervisorConfig{
		{},
		{Cmd: []string{"/bin/stage"}},
		{Cmd: []string{"/bin/stage"}, PidFile: "/run/stage.pid"},
		{Cmd: []string{"/bin/stage"}, PidFile: "/run/stage.pid", LogFile: "/var/log/stage.log", EnvFile: filepath.Join(t.TempDir(), "missing.env")},
	} {
		if _, err := newStageUISupervisor(StageUIConfig{Supervise: bad}); err == nil {
			t.Errorf("newStageUISupervisor(%+v) = nil error, want a config error", bad)
		}
	}
}

func TestStageUISupervisor_ServerLifecycle(t *testing.T) {
	child := newStubChild(t, stubChildParams{})
	s := &Server{cfg: &Config{StageUI: StageUIConfig{
		Addr:      "127.0.0.1:0",
		Upstream:  "http://127.0.0.1:1",
		Supervise: child.supervisorConfig(t),
	}}}

	s.startStageUI()
	if s.stageUISrv == nil {
		t.Fatal("listener not started")
	}
	if s.stageUISup == nil {
		t.Fatal("supervisor not started with the server")
	}
	pid := child.waitPidFromFile(t)
	waitAlive(t, pid)
	t.Cleanup(func() { killAndWait(t, pid) })

	// The listener serves the status route (guard accepts the loopback host).
	resp, err := http.Get("http://" + s.stageUIAddr + stageUISupervisorPath)
	if err != nil {
		t.Fatalf("status route: %v", err)
	}
	defer resp.Body.Close()
	var st stageUISupervisorStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	if !st.Enabled || st.PID != pid {
		t.Errorf("status on the listener: %+v (pid %d)", st, pid)
	}

	s.stopStageUI()
	if !processAlive(pid) {
		t.Fatalf("stopStageUI killed the child (pid %d)", pid)
	}
	if _, err := http.Get("http://" + s.stageUIAddr + "/hello"); err == nil {
		t.Error("listener still up after stopStageUI")
	}

	// A supervise block without a listener still runs the upstream.
	child2 := newStubChild(t, stubChildParams{})
	s2 := &Server{cfg: &Config{StageUI: StageUIConfig{Supervise: child2.supervisorConfig(t)}}}
	s2.startStageUI()
	if s2.stageUISrv != nil {
		t.Error("listener started with an empty addr")
	}
	if s2.stageUISup == nil {
		t.Fatal("supervisor not started without a listener")
	}
	pid2 := child2.waitPidFromFile(t)
	waitAlive(t, pid2)
	t.Cleanup(func() { killAndWait(t, pid2) })
	s2.stopStageUI()
	if !processAlive(pid2) {
		t.Fatalf("stopStageUI killed the child (pid %d)", pid2)
	}
}

// fileSize reports the size of path, or 0 when it does not exist.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}
