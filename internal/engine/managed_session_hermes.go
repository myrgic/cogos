// managed_session_hermes.go — HermesACPDriver: the kernel owns Hermes ACP
// agent processes (`hermes --profile <p> acp`) and the ACP sessions inside
// them, behind the ADR-093 ManagedSession lifecycle.
//
// Brief: cog apps/stage/docs/briefs/agent-processes-in-kernel.md. The stage
// used to spawn these processes as its own children, so restarting the stage
// meant waiting for (or killing) every in-flight turn. With this driver the
// kernel holds the pipes, the in-flight session/prompt calls and any parked
// session/request_permission; the stage becomes a reconnectable view over
// the HTTP/WS surface in serve_managed_sessions.go.
//
// Shape:
//   - hermesProcess: one agent subprocess + its acprpc.Conn. ACP multiplexes
//     sessions over one connection, and session/fork must run in the process
//     that holds the parent, so a fork shares its parent's process; a fresh
//     session (or a load of an on-disk session) gets its own process.
//   - HermesACPSession: one ACP session — event ring, lifecycle state,
//     at-most-one in-flight turn, and parked permission requests.
//
// Crash policy (ADR-093 §4): a process that dies within CrashWindow of its
// start marks its sessions Crashed and is not retried (a broken install or
// bad profile surfaces instead of looping). A later death restarts the
// process with exponential backoff up to MaxRestarts, re-initializes and
// session/load's each of its sessions; the turn that was in flight is lost
// and reported as a turn_end with error "agent process exited".
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myrgic/cogos/internal/acprpc"
)

// DriverHermesACP is the driver name accepted by POST /v1/managed-sessions.
const DriverHermesACP = "hermes-acp"

// Errors surfaced by the driver (mapped to HTTP statuses by the handlers).
var (
	ErrManagedSessionNotFound = errors.New("managed session: not found")
	ErrTurnInFlight           = errors.New("managed session: a turn is already in flight")
	ErrInvalidManagedSession  = errors.New("managed session: invalid request")
	ErrPermissionNotFound     = errors.New("managed session: no such pending permission request")
)

// HermesACPOpts configures one Create call.
type HermesACPOpts struct {
	Profile    string            `json:"profile,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	ForkOf     string            `json:"fork_of,omitempty"`     // parent session id (must be managed here)
	Load       string            `json:"load,omitempty"`        // existing on-disk session id to session/load
	McpServers []json.RawMessage `json:"mcp_servers,omitempty"` // passed through verbatim
}

// HermesACPDriverConfig tunes the driver. Zero values pick defaults.
type HermesACPDriverConfig struct {
	// Command builds the agent argv for a profile. Default:
	// ["hermes", "--profile", profile, "acp"] (profile omitted when empty).
	Command func(profile string) []string
	// Env is the subprocess environment (default os.Environ()).
	Env []string
	// RingSize bounds each session's replay buffer.
	RingSize int
	// PermissionTimeout is how long a permission request waits in the kernel
	// for an answer before resolving as cancelled. Default 30 minutes — long
	// enough to survive a stage restart, short enough not to wedge a turn.
	PermissionTimeout time.Duration
	// InitTimeout bounds initialize + session/new|fork|load. Default 3 min.
	InitTimeout time.Duration
	// CrashWindow, MaxRestarts, RestartBackoff: ADR-093 §4 restart policy.
	CrashWindow    time.Duration // default 10s
	MaxRestarts    int           // default 3
	RestartBackoff time.Duration // default 1s, doubled per restart

	// afterRestartLoad is a test seam: called after each restart's
	// session/load returns, before its state write.
	afterRestartLoad func(*HermesACPSession)
	// afterCreateCall is a test seam: called in Create after the agent's
	// session/new|load|fork returned, before the session is registered Live.
	afterCreateCall func(*hermesProcess)
}

func (c *HermesACPDriverConfig) defaults() {
	if c.Command == nil {
		c.Command = func(profile string) []string {
			if profile == "" {
				return []string{"hermes", "acp"}
			}
			return []string{"hermes", "--profile", profile, "acp"}
		}
	}
	if c.RingSize <= 0 {
		c.RingSize = DefaultManagedEventRingSize
	}
	if c.PermissionTimeout <= 0 {
		c.PermissionTimeout = 30 * time.Minute
	}
	if c.InitTimeout <= 0 {
		c.InitTimeout = 3 * time.Minute
	}
	if c.CrashWindow <= 0 {
		c.CrashWindow = 10 * time.Second
	}
	if c.MaxRestarts <= 0 {
		c.MaxRestarts = 3
	}
	if c.RestartBackoff <= 0 {
		c.RestartBackoff = time.Second
	}
}

// HermesACPDriver owns Hermes ACP processes and the sessions inside them.
type HermesACPDriver struct {
	cfg HermesACPDriverConfig

	mu       sync.Mutex
	sessions map[string]*HermesACPSession
	loads    map[string]*loadClaim // in-flight Create{Load: id}, one per id
	closed   bool
}

// loadClaim is the single in-flight load of one session id. A second
// Create{Load: id} (an overlapping /resume) waits on done and returns the
// same result instead of spawning a second process for the same session.
type loadClaim struct {
	done      chan struct{}
	sess      *HermesACPSession
	err       error
	cancelled bool // Delete(id) arrived while the load was in flight; guarded by HermesACPDriver.mu
}

// NewHermesACPDriver returns a driver with no sessions.
func NewHermesACPDriver(cfg HermesACPDriverConfig) *HermesACPDriver {
	cfg.defaults()
	return &HermesACPDriver{cfg: cfg, sessions: map[string]*HermesACPSession{}, loads: map[string]*loadClaim{}}
}

// ── process ─────────────────────────────────────────────────────────────────

type hermesProcess struct {
	d       *HermesACPDriver
	profile string
	cwd     string

	mu        sync.Mutex
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	conn      *acprpc.Conn
	startedAt time.Time
	restarts  int
	stopping  bool
	sessions  map[string]*HermesACPSession
	agentInfo json.RawMessage
}

func (d *HermesACPDriver) newProcess(profile, cwd string) *hermesProcess {
	return &hermesProcess{d: d, profile: profile, cwd: cwd, sessions: map[string]*HermesACPSession{}}
}

// start spawns the subprocess and runs initialize. The process is NOT tied
// to any request context: the kernel owns its lifetime.
func (p *hermesProcess) start(ctx context.Context) (*acprpc.Conn, error) {
	argv := p.d.cfg.Command(p.profile)
	if len(argv) == 0 {
		return nil, errors.New("hermes-acp: empty command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = p.cwd
	cmd.Env = p.d.cfg.Env
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("hermes-acp: start %v: %w", argv, err)
	}
	conn := acprpc.NewConn(stdout, stdin, p)
	p.mu.Lock()
	p.cmd, p.stdin, p.conn, p.startedAt = cmd, stdin, conn, time.Now()
	p.mu.Unlock()

	ictx, cancel := context.WithTimeout(ctx, p.d.cfg.InitTimeout)
	defer cancel()
	var res struct {
		AgentInfo json.RawMessage `json:"agentInfo"`
	}
	err = conn.Call(ictx, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{"fs": map[string]any{"readTextFile": false, "writeTextFile": false}, "terminal": false},
		"clientInfo":         map[string]any{"name": "cogos-kernel", "version": "0.1"},
	}, &res)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("hermes-acp: initialize: %w", err)
	}
	p.mu.Lock()
	p.agentInfo = res.AgentInfo
	p.mu.Unlock()
	go p.watch(cmd, conn)
	return conn, nil
}

func (p *hermesProcess) currentConn() *acprpc.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn
}

// watch waits for the process to exit and applies the crash policy.
func (p *hermesProcess) watch(cmd *exec.Cmd, conn *acprpc.Conn) {
	<-conn.Done()
	waitErr := cmd.Wait()

	p.mu.Lock()
	stopping := p.stopping
	lived := time.Since(p.startedAt)
	restarts := p.restarts
	sessions := make([]*HermesACPSession, 0, len(p.sessions))
	for _, s := range p.sessions {
		sessions = append(sessions, s)
	}
	p.mu.Unlock()

	if stopping {
		for _, s := range sessions {
			s.setState(StateDetached, nil)
		}
		return
	}
	exitErr := waitErr
	if exitErr == nil {
		exitErr = errors.New("agent process exited")
	}
	if lived < p.d.cfg.CrashWindow || restarts >= p.d.cfg.MaxRestarts {
		for _, s := range sessions {
			s.setState(StateCrashed, exitErr)
		}
		return
	}
	for _, s := range sessions {
		s.setState(StateStarting, exitErr)
	}
	time.Sleep(p.d.cfg.RestartBackoff << restarts)
	p.mu.Lock()
	p.restarts++
	if p.stopping {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	conn, err := p.start(context.Background())
	if err != nil {
		for _, s := range sessions {
			s.setState(StateCrashed, err)
		}
		return
	}
	for _, s := range sessions {
		// A session deleted during the backoff or the restart is gone from
		// p.sessions: do not load it into the new process.
		if p.session(s.id) != s {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.d.cfg.InitTimeout)
		err := conn.Call(ctx, "session/load", map[string]any{
			"sessionId": s.id, "cwd": s.cwd, "mcpServers": s.mcpServers(),
		}, nil)
		cancel()
		if hook := p.d.cfg.afterRestartLoad; hook != nil {
			hook(s)
		}
		if err != nil {
			p.setIfCurrent(conn, s, StateCrashed, fmt.Errorf("restart: session/load: %w", err))
			continue
		}
		p.setIfCurrent(conn, s, StateLive, nil)
	}
}

// setIfCurrent applies a state change decided by a restart that loaded
// sessions into conn, but only while conn is still this process's current
// connection and has not died. This goroutine is the OLD generation's
// watcher: the new generation has its own watch() that decides its own
// crash policy, and a stale Live (or Crashed) landing after that decision
// would overwrite it, leaving a session "live" over a dead connection with
// no retry. Checked under p.mu, the lock the new watcher takes to snapshot
// its sessions, so either this write lands first and the new watcher's
// verdict follows it, or conn is already Done and this write is skipped.
func (p *hermesProcess) setIfCurrent(conn *acprpc.Conn, s *HermesACPSession, st ManagedSessionState, err error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != conn {
		return false
	}
	select {
	case <-conn.Done():
		return false
	default:
	}
	s.setState(st, err)
	return true
}

// stop closes stdin (graceful), then kills after a grace period.
func (p *hermesProcess) stop() {
	p.mu.Lock()
	p.stopping = true
	cmd, stdin, conn := p.cmd, p.stdin, p.conn
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = stdin.Close()
	select {
	case <-conn.Done():
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
	}
}

func (p *hermesProcess) session(id string) *HermesACPSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessions[id]
}

// HandleNotification routes session/update into the owning session's ring.
func (p *hermesProcess) HandleNotification(method string, params json.RawMessage) {
	if method != "session/update" {
		return
	}
	var u struct {
		SessionID string          `json:"sessionId"`
		Update    json.RawMessage `json:"update"`
	}
	if json.Unmarshal(params, &u) != nil {
		return
	}
	if s := p.session(u.SessionID); s != nil {
		s.ring.Append(MSEventSessionUpdate, u.Update)
	}
}

// HandleRequest answers agent→client requests. session/request_permission
// parks in the kernel until a client answers or the timeout fires.
func (p *hermesProcess) HandleRequest(method string, params json.RawMessage) (any, error) {
	if method != "session/request_permission" {
		return nil, &acprpc.Error{Code: acprpc.CodeMethodNotFound, Message: "cogos kernel client does not support " + method}
	}
	var hdr struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(params, &hdr)
	s := p.session(hdr.SessionID)
	if s == nil {
		return permissionOutcome(""), nil
	}
	return permissionOutcome(s.parkPermission(params)), nil
}

func permissionOutcome(optionID string) map[string]any {
	if optionID == "" {
		return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}
	}
	return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": optionID}}
}

// ── session ─────────────────────────────────────────────────────────────────

// HermesACPSession is one kernel-managed ACP session.
type HermesACPSession struct {
	id        string
	profile   string
	cwd       string
	forkOf    string
	createdAt time.Time
	proc      *hermesProcess
	ring      *EventRing
	permTO    time.Duration
	mcp       []json.RawMessage

	mu       sync.Mutex
	state    ManagedSessionState
	lastErr  error
	turn     *hermesTurn
	mode     string
	perms    map[string]*pendingPermission
	permSeq  atomic.Uint64
	turnSeq  atomic.Uint64
	detached bool
}

type hermesTurn struct {
	id        string
	startedAt time.Time
}

type pendingPermission struct {
	ID        string          `json:"request_id"`
	Params    json.RawMessage `json:"params"`
	CreatedAt time.Time       `json:"created_at"`
	answer    chan string
}

// ManagedSessionInfo is the JSON view of a session (GET routes).
type ManagedSessionInfo struct {
	SessionID          string               `json:"session_id"`
	Driver             string               `json:"driver"`
	Profile            string               `json:"profile,omitempty"`
	Cwd                string               `json:"cwd,omitempty"`
	ForkOf             string               `json:"fork_of,omitempty"`
	State              string               `json:"state"`
	Error              string               `json:"error,omitempty"`
	Mode               string               `json:"mode,omitempty"`
	CreatedAt          time.Time            `json:"created_at"`
	LastSeq            uint64               `json:"last_seq"`
	TurnInFlight       string               `json:"turn_in_flight,omitempty"`
	PendingPermissions []*pendingPermission `json:"pending_permissions"`
}

func (s *HermesACPSession) ID() string         { return s.id }
func (s *HermesACPSession) Events() *EventRing { return s.ring }
func (s *HermesACPSession) mcpServers() []json.RawMessage {
	if s.mcp == nil {
		return []json.RawMessage{}
	}
	return s.mcp
}

// State returns the current ADR-093 lifecycle state.
func (s *HermesACPSession) State() ManagedSessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *HermesACPSession) setState(st ManagedSessionState, err error) {
	s.mu.Lock()
	// Detached is terminal: Delete() removed the session, so a restart that
	// snapshotted it earlier must not resurrect it (Starting/Live/Crashed).
	if s.state == StateDetached || (s.state == st && err == nil) {
		s.mu.Unlock()
		return
	}
	s.state, s.lastErr = st, err
	s.mu.Unlock()
	data := map[string]any{"state": st.String()}
	if err != nil {
		data["error"] = err.Error()
	}
	s.ring.Append(MSEventState, data)
	if st == StateCrashed || st == StateDetached {
		s.cancelAllPermissions("session " + st.String())
	}
}

// Info snapshots the session for the HTTP surface.
func (s *HermesACPSession) Info() ManagedSessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := ManagedSessionInfo{
		SessionID: s.id, Driver: DriverHermesACP, Profile: s.profile, Cwd: s.cwd, ForkOf: s.forkOf,
		State: s.state.String(), Mode: s.mode, CreatedAt: s.createdAt, LastSeq: s.ring.LastSeq(),
		PendingPermissions: make([]*pendingPermission, 0, len(s.perms)),
	}
	if s.lastErr != nil {
		info.Error = s.lastErr.Error()
	}
	if s.turn != nil {
		info.TurnInFlight = s.turn.id
	}
	for _, p := range s.perms {
		info.PendingPermissions = append(info.PendingPermissions, p)
	}
	return info
}

func (s *HermesACPSession) live() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateLive {
		return fmt.Errorf("%w: session %s is %s", ErrSessionNotLive, s.id, s.state)
	}
	return nil
}

// Prompt starts a turn and returns its id at once; the turn runs in the
// kernel and ends with a turn_end event. content is either a string or an
// ACP content-block array (passed through verbatim).
func (s *HermesACPSession) Prompt(content json.RawMessage) (string, error) {
	if err := s.live(); err != nil {
		return "", err
	}
	blocks := content
	var text string
	if json.Unmarshal(content, &text) == nil {
		blocks, _ = json.Marshal([]map[string]any{{"type": "text", "text": text}})
	}
	s.mu.Lock()
	if s.state != StateLive { // re-check under the lock that registers the turn: it may have changed since live()
		st := s.state
		s.mu.Unlock()
		return "", fmt.Errorf("%w: session %s is %s", ErrSessionNotLive, s.id, st)
	}
	if s.turn != nil {
		s.mu.Unlock()
		return "", ErrTurnInFlight
	}
	turn := &hermesTurn{id: "t" + strconv.FormatUint(s.turnSeq.Add(1), 10), startedAt: time.Now()}
	s.turn = turn
	s.mu.Unlock()

	s.ring.Append(MSEventTurnStart, map[string]any{"turn_id": turn.id, "prompt": blocks})
	conn := s.proc.currentConn()
	go func() {
		var res struct {
			StopReason string `json:"stopReason"`
		}
		err := conn.Call(context.Background(), "session/prompt", map[string]any{"sessionId": s.id, "prompt": blocks}, &res)
		end := map[string]any{"turn_id": turn.id, "stop_reason": res.StopReason}
		if err != nil {
			if errors.Is(err, acprpc.ErrClosed) {
				err = errors.New("agent process exited")
			}
			end["error"] = err.Error()
		}
		s.mu.Lock()
		if s.turn == turn {
			s.turn = nil
		}
		s.mu.Unlock()
		s.ring.Append(MSEventTurnEnd, end)
	}()
	return turn.id, nil
}

// Cancel sends session/cancel and resolves parked permissions as cancelled
// (the ACP contract: a cancelled turn's pending permission requests answer
// "cancelled").
func (s *HermesACPSession) Cancel() error {
	if err := s.live(); err != nil {
		return err
	}
	s.cancelAllPermissions("turn cancelled")
	return s.proc.currentConn().Notify("session/cancel", map[string]any{"sessionId": s.id})
}

// SetMode calls session/set_mode (Hermes: default | accept_edits | dont_ask).
func (s *HermesACPSession) SetMode(ctx context.Context, modeID string) error {
	if err := s.live(); err != nil {
		return err
	}
	if err := s.proc.currentConn().Call(ctx, "session/set_mode", map[string]any{"sessionId": s.id, "modeId": modeID}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.mode = modeID
	s.mu.Unlock()
	return nil
}

// parkPermission records a request, emits it, and blocks until answered,
// cancelled or timed out. Returns the chosen option id ("" = cancelled).
func (s *HermesACPSession) parkPermission(params json.RawMessage) string {
	pp := &pendingPermission{
		ID:        "perm-" + strconv.FormatUint(s.permSeq.Add(1), 10),
		Params:    params,
		CreatedAt: time.Now().UTC(),
		answer:    make(chan string, 1),
	}
	s.mu.Lock()
	if s.perms == nil {
		s.perms = map[string]*pendingPermission{}
	}
	s.perms[pp.ID] = pp
	s.mu.Unlock()
	s.ring.Append(MSEventPermissionRequest, map[string]any{"request_id": pp.ID, "params": params})

	timer := time.NewTimer(s.permTO)
	defer timer.Stop()
	select {
	case opt := <-pp.answer:
		return opt
	case <-timer.C:
		if s.takePermission(pp.ID) != nil {
			s.ring.Append(MSEventPermissionResolved, map[string]any{"request_id": pp.ID, "outcome": "cancelled", "by": "timeout"})
		}
		return ""
	}
}

func (s *HermesACPSession) takePermission(id string) *pendingPermission {
	s.mu.Lock()
	defer s.mu.Unlock()
	pp := s.perms[id]
	delete(s.perms, id)
	return pp
}

// ResolvePermission answers a parked request. optionID "" means cancelled.
func (s *HermesACPSession) ResolvePermission(requestID, optionID string) error {
	pp := s.takePermission(requestID)
	if pp == nil {
		return ErrPermissionNotFound
	}
	outcome := "selected"
	if optionID == "" {
		outcome = "cancelled"
	}
	s.ring.Append(MSEventPermissionResolved, map[string]any{"request_id": requestID, "outcome": outcome, "option_id": optionID, "by": "client"})
	pp.answer <- optionID
	return nil
}

func (s *HermesACPSession) cancelAllPermissions(by string) {
	s.mu.Lock()
	perms := s.perms
	s.perms = map[string]*pendingPermission{}
	s.mu.Unlock()
	for id, pp := range perms {
		s.ring.Append(MSEventPermissionResolved, map[string]any{"request_id": id, "outcome": "cancelled", "by": by})
		pp.answer <- ""
	}
}

// ── driver operations ───────────────────────────────────────────────────────

// Create starts a new session (fresh process + session/new), forks an
// existing managed session (its process + session/fork), or loads an
// on-disk session (fresh process + session/load). A load of an id that is
// already Live or restarting returns that session (idempotent attach,
// ADR-093 §6); overlapping loads of one id share a single process.
func (d *HermesACPDriver) Create(ctx context.Context, opts HermesACPOpts) (*HermesACPSession, error) {
	if opts.ForkOf != "" && opts.Load != "" {
		return nil, fmt.Errorf("%w: fork_of and load are mutually exclusive", ErrInvalidManagedSession)
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, errors.New("hermes-acp: driver closed")
	}
	var parent *HermesACPSession
	if opts.ForkOf != "" {
		parent = d.sessions[opts.ForkOf]
		if parent == nil {
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: fork_of %s", ErrManagedSessionNotFound, opts.ForkOf)
		}
	}
	if opts.Load == "" {
		d.mu.Unlock()
		return d.create(ctx, opts, parent)
	}
	// Decided under d.mu so two resumes of one id cannot both start a
	// process. Live: return it. Starting: the driver's own crash-restart is
	// in flight; return it too (the caller sees state=starting and tails
	// events) instead of racing it with a second process for the same id.
	if existing := d.sessions[opts.Load]; existing != nil {
		if st := existing.State(); st == StateLive || st == StateStarting {
			d.mu.Unlock()
			return existing, nil
		}
	}
	if claim := d.loads[opts.Load]; claim != nil { // a load of this id is in flight: share it
		d.mu.Unlock()
		select {
		case <-claim.done:
			return claim.sess, claim.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	claim := &loadClaim{done: make(chan struct{})}
	d.loads[opts.Load] = claim
	d.mu.Unlock()

	claim.sess, claim.err = d.create(ctx, opts, parent)

	d.mu.Lock()
	delete(d.loads, opts.Load)
	cancelled := claim.cancelled
	d.mu.Unlock()
	if cancelled && claim.sess != nil {
		// Delete(id) landed while the load was in flight and found nothing
		// registered yet: honour it now, so a deleted id is not resurrected.
		_ = d.Delete(claim.sess.id)
		claim.sess, claim.err = nil, fmt.Errorf("%w: %s deleted during load", ErrManagedSessionNotFound, opts.Load)
	}
	close(claim.done)
	return claim.sess, claim.err
}

// create does the spawning for Create; callers have already resolved the
// fork parent and (for loads) claimed the id.
func (d *HermesACPDriver) create(ctx context.Context, opts HermesACPOpts, parent *HermesACPSession) (*HermesACPSession, error) {
	mcp := opts.McpServers
	if mcp == nil {
		mcp = []json.RawMessage{}
	}
	var proc *hermesProcess
	if parent != nil {
		if err := parent.live(); err != nil {
			return nil, err
		}
		proc = parent.proc
		if opts.Cwd == "" {
			opts.Cwd = parent.cwd
		}
		if opts.Profile == "" {
			opts.Profile = parent.profile
		}
	} else {
		proc = d.newProcess(opts.Profile, opts.Cwd)
		if _, err := proc.start(ctx); err != nil {
			return nil, err
		}
	}

	cctx, cancel := context.WithTimeout(ctx, d.cfg.InitTimeout)
	defer cancel()
	var sid string
	var err error
	// Register before session/load returns: load replays history as
	// session/update notifications, which must land in the ring.
	sess := &HermesACPSession{
		profile: opts.Profile, cwd: opts.Cwd, forkOf: opts.ForkOf, createdAt: time.Now().UTC(),
		proc: proc, permTO: d.cfg.PermissionTimeout, mcp: opts.McpServers, state: StateStarting,
		perms: map[string]*pendingPermission{},
	}
	switch {
	case parent != nil:
		var r struct {
			SessionID string `json:"sessionId"`
		}
		err = proc.currentConn().Call(cctx, "session/fork", map[string]any{"sessionId": parent.id, "cwd": opts.Cwd, "mcpServers": mcp}, &r)
		sid = r.SessionID
	case opts.Load != "":
		sid = opts.Load
		sess.id, sess.ring = sid, NewEventRing(sid, d.cfg.RingSize)
		proc.mu.Lock()
		proc.sessions[sid] = sess
		proc.mu.Unlock()
		err = proc.currentConn().Call(cctx, "session/load", map[string]any{"sessionId": sid, "cwd": opts.Cwd, "mcpServers": mcp}, nil)
	default:
		var r struct {
			SessionID string `json:"sessionId"`
		}
		err = proc.currentConn().Call(cctx, "session/new", map[string]any{"cwd": opts.Cwd, "mcpServers": mcp}, &r)
		sid = r.SessionID
	}
	if err == nil && sid == "" {
		err = errors.New("agent returned no sessionId")
	}
	if err != nil {
		proc.mu.Lock()
		delete(proc.sessions, sid)
		empty := len(proc.sessions) == 0
		proc.mu.Unlock()
		if parent == nil && empty {
			proc.stop()
		}
		return nil, fmt.Errorf("hermes-acp: create session: %w", err)
	}
	if sess.ring == nil {
		sess.id, sess.ring = sid, NewEventRing(sid, d.cfg.RingSize)
		proc.mu.Lock()
		proc.sessions[sid] = sess
		proc.mu.Unlock()
	}
	if hook := d.cfg.afterCreateCall; hook != nil {
		hook(proc)
	}
	// The process may have died while the session/new|load|fork call was in
	// flight; its watcher then already decided this session's fate. Only go
	// Live over a connection that is still current and alive (same guard as
	// the restart path), else report the death instead of registering a
	// "live" session over a dead process.
	if !proc.setIfCurrent(proc.currentConn(), sess, StateLive, nil) {
		d.detach(sess)
		return nil, fmt.Errorf("hermes-acp: create session: agent process exited during session setup")
	}

	d.mu.Lock()
	replaced := d.sessions[sid]
	d.sessions[sid] = sess
	d.mu.Unlock()
	if replaced != nil && replaced != sess {
		// A Crashed/Detached entry for this id (a resume of a dead session):
		// detach it so its process is stopped and its watchers see the end,
		// instead of overwriting the registry entry and orphaning both.
		d.detach(replaced)
	}
	return sess, nil
}

// Get returns a managed session or nil.
func (d *HermesACPDriver) Get(id string) *HermesACPSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[id]
}

// List snapshots every managed session.
func (d *HermesACPDriver) List() []ManagedSessionInfo {
	d.mu.Lock()
	all := make([]*HermesACPSession, 0, len(d.sessions))
	for _, s := range d.sessions {
		all = append(all, s)
	}
	d.mu.Unlock()
	out := make([]ManagedSessionInfo, 0, len(all))
	for _, s := range all {
		out = append(out, s.Info())
	}
	return out
}

// Delete detaches a session: cancels its turn and parked permissions,
// removes it from the registry, and stops its process once no session uses
// it. Idempotent (ADR-093 §6): an unknown id is not an error.
func (d *HermesACPDriver) Delete(id string) error {
	d.mu.Lock()
	s := d.sessions[id]
	delete(d.sessions, id)
	if claim := d.loads[id]; claim != nil {
		claim.cancelled = true
	}
	d.mu.Unlock()
	if s == nil {
		return nil
	}
	d.detach(s)
	return nil
}

// detach ends a session that is no longer in (or never made it into) the
// registry: cancels its turn, leaves its process, and closes its ring.
func (d *HermesACPDriver) detach(s *HermesACPSession) {
	if s.State() == StateLive {
		_ = s.Cancel()
	}
	p := s.proc
	p.mu.Lock()
	if p.sessions[s.id] == s {
		delete(p.sessions, s.id)
	}
	empty := len(p.sessions) == 0
	p.mu.Unlock()
	s.setState(StateDetached, nil)
	if empty {
		p.stop()
	}
	s.ring.Close()
}

// TurnsInFlight counts sessions with a running turn (self-update gate seam:
// the brief says the kernel should refuse to restart while one is running).
func (d *HermesACPDriver) TurnsInFlight() int {
	n := 0
	for _, info := range d.List() {
		if info.TurnInFlight != "" {
			n++
		}
	}
	return n
}

// Close detaches every session and stops every process.
func (d *HermesACPDriver) Close() {
	d.mu.Lock()
	d.closed = true
	ids := make([]string, 0, len(d.sessions))
	for id := range d.sessions {
		ids = append(ids, id)
	}
	d.mu.Unlock()
	for _, id := range ids {
		_ = d.Delete(id)
	}
}
