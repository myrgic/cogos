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
	"regexp"
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
	// ErrManagedSessionForbidden: the request is well-formed but the caller
	// may not do it (today: a fork naming a room other than its parent's).
	ErrManagedSessionForbidden = errors.New("managed session: forbidden")
)

// HermesACPOpts configures one Create call.
type HermesACPOpts struct {
	Profile    string            `json:"profile,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	ForkOf     string            `json:"fork_of,omitempty"`     // parent session id (must be managed here)
	Load       string            `json:"load,omitempty"`        // existing on-disk session id to session/load
	McpServers []json.RawMessage `json:"mcp_servers,omitempty"` // passed through verbatim
	Room       string            `json:"room,omitempty"`        // declared room this session lives in (validated)
	// Class names a kernel-declared runtime class (managed_session_classes.go).
	// Empty = the built-in default class: today's behavior, request profile.
	Class string `json:"class,omitempty"`
	// AllowClass is the caller's class authority, set by the HTTP layer from
	// the grant (json:"-": a body can never supply it). nil = unrestricted.
	// It is asked about the EFFECTIVE class ("" for the default class).
	AllowClass func(class string) bool `json:"-"`
	// Actor is the person acting, taken from the authenticated grant by the
	// HTTP layer. json:"-" so a request body can never supply it.
	Actor string `json:"-"`
}

// roomIDPattern bounds a managed session's room name.
const roomIDPattern = `^[a-z0-9][a-z0-9-]{0,62}$`

var roomIDRe = regexp.MustCompile(roomIDPattern)

func validRoomID(s string) bool { return roomIDRe.MatchString(s) }

// HermesACPDriverConfig tunes the driver. Zero values pick defaults.
type HermesACPDriverConfig struct {
	// Command builds the agent argv for a profile. Default:
	// ["hermes", "--profile", profile, "acp"] (profile omitted when empty).
	Command func(profile string) []string
	// Env is the subprocess environment (default os.Environ()).
	Env []string
	// Classes is the runtime class registry (kernel.yaml managedSessions).
	// nil = no named classes; every named class is refused as unknown.
	Classes *RuntimeClassRegistry
	// RecordDir persists each session's runtime class (managed_session_records.go)
	// so a cold resume after a restart checks the recorded class, not the
	// request's. "" = no durable records: every cold resume reads as the
	// legacy default class.
	RecordDir string
	// JournalDir is where this driver's session journals are written —
	// <workspace>/.cog/run/sessions for the kernel's own workspace. Per driver,
	// never a process global: two kernels in one process each keep their own
	// (internal/testkernel boots two; ADR-101's isolation discipline). "" = the
	// standalone fallback in DefaultSessionJournalDir.
	JournalDir string
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

	// ProbeCacheTTL is how long a class readiness result (probed or recorded
	// from a successful create) is served before the class is probed again.
	// Default 60s.
	ProbeCacheTTL time.Duration

	// now is a test seam for the readiness cache clock; nil = time.Now.
	now func() time.Time

	// afterRestartLoad is a test seam: called after each restart's
	// session/load returns, before its state write.
	afterRestartLoad func(*HermesACPSession)
	// afterCreateCall is a test seam: called in Create after the agent's
	// session/new|load|fork returned, before the session is registered Live.
	afterCreateCall func(*hermesProcess)
	// afterRingCreate is a test seam: called the moment a loaded session's
	// event ring exists, before the session/load call that may fail. It lets a
	// test hold the ring of a load that never goes live (Create returns nil on
	// error, so the ring cannot be read off the result).
	afterRingCreate func(*EventRing)
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
	if c.ProbeCacheTTL <= 0 {
		c.ProbeCacheTTL = 60 * time.Second
	}
}

// HermesACPDriver owns Hermes ACP processes and the sessions inside them.
type HermesACPDriver struct {
	cfg HermesACPDriverConfig

	mu       sync.Mutex
	sessions map[string]*HermesACPSession
	loads    map[string]*loadClaim // in-flight Create{Load: id}, one per id
	closed   bool

	classProbeMu       sync.Mutex
	classProbeCache    map[string]ManagedSessionClassStatus
	classProbeInflight map[string]chan struct{}
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
	return &HermesACPDriver{cfg: cfg, sessions: map[string]*HermesACPSession{}, loads: map[string]*loadClaim{}, classProbeCache: map[string]ManagedSessionClassStatus{}, classProbeInflight: map[string]chan struct{}{}}
}

// invalidateClassReadiness drops a class's cached readiness entry.
func (d *HermesACPDriver) invalidateClassReadiness(name string) {
	d.classProbeMu.Lock()
	delete(d.classProbeCache, name)
	d.classProbeMu.Unlock()
}

// clockNow is the readiness cache clock (time.Now unless a test seam is set).
func (d *HermesACPDriver) clockNow() time.Time {
	if d.cfg.now != nil {
		return d.cfg.now()
	}
	return time.Now()
}

// ── process ─────────────────────────────────────────────────────────────────

type hermesProcess struct {
	d       *HermesACPDriver
	profile string
	cwd     string
	class   *ResolvedRuntimeClass // nil = the built-in default class

	mu                sync.Mutex
	cmd               *exec.Cmd
	stdin             io.WriteCloser
	conn              *acprpc.Conn
	startedAt         time.Time
	restarts          int
	stopping          bool
	sessions          map[string]*HermesACPSession
	agentInfo         json.RawMessage
	protocolVersion   int
	agentCapabilities json.RawMessage
}

func (d *HermesACPDriver) newProcess(profile, cwd string, class *ResolvedRuntimeClass) *hermesProcess {
	return &hermesProcess{d: d, profile: profile, cwd: cwd, class: class, sessions: map[string]*HermesACPSession{}}
}

// start spawns the subprocess and runs initialize. The process is NOT tied
// to any request context: the kernel owns its lifetime.
func (p *hermesProcess) start(ctx context.Context) (*acprpc.Conn, error) {
	var argv, env []string
	if p.class != nil {
		// A runtime class owns argv and env outright (allowlist, resolved at
		// create from the driver's base env). A restart reuses it verbatim.
		argv, env = p.class.Argv, p.class.Env
	} else {
		argv = p.d.cfg.Command(p.profile)
		env = p.d.cfg.Env
		if env == nil {
			env = os.Environ()
		}
	}
	if len(argv) == 0 {
		return nil, errors.New("hermes-acp: empty command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = p.cwd
	cmd.Env = env
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
		return nil, fmt.Errorf("hermes-acp: start process: %w", err)
	}
	conn := acprpc.NewConn(stdout, stdin, p)
	p.mu.Lock()
	p.cmd, p.stdin, p.conn, p.startedAt = cmd, stdin, conn, time.Now()
	p.mu.Unlock()

	ictx, cancel := context.WithTimeout(ctx, p.d.cfg.InitTimeout)
	defer cancel()
	var res struct {
		ProtocolVersion   int             `json:"protocolVersion"`
		AgentInfo         json.RawMessage `json:"agentInfo"`
		AgentCapabilities json.RawMessage `json:"agentCapabilities"`
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
	if res.ProtocolVersion != 1 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("hermes-acp: initialize: unsupported protocol version %d", res.ProtocolVersion)
	}
	p.mu.Lock()
	p.agentInfo = res.AgentInfo
	p.protocolVersion = res.ProtocolVersion
	p.agentCapabilities = res.AgentCapabilities
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
		// The class's agent is down for good: its cached readiness is stale.
		// Drop it so the next read re-probes (before the Crashed state is
		// visible, so a reader that sees Crashed never sees the old receipt).
		if p.class != nil {
			p.d.invalidateClassReadiness(p.class.Name)
		}
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
		if p.class != nil {
			p.d.invalidateClassReadiness(p.class.Name)
		}
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
		if err == nil {
			s.reapplySettings(ctx, conn) // load rebuilds with defaults: put back the client's model and mode
		}
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
	room      string // declared room; immutable after create
	class     string // runtime class name ("" = default); immutable after create
	receipt   *RuntimeClassReceipt
	actor     string // person from the creating grant; immutable after create
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
	model    string
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
	Room               string               `json:"room,omitempty"`
	Class              string               `json:"class,omitempty"`
	Receipt            *RuntimeClassReceipt `json:"receipt,omitempty"`
	Actor              string               `json:"actor,omitempty"`
	State              string               `json:"state"`
	Error              string               `json:"error,omitempty"`
	Mode               string               `json:"mode,omitempty"`
	Model              string               `json:"model,omitempty"`
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
		SessionID: s.id, Driver: DriverHermesACP, Profile: s.profile, Cwd: s.cwd, ForkOf: s.forkOf, Room: s.room, Class: s.class, Receipt: s.receipt, Actor: s.actor,
		State: s.state.String(), Mode: s.mode, Model: s.model, CreatedAt: s.createdAt, LastSeq: s.ring.LastSeq(),
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

// SetModel calls session/set_model (Hermes rebuilds the session's agent on
// modelId and keeps its history). Hermes resolves a named custom provider
// only from the full "custom:<provider>:<model>" form; the id is passed
// through verbatim and remembered so a crash-restart re-applies it.
func (s *HermesACPSession) SetModel(ctx context.Context, modelID string) error {
	if err := s.live(); err != nil {
		return err
	}
	if err := s.proc.currentConn().Call(ctx, "session/set_model", map[string]any{"sessionId": s.id, "modelId": modelID}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.model = modelID
	s.mu.Unlock()
	return nil
}

// reapplySettings restores the mode and model a client chose after a
// crash-restart's session/load, which rebuilds the session from the agent's
// store with its defaults. Best effort: a failure is recorded on the event
// stream, not fatal (the session is usable, only on the default setting).
func (s *HermesACPSession) reapplySettings(ctx context.Context, conn *acprpc.Conn) {
	s.mu.Lock()
	mode, model := s.mode, s.model
	s.mu.Unlock()
	if model != "" {
		if err := conn.Call(ctx, "session/set_model", map[string]any{"sessionId": s.id, "modelId": model}, nil); err != nil {
			s.ring.Append(MSEventState, map[string]any{"state": s.State().String(), "error": "restart: re-apply model: " + err.Error()})
		}
	}
	if mode != "" {
		if err := conn.Call(ctx, "session/set_mode", map[string]any{"sessionId": s.id, "modeId": mode}, nil); err != nil {
			s.ring.Append(MSEventState, map[string]any{"state": s.State().String(), "error": "restart: re-apply mode: " + err.Error()})
		}
	}
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
	if opts.Room != "" && !validRoomID(opts.Room) {
		return nil, fmt.Errorf("%w: room must match %s", ErrInvalidManagedSession, roomIDPattern)
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
			if opts.AllowClass != nil && !opts.AllowClass(existing.class) {
				return nil, fmt.Errorf("%w: caller may not attach this session", ErrManagedSessionForbidden)
			}
			// Every resume path (live, crashed, cold, shared load) refuses a
			// request that names a class other than the session's own.
			if opts.Class != "" && opts.Class != existing.class {
				return nil, fmt.Errorf("%w: a resume cannot change a session's class", ErrManagedSessionForbidden)
			}
			return existing, nil
		}
		// Registered but not running (e.g. Crashed): the resume keeps the
		// session's original class, and the caller needs authority over it.
		// Authority is checked first and no refusal names the recorded class,
		// so a caller cannot probe another session's class.
		if opts.AllowClass != nil && !opts.AllowClass(existing.class) {
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: caller may not attach this session", ErrManagedSessionForbidden)
		}
		if opts.Class != "" && opts.Class != existing.class {
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: a resume cannot change a session's class", ErrManagedSessionForbidden)
		}
		opts.Class = existing.class
	}
	if d.sessions[opts.Load] == nil && d.loads[opts.Load] == nil {
		// Cold load: the id is not resident (kernel restart). The recorded
		// class is the ground truth: the request may not change it, and
		// omitting it inherits the record (never a downgrade to the default).
		rec, err := d.loadRecord(opts.Load)
		if err != nil {
			// Fail closed and classify it the way DeleteAuthorized does: an
			// unreadable record is a data-integrity refusal (403), not an
			// upstream agent failure (502).
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: %v", ErrManagedSessionForbidden, err)
		}
		if opts.AllowClass != nil && !opts.AllowClass(rec.Class) {
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: caller may not attach this session", ErrManagedSessionForbidden)
		}
		if opts.Class != "" && opts.Class != rec.Class {
			d.mu.Unlock()
			return nil, fmt.Errorf("%w: a resume cannot change a session's class", ErrManagedSessionForbidden)
		}
		opts.Class = rec.Class
	}
	if claim := d.loads[opts.Load]; claim != nil { // a load of this id is in flight: share it
		d.mu.Unlock()
		select {
		case <-claim.done:
			// Every caller needs authority over the class the load resolved to.
			if claim.sess != nil && opts.AllowClass != nil && !opts.AllowClass(claim.sess.class) {
				return nil, fmt.Errorf("%w: caller may not attach this session", ErrManagedSessionForbidden)
			}
			// Same rule as the resident and cold paths: a resume cannot
			// change the session's class, even when it joins a shared load.
			if claim.sess != nil && opts.Class != "" && opts.Class != claim.sess.class {
				return nil, fmt.Errorf("%w: a resume cannot change a session's class", ErrManagedSessionForbidden)
			}
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
	var receipt *RuntimeClassReceipt
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
		// Room and actor are inherited. Cross-room fork is not in G0, so a
		// room that differs from the parent's is refused.
		// Authority over the parent's class comes first, and no refusal
		// names the parent's class or room.
		if opts.AllowClass != nil && !opts.AllowClass(parent.class) {
			return nil, fmt.Errorf("%w: caller may not fork this session", ErrManagedSessionForbidden)
		}
		if opts.Room != "" && opts.Room != parent.room {
			return nil, fmt.Errorf("%w: cross-room fork is not supported", ErrManagedSessionForbidden)
		}
		opts.Room = parent.room
		if parent.actor != "" {
			opts.Actor = parent.actor
		}
		// A fork runs in its parent's process, so it has the parent's class.
		if opts.Class != "" && opts.Class != parent.class {
			return nil, fmt.Errorf("%w: a fork cannot change its parent's class", ErrManagedSessionForbidden)
		}
		opts.Class = parent.class
		// A fork runs in the parent's process and inherits the parent's class,
		// so no class policy is re-resolved here to check request-supplied
		// servers against; and the fork's receipt is the parent's receipt,
		// which records the servers the PARENT was started with. Refuse
		// mcp_servers on the fork path outright rather than thread them into
		// the session/fork RPC under a receipt that never mentions them (the
		// pre-fix bypass: POST fork_of=<parent> + mcp_servers handed a forked
		// session tools no audit trail records, on any parent class).
		if len(opts.McpServers) > 0 {
			return nil, fmt.Errorf("%w: mcp_servers come from the forked session's parent and may not be sent", ErrInvalidManagedSession)
		}
		receipt = parent.receipt
		if opts.AllowClass != nil && !opts.AllowClass(opts.Class) {
			return nil, fmt.Errorf("%w: caller may not fork this session", ErrManagedSessionForbidden)
		}
	} else {
		var class *ResolvedRuntimeClass
		if opts.AllowClass != nil && !opts.AllowClass(opts.Class) {
			return nil, fmt.Errorf("%w: caller may not create runtime class %q", ErrManagedSessionForbidden, opts.Class)
		}
		if opts.Class != "" {
			base := d.cfg.Env
			if base == nil {
				base = os.Environ()
			}
			var err error
			if class, err = d.cfg.Classes.Resolve(opts.Class, base, opts.Cwd); err != nil {
				return nil, err
			}
			// The class owns the profile. It accepts the request's
			// mcp_servers only when its policy says "request"; every other
			// policy (none, configured) keeps the old refusal, so this is
			// the same message such a request got before.
			if opts.Profile != "" || (len(opts.McpServers) > 0 && class.McpServers != RuntimeClassMcpRequest) {
				return nil, fmt.Errorf("%w: profile and mcp_servers come from runtime class %q and may not be sent", ErrInvalidManagedSession, opts.Class)
			}
			opts.Profile, opts.Cwd = class.Profile, class.Cwd
			receipt = class.Receipt()
			if class.McpServers == RuntimeClassMcpRequest {
				// The class accepts the request's servers: they travel with
				// the session/new call and are recorded on the receipt.
				receipt.McpServers = mcp
			} else {
				mcp = []json.RawMessage{}
			}
		}
		proc = d.newProcess(opts.Profile, opts.Cwd, class)
		if _, err := proc.start(ctx); err != nil {
			// A failure caused by the caller's own context ending says
			// nothing about the class; only a genuine failure (or the
			// InitTimeout deadline, which leaves ctx live) is cached.
			if class != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				d.markClassUnavailable(class.Name, err)
			}
			return nil, err
		}
		if class != nil {
			d.recordClassReadinessFromProcess(class, proc)
		}
	}

	cctx, cancel := context.WithTimeout(ctx, d.cfg.InitTimeout)
	defer cancel()
	var sid string
	var err error
	// Register before session/load returns: load replays history as
	// session/update notifications, which must land in the ring.
	sess := &HermesACPSession{
		profile: opts.Profile, cwd: opts.Cwd, forkOf: opts.ForkOf, room: opts.Room, class: opts.Class, receipt: receipt, actor: opts.Actor, createdAt: time.Now().UTC(),
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
		// Both of these belong here, and the merge is the only place they meet: the test seam lets a test capture a
		// ring that is created before the load can fail (so the close below is testable at all), and the journal is
		// what 706 wires onto the same ring. The seam runs first, while the ring is still bare.
		if hook := d.cfg.afterRingCreate; hook != nil {
			hook(sess.ring)
		}
		if j := newSessionJournalForSession(sess, d.cfg.JournalDir); j != nil {
			sess.ring.SetJournal(j)
		}
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
		// The session is leaving proc.sessions without ever going live, so
		// nothing else will ever end its ring: close it here, exactly once.
		// Only the opts.Load branch has created a ring before the call; the
		// other branches leave sess.ring nil until after this point.
		if sess.ring != nil {
			sess.ring.Close()
		}
		if parent == nil && empty {
			proc.stop()
		}
		return nil, fmt.Errorf("hermes-acp: create session: %w", err)
	}
	if sess.ring == nil {
		sess.id, sess.ring = sid, NewEventRing(sid, d.cfg.RingSize)
		if j := newSessionJournalForSession(sess, d.cfg.JournalDir); j != nil {
			sess.ring.SetJournal(j)
		}
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

	if err := d.saveRecord(managedSessionRecord{SessionID: sid, Class: sess.class, CreatedAt: sess.createdAt}); err != nil && sess.class != "" {
		d.detach(sess)
		return nil, fmt.Errorf("hermes-acp: persist session class: %w", err)
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
	d.removeRecord(id)
	return d.drop(id)
}

// DeleteAuthorized is Delete behind the class-authority predicate (nil = no
// restriction). The class is the resident session's, else the durable
// record's (a cold id): a record is never removed by a caller who could not
// attach the session it describes. An unreadable record fails closed.
func (d *HermesACPDriver) DeleteAuthorized(id string, allow func(class string) bool) error {
	if allow != nil {
		var class string
		if sess := d.Get(id); sess != nil {
			class = sess.class
		} else {
			rec, err := d.loadRecord(id)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrManagedSessionForbidden, err)
			}
			class = rec.Class
		}
		if !allow(class) {
			return fmt.Errorf("%w: caller may not delete this session", ErrManagedSessionForbidden)
		}
	}
	return d.Delete(id)
}

// drop unregisters and detaches a session but keeps its durable record (Close
// at kernel shutdown must not forget which class a session belongs to).
func (d *HermesACPDriver) drop(id string) error {
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

// recordClassReadinessFromProcess caches a ready receipt derived from a
// process that successfully initialized, so a later probe/create does not
// spawn a second expensive child for the same class.
func (d *HermesACPDriver) recordClassReadinessFromProcess(class *ResolvedRuntimeClass, p *hermesProcess) {
	p.mu.Lock()
	pv := p.protocolVersion
	caps := p.agentCapabilities
	p.mu.Unlock()
	name := class.Name
	st := ManagedSessionClassStatus{
		Class:             name,
		Status:            "ready",
		ProbedAt:          d.clockNow().UTC(),
		ProtocolVersion:   pv,
		AgentCapabilities: caps,
		Receipt:           class.Receipt(),
	}
	d.classProbeMu.Lock()
	d.classProbeCache[name] = st
	d.classProbeMu.Unlock()
}

func (d *HermesACPDriver) markClassUnavailable(name string, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	st := ManagedSessionClassStatus{
		Class:    name,
		Status:   "unavailable",
		ProbedAt: d.clockNow().UTC(),
		Reason:   reason,
	}
	d.classProbeMu.Lock()
	d.classProbeCache[name] = st
	d.classProbeMu.Unlock()
}

// ClassNames returns the declared (sorted) class names.
func (d *HermesACPDriver) ClassNames() []string {
	if d.cfg.Classes == nil {
		return nil
	}
	return d.cfg.Classes.Names()
}

func (d *HermesACPDriver) getCachedClassReadiness(name string) (ManagedSessionClassStatus, bool) {
	d.classProbeMu.Lock()
	defer d.classProbeMu.Unlock()
	st, ok := d.classProbeCache[name]
	return st, ok
}

// ClassStatus probes (or returns the cached receipt for) one declared class.
func (d *HermesACPDriver) ClassStatus(ctx context.Context, name string) ManagedSessionClassStatus {
	return d.probeClass(ctx, name)
}

// ProbeClass is an alias for ClassStatus.
func (d *HermesACPDriver) ProbeClass(ctx context.Context, name string) ManagedSessionClassStatus {
	return d.probeClass(ctx, name)
}

// ProbeClasses probes every declared class.
func (d *HermesACPDriver) ProbeClasses(ctx context.Context) []ManagedSessionClassStatus {
	names := d.ClassNames()
	out := make([]ManagedSessionClassStatus, 0, len(names))
	for _, n := range names {
		out = append(out, d.probeClass(ctx, n))
	}
	return out
}

// ClassStatuses is an alias for ProbeClasses.
func (d *HermesACPDriver) ClassStatuses(ctx context.Context) []ManagedSessionClassStatus {
	return d.ProbeClasses(ctx)
}

// ClassReadiness is an alias for ProbeClasses.
func (d *HermesACPDriver) ClassReadiness(ctx context.Context) []ManagedSessionClassStatus {
	return d.ProbeClasses(ctx)
}

// ReadinessReceipts is an alias for ProbeClasses.
func (d *HermesACPDriver) ReadinessReceipts(ctx context.Context) []ManagedSessionClassStatus {
	return d.ProbeClasses(ctx)
}

func (d *HermesACPDriver) probeClass(ctx context.Context, name string) ManagedSessionClassStatus {
	d.classProbeMu.Lock()
	if cached, ok := d.classProbeCache[name]; ok {
		if d.clockNow().Sub(cached.ProbedAt) < d.cfg.ProbeCacheTTL {
			d.classProbeMu.Unlock()
			return cached
		}
		// Expired: drop it and re-probe (an inflight probe is joined below).
		delete(d.classProbeCache, name)
	}
	if ch, ok := d.classProbeInflight[name]; ok {
		d.classProbeMu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: d.clockNow().UTC(), Reason: ctx.Err().Error()}
		}
		d.classProbeMu.Lock()
		cached := d.classProbeCache[name]
		d.classProbeMu.Unlock()
		if cached.Class == "" {
			cached.Class = name
		}
		return cached
	}
	ch := make(chan struct{})
	d.classProbeInflight[name] = ch
	d.classProbeMu.Unlock()

	// The result lands in a cache shared by every caller, so the probe must
	// not die with this caller's context. It stays bounded by InitTimeout.
	res := d.runClassProbe(context.WithoutCancel(ctx), name)

	d.classProbeMu.Lock()
	d.classProbeCache[name] = res
	delete(d.classProbeInflight, name)
	d.classProbeMu.Unlock()
	close(ch)
	return res
}

func (d *HermesACPDriver) runClassProbe(ctx context.Context, name string) ManagedSessionClassStatus {
	ts := d.clockNow().UTC()
	if d.cfg.Classes == nil || !d.cfg.Classes.Has(name) {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: "unknown runtime class"}
	}
	base := d.cfg.Env
	if base == nil {
		base = os.Environ()
	}
	rc, err := d.cfg.Classes.Resolve(name, base, "")
	if err != nil {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: err.Error()}
	}
	argv, env := rc.Argv, rc.Env
	cwd := rc.Cwd
	if len(argv) == 0 {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: "empty argv"}
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = env
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: err.Error()}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: err.Error()}
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: ts, Reason: err.Error()}
	}

	conn := acprpc.NewConn(stdout, stdin, acpProbeHandler{})
	ictx, cancel := context.WithTimeout(ctx, d.cfg.InitTimeout)
	var res struct {
		ProtocolVersion   int             `json:"protocolVersion"`
		AgentInfo         json.RawMessage `json:"agentInfo"`
		AgentCapabilities json.RawMessage `json:"agentCapabilities"`
	}
	callErr := conn.Call(ictx, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{"fs": map[string]any{"readTextFile": false, "writeTextFile": false}, "terminal": false},
		"clientInfo":         map[string]any{"name": "cogos-kernel", "version": "0.1"},
	}, &res)
	cancel()
	// kill and reap
	_ = cmd.Process.Kill()
	_ = cmd.Wait()

	if callErr != nil {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: d.clockNow().UTC(), Reason: callErr.Error()}
	}
	if res.ProtocolVersion != 1 {
		return ManagedSessionClassStatus{Class: name, Status: "unavailable", ProbedAt: d.clockNow().UTC(), Reason: fmt.Sprintf("unsupported protocol version %d", res.ProtocolVersion)}
	}
	caps := res.AgentCapabilities
	st := ManagedSessionClassStatus{
		Class:             name,
		Status:            "ready",
		ProbedAt:          d.clockNow().UTC(),
		ProtocolVersion:   res.ProtocolVersion,
		AgentCapabilities: caps,
		Receipt:           rc.Receipt(),
	}
	return st
}

type acpProbeHandler struct{}

func (acpProbeHandler) HandleRequest(string, json.RawMessage) (any, error) {
	return nil, &acprpc.Error{Code: acprpc.CodeMethodNotFound, Message: "not supported"}
}
func (acpProbeHandler) HandleNotification(string, json.RawMessage) {}

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
		_ = d.drop(id)
	}
}
