// managed_session.go — ADR-093 §4 ManagedSession lifecycle state machine
// and session-ID-keyed registry, layered over internal/acp.Subprocess as
// the engine driver.
//
// Package-ownership note (conductor-forensics-2026-08-28.md §5, ratified):
// internal/acp stays the engine driver (own a claude subprocess, parse
// stream-json, expose typed events) — it is not where ACP wire code goes.
// This file is the ADR-093 §2 ManagedSession primitive that sits one layer
// above it in the kernel; the ACP agent face (initialize/session.new/
// session.prompt over coder/acp-go-sdk) is a future internal/acpserver
// package that will serve sessions FROM the registry below, not a
// responsibility of this file.
//
// Status: skeleton. This lane (L3, acp-conductor-research-2026-08-28.md
// §7) delivers the §4 state machine, the session-ID-keyed registry, and a
// Cancel() that encodes the L1 cancellation spike's verdict. It
// deliberately does NOT include:
//   - the ADR-093 §7/§8-Commit-2 HTTP surface (POST /v1/managed-sessions/
//     {id}/resume, GET /v1/managed-sessions, DELETE /v1/managed-sessions/
//     {id}) — not part of the L3 task row, follow-up work;
//   - §3's SessionChannel / mod3 seat wiring — Heartbeat/CheckStalled
//     below are the seam a future channel integration calls into, but
//     nothing here registers a mod3 seat or drives heartbeats itself;
//   - §4's restart/backoff policy (crash_window, max_restarts) — Crashed
//     is a terminal state here; a caller (or a future restart supervisor)
//     decides whether to spawn a replacement.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/myrgic/cogos/internal/acp"
)

// ManagedSessionState is one of the ADR-093 §4 lifecycle states.
type ManagedSessionState int

const (
	// StateStarting: subprocess invoked, system.init not yet observed.
	StateStarting ManagedSessionState = iota
	// StateLive: system.init observed — seat registered with mod3 in the
	// ADR's full design; here, "the underlying claude process has emitted
	// a real session_id and turns can be sent/cancelled safely."
	StateLive
	// StateStalled: registered but no heartbeat within the bound
	// (ADR-093 §4: "> 2 * heartbeat_interval"). Reserved for when a
	// SessionChannel (§3, mod3-backed) drives Heartbeat calls into this
	// session; nothing in this skeleton calls Heartbeat on its own.
	StateStalled
	// StateDetached: clean shutdown — either an explicit Detach() call, or
	// the subprocess exited on its own (EOF/exit 0) without one.
	StateDetached
	// StateCrashed: the subprocess exited unexpectedly (Wait returned a
	// non-nil error) with no preceding Detach() call. Note this also
	// covers "we sent CancelSIGINT and the process exited non-zero as a
	// result" — from this object's perspective that subprocess instance
	// is gone; resuming the same claude session_id means registering a
	// new ManagedSession via ManagedSessionRegistry.Resume, not reviving this one.
	StateCrashed
)

func (s ManagedSessionState) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateLive:
		return "live"
	case StateStalled:
		return "stalled"
	case StateDetached:
		return "detached"
	case StateCrashed:
		return "crashed"
	default:
		return "unknown"
	}
}

// ErrCancelBeforeInit is returned by ManagedSession.Cancel when a
// CancelSIGINT is requested before system.init has been observed.
//
// This is the L1-verified gate (cogos worktree spike-acp-l1, branch
// spike/acp-golden-corpus, cancellation_live_test.go, 2026-08-28): SIGINT
// delivered during the subprocess's startup/hook window left an
// unresumable session in 3 of 4 trials (the process terminated before
// system.init was even emitted), while SIGINT delivered after system.init
// left a resumable session in 4 of 4 trials. The finding was explicit that
// the gate belongs on the state machine — "has init arrived?" — not on
// wall-clock elapsed since Send; a fixed delay was the live test's own
// first-pass mistake, caught and corrected before landing.
//
// CancelStdinClose is NOT gated by this. Per the same spike, stdin-close
// is "no more turns" semantics: it lets any in-flight turn run to natural
// completion rather than aborting it, so there is no unresumable-session
// race for the gate to guard against.
var ErrCancelBeforeInit = errors.New("managed session: cannot SIGINT-cancel before system.init is observed (subprocess may still be in the startup/hook window); retry once live, or use CancelStdinClose for a graceful no-more-turns stop")

// ErrSessionNotLive is returned by Send/Cancel once a session has left the
// live window (detached or crashed).
var ErrSessionNotLive = errors.New("managed session: not live")

// ManagedSessionOpts configures a New/Resume call. Mirrors acp.SpawnOpts
// (which is itself ADR-093 §2's SessionOpts) rather than re-declaring a
// parallel shape the caller has to keep in sync.
type ManagedSessionOpts struct {
	ClaudePath string
	Model      string
	Cwd        string
	ExtraArgs  []string
	Env        []string
}

// ManagedSession owns one long-lived claude subprocess (via internal/acp)
// and tracks its ADR-093 §4 lifecycle state. It is the substrate's unit of
// agent attachment; ManagedSessionRegistry below is keyed by SessionID.
type ManagedSession struct {
	SessionID string
	StartedAt time.Time

	proc  *acp.Subprocess
	outCh chan acp.Event

	mu            sync.Mutex
	state         ManagedSessionState
	sawInit       bool
	lastHeartbeat time.Time
	exitErr       error
}

// NewManagedSession spawns a fresh session (fresh --session-id) and
// returns it in StateStarting.
func NewManagedSession(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
	return newManagedSession(ctx, acp.SpawnOpts{
		ClaudePath:     opts.ClaudePath,
		SessionID:      sessionID,
		ResumeExisting: false,
		Model:          opts.Model,
		Cwd:            opts.Cwd,
		ExtraArgs:      opts.ExtraArgs,
		Env:            opts.Env,
	})
}

// ResumeManagedSession attaches to an existing on-disk claude session
// (--resume) and returns it in StateStarting until system.init confirms
// the resume succeeded.
func ResumeManagedSession(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
	return newManagedSession(ctx, acp.SpawnOpts{
		ClaudePath:     opts.ClaudePath,
		SessionID:      sessionID,
		ResumeExisting: true,
		Model:          opts.Model,
		Cwd:            opts.Cwd,
		ExtraArgs:      opts.ExtraArgs,
		Env:            opts.Env,
	})
}

func newManagedSession(ctx context.Context, spawnOpts acp.SpawnOpts) (*ManagedSession, error) {
	proc, err := acp.Spawn(ctx, spawnOpts)
	if err != nil {
		return nil, fmt.Errorf("managed session: spawn: %w", err)
	}
	ms := &ManagedSession{
		SessionID: spawnOpts.SessionID,
		StartedAt: time.Now(),
		proc:      proc,
		outCh:     make(chan acp.Event, 32),
		state:     StateStarting,
	}
	ms.startEventPump()
	return ms, nil
}

// startEventPump is the session's single reader of the underlying
// subprocess's event channel. acp.Subprocess.Events() has one intended
// reader (its own doc comment: "the same channel across the Subprocess's
// lifetime"); ManagedSession is that reader, both to drive its own state
// machine (system.init -> Live; channel close -> Detached/Crashed) and to
// relay every frame onward to whoever calls Events() on this
// ManagedSession, via outCh. A caller therefore sees the identical frame
// sequence acp.Subprocess produced, just through ManagedSession's channel
// instead of the underlying one directly.
func (ms *ManagedSession) startEventPump() {
	go func() {
		for ev := range ms.proc.Events() {
			if ev.System != nil && ev.System.Subtype == acp.SystemSubtypeInit {
				ms.mu.Lock()
				ms.sawInit = true
				if ms.state == StateStarting {
					ms.state = StateLive
				}
				ms.mu.Unlock()
			}
			ms.outCh <- ev
		}
		close(ms.outCh)

		err := ms.proc.Wait()
		ms.mu.Lock()
		defer ms.mu.Unlock()
		if ms.state == StateDetached {
			// An explicit Detach() already claimed this outcome —
			// whatever the exit code, it was requested.
			return
		}
		ms.exitErr = err
		if err != nil {
			ms.state = StateCrashed
		} else {
			// Clean exit (code 0) without a prior Detach() call — e.g.
			// stdin-close semantics: the in-flight turn ran to
			// completion and the process then exited on EOF (L1
			// finding, 2026-08-28) without the caller ever calling
			// Detach. Treat as a graceful end, not a crash.
			ms.state = StateDetached
		}
	}()
}

// State returns the session's current lifecycle state.
func (ms *ManagedSession) State() ManagedSessionState {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.state
}

// ExitErr returns the underlying subprocess's exit error, if the session
// has reached StateCrashed. Returns nil in every other state.
func (ms *ManagedSession) ExitErr() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.exitErr
}

// Events returns the channel of typed stream-json events relayed from the
// underlying subprocess. Closes once the subprocess exits and the pump
// finishes draining it.
func (ms *ManagedSession) Events() <-chan acp.Event { return ms.outCh }

// Send delivers one prompt to the underlying subprocess. Rejected once the
// session has left the live window (ADR-093 §6: sending into an
// already-gone session is a caller error, not a silent no-op).
func (ms *ManagedSession) Send(p acp.PromptInput) error {
	ms.mu.Lock()
	state := ms.state
	ms.mu.Unlock()
	if state == StateDetached || state == StateCrashed {
		return fmt.Errorf("%w: session %s is %s", ErrSessionNotLive, ms.SessionID, state)
	}
	return ms.proc.Send(p)
}

// Cancel asks the underlying subprocess to stop its current turn using the
// given mode. See ErrCancelBeforeInit for the gate this enforces on
// CancelSIGINT; CancelStdinClose passes straight through.
func (ms *ManagedSession) Cancel(mode acp.CancelMode) error {
	ms.mu.Lock()
	state := ms.state
	sawInit := ms.sawInit
	ms.mu.Unlock()

	if state == StateDetached || state == StateCrashed {
		return fmt.Errorf("%w: session %s is %s", ErrSessionNotLive, ms.SessionID, state)
	}
	if mode == acp.CancelSIGINT && !sawInit {
		return ErrCancelBeforeInit
	}
	return ms.proc.Cancel(mode)
}

// Heartbeat records a liveness signal. Nothing in this skeleton calls this
// on its own — it is the seam a future SessionChannel (ADR-093 §3,
// mod3-backed) integration drives.
func (ms *ManagedSession) Heartbeat() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.lastHeartbeat = time.Now()
	if ms.state == StateStalled {
		ms.state = StateLive
	}
}

// CheckStalled transitions Live -> Stalled if no Heartbeat has landed
// within staleAfter. The caller is responsible for invoking this on a
// ticker (ADR-093 §4 default: 2 * heartbeat_interval); no ticker runs
// internally in this skeleton.
func (ms *ManagedSession) CheckStalled(staleAfter time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.state != StateLive || ms.lastHeartbeat.IsZero() {
		return
	}
	if time.Since(ms.lastHeartbeat) > staleAfter {
		ms.state = StateStalled
	}
}

// Detach stops the managed process cleanly via a graceful stdin-close
// (ADR-093's "no more turns" semantics, per the L1 spike). Idempotent per
// ADR-093 §6: calling Detach against an already-detached or already-gone
// session succeeds as a no-op.
func (ms *ManagedSession) Detach() error {
	ms.mu.Lock()
	switch ms.state {
	case StateDetached, StateCrashed:
		ms.mu.Unlock()
		return nil
	}
	ms.state = StateDetached
	ms.mu.Unlock()

	if err := ms.proc.Cancel(acp.CancelStdinClose); err != nil {
		return fmt.Errorf("managed session %s: detach: %w", ms.SessionID, err)
	}
	return nil
}

// spawnNewManagedSession and spawnResumeManagedSession are package-level
// indirections over NewManagedSession/ResumeManagedSession purely so tests
// can substitute a counting/stub spawner to verify the registry's
// singleflight behavior (see managed_session_test.go's
// TestManagedSessionRegistry_*_ConcurrentSameID_SingleSpawn) without adding
// a new dependency or threading a spawner interface through every call
// site. Production code always uses the defaults below.
var (
	spawnNewManagedSession    = NewManagedSession
	spawnResumeManagedSession = ResumeManagedSession
)

// ManagedSessionRegistry is the session-ID-keyed registry ADR-093 §2/§7 describes
// (ManagedSession.Get, and the eventual GET /v1/managed-sessions surface —
// not part of this lane; see the file-level doc comment). This skeleton is
// the in-process map only.
type ManagedSessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*ManagedSession
	// inflight tracks sessionIDs currently being spawned by New/Resume, so
	// concurrent callers racing the same not-yet-registered ID share the
	// one winner's result instead of each spawning their own subprocess.
	// See getOrSpawn.
	inflight map[string]*inflightSpawn
}

// inflightSpawn is a single-use reservation for one sessionID's spawn.
// Exactly one goroutine (the one that installs it into
// ManagedSessionRegistry.inflight) performs the actual spawn and later
// writes ms/err before closing done; every other goroutine that finds this
// entry only reads ms/err after observing done closed, which is safe under
// the Go memory model (a channel close happens-before the corresponding
// receive completes) without any additional lock.
type inflightSpawn struct {
	done chan struct{}
	ms   *ManagedSession
	err  error
}

// NewManagedSessionRegistry returns an empty registry.
func NewManagedSessionRegistry() *ManagedSessionRegistry {
	return &ManagedSessionRegistry{
		sessions: make(map[string]*ManagedSession),
		inflight: make(map[string]*inflightSpawn),
	}
}

// getOrSpawn implements ADR-093 §6 idempotent session creation with
// singleflight-per-ID semantics. It replaces the naive
// "RLock/check/RUnlock/spawn/Lock/insert" sequence, which has a
// check-then-act race: two concurrent callers for the same
// not-yet-registered sessionID can both observe "not live" before either
// has inserted, and both then spawn a real subprocess — only the second
// insert survives, orphaning the first ManagedSession's subprocess and its
// startEventPump goroutine (it blocks forever once the 32-slot outCh fills,
// since nothing is left calling Events() on it).
//
// The fix: reserve the sessionID under the write lock (install a
// placeholder inflightSpawn) *before* releasing the lock to spawn — the
// spawn itself must happen unlocked since it can take real wall-clock
// time (subprocess exec + first-byte). Any other caller that arrives while
// a reservation is outstanding waits on that reservation's done channel
// instead of starting a second spawn, and receives the exact same
// *ManagedSession the winner produced. On spawn failure the reservation is
// cleared (not left installed) so a later call can retry cleanly rather
// than being wedged behind a permanently-failed placeholder.
func (r *ManagedSessionRegistry) getOrSpawn(sessionID string, spawn func() (*ManagedSession, error)) (*ManagedSession, error) {
	for {
		r.mu.Lock()
		if existing := r.liveLocked(sessionID); existing != nil {
			r.mu.Unlock()
			return existing, nil
		}
		if inf, ok := r.inflight[sessionID]; ok {
			r.mu.Unlock()
			<-inf.done
			if inf.err != nil {
				// The winner's spawn failed and already cleared the
				// reservation. Loop back around: this caller (or whoever
				// gets there first) becomes the new spawner.
				continue
			}
			return inf.ms, nil
		}

		// Reserve sessionID before unlocking. No other goroutine can win
		// this race for the same ID until we resolve inf.done below.
		inf := &inflightSpawn{done: make(chan struct{})}
		r.inflight[sessionID] = inf
		r.mu.Unlock()

		ms, err := spawn()

		r.mu.Lock()
		delete(r.inflight, sessionID)
		if err == nil {
			r.sessions[sessionID] = ms
		}
		r.mu.Unlock()

		inf.ms, inf.err = ms, err
		close(inf.done)
		return ms, err
	}
}

// liveLocked returns the registered session for id if it is neither
// Detached nor Crashed. Caller must hold r.mu (read or write).
func (r *ManagedSessionRegistry) liveLocked(id string) *ManagedSession {
	ms, ok := r.sessions[id]
	if !ok {
		return nil
	}
	switch ms.State() {
	case StateDetached, StateCrashed:
		return nil
	default:
		return ms
	}
}

// New creates a fresh session and registers it under sessionID. Idempotent
// per ADR-093 §6: if sessionID already maps to a live session, that
// session is returned rather than starting a second subprocess for the
// same ID. Concurrent New calls for the same not-yet-registered sessionID
// share one spawn (see getOrSpawn) rather than each spawning their own
// subprocess.
func (r *ManagedSessionRegistry) New(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
	return r.getOrSpawn(sessionID, func() (*ManagedSession, error) {
		return spawnNewManagedSession(ctx, sessionID, opts)
	})
}

// Resume attaches to an existing on-disk session, or returns the
// already-live in-process one if present (ADR-093 §6 idempotency).
// Concurrent Resume calls for the same not-yet-registered sessionID share
// one spawn (see getOrSpawn) rather than each spawning their own
// subprocess.
func (r *ManagedSessionRegistry) Resume(ctx context.Context, sessionID string, opts ManagedSessionOpts) (*ManagedSession, error) {
	return r.getOrSpawn(sessionID, func() (*ManagedSession, error) {
		return spawnResumeManagedSession(ctx, sessionID, opts)
	})
}

// Get returns the session registered under sessionID, or nil if none is
// attached — mirrors ADR-093 §2's ManagedSession.Get. Unlike New/Resume,
// this returns a Detached/Crashed session too if it is still in the map
// (callers wanting "live only" should check .State()).
func (r *ManagedSessionRegistry) Get(sessionID string) *ManagedSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[sessionID]
}

// Detach stops the managed process for sessionID. Idempotent: detaching an
// unregistered or already-detached session succeeds as a no-op (ADR-093
// §6).
func (r *ManagedSessionRegistry) Detach(sessionID string) error {
	r.mu.RLock()
	ms, ok := r.sessions[sessionID]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	return ms.Detach()
}

// List returns a snapshot of every registered session ID and its current
// state — the shape a future GET /v1/managed-sessions handler would
// serialize.
func (r *ManagedSessionRegistry) List() map[string]ManagedSessionState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]ManagedSessionState, len(r.sessions))
	for id, ms := range r.sessions {
		out[id] = ms.State()
	}
	return out
}
