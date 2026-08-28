package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

// SpawnOpts configures a Subprocess launch. The zero value gives a fresh
// session with no resume; the typical kernel use is to set SessionID to the
// harness session_id, ResumeExisting=true, and Cwd to the workspace dir.
type SpawnOpts struct {
	// ClaudePath overrides the resolved path to the claude binary. Empty
	// means "find on PATH" via exec.LookPath.
	ClaudePath string
	// SessionID, if non-empty, is passed as --session-id (fresh session
	// pinned to that id) or --resume (continuation), per ResumeExisting.
	SessionID      string
	ResumeExisting bool
	// Model overrides --model. Empty means claude's default.
	Model string
	// Cwd, if set, becomes the subprocess's working directory.
	Cwd string
	// ExtraArgs are appended verbatim before the implicit
	// --input-format/--output-format/--verbose flags. Use sparingly.
	ExtraArgs []string
	// Env overrides for the subprocess. nil means inherit os.Environ().
	Env []string
}

// Subprocess is a single long-lived `claude --print --input-format stream-json
// --output-format stream-json` instance.  It owns stdin/stdout and exposes a
// typed event channel for stream-json output and a Send method for
// stream-json input.
//
// Concurrency contract:
//   - Send may be called from any goroutine; writes are serialized.
//   - Events() returns the same channel across the Subprocess's lifetime;
//     it closes when the subprocess exits and the reader goroutine drains.
//   - Wait blocks until the subprocess exits and returns its exit error.
type Subprocess struct {
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  io.ReadCloser
	enc  *json.Encoder
	send sync.Mutex // serializes writes to stdin

	events chan Event
	waitCh chan error // single-shot exit
}

// Spawn launches the claude subprocess and returns once it has started; it
// does NOT wait for the first stream-json frame to arrive. Events() begins
// emitting as the subprocess produces them.
func Spawn(ctx context.Context, opts SpawnOpts) (*Subprocess, error) {
	bin := opts.ClaudePath
	if bin == "" {
		resolved, err := exec.LookPath("claude")
		if err != nil {
			return nil, fmt.Errorf("acp: claude binary not on PATH (set SpawnOpts.ClaudePath): %w", err)
		}
		bin = resolved
	}

	args := []string{
		"--print",
		"--verbose", // required for --output-format stream-json with --print
		"--output-format", "stream-json",
		"--input-format", "stream-json",
	}
	if opts.SessionID != "" {
		if opts.ResumeExisting {
			args = append(args, "--resume", opts.SessionID)
		} else {
			args = append(args, "--session-id", opts.SessionID)
		}
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	args = append(args, opts.ExtraArgs...)

	cmd := exec.CommandContext(ctx, bin, args...)
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	if opts.Env != nil {
		cmd.Env = opts.Env
	}

	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: stdin pipe: %w", err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		_ = in.Close()
		return nil, fmt.Errorf("acp: stdout pipe: %w", err)
	}
	// stderr goes through to the parent's stderr by default — that's where
	// claude's auth errors and crash diagnostics surface. Operator wants to
	// see those during the spike.

	if err := cmd.Start(); err != nil {
		_ = in.Close()
		_ = out.Close()
		return nil, fmt.Errorf("acp: start claude: %w", err)
	}

	sp := &Subprocess{
		cmd:    cmd,
		in:     in,
		out:    out,
		enc:    json.NewEncoder(in),
		events: make(chan Event, 32),
		waitCh: make(chan error, 1),
	}
	go sp.readLoop()
	go func() { sp.waitCh <- cmd.Wait() }()

	return sp, nil
}

// readLoop drains stdout, parsing one NDJSON line per iteration, and pushes
// typed Events down sp.events. Malformed lines are dropped with a synthetic
// UnknownEvent (zero Type) so the consumer can still see they happened.
// Closes sp.events when stdout EOFs or hits an unrecoverable read error.
func (s *Subprocess) readLoop() {
	defer close(s.events)
	// Claude's per-line frames can be long when an assistant message
	// arrives in one frame with embedded tool inputs; the default scanner
	// buffer (64KB) overflows. Bump to 1MB.
	scanner := bufio.NewScanner(s.out)
	const maxLine = 1 << 20
	scanner.Buffer(make([]byte, 0, 64<<10), maxLine)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		ev, err := ParseLine(line)
		if err != nil {
			s.events <- Event{Unknown: &UnknownEvent{Type: "", Raw: append([]byte(nil), line...)}}
			continue
		}
		s.events <- ev
	}
}

// Send writes one PromptInput to claude's stdin. Safe to call concurrently;
// the first writer wins ordering on the line boundary, subsequent writers
// block until the encoder finishes.
func (s *Subprocess) Send(p PromptInput) error {
	s.send.Lock()
	defer s.send.Unlock()
	if err := s.enc.Encode(p); err != nil {
		return fmt.Errorf("acp: send prompt: %w", err)
	}
	return nil
}

// CloseInput closes claude's stdin. After this returns, no further prompts
// can be sent; claude will finish processing any queued input and exit.
// The caller should still drain Events() until it closes.
func (s *Subprocess) CloseInput() error {
	s.send.Lock()
	defer s.send.Unlock()
	return s.in.Close()
}

// Events returns the channel of typed stream-json events. The channel
// closes when the subprocess exits and the read loop finishes.
func (s *Subprocess) Events() <-chan Event { return s.events }

// CancelMode selects how Cancel asks the subprocess to stop an in-flight
// turn. ADR-093 §10 flagged both as unvalidated; this is that validation
// surface. Neither variant waits for or guarantees a particular outcome —
// see cancellation_test.go for what each was observed to do against a real
// `claude` subprocess.
type CancelMode int

const (
	// CancelSIGINT sends SIGINT to the subprocess, mirroring what a
	// terminal delivers on Ctrl-C.
	CancelSIGINT CancelMode = iota
	// CancelStdinClose closes stdin without signaling the process. This is
	// exactly CloseInput — kept as a named CancelMode so callers can pick
	// a cancellation strategy without caring which mechanism it maps to.
	CancelStdinClose
)

// Cancel asks the subprocess to stop its current turn using the given
// mode. It returns as soon as the request is issued (signal delivered, or
// stdin closed) — it does NOT wait for the subprocess to exit or for
// Events() to drain. Callers should keep draining Events() and call Wait()
// to observe the actual outcome (trailing frames, exit code).
func (s *Subprocess) Cancel(mode CancelMode) error {
	switch mode {
	case CancelSIGINT:
		if s.cmd.Process == nil {
			return fmt.Errorf("acp: cancel: subprocess not started")
		}
		if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
			return fmt.Errorf("acp: cancel: signal: %w", err)
		}
		return nil
	case CancelStdinClose:
		return s.CloseInput()
	default:
		return fmt.Errorf("acp: cancel: unknown mode %d", mode)
	}
}

// Wait blocks until the subprocess exits and returns its exit error
// (nil on clean exit). May be called from any goroutine.
func (s *Subprocess) Wait() error { return <-s.waitCh }
