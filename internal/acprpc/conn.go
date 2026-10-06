// Package acprpc is a minimal JSON-RPC 2.0 peer over newline-delimited
// stdio, the transport the Agent Client Protocol (ACP) uses between a client
// and an agent subprocess (e.g. `hermes acp`).
//
// It is deliberately protocol-agnostic: it knows requests, responses and
// notifications, not ACP methods. The ACP client semantics (initialize,
// session/new|load|fork|prompt|cancel|set_mode, session/request_permission)
// live with the caller — see internal/engine/managed_session_hermes.go.
//
// Package-ownership note: internal/acp is the Claude Code stream-json engine
// driver and is NOT ACP JSON-RPC despite the name (see the doc comment at the
// top of internal/engine/managed_session.go). This package is the JSON-RPC
// wire layer for agents that actually speak ACP.
package acprpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// MaxLineBytes bounds a single JSON-RPC message. ACP session/load replays a
// whole conversation as updates, and tool results can be large; the stage's
// Python client uses the same 16 MiB limit.
const MaxLineBytes = 16 << 20

// ErrClosed is returned for calls pending when the peer's stream ends, and
// for calls made after it ended.
var ErrClosed = errors.New("acprpc: connection closed")

// Error is a JSON-RPC error object, returned by Call when the peer answers a
// request with an error.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message) }

// Standard JSON-RPC error codes used when answering peer requests.
const (
	CodeMethodNotFound = -32601
	CodeInternalError  = -32603
)

// message is the union wire shape of a request, response or notification.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Handler receives the peer's inbound traffic. HandleRequest is called on its
// own goroutine (it may block, e.g. waiting for a human to answer a
// permission request) and must eventually return a result or an error; the
// error is sent back as a JSON-RPC error (an *Error is sent verbatim, any
// other error as CodeInternalError). HandleNotification is called inline on
// the read loop, in wire order, and must not block for long.
type Handler interface {
	HandleRequest(method string, params json.RawMessage) (any, error)
	HandleNotification(method string, params json.RawMessage)
}

// Conn is one JSON-RPC peer over a reader/writer pair.
type Conn struct {
	w       io.Writer
	writeMu sync.Mutex

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan message
	closed  bool
	done    chan struct{}
	readErr error

	handler Handler
}

// NewConn starts a read loop over r and returns the connection. The loop runs
// until r returns EOF or an error; Done is closed at that point and every
// pending Call fails with ErrClosed.
func NewConn(r io.Reader, w io.Writer, h Handler) *Conn {
	c := &Conn{
		w:       w,
		pending: make(map[string]chan message),
		done:    make(chan struct{}),
		handler: h,
	}
	go c.readLoop(r)
	return c
}

// Done is closed once the inbound stream has ended.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns the read error that ended the stream (nil for a clean EOF).
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

func (c *Conn) write(m message) error {
	m.JSONRPC = "2.0"
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.w.Write(b)
	return err
}

// Call sends a request and waits for its response, ctx cancellation, or the
// end of the stream. result may be nil to discard the result.
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("acprpc: marshal %s params: %w", method, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.nextID++
	id := strconv.FormatInt(c.nextID, 10)
	ch := make(chan message, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	if err := c.write(message{ID: json.RawMessage(id), Method: method, Params: raw}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("acprpc: write %s: %w", method, err)
	}

	select {
	case m, ok := <-ch:
		if !ok {
			return ErrClosed
		}
		if m.Error != nil {
			return m.Error
		}
		if result != nil && len(m.Result) > 0 {
			if err := json.Unmarshal(m.Result, result); err != nil {
				return fmt.Errorf("acprpc: decode %s result: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

// Notify sends a notification (no response expected).
func (c *Conn) Notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("acprpc: marshal %s params: %w", method, err)
	}
	return c.write(message{Method: method, Params: raw})
}

func (c *Conn) readLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), MaxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var m message
		if err := json.Unmarshal(line, &m); err != nil {
			// Non-JSON stdout (a stray print in the agent) is skipped, as the
			// stage's Python client does, rather than killing the session.
			continue
		}
		switch {
		case m.Method != "" && len(m.ID) > 0:
			go c.serveRequest(m)
		case m.Method != "":
			if c.handler != nil {
				c.handler.HandleNotification(m.Method, m.Params)
			}
		case len(m.ID) > 0:
			c.mu.Lock()
			ch, ok := c.pending[string(m.ID)]
			delete(c.pending, string(m.ID))
			c.mu.Unlock()
			if ok {
				ch <- m
			}
		}
	}
	c.mu.Lock()
	c.closed = true
	c.readErr = sc.Err()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.mu.Unlock()
	close(c.done)
}

func (c *Conn) serveRequest(m message) {
	if c.handler == nil {
		_ = c.write(message{ID: m.ID, Error: &Error{Code: CodeMethodNotFound, Message: "no handler"}})
		return
	}
	res, err := c.handler.HandleRequest(m.Method, m.Params)
	if err != nil {
		var rpcErr *Error
		if !errors.As(err, &rpcErr) {
			rpcErr = &Error{Code: CodeInternalError, Message: err.Error()}
		}
		_ = c.write(message{ID: m.ID, Error: rpcErr})
		return
	}
	raw, err := json.Marshal(res)
	if err != nil {
		_ = c.write(message{ID: m.ID, Error: &Error{Code: CodeInternalError, Message: err.Error()}})
		return
	}
	_ = c.write(message{ID: m.ID, Result: raw})
}
