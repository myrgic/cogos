package acprpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/acprpc"
	"github.com/myrgic/cogos/internal/acprpc/acpfake"
)

// recorder is a client-side Handler that records notifications and answers
// permission requests with a fixed option.
type recorder struct {
	mu      sync.Mutex
	updates []string
	option  string
}

func (r *recorder) HandleNotification(method string, params json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, method+" "+string(params))
}

func (r *recorder) HandleRequest(method string, params json.RawMessage) (any, error) {
	if method != "session/request_permission" {
		return nil, &acprpc.Error{Code: acprpc.CodeMethodNotFound, Message: method}
	}
	return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": r.option}}, nil
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.updates...)
}

// pair wires a client Conn to an in-process fake agent over two pipes.
func pair(t *testing.T, h acprpc.Handler) (*acprpc.Conn, func()) {
	t.Helper()
	agentIn, clientOut := io.Pipe()
	clientIn, agentOut := io.Pipe()
	acpfake.Serve(agentIn, agentOut, false)
	c := acprpc.NewConn(clientIn, clientOut, h)
	return c, func() { clientOut.Close(); agentOut.Close() }
}

func TestConn_InitializeNewPrompt(t *testing.T) {
	rec := &recorder{}
	c, stop := pair(t, rec)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var init struct {
		AgentInfo struct{ Name string } `json:"agentInfo"`
	}
	if err := c.Call(ctx, "initialize", map[string]any{"protocolVersion": 1}, &init); err != nil {
		t.Fatal(err)
	}
	if init.AgentInfo.Name != "acpfake" {
		t.Fatalf("agentInfo = %+v", init)
	}
	var ns struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.Call(ctx, "session/new", map[string]any{"cwd": "/tmp", "mcpServers": []any{}}, &ns); err != nil {
		t.Fatal(err)
	}
	var pr struct {
		StopReason string `json:"stopReason"`
	}
	err := c.Call(ctx, "session/prompt", map[string]any{
		"sessionId": ns.SessionID, "prompt": []any{map[string]any{"type": "text", "text": "hello"}},
	}, &pr)
	if err != nil || pr.StopReason != "end_turn" {
		t.Fatalf("prompt: %v %+v", err, pr)
	}
	ups := rec.snapshot()
	if len(ups) != 1 || !strings.Contains(ups[0], "echo:hello") {
		t.Fatalf("updates = %v", ups)
	}
}

func TestConn_ErrorResponse(t *testing.T) {
	c, stop := pair(t, &recorder{})
	defer stop()
	err := c.Call(context.Background(), "no/such", map[string]any{}, nil)
	var rpcErr *acprpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != acprpc.CodeMethodNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestConn_ServesPeerRequests(t *testing.T) {
	rec := &recorder{option: "allow"}
	c, stop := pair(t, rec)
	defer stop()
	err := c.Call(context.Background(), "session/prompt", map[string]any{
		"sessionId": "s", "prompt": []any{map[string]any{"type": "text", "text": "permission please"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ups := rec.snapshot()
	if len(ups) != 1 || !strings.Contains(ups[0], "permission:selected:allow") {
		t.Fatalf("updates = %v", ups)
	}
}

func TestConn_NotifyCancelsSlowTurn(t *testing.T) {
	c, stop := pair(t, &recorder{})
	defer stop()
	done := make(chan string, 1)
	go func() {
		var pr struct {
			StopReason string `json:"stopReason"`
		}
		_ = c.Call(context.Background(), "session/prompt", map[string]any{
			"sessionId": "s", "prompt": []any{map[string]any{"type": "text", "text": "slow"}},
		}, &pr)
		done <- pr.StopReason
	}()
	// Retry the cancel until the fake has registered the turn.
	deadline := time.After(5 * time.Second)
	for {
		_ = c.Notify("session/cancel", map[string]any{"sessionId": "s"})
		select {
		case sr := <-done:
			if sr != "cancelled" {
				t.Fatalf("stopReason = %q", sr)
			}
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("turn never cancelled")
		}
	}
}

func TestConn_PendingCallFailsOnClose(t *testing.T) {
	clientIn, agentOut := io.Pipe()
	c := acprpc.NewConn(clientIn, io.Discard, nil)
	errc := make(chan error, 1)
	go func() { errc <- c.Call(context.Background(), "x", nil, nil) }()
	time.Sleep(20 * time.Millisecond)
	agentOut.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, acprpc.ErrClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending call did not fail on close")
	}
	<-c.Done()
	if err := c.Call(context.Background(), "x", nil, nil); !errors.Is(err, acprpc.ErrClosed) {
		t.Fatalf("call after close: %v", err)
	}
}

func TestConn_SkipsNonJSONAndCtxCancel(t *testing.T) {
	clientIn, agentOut := io.Pipe()
	c := acprpc.NewConn(clientIn, io.Discard, nil)
	go func() { _, _ = agentOut.Write([]byte("not json\n\n")) }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Call(ctx, "x", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-c.Done():
		t.Fatal("non-JSON line closed the connection")
	default:
	}
	agentOut.Close()
}
