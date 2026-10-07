// Package acpfake is a tiny scripted ACP agent for tests. It speaks the
// subset of the Agent Client Protocol that the stage (and the kernel's
// HermesACPDriver) use — initialize, session/new|fork|load|prompt|set_mode,
// the session/cancel notification, and the agent→client
// session/request_permission request — over newline-delimited JSON-RPC.
//
// Prompt text drives behaviour:
//
//	"permission ..." → asks the client for permission, then reports the outcome
//	"slow ..."       → streams nothing until session/cancel, then stopReason "cancelled"
//	"crash ..."      → the process exits with status 3 (subprocess mode only)
//	anything else    → one agent_message_chunk echoing the text, stopReason "end_turn"
//
// Tests run it in-process (Serve over pipes) or as a subprocess by
// re-executing the test binary with ACPFAKE_AGENT=1 (see MaybeRunMain).
package acpfake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/myrgic/cogos/internal/acprpc"
)

// EnvVar, when set to "1" in a test binary's environment, makes MaybeRunMain
// run the fake agent on stdin/stdout and exit.
const EnvVar = "ACPFAKE_AGENT"

// MaybeRunMain is called from a test package's TestMain. When EnvVar is set
// it serves the fake agent over the process's stdio and exits; otherwise it
// returns immediately.
func MaybeRunMain() {
	if os.Getenv(EnvVar) != "1" {
		return
	}
	<-Serve(os.Stdin, os.Stdout, true)
	os.Exit(0)
}

type agent struct {
	conn       *acprpc.Conn
	subprocess bool

	mu      sync.Mutex
	next    int
	modes   map[string]string
	cancels map[string]chan struct{}
}

// Serve runs the fake agent over r/w. The returned channel closes when r
// ends. subprocess enables the "crash" behaviour (os.Exit).
func Serve(r io.Reader, w io.Writer, subprocess bool) <-chan struct{} {
	a := &agent{subprocess: subprocess, modes: map[string]string{}, cancels: map[string]chan struct{}{}}
	a.conn = acprpc.NewConn(r, w, a)
	return a.conn.Done()
}

type sessionParams struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	ModeID    string `json:"modeId"`
	Prompt    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"prompt"`
}

func (a *agent) newID(prefix string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next++
	return fmt.Sprintf("%s-%d", prefix, a.next)
}

func (a *agent) update(sid string, update map[string]any) {
	_ = a.conn.Notify("session/update", map[string]any{"sessionId": sid, "update": update})
}

func chunk(text string) map[string]any {
	return map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": text},
	}
}

func (a *agent) HandleNotification(method string, params json.RawMessage) {
	if method != "session/cancel" {
		return
	}
	var p sessionParams
	_ = json.Unmarshal(params, &p)
	a.mu.Lock()
	if ch, ok := a.cancels[p.SessionID]; ok {
		close(ch)
		delete(a.cancels, p.SessionID)
	}
	a.mu.Unlock()
}

func (a *agent) HandleRequest(method string, params json.RawMessage) (any, error) {
	var p sessionParams
	_ = json.Unmarshal(params, &p)
	switch method {
	case "initialize":
		return map[string]any{
			"protocolVersion": 1,
			"agentInfo":       map[string]any{"name": "acpfake", "version": "0"},
			"agentCapabilities": map[string]any{
				"loadSession": true,
			},
		}, nil
	case "session/new":
		return map[string]any{"sessionId": a.newID("sess")}, nil
	case "session/fork":
		if p.SessionID == "" {
			return nil, &acprpc.Error{Code: -32602, Message: "fork needs sessionId"}
		}
		return map[string]any{"sessionId": a.newID("fork")}, nil
	case "session/load":
		a.update(p.SessionID, chunk("replayed history for "+p.SessionID))
		return map[string]any{}, nil
	case "session/set_mode":
		a.mu.Lock()
		a.modes[p.SessionID] = p.ModeID
		a.mu.Unlock()
		a.update(p.SessionID, map[string]any{"sessionUpdate": "current_mode_update", "currentModeId": p.ModeID})
		return map[string]any{}, nil
	case "session/prompt":
		return a.prompt(p)
	}
	return nil, &acprpc.Error{Code: acprpc.CodeMethodNotFound, Message: "acpfake: no " + method}
}

func (a *agent) prompt(p sessionParams) (any, error) {
	var text string
	for _, b := range p.Prompt {
		text += b.Text
	}
	switch {
	case strings.HasPrefix(text, "crash") && a.subprocess:
		os.Exit(3)
	case strings.HasPrefix(text, "slow"):
		ch := make(chan struct{})
		a.mu.Lock()
		a.cancels[p.SessionID] = ch
		a.mu.Unlock()
		a.update(p.SessionID, chunk("working..."))
		<-ch
		return map[string]any{"stopReason": "cancelled"}, nil
	case strings.HasPrefix(text, "permission"):
		var res struct {
			Outcome struct {
				Outcome  string `json:"outcome"`
				OptionID string `json:"optionId"`
			} `json:"outcome"`
		}
		err := a.conn.Call(context.Background(), "session/request_permission", map[string]any{
			"sessionId": p.SessionID,
			"toolCall":  map[string]any{"toolCallId": "tc-1", "title": "write file"},
			"options": []map[string]any{
				{"optionId": "allow", "name": "Allow", "kind": "allow_once"},
				{"optionId": "deny", "name": "Deny", "kind": "reject_once"},
			},
		}, &res)
		if err != nil {
			return nil, err
		}
		a.update(p.SessionID, chunk("permission:"+res.Outcome.Outcome+":"+res.Outcome.OptionID))
		return map[string]any{"stopReason": "end_turn"}, nil
	}
	a.update(p.SessionID, chunk("echo:"+text))
	return map[string]any{"stopReason": "end_turn"}, nil
}
