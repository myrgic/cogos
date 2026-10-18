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
//	"ignore_cancel ." → streams one update, then parks the turn forever and never
//	                   honours session/cancel (an agent that drops the notification)
//	"crash ..."      → the process exits with status 3 (subprocess mode only)
//	"modelinfo ..."  → reports the model and the count of set_model requests this
//	                   session has received (what the agent held when the prompt
//	                   reached it, in one chunk)
//	"huge ..."       → streams agent text well past the kernel's 1 MiB report cap
//	anything else    → one agent_message_chunk echoing the text, stopReason "end_turn"
//
// A session/set_model naming a model id with the "reject" prefix is refused with
// an error, so a caller's model-application failure path is reachable.
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

// FailLoadPrefix is a reserved session id prefix: a session/load naming an id
// starting with it is refused with an error. It exists so a caller's
// load-failure path is reachable from a test, the way "reject" makes a
// set_model failure reachable.
const FailLoadPrefix = "fail-load:"

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

	mu        sync.Mutex
	next      int
	modes     map[string]string
	models    map[string]string
	modelSets map[string]int // session id -> set_model requests received
	cancels   map[string]chan struct{}
}

// Serve runs the fake agent over r/w. The returned channel closes when r
// ends. subprocess enables the "crash" behaviour (os.Exit).
func Serve(r io.Reader, w io.Writer, subprocess bool) <-chan struct{} {
	a := &agent{subprocess: subprocess, modes: map[string]string{}, models: map[string]string{},
		modelSets: map[string]int{}, cancels: map[string]chan struct{}{}}
	a.conn = acprpc.NewConn(r, w, a)
	return a.conn.Done()
}

type sessionParams struct {
	SessionID string `json:"sessionId"`
	Cwd       string `json:"cwd"`
	ModeID    string `json:"modeId"`
	ModelID   string `json:"modelId"`
	Prompt    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"prompt"`
}

func (a *agent) newID(prefix string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next++
	// pid-qualified: each fake agent is its own process with its own counter, and
	// the kernel registry is keyed by session id across processes.
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), a.next)
}

func (a *agent) update(sid string, update map[string]any) {
	_ = a.conn.Notify("session/update", map[string]any{"sessionId": sid, "update": update})
}

// hugeChunks/hugeChunkBytes size the "huge ..." turn's agent text: 160 chunks
// of 8 KiB, comfortably past the kernel's 1 MiB report cap.
const (
	hugeChunks     = 160
	hugeChunkBytes = 8 << 10
)

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
	case "session/set_model":
		if p.ModelID == "" {
			return nil, &acprpc.Error{Code: -32602, Message: "set_model needs modelId"}
		}
		a.mu.Lock()
		a.modelSets[p.SessionID]++ // every request the agent received, refused ones included
		a.mu.Unlock()
		if strings.HasPrefix(p.ModelID, "reject") {
			return nil, &acprpc.Error{Code: -32602, Message: "set_model refused " + p.ModelID}
		}
		a.mu.Lock()
		a.models[p.SessionID] = p.ModelID
		a.mu.Unlock()
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
	case strings.HasPrefix(text, "ignore_cancel"):
		// An agent that never honours session/cancel: it reports that it is
		// working, then parks the turn forever. The process still exits when
		// its stdin closes (MaybeRunMain), so a client that gives up and
		// stops the session still sees the turn end.
		a.update(p.SessionID, chunk("working..."))
		select {}
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
	case strings.HasPrefix(text, "modelinfo"):
		// Reports what THIS agent process holds for the session, and how many
		// set_model requests it has received: a caller that applies a model
		// before the prompt shows both here.
		a.mu.Lock()
		model, calls := a.models[p.SessionID], a.modelSets[p.SessionID]
		a.mu.Unlock()
		a.update(p.SessionID, chunk(fmt.Sprintf("modelinfo:model=%s:set_model_calls=%d", model, calls)))
		return map[string]any{"stopReason": "end_turn"}, nil
	case strings.HasPrefix(text, "huge"):
		// Agent text far past the kernel's report cap (hugeChunks *
		// hugeChunkBytes = 1.25 MiB), in chunks the size of a real update.
		for i := 0; i < hugeChunks; i++ {
			a.update(p.SessionID, chunk(strings.Repeat("x", hugeChunkBytes)))
		}
		return map[string]any{"stopReason": "end_turn"}, nil
	case strings.HasPrefix(text, "settings"):
		// Reports what THIS agent process holds for the session (a fresh
		// process after a crash knows only what was re-applied to it).
		a.mu.Lock()
		mode, model := a.modes[p.SessionID], a.models[p.SessionID]
		a.mu.Unlock()
		a.update(p.SessionID, chunk("settings:mode="+mode+":model="+model))
		return map[string]any{"stopReason": "end_turn"}, nil
	}
	a.update(p.SessionID, chunk("echo:"+text))
	return map[string]any{"stopReason": "end_turn"}, nil
}
