// bep_dispatch.go — Phase 2 S4: remote harness dispatch over BEP.
//
// Adds two message types to the existing BEP channel:
//
//	MessageTypeDispatch       (8) — caller sends a DispatchRequest JSON body
//	MessageTypeDispatchResult (9) — remote sends a DispatchBatchResult JSON body
//
// Correlation: each in-flight dispatch is given a uint32 ID embedded in a thin
// envelope (dispatchEnvelope). The receive side echoes the same ID back so that
// concurrent dispatches on a single connection don't cross.
//
// The gate is cluster.enabled: when no BEPEngine is wired, or when no peer
// matches the requested TargetNode, the call fails fast with a clear error.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	bep "github.com/myrgic/cogos/pkg/substrate/bep"
)

// ─── Correlation envelope ────────────────────────────────────────────────────

// dispatchEnvelope is the wire format for MessageTypeDispatch and
// MessageTypeDispatchResult. The ID field correlates request↔result so that
// concurrent dispatches over a single connection don't mix up their responses.
type dispatchEnvelope struct {
	ID      uint32               `json:"id"`
	Request *DispatchRequest     `json:"request,omitempty"` // set on request side
	Result  *DispatchBatchResult `json:"result,omitempty"`  // set on result side
	Error   string               `json:"error,omitempty"`   // set on result side for fatal errors
}

// ─── BEPEngine dispatch extensions ──────────────────────────────────────────

// dispatchInFlight tracks pending remote dispatch calls keyed by correlation ID.
type dispatchInFlight struct {
	ch chan dispatchEnvelope
}

// dispatchCounter is the global correlation ID generator (atomic, per-process).
var dispatchCounter uint32

// nextDispatchID returns the next unique correlation ID.
func nextDispatchID() uint32 {
	return atomic.AddUint32(&dispatchCounter, 1)
}

// SetDispatcher wires an AgentDispatcher into the engine so the receive side
// can run DispatchToHarness locally when a MessageTypeDispatch arrives from a
// peer. Call this after constructing the engine and before Start().
func (e *BEPEngine) SetDispatcher(d AgentDispatcher) {
	e.mu.Lock()
	e.dispatcher = d
	e.mu.Unlock()
}

// findPeerByName returns the PeerConnection whose Name matches target (case-
// sensitive), or nil if no connected peer has that name.
func (e *BEPEngine) findPeerByName(target string) *PeerConnection {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	for _, pc := range e.peers {
		if pc.Name == target {
			return pc
		}
	}
	return nil
}

// RemoteDispatch sends a DispatchRequest to the peer named target and blocks
// until the matching result arrives or the context deadline fires. It is
// called by QueryDispatchToHarness when DispatchRequest.TargetNode is set.
func (e *BEPEngine) RemoteDispatch(ctx context.Context, target string, req DispatchRequest) (*DispatchBatchResult, error) {
	pc := e.findPeerByName(target)
	if pc == nil {
		return nil, &AgentControllerError{
			Code:    "peer_not_connected",
			Message: fmt.Sprintf("cluster dispatch: peer %q is not connected", target),
		}
	}

	id := nextDispatchID()
	env := dispatchEnvelope{ID: id, Request: &req}
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("cluster dispatch: marshal request: %w", err)
	}

	// Register in-flight channel before sending so we never miss the reply.
	ch := make(chan dispatchEnvelope, 1)
	e.inflightMu.Lock()
	e.inflight[id] = &dispatchInFlight{ch: ch}
	e.inflightMu.Unlock()

	defer func() {
		e.inflightMu.Lock()
		delete(e.inflight, id)
		e.inflightMu.Unlock()
	}()

	if err := pc.Wire.WriteMessage(bep.MessageTypeDispatch, payload); err != nil {
		return nil, &AgentControllerError{
			Code:    "peer_send_error",
			Message: fmt.Sprintf("cluster dispatch: send to %q: %v", target, err),
		}
	}

	select {
	case reply := <-ch:
		if reply.Error != "" {
			return nil, &AgentControllerError{
				Code:    "remote_error",
				Message: fmt.Sprintf("cluster dispatch: remote %q: %s", target, reply.Error),
			}
		}
		if reply.Result == nil {
			return nil, &AgentControllerError{
				Code:    "remote_error",
				Message: fmt.Sprintf("cluster dispatch: remote %q returned nil result", target),
			}
		}
		return reply.Result, nil
	case <-ctx.Done():
		return nil, &AgentControllerError{
			Code:    "timeout",
			Message: fmt.Sprintf("cluster dispatch: timed out waiting for result from %q", target),
		}
	}
}

// handleDispatchMessage is called from the peer loop when a MessageTypeDispatch
// arrives. It runs the request locally (clearing TargetNode to prevent loops)
// and sends back a MessageTypeDispatchResult.
func (e *BEPEngine) handleDispatchMessage(pc *PeerConnection, payload []byte) {
	var env dispatchEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Printf("[bep-dispatch] bad dispatch envelope from %s: %v", pc.Name, err)
		return
	}
	if env.Request == nil {
		log.Printf("[bep-dispatch] dispatch envelope from %s has nil request", pc.Name)
		return
	}

	e.mu.Lock()
	d := e.dispatcher
	e.mu.Unlock()

	var reply dispatchEnvelope
	reply.ID = env.ID

	if d == nil {
		reply.Error = "no dispatcher configured on this node"
	} else {
		// Clear TargetNode to prevent routing loops.
		req := *env.Request
		req.TargetNode = ""
		if err := req.Normalize(); err != nil {
			reply.Error = fmt.Sprintf("normalize: %v", err)
		} else {
			// Use a fresh background context with the request's own timeout.
			timeout := time.Duration(req.TimeoutSeconds) * time.Second
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
			defer cancel()

			result, err := d.DispatchToHarness(ctx, req)
			if err != nil {
				reply.Error = err.Error()
			} else {
				reply.Result = result
			}
		}
	}

	out, err := json.Marshal(reply)
	if err != nil {
		log.Printf("[bep-dispatch] marshal result for %s: %v", pc.Name, err)
		return
	}
	if err := pc.Wire.WriteMessage(bep.MessageTypeDispatchResult, out); err != nil {
		log.Printf("[bep-dispatch] send result to %s: %v", pc.Name, err)
	}
}

// handleDispatchResultMessage is called from the peer loop when a
// MessageTypeDispatchResult arrives. It delivers the envelope to the matching
// in-flight channel.
func (e *BEPEngine) handleDispatchResultMessage(payload []byte) {
	var env dispatchEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Printf("[bep-dispatch] bad dispatch result envelope: %v", err)
		return
	}

	e.inflightMu.Lock()
	inf, ok := e.inflight[env.ID]
	e.inflightMu.Unlock()

	if !ok {
		log.Printf("[bep-dispatch] no in-flight dispatch for id=%d (already timed out?)", env.ID)
		return
	}
	select {
	case inf.ch <- env:
	default:
		log.Printf("[bep-dispatch] in-flight channel full for id=%d, dropping result", env.ID)
	}
}

// ─── Additional BEPEngine fields (stored via mu) ────────────────────────────
// dispatcher and inflight are declared in bep_engine_dispatch_fields.go
// (injected into BEPEngine struct via the fields file below).

// inflightMu is intentionally a separate mutex from BEPEngine.mu to avoid
// deadlock: the peer loop holds no external locks when delivering results.

// See bep_engine.go — the struct fields are added there directly.
// This comment exists so the reader can trace back to the field declarations.

// ─── Wire-up helpers (used from bep_engine.go peer loop) ────────────────────

// dispatchFields is embedded into BEPEngine (see bep_engine.go additions).
type dispatchFields struct {
	dispatcher AgentDispatcher
	inflightMu sync.Mutex
	inflight   map[uint32]*dispatchInFlight
}
