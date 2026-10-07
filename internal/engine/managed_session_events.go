// managed_session_events.go — sequence-numbered, bounded event ring for
// kernel-owned agent sessions (ADR-093 §7 event surface, brief
// agent-processes-in-kernel.md design point 2).
//
// Every event a driven session produces (ACP session/update, permission
// request/resolution, turn start/end, lifecycle state) is appended here with
// a monotonically increasing per-session Seq. A client that reconnects with
// since=<last seq it saw> gets every retained event after that point, then
// the live tail, with no gap and no duplicate — Subscribe takes the replay
// snapshot and registers the live channel under one lock. When the ring has
// already evicted events the client needs, Subscribe reports a gap so the
// client can refetch history from the agent's own store (Hermes SessionDB).
package engine

import (
	"encoding/json"
	"sync"
	"time"
)

// DefaultManagedEventRingSize bounds per-session replay (brief: "e.g. 2,000
// events").
const DefaultManagedEventRingSize = 2000

// Event kinds emitted on a managed session's stream.
const (
	MSEventSessionUpdate      = "session_update"      // data: the ACP update object verbatim
	MSEventPermissionRequest  = "permission_request"  // data: {request_id, params}
	MSEventPermissionResolved = "permission_resolved" // data: {request_id, outcome, option_id, by}
	MSEventTurnStart          = "turn_start"          // data: {turn_id, prompt}
	MSEventTurnEnd            = "turn_end"            // data: {turn_id, stop_reason, error}
	MSEventState              = "state"               // data: {state, error}
)

// ManagedSessionEvent is one entry on a managed session's event stream, and
// the exact JSON frame the events WebSocket sends.
type ManagedSessionEvent struct {
	Seq       uint64          `json:"seq"`
	SessionID string          `json:"session_id"`
	Time      time.Time       `json:"time"`
	Kind      string          `json:"kind"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// EventRing is a bounded, sequence-numbered event log with live fan-out.
type EventRing struct {
	sessionID string
	size      int

	mu     sync.Mutex
	buf    []ManagedSessionEvent // ring storage, len <= size
	start  int                   // index of oldest event in buf
	seq    uint64                // seq of the newest event (0 = none yet)
	subs   map[chan ManagedSessionEvent]struct{}
	closed bool
}

// NewEventRing returns an empty ring holding at most size events.
func NewEventRing(sessionID string, size int) *EventRing {
	if size <= 0 {
		size = DefaultManagedEventRingSize
	}
	return &EventRing{sessionID: sessionID, size: size, subs: map[chan ManagedSessionEvent]struct{}{}}
}

// Append records an event and fans it out to live subscribers. A subscriber
// whose buffer is full is dropped (its channel closed) rather than blocking
// the agent: it reconnects with since= and replays from the ring.
func (r *EventRing) Append(kind string, data any) ManagedSessionEvent {
	var raw json.RawMessage
	switch d := data.(type) {
	case nil:
	case json.RawMessage:
		raw = d
	default:
		b, err := json.Marshal(d)
		if err == nil {
			raw = b
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	ev := ManagedSessionEvent{Seq: r.seq, SessionID: r.sessionID, Time: time.Now().UTC(), Kind: kind, Data: raw}
	if len(r.buf) < r.size {
		r.buf = append(r.buf, ev)
	} else {
		r.buf[r.start] = ev
		r.start = (r.start + 1) % r.size
	}
	for ch := range r.subs {
		select {
		case ch <- ev:
		default:
			delete(r.subs, ch)
			close(ch)
		}
	}
	return ev
}

// LastSeq returns the newest event's seq (0 if none).
func (r *EventRing) LastSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// oldestLocked returns the seq of the oldest retained event (0 if empty).
func (r *EventRing) oldestLocked() uint64 {
	if len(r.buf) == 0 {
		return 0
	}
	return r.buf[r.start].Seq
}

// Since returns retained events with Seq > since, plus whether events the
// caller needed were already evicted (gap).
func (r *EventRing) Since(since uint64) (events []ManagedSessionEvent, gap bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sinceLocked(since)
}

func (r *EventRing) sinceLocked(since uint64) ([]ManagedSessionEvent, bool) {
	oldest := r.oldestLocked()
	gap := oldest > 0 && since+1 < oldest
	out := make([]ManagedSessionEvent, 0)
	for i := 0; i < len(r.buf); i++ {
		ev := r.buf[(r.start+i)%len(r.buf)]
		if ev.Seq > since {
			out = append(out, ev)
		}
	}
	return out, gap
}

// RingSubscription is a replay snapshot plus a live channel.
type RingSubscription struct {
	Replay    []ManagedSessionEvent
	Gap       bool
	OldestSeq uint64
	LastSeq   uint64
	// Live delivers events after Replay. Closed when the subscriber falls
	// behind (buffer full), on Unsubscribe, or when the ring is closed.
	Live <-chan ManagedSessionEvent
	ch   chan ManagedSessionEvent
}

// Subscribe atomically snapshots events after since and registers a live
// channel, so the caller sees every event exactly once in seq order.
func (r *EventRing) Subscribe(since uint64, buffer int) *RingSubscription {
	if buffer <= 0 {
		buffer = 256
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	replay, gap := r.sinceLocked(since)
	ch := make(chan ManagedSessionEvent, buffer)
	if r.closed {
		close(ch)
	} else {
		r.subs[ch] = struct{}{}
	}
	return &RingSubscription{Replay: replay, Gap: gap, OldestSeq: r.oldestLocked(), LastSeq: r.seq, Live: ch, ch: ch}
}

// Unsubscribe removes a subscription (idempotent).
func (r *EventRing) Unsubscribe(s *RingSubscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.subs[s.ch]; ok {
		delete(r.subs, s.ch)
		close(s.ch)
	}
}

// Close ends every live subscription. Retained events stay readable via
// Since/Subscribe (a client may still replay a finished session's tail).
func (r *EventRing) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for ch := range r.subs {
		delete(r.subs, ch)
		close(ch)
	}
}
