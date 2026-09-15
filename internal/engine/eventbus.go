// eventbus.go — in-process event broker for the kernel ledger.
//
// The broker is the live fan-out companion to the hash-chained ledger. Every
// call to AppendEvent publishes the envelope here; subscribers get filtered,
// non-blocking deliveries via a buffered channel. A small ring buffer retains
// recent events so that reconnecting subscribers can replay the tail without
// re-reading the JSONL ledger.
//
// Design constraints (from Agent N's design survey):
//
//   - Broker hook lives INSIDE AppendEvent (the single write sink) so that
//     consolidate.go, cogblock_ledger.go, process.go — all of which call
//     AppendEvent directly — feed the live bus without extra wiring.
//   - Publish is fire-and-forget: the ledger never blocks on a slow SSE
//     consumer. Full subscriber channels drop events and increment a counter.
//   - A package-level currentBroker is set once by NewProcess; tests that want
//     isolation use SetCurrentBroker(nil).
//   - Close unblocks all waiting subscribers so servers can shut down cleanly.
package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultRingSize is the default capacity of the recent-events ring
	// buffer. See Agent N §7.1 — 1024 events × ~320 B/event ≈ 320 KB.
	DefaultRingSize = 1024

	// DefaultSubChanBuffer is the per-subscriber channel buffer. A healthy
	// SSE writer empties this quickly; a wedged one gets dropped frames.
	DefaultSubChanBuffer = 64

	// DefaultMaxSubscribers caps concurrent subscribers per broker. Single
	// global kernel bus — no per-bus scoping.
	DefaultMaxSubscribers = 50
)

// EventFilter selects which events a subscriber receives. Zero values are
// no-ops (don't filter on that field).
type EventFilter struct {
	// SessionID filters to a single session when non-empty.
	SessionID string

	// EventTypePattern is the raw pattern (exact, "prefix.*", or "*").
	// It is compiled once on Subscribe via compileEventTypeMatcher.
	EventTypePattern string

	// Source filters on envelope.Metadata.Source (e.g. "kernel-v3").
	Source string

	// Since, when non-zero, drops envelopes whose parsed Timestamp is
	// before it. Used for live-only tailing; replay uses the ring +
	// disk path instead.
	Since time.Time
}

// subscriber is an internal record for a live consumer.
type subscriber struct {
	id        int
	ch        chan *EventEnvelope
	filter    EventFilter
	matcher   func(string) bool // compiled from filter.EventTypePattern
	ctx       context.Context
	cancel    context.CancelFunc
	connected time.Time
	dropped   atomic.Uint64
}

// eventRing is a fixed-capacity circular buffer of recent envelopes.
// Reads take RLock; Push takes Lock. The buffer stores pointers, so
// copy-on-write isn't needed — envelopes are effectively immutable once
// AppendEvent has returned.
type eventRing struct {
	mu   sync.RWMutex
	buf  []*EventEnvelope
	head int // next write position
	size int // items currently stored (capped at cap)
	cap  int
}

func newEventRing(capacity int) *eventRing {
	if capacity <= 0 {
		capacity = DefaultRingSize
	}
	return &eventRing{buf: make([]*EventEnvelope, capacity), cap: capacity}
}

// Push appends env to the ring, evicting the oldest event when full.
func (r *eventRing) Push(env *EventEnvelope) {
	r.mu.Lock()
	r.buf[r.head] = env
	r.head = (r.head + 1) % r.cap
	if r.size < r.cap {
		r.size++
	}
	r.mu.Unlock()
}

// Snapshot returns a slice of the current contents in chronological order
// (oldest first). The returned slice is a copy; callers may mutate freely.
func (r *eventRing) Snapshot() []*EventEnvelope {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*EventEnvelope, 0, r.size)
	if r.size == 0 {
		return out
	}
	start := r.head - r.size
	if start < 0 {
		start += r.cap
	}
	for i := 0; i < r.size; i++ {
		idx := (start + i) % r.cap
		out = append(out, r.buf[idx])
	}
	return out
}

// Len returns the current number of items in the ring.
func (r *eventRing) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

// Cap returns the ring capacity.
func (r *eventRing) Cap() int {
	return r.cap
}

// EventBroker fans out kernel events to live subscribers.
type EventBroker struct {
	mu             sync.RWMutex
	subs           map[int]*subscriber
	nextID         int
	ring           *eventRing
	closed         atomic.Bool
	maxSubscribers int
	chanBuffer     int
}

// EventBrokerOptions configures NewEventBroker.
type EventBrokerOptions struct {
	RingSize       int
	MaxSubscribers int
	ChanBuffer     int
}

// NewEventBroker constructs a broker with sane defaults.
func NewEventBroker(opts EventBrokerOptions) *EventBroker {
	if opts.RingSize <= 0 {
		opts.RingSize = DefaultRingSize
	}
	if opts.MaxSubscribers <= 0 {
		opts.MaxSubscribers = DefaultMaxSubscribers
	}
	if opts.ChanBuffer <= 0 {
		opts.ChanBuffer = DefaultSubChanBuffer
	}
	return &EventBroker{
		subs:           make(map[int]*subscriber),
		ring:           newEventRing(opts.RingSize),
		maxSubscribers: opts.MaxSubscribers,
		chanBuffer:     opts.ChanBuffer,
	}
}

// ErrBrokerClosed is returned by Subscribe after Close has been called.
type ErrBrokerClosed struct{}

func (ErrBrokerClosed) Error() string { return "event broker: closed" }

// ErrTooManySubscribers is returned when Subscribe would exceed maxSubscribers.
type ErrTooManySubscribers struct{ Max int }

func (e ErrTooManySubscribers) Error() string {
	return "event broker: too many subscribers"
}

// ── Package-level broker registry ───────────────────────────────────────────
//
// In production there is exactly one *Process per kernel, so exactly one
// broker. In tests the suite spins up many parallel processes; a single-slot
// "current broker" would race. We use a registry instead — AppendEvent
// publishes to every installed broker, matchers on the broker side handle
// the filtering. This is the same big-O cost as single-slot for N=1 and
// works correctly for N>1 without serialising tests.

var (
	brokersMu sync.RWMutex
	brokers   = map[*EventBroker]struct{}{}
	// legacyCurrent tracks the most recently installed broker for the
	// CurrentBroker() convenience accessor used in tests. Not consulted
	// by AppendEvent.
	legacyCurrent *EventBroker
)

// RegisterBroker adds b to the package-level broker registry. AppendEvent
// publishes to every registered broker. Idempotent. Safe for concurrent use.
func RegisterBroker(b *EventBroker) {
	if b == nil {
		return
	}
	brokersMu.Lock()
	brokers[b] = struct{}{}
	legacyCurrent = b
	brokersMu.Unlock()
}

// UnregisterBroker removes b from the registry. Idempotent. Safe for concurrent use.
func UnregisterBroker(b *EventBroker) {
	if b == nil {
		return
	}
	brokersMu.Lock()
	delete(brokers, b)
	if legacyCurrent == b {
		legacyCurrent = nil
	}
	brokersMu.Unlock()
}

// SetCurrentBroker replaces the registry with just b (or clears it if nil).
// Kept for compatibility with earlier single-slot wiring; prefer Register /
// UnregisterBroker for additive wiring.
func SetCurrentBroker(b *EventBroker) {
	brokersMu.Lock()
	brokers = map[*EventBroker]struct{}{}
	if b != nil {
		brokers[b] = struct{}{}
	}
	legacyCurrent = b
	brokersMu.Unlock()
}

// CurrentBroker returns the most recently registered broker, or nil.
// Tests use this as a convenience — production code should hold a direct
// *Process reference and call p.Broker().
func CurrentBroker() *EventBroker {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	return legacyCurrent
}

// brokerSnapshot returns a slice copy of all registered brokers so that
// AppendEvent can publish without holding the registry lock across sends.
func brokerSnapshot() []*EventBroker {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	if len(brokers) == 0 {
		return nil
	}
	out := make([]*EventBroker, 0, len(brokers))
	for b := range brokers {
		out = append(out, b)
	}
	return out
}

// ── Publish / Subscribe ─────────────────────────────────────────────────────

// Publish fans env out to matching subscribers. Non-blocking: a full sub
// channel increments its drop counter rather than stalling the ledger. Safe
// after Close — returns silently.
func (b *EventBroker) Publish(env *EventEnvelope) {
	if b == nil || env == nil {
		return
	}
	if b.closed.Load() {
		return
	}
	// Ring first; cheap enough to do unconditionally.
	b.ring.Push(env)

	// Snapshot subscribers under RLock, then release before sending to avoid
	// holding the lock across a channel send (even a non-blocking one).
	b.mu.RLock()
	snap := make([]*subscriber, 0, len(b.subs))
	for _, s := range b.subs {
		snap = append(snap, s)
	}
	b.mu.RUnlock()

	for _, s := range snap {
		if !subscriberMatches(s, env) {
			continue
		}
		select {
		case s.ch <- env:
		default:
			s.dropped.Add(1)
		}
	}
}

// subscriberMatches applies the filter to env and reports whether it should
// be delivered.
func subscriberMatches(s *subscriber, env *EventEnvelope) bool {
	if s == nil || env == nil {
		return false
	}
	if s.filter.SessionID != "" && env.HashedPayload.SessionID != s.filter.SessionID {
		return false
	}
	if s.matcher != nil && !s.matcher(env.HashedPayload.Type) {
		return false
	}
	if s.filter.Source != "" && env.Metadata.Source != s.filter.Source {
		return false
	}
	if !s.filter.Since.IsZero() {
		ts, err := time.Parse(time.RFC3339, env.HashedPayload.Timestamp)
		if err != nil || ts.Before(s.filter.Since) {
			return false
		}
	}
	return true
}

// Subscription is returned by Subscribe. Close unsubscribes and drains the
// channel. Dropped returns the number of events dropped due to a full
// channel — callers can surface this to consumers as a _meta frame.
type Subscription struct {
	Events  <-chan *EventEnvelope
	Dropped func() uint64
	Cancel  func()
}

// Subscribe registers a new live subscriber. The returned channel streams
// matching events; when the passed ctx is cancelled or Cancel is called,
// the subscription is removed and the channel closed.
//
// Replay is handled by the caller via RingReplay — keeping replay out of
// Subscribe avoids a race where events can arrive on the channel before
// the caller has finished streaming the ring snapshot.
func (b *EventBroker) Subscribe(ctx context.Context, filter EventFilter) (*Subscription, error) {
	if b == nil {
		return nil, ErrBrokerClosed{}
	}
	if b.closed.Load() {
		return nil, ErrBrokerClosed{}
	}

	matcher, err := compileEventTypeMatcher(filter.EventTypePattern)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	if len(b.subs) >= b.maxSubscribers {
		b.mu.Unlock()
		return nil, ErrTooManySubscribers{Max: b.maxSubscribers}
	}

	subCtx, cancel := context.WithCancel(ctx)
	s := &subscriber{
		ch:        make(chan *EventEnvelope, b.chanBuffer),
		filter:    filter,
		matcher:   matcher,
		ctx:       subCtx,
		cancel:    cancel,
		connected: time.Now().UTC(),
	}
	id := b.nextID
	s.id = id
	b.nextID++
	b.subs[id] = s
	b.mu.Unlock()

	// When ctx is cancelled upstream (client disconnect, max_duration),
	// remove ourselves and close the channel so the reader loop exits.
	go func() {
		<-subCtx.Done()
		b.removeSub(id)
	}()

	sub := &Subscription{
		Events:  s.ch,
		Dropped: func() uint64 { return s.dropped.Load() },
		Cancel:  cancel,
	}
	return sub, nil
}

func (b *EventBroker) removeSub(id int) {
	b.mu.Lock()
	s, ok := b.subs[id]
	if ok {
		delete(b.subs, id)
	}
	b.mu.Unlock()
	if ok {
		// close under the sub's own cancel guarantee — no more sends
		// will race because Publish snapshots subs under RLock before
		// sending.
		close(s.ch)
	}
}

// SubscriberCount returns the current number of live subscribers.
func (b *EventBroker) SubscriberCount() int {
	if b == nil {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// RingReplay returns a filtered copy of the current ring contents. Callers
// use this to drain the replay window before consuming live events. Events
// with a parsed timestamp earlier than `since` are dropped when since is
// non-zero.
func (b *EventBroker) RingReplay(filter EventFilter, since time.Time) []*EventEnvelope {
	if b == nil {
		return nil
	}
	matcher, err := compileEventTypeMatcher(filter.EventTypePattern)
	if err != nil {
		// An invalid pattern at replay time is unusual — the Subscribe
		// path already validated it. Fall back to "no matcher" (the
		// downstream subscriber will still filter).
		matcher = nil
	}
	pseudo := &subscriber{filter: filter, matcher: matcher}
	all := b.ring.Snapshot()
	out := make([]*EventEnvelope, 0, len(all))
	for _, env := range all {
		if !since.IsZero() {
			ts, err := time.Parse(time.RFC3339, env.HashedPayload.Timestamp)
			if err != nil || ts.Before(since) {
				continue
			}
		}
		if !subscriberMatches(pseudo, env) {
			continue
		}
		out = append(out, env)
	}
	return out
}

// RingOldestHash returns the hash of the oldest envelope currently in the
// ring, or "" if empty. Used by SSE handlers to decide whether a
// Last-Event-ID resume point fits inside the ring or needs to fall through
// to disk.
func (b *EventBroker) RingOldestHash() string {
	if b == nil {
		return ""
	}
	snap := b.ring.Snapshot()
	if len(snap) == 0 {
		return ""
	}
	return snap[0].Metadata.Hash
}

// RingContainsHash reports whether the given hash is present in the ring.
func (b *EventBroker) RingContainsHash(hash string) bool {
	if b == nil || hash == "" {
		return false
	}
	for _, env := range b.ring.Snapshot() {
		if env.Metadata.Hash == hash {
			return true
		}
	}
	return false
}

// Close unblocks all current subscribers, unregisters the broker so
// AppendEvent stops publishing to it, and prevents new subscriptions.
// Idempotent.
func (b *EventBroker) Close() error {
	if b == nil {
		return nil
	}
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	UnregisterBroker(b)
	b.mu.Lock()
	subs := b.subs
	b.subs = map[int]*subscriber{}
	b.mu.Unlock()
	for _, s := range subs {
		s.cancel()
		// removeSub closes the channel, but we skipped that by swapping
		// b.subs; close here to unblock any pending reader.
		// Guard against a concurrent close.
		defer func(ch chan *EventEnvelope) {
			defer func() { _ = recover() }()
			close(ch)
		}(s.ch)
	}
	return nil
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// ParseSinceDuration parses `since` as either an RFC3339 timestamp or a
// shorthand duration like "5m", "1h", "30s". Relative forms are resolved
// against `now`. Empty string returns the zero Time.
func ParseSinceDuration(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	// Try RFC3339 first.
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, nil
	}
	// Fall back to duration shorthand. Go's time.ParseDuration already
	// handles "5m", "1h30m", "45s" — we just need to subtract from now.
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Time{}, err
	}
	return now.Add(-d), nil
}
