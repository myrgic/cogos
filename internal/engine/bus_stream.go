// bus_stream.go — bus-session-backed SSE broker (parallel to the ledger
// EventBroker in eventbus.go).
//
// Track 5 Phase 3: library-only port of root's bus_stream.go broker so that
// AppendEvent can fan out to SSE subscribers. No HTTP route is registered in
// this phase — PR #16 already owns `/v1/events/stream` (ledger-backed). The
// broker exists here so future work (or a caller that wires its own SSE
// handler) can subscribe to per-bus events without re-running the port.
//
// This surface is intentionally distinct from the ledger EventBroker:
//
//	EventBroker  (eventbus.go)      — ledger-append events, hash-chain over all events
//	BusEventBroker (this file)      — per-bus events from .cog/.state/buses/{id}/events.jsonl
//
// Per I2 the two chains stay separate.
package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Defaults copied verbatim from root so reapers/limits behave identically.
const (
	busSSEMaxPerBus      = 25
	busSSEIdleTimeout    = 2 * time.Minute
	busSSEReaperInterval = 30 * time.Second
)

// busSSESubscriber tracks per-connection metadata for liveness detection.
type busSSESubscriber struct {
	ch         chan *BusBlock
	ctx        context.Context // request context; Done() = client disconnected
	lastWrite  time.Time
	consumerID string // optional consumer identity for dedup
}

// BusEventBroker manages SSE subscribers for real-time bus event delivery.
// Per-bus indexed, with a wildcard key "*" for cross-bus subscriptions.
type BusEventBroker struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan *BusBlock]*busSSESubscriber
}

// NewBusEventBroker constructs an empty broker.
func NewBusEventBroker() *BusEventBroker {
	return &BusEventBroker{
		subscribers: make(map[string]map[chan *BusBlock]*busSSESubscriber),
	}
}

// StartReaper launches a background goroutine that periodically sweeps stale
// SSE connections across all buses. This is the "belt" — catches abandoned
// connections regardless of whether new ones are arriving.
func (b *BusEventBroker) StartReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(busSSEReaperInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reaped := b.sweepAll()
				if reaped > 0 {
					slog.Debug("bus-stream: reaper closed stale connections", "count", reaped)
				}
			}
		}
	}()
}

// sweepAll sweeps stale/dead subscribers across ALL buses.
func (b *BusEventBroker) sweepAll() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for busID := range b.subscribers {
		now := time.Now()
		subs := b.subscribers[busID]
		for ch, sub := range subs {
			dead := false
			select {
			case <-sub.ctx.Done():
				dead = true
			default:
			}
			if !dead && now.Sub(sub.lastWrite) > busSSEIdleTimeout {
				dead = true
			}
			if dead {
				delete(subs, ch)
				close(ch)
				total++
			}
		}
		if len(subs) == 0 {
			delete(b.subscribers, busID)
		}
	}
	return total
}

// Subscribe registers a channel to receive events for a given bus.
// If consumerID is non-empty, any existing subscription with the same
// consumer identity is evicted first. If the bus is at the connection
// limit, stale/dead subscribers are swept first; returns false if still
// at capacity afterward.
func (b *BusEventBroker) Subscribe(busID string, ch chan *BusBlock, ctx context.Context, consumerID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscribers[busID] == nil {
		b.subscribers[busID] = make(map[chan *BusBlock]*busSSESubscriber)
	}

	// Consumer-identity dedup: close previous connection from same consumer.
	if consumerID != "" {
		for oldCh, sub := range b.subscribers[busID] {
			if sub.consumerID == consumerID {
				delete(b.subscribers[busID], oldCh)
				close(oldCh)
				slog.Debug("bus-stream: replaced connection for consumer",
					"consumer", consumerID, "bus", busID)
				break
			}
		}
	}

	if len(b.subscribers[busID]) >= busSSEMaxPerBus {
		b.sweepLocked(busID)
	}
	if len(b.subscribers[busID]) >= busSSEMaxPerBus {
		slog.Warn("bus-stream: REJECT at capacity", "bus", busID, "cap", busSSEMaxPerBus)
		return false
	}

	b.subscribers[busID][ch] = &busSSESubscriber{
		ch:         ch,
		ctx:        ctx,
		lastWrite:  time.Now(),
		consumerID: consumerID,
	}
	return true
}

// sweepLocked removes dead or idle subscribers for a bus.
// Caller must hold b.mu (write lock).
func (b *BusEventBroker) sweepLocked(busID string) {
	subs, ok := b.subscribers[busID]
	if !ok {
		return
	}
	now := time.Now()
	for ch, sub := range subs {
		dead := false
		select {
		case <-sub.ctx.Done():
			dead = true
		default:
		}
		if !dead && now.Sub(sub.lastWrite) > busSSEIdleTimeout {
			dead = true
		}
		if dead {
			slog.Debug("bus-stream: evicting stale subscriber",
				"bus", busID, "last_write_ago", now.Sub(sub.lastWrite).Round(time.Second))
			delete(subs, ch)
			close(ch)
		}
	}
	if len(subs) == 0 {
		delete(b.subscribers, busID)
	}
}

// SubscriberCount returns the number of active subscribers for a bus.
func (b *BusEventBroker) SubscriberCount(busID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[busID])
}

// Unsubscribe removes a channel from a bus's subscriber set.
func (b *BusEventBroker) Unsubscribe(busID string, ch chan *BusBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subs, ok := b.subscribers[busID]; ok {
		delete(subs, ch)
		if len(subs) == 0 {
			delete(b.subscribers, busID)
		}
	}
}

// TouchWrite updates the lastWrite timestamp for a subscriber.
func (b *BusEventBroker) TouchWrite(busID string, ch chan *BusBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subs, ok := b.subscribers[busID]; ok {
		if sub, ok := subs[ch]; ok {
			sub.lastWrite = time.Now()
		}
	}
}

// Publish sends an event to all subscribers of a bus AND to wildcard ("*")
// subscribers. Non-blocking: drops if channel is full.
func (b *BusEventBroker) Publish(busID string, evt *BusBlock) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if subs, ok := b.subscribers[busID]; ok {
		for ch := range subs {
			select {
			case ch <- evt:
			default:
			}
		}
	}

	if busID != "*" {
		if subs, ok := b.subscribers["*"]; ok {
			for ch := range subs {
				select {
				case ch <- evt:
				default:
				}
			}
		}
	}
}
