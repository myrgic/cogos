package engine

import (
	"context"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/acprpc/acpfake"
)

// Review #666 round 2. The opts.Load branch creates and registers the
// session's event ring before session/load returns, because the load replays
// history as session/update notifications that must land in the ring. If that
// call fails, Create deletes the session from proc.sessions and returns nil —
// and EventRing.Close is the only thing that ends a ring, so the ring and
// every subscription on it used to outlive their session forever.
//
// The fake refuses session/load for an id carrying acpfake.FailLoadPrefix, so
// that path is reachable from a test; the driver's afterRingCreate seam hands
// the test the ring Create has discarded.
func TestHermesACPDriver_LoadFailureClosesRing(t *testing.T) {
	var ring *EventRing
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.afterRingCreate = func(r *EventRing) { ring = r }
	})
	sid := acpfake.FailLoadPrefix + "gone"
	s, err := d.Create(context.Background(), HermesACPOpts{Cwd: t.TempDir(), Load: sid})
	if err == nil {
		t.Fatalf("load of the refused id %q returned a session in state %s, want an error", sid, s.State())
	}
	if ring == nil {
		t.Fatalf("the failed load created no ring: the close under test was never exercised")
	}
	if n := len(d.List()); n != 0 {
		t.Fatalf("failed load left %d session(s) registered", n)
	}
	// Close ends every live subscription. On a ring that was never closed this
	// receive parks: the buffer is empty and no event was retained.
	sub := ring.Subscribe(0, 1)
	select {
	case _, ok := <-sub.Live:
		if ok {
			t.Fatalf("ring %s survived the failed load: its subscription is live", sid)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("ring %s survived the failed load: its subscription was never closed", sid)
	}
}
