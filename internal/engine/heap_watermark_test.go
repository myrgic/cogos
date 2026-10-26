package engine

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// stubHeap replaces heapLiveBytes for the duration of a test.
func stubHeap(t *testing.T, bytes uint64) {
	t.Helper()
	prev := heapLiveBytes
	heapLiveBytes = func() uint64 { return bytes }
	t.Cleanup(func() { heapLiveBytes = prev })
}

// setWatermark overrides reconcileHeapWatermarkBytes for the duration of a test.
func setWatermark(t *testing.T, bytes uint64) {
	t.Helper()
	prev := reconcileHeapWatermarkBytes
	reconcileHeapWatermarkBytes = bytes
	t.Cleanup(func() { reconcileHeapWatermarkBytes = prev })
}

// heapFake wraps flakyReconcilable and opts in to HeapHeavy, counting LoadConfig.
type heapFake struct {
	*flakyReconcilable
	heavy bool
	loads atomic.Int32
}

func (h *heapFake) HeapHeavy() bool { return h.heavy }

func (h *heapFake) LoadConfig(s string) (any, error) {
	h.loads.Add(1)
	return h.flakyReconcilable.LoadConfig(s)
}

func TestHeapGuardSkip_HeavyAboveWatermark(t *testing.T) {
	setWatermark(t, 1<<30)
	stubHeap(t, 2<<30)
	before := heapGuardSkips.Load()
	if !heapGuardSkip(&heapFake{flakyReconcilable: newFlakyReconcilable("h"), heavy: true}, "h") {
		t.Fatal("heavy provider above watermark: want skip")
	}
	if heapGuardSkips.Load() != before+1 {
		t.Errorf("skip counter did not advance")
	}
}

func TestHeapGuardSkip_HeavyBelowWatermark(t *testing.T) {
	setWatermark(t, 1<<30)
	stubHeap(t, 512<<20)
	if heapGuardSkip(&heapFake{flakyReconcilable: newFlakyReconcilable("h"), heavy: true}, "h") {
		t.Fatal("heavy provider below watermark: want no skip")
	}
}

func TestHeapGuardSkip_LightProviderAboveWatermark(t *testing.T) {
	setWatermark(t, 1<<30)
	stubHeap(t, 2<<30)
	if heapGuardSkip(&heapFake{flakyReconcilable: newFlakyReconcilable("l"), heavy: false}, "l") {
		t.Fatal("HeapHeavy()=false: want no skip")
	}
}

func TestHeapGuardSkip_NotImplementingAboveWatermark(t *testing.T) {
	setWatermark(t, 1<<30)
	stubHeap(t, 2<<30)
	if heapGuardSkip(newFlakyReconcilable("n"), "n") {
		t.Fatal("provider without HeapHeavy: want no skip")
	}
}

func TestHeapGuardSkip_ZeroWatermarkDisables(t *testing.T) {
	setWatermark(t, 0)
	stubHeap(t, 8<<30)
	if heapGuardSkip(&heapFake{flakyReconcilable: newFlakyReconcilable("h"), heavy: true}, "h") {
		t.Fatal("watermark 0: want guard disabled")
	}
}

func TestParseHeapWatermarkMB(t *testing.T) {
	if b, ok := parseHeapWatermarkMB("512"); !ok || b != 512<<20 {
		t.Errorf(`"512" = (%d, %v), want (512 MiB, true)`, b, ok)
	}
	if b, ok := parseHeapWatermarkMB("0"); !ok || b != 0 {
		t.Errorf(`"0" = (%d, %v), want (0, true) disabled`, b, ok)
	}
	if _, ok := parseHeapWatermarkMB("abc"); ok {
		t.Errorf(`"abc" ok = true, want false`)
	}
}

// TestRunOneCycle_HeapGuardSkipsHeavyProvider: above the watermark a heap-heavy
// provider is not loaded or fetched, the cycle returns nil, and no failure is
// recorded. The negative control shows that below the watermark the same
// provider does run.
func TestRunOneCycle_HeapGuardSkipsHeavyProvider(t *testing.T) {
	setWatermark(t, 1<<30)

	// Guarded: heap above watermark.
	stubHeap(t, 2<<30)
	guarded := &heapFake{flakyReconcilable: newFlakyReconcilable("heavy-guarded"), heavy: true}
	guarded.setFailCount(0)
	d := newBackoffTestDaemon(t, guarded, -1)
	if err := d.runOneCycle(context.Background(), guarded.Type()); err != nil {
		t.Fatalf("skipped cycle returned %v, want nil", err)
	}
	if n := guarded.loads.Load(); n != 0 {
		t.Errorf("LoadConfig calls = %d, want 0 when guarded", n)
	}
	if n := guarded.fetchCount.Load(); n != 0 {
		t.Errorf("FetchLive calls = %d, want 0 when guarded", n)
	}
	if !d.backoff.Ready(guarded.Type(), int(d.tickSeq.Load())) {
		t.Errorf("skipped cycle left a backoff window; want none (skip is not a failure)")
	}

	// Negative control: heap below watermark, same provider runs.
	stubHeap(t, 256<<20)
	control := &heapFake{flakyReconcilable: newFlakyReconcilable("heavy-control"), heavy: true}
	control.setFailCount(0)
	dc := newBackoffTestDaemon(t, control, -1)
	_ = dc.runOneCycle(context.Background(), control.Type())
	if n := control.loads.Load(); n == 0 {
		t.Errorf("negative control: LoadConfig not called below watermark")
	}
	if n := control.fetchCount.Load(); n == 0 {
		t.Errorf("negative control: FetchLive not called below watermark")
	}
}

var _ reconcile.Reconcilable = (*heapFake)(nil)
