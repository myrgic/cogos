// heap_watermark.go — backstop for memory-heavy reconcile providers (#716).
//
// When the Go live heap is already above a watermark, providers that opt in
// via HeapHeavy() are skipped for the current reconcile cycle instead of
// letting the heap grow without bound. Skips are logged (throttled per
// provider) and counted in heapGuardSkips for telemetry and tests.
package engine

import (
	"log/slog"
	"os"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// heapHeavyProvider is the optional interface a Reconcilable implements to
// opt in to the heap watermark guard.
type heapHeavyProvider interface {
	HeapHeavy() bool
}

// reconcileHeapWatermarkBytes is the live-heap ceiling (3 GiB by default).
// Overridable at startup via COGOS_RECONCILE_HEAP_WATERMARK_MB; 0 disables.
var reconcileHeapWatermarkBytes uint64 = 3 << 30

// heapLiveBytes reads the live heap cheaply (no stop-the-world). A var so
// tests can stub it.
var heapLiveBytes = func() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample[0].Value.Uint64()
}

// heapGuardSkips counts providers skipped by the guard.
var heapGuardSkips atomic.Uint64

const heapGuardWarnEvery = time.Minute

var (
	heapGuardWarnMu   sync.Mutex
	heapGuardWarnLast = map[string]time.Time{}
)

// parseHeapWatermarkMB parses an integer MiB value. Returns the watermark in
// bytes; "0" yields 0 (guard disabled). ok is false for unparsable input.
func parseHeapWatermarkMB(s string) (uint64, bool) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n << 20, true
}

var heapWatermarkEnvOnce sync.Once

func loadHeapWatermarkFromEnv() {
	v, set := os.LookupEnv("COGOS_RECONCILE_HEAP_WATERMARK_MB")
	if !set {
		return
	}
	b, ok := parseHeapWatermarkMB(v)
	if !ok {
		slog.Warn("reconcile: unparsable COGOS_RECONCILE_HEAP_WATERMARK_MB; keeping default",
			"value", v, "default_mb", reconcileHeapWatermarkBytes>>20)
		return
	}
	reconcileHeapWatermarkBytes = b
}

func init() {
	heapWatermarkEnvOnce.Do(loadHeapWatermarkFromEnv)
}

// heapGuardSkip reports whether provider should be skipped this cycle because
// it is heap-heavy and the live heap is above the watermark.
func heapGuardSkip(provider any, providerType string) bool {
	hp, ok := provider.(heapHeavyProvider)
	if !ok || !hp.HeapHeavy() {
		return false
	}
	watermark := reconcileHeapWatermarkBytes
	if watermark == 0 {
		return false
	}
	heap := heapLiveBytes()
	if heap <= watermark {
		return false
	}
	heapGuardSkips.Add(1)

	heapGuardWarnMu.Lock()
	now := time.Now()
	warn := now.Sub(heapGuardWarnLast[providerType]) >= heapGuardWarnEvery
	if warn {
		heapGuardWarnLast[providerType] = now
	}
	heapGuardWarnMu.Unlock()

	if warn {
		slog.Warn("reconcile: heap above watermark; skipping memory-heavy provider this cycle",
			"provider", providerType,
			"heap_mb", heap>>20,
			"watermark_mb", watermark>>20,
		)
	}
	return true
}
