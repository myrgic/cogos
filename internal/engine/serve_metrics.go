// serve_metrics.go — GET /metrics (RFC-040 S1).
//
// One HTTP handler exposing the CURRENT kernel health snapshot (provider
// counts + RFC-040 S0 host gauges) in Prometheus text exposition format
// 0.0.4. This is a courtesy interop door, not a commitment to the prom
// ecosystem (RFC-040 §S1): no history, no aggregation, no query language —
// exactly the values the tick most recently produced, rendered fresh on
// every scrape.
//
// Side-effect-free: handleMetrics calls buildKernelHealthSnapshotPeek, the
// existing non-consuming form of the snapshot builder (see
// autonomic_ticker.go), so scraping /metrics can never steal the delta the
// autonomic ticker's #432 abandoned-inference escalation depends on. Per
// RFC-040 N1, this endpoint is explicitly exempt from the "no continuous
// gauge readout" rule — N1 governs agent per-turn context injection only; a
// raw current-value HTTP dump is the whole point of S1.
package engine

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// prometheusContentType is the exact Content-Type the Prometheus text
// exposition format 0.0.4 spec expects.
const prometheusContentType = "text/plain; version=0.0.4"

// handleMetrics serves GET /metrics: the current KernelHealthSnapshot
// (provider health counts + host vitals) rendered as Prometheus text
// exposition format 0.0.4.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := buildKernelHealthSnapshotPeek(r.Context())

	w.Header().Set("Content-Type", prometheusContentType)
	_, _ = w.Write([]byte(renderPrometheusMetrics(snap) + renderTokenSpendMetrics()))
}

// promMetric describes one exposition-format metric block: HELP + TYPE
// lines followed by exactly one sample. v is pre-formatted so gauge
// (uint64/float64) and counter (int64) values share one rendering path.
type promMetric struct {
	name    string
	help    string
	mtype   string // "gauge" or "counter"
	present bool   // false suppresses the whole block (an omitted host gauge)
	value   string
}

// renderPrometheusMetrics renders snap as Prometheus text exposition format
// 0.0.4: current values only, one # HELP and # TYPE line per metric, no
// history and no aggregation (RFC-040 S1). Provider-count and anomaly
// metrics are always present (HealthCounts/Anomalies are plain ints, never
// absent); host-gauge metrics are present only when the corresponding
// HostVitals field was successfully sampled — RFC-040's soft-degrade
// contract applies here exactly as it does to the JSON snapshot: an
// unsampled gauge is a missing metric, never a fabricated zero.
func renderPrometheusMetrics(snap KernelHealthSnapshot) string {
	metrics := []promMetric{
		{
			name:    "cogos_providers_healthy",
			help:    "Number of registered providers currently healthy, per the latest autonomic tick.",
			mtype:   "gauge",
			present: true,
			value:   strconv.Itoa(snap.Counts.Healthy),
		},
		{
			name:    "cogos_providers_degraded",
			help:    "Number of registered providers currently degraded, per the latest autonomic tick.",
			mtype:   "gauge",
			present: true,
			value:   strconv.Itoa(snap.Counts.Degraded),
		},
		{
			name:    "cogos_providers_missing",
			help:    "Number of registered providers currently missing, per the latest autonomic tick.",
			mtype:   "gauge",
			present: true,
			value:   strconv.Itoa(snap.Counts.Missing),
		},
		{
			name:    "cogos_providers_suspended",
			help:    "Number of registered providers currently suspended, per the latest autonomic tick.",
			mtype:   "gauge",
			present: true,
			value:   strconv.Itoa(snap.Counts.Suspended),
		},
		{
			name:    "cogos_anomalies",
			help:    "Abandoned/canceled internal inference calls (#432) observed since the previous autonomic tick.",
			mtype:   "gauge",
			present: true,
			value:   strconv.Itoa(snap.Anomalies),
		},
		{
			name:    "cogos_anomalies_total",
			help:    "Cumulative abandoned/canceled internal inference calls (#432) since kernel process start.",
			mtype:   "counter",
			present: true,
			value:   strconv.FormatInt(snap.AnomaliesTotal, 10),
		},
		{
			name:    "cogos_disk_free_bytes",
			help:    "Free disk space, in bytes, on the kernel's workspace volume (RFC-040 S0).",
			mtype:   "gauge",
			present: snap.HostVitals.DiskFreeBytes != nil,
			value:   formatUint64Ptr(snap.HostVitals.DiskFreeBytes),
		},
		{
			name:    "cogos_mem_free_bytes",
			help:    "Free host memory, in bytes (RFC-040 S0). Omitted on platforms with no cheap non-cgo reading.",
			mtype:   "gauge",
			present: snap.HostVitals.MemFreeBytes != nil,
			value:   formatUint64Ptr(snap.HostVitals.MemFreeBytes),
		},
		{
			name:    "cogos_load1",
			help:    "1-minute host load average (RFC-040 S0). Omitted on platforms with no cheap non-cgo reading.",
			mtype:   "gauge",
			present: snap.HostVitals.Load1 != nil,
			value:   formatFloat64Ptr(snap.HostVitals.Load1),
		},
		{
			name:    "cogos_uptime_seconds",
			help:    "Kernel process uptime, in seconds — time since this process started, not machine boot time (RFC-040 S0).",
			mtype:   "gauge",
			present: snap.HostVitals.UptimeSeconds != nil,
			value:   formatUint64Ptr(snap.HostVitals.UptimeSeconds),
		},
		{
			name:    "cogos_inference_p50_ms",
			help:    "Rolling median duration, in ms, of non-streaming completions: dispatch, tool-loop, autonomic consult, and external chat/Anthropic-compat HTTP. Excludes streaming (RFC-040 S0). Absent until at least one such call has completed.",
			mtype:   "gauge",
			present: snap.HostVitals.InferenceP50Ms != nil,
			value:   formatFloat64Ptr(snap.HostVitals.InferenceP50Ms),
		},
		{
			name:    "cogos_inference_queue",
			help:    "Current count of in-flight non-streaming completions (dispatch, tool-loop, autonomic consult, external chat/Anthropic-compat HTTP; excludes streaming) (RFC-040 S0). Not a literal FIFO queue depth; see dispatch_inference_metrics.go.",
			mtype:   "gauge",
			present: snap.HostVitals.InferenceQueue != nil,
			value:   formatIntPtr(snap.HostVitals.InferenceQueue),
		},
	}

	var b strings.Builder
	for _, m := range metrics {
		if !m.present {
			continue
		}
		fmt.Fprintf(&b, "# HELP %s %s\n", m.name, m.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", m.name, m.mtype)
		fmt.Fprintf(&b, "%s %s\n", m.name, m.value)
	}
	return b.String()
}

// tokenSpendKey identifies one cogos_tokens_spent_total label set.
type tokenSpendKey struct {
	provider string
	model    string
}

// tokenSpend is the process-lifetime token accumulator fed by RecordTurn.
// Guarded by tokenSpendMu: RecordTurn and the /metrics renderer run
// concurrently. Counts reset on kernel restart (Prometheus counter
// semantics: a reset is a normal counter discontinuity).
var (
	tokenSpendMu sync.Mutex
	tokenSpend   = map[tokenSpendKey]*TurnUsage{}
)

// recordTokenSpend adds one turn's usage to the per-(provider, model)
// accumulator. A zero usage (errored turn, no provider usage) is a no-op,
// so nothing is fabricated.
func recordTokenSpend(provider, model string, u TurnUsage) {
	if u == (TurnUsage{}) {
		return
	}
	tokenSpendMu.Lock()
	defer tokenSpendMu.Unlock()
	k := tokenSpendKey{provider: provider, model: model}
	acc := tokenSpend[k]
	if acc == nil {
		acc = &TurnUsage{}
		tokenSpend[k] = acc
	}
	acc.InputTokens += u.InputTokens
	acc.OutputTokens += u.OutputTokens
	acc.CacheReadTokens += u.CacheReadTokens
	acc.CacheWriteTokens += u.CacheWriteTokens
}

// renderTokenSpendMetrics renders the cogos_tokens_spent_total counter
// family, one series per (provider, model, kind). Kept separate from
// renderPrometheusMetrics because it is a labeled family, not the
// one-sample-per-block shape that renderPrometheusMetrics enforces.
// Empty when no turn has recorded usage yet (no fabricated zero series).
func renderTokenSpendMetrics() string {
	tokenSpendMu.Lock()
	keys := make([]tokenSpendKey, 0, len(tokenSpend))
	snap := make(map[tokenSpendKey]TurnUsage, len(tokenSpend))
	for k, v := range tokenSpend {
		keys = append(keys, k)
		snap[k] = *v
	}
	tokenSpendMu.Unlock()
	if len(keys) == 0 {
		return ""
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].provider != keys[j].provider {
			return keys[i].provider < keys[j].provider
		}
		return keys[i].model < keys[j].model
	})

	var b strings.Builder
	const name = "cogos_tokens_spent_total"
	fmt.Fprintf(&b, "# HELP %s Cumulative provider tokens spent per provider, model, and kind, since kernel process start (rolled up from turn usage in RecordTurn).\n", name)
	fmt.Fprintf(&b, "# TYPE %s counter\n", name)
	for _, k := range keys {
		u := snap[k]
		for _, kv := range []struct {
			kind string
			n    int
		}{
			{"input", u.InputTokens},
			{"output", u.OutputTokens},
			{"cache_read", u.CacheReadTokens},
			{"cache_write", u.CacheWriteTokens},
		} {
			fmt.Fprintf(&b, "%s{provider=%q,model=%q,kind=%q} %d\n", name, k.provider, k.model, kv.kind, kv.n)
		}
	}
	return b.String()
}

func formatUint64Ptr(v *uint64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatUint(*v, 10)
}

func formatFloat64Ptr(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'g', -1, 64)
}

func formatIntPtr(v *int) string {
	if v == nil {
		return ""
	}
	return strconv.Itoa(*v)
}
