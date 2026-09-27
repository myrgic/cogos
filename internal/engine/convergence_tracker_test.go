package engine

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func findConv(snap []ProviderConvergence, name string) (ProviderConvergence, bool) {
	for _, s := range snap {
		if s.Provider == name {
			return s, true
		}
	}
	return ProviderConvergence{}, false
}

func hasReason(s ProviderConvergence, reason string) bool {
	for _, r := range s.Reasons {
		if r == reason {
			return true
		}
	}
	return false
}

// ─── Log capture ──────────────────────────────────────────────────────────────

// logLine is one captured slog record, decoded from the JSON handler.
type logLine struct {
	Level string
	Msg   string
	Attrs map[string]any
}

// syncBuf is an io.Writer safe for the handler to write from any goroutine.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs redirects the default slog logger into a buffer for the duration
// of the test and returns a reader for the captured records. Debug level so
// throttled-to-Debug lines are observable. slog.SetDefault is global, so tests
// using this must not call t.Parallel().
func captureLogs(t *testing.T) func() []logLine {
	t.Helper()
	buf := &syncBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	return func() []logLine {
		var out []logLine
		for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if raw == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(raw), &m); err != nil {
				continue
			}
			level, _ := m["level"].(string)
			msg, _ := m["msg"].(string)
			out = append(out, logLine{Level: level, Msg: msg, Attrs: m})
		}
		return out
	}
}

func countMsg(lines []logLine, msg string) int {
	n := 0
	for _, l := range lines {
		if l.Msg == msg {
			n++
		}
	}
	return n
}

func linesWithMsg(lines []logLine, msg string) []logLine {
	var out []logLine
	for _, l := range lines {
		if l.Msg == msg {
			out = append(out, l)
		}
	}
	return out
}

// ─── Threshold behaviour ──────────────────────────────────────────────────────

func TestConvergenceTracker_FlagsOverBudgetAfterThreshold(t *testing.T) {
	tr := newConvergenceTracker(ConvergenceConfig{
		CycleBudget:      100 * time.Millisecond,
		OverBudgetCycles: 3,
		ClearCycles:      1,
	})
	// Two over-budget cycles: not yet flagged (threshold is 3).
	tr.Observe("slow", convObservation{CycleMs: 500, FetchMs: 480})
	tr.Observe("slow", convObservation{CycleMs: 500, FetchMs: 480})
	if s, _ := findConv(tr.Snapshot(), "slow"); s.Flagged {
		t.Fatalf("flagged after 2 cycles, want not flagged: %+v", s)
	}
	// Third over-budget cycle: flagged with the over_budget reason.
	tr.Observe("slow", convObservation{CycleMs: 500, FetchMs: 480})
	s, ok := findConv(tr.Snapshot(), "slow")
	if !ok || !s.Flagged || !hasReason(s, "over_budget") {
		t.Fatalf("want flagged over_budget after 3 cycles, got %+v", s)
	}
	if s.LastFetchMs != 480 {
		t.Errorf("LastFetchMs = %d, want 480 (dominant phase surfaced)", s.LastFetchMs)
	}
	// A cycle back under budget clears the flag (ClearCycles: 1 here).
	tr.Observe("slow", convObservation{CycleMs: 50, FetchMs: 40})
	if s, _ := findConv(tr.Snapshot(), "slow"); s.Flagged {
		t.Errorf("flag should clear when back under budget, got %+v", s)
	}
}

func TestConvergenceTracker_FlagsPersistentDegraded(t *testing.T) {
	tr := newConvergenceTracker(ConvergenceConfig{DegradedCycles: 3, ClearCycles: 1})
	for i := 0; i < 3; i++ {
		tr.Observe("component", convObservation{CycleMs: 5, Degraded: true}) // fast, but degraded
	}
	s, ok := findConv(tr.Snapshot(), "component")
	if !ok || !s.Flagged || !hasReason(s, "degraded") {
		t.Fatalf("want flagged degraded after 3 cycles, got %+v", s)
	}
	// A healthy cycle clears it (ClearCycles: 1 here).
	tr.Observe("component", convObservation{CycleMs: 5})
	if s, _ := findConv(tr.Snapshot(), "component"); s.Flagged {
		t.Errorf("flag should clear when healthy, got %+v", s)
	}
}

func TestConvergenceTracker_HealthyFastProviderNeverFlags(t *testing.T) {
	tr := newConvergenceTracker(ConvergenceConfig{
		CycleBudget:      100 * time.Millisecond,
		OverBudgetCycles: 3,
		DegradedCycles:   3,
	})
	// An actively-changing provider that stays fast and healthy must never flag,
	// even over many cycles — "has changes" is intentionally not a signal.
	for i := 0; i < 25; i++ {
		tr.Observe("conversations", convObservation{CycleMs: 40, FetchMs: 20})
	}
	if s, _ := findConv(tr.Snapshot(), "conversations"); s.Flagged {
		t.Errorf("fast healthy provider flagged, want never: %+v", s)
	}
}

// TestReconcileDaemon_ConvergenceSurface exercises the public snapshot the way an
// operator/agent (or a CI assertion) would: drive the daemon's tracker with
// numbers matching the real conversations regression (~950ms FetchLive) and
// assert it is surfaced as a flagged over_budget provider.
func TestReconcileDaemon_ConvergenceSurface(t *testing.T) {
	d := NewReconcileDaemon(ReconcileDaemonConfig{
		Convergence: ConvergenceConfig{CycleBudget: 100 * time.Millisecond, OverBudgetCycles: 2},
	})
	d.health.Observe("conversations", convObservation{CycleMs: 950, FetchMs: 920})
	d.health.Observe("conversations", convObservation{CycleMs: 1000, FetchMs: 960})

	s, ok := findConv(d.ProviderConvergence(), "conversations")
	if !ok || !s.Flagged || !hasReason(s, "over_budget") {
		t.Fatalf("daemon should surface the slow provider via ProviderConvergence(), got %+v", d.ProviderConvergence())
	}
}

// ─── Episode identity: the counter fix ────────────────────────────────────────

// TestConvergenceTracker_PersistentAnomalyRaisesExactlyOnce is the test for the
// headline requirement: one persistent condition is ONE anomaly, no matter how
// long it lasts.
//
// Before this change, 500 degraded cycles at ReWarnEvery=20 emitted ~25 lines
// under the identical "reconcile: provider anomaly" message, and the operator's
// counter tallied every one — which is how a single never-clearing provider
// drove that counter from 142 to 236 in a day.
func TestConvergenceTracker_PersistentAnomalyRaisesExactlyOnce(t *testing.T) {
	read := captureLogs(t)
	tr := newConvergenceTracker(ConvergenceConfig{
		DegradedCycles: 3,
		ReWarnEvery:    20,
		ClearCycles:    2,
	})

	for i := 0; i < 500; i++ {
		tr.Observe("lms-model-state", convObservation{CycleMs: 5, Degraded: true})
	}

	lines := read()
	if got := countMsg(lines, msgAnomalyRaised); got != 1 {
		t.Fatalf("raise lines = %d, want exactly 1 for one continuous condition", got)
	}
	if got := countMsg(lines, msgAnomalyCleared); got != 0 {
		t.Fatalf("cleared lines = %d, want 0 while the condition is still open", got)
	}

	// The condition must stay LOUD. Suppression here is deduplication of a
	// report, never silencing of a condition.
	persist := linesWithMsg(lines, msgAnomalyPersist)
	if len(persist) < 20 {
		t.Fatalf("persist heartbeats = %d, want >= 20 (a standing anomaly must stay visible)", len(persist))
	}
	for _, l := range persist {
		if l.Level != "WARN" {
			t.Errorf("persist heartbeat at level %s, want WARN", l.Level)
		}
		if ep, _ := l.Attrs["episode"].(float64); int(ep) != 1 {
			t.Errorf("persist heartbeat episode = %v, want 1", l.Attrs["episode"])
		}
	}

	// Recovery closes episode 1 exactly once.
	for i := 0; i < 2; i++ {
		tr.Observe("lms-model-state", convObservation{CycleMs: 5})
	}
	lines = read()
	cleared := linesWithMsg(lines, msgAnomalyCleared)
	if len(cleared) != 1 {
		t.Fatalf("cleared lines = %d, want exactly 1", len(cleared))
	}
	if cleared[0].Level != "INFO" {
		t.Errorf("cleared at level %s, want INFO", cleared[0].Level)
	}
	if ep, _ := cleared[0].Attrs["episode"].(float64); int(ep) != 1 {
		t.Errorf("cleared episode = %v, want 1", cleared[0].Attrs["episode"])
	}

	// A genuinely NEW episode is still counted.
	for i := 0; i < 3; i++ {
		tr.Observe("lms-model-state", convObservation{CycleMs: 5, Degraded: true})
	}
	lines = read()
	raises := linesWithMsg(lines, msgAnomalyRaised)
	if len(raises) != 2 {
		t.Fatalf("raise lines after recurrence = %d, want 2", len(raises))
	}
	if ep, _ := raises[1].Attrs["episode"].(float64); int(ep) != 2 {
		t.Errorf("second raise episode = %v, want 2", raises[1].Attrs["episode"])
	}
}

// TestConvergenceTracker_PersistMessageIsNotCountedAsARaise pins the
// cross-surface contract with the operator's out-of-repo vitals probe, which
// counts a line as a raise iff it contains "provider anomaly" and does not
// contain "provider anomaly cleared".
//
// This is the single assertion that stops a future rename from silently
// regressing the operator's instrument. The coupling is a substring match
// against a Python grep in another tree; the lifecycle attribute added to all
// three messages is the durable joint, and this test guards the interim.
func TestConvergenceTracker_PersistMessageIsNotCountedAsARaise(t *testing.T) {
	const raiseSubstr = "provider anomaly" // what the vitals hook greps for

	if !strings.Contains(msgAnomalyRaised, raiseSubstr) {
		t.Fatalf("raise message %q must contain %q or the hook counts nothing at all",
			msgAnomalyRaised, raiseSubstr)
	}
	if strings.Contains(msgAnomalyPersist, raiseSubstr) {
		t.Fatalf("persist message %q contains %q — the vitals hook would count "+
			"every heartbeat as a new anomaly (the bug this fixes)", msgAnomalyPersist, raiseSubstr)
	}
	if !strings.Contains(msgAnomalyCleared, raiseSubstr+" cleared") {
		t.Fatalf("cleared message %q must contain %q so the hook's exclusion still matches",
			msgAnomalyCleared, raiseSubstr+" cleared")
	}
}

// TestConvergenceTracker_VitalsCounterSeesOneNetAnomaly runs the operator's
// actual counting rule over the captured log for a long-lived condition and
// asserts the number it produces. This is the end-to-end expression of the
// requirement: 1, not 25.
func TestConvergenceTracker_VitalsCounterSeesOneNetAnomaly(t *testing.T) {
	read := captureLogs(t)
	tr := newConvergenceTracker(ConvergenceConfig{DegradedCycles: 3, ReWarnEvery: 20})
	for i := 0; i < 500; i++ {
		tr.Observe("lms-model-state", convObservation{CycleMs: 5, Degraded: true})
	}

	// Replica of kernel-vitals-probe.py's rule.
	raised, cleared := 0, 0
	for _, l := range read() {
		switch {
		case strings.Contains(l.Msg, "provider anomaly cleared"):
			cleared++
		case strings.Contains(l.Msg, "provider anomaly"):
			raised++
		}
	}
	if net := raised - cleared; net != 1 {
		t.Fatalf("vitals counter net = %d (raised=%d cleared=%d), want 1 for one continuous condition",
			net, raised, cleared)
	}
}

// TestConvergenceTracker_ReasonSetChangeOpensNewEpisode covers the case where a
// flagged provider's anomaly changes shape. The reason set a consumer harvests
// comes from the raise line, so without re-raising it would freeze at whatever
// the condition looked like when it first appeared and never learn the new axis.
// Closing the old episode before opening the new one keeps a raised-minus-
// cleared counter correct.
func TestConvergenceTracker_ReasonSetChangeOpensNewEpisode(t *testing.T) {
	read := captureLogs(t)
	tr := newConvergenceTracker(ConvergenceConfig{
		CycleBudget:      100 * time.Millisecond,
		OverBudgetCycles: 3,
		DegradedCycles:   3,
		ReWarnEvery:      1000, // no heartbeats within this test
	})

	// Open an episode on the degraded axis alone.
	for i := 0; i < 3; i++ {
		tr.Observe("p", convObservation{CycleMs: 5, Degraded: true})
	}
	// Now also cross the cost budget: reason set becomes over_budget+degraded.
	for i := 0; i < 3; i++ {
		tr.Observe("p", convObservation{CycleMs: 900, Degraded: true})
	}

	lines := read()
	raises := linesWithMsg(lines, msgAnomalyRaised)
	if len(raises) != 2 {
		t.Fatalf("raise lines = %d, want 2 (shape changed)", len(raises))
	}
	if got := countMsg(lines, msgAnomalyCleared); got != 1 {
		t.Fatalf("cleared lines = %d, want 1 so net-open stays at 1", got)
	}

	// The second raise must carry BOTH axes, which is the whole point.
	reasons, _ := raises[1].Attrs["reasons"].([]any)
	if len(reasons) != 2 {
		t.Fatalf("second raise reasons = %v, want both over_budget and degraded", raises[1].Attrs["reasons"])
	}

	s, _ := findConv(tr.Snapshot(), "p")
	if s.Episode != 2 || !s.Flagged {
		t.Fatalf("snapshot after shape change = %+v, want episode 2 and still flagged", s)
	}
}

// TestConvergenceTracker_ClearRequiresHysteresis covers the raise/clear
// asymmetry: raising took 3 consecutive bad cycles while clearing took a single
// good one, so a provider oscillating around its budget churned an episode pair
// every couple of ticks (~28/day for `conversations` in production).
func TestConvergenceTracker_ClearRequiresHysteresis(t *testing.T) {
	read := captureLogs(t)
	tr := newConvergenceTracker(ConvergenceConfig{
		CycleBudget:      100 * time.Millisecond,
		OverBudgetCycles: 3,
		ClearCycles:      3,
		ReWarnEvery:      1000,
	})

	for i := 0; i < 3; i++ {
		tr.Observe("straddler", convObservation{CycleMs: 500})
	}
	// Oscillate: one good cycle then bad again, six times over. A single good
	// cycle must NOT close the episode.
	for i := 0; i < 6; i++ {
		tr.Observe("straddler", convObservation{CycleMs: 50})
		tr.Observe("straddler", convObservation{CycleMs: 500})
	}

	lines := read()
	if got := countMsg(lines, msgAnomalyCleared); got != 0 {
		t.Fatalf("cleared lines = %d during oscillation, want 0 (hysteresis)", got)
	}
	if got := countMsg(lines, msgAnomalyRaised); got != 1 {
		t.Fatalf("raise lines = %d during oscillation, want 1", got)
	}

	// A sustained recovery does close it.
	for i := 0; i < 3; i++ {
		tr.Observe("straddler", convObservation{CycleMs: 50})
	}
	if got := countMsg(read(), msgAnomalyCleared); got != 1 {
		t.Fatalf("cleared lines after sustained recovery = %d, want 1", got)
	}
}

// TestConvergenceTracker_SnapshotCarriesEpisodeAndOpenSeconds proves the
// structural counter is populated, so a consumer never has to grep log text.
func TestConvergenceTracker_SnapshotCarriesEpisodeAndOpenSeconds(t *testing.T) {
	tr := newConvergenceTracker(ConvergenceConfig{DegradedCycles: 2, ClearCycles: 1})
	for i := 0; i < 2; i++ {
		tr.Observe("p", convObservation{CycleMs: 5, Degraded: true})
	}
	s, ok := findConv(tr.Snapshot(), "p")
	if !ok || s.Episode != 1 || !s.Flagged {
		t.Fatalf("snapshot = %+v, want episode 1 and flagged", s)
	}
	if s.OpenSeconds < 0 {
		t.Errorf("OpenSeconds = %d, want >= 0", s.OpenSeconds)
	}

	// Not flagged ⇒ no open duration reported.
	tr.Observe("p", convObservation{CycleMs: 5})
	s, _ = findConv(tr.Snapshot(), "p")
	if s.Flagged || s.OpenSeconds != 0 {
		t.Errorf("after recovery snapshot = %+v, want unflagged with OpenSeconds 0", s)
	}
	if s.Episode != 1 {
		t.Errorf("Episode = %d after recovery, want 1 retained (episodes are monotonic)", s.Episode)
	}
}

// TestConvergenceTracker_QuarantineIsItsOwnAnomalyAxis covers the provider
// shape that motivated this change: it reports Health()==Suspended, so the
// degraded axis never sees it, and before quarantine became an axis it
// contributed nothing to the anomaly counter at all — its ~1,100 daily WARNs
// were the only live indication it was broken.
func TestConvergenceTracker_QuarantineIsItsOwnAnomalyAxis(t *testing.T) {
	read := captureLogs(t)
	tr := newConvergenceTracker(ConvergenceConfig{DegradedCycles: 3, ReWarnEvery: 1000})

	// Not degraded (Suspended reads as not-degraded here), never over budget:
	// without the quarantine axis this provider is invisible.
	for i := 0; i < 5; i++ {
		tr.Observe("lms/remote", convObservation{CycleMs: 5})
	}
	if s, _ := findConv(tr.Snapshot(), "lms/remote"); s.Flagged {
		t.Fatalf("provider flagged before quarantine, got %+v", s)
	}

	// Daemon quarantines it: exactly one anomaly, and it stays open.
	for i := 0; i < 50; i++ {
		tr.Observe("lms/remote", convObservation{CycleMs: 5, Quarantined: true})
	}
	if got := countMsg(read(), msgAnomalyRaised); got != 1 {
		t.Fatalf("raise lines = %d, want exactly 1 for the quarantine episode", got)
	}
	s, _ := findConv(tr.Snapshot(), "lms/remote")
	if !s.Flagged || !hasReason(s, "quarantined") {
		t.Fatalf("snapshot = %+v, want flagged with the quarantined reason", s)
	}
}
