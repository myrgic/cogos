// inference_pins.go — typed record of how a configured model pin resolved.
//
// Before this file, the only trace of "configured local model X not loaded,
// using Y" was a free-text note concatenated into the agent cycle's Reason
// string (surfaced as /v1/agent/status.last_reason). Operators had to grep a
// sentence to learn the kernel was silently running a different model than
// the one they pinned. This records the same resolution as a typed value so
// GET /v1/inference can show declared-vs-executed per call site.
//
// Process-local, in-memory, last-writer-wins per site. It is an observation
// record, not a control surface: nothing reads it to make routing decisions.
package engine

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// PinReasonDeclared means the executed model is the configured one.
const PinReasonDeclared = "declared"

// PinResolution is one call site's most recent declared→executed outcome.
type PinResolution struct {
	// Site identifies the resolving call site (e.g. "dispatch", "assess").
	Site string `json:"site"`
	// Requested is the configured/preferred model (may be empty when nothing
	// was pinned and the first advertised model was used).
	Requested string `json:"requested,omitempty"`
	// Resolved is the model actually used. Empty when resolution failed.
	Resolved string `json:"resolved,omitempty"`
	// Reason is "declared" or "fallback:<cause>" where cause is one of
	// not-loaded | route-unavailable | none-loaded | other.
	Reason string `json:"reason"`
	// Detail is the original human-readable resolver note, if any.
	Detail    string    `json:"detail,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	pinResolutionsMu sync.RWMutex
	pinResolutions   = map[string]PinResolution{}
)

// classifyPinNote maps a resolveDispatchLocalModel note to a typed reason.
func classifyPinNote(note string) string {
	n := strings.ToLower(strings.TrimSpace(note))
	switch {
	case n == "":
		return PinReasonDeclared
	case strings.Contains(n, "not loaded, using"):
		return "fallback:not-loaded"
	case strings.Contains(n, "no local models are loaded"):
		return "fallback:none-loaded"
	case strings.Contains(n, "route unavailable"):
		return "fallback:route-unavailable"
	default:
		return "fallback:other"
	}
}

// recordPinResolution stores the outcome of one model-pin resolution. note is
// the resolver's free-text diagnostic (classified into a typed Reason via
// classifyPinNote); use it for call sites whose note is EMPTY on the declared
// path and non-empty only when something fell back (resolveDispatchLocalModel
// and the explicit-provider branch — see recordPinResolutionTyped for sites
// that always emit a descriptive note regardless of outcome).
func recordPinResolution(site, requested, resolved, note string) {
	recordPinResolutionTyped(site, requested, resolved, classifyPinNote(note), note)
}

// recordPinResolutionTyped stores the outcome of one model-pin resolution
// with an EXPLICIT reason, for call sites that always build a descriptive
// note (for the Detail field) regardless of whether the resolution was
// declared or a fallback — classifyPinNote's note=="" heuristic can't
// distinguish those cases, so the caller classifies directly instead.
func recordPinResolutionTyped(site, requested, resolved, reason, note string) {
	if site == "" {
		return
	}
	if reason == "" {
		reason = PinReasonDeclared
	}
	rec := PinResolution{
		Site:      site,
		Requested: requested,
		Resolved:  resolved,
		Reason:    reason,
		Detail:    strings.TrimSpace(note),
		Timestamp: time.Now().UTC(),
	}
	pinResolutionsMu.Lock()
	pinResolutions[site] = rec
	pinResolutionsMu.Unlock()
}

// PinReasonOverridden means the caller's explicit model request overrode a
// configured default (not a failure fallback, but a declared-vs-executed
// divergence worth surfacing the same way).
const PinReasonOverridden = "fallback:overridden"

// snapshotPinResolutions returns every recorded site, ordered by site name.
func snapshotPinResolutions() []PinResolution {
	pinResolutionsMu.RLock()
	out := make([]PinResolution, 0, len(pinResolutions))
	for _, r := range pinResolutions {
		out = append(out, r)
	}
	pinResolutionsMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Site < out[j].Site })
	return out
}

// resetPinResolutionsForTest clears the table. Test-only.
func resetPinResolutionsForTest() {
	pinResolutionsMu.Lock()
	pinResolutions = map[string]PinResolution{}
	pinResolutionsMu.Unlock()
}
