// serve_stage_ui_keys.go — the Stage UI listener's key shadow (P3b slice S4).
//
// The Python Stage stays authoritative for every key. In "shadow" mode the
// listener also resolves the key segment of each keyed request (/r/ /k/ /h/)
// against the share-link store (serve_share_links.go) and compares the kernel's
// verdict with the Stage's own answer. Nothing about the response changes: the
// comparison runs in the proxy's ModifyResponse, after the upstream answered,
// and only reads. Disagreements are logged and counted so the cutover to
// "enforce" (S5) can be gated on 24 h of zero disagreements (plan unknown U7).
//
// kernel.yaml (stage_ui.keys):
//
//	off     (default) no resolution, no counters
//	shadow  resolve and compare; never alters a response
//	enforce (S5) the listener answers the unknown-key cases itself and never
//	        sends them upstream: a key the share-link store does not hold
//	        (unknown, revoked, removed, rekeyed, or a room id) gets the Stage's
//	        own 404 "not found" (a websocket gets the bare 403 the Stage's
//	        pre-accept close 4404 becomes on the wire), and a home key used on
//	        /r/ or /k/ gets the same 404. A key the store holds is proxied as
//	        before and the Stage still decides everything else. The shadow
//	        comparison keeps running for what is proxied.
//
// What an enforced miss does to the lockout (R14): an unknown key counts, a
// home key on a room route does not (the Stage's key_known accepts a home key
// whatever the prefix), a SELF caller never counts. See keyRefusal.
//
// Verdicts. A key is "known" when it opens something.
//   - Stage verdict: a keyed request the upstream did not answer with a
//     candidate-miss (404, or a failed websocket upgrade) was accepted, so the
//     key is known. A candidate-miss is settled by the existing probeKeyKnown
//     (the same call the lockout uses, run once and shared).
//   - Kernel verdict: the segment hashes to a live share-link of kind "share".
//
// /pair/ codes are not share links until S8/S9, so they are not compared; they
// are counted as skipped.
//
// Log line (slog, level WARN, rate limited to 10 per minute; the next line
// that is logged carries the number suppressed before it):
//
//	stage-ui: key-disagree class=<r|k|h> stage=<known|unknown> kernel=<known|unknown> hash=<8 hex chars> suppressed=<n>
//
// The key itself and the full hash are never logged.
package engine

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Stage UI key modes.
const (
	StageUIKeysOff     = "off"
	StageUIKeysShadow  = "shadow"
	StageUIKeysEnforce = "enforce"
)

const (
	keyDisagreeLogMax    = 10
	keyDisagreeLogWindow = time.Minute
)

// normalizeStageUIKeysMode maps the config value to the effective mode.
// ok=false means the value was unrecognised (effective mode is off).
func normalizeStageUIKeysMode(v string) (mode string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", StageUIKeysOff:
		return StageUIKeysOff, true
	case StageUIKeysShadow:
		return StageUIKeysShadow, true
	case StageUIKeysEnforce:
		return StageUIKeysEnforce, true
	}
	return StageUIKeysOff, false
}

// keyShadow holds the shadow counters. Safe for concurrent use.
type keyShadow struct {
	store func() *ShareLinkStore
	now   func() time.Time

	agreeKnown         atomic.Int64
	agreeUnknown       atomic.Int64
	disagreeKernelMiss atomic.Int64 // Stage knows the key, kernel does not
	disagreeKernelHit  atomic.Int64 // kernel knows the key, Stage does not
	skippedPair        atomic.Int64
	skippedNoStore     atomic.Int64

	mu         sync.Mutex
	winStart   time.Time
	winLogged  int
	suppressed int
}

func newKeyShadow(store func() *ShareLinkStore) *keyShadow {
	return &keyShadow{store: store, now: time.Now}
}

// KeyShadowStats is the counter snapshot served at GET /v1/stage/keys/shadow.
type KeyShadowStats struct {
	Mode               string `json:"mode"`
	AgreeKnown         int64  `json:"agree_known"`
	AgreeUnknown       int64  `json:"agree_unknown"`
	DisagreeKernelMiss int64  `json:"disagree_stage_known_kernel_unknown"`
	DisagreeKernelHit  int64  `json:"disagree_stage_unknown_kernel_known"`
	Agreements         int64  `json:"agreements"`
	Disagreements      int64  `json:"disagreements"`
	SkippedPair        int64  `json:"skipped_pair"`
	SkippedNoStore     int64  `json:"skipped_no_store"`
}

func (k *keyShadow) stats(mode string) KeyShadowStats {
	s := KeyShadowStats{
		Mode:               mode,
		AgreeKnown:         k.agreeKnown.Load(),
		AgreeUnknown:       k.agreeUnknown.Load(),
		DisagreeKernelMiss: k.disagreeKernelMiss.Load(),
		DisagreeKernelHit:  k.disagreeKernelHit.Load(),
		SkippedPair:        k.skippedPair.Load(),
		SkippedNoStore:     k.skippedNoStore.Load(),
	}
	s.Agreements = s.AgreeKnown + s.AgreeUnknown
	s.Disagreements = s.DisagreeKernelMiss + s.DisagreeKernelHit
	return s
}

// kernelKnows resolves the key segment against the store. known=false with
// ok=false means there is no store to ask.
func (k *keyShadow) kernelKnows(seg string) (known, ok bool) {
	st := k.store()
	if st == nil {
		return false, false
	}
	l, found := st.Resolve(seg)
	return found && l.Kind == ShareLinkKindShare, true
}

// compare records one comparison. class is the route prefix letter.
func (k *keyShadow) compare(class, seg string, stageKnown bool) {
	kernel, ok := k.kernelKnows(seg)
	if !ok {
		k.skippedNoStore.Add(1)
		return
	}
	switch {
	case kernel == stageKnown && kernel:
		k.agreeKnown.Add(1)
	case kernel == stageKnown:
		k.agreeUnknown.Add(1)
	default:
		if stageKnown {
			k.disagreeKernelMiss.Add(1)
		} else {
			k.disagreeKernelHit.Add(1)
		}
		k.logDisagree(class, seg, stageKnown, kernel)
	}
}

func verdictWord(known bool) string {
	if known {
		return "known"
	}
	return "unknown"
}

func (k *keyShadow) logDisagree(class, seg string, stageKnown, kernel bool) {
	now := k.now()
	k.mu.Lock()
	if k.winStart.IsZero() || now.Sub(k.winStart) >= keyDisagreeLogWindow {
		k.winStart, k.winLogged = now, 0
	}
	if k.winLogged >= keyDisagreeLogMax {
		k.suppressed++
		k.mu.Unlock()
		return
	}
	k.winLogged++
	sup := k.suppressed
	k.suppressed = 0
	k.mu.Unlock()
	h := sha256Hex(seg)
	slog.Warn("stage-ui: key-disagree",
		"class", class, "stage", verdictWord(stageKnown), "kernel", verdictWord(kernel),
		"hash", h[:8], "suppressed", sup)
}

// keyedClass returns the route class ("r", "k", "h") and key segment of a
// keyed path. pair=true for /pair/ (not compared). ok=false if not keyed.
func keyedClass(p string) (class, seg string, pair, ok bool) {
	for _, pre := range []string{"/r/", "/k/", "/h/", "/pair/"} {
		if strings.HasPrefix(p, pre) {
			parts := strings.Split(p, "/")
			if len(parts) > 2 {
				seg = parts[2]
			}
			if pre == "/pair/" {
				return "pair", seg, true, true
			}
			return pre[1:2], seg, false, true
		}
	}
	return "", "", false, false
}

// observe runs the shadow comparison for one proxied response. It only reads.
// stageKnown is computed lazily so the probe runs at most once per request.
func (k *keyShadow) observe(ctx context.Context, info *stageReqInfo, resp *http.Response, stageKnown func() bool) {
	class, seg, pair, ok := keyedClass(info.path)
	if !ok {
		return
	}
	if pair {
		k.skippedPair.Add(1)
		return
	}
	if seg == "" {
		return
	}
	k.compare(class, seg, stageKnown())
}

// keyShadowStatsHandler serves GET /v1/stage/keys/shadow. The path sits under
// the share-link prefix, so the grant gate requires stage:keys on it already.
func (s *Server) handleKeyShadowStats(w http.ResponseWriter, r *http.Request) {
	s.stageUIMu.Lock()
	g := s.stageUIGuard
	s.stageUIMu.Unlock()
	if g == nil || g.shadow == nil {
		writeJSONResp(w, http.StatusOK, KeyShadowStats{Mode: StageUIKeysOff})
		return
	}
	writeJSONResp(w, http.StatusOK, g.shadow.stats(g.keysMode))
}

// stageHomeSurfacePrefix is the surface namespace of home keys (keys_sync.py:
// stage/home/<pid>). Room and member keys live under stage/room/.
const stageHomeSurfacePrefix = "stage/home/"

// keyRefusal is keys=enforce. For a keyed /r/ /k/ /h/ request it asks the
// share-link store about the key segment and reports whether the listener
// answers the request itself (refuse) and whether that answer is a guess that
// counts toward the lockout (count).
//
//   - segment not held as a live "share" link: unknown, revoked, removed,
//     rekeyed or a room id. refuse and count (R14: an unknown key counts).
//   - a home key (surface stage/home/...) on /r/ or /k/: the Stage answers 404
//     and its key_known says the key is real, so refuse and do not count (K07).
//   - anything else: proxy; the Stage decides (a held key opening a missing
//     file, a room key on /h/, owner-only gates, ...).
//
// Nothing is decided without a store or for /pair/ (a code is not a share link
// until S8/S9) or an empty segment: those go upstream unchanged.
func (k *keyShadow) keyRefusal(path string) (refuse, count bool) {
	class, seg, pair, ok := keyedClass(path)
	if !ok || pair || seg == "" {
		return false, false
	}
	st := k.store()
	if st == nil {
		return false, false
	}
	l, found := st.Resolve(seg)
	if !found || l.Kind != ShareLinkKindShare {
		return true, true
	}
	if class != "h" && strings.HasPrefix(l.Surface, stageHomeSurfacePrefix) {
		return true, false
	}
	return false, false
}

// writeStageKeyRefusal writes the Stage's own answer for a refused key:
// starlette PlainTextResponse("not found", 404), or for a websocket upgrade
// the bare 403 that the pre-accept close 4404 is on the wire.
func writeStageKeyRefusal(w http.ResponseWriter, ws bool) {
	if ws {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	const body = "not found"
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", "9")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(body))
}
