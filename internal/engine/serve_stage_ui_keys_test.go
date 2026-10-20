package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic keys. The Stage stub knows exactly these; the others are guesses.
const (
	skGK  = "GUESTMEMBERKEY0000000001" // guest member key of the vector room
	skGH  = "GUESTHOMEKEY000000000001" // guest home key
	skBAD = "UNKNOWNKEY00000000000x"
	skRID = "fdvshared" // a room id, not a key
)

// stageStub answers like the Python Stage for the keyed routes used here.
// Room keys open /r/ and /k/ only, home keys open /h/ only (a home key on /r/
// is a 404 that still counts as a known key, as in guard.key_known).
func stageStub(known ...string) *httptest.Server {
	k, hk := map[string]bool{}, map[string]bool{}
	for _, x := range known {
		if strings.HasPrefix(x, "GUESTHOME") {
			hk[x] = true
		} else {
			k[x] = true
		}
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) > 2 && (parts[1] == "r" || parts[1] == "k" || parts[1] == "h") {
			if (parts[1] == "h" && !hk[parts[2]]) || (parts[1] != "h" && !k[parts[2]]) {
				if strings.Contains(r.URL.Path, "/ws/") {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/monengine") {
				http.Error(w, "not found", http.StatusNotFound) // known key, missing file
				return
			}
			w.Header().Set("X-Stage", "py")
			fmt.Fprint(w, "ok:", r.URL.Path)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/pair" {
			fmt.Fprint(w, `{"code":"PC1","secret":"PS1"}`)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
}

func shadowStore(t *testing.T, keys map[string]string) *ShareLinkStore {
	t.Helper()
	st := NewShareLinkStoreWithLedger("")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	i := 0
	for tok, kind := range keys {
		i++
		_, err := st.Import(ShareLinkImport{Kind: kind, Surface: fmt.Sprintf("stage/test-%d", i), Actor: "chaz",
			Role: "guest", Scope: []string{"stage:view"}, IntegrityHash: sha256Hex(tok)}, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func shadowHandler(t *testing.T, mode string, up string, st *ShareLinkStore) (http.Handler, *stageGuard) {
	t.Helper()
	cfg := StageUIConfig{Upstream: up, PublicHost: guardTH, Keys: mode}
	if st != nil {
		cfg.shareLinks = func() *ShareLinkStore { return st }
	}
	h, g, err := newStageUIHandlerG(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h, g
}

func shadowDo(h http.Handler, method, target, ip string, ws bool, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = guardPeer
	req.Host = guardTH
	if ip != "" {
		req.Header.Set("Cf-Connecting-Ip", ip)
	}
	if ws {
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// captureSlog swaps the default logger for a buffer.
func captureSlog(t *testing.T) *lockedBuf {
	t.Helper()
	b := &lockedBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return b
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// vectorRow mirrors the vendored S1 JSON (testdata/stage-front-door-key-vectors.json).
type vectorRow struct {
	ID    string       `json:"id"`
	Steps []vectorStep `json:"steps"`
}
type vectorStep struct {
	Kind    string            `json:"kind"`
	Peer    string            `json:"peer"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Action  string            `json:"action"`
	Expect  struct {
		Status     int            `json:"status"`
		WSClose    int            `json:"ws_close"`
		GuardFails map[string]int `json:"guard_fails_len"`
	} `json:"expect"`
}

// TestStageUIKeys_Vectors drives K01, K05-K07 and R14-R16 through the listener
// in shadow mode, with a real probe against the stub Stage. Every step must get
// the vector's status (a websocket close 4404 is a bare 403 on the wire), the
// guard's miss tally must match, and the kernel must agree with the Stage on
// every keyed step (zero disagreements).
func TestStageUIKeys_Vectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/stage-front-door-key-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Rows []vectorRow `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Rows) != 7 {
		t.Fatalf("vendored rows: got %d want 7", len(doc.Rows))
	}
	sub := strings.NewReplacer("{BAD}", skBAD, "{RID}", skRID, "{GK}", skGK, "{GH}", skGH, "{PC}", "PC1", "{PS}", "PS1")
	for _, row := range doc.Rows {
		row := row
		t.Run(row.ID, func(t *testing.T) {
			up := stageStub(skGK, skGH)
			defer up.Close()
			st := shadowStore(t, map[string]string{skGK: ShareLinkKindShare, skGH: ShareLinkKindShare})
			h, g := shadowHandler(t, StageUIKeysShadow, up.URL, st)
			for i, s := range row.Steps {
				if s.Action != "" || s.Kind == "state" {
					if s.Kind == "state" {
						if n, ok := s.Expect.GuardFails["203.0.113.7"]; ok && len(g.fails["203.0.113.7"]) != n {
							t.Fatalf("step %d: guard fails %d want %d", i, len(g.fails["203.0.113.7"]), n)
						}
					}
					continue
				}
				method := s.Method
				if method == "" {
					method = "GET"
				}
				ip := ""
				if s.Peer == "TUN" {
					ip = "203.0.113.7"
				}
				var hdr []string
				for k, v := range s.Headers {
					hdr = append(hdr, k, v)
				}
				rec := shadowDo(h, method, sub.Replace(s.Path), ip, s.Kind == "ws", hdr...)
				want := s.Expect.Status
				if s.Kind == "ws" {
					want = 403 // close 4404 and 4403 are both a bare 403 on the wire
				}
				if want != 0 && rec.Code != want {
					t.Fatalf("step %d %s: status %d want %d", i, s.Path, rec.Code, want)
				}
				if n, ok := s.Expect.GuardFails["203.0.113.7"]; ok && len(g.fails["203.0.113.7"]) != n {
					t.Fatalf("step %d: guard fails %d want %d", i, len(g.fails["203.0.113.7"]), n)
				}
			}
			if st := g.shadow.stats(g.keysMode); st.Disagreements != 0 {
				t.Fatalf("%s: kernel disagreed with the Stage: %+v", row.ID, st)
			}
		})
	}
}

// Agreement is counted for known and unknown keys; a room id is not a key.
func TestStageUIKeys_AgreementCounters(t *testing.T) {
	up := stageStub(skGK, skGH)
	defer up.Close()
	st := shadowStore(t, map[string]string{skGK: ShareLinkKindShare, skGH: ShareLinkKindShare})
	h, g := shadowHandler(t, StageUIKeysShadow, up.URL, st)
	for _, p := range []string{"/r/" + skGK + "/dashboard/orb.html", "/h/" + skGH + "/", "/r/" + skGK + "/monengine"} {
		shadowDo(h, "GET", p, "203.0.113.7", false)
	}
	for _, p := range []string{"/r/" + skBAD + "/v/", "/r/" + skRID + "/dashboard/orb.html"} {
		shadowDo(h, "GET", p, "203.0.113.8", false)
	}
	shadowDo(h, "GET", "/pair/PC1?secret=x", "203.0.113.9", false)
	s := g.shadow.stats(g.keysMode)
	if s.AgreeKnown != 3 || s.AgreeUnknown != 2 || s.Disagreements != 0 || s.SkippedPair != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

// Disagreement in both directions is counted, logged once with class, both
// verdicts and an 8-char hash prefix, and never leaks the key or full hash.
func TestStageUIKeys_DisagreeLog(t *testing.T) {
	buf := captureSlog(t)
	up := stageStub(skGK) // Stage knows GK
	defer up.Close()
	// kernel knows only BAD (stale/extra link), not GK
	st := shadowStore(t, map[string]string{skBAD: ShareLinkKindShare})
	h, g := shadowHandler(t, StageUIKeysShadow, up.URL, st)
	shadowDo(h, "GET", "/r/"+skGK+"/dashboard/orb.html", "203.0.113.7", false)
	shadowDo(h, "GET", "/k/"+skBAD+"/x", "203.0.113.7", false)
	s := g.shadow.stats(g.keysMode)
	if s.DisagreeKernelMiss != 1 || s.DisagreeKernelHit != 1 {
		t.Fatalf("stats: %+v", s)
	}
	out := buf.String()
	if n := strings.Count(out, "stage-ui: key-disagree"); n != 2 {
		t.Fatalf("want 2 disagree lines, got %d:\n%s", n, out)
	}
	for _, want := range []string{"class=r", "class=k", "stage=known", "kernel=unknown", "stage=unknown", "kernel=known", "hash=" + sha256Hex(skGK)[:8], "hash=" + sha256Hex(skBAD)[:8]} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q:\n%s", want, out)
		}
	}
	for _, leak := range []string{skGK, skBAD, sha256Hex(skGK), sha256Hex(skBAD), sha256Hex(skGK)[:9]} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaks %q:\n%s", leak, out)
		}
	}
}

// Past the window cap lines are suppressed (counters still count), and the
// next logged line carries the suppressed number.
func TestStageUIKeys_DisagreeLogRateLimit(t *testing.T) {
	buf := captureSlog(t)
	clock := time.Unix(1_700_000_000, 0)
	k := newKeyShadow(func() *ShareLinkStore { return NewShareLinkStoreWithLedger("") })
	k.now = func() time.Time { return clock }
	for i := 0; i < 25; i++ {
		k.compare("r", fmt.Sprintf("seg%d", i), true) // Stage knows, kernel empty
	}
	if n := strings.Count(buf.String(), "stage-ui: key-disagree"); n != keyDisagreeLogMax {
		t.Fatalf("logged %d want %d", n, keyDisagreeLogMax)
	}
	if got := k.disagreeKernelMiss.Load(); got != 25 {
		t.Fatalf("counted %d want 25", got)
	}
	clock = clock.Add(2 * time.Minute)
	k.compare("r", "later", true)
	if !strings.Contains(buf.String(), "suppressed=15") {
		t.Fatalf("next line should carry suppressed=15:\n%s", buf.String())
	}
}

// Device-kind links are cookies, not path keys: a path segment that only
// matches one is unknown to the kernel.
func TestStageUIKeys_DeviceKindIsNotAPathKey(t *testing.T) {
	st := shadowStore(t, map[string]string{skGK: ShareLinkKindDevice})
	k := newKeyShadow(func() *ShareLinkStore { return st })
	if known, ok := k.kernelKnows(skGK); !ok || known {
		t.Fatalf("device link counted as a path key: known=%v ok=%v", known, ok)
	}
}

// Shadow mode never changes a response: same status, headers and body as off,
// and as a listener with no store at all, across hits, misses, websockets and
// a disagreeing store.
func TestStageUIKeys_ShadowNeverChangesAResponse(t *testing.T) {
	up := stageStub(skGK, skGH)
	defer up.Close()
	agree := shadowStore(t, map[string]string{skGK: ShareLinkKindShare, skGH: ShareLinkKindShare})
	wrong := shadowStore(t, map[string]string{skBAD: ShareLinkKindShare})
	reqs := []struct {
		m, p string
		ws   bool
	}{
		{"GET", "/r/" + skGK + "/dashboard/orb.html", false},
		{"GET", "/r/" + skGK + "/monengine", false},
		{"GET", "/h/" + skGH + "/", false},
		{"GET", "/r/" + skBAD + "/v/", false},
		{"GET", "/r/" + skRID + "/dashboard/orb.html", false},
		{"GET", "/r/" + skBAD + "/ws/pad", true},
		{"GET", "/r/" + skGK + "/ws/pad", true},
		{"POST", "/pair", false},
		{"GET", "/pair/PC1?secret=x", false},
		{"GET", "/healthz", false},
		{"GET", "/local/people", false}, // refused by the guard on the tunnel host
	}
	fingerprint := func(h http.Handler) []string {
		var out []string
		for _, r := range reqs {
			rec := shadowDo(h, r.m, r.p, "203.0.113.7", r.ws)
			var hk []string
			for k, v := range rec.Header() {
				if k == "Date" { // wall clock, differs across runs a second apart
					continue
				}
				hk = append(hk, k+"="+strings.Join(v, ","))
			}
			out = append(out, fmt.Sprintf("%d|%d|%s|%v", rec.Code, len(hk), rec.Body.String(), sortedStrings(hk)))
		}
		return out
	}
	hOff, gOff := shadowHandler(t, StageUIKeysOff, up.URL, agree)
	base := fingerprint(hOff)
	if gOff.shadow != nil {
		t.Fatal("off mode built a shadow")
	}
	for name, st := range map[string]*ShareLinkStore{"agree": agree, "disagree": wrong, "nostore": nil} {
		for _, mode := range []string{StageUIKeysShadow, "bogus"} {
			h, _ := shadowHandler(t, mode, up.URL, st)
			got := fingerprint(h)
			for i := range base {
				if got[i] != base[i] {
					t.Fatalf("%s/%s req %d %s: response changed\n off: %s\n got: %s", name, mode, i, reqs[i].p, base[i], got[i])
				}
			}
		}
	}
}

func sortedStrings(s []string) []string {
	for i := range s {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
	return s
}

// The lockout is untouched by shadow: unknown-key guesses still count and
// lock the same way, with the probe running once per response.
func TestStageUIKeys_LockoutUnchanged(t *testing.T) {
	up := stageStub(skGK)
	defer up.Close()
	st := shadowStore(t, map[string]string{skGK: ShareLinkKindShare})
	for _, mode := range []string{StageUIKeysOff, StageUIKeysShadow} {
		h, g := shadowHandler(t, mode, up.URL, st)
		for i := 0; i < stageGuardLimit+1; i++ {
			shadowDo(h, "GET", fmt.Sprintf("/r/GUESS%019d/x", i), "203.0.113.50", false)
		}
		if !g.isLocked("203.0.113.50") {
			t.Fatalf("mode %s: expected lockout after %d guesses", mode, stageGuardLimit+1)
		}
		if rec := shadowDo(h, "GET", "/r/"+skGK+"/x", "203.0.113.50", false); rec.Code != 429 {
			t.Fatalf("mode %s: locked ip got %d", mode, rec.Code)
		}
	}
}

func TestStageUIKeys_ModeNormalization(t *testing.T) {
	for in, want := range map[string]struct {
		mode string
		ok   bool
	}{
		"":         {StageUIKeysOff, true},
		"off":      {StageUIKeysOff, true},
		"Shadow":   {StageUIKeysShadow, true},
		"enforce":  {StageUIKeysEnforce, true},
		" ENFORCE": {StageUIKeysEnforce, true},
		"on":       {StageUIKeysOff, false},
	} {
		m, ok := normalizeStageUIKeysMode(in)
		if m != want.mode || ok != want.ok {
			t.Errorf("%q: got %s %v want %+v", in, m, ok, want)
		}
	}
}

func TestStageUIKeys_ConfigKey(t *testing.T) {
	cfg := &Config{}
	applyStageUISection(cfg, &stageUISection{Keys: "shadow"})
	if cfg.StageUI.Keys != "shadow" {
		t.Fatalf("keys not applied: %q", cfg.StageUI.Keys)
	}
}

var _ = context.Background
