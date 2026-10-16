// serve_stage_ui_owned_test.go — owned route prefixes and upstream-hit
// counters for the Stage UI listener (P4-0b). See the header of
// serve_stage_ui.go for what the behaviour is and why.
package engine

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ownedUpstreamBody is what the stub Python Stage answers with.
const ownedUpstreamBody = "python-stage"

// ownedUpstream starts a stub upstream and returns it plus a race-safe hit
// counter.
func ownedUpstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(ownedUpstreamBody))
	}))
	t.Cleanup(up.Close)
	return up, &hits
}

// ownedHandler builds the listener handler against upURL.
func ownedHandler(t *testing.T, upURL string, c StageUIConfig) http.Handler {
	t.Helper()
	c.Upstream = upURL
	h, _, err := newStageUIHandlerG(c)
	if err != nil {
		t.Fatalf("newStageUIHandlerG: %v", err)
	}
	return h
}

// ownedRequest drives the handler in-process (no client-side path rewriting),
// as a loopback caller the guard accepts.
func ownedRequest(t *testing.T, h http.Handler, method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Host = "localhost"
	req.RemoteAddr = "127.0.0.1:5000"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ownedStatsBody is the JSON body of GET /v1/stage-ui/stats.
type ownedStatsBody struct {
	Upstream         map[string]int64 `json:"upstream"`
	OwnedFallthrough map[string]int64 `json:"owned_fallthrough"`
}

// ownedStatsOf reads the stats endpoint and decodes it.
func ownedStatsOf(t *testing.T, h http.Handler) ownedStatsBody {
	t.Helper()
	rec := ownedRequest(t, h, http.MethodGet, "/v1/stage-ui/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats: got %d body %q", rec.Code, rec.Body.String())
	}
	var s ownedStatsBody
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("stats decode: %v (body %q)", err, rec.Body.String())
	}
	return s
}

// syncLogBuf is a log sink safe to read while the listener's goroutines write.
type syncLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureStageLogs points slog at a buffer for the duration of the test.
func captureStageLogs(t *testing.T) *syncLogBuf {
	t.Helper()
	b := &syncLogBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(b, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return b
}

func TestStageUIOwnedNeverReachesUpstream(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})

	rec := ownedRequest(t, h, http.MethodGet, "/v1/rooms/abc", nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("owned path: got %d body %q, want 502", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}
}

func TestStageUIOwnedFallthroughLogged(t *testing.T) {
	logs := captureStageLogs(t)
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})
	front := httptest.NewServer(h)
	t.Cleanup(front.Close)

	req, err := http.NewRequest(http.MethodGet, front.URL+"/v1/rooms/abc?k=SECRET-TEST-VALUE", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", "sid=SECRET-TEST-VALUE")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", resp.StatusCode)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}

	out := logs.String()
	if !strings.Contains(out, "stage-ui: owned-fallthrough") {
		t.Errorf("log missing the owned-fallthrough line: %q", out)
	}
	if !strings.Contains(out, "/v1/rooms/abc") {
		t.Errorf("log missing the path: %q", out)
	}
	if strings.Contains(out, "SECRET-TEST-VALUE") {
		t.Errorf("log leaked request material (query or header): %q", out)
	}
}

func TestStageUIOwnedUnownedStillProxies(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})

	rec := ownedRequest(t, h, http.MethodGet, "/v1/other/x", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != ownedUpstreamBody {
		t.Fatalf("unowned path: got %d %q, want 200 %q", rec.Code, rec.Body.String(), ownedUpstreamBody)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestStageUIOwnedPrefixBoundary(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})

	// Not under the prefix: /v1/roomsx is a different group.
	rec := ownedRequest(t, h, http.MethodGet, "/v1/roomsx", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != ownedUpstreamBody {
		t.Fatalf("/v1/roomsx must proxy: got %d %q", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits after /v1/roomsx = %d, want 1", got)
	}

	// The same route, spelled around the prefix: both clean to /v1/rooms/a.
	for _, target := range []string{"//v1/rooms/a", "/v1/rooms/../rooms/a"} {
		rec := ownedRequest(t, h, http.MethodGet, target, nil)
		if rec.Code != http.StatusBadGateway {
			t.Errorf("%s: got %d body %q, want 502", target, rec.Code, rec.Body.String())
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1 (owned spellings must not proxy)", got)
	}
}

func TestStageUIOwnedStaticStillWins(t *testing.T) {
	up, hits := ownedUpstream(t)
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "app-icons"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app-icons", "a.png"), []byte("PNGDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := ownedHandler(t, up.URL, StageUIConfig{StaticDir: dir, OwnedPrefixes: []string{"/app-icons"}})

	rec := ownedRequest(t, h, http.MethodGet, "/app-icons/a.png", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "PNGDATA" {
		t.Fatalf("static file: got %d %q, want 200 PNGDATA", rec.Code, rec.Body.String())
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}
}

func TestStageUIOwnedCounterGroups(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})

	for i := 0; i < 2; i++ {
		ownedRequest(t, h, http.MethodGet, "/v1/other/x", nil)
	}
	ownedRequest(t, h, http.MethodGet, "/health", nil)
	for i := 0; i < 3; i++ {
		ownedRequest(t, h, http.MethodGet, "/v1/rooms/a", nil)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("upstream hits = %d, want 3", got)
	}

	s := ownedStatsOf(t, h)
	if len(s.Upstream) != 2 || s.Upstream["/v1/other"] != 2 || s.Upstream["/health"] != 1 {
		t.Errorf("upstream = %v, want map[/v1/other:2 /health:1]", s.Upstream)
	}
	if len(s.OwnedFallthrough) != 1 || s.OwnedFallthrough["/v1/rooms"] != 3 {
		t.Errorf("owned_fallthrough = %v, want map[/v1/rooms:3]", s.OwnedFallthrough)
	}
	if _, ok := s.Upstream["/v1/stage-ui"]; ok {
		t.Errorf("the stats request counted itself: %v", s.Upstream)
	}
}

func TestStageUIOwnedStatsMethodNotAllowed(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})

	rec := ownedRequest(t, h, http.MethodPost, "/v1/stage-ui/stats", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST stats: got %d, want 405", rec.Code)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("POST stats reached the upstream: %d hits", got)
	}
	s := ownedStatsOf(t, h)
	if len(s.Upstream) != 0 || len(s.OwnedFallthrough) != 0 {
		t.Fatalf("405 touched the counters: upstream=%v owned=%v", s.Upstream, s.OwnedFallthrough)
	}
}

func TestStageUIOwnedStatsRaceFree(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := ownedHandler(t, up.URL, StageUIConfig{OwnedPrefixes: []string{"/v1/rooms"}})
	front := httptest.NewServer(h)
	t.Cleanup(front.Close)
	client := front.Client()
	client.Timeout = 30 * time.Second

	const goroutines, each = 50, 20
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				resp, err := client.Get(front.URL + "/v1/other/x")
				if err != nil {
					t.Errorf("proxied request: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("proxied request: got %d, want 200", resp.StatusCode)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := hits.Load(); got != goroutines*each {
		t.Fatalf("upstream hits = %d, want %d", got, goroutines*each)
	}

	s := ownedStatsOf(t, h)
	var total int64
	for _, n := range s.Upstream {
		total += n
	}
	if total != goroutines*each {
		t.Fatalf("upstream total = %d, want %d (map %v)", total, goroutines*each, s.Upstream)
	}
	if len(s.OwnedFallthrough) != 0 {
		t.Fatalf("owned_fallthrough = %v, want empty", s.OwnedFallthrough)
	}
}

func TestStageUIOwnedConfigRejectsBadPrefix(t *testing.T) {
	for _, bad := range [][]string{{""}, {"v1/rooms"}, {"/v1/rooms", ""}} {
		if err := validateStageUIOwnedPrefixes(bad); err == nil {
			t.Errorf("validateStageUIOwnedPrefixes(%q) = nil, want error", bad)
		}
		up, _ := ownedUpstream(t)
		if _, _, err := newStageUIHandlerG(StageUIConfig{Upstream: up.URL, OwnedPrefixes: bad}); err == nil {
			t.Errorf("newStageUIHandlerG with owned_prefixes %q: want error", bad)
		}
	}
	if err := validateStageUIOwnedPrefixes([]string{"/v1/rooms", "/app-icons/"}); err != nil {
		t.Errorf("validateStageUIOwnedPrefixes(good) = %v, want nil", err)
	}
	if err := validateStageUIOwnedPrefixes(nil); err != nil {
		t.Errorf("validateStageUIOwnedPrefixes(nil) = %v, want nil", err)
	}
}

func TestStageUIOwnedConfigYAML(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, ".cog", "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	y := "stage_ui:\n  addr: 127.0.0.1:7873\n  owned_prefixes: [/v1/rooms, /v1/mods]\n"
	if err := os.WriteFile(filepath.Join(ws, ".cog", "config", "kernel.yaml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.StageUI.OwnedPrefixes; len(got) != 2 || got[0] != "/v1/rooms" || got[1] != "/v1/mods" {
		t.Fatalf("owned_prefixes not applied: %q", got)
	}
}
