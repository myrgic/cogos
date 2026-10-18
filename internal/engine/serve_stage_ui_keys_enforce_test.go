package engine

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// Tests for stage_ui.keys=enforce (P3b S5). The upstream stub records every
// request that arrives with the client's Host (the lockout's own probes use
// Host 127.0.0.1 and are not client requests), so a test can say which
// requests reached the Stage and which the kernel answered itself.

type recordingStage struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

func newRecordingStage(known ...string) *recordingStage {
	inner := stageStub(known...)
	rs := &recordingStage{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host == guardTH {
			rs.mu.Lock()
			rs.seen = append(rs.seen, r.Method+" "+r.URL.Path)
			rs.mu.Unlock()
		}
		// Forward to the plain stub.
		req, _ := http.NewRequest(r.Method, inner.URL+r.URL.RequestURI(), r.Body)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 4096)
		for {
			n, e := resp.Body.Read(buf)
			_, _ = w.Write(buf[:n])
			if e != nil {
				break
			}
		}
	}))
	return rs
}

func (r *recordingStage) hits() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func (r *recordingStage) reset() {
	r.mu.Lock()
	r.seen = nil
	r.mu.Unlock()
}

func enforceFixture(t *testing.T, mode string) (http.Handler, *stageGuard, *recordingStage, *ShareLinkStore) {
	t.Helper()
	up := newRecordingStage(skGK, skGH)
	t.Cleanup(up.Close)
	st := shadowStore(t, map[string]string{skGK: ShareLinkKindShare, skGH: ShareLinkKindShare})
	// Mark the home key by its surface, as keys_sync.py issues it.
	st = homeAwareStore(t, st)
	h, g := shadowHandler(t, mode, up.URL, st)
	return h, g, up, st
}

// homeAwareStore rebuilds a store where skGK is a room member key and skGH a
// home key, with the surfaces keys_sync.py uses.
func homeAwareStore(t *testing.T, _ *ShareLinkStore) *ShareLinkStore {
	t.Helper()
	st := NewShareLinkStoreWithLedger("")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	for tok, surface := range map[string]string{
		skGK: "stage/room/fdvshared/member/guest",
		skGH: "stage/home/guest",
	} {
		if _, err := st.Import(ShareLinkImport{Kind: ShareLinkKindShare, Surface: surface, Actor: "guest",
			Role: "guest", Scope: []string{"stage:view"}, IntegrityHash: sha256Hex(tok)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// The vendored vectors in enforce mode. Every step whose key the kernel does
// not hold must be answered by the kernel: same status as the vector, and the
// Stage never sees it. A step on a held key (or /pair) must reach the Stage.
func TestStageUIKeys_EnforceVectors(t *testing.T) {
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
	sub := strings.NewReplacer("{BAD}", skBAD, "{RID}", skRID, "{GK}", skGK, "{GH}", skGH, "{PC}", "PC1", "{PS}", "PS1")
	answeredByKernel := 0
	for _, row := range doc.Rows {
		row := row
		t.Run(row.ID, func(t *testing.T) {
			h, g, up, _ := enforceFixture(t, StageUIKeysEnforce)
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
				p := sub.Replace(s.Path)
				up.reset()
				rec := shadowDo(h, method, p, ip, s.Kind == "ws", hdr...)
				want := s.Expect.Status
				if s.Kind == "ws" {
					want = 403 // close 4404 and 4403 are both a bare 403 on the wire
				}
				if want != 0 && rec.Code != want {
					t.Fatalf("step %d %s: status %d want %d", i, s.Path, rec.Code, want)
				}
				_, seg, pair, _ := keyedClass(p)
				originRefused := s.Headers["origin"] != "" // the guard's own 4403, not the key gate
				heldKey := seg == skGK
				mustReach := pair || heldKey || !strings.HasPrefix(p, "/r/")
				switch {
				case originRefused:
					if len(up.hits()) != 0 {
						t.Fatalf("step %d %s: guard refusal reached the Stage: %v", i, p, up.hits())
					}
				case mustReach:
					if len(up.hits()) != 1 {
						t.Fatalf("step %d %s: a held key must reach the Stage, hits=%v", i, p, up.hits())
					}
				default:
					answeredByKernel++
					if hits := up.hits(); len(hits) != 0 {
						t.Fatalf("step %d %s: the kernel must answer this itself, Stage saw %v", i, p, hits)
					}
					if s.Kind != "ws" && rec.Body.String() != "not found" {
						t.Fatalf("step %d %s: body %q want %q", i, p, rec.Body.String(), "not found")
					}
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
	if answeredByKernel < 7 {
		t.Fatalf("only %d vector steps were answered by the kernel; the table is not exercising enforce", answeredByKernel)
	}
}

// Shadow on the same table still proxies every row to the Stage.
func TestStageUIKeys_ShadowStillProxiesEveryVectorRow(t *testing.T) {
	raw, _ := os.ReadFile("testdata/stage-front-door-key-vectors.json")
	var doc struct {
		Rows []vectorRow `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	sub := strings.NewReplacer("{BAD}", skBAD, "{RID}", skRID, "{GK}", skGK, "{GH}", skGH, "{PC}", "PC1", "{PS}", "PS1")
	for _, mode := range []string{StageUIKeysOff, StageUIKeysShadow} {
		h, _, up, _ := enforceFixture(t, mode)
		steps := 0
		for _, row := range doc.Rows {
			for _, s := range row.Steps {
				if s.Action != "" || s.Kind == "state" || s.Headers["origin"] != "" {
					continue
				}
				method := s.Method
				if method == "" {
					method = "GET"
				}
				ip := ""
				if s.Peer == "TUN" {
					ip = "203.0.113.20"
				}
				up.reset()
				shadowDo(h, method, sub.Replace(s.Path), ip, s.Kind == "ws")
				steps++
				if len(up.hits()) != 1 {
					t.Fatalf("%s %s %s: Stage hits %v want exactly 1", mode, row.ID, s.Path, up.hits())
				}
			}
		}
		if steps == 0 {
			t.Fatal("no steps ran")
		}
	}
}

// K10-K13 and K23: a key the Stage still honours but the store has revoked,
// removed or rekeyed is a kernel 404 on every route, upload/hear/media
// included, without reaching the Stage.
func TestStageUIKeys_EnforceRevokedAndRoutes(t *testing.T) {
	h, _, up, st := enforceFixture(t, StageUIKeysEnforce)
	routes := []string{"/dashboard/orb.html", "/v/", "/cog", "/upload", "/hear", "/media/x", "/device"}
	for _, rt := range routes {
		up.reset()
		if rec := shadowDo(h, "GET", "/r/"+skGK+rt, "203.0.113.7", false); rec.Code != 200 || len(up.hits()) != 1 {
			t.Fatalf("live key %s: code %d hits %v", rt, rec.Code, up.hits())
		}
	}
	// Revoke the member key in the store (the Stage still knows it).
	l, ok := st.Resolve(skGK)
	if !ok {
		t.Fatal("setup: key not resolvable")
	}
	if _, err := st.Revoke(l.LinkID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	for _, rt := range routes {
		for _, pre := range []string{"/r/", "/k/"} {
			up.reset()
			rec := shadowDo(h, "POST", pre+skGK+rt, "203.0.113.31", false)
			if rec.Code != 404 || rec.Body.String() != "not found" {
				t.Fatalf("revoked %s%s: %d %q", pre, rt, rec.Code, rec.Body.String())
			}
			if got := up.hits(); len(got) != 0 {
				t.Fatalf("revoked key reached the Stage: %v", got)
			}
		}
	}
	// websocket on a revoked key: the bare 403, no Stage.
	up.reset()
	if rec := shadowDo(h, "GET", "/r/"+skGK+"/ws/pad", "203.0.113.31", true); rec.Code != 403 || len(up.hits()) != 0 {
		t.Fatalf("revoked ws: %d hits %v", rec.Code, up.hits())
	}
}

// A miss still counts in enforce mode (R14): the lockout trips on the same
// 21st unknown key, and a home key on a room route does not count.
func TestStageUIKeys_EnforceMissCountsTowardLockout(t *testing.T) {
	h, g, up, _ := enforceFixture(t, StageUIKeysEnforce)
	ip := "203.0.113.60"
	for i := 0; i < stageGuardLimit; i++ {
		shadowDo(h, "GET", fmt.Sprintf("/r/GUESS%019d/x", i), ip, false)
	}
	if got := len(g.fails[ip]); got != stageGuardLimit {
		t.Fatalf("misses counted %d want %d", got, stageGuardLimit)
	}
	if g.isLocked(ip) {
		t.Fatal("locked at the limit; it locks on the next miss")
	}
	shadowDo(h, "GET", "/r/GUESSxxxxxxxxxxxxxxxxx/x", ip, false)
	if !g.isLocked(ip) {
		t.Fatal("no lockout after more than the limit of enforced misses")
	}
	if rec := shadowDo(h, "GET", "/r/"+skGK+"/dashboard/orb.html", ip, false); rec.Code != 429 {
		t.Fatalf("locked ip got %d want 429", rec.Code)
	}
	if hits := up.hits(); len(hits) != 0 {
		t.Fatalf("enforced misses reached the Stage: %v", hits)
	}
	// K07 does not count; a SELF caller never counts.
	other := "203.0.113.61"
	for i := 0; i < 30; i++ {
		if rec := shadowDo(h, "GET", "/r/"+skGH+"/dashboard/orb.html", other, false); rec.Code != 404 {
			t.Fatalf("home key on a room route: %d", rec.Code)
		}
	}
	if len(g.fails[other]) != 0 {
		t.Fatalf("a home key on a room route counted %d misses", len(g.fails[other]))
	}
	for i := 0; i < 30; i++ {
		shadowDo(h, "GET", fmt.Sprintf("/r/SELF%020d/x", i), "", false)
	}
	if g.isLocked("127.0.0.1") || len(g.fails["127.0.0.1"]) != 0 {
		t.Fatal("a SELF caller was counted")
	}
}

// Enforce without a store cannot decide anything: it runs as off and the
// Stage stays authoritative.
func TestStageUIKeys_EnforceWithoutStoreIsOff(t *testing.T) {
	up := newRecordingStage(skGK)
	defer up.Close()
	h, g := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	if g.shadow != nil || g.keysMode != "" {
		t.Fatalf("enforce without a store built state: mode=%q", g.keysMode)
	}
	if rec := shadowDo(h, "GET", "/r/"+skBAD+"/x", "203.0.113.7", false); rec.Code != 404 || len(up.hits()) != 1 {
		t.Fatalf("code %d hits %v", rec.Code, up.hits())
	}
}

// /pair/ and a held key are never answered by the kernel; the answer to an
// unknown key is byte-for-byte what the Stage sends (starlette PlainText).
func TestStageUIKeys_EnforceAnswerShape(t *testing.T) {
	h, _, up, _ := enforceFixture(t, StageUIKeysEnforce)
	rec := shadowDo(h, "GET", "/r/"+skBAD+"/dashboard/orb.html", "203.0.113.7", false)
	if rec.Code != 404 || rec.Body.String() != "not found" ||
		rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" || rec.Header().Get("Content-Length") != "9" {
		t.Fatalf("shape: %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}
	up.reset()
	shadowDo(h, "GET", "/pair/PC1?secret=x", "203.0.113.7", false)
	if len(up.hits()) != 1 {
		t.Fatalf("/pair/ must go upstream: %v", up.hits())
	}
}
