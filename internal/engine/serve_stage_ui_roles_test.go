package engine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Tests for stage_ui.keys=enforce, roles half (P3b S6). Front-door vectors
// O01, O02, O04, O06 and O09, and probe rows 5a and 5b. The upstream stub
// records every client request, so each case says whether the Stage was
// reached. Synthetic keys only.

const (
	rkOwner  = "OWNERROOMKEY0000000001" // owner's room key
	rkGuest  = "GUESTROOMKEY0000000001" // guest member key
	rkCollab = "COLLABROOMKEY000000001" // collaborator member key
	rkOwnerH = "OWNERHOMEKEY0000000001" // owner's home key
	rkGuestH = "GUESTHOMEKEY0000000002" // guest home key
	rkCollH  = "COLLABHOMEKEY000000001" // collaborator home key
)

type roleStage struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

// newRoleStage answers 200 "stage:<method> <path>" to every client request.
func newRoleStage() *roleStage {
	rs := &roleStage{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host == guardTH {
			rs.mu.Lock()
			rs.seen = append(rs.seen, r.Method+" "+r.URL.Path)
			rs.mu.Unlock()
		}
		w.Header().Set("X-Stage", "py")
		fmt.Fprintf(w, "stage:%s %s", r.Method, r.URL.Path)
	}))
	return rs
}

func (r *roleStage) hits() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func (r *roleStage) reset() {
	r.mu.Lock()
	r.seen = nil
	r.mu.Unlock()
}

func rolesStore(t *testing.T) *ShareLinkStore {
	t.Helper()
	st := NewShareLinkStoreWithLedger("")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	for _, k := range []struct{ tok, surface, actor, role string }{
		{rkOwner, "stage/room/r1/owner", "chaz", "owner"},
		{rkGuest, "stage/room/r1/member/vroku", "vroku", "guest"},
		{rkCollab, "stage/room/r1/member/colin", "colin", "collaborator"},
		{rkOwnerH, "stage/home/chaz", "chaz", "owner"},
		{rkGuestH, "stage/home/vroku", "vroku", "guest"},
		{rkCollH, "stage/home/colin", "colin", "collaborator"},
	} {
		if _, err := st.Import(ShareLinkImport{Kind: ShareLinkKindShare, Surface: k.surface, Actor: k.actor,
			Role: k.role, Scope: []string{"stage:view"}, IntegrityHash: sha256Hex(k.tok)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func rolesFixture(t *testing.T, mode string) (http.Handler, *stageGuard, *roleStage) {
	t.Helper()
	up := newRoleStage()
	t.Cleanup(up.Close)
	h, g := shadowHandler(t, mode, up.URL, rolesStore(t))
	return h, g, up
}

// The owner_key_only routes of stage.py:1915-1935 (GET/POST are what O01 names).
var roleOwnerOnlyPaths = []string{
	"/monengine", "/monengine/", "/monengine/packs/a.js",
	"/voicetest/voices", "/voicetest/synth",
	"/deck", "/deck/notes.md",
	"/canvases", "/canvas/c1",
	"/surface",
	"/oracles", "/oracles/x/y",
	"/oscine", "/oscine/bridge", "/oscine/layouts/a.json",
	"/studio", "/studio/", "/studio/api/x",
	"/ear",
	"/device/request",
}

// O01. A guest or collaborator key on every owner-only route: 404 "not found",
// before the Stage. The owner's key reaches it. Same on /k/.
func TestStageUIRoles_O01_OwnerOnlyRoutes404(t *testing.T) {
	h, g, up := rolesFixture(t, StageUIKeysEnforce)
	for _, pre := range []string{"/r/", "/k/"} {
		for _, key := range []string{rkGuest, rkCollab} {
			for _, p := range roleOwnerOnlyPaths {
				for _, m := range []string{"GET", "POST"} {
					up.reset()
					rec := shadowDo(h, m, pre+key+p, "203.0.113.7", false)
					if rec.Code != 404 || rec.Body.String() != "not found" {
						t.Fatalf("%s %s%s...%s: got %d %q want 404 \"not found\"", m, pre, key[:5], p, rec.Code, rec.Body.String())
					}
					if up.hits() != 0 {
						t.Fatalf("%s %s%s...%s reached the Stage", m, pre, key[:5], p)
					}
				}
			}
		}
		for _, p := range roleOwnerOnlyPaths {
			up.reset()
			rec := shadowDo(h, "GET", pre+rkOwner+p, "203.0.113.7", false)
			if rec.Code != 200 || up.hits() != 1 {
				t.Fatalf("owner key on %s%s: got %d hits=%d, want the Stage to answer", pre, "OWNER", rec.Code, up.hits())
			}
		}
	}
	// A role refusal is the Stage's answer to a known key: it is not a guess.
	if n := len(g.fails["203.0.113.7"]); n != 0 {
		t.Fatalf("role refusals counted %d lockout misses, want 0", n)
	}
}

// O01, the content type and length of the refusal match starlette's PlainTextResponse.
func TestStageUIRoles_O01_RefusalHeaders(t *testing.T) {
	h, _, _ := rolesFixture(t, StageUIKeysEnforce)
	rec := shadowDo(h, "GET", "/r/"+rkGuest+"/deck", "203.0.113.7", false)
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("content-type %q", ct)
	}
	if cl := rec.Header().Get("Content-Length"); cl != "9" {
		t.Fatalf("content-length %q", cl)
	}
}

// O02. The two owner-only sockets: a non-owner key is closed (bare 403 on the
// wire), before the Stage.
func TestStageUIRoles_O02_OwnerOnlySockets(t *testing.T) {
	h, _, up := rolesFixture(t, StageUIKeysEnforce)
	for _, key := range []string{rkGuest, rkCollab} {
		for _, p := range []string{"/ear/ws", "/oscine/bridge"} {
			up.reset()
			rec := shadowDo(h, "GET", "/r/"+key+p, "203.0.113.7", true)
			if rec.Code != 403 || up.hits() != 0 {
				t.Fatalf("ws %s: got %d hits=%d want 403, 0", p, rec.Code, up.hits())
			}
		}
	}
	for _, p := range []string{"/ear/ws", "/oscine/bridge"} {
		up.reset()
		shadowDo(h, "GET", "/r/"+rkOwner+p, "203.0.113.7", true)
		if up.hits() != 1 {
			t.Fatalf("owner ws %s must reach the Stage", p)
		}
	}
	// The gate on the pad socket stays in Python (O15-O18): a guest still reaches it.
	up.reset()
	shadowDo(h, "GET", "/r/"+rkGuest+"/ws/pad", "203.0.113.7", true)
	if up.hits() != 1 {
		t.Fatal("/ws/pad is a Stage-side gate and must still be proxied for a guest")
	}
}

// O04. /collab: 403 json for a known non-owner (guest and collaborator), 404
// for an unknown key (S5), owner reaches the Stage.
func TestStageUIRoles_O04_Collab403(t *testing.T) {
	h, g, up := rolesFixture(t, StageUIKeysEnforce)
	for _, key := range []string{rkGuest, rkCollab} {
		for _, m := range []string{"GET", "POST"} {
			up.reset()
			rec := shadowDo(h, m, "/r/"+key+"/collab", "203.0.113.7", false)
			if rec.Code != 403 || up.hits() != 0 {
				t.Fatalf("%s collab: got %d hits=%d want 403, 0", m, rec.Code, up.hits())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type %q", ct)
			}
			if b := rec.Body.String(); b != `{"error":"owner only"}` {
				t.Fatalf("body %q", b)
			}
		}
	}
	up.reset()
	if rec := shadowDo(h, "GET", "/r/"+rkOwner+"/collab", "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
		t.Fatalf("owner collab: %d hits=%d", rec.Code, up.hits())
	}
	up.reset()
	if rec := shadowDo(h, "GET", "/r/"+skBAD+"/collab", "203.0.113.8", false); rec.Code != 404 || up.hits() != 0 {
		t.Fatalf("unknown key collab: %d hits=%d want 404, 0", rec.Code, up.hits())
	}
	if n := len(g.fails["203.0.113.7"]); n != 0 {
		t.Fatalf("collab refusals counted %d lockout misses", n)
	}
}

// O05 and O06. GET /cog is the Stage's (owner data stripped for a guest there);
// POST /cog by a non-owner is 403 "only Chaz can do that" at the kernel.
func TestStageUIRoles_O06_CogPost403(t *testing.T) {
	h, _, up := rolesFixture(t, StageUIKeysEnforce)
	for _, key := range []string{rkGuest, rkCollab} {
		up.reset()
		if rec := shadowDo(h, "GET", "/r/"+key+"/cog", "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
			t.Fatalf("GET /cog by a non-owner is the Stage's to answer: %d hits=%d", rec.Code, up.hits())
		}
		up.reset()
		rec := shadowDo(h, "POST", "/r/"+key+"/cog", "203.0.113.7", false)
		if rec.Code != 403 || up.hits() != 0 {
			t.Fatalf("POST /cog: got %d hits=%d want 403, 0", rec.Code, up.hits())
		}
		if b := rec.Body.String(); b != `{"error":"only Chaz can do that"}` {
			t.Fatalf("body %q", b)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type %q", ct)
		}
	}
	up.reset()
	if rec := shadowDo(h, "POST", "/r/"+rkOwner+"/cog", "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
		t.Fatalf("owner POST /cog: %d hits=%d", rec.Code, up.hits())
	}
}

// O09 and probe row 5a. A non-owner home key on the owner-only /api routes:
// 404 {"error":"unknown"}, before the Stage. GET sessions is quiet (O10) and
// still goes to the Stage, which answers {"sessions": []}.
func TestStageUIRoles_O09_HomeOwnerOnlyAPI(t *testing.T) {
	h, g, up := rolesFixture(t, StageUIKeysEnforce)
	for _, key := range []string{rkGuestH, rkCollH} {
		for _, what := range []string{"devices", "session", "continue", "app-rooms", "app-room", "boards"} {
			for _, m := range []string{"GET", "POST"} {
				up.reset()
				rec := shadowDo(h, m, "/h/"+key+"/api/"+what, "203.0.113.7", false)
				if rec.Code != 404 || rec.Body.String() != `{"error":"unknown"}` || up.hits() != 0 {
					t.Fatalf("%s api/%s: got %d %q hits=%d", m, what, rec.Code, rec.Body.String(), up.hits())
				}
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Fatalf("content-type %q", ct)
				}
			}
		}
		// O10: GET sessions is quiet and is the Stage's answer; POST sessions is refused.
		up.reset()
		if rec := shadowDo(h, "GET", "/h/"+key+"/api/sessions", "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
			t.Fatalf("GET api/sessions must reach the Stage: %d hits=%d", rec.Code, up.hits())
		}
		up.reset()
		if rec := shadowDo(h, "POST", "/h/"+key+"/api/sessions", "203.0.113.7", false); rec.Code != 404 || up.hits() != 0 {
			t.Fatalf("POST api/sessions: %d hits=%d want 404, 0", rec.Code, up.hits())
		}
		// Routes a guest may use are not touched.
		for _, what := range []string{"rooms", "desk", "stamp", "home"} {
			up.reset()
			if rec := shadowDo(h, "GET", "/h/"+key+"/api/"+what, "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
				t.Fatalf("api/%s must reach the Stage: %d hits=%d", what, rec.Code, up.hits())
			}
		}
	}
	for _, what := range []string{"devices", "session", "continue", "app-rooms", "app-room", "boards", "sessions"} {
		up.reset()
		if rec := shadowDo(h, "GET", "/h/"+rkOwnerH+"/api/"+what, "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
			t.Fatalf("owner home key on api/%s: %d hits=%d", what, rec.Code, up.hits())
		}
	}
	if n := len(g.fails["203.0.113.7"]); n != 0 {
		t.Fatalf("home refusals counted %d lockout misses", n)
	}
}

// What the table must not touch: near-miss paths, a room key on /h/, routes a
// guest may use, methods the Stage answers 405 itself.
func TestStageUIRoles_NearMissesAreProxied(t *testing.T) {
	h, _, up := rolesFixture(t, StageUIKeysEnforce)
	for _, p := range []string{
		"/r/" + rkGuest + "/deck/",          // starlette redirects the trailing slash; not the owner gate
		"/r/" + rkGuest + "/canvas/a/b",     // {cid} is one segment
		"/r/" + rkGuest + "/canvas/",        // empty {cid}
		"/r/" + rkGuest + "/monengine-x",    // not a prefix match
		"/r/" + rkGuest + "/deckx",          //
		"/r/" + rkGuest + "/device",         // O03: any key may ask its own status
		"/r/" + rkGuest + "/complete",       // O07
		"/r/" + rkGuest + "/hear",           // O08, the Stage's gate
		"/r/" + rkGuest + "/dashboard/x.js", // static
		"/r/" + rkGuest + "/ear/ws",         // plain GET on a socket-only path: the Stage's 404, not ours
		"/r/" + rkGuest + "//deck",          // a different path to starlette
		"/h/" + rkGuest + "/api/devices",    // a room key on /h/: the Stage decides
		"/h/" + rkGuestH + "/api/devices/x", // not the route
		"/h/" + rkGuestH + "/ws/pad",
	} {
		up.reset()
		if rec := shadowDo(h, "GET", p, "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
			t.Fatalf("%s must be proxied: %d hits=%d", strings.ReplaceAll(p, rkGuest, "GK"), rec.Code, up.hits())
		}
	}
	// A method the route does not list is the Stage's 405, not the owner gate.
	up.reset()
	if rec := shadowDo(h, "PATCH", "/r/"+rkGuest+"/collab", "203.0.113.7", false); up.hits() != 1 {
		t.Fatalf("PATCH collab must be proxied: %d", rec.Code)
	}
	up.reset()
	if rec := shadowDo(h, "PUT", "/r/"+rkGuest+"/cog", "203.0.113.7", false); up.hits() != 1 {
		t.Fatalf("PUT cog must be proxied: %d", rec.Code)
	}
	up.reset()
	if rec := shadowDo(h, "PATCH", "/h/"+rkGuestH+"/api/devices", "203.0.113.7", false); up.hits() != 1 {
		t.Fatalf("PATCH api/devices must be proxied: %d", rec.Code)
	}
}

// Off and shadow: byte for byte. Every request, owner-only or not, reaches the
// Stage and gets the Stage's answer.
func TestStageUIRoles_OffAndShadowUnchanged(t *testing.T) {
	for _, mode := range []string{StageUIKeysOff, StageUIKeysShadow} {
		h, _, up := rolesFixture(t, mode)
		n := 0
		for _, key := range []string{rkGuest, rkCollab} {
			for _, p := range roleOwnerOnlyPaths {
				up.reset()
				rec := shadowDo(h, "GET", "/r/"+key+p, "203.0.113.7", false)
				if rec.Code != 200 || up.hits() != 1 || rec.Header().Get("X-Stage") != "py" ||
					rec.Body.String() != "stage:GET /r/"+key+p {
					t.Fatalf("%s: %s must pass through: %d %q hits=%d", mode, p, rec.Code, rec.Body.String(), up.hits())
				}
				n++
			}
			for _, c := range [][2]string{{"POST", "/cog"}, {"GET", "/collab"}, {"POST", "/collab"}} {
				up.reset()
				if rec := shadowDo(h, c[0], "/r/"+key+c[1], "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
					t.Fatalf("%s: %s %s must pass through", mode, c[0], c[1])
				}
			}
		}
		for _, what := range []string{"devices", "session", "continue", "app-rooms", "app-room", "boards", "sessions"} {
			up.reset()
			if rec := shadowDo(h, "GET", "/h/"+rkGuestH+"/api/"+what, "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
				t.Fatalf("%s: api/%s must pass through", mode, what)
			}
		}
		for _, p := range []string{"/ear/ws", "/oscine/bridge"} {
			up.reset()
			shadowDo(h, "GET", "/r/"+rkGuest+p, "203.0.113.7", true)
			if up.hits() != 1 {
				t.Fatalf("%s: ws %s must pass through", mode, p)
			}
		}
		if n == 0 {
			t.Fatal("no paths exercised")
		}
	}
}

// A link with no store behind it (store nil) refuses nothing and proxies.
func TestStageUIRoles_NoStoreProxies(t *testing.T) {
	up := newRoleStage()
	defer up.Close()
	h, _ := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	if rec := shadowDo(h, "GET", "/r/"+rkGuest+"/deck", "", false); rec.Code != 200 || up.hits() != 1 {
		t.Fatalf("without a store the Stage decides: %d hits=%d", rec.Code, up.hits())
	}
}

// A revoked or unknown key is S5's answer (404 not found, counted) and never
// reaches the role table.
func TestStageUIRoles_RevokedKeyStaysS5(t *testing.T) {
	h, g, up := rolesFixture(t, StageUIKeysEnforce)
	rec := shadowDo(h, "POST", "/r/"+skBAD+"/cog", "203.0.113.9", false)
	if rec.Code != 404 || rec.Body.String() != "not found" || up.hits() != 0 {
		t.Fatalf("unknown key POST /cog: %d %q hits=%d", rec.Code, rec.Body.String(), up.hits())
	}
	if len(g.fails["203.0.113.9"]) != 1 {
		t.Fatal("an unknown key must still count (R14)")
	}
}

// The role comes from the link, not the surface name: a link whose role is
// owner on a member surface is the owner; a guest-role link on an owner
// surface is not.
func TestStageUIRoles_RoleIsTheLinkRole(t *testing.T) {
	st := NewShareLinkStoreWithLedger("")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	for tok, role := range map[string]string{"ROLEASOWNERKEY00000001": "owner", "ROLEASGUESTKEY0000001x": "guest"} {
		if _, err := st.Import(ShareLinkImport{Kind: ShareLinkKindShare, Surface: "stage/room/r2/owner/" + role, Actor: "x",
			Role: role, Scope: []string{"stage:view"}, IntegrityHash: sha256Hex(tok)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	up := newRoleStage()
	defer up.Close()
	h, _ := shadowHandler(t, StageUIKeysEnforce, up.URL, st)
	if rec := shadowDo(h, "GET", "/r/ROLEASGUESTKEY0000001x/deck", "203.0.113.7", false); rec.Code != 404 || up.hits() != 0 {
		t.Fatalf("guest-role link on an owner surface must be refused: %d hits=%d", rec.Code, up.hits())
	}
	if rec := shadowDo(h, "GET", "/r/ROLEASOWNERKEY00000001/deck", "203.0.113.7", false); rec.Code != 200 || up.hits() != 1 {
		t.Fatalf("owner-role link must pass: %d hits=%d", rec.Code, up.hits())
	}
}
