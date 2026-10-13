package engine

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// g0Server builds a grant-gated server with a fake Hermes driver.
func g0Server(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := newGrantAuthTestServer(t)
	srv.managedSessions = fakeHermesDriver(t, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

func g0Do(t *testing.T, ts *httptest.Server, token, method, path string, body any, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, ts.URL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func mintActorTestGrant(t *testing.T, srv *Server, surface, actor string, scopes ...string) string {
	t.Helper()
	g, err := srv.identityGrants.MintOrReuseActor(surface, actor, scopes, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return g.Token
}

func TestManagedSessionScope_InferenceOnlyGrantRefused(t *testing.T) {
	srv, ts := g0Server(t)
	tok := mintScopedTestGrant(t, srv, "infer-only", ScopeInference)
	if code := g0Do(t, ts, tok, "POST", "/v1/managed-sessions", map[string]any{}, nil); code != http.StatusForbidden {
		t.Fatalf("inference-only create = %d, want 403", code)
	}
	// Reads stay grant-exempt.
	if code := g0Do(t, ts, "", "GET", "/v1/managed-sessions", nil, nil); code != http.StatusOK {
		t.Fatalf("list = %d, want 200", code)
	}
}

func TestManagedSessionScope_MappingTable(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/v1/managed-sessions", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/prompt", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/cancel", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/permission", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/mode", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/model", ScopeManagedSessions},
		{"POST", "/v1/managed-sessions/x/resume", ScopeManagedSessions},
		{"DELETE", "/v1/managed-sessions/x", ScopeManagedSessions},
		{"GET", "/v1/managed-sessions", ""},
		{"GET", "/v1/managed-sessions/x/events", ""},
		// Documents the reserved mapping: no room-apply route exists yet.
		{"POST", "/v1/rooms/apply", ScopeRoomsWrite},
		{"PUT", "/v1/rooms/demo", ScopeRoomsWrite},
		{"GET", "/v1/rooms", ""},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, nil)
		if got := requiredScopeForRequest(r); got != c.want {
			t.Errorf("%s %s = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

func TestManagedSessionScope_LegacyWriteSurfaceGrantStillDrives(t *testing.T) {
	srv, ts := g0Server(t)
	// The stage's grant: surface grant, scope ["write"], no actor.
	tok := mintScopedTestGrant(t, srv, "stage-studio", ScopeWrite)
	var info ManagedSessionInfo
	if code := g0Do(t, ts, tok, "POST", "/v1/managed-sessions", map[string]any{}, &info); code != http.StatusCreated {
		t.Fatalf("stage-studio create = %d, want 201", code)
	}
	// An actor grant with only write does NOT get the compatibility rule.
	atok := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeWrite)
	if code := g0Do(t, ts, atok, "POST", "/v1/managed-sessions", map[string]any{}, nil); code != http.StatusForbidden {
		t.Fatalf("write-only actor grant create = %d, want 403", code)
	}
}

func TestManagedSessionRoom_ActorFromGrantAndFork(t *testing.T) {
	srv, ts := g0Server(t)
	chaz := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeManagedSessions)
	vroku := mintActorTestGrant(t, srv, "discord-vroku", "vroku", ScopeManagedSessions)

	// Body-supplied actor is ignored: the grant's actor wins.
	var a ManagedSessionInfo
	code := g0Do(t, ts, chaz, "POST", "/v1/managed-sessions",
		map[string]any{"room": "den", "actor": "vroku"}, &a)
	if code != http.StatusCreated {
		t.Fatalf("create = %d", code)
	}
	if a.Actor != "chaz" || a.Room != "den" {
		t.Fatalf("created actor/room = %q/%q, want chaz/den", a.Actor, a.Room)
	}

	// Bad room is a 400.
	if code := g0Do(t, ts, chaz, "POST", "/v1/managed-sessions", map[string]any{"room": "Bad Room"}, nil); code != http.StatusBadRequest {
		t.Fatalf("bad room = %d, want 400", code)
	}

	// Same actor forks: inherits room+actor; naming the same room is fine.
	var f1 ManagedSessionInfo
	if code := g0Do(t, ts, chaz, "POST", "/v1/managed-sessions",
		map[string]any{"fork_of": a.SessionID, "room": "den"}, &f1); code != http.StatusCreated {
		t.Fatalf("same-room fork = %d", code)
	}
	if f1.Room != "den" || f1.Actor != "chaz" {
		t.Fatalf("fork room/actor = %q/%q, want den/chaz", f1.Room, f1.Actor)
	}
	// Cross-room fork refused in G0.
	if code := g0Do(t, ts, chaz, "POST", "/v1/managed-sessions",
		map[string]any{"fork_of": a.SessionID, "room": "elsewhere"}, nil); code != http.StatusForbidden {
		t.Fatalf("cross-room fork = %d, want 403", code)
	}
	// A different actor forking inherits room and the parent's actor.
	var f2 ManagedSessionInfo
	if code := g0Do(t, ts, vroku, "POST", "/v1/managed-sessions",
		map[string]any{"fork_of": a.SessionID}, &f2); code != http.StatusCreated {
		t.Fatalf("cross-actor fork = %d", code)
	}
	if f2.Room != "den" || f2.Actor != "chaz" {
		t.Fatalf("cross-actor fork room/actor = %q/%q, want den/chaz", f2.Room, f2.Actor)
	}

	// ?room= filters; GET by id carries both fields.
	var other ManagedSessionInfo
	// load with a distinct id: the fake agent names every fresh process's
	// session "sess-1", which would replace the parent in the registry.
	g0Do(t, ts, vroku, "POST", "/v1/managed-sessions", map[string]any{"room": "lab", "load": "lab-1"}, &other)
	var list struct {
		Sessions []ManagedSessionInfo `json:"sessions"`
	}
	g0Do(t, ts, "", "GET", "/v1/managed-sessions?room=lab", nil, &list)
	if len(list.Sessions) != 1 || list.Sessions[0].SessionID != other.SessionID || list.Sessions[0].Actor != "vroku" {
		t.Fatalf("room=lab list = %+v", list.Sessions)
	}
	g0Do(t, ts, "", "GET", "/v1/managed-sessions?room=den", nil, &list)
	if len(list.Sessions) != 3 {
		t.Fatalf("room=den count = %d, want 3: %+v (a=%s f1=%s f2=%s)", len(list.Sessions), list.Sessions, a.SessionID, f1.SessionID, f2.SessionID)
	}
	g0Do(t, ts, "", "GET", "/v1/managed-sessions", nil, &list)
	if len(list.Sessions) != 4 {
		t.Fatalf("unfiltered count = %d, want 4", len(list.Sessions))
	}
}

func TestManagedSessionRoom_SurvivesCrashRestart(t *testing.T) {
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) { c.CrashWindow = time.Nanosecond })
	sess, err := d.Create(t.Context(), HermesACPOpts{Room: "den", Actor: "chaz"})
	if err != nil {
		t.Fatal(err)
	}
	// Session identity fields are set once on the struct the restart path
	// re-uses; assert they are still reported after a state transition.
	sess.setState(StateStalled, nil)
	sess.setState(StateLive, nil)
	if i := sess.Info(); i.Room != "den" || i.Actor != "chaz" {
		t.Fatalf("after restart-path state churn room/actor = %q/%q", i.Room, i.Actor)
	}
}

func TestGrantActor_MintListVerifyAndRebuild(t *testing.T) {
	root := t.TempDir()
	reg := NewIdentityGrantRegistryWithLedger(root)
	g, err := reg.MintOrReuseActor("discord-chaz", "chaz", []string{ScopeManagedSessions}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if g.Actor != "chaz" {
		t.Fatalf("actor = %q", g.Actor)
	}
	// Same surface, different actor: not a reuse.
	g2, err := reg.MintOrReuseActor("discord-chaz", "vroku", []string{ScopeManagedSessions}, time.Hour)
	if err != nil || g2.GrantID == g.GrantID || g2.Actor != "vroku" {
		t.Fatalf("actor change should supersede: %+v err=%v", g2, err)
	}
	if _, err := reg.MintOrReuseActor("x", "Bad Actor", nil, time.Hour); err == nil {
		t.Fatal("invalid actor accepted")
	}
	if _, err := reg.ExtendGrant("discord-chaz", 2*time.Hour); err != nil {
		t.Fatal(err)
	}

	rebuilt, err := RebuildIdentityGrantRegistryFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := rebuilt.VerifyAny(g2.Token)
	if !ok {
		t.Fatal("grant did not verify after rebuild")
	}
	if got.Actor != "vroku" {
		t.Fatalf("actor after rebuild = %q, want vroku", got.Actor)
	}
	if _, ok := rebuilt.VerifyAny(g.Token); ok {
		t.Fatal("superseded grant verifies after rebuild")
	}
}

func TestGrantActor_MintRouteRules(t *testing.T) {
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// admin grant, no actor, for its own surface.
	adm := mintScopedTestGrant(t, srv, "ops", ScopeAdmin, ScopeManagedSessions)
	// A non-root caller may not name an actor it is not.
	if code := g0Do(t, ts, adm, "POST", "/v1/identity/grants",
		map[string]any{"surface": "ops", "actor": "chaz", "scope": []string{ScopeManagedSessions}}, nil); code != http.StatusForbidden {
		t.Fatalf("self-mint naming an actor = %d, want 403", code)
	}
	// Non-admin grants cannot mint at all (unchanged).
	w := mintScopedTestGrant(t, srv, "w", ScopeWrite)
	if code := g0Do(t, ts, w, "POST", "/v1/identity/grants",
		map[string]any{"surface": "w", "actor": "chaz"}, nil); code != http.StatusForbidden {
		t.Fatalf("write-only mint = %d, want 403", code)
	}
	// Node-root may mint an actor grant; list and verify return the actor.
	root := mintScopedTestGrant(t, srv, nodeRootSurface, nodeRootScope)
	var minted identityGrantMintResponse
	if code := g0Do(t, ts, root, "POST", "/v1/identity/grants",
		map[string]any{"surface": "discord-chaz", "actor": "chaz", "scope": []string{ScopeManagedSessions}}, &minted); code != http.StatusOK || minted.Actor != "chaz" {
		t.Fatalf("root mint = %d actor=%q", code, minted.Actor)
	}
	var list struct {
		Grants []identityGrantListEntry `json:"grants"`
	}
	g0Do(t, ts, root, "GET", "/v1/identity/grants", nil, &list)
	found := false
	for _, e := range list.Grants {
		if e.Surface == "discord-chaz" && e.Actor == "chaz" {
			found = true
		}
	}
	if !found {
		t.Fatalf("list lacks actor: %+v", list.Grants)
	}
	var v identityVerifyResponse
	g0Do(t, ts, "", "POST", "/v1/identity/verify", map[string]any{"surface": "discord-chaz", "token": minted.Token}, &v)
	if !v.Valid || v.Actor != "chaz" {
		t.Fatalf("verify = %+v", v)
	}
	if code := g0Do(t, ts, root, "POST", "/v1/identity/grants",
		map[string]any{"surface": "bad", "actor": "NOPE!"}, nil); code != http.StatusBadRequest {
		t.Fatalf("bad actor = %d, want 400", code)
	}
}
