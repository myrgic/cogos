// serve_stage_ui_local_auth_test.go — the Stage UI listener's optional /local
// grant gate (stage_ui.local_auth: off | enforce).
//
// This file pins the contract, with a stub upstream that counts hits:
//
//   - off (default, and any unrecognised value): /local behaves exactly as
//     before this config existed; no grant is ever consulted.
//   - enforce: EVERY /local request — raw spelling, the cleaned spellings the
//     guard already normalises, and websocket upgrades — must carry a live,
//     unrevoked, unexpired grant with scope "stage:local" and actor "chaz",
//     presented as X-Cogos-Grant or Authorization: Bearer. Otherwise the
//     kernel answers 401 and the upstream is never hit.
//   - a grant with the wrong scope, the wrong actor, or a bogus token is
//     refused the same way; so is an expired or revoked one.
//   - the /local lockout exemption applies only to a granted request, so an
//     UNgranted /local from a locked-out IP is 429, not exempt.
//   - no token value appears in any log line.
//   - non-/local routes are untouched.
package engine

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testLocalActor is the operator the tests configure as stage_ui.local_actor.
const testLocalActor = "chaz"

// localGrant mints one grant for the /local auth tests.
func localGrant(t *testing.T, reg *IdentityGrantRegistry, surface, actor string, scopes []string, ttl time.Duration) *IdentityGrant {
	t.Helper()
	g, err := reg.MintOrReuseActor(surface, actor, scopes, ttl)
	if err != nil {
		t.Fatalf("MintOrReuseActor(%s): %v", surface, err)
	}
	return g
}

// localAuthHandlerG builds the listener with local_auth mode and, when reg is
// non-nil, the grant registry it verifies against.
func localAuthHandlerG(t *testing.T, upURL, mode string, reg *IdentityGrantRegistry) (http.Handler, *stageGuard) {
	t.Helper()
	c := StageUIConfig{Upstream: upURL, LocalAuth: mode, LocalActor: testLocalActor}
	if reg != nil {
		c.grants = func() *IdentityGrantRegistry { return reg }
	}
	h, g, err := newStageUIHandlerG(c)
	if err != nil {
		t.Fatalf("newStageUIHandlerG: %v", err)
	}
	return h, g
}

func localAuthHandler(t *testing.T, upURL, mode string, reg *IdentityGrantRegistry) http.Handler {
	t.Helper()
	h, _ := localAuthHandlerG(t, upURL, mode, reg)
	return h
}

// localAuthDo drives the handler in-process as a loopback caller. peer "" /
// "self" is the Stage host itself (no cf-connecting-ip); anything else is a
// tunnel caller carrying that cf-connecting-ip.
func localAuthDo(h http.Handler, method, target, peer string, hdr map[string]string, ws bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.Host = "localhost"
	req.RemoteAddr = "127.0.0.1:5000"
	if peer != "" && peer != "self" {
		req.Header.Set("Cf-Connecting-Ip", peer)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if ws {
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Enforce: the grant is the only way into /local, and every way of not having
// one is 401 with no upstream hit.
func TestStageUILocalAuth_EnforceRequiresGrant(t *testing.T) {
	reg := NewIdentityGrantRegistry()
	good := localGrant(t, reg, "good", "chaz", []string{ScopeStageLocal}, time.Hour)
	wrongScope := localGrant(t, reg, "wrong-scope", "chaz", []string{"write"}, time.Hour)
	wrongActor := localGrant(t, reg, "wrong-actor", "someone", []string{ScopeStageLocal}, time.Hour)
	expired := localGrant(t, reg, "expired", "chaz", []string{ScopeStageLocal}, time.Nanosecond)
	revoked := localGrant(t, reg, "revoked", "chaz", []string{ScopeStageLocal}, time.Hour)
	if _, err := reg.Revoke(revoked.GrantID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	up, hits := ownedUpstream(t)
	h := localAuthHandler(t, up.URL, StageUILocalAuthEnforce, reg)

	cases := []struct {
		name    string
		method  string
		target  string
		hdr     map[string]string
		want    int
		reaches bool
	}{
		{"no-credential", http.MethodGet, "/local/rooms", nil, 401, false},
		{"header-grant", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: good.Token}, 200, true},
		{"bearer-grant", http.MethodGet, "/local/rooms", map[string]string{"Authorization": "Bearer " + good.Token}, 200, true},
		{"header-wins-over-bogus-bearer", http.MethodGet, "/local/rooms",
			map[string]string{GrantHeaderName: good.Token, "Authorization": "Bearer not-a-grant"}, 200, true},
		{"bogus-token", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: "not-a-grant"}, 401, false},
		{"wrong-scope", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: wrongScope.Token}, 401, false},
		{"wrong-actor", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: wrongActor.Token}, 401, false},
		{"expired", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: expired.Token}, 401, false},
		{"revoked", http.MethodGet, "/local/rooms", map[string]string{GrantHeaderName: revoked.Token}, 401, false},
		// A /local write is still a /local request: authenticate before content.
		{"post-ungranted", http.MethodPost, "/local/rooms", nil, 401, false},
		{"post-granted", http.MethodPost, "/local/rooms", map[string]string{GrantHeaderName: good.Token}, 200, true},
		// Other routes are not this gate's business.
		{"non-local-unaffected", http.MethodGet, "/healthz", nil, 200, true},
		{"keyed-route-unaffected", http.MethodGet, "/r/K/dashboard/x", nil, 200, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			before := hits.Load()
			rec := localAuthDo(h, c.method, c.target, "self", c.hdr, false)
			if rec.Code != c.want {
				t.Fatalf("status %d want %d (body %q)", rec.Code, c.want, rec.Body.String())
			}
			got := hits.Load() - before
			if c.reaches && got != 1 {
				t.Fatalf("upstream hits %d want 1", got)
			}
			if !c.reaches && got != 0 {
				t.Fatalf("refused request reached the upstream (%d hits)", got)
			}
		})
	}
}

// Enforce covers the cleaned-path spellings the guard already normalises, in
// both directions: ungranted is 401, granted is let through.
func TestStageUILocalAuth_EnforceCleanedSpellings(t *testing.T) {
	reg := NewIdentityGrantRegistry()
	good := localGrant(t, reg, "good", "chaz", []string{ScopeStageLocal}, time.Hour)
	up, hits := ownedUpstream(t)
	h := localAuthHandler(t, up.URL, StageUILocalAuthEnforce, reg)

	for _, target := range []string{"/local/rooms", "//local/rooms", "/./local/rooms", "/local/../local/rooms"} {
		target := target
		t.Run(target, func(t *testing.T) {
			before := hits.Load()
			if rec := localAuthDo(h, http.MethodGet, target, "self", nil, false); rec.Code != 401 {
				t.Fatalf("ungranted: got %d want 401 (body %q)", rec.Code, rec.Body.String())
			}
			if hits.Load() != before {
				t.Fatalf("ungranted %s reached the upstream", target)
			}
			if rec := localAuthDo(h, http.MethodGet, target, "self", map[string]string{GrantHeaderName: good.Token}, false); rec.Code != 200 {
				t.Fatalf("granted: got %d want 200 (body %q)", rec.Code, rec.Body.String())
			}
			if hits.Load() != before+1 {
				t.Fatalf("granted %s did not reach the upstream", target)
			}
		})
	}
}

// Websocket upgrades on /local are gated the same way.
func TestStageUILocalAuth_EnforceWebSocket(t *testing.T) {
	reg := NewIdentityGrantRegistry()
	good := localGrant(t, reg, "good", "chaz", []string{ScopeStageLocal}, time.Hour)
	up, hits := ownedUpstream(t)
	h := localAuthHandler(t, up.URL, StageUILocalAuthEnforce, reg)

	before := hits.Load()
	if rec := localAuthDo(h, http.MethodGet, "/local/ws", "self", nil, true); rec.Code != 401 {
		t.Fatalf("ungranted ws: got %d want 401 (body %q)", rec.Code, rec.Body.String())
	}
	if hits.Load() != before {
		t.Fatal("ungranted websocket reached the upstream")
	}
	if rec := localAuthDo(h, http.MethodGet, "/local/ws", "self", map[string]string{GrantHeaderName: good.Token}, true); rec.Code != 200 {
		t.Fatalf("granted ws: got %d want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if hits.Load() != before+1 {
		t.Fatal("granted websocket did not reach the upstream")
	}
}

// The lockout exemption for /local must not apply to an ungranted request.
func TestStageUILocalAuth_EnforceLockoutExemptionNeedsGrant(t *testing.T) {
	reg := NewIdentityGrantRegistry()
	good := localGrant(t, reg, "good", "chaz", []string{ScopeStageLocal}, time.Hour)
	up, hits := ownedUpstream(t)
	h, g := localAuthHandlerG(t, up.URL, StageUILocalAuthEnforce, reg)

	const lockedIP = "203.0.113.9"
	g.mu.Lock()
	g.locked[lockedIP] = time.Now().Add(time.Hour)
	g.mu.Unlock()

	before := hits.Load()
	if rec := localAuthDo(h, http.MethodGet, "/local/rooms", lockedIP, nil, false); rec.Code != 429 {
		t.Fatalf("ungranted /local from a locked IP: got %d want 429 (body %q)", rec.Code, rec.Body.String())
	}
	if hits.Load() != before {
		t.Fatal("ungranted /local from a locked IP reached the upstream")
	}
	// With the grant the exemption applies again; the not-local rule still
	// refuses a tunnel caller, but the answer is no longer the lockout.
	if rec := localAuthDo(h, http.MethodGet, "/local/rooms", lockedIP, map[string]string{GrantHeaderName: good.Token}, false); rec.Code == 429 {
		t.Fatalf("granted /local from a locked IP was still locked out (body %q)", rec.Body.String())
	}
}

// Enforce with no grant registry must fail closed, never open the door.
func TestStageUILocalAuth_EnforceWithoutRegistryFailsClosed(t *testing.T) {
	up, hits := ownedUpstream(t)
	h := localAuthHandler(t, up.URL, StageUILocalAuthEnforce, nil)
	before := hits.Load()
	if rec := localAuthDo(h, http.MethodGet, "/local/rooms", "self", nil, false); rec.Code != 401 {
		t.Fatalf("enforce without a registry: got %d want 401", rec.Code)
	}
	if hits.Load() != before {
		t.Fatal("enforce without a registry let a /local request through")
	}
}

// Off (and every unrecognised value) is exactly the old behaviour: no grant
// needed, /local still reaches the upstream.
func TestStageUILocalAuth_OffIsUnchanged(t *testing.T) {
	reg := NewIdentityGrantRegistry() // present, but must not be consulted
	up, hits := ownedUpstream(t)
	for _, mode := range []string{"", StageUILocalAuthOff, "bogus"} {
		h := localAuthHandler(t, up.URL, mode, reg)
		for _, target := range []string{"/local/rooms", "//local/rooms", "/./local/rooms"} {
			before := hits.Load()
			if rec := localAuthDo(h, http.MethodGet, target, "self", nil, false); rec.Code != 200 {
				t.Fatalf("mode %q %s: got %d want 200 (off must not gate)", mode, target, rec.Code)
			}
			if hits.Load() != before+1 {
				t.Fatalf("mode %q %s: did not reach the upstream", mode, target)
			}
		}
		before := hits.Load()
		if rec := localAuthDo(h, http.MethodGet, "/local/ws", "self", nil, true); rec.Code != 200 {
			t.Fatalf("mode %q ws: got %d want 200", mode, rec.Code)
		}
		if hits.Load() != before+1 {
			t.Fatalf("mode %q ws: did not reach the upstream", mode)
		}
	}
}

// No log line may ever carry a token value.
func TestStageUILocalAuth_NeverLogsToken(t *testing.T) {
	logs := captureStageLogs(t)
	reg := NewIdentityGrantRegistry()
	good := localGrant(t, reg, "good", "chaz", []string{ScopeStageLocal}, time.Hour)
	up, _ := ownedUpstream(t)
	h := localAuthHandler(t, up.URL, StageUILocalAuthEnforce, reg)

	const bogus = "SECRET-BOGUS-TOKEN-123"
	localAuthDo(h, http.MethodGet, "/local/rooms", "self", nil, false)
	localAuthDo(h, http.MethodGet, "/local/rooms", "self", map[string]string{GrantHeaderName: bogus}, false)
	localAuthDo(h, http.MethodGet, "/local/rooms", "self", map[string]string{GrantHeaderName: good.Token}, false)
	localAuthDo(h, http.MethodGet, "/local/rooms", "self", map[string]string{"Authorization": "Bearer " + good.Token}, false)

	out := logs.String()
	if !strings.Contains(out, "stage-ui:") {
		t.Fatalf("expected stage-ui log lines, got:\n%s", out)
	}
	for _, secret := range []string{good.Token, bogus} {
		if strings.Contains(out, secret) {
			t.Fatalf("log leaked a token value %q:\n%s", secret, out)
		}
	}
}

func TestStageUILocalAuth_ConfigYAML(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, ".cog", "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StageUI.LocalAuth != "" {
		t.Fatalf("local_auth must default empty (off), got %q", cfg.StageUI.LocalAuth)
	}
	y := "stage_ui:\n  addr: 127.0.0.1:7873\n  local_auth: enforce\n"
	if err := os.WriteFile(filepath.Join(ws, ".cog", "config", "kernel.yaml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StageUI.LocalAuth != "enforce" {
		t.Fatalf("local_auth not applied: %q", cfg.StageUI.LocalAuth)
	}
}

func TestStageUILocalAuth_ConfigSection(t *testing.T) {
	cfg := &Config{}
	applyStageUISection(cfg, &stageUISection{LocalAuth: "enforce"})
	if cfg.StageUI.LocalAuth != "enforce" {
		t.Fatalf("local_auth not applied: %q", cfg.StageUI.LocalAuth)
	}
	cfg2 := &Config{}
	applyStageUISection(cfg2, &stageUISection{})
	if cfg2.StageUI.LocalAuth != "" {
		t.Fatalf("empty section must leave local_auth empty, got %q", cfg2.StageUI.LocalAuth)
	}
}

func TestStageUILocalAuth_ModeNormalization(t *testing.T) {
	for in, want := range map[string]struct {
		mode string
		ok   bool
	}{
		"":         {StageUILocalAuthOff, true},
		"off":      {StageUILocalAuthOff, true},
		"Off":      {StageUILocalAuthOff, true},
		" enforce": {StageUILocalAuthEnforce, true},
		"enforce":  {StageUILocalAuthEnforce, true},
		"ENFORCE":  {StageUILocalAuthEnforce, true},
		"on":       {StageUILocalAuthOff, false},
		"shadow":   {StageUILocalAuthOff, false},
	} {
		m, ok := normalizeStageUILocalAuthMode(in)
		if m != want.mode || ok != want.ok {
			t.Errorf("%q: got %s %v want %s %v", in, m, ok, want.mode, want.ok)
		}
	}
}

// Under enforce with no local_actor configured, even a grant that would
// otherwise pass is refused: there is no operator to act for (fail closed).
func TestStageUILocalAuth_EnforceWithoutActorFailsClosed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("upstream reached: %s", r.URL.Path)
	}))
	defer up.Close()
	reg := NewIdentityGrantRegistry()
	g := localGrant(t, reg, "stage-local", testLocalActor, []string{ScopeStageLocal}, time.Hour)
	c := StageUIConfig{Upstream: up.URL, LocalAuth: "enforce"}
	c.grants = func() *IdentityGrantRegistry { return reg }
	h, _, err := newStageUIHandlerG(c)
	if err != nil {
		t.Fatalf("newStageUIHandlerG: %v", err)
	}
	rec := localAuthDo(h, http.MethodGet, "/local/rooms", "self", map[string]string{"X-Cogos-Grant": g.Token}, false)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("enforce without local_actor = %d; want 401", rec.Code)
	}
}
