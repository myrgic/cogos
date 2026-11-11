package engine

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// K5: the OAuth AS on the public stage_ui listener (:7873). These tests build
// the real listener handler (newStageUIHandlerG) in front of a stub Python
// stage, with the kernel's real OAuth handlers behind oauthPublic, and drive
// it the way the tunnel does: Host = the public host, Cf-Connecting-Ip set.

const (
	k5PublicHost = "stage.example.test"
	// k5StageBody is what the stub Python stage answers: any response with
	// this body provably came from the proxy, not from a kernel handler.
	k5StageBody = "python-stage"
)

type k5Env struct {
	*oauthAuthzEnv
	listener  *httptest.Server
	handler   http.Handler
	hits      atomic.Int64 // requests that reached the stub stage
	lastPath  atomic.Value // string
	lastGrant atomic.Value // string: X-Cogos-Grant the stage saw
}

func newK5Env(t *testing.T, oauthOn bool) *k5Env {
	t.Helper()
	env := &k5Env{oauthAuthzEnv: newOAuthAuthzEnv(t)}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.hits.Add(1)
		env.lastPath.Store(r.URL.Path)
		env.lastGrant.Store(r.Header.Get(GrantHeaderName))
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(k5StageBody))
	}))
	t.Cleanup(up.Close)

	c := StageUIConfig{Upstream: up.URL, PublicHost: k5PublicHost}
	if oauthOn {
		c.oauthPublic = env.srv.oauthPublicHandler
	}
	h, _, err := newStageUIHandlerG(c)
	if err != nil {
		t.Fatalf("newStageUIHandlerG: %v", err)
	}
	env.handler = h
	env.listener = httptest.NewServer(h)
	t.Cleanup(env.listener.Close)
	return env
}

// do sends one request through the listener as a tunnel caller from ip.
func (e *k5Env) do(t *testing.T, method, target, ip string, hdr map[string]string, body string) (int, string, http.Header) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.listener.URL+target, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = k5PublicHost
	if ip != "" {
		req.Header.Set("Cf-Connecting-Ip", ip)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func (e *k5Env) viaStage(body string) bool { return body == k5StageBody }

func TestOAuthListenerPath_ExactAllowlist(t *testing.T) {
	yes := []string{
		"/.well-known/oauth-authorization-server",
		"/oauth/authorize",
		"/oauth/authorize/status/oar-0123456789abcdef",
		"/oauth/authorize/status/X_y-9",
		"/oauth/token",
	}
	for _, p := range yes {
		if !isOAuthListenerPath(p) {
			t.Errorf("%q must be on the listener allowlist", p)
		}
	}
	no := []string{
		// The prefix helper would have admitted every /oauth/... one of these.
		"/oauth/", "/oauth", "/oauth/anything-else", "/oauth/register", "/oauth/revoke",
		"/oauth/token/", "/oauth/token/x", "/oauth/authorize/", "/oauth/authorize/x",
		"/oauth/authorize/status", "/oauth/authorize/status/",
		"/oauth/authorize/status/a/b", "/oauth/authorize/status/a/", "/oauth/authorize/status/a.b",
		"/oauth/authorize/status/a b", "/oauth/authorize/status/a%b",
		"/oauth/authorize/status/" + strings.Repeat("a", oauthStatusIDMax+1),
		// Never the internal tree.
		"/v1/oauth", "/v1/oauth/", "/v1/oauth/introspect", "/v1/oauth/grants",
		"/v1/oauth/grants/g1/revoke", "/v1/oauth/requests/oar-1/approve", "/v1/oauth/requests/oar-1/deny",
		// Spelling variants that only a cleaning/case-folding matcher would admit.
		"//oauth/token", "/oauth//token", "/./oauth/token", "/oauth/../oauth/token", "/OAuth/token",
		"/OAUTH/TOKEN", "/.well-known/oauth-authorization-server/", "/.well-known/oauth-protected-resource",
		"/.well-known/oauth-authorization-server/x", "/x/oauth/token", "",
	}
	for _, p := range no {
		if isOAuthListenerPath(p) {
			t.Errorf("%q must NOT be on the listener allowlist", p)
		}
	}
	// The grant-gate helper stays a prefix match on :6932: K5 must not have
	// widened or narrowed it.
	if !isOAuthPublicPath("/oauth/anything-else") || isOAuthPublicPath("/v1/oauth/introspect") {
		t.Error("isOAuthPublicPath (the :6932 gate exemption) changed")
	}
}

// The four public paths serve from the kernel on the listener, end to end:
// metadata, authorize, status poll, token. Internal approve stays on :6932.
func TestOAuthListener_FourPublicPathsServe(t *testing.T) {
	env := newK5Env(t, true)
	before := env.hits.Load()

	// 1. metadata
	code, body, hdr := env.do(t, "GET", oauthWellKnownASPath, "203.0.113.7", nil, "")
	t.Logf("1. :7873 GET %s -> %d  Content-Type=%s\n%s", oauthWellKnownASPath, code, hdr.Get("Content-Type"), body)
	if code != 200 || !strings.Contains(body, `"token_endpoint": "https://stage.example.test/oauth/token"`) ||
		!strings.Contains(body, `"S256"`) || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("metadata not served by the kernel: %d %s", code, body)
	}

	// 2. authorize
	q := validQuery()
	code, body, _ = env.do(t, "GET", "/oauth/authorize?"+q.Encode(), "203.0.113.7", map[string]string{"Accept": "application/json"}, "")
	t.Logf("2. :7873 GET /oauth/authorize?... -> %d\n%s", code, body)
	var res authzResult
	if code != 200 {
		t.Fatalf("authorize = %d %s", code, body)
	}
	mustJSON(t, body, &res)
	if res.RequestID == "" || res.PollSecret == "" || res.StatusPath == "" {
		t.Fatalf("authorize body incomplete: %s", body)
	}

	// 3. status (still pending: short long-poll in the test env)
	code, body, _ = env.do(t, "GET", res.StatusPath, "203.0.113.7", map[string]string{oauthPollSecretHeader: res.PollSecret}, "")
	t.Logf("3. :7873 GET %s (pending) -> %d %s", res.StatusPath, code, strings.TrimSpace(body))
	if code != http.StatusAccepted {
		t.Fatalf("pending status = %d %s", code, body)
	}

	// The stage approves over :6932 (internal path, grant-gated).
	if c, b := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", env.consentToken(t),
		map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash}); c != 200 {
		t.Fatalf("approve on :6932 = %d %s", c, b)
	}
	code, body, _ = env.do(t, "GET", res.StatusPath, "203.0.113.7", map[string]string{oauthPollSecretHeader: res.PollSecret}, "")
	if code != 200 {
		t.Fatalf("approved status = %d %s", code, body)
	}
	var st struct {
		Redirect string `json:"redirect"`
	}
	mustJSON(t, body, &st)
	u, err := url.Parse(st.Redirect)
	if err != nil || !strings.HasPrefix(u.Query().Get("code"), OAuthAuthCodePrefix) {
		t.Fatalf("no code in status redirect: %s", body)
	}
	authCode := u.Query().Get("code")
	t.Logf("3b. :7873 GET %s (approved) -> %d redirect=%s...", res.StatusPath, code, st.Redirect[:strings.Index(st.Redirect, "code=")+9])

	// 4. token
	code, body, hdr = env.do(t, "POST", oauthTokenPath, "203.0.113.7",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, exchangeForm(authCode).Encode())
	t.Logf("4. :7873 POST /oauth/token -> %d Cache-Control=%s\n%s", code, hdr.Get("Cache-Control"), body)
	if code != 200 || hdr.Get("Cache-Control") != "no-store" {
		t.Fatalf("token = %d %s", code, body)
	}
	tok := tokenFields(t, body)
	if !strings.HasPrefix(tok.AccessToken, OAuthAccessTokenPrefix) || tok.RefreshToken == "" {
		t.Fatalf("token body: %s", body)
	}

	if n := env.hits.Load() - before; n != 0 {
		t.Errorf("%d of the four public-path requests reached the stage proxy; the kernel must answer them", n)
	}
}

// Nothing but the four paths is kernel-served: every other /oauth/... path and
// all of /v1/oauth/* fall through to the proxy, even carrying a real,
// correctly-scoped grant.
func TestOAuthListener_NoOtherOAuthPathIsServed(t *testing.T) {
	env := newK5Env(t, true)
	introspectTok := mintScopedTestGrant(t, env.srv, "stage-studio-test", ScopeOAuthIntrospect, ScopeOAuthConsent, ScopeAdmin)

	paths := []struct{ method, path string }{
		{"POST", "/v1/oauth/introspect"},
		{"GET", "/v1/oauth/grants"},
		{"POST", "/v1/oauth/grants/og-1/revoke"},
		{"POST", "/v1/oauth/requests/oar-1/approve"},
		{"POST", "/v1/oauth/requests/oar-1/deny"},
		{"GET", "/v1/oauth"},
		{"GET", "/oauth/anything-else"},
		{"POST", "/oauth/register"},
		{"GET", "/oauth/"},
		{"GET", "/oauth/token/"},
		{"POST", "/oauth/token/x"},
		{"GET", "/oauth/authorize/status/"},
		{"GET", "/oauth/authorize/status/a/b"},
		{"GET", "//oauth/token"},
		{"GET", "/OAuth/token"},
		{"GET", "/.well-known/oauth-authorization-server/"},
	}
	for _, tc := range paths {
		for _, withGrant := range []bool{false, true} {
			hdr := map[string]string{"Content-Type": "application/json"}
			if withGrant {
				hdr[GrantHeaderName] = introspectTok
			}
			before := env.hits.Load()
			code, body, _ := env.do(t, tc.method, tc.path, "203.0.113.9", hdr, "")
			// Proxied == the stub stage's own answer, and the stub saw the hit.
			if code != 200 || !env.viaStage(body) || env.hits.Load() != before+1 {
				t.Errorf("%s %s (grant=%v) was not proxied: %d %q (stage hits +%d)",
					tc.method, tc.path, withGrant, code, body, env.hits.Load()-before)
			}
		}
	}
	// The kernel's introspect handler was never run: build the same request
	// against :6932's handler to show what a kernel answer would look like.
	c, b := env.post(t, "/v1/oauth/introspect", introspectTok, map[string]string{"token": "cgot_x"})
	if c == 200 && env.viaStage(b) {
		t.Fatalf("control request on :6932 looks proxied: %d %s", c, b)
	}
	t.Logf("proof: POST /v1/oauth/introspect on :6932 -> %d %s", c, strings.TrimSpace(b))
	_, b7873, _ := env.do(t, "POST", "/v1/oauth/introspect", "203.0.113.9",
		map[string]string{"Content-Type": "application/json", GrantHeaderName: introspectTok}, `{"token":"x"}`)
	t.Logf("proof: POST /v1/oauth/introspect on :7873 (same valid oauth:introspect grant) -> %q (the stub stage's body: proxied, not a kernel handler)", b7873)
}

// The dedicated mux is a second, independent allowlist: even if a path slipped
// past isOAuthListenerPath, the handler behind it has no /v1/oauth/* route.
func TestOAuthPublicHandler_MuxHasOnlyThePublicRoutes(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	h := env.srv.oauthPublicHandler()
	if h == nil {
		t.Fatal("oauth enabled: want a handler")
	}
	tok := mintScopedTestGrant(t, env.srv, "stage-studio-test", ScopeOAuthIntrospect, ScopeOAuthConsent, ScopeAdmin)
	for _, p := range []string{"/v1/oauth/introspect", "/v1/oauth/grants", "/v1/oauth/requests/x/approve", "/v1/attention", "/health", "/oauth/anything-else"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", p, strings.NewReader("{}"))
		req.Header.Set(GrantHeaderName, tok)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("public mux answered %s with %d %s; want 404", p, rec.Code, rec.Body.String())
		}
	}
}

func TestOAuthListener_DisabledMeansProxiedAsBefore(t *testing.T) {
	env := newK5Env(t, false)
	// And the real startStageUI wiring with oauth off yields no handler.
	srvOff := &Server{cfg: &Config{}}
	if srvOff.oauthPublicHandler() != nil {
		t.Fatal("oauth disabled must give a nil public handler")
	}
	for _, p := range []string{oauthWellKnownASPath, "/oauth/authorize", "/oauth/token"} {
		code, body, _ := env.do(t, "GET", p, "203.0.113.1", nil, "")
		if code != 200 || !env.viaStage(body) {
			t.Errorf("%s with no oauth handler: %d %q, want the proxied stage answer", p, code, body)
		}
	}
}

// The guard still runs first: an unknown Host never reaches the kernel's AS.
func TestOAuthListener_GuardStillGatesHost(t *testing.T) {
	env := newK5Env(t, true)
	req, _ := http.NewRequest("GET", env.listener.URL+oauthWellKnownASPath, nil)
	req.Host = "evil.example"
	req.Header.Set("Cf-Connecting-Ip", "203.0.113.2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Errorf("foreign Host got %d, want 421 from the guard", resp.StatusCode)
	}
}

// An OAuth access token is not a credential anywhere the kernel decides:
// presented on the listener to a non-OAuth route it is simply proxied (the
// stage decides, via its own Bearer gate), and it never reaches a kernel
// grant-gated handler. The current listener behavior for /v1/attention is
// "not kernel-owned, so proxy to the stage" (newStageUIHandlerG: static ->
// owned prefixes -> proxy; /v1/attention is not an owned prefix).
func TestOAuthListener_AccessTokenReachesNoKernelHandler(t *testing.T) {
	env := newK5Env(t, true)
	code, _ := env.approvedCode(t)
	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 200 {
		t.Fatalf("mint token via :6932: %d %s", c, b)
	}
	tok := tokenFields(t, b)

	// Control: the same token on :6932's gated /v1/attention is refused by the
	// KERNEL (K4), proving that route is the kernel's and is token-aware.
	c, b = env.post(t, "/v1/attention", tok.AccessToken, map[string]string{})
	if c != http.StatusForbidden || errorType(t, b) != "oauth_token_not_grant" {
		t.Fatalf("control: :6932 /v1/attention = %d %s", c, b)
	}
	t.Logf("control :6932 POST /v1/attention (oauth access token as X-Cogos-Grant) -> %d %s", c, strings.TrimSpace(b))

	for _, hdrName := range []string{GrantHeaderName, "Authorization"} {
		val := tok.AccessToken
		if hdrName == "Authorization" {
			val = "Bearer " + tok.AccessToken
		}
		for _, p := range []string{"/v1/attention", "/h/somekey/", "/local/x", "/mcp"} {
			before := env.hits.Load()
			code, body, _ := env.do(t, "POST", p, "203.0.113.5", map[string]string{hdrName: val, "Content-Type": "application/json"}, "{}")
			switch {
			case strings.HasPrefix(p, "/local"):
				// The stage guard refuses /local to tunnel callers before any
				// proxying; either way no kernel API handler answers.
				if code == 200 && !env.viaStage(body) {
					t.Errorf("%s answered by something other than the guard/proxy: %d %q", p, code, body)
				}
			default:
				if code != 200 || !env.viaStage(body) || env.hits.Load() != before+1 {
					t.Errorf("%s with %s=oauth token: %d %q — want the proxy's answer", p, hdrName, code, body)
				}
			}
			if strings.Contains(body, "oauth_token_not_grant") {
				t.Errorf("%s reached the kernel grant gate on the public listener: %q", p, body)
			}
		}
	}
	t.Logf(":7873 POST /v1/attention with the same oauth token -> proxied to the stage (stub body %q), never the kernel's 403", k5StageBody)
}

// ─── /oauth/token rate limit ─────────────────────────────────────────────────

func TestOAuthListener_TokenRateLimit429WithRetryAfter(t *testing.T) {
	env := newK5Env(t, true)
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	bad := url.Values{"grant_type": {"authorization_code"}, "code": {"cgac_nope"}}.Encode()

	var firstBlocked int
	var blockedBody string
	var blockedHdr http.Header
	for i := 1; i <= oauthTokenIPLimit+5; i++ {
		code, body, hdr := env.do(t, "POST", oauthTokenPath, "198.51.100.10", form, bad)
		if i <= oauthTokenIPLimit {
			if code == http.StatusTooManyRequests {
				t.Fatalf("request %d of %d was limited early", i, oauthTokenIPLimit)
			}
			continue
		}
		if code != http.StatusTooManyRequests {
			t.Fatalf("request %d: %d %s, want 429", i, code, body)
		}
		if firstBlocked == 0 {
			firstBlocked, blockedBody, blockedHdr = i, body, hdr
		}
	}
	ra, err := strconv.Atoi(blockedHdr.Get("Retry-After"))
	if err != nil || ra < 1 || ra > int(oauthTokenIPWindow/time.Second) {
		t.Errorf("Retry-After = %q, want 1..%d seconds", blockedHdr.Get("Retry-After"), int(oauthTokenIPWindow/time.Second))
	}
	if !strings.Contains(blockedBody, "temporarily_unavailable") || blockedHdr.Get("Cache-Control") != "no-store" {
		t.Errorf("429 body/headers: %s / %v", blockedBody, blockedHdr)
	}
	t.Logf("request #%d from 198.51.100.10 -> 429 Retry-After=%s Cache-Control=%s\n%s",
		firstBlocked, blockedHdr.Get("Retry-After"), blockedHdr.Get("Cache-Control"), strings.TrimSpace(blockedBody))

	// Per-IP: another caller is unaffected.
	if code, body, _ := env.do(t, "POST", oauthTokenPath, "198.51.100.11", form, bad); code == http.StatusTooManyRequests {
		t.Errorf("a different IP was limited: %s", body)
	}
	// The limit is on /oauth/token only: the other public paths still answer.
	if code, _, _ := env.do(t, "GET", oauthWellKnownASPath, "198.51.100.10", nil, ""); code != 200 {
		t.Errorf("metadata from the limited IP = %d", code)
	}
	// A limited caller does not touch the proxy.
	if env.hits.Load() != 0 {
		t.Errorf("%d request(s) reached the stage", env.hits.Load())
	}
}

// An unauthenticated attacker cannot dodge the per-IP limit by omitting
// Cf-Connecting-Ip from a loopback peer (everything then shares the peer key),
// and the key falls back to the TCP peer when no tunnel header is present.
func TestOAuthListener_TokenRateLimitKeysOnPeerWhenNoTunnelHeader(t *testing.T) {
	env := newK5Env(t, true)
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	var got429 bool
	for i := 0; i < oauthTokenIPLimit+2; i++ {
		if code, _, _ := env.do(t, "POST", oauthTokenPath, "", form, "grant_type=x"); code == http.StatusTooManyRequests {
			got429 = true
		}
	}
	if !got429 {
		t.Error("no limit applied to header-less loopback callers")
	}
}

func TestOAuthPublicLimiter_WindowResetOverflowAndGlobal(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	l := newOAuthPublicLimiter(2, 5, time.Minute, 2)
	l.now = func() time.Time { return now }

	if ok, _ := l.allow("a"); !ok {
		t.Fatal("a#1")
	}
	if ok, _ := l.allow("a"); !ok {
		t.Fatal("a#2")
	}
	ok, ra := l.allow("a")
	if ok || ra != time.Minute {
		t.Fatalf("a#3 ok=%v retry=%v, want blocked for the full window", ok, ra)
	}
	now = now.Add(40 * time.Second)
	if _, ra := l.allow("a"); ra != 20*time.Second {
		t.Errorf("retry after 40s = %v, want 20s", ra)
	}
	now = now.Add(21 * time.Second)
	if ok, _ := l.allow("a"); !ok {
		t.Error("window did not reset")
	}

	// Table bound: past maxIPs, unseen IPs share one overflow bucket instead
	// of growing the table or displacing a tracked client.
	l = newOAuthPublicLimiter(2, 1000, time.Minute, 2)
	l.now = func() time.Time { return now }
	l.allow("x")
	l.allow("y")
	l.allow("z1") // overflow bucket: 1
	l.allow("z2") // overflow bucket: 2
	if ok, _ := l.allow("z3"); ok {
		t.Error("overflow bucket did not limit")
	}
	if len(l.perIP) != 2 {
		t.Errorf("per-IP table grew to %d, cap is 2", len(l.perIP))
	}
	if ok, _ := l.allow("x"); !ok { // x still has its second slot
		t.Error("a tracked client was displaced by the overflow")
	}

	// Global ceiling across many IPs.
	l = newOAuthPublicLimiter(10, 3, time.Minute, 100)
	l.now = func() time.Time { return now }
	for i, ip := range []string{"p", "q", "r"} {
		if ok, _ := l.allow(ip); !ok {
			t.Fatalf("global #%d refused early", i)
		}
	}
	if ok, ra := l.allow("s"); ok || ra < time.Second {
		t.Errorf("global ceiling: ok=%v retry=%v", ok, ra)
	}
}

// The production wiring: startStageUI binds a real TCP listener and must hand
// it the OAuth handler (c.oauthPublic = s.oauthPublicHandler). Everything above
// builds the handler directly; this proves the line that connects them.
func TestOAuthListener_StartStageUIWiresTheAS(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	var hits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(k5StageBody))
	}))
	defer up.Close()
	env.srv.cfg.StageUI = StageUIConfig{Addr: "127.0.0.1:0", Upstream: up.URL, PublicHost: k5PublicHost}
	env.srv.startStageUI()
	defer env.srv.stopStageUI()
	env.srv.stageUIMu.Lock()
	addr := env.srv.stageUIAddr
	env.srv.stageUIMu.Unlock()
	if addr == "" {
		t.Fatal("stage_ui listener did not start")
	}
	get := func(path string) (int, string) {
		req, _ := http.NewRequest("GET", "http://"+addr+path, nil)
		req.Host = k5PublicHost
		req.Header.Set("Cf-Connecting-Ip", "203.0.113.77")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	code, body := get(oauthWellKnownASPath)
	if code != 200 || !strings.Contains(body, `"issuer": "https://stage.example.test"`) {
		t.Errorf("real listener %s: %d %s", addr, code, body)
	}
	if code, body := get("/v1/oauth/grants"); code != 200 || body != k5StageBody {
		t.Errorf("real listener /v1/oauth/grants: %d %q, want the proxied stage body", code, body)
	}
	t.Logf("real listener %s: metadata 200 from the kernel; /v1/oauth/grants proxied (stage hits=%d)", addr, hits.Load())
}

// A slow-body POST to /oauth/token cannot pin the connection: the listener has
// no server-wide ReadTimeout, so the handler sets a read deadline itself.
func TestOAuthListener_TokenSlowBodyIsCutOff(t *testing.T) {
	old := oauthTokenReadTimeout
	oauthTokenReadTimeout = 300 * time.Millisecond
	t.Cleanup(func() { oauthTokenReadTimeout = old })
	env := newK5Env(t, true)

	conn, err := net.Dial("tcp", env.listener.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Promise a body, send none.
	_, _ = conn.Write([]byte("POST /oauth/token HTTP/1.1\r\nHost: " + k5PublicHost + "\r\n" +
		"Cf-Connecting-Ip: 203.0.113.50\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n\r\ngrant_type="))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response to a stalled token body within 5s: %v", err)
	}
	defer resp.Body.Close()
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("stalled body cut off after %v", d)
	}
	b, _ := io.ReadAll(resp.Body)
	t.Logf("stalled body -> %d after %v: %s", resp.StatusCode, time.Since(start).Round(10*time.Millisecond), strings.TrimSpace(string(b)))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 invalid_request", resp.StatusCode)
	}
}

func mustJSON(t *testing.T, s string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(s), v); err != nil {
		t.Fatalf("json: %v: %s", err, s)
	}
}

// F1 (K5 follow-up): the read deadline must be armed BEFORE the rate-limit
// check. An over-limit caller that promises a body and stalls would otherwise
// pin the connection: net/http drains the unread request body before it writes
// the 429 header, so with no deadline the 429 never leaves the server.
func TestOAuthListener_StalledBodyFromRateLimitedIPIsCutOff(t *testing.T) {
	old := oauthTokenReadTimeout
	oauthTokenReadTimeout = 300 * time.Millisecond
	t.Cleanup(func() { oauthTokenReadTimeout = old })
	env := newK5Env(t, true)
	form := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	const ip = "203.0.113.60"

	// A fast request under the limit is unaffected by the early deadline.
	start := time.Now()
	if code, body, _ := env.do(t, "POST", oauthTokenPath, ip, form, "grant_type=x"); code == http.StatusTooManyRequests {
		t.Fatalf("first request limited: %s", body)
	}
	if d := time.Since(start); d > oauthTokenReadTimeout {
		t.Errorf("fast request took %v, longer than the read deadline %v", d, oauthTokenReadTimeout)
	}
	// Exhaust the rest of the IP's window.
	for i := 1; i < oauthTokenIPLimit; i++ {
		if code, body, _ := env.do(t, "POST", oauthTokenPath, ip, form, "grant_type=x"); code == http.StatusTooManyRequests {
			t.Fatalf("request %d limited early: %s", i+1, body)
		}
	}
	if code, _, _ := env.do(t, "POST", oauthTokenPath, ip, form, "grant_type=x"); code != http.StatusTooManyRequests {
		t.Fatalf("setup: ip not rate limited, got %d", code)
	}

	// Now the over-limit caller sends headers promising a body and stalls.
	conn, err := net.Dial("tcp", env.listener.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("POST /oauth/token HTTP/1.1\r\nHost: " + k5PublicHost + "\r\n" +
		"Cf-Connecting-Ip: " + ip + "\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 100\r\n\r\ngrant_type="))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start = time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("rate-limited stalled request was held open past 5s (deadline %v): %v", oauthTokenReadTimeout, err)
	}
	defer resp.Body.Close()
	t.Logf("rate-limited + stalled body -> %d after %v (read deadline %v)", resp.StatusCode, elapsed.Round(10*time.Millisecond), oauthTokenReadTimeout)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
	if elapsed > 2*time.Second {
		t.Errorf("429 delivered after %v, want about the %v deadline", elapsed, oauthTokenReadTimeout)
	}
}

// F2 (K5 follow-up): IPv6 callers are bucketed by /64, so one host rotating
// through its own /64 cannot mint unlimited fresh per-IP buckets and starve the
// global ceiling. IPv4 stays keyed per full address.
func TestOAuthPublicLimiter_IPv6KeysOnSlash64(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	mk := func(ipLimit, global int) *oauthPublicLimiter {
		l := newOAuthPublicLimiter(ipLimit, global, time.Minute, 100)
		l.now = func() time.Time { return now }
		return l
	}

	// Many distinct addresses in one /64 share ONE bucket and exhaust it together.
	l := mk(3, 1000)
	same := []string{
		"2001:db8:1:2::1", "2001:db8:1:2::2", "2001:db8:1:2:aaaa:bbbb:cccc:dddd",
		"2001:db8:1:2:ffff:ffff:ffff:ffff", "2001:0db8:0001:0002:0000:0000:0000:0003", "2001:DB8:1:2::4",
	}
	var allowed int
	for _, ip := range same {
		if ok, _ := l.allow(ip); ok {
			allowed++
		}
	}
	t.Logf("%d addresses in 2001:db8:1:2::/64: %d allowed, %d limited, %d bucket(s) in table", len(same), allowed, len(same)-allowed, len(l.perIP))
	if allowed != 3 || len(l.perIP) != 1 {
		t.Errorf("same /64: allowed=%d (want 3) buckets=%d (want 1)", allowed, len(l.perIP))
	}
	// Rotating 200 more addresses in the /64 never gets a fresh bucket.
	for i := 0; i < 200; i++ {
		if ok, _ := l.allow("2001:db8:1:2:0:0:" + strconv.FormatInt(int64(i), 16) + ":1"); ok {
			t.Fatalf("rotation %d within the /64 got a fresh allowance", i)
		}
	}
	if len(l.perIP) != 1 {
		t.Errorf("rotation grew the table to %d", len(l.perIP))
	}
	// A different /64 (differs in the 4th hextet), and a v4 address, are independent.
	if ok, _ := l.allow("2001:db8:1:3::1"); !ok {
		t.Error("a different /64 shared the exhausted bucket")
	}
	if ok, _ := l.allow("198.51.100.1"); !ok {
		t.Error("an IPv4 address shared the exhausted bucket")
	}
	if len(l.perIP) != 3 {
		t.Errorf("buckets = %d, want 3 (two /64s + one v4)", len(l.perIP))
	}

	// The starvation case: one /64 can burn at most ONE per-IP allowance of the
	// global ceiling, leaving the rest for a legitimate client.
	l = mk(5, 20)
	for i := 0; i < 500; i++ {
		l.allow("2001:db8:aaaa:bbbb:" + strconv.FormatInt(int64(i), 16) + "::1")
	}
	if ok, _ := l.allow("2001:db8:cccc:dddd::1"); !ok {
		t.Error("a /64 rotator starved the global ceiling for a legitimate client")
	}

	// IPv4: still per full address, no grouping by /24.
	l = mk(2, 1000)
	for _, ip := range []string{"198.51.100.1", "198.51.100.2", "198.51.100.3"} {
		if ok, _ := l.allow(ip); !ok {
			t.Errorf("%s limited on first use", ip)
		}
	}
	if len(l.perIP) != 3 {
		t.Errorf("IPv4 buckets = %d, want 3 (one per address)", len(l.perIP))
	}
	l.allow("198.51.100.1")
	if ok, _ := l.allow("198.51.100.1"); ok {
		t.Error("IPv4 address not limited on its own bucket")
	}
	// IPv4-mapped IPv6 is the same caller as its IPv4 form, not a /64 group.
	l = mk(1, 1000)
	l.allow("203.0.113.9")
	if ok, _ := l.allow("::ffff:203.0.113.9"); ok {
		t.Error("IPv4-mapped form got a separate bucket")
	}
	if ok, _ := l.allow("::ffff:203.0.113.10"); !ok {
		t.Error("distinct IPv4-mapped address was folded into another")
	}
	// A zone suffix does not mint a new bucket.
	l = mk(1, 1000)
	l.allow("fe80::1%eth0")
	if ok, _ := l.allow("fe80::2%eth1"); ok {
		t.Error("zone suffix split a /64")
	}
}

func TestOAuthLimiterKey(t *testing.T) {
	for in, want := range map[string]string{
		"198.51.100.7":                  "198.51.100.7",
		"2001:db8:1:2:aaaa:bbbb:cccc:d": "2001:db8:1:2::/64",
		"2001:db8:1:2::":                "2001:db8:1:2::/64",
		"::ffff:198.51.100.7":           "198.51.100.7",
		"not-an-ip":                     "not-an-ip",
		"":                              "",
		strings.Repeat("x", 100):        strings.Repeat("x", 64),
	} {
		if got := oauthLimiterKey(in); got != want {
			t.Errorf("oauthLimiterKey(%q) = %q, want %q", in, got, want)
		}
	}
}
