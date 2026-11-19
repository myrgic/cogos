package engine

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testCIMDHost     = "claude.ai"
	testClientID     = "https://claude.ai/oauth/mcp-oauth-client-metadata"
	testRedirectURI  = "https://claude.ai/api/mcp/auth_callback"
	testResource     = "https://stage.example.test/mcp"
	testVerifier     = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	testPollWaitFast = 150 * time.Millisecond
)

// fakeCIMDHost stands in for claude.ai: a TLS server with a real certificate
// for the host name, reached through the fetcher's own dial hook, so the
// production validation path (allowlist, resolve-and-vet, TLS verify against
// the name, no redirects, caps) is what runs.
type fakeCIMDHost struct {
	srv     *httptest.Server
	mu      sync.Mutex
	handler http.HandlerFunc
	hits    int
}

func newFakeCIMDHost(t *testing.T) *fakeCIMDHost {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: testCIMDHost},
		DNSNames:              []string{testCIMDHost},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCIMDHost{}
	f.handler = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"client_id":%q,"client_name":"Claude","client_uri":"https://claude.ai","redirect_uris":[%q],`+
			`"grant_types":["authorization_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none"}`,
			testClientID, testRedirectURI)
	}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.hits++
		h := f.handler
		f.mu.Unlock()
		h(w, r)
	}))
	f.srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	f.srv.StartTLS()
	t.Cleanup(f.srv.Close)

	pool := x509.NewCertPool()
	cert, _ := x509.ParseCertificate(der)
	pool.AddCert(cert)
	_ = pool
	f.srv.Config.ErrorLog = nil
	return f
}

func (f *fakeCIMDHost) setHandler(h http.HandlerFunc) { f.mu.Lock(); f.handler = h; f.mu.Unlock() }
func (f *fakeCIMDHost) hitCount() int                 { f.mu.Lock(); defer f.mu.Unlock(); return f.hits }

// wire points fetcher at f. lookup returns a PUBLIC address (so the vetting
// passes) and dial ignores it, going to the test server.
func (f *fakeCIMDHost) wire(t *testing.T, fe *oauthClientMetadataFetcher) {
	t.Helper()
	cert := f.srv.Certificate()
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	fe.lookup = func(ctx context.Context, host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}
	fe.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, f.srv.Listener.Addr().String())
	}
	fe.tlsConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

type oauthAuthzEnv struct {
	srv   *Server
	ts    *httptest.Server
	cimd  *fakeCIMDHost
	state *oauthASState
	mu    sync.Mutex
	// notices received by the fake stage
	notices []oauthConsentNotice
	// notifyErr makes the fake stage unreachable.
	notifyErr error
	// deviceLink is the live device link behind testDeviceHash.
	deviceLink *ShareLink
}

// pairDevice mirrors a paired device (by credential hash) into the share-link
// store the way the stage's keys_sync does.
func (e *oauthAuthzEnv) pairDevice(t *testing.T, hash string) *ShareLink {
	t.Helper()
	l, err := e.srv.shareLinks.Import(ShareLinkImport{
		Kind: ShareLinkKindDevice, Surface: "stage/device/" + hash[:8], Actor: oauthActor, Role: "owner",
		Scope: []string{"stage:device"}, IntegrityHash: hash,
	}, "")
	if err != nil {
		t.Fatalf("pair device: %v", err)
	}
	return l
}

func newOAuthAuthzEnv(t *testing.T) *oauthAuthzEnv {
	t.Helper()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.WriteRouteGrantAuthDisabled = false
	// Routes register inside NewServer, so oauth must be on before it runs.
	cfg.OAuth = OAuthConfig{Enabled: true, Issuer: "https://stage.example.test", Resource: testResource}
	srv := NewServer(cfg, makeNucleus("Test", "unit-tester"), NewProcess(cfg, makeNucleus("Test", "unit-tester")))
	env := &oauthAuthzEnv{srv: srv, cimd: newFakeCIMDHost(t)}
	// The stage mirrors its paired devices into the share-link store; the D3
	// check (ApproverDeviceLive) reads that mirror at exchange, refresh and
	// introspection, so the test device is live unless a test revokes it.
	env.deviceLink = env.pairDevice(t, testDeviceHash)
	env.state = srv.asState()
	env.state.pollWait = testPollWaitFast
	env.cimd.wire(t, env.state.cimd)
	env.state.notify = func(ctx context.Context, target string, p oauthConsentNotice) error {
		env.mu.Lock()
		defer env.mu.Unlock()
		if env.notifyErr != nil {
			return env.notifyErr
		}
		env.notices = append(env.notices, p)
		return nil
	}
	env.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(env.ts.Close)
	return env
}

// validQuery is the query claude.ai would send.
func validQuery() url.Values {
	return url.Values{
		"response_type":         {"code"},
		"client_id":             {testClientID},
		"redirect_uri":          {testRedirectURI},
		"code_challenge":        {OAuthPKCEChallengeS256(testVerifier)},
		"code_challenge_method": {"S256"},
		"resource":              {testResource},
		"scope":                 {"mcp:connector"},
		"state":                 {"st-123"},
	}
}

func (e *oauthAuthzEnv) authorize(t *testing.T, q url.Values, accept string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest("GET", e.ts.URL+"/oauth/authorize?"+q.Encode(), nil)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

type authzResult struct {
	RequestID  string `json:"request_id"`
	UserCode   string `json:"user_code"`
	PollSecret string `json:"poll_secret"`
	StatusPath string `json:"status_path"`
	Status     string `json:"status"`
}

func (e *oauthAuthzEnv) startFlow(t *testing.T) authzResult {
	t.Helper()
	code, body, _ := e.authorize(t, validQuery(), "application/json")
	if code != 200 {
		t.Fatalf("authorize = %d: %s", code, body)
	}
	var res authzResult
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("authorize body: %v: %s", err, body)
	}
	return res
}

func (e *oauthAuthzEnv) post(t *testing.T, path, token string, body any) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", e.ts.URL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(GrantHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (e *oauthAuthzEnv) status(t *testing.T, res authzResult, secret string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", e.ts.URL+res.StatusPath, nil)
	req.Header.Set(oauthPollSecretHeader, secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (e *oauthAuthzEnv) consentToken(t *testing.T) string {
	return mintScopedTestGrant(t, e.srv, "stage-studio-test", ScopeOAuthConsent)
}

var testDeviceHash = sha256Hex("paired-device-cookie")

// ─── /authorize validation ───────────────────────────────────────────────────

func TestOAuthAuthorize_ValidCreatesPendingRequest(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	if res.Status != "pending" || !strings.HasPrefix(res.RequestID, "oar-") || len(res.UserCode) != 6 || res.PollSecret == "" {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, c := range res.UserCode {
		if !strings.ContainsRune(pairingCodeAlphabet, c) {
			t.Errorf("user code %q has char outside the pairing alphabet", res.UserCode)
		}
	}
	if len(env.notices) != 1 || env.notices[0].RequestID != res.RequestID || env.notices[0].UserCode != res.UserCode {
		t.Fatalf("stage notice = %+v", env.notices)
	}
	n := env.notices[0]
	if n.ClientName != "Claude" || n.Resource != testResource || len(n.Scope) != 1 || n.Scope[0] != "mcp:connector" {
		t.Errorf("notice content: %+v", n)
	}
	raw, _ := json.Marshal(n)
	if strings.Contains(string(raw), res.PollSecret) {
		t.Error("the poll secret leaked into the stage notice")
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("authorize must not create a grant: consent is out-of-band")
	}
}

func TestOAuthAuthorize_HTMLPageTellsUserToApproveOnPairedDevice(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, body, hdr := env.authorize(t, validQuery(), "text/html")
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("authorize = %d %s", code, hdr.Get("Content-Type"))
	}
	env.mu.Lock()
	uc := env.notices[0].UserCode
	env.mu.Unlock()
	for _, want := range []string{"Approve on your paired device", uc, "/oauth/authorize/status/"} {
		if !strings.Contains(body, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if hdr.Get("Cache-Control") != "no-store" || !strings.Contains(hdr.Get("Content-Security-Policy"), "nonce-") {
		t.Errorf("page headers: %v", hdr)
	}
}

func TestOAuthAuthorize_Rejections(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	cases := []struct {
		name   string
		mutate func(q url.Values)
		want   string
	}{
		{"bad redirect_uri", func(q url.Values) { q.Set("redirect_uri", "https://evil.example/cb") }, "invalid_request"},
		{"redirect_uri prefix trick", func(q url.Values) { q.Set("redirect_uri", testRedirectURI+"/../x") }, "invalid_request"},
		{"redirect_uri trailing slash", func(q url.Values) { q.Set("redirect_uri", testRedirectURI+"/") }, "invalid_request"},
		{"missing redirect_uri", func(q url.Values) { q.Del("redirect_uri") }, "invalid_request"},
		{"missing PKCE challenge", func(q url.Values) { q.Del("code_challenge") }, "invalid_request"},
		{"malformed PKCE challenge", func(q url.Values) { q.Set("code_challenge", "short") }, "invalid_request"},
		{"plain PKCE", func(q url.Values) { q.Set("code_challenge_method", "plain") }, "invalid_request"},
		{"missing PKCE method", func(q url.Values) { q.Del("code_challenge_method") }, "invalid_request"},
		{"wrong scope", func(q url.Values) { q.Set("scope", "admin") }, "invalid_scope"},
		{"wrong resource", func(q url.Values) { q.Set("resource", "https://stage.example.test/other") }, "invalid_target"},
		{"missing resource", func(q url.Values) { q.Del("resource") }, "invalid_target"},
		{"token response_type", func(q url.Values) { q.Set("response_type", "token") }, "unsupported_response_type"},
		{"missing client_id", func(q url.Values) { q.Del("client_id") }, "invalid_request"},
		{"duplicated parameter", func(q url.Values) { q.Add("redirect_uri", "https://evil.example/cb") }, "invalid_request"},
		{"state with CRLF", func(q url.Values) { q.Set("state", "a\r\nb") }, "invalid_request"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := validQuery()
			c.mutate(q)
			code, body, _ := env.authorize(t, q, "application/json")
			if code < 400 || code >= 500 {
				t.Fatalf("status = %d, want 4xx: %s", code, body)
			}
			var e map[string]string
			_ = json.Unmarshal([]byte(body), &e)
			if e["error"] != c.want {
				t.Errorf("error = %q, want %q (%s)", e["error"], c.want, body)
			}
		})
	}
	if len(env.notices) != 0 {
		t.Errorf("a rejected request reached the stage: %+v", env.notices)
	}
}

func TestOAuthAuthorize_RejectionsNeverRedirect(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	q := validQuery()
	q.Set("code_challenge_method", "plain")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(env.ts.URL + "/oauth/authorize?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 3 || resp.Header.Get("Location") != "" {
		t.Fatalf("error response redirected: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestOAuthAuthorize_CheapChecksPrecedeOutboundFetch(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	q := validQuery()
	q.Set("redirect_uri", "https://evil.example/cb")
	env.authorize(t, q, "application/json")
	if env.cimd.hitCount() != 0 {
		t.Errorf("the CIMD host was contacted for a request that fails local checks (%d hits)", env.cimd.hitCount())
	}
}

// ─── SSRF guard ──────────────────────────────────────────────────────────────

func TestOAuthAuthorize_SSRF_ClientHostNotAllowlisted(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	for _, cid := range []string{
		"https://evil.example/oauth/meta",
		"https://claude.ai.evil.example/oauth/meta",
		"https://evil.example/@claude.ai/x",
		"https://user@claude.ai/oauth/meta",
		"https://claude.ai:8443/oauth/meta",
		"http://claude.ai/oauth/meta",
		"https://127.0.0.1/oauth/meta",
		"https://169.254.169.254/latest/meta-data",
		"https://claude.ai/",
		"https://claude.ai/oauth/meta#frag",
		"not a url",
	} {
		q := validQuery()
		q.Set("client_id", cid)
		code, body, _ := env.authorize(t, q, "application/json")
		if code != 400 || !strings.Contains(body, "invalid_client") {
			t.Errorf("client_id %q = %d %s, want 400 invalid_client", cid, code, body)
		}
	}
	if env.cimd.hitCount() != 0 {
		t.Errorf("an outbound fetch happened for a non-allowlisted host (%d hits)", env.cimd.hitCount())
	}
}

func TestOAuthSSRF_PrivateAddressesRefused(t *testing.T) {
	for _, ip := range []string{
		"127.0.0.1", "10.0.0.5", "172.16.3.4", "192.168.10.191", "169.254.169.254", "100.64.1.1", "0.0.0.0",
		"::1", "fe80::1", "fc00::1", "::ffff:10.0.0.1", "224.0.0.1", "240.0.0.1", "64:ff9b::a00:1",
	} {
		if oauthPublicIP(net.ParseIP(ip)) {
			t.Errorf("%s must not be treated as public", ip)
		}
	}
	for _, ip := range []string{"93.184.216.34", "160.79.104.10", "2606:4700::1111"} {
		if !oauthPublicIP(net.ParseIP(ip)) {
			t.Errorf("%s should be public", ip)
		}
	}
}

func TestOAuthSSRF_AllowlistedHostResolvingToPrivateIPIsRefused(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	for name, ips := range map[string][]net.IP{
		"loopback":        {net.ParseIP("127.0.0.1")},
		"metadata":        {net.ParseIP("169.254.169.254")},
		"rfc1918":         {net.ParseIP("10.1.2.3")},
		"mixed (rebind)":  {net.ParseIP("93.184.216.34"), net.ParseIP("192.168.1.1")},
		"ipv6 loopback":   {net.ParseIP("::1")},
		"no addresses":    {},
		"v4-mapped local": {net.ParseIP("::ffff:127.0.0.1")},
	} {
		ips := ips
		env.state.cimd.lookup = func(ctx context.Context, host string) ([]net.IP, error) { return ips, nil }
		env.state.cimd.cache = map[string]*oauthClientMetadata{}
		code, body, _ := env.authorize(t, validQuery(), "application/json")
		if code != 400 || !strings.Contains(body, "invalid_client") {
			t.Errorf("%s: authorize = %d %s, want 400 invalid_client", name, code, body)
		}
	}
	if env.cimd.hitCount() != 0 {
		t.Errorf("the vetting failed open: the host was dialled %d times", env.cimd.hitCount())
	}
}

func TestOAuthSSRF_RedirectsAreNotFollowed(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	env.cimd.setHandler(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://169.254.169.254/latest/meta-data", http.StatusFound)
	})
	code, body, _ := env.authorize(t, validQuery(), "application/json")
	if code != 400 || !strings.Contains(body, "invalid_client") {
		t.Fatalf("redirecting metadata host = %d %s", code, body)
	}
	if env.cimd.hitCount() != 1 {
		t.Errorf("hits = %d, want exactly 1 (the redirect must not be chased)", env.cimd.hitCount())
	}
}

func TestOAuthSSRF_DocumentValidation(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	cases := map[string]http.HandlerFunc{
		"client_id mismatch": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"client_id":"https://claude.ai/other","redirect_uris":[%q],"token_endpoint_auth_method":"none"}`, testRedirectURI)
		},
		"redirect_uri not in document": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"client_id":%q,"redirect_uris":["https://claude.ai/somewhere/else"],"token_endpoint_auth_method":"none"}`, testClientID)
		},
		"confidential client": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"client_id":%q,"redirect_uris":[%q],"token_endpoint_auth_method":"client_secret_basic"}`, testClientID, testRedirectURI)
		},
		"not json content-type": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprintf(w, `{"client_id":%q,"redirect_uris":[%q],"token_endpoint_auth_method":"none"}`, testClientID, testRedirectURI)
		},
		"oversized body": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"client_id":%q,"pad":"%s"}`, testClientID, strings.Repeat("a", 70<<10))
		},
		"404": func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
		"garbage": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, "<<<")
		},
	}
	for name, h := range cases {
		env.cimd.setHandler(h)
		env.state.cimd.cache = map[string]*oauthClientMetadata{}
		code, body, _ := env.authorize(t, validQuery(), "application/json")
		if code != 400 {
			t.Errorf("%s: authorize = %d %s, want 400", name, code, body)
		}
	}
	if len(env.notices) != 0 {
		t.Errorf("a request with a bad client document reached the stage")
	}
}

func TestOAuthSSRF_FetchHasTimeout(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	release := make(chan struct{})
	env.cimd.setHandler(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		case <-time.After(20 * time.Second):
		}
	})
	defer close(release)
	start := time.Now()
	code, _, _ := env.authorize(t, validQuery(), "application/json")
	if code != 400 {
		t.Fatalf("hung metadata host = %d, want 400", code)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Errorf("took %v; the fetch timeout is 5s", d)
	}
}

func TestOAuthCIMD_CacheHonoursAllowlistAndTTL(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	env.startFlow(t)
	env.startFlow(t)
	if h := env.cimd.hitCount(); h != 1 {
		t.Errorf("hits = %d, want 1 (second request served from cache)", h)
	}
	// A host removed from the allowlist stops resolving even when cached.
	env.srv.cfg.OAuth.AllowedClientHosts = []string{"other.example"}
	code, _, _ := env.authorize(t, validQuery(), "application/json")
	if code != 400 {
		t.Errorf("de-listed host still accepted from cache: %d", code)
	}
}

// ─── stage notify ────────────────────────────────────────────────────────────

func TestOAuthAuthorize_StageUnreachableDropsRequest(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	env.notifyErr = fmt.Errorf("connection refused")
	code, body, _ := env.authorize(t, validQuery(), "application/json")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("authorize = %d %s, want 503", code, body)
	}
	if !strings.Contains(body, "stage unreachable") {
		t.Errorf("body: %s", body)
	}
	env.state.mu.Lock()
	n := len(env.state.reqs)
	env.state.mu.Unlock()
	if n != 0 {
		t.Errorf("%d request(s) left behind after a failed notify", n)
	}
}

func TestOAuthNotifyStage_RealHTTPPost(t *testing.T) {
	var got oauthConsentNotice
	var ct string
	stage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(200)
	}))
	defer stage.Close()
	st := newOAuthASState()
	want := oauthConsentNotice{RequestID: "oar-1", UserCode: "BCDFGH", ClientName: "Claude"}
	if err := st.notifyStage(context.Background(), stage.URL+"/local/oauth-consent", want); err != nil {
		t.Fatal(err)
	}
	if got.RequestID != "oar-1" || got.UserCode != "BCDFGH" || ct != "application/json" {
		t.Errorf("stage received %+v (%s)", got, ct)
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer bad.Close()
	if err := st.notifyStage(context.Background(), bad.URL, want); err == nil {
		t.Error("a 500 from the stage must be an error")
	}
	if err := st.notifyStage(context.Background(), "http://127.0.0.1:1/x", want); err == nil {
		t.Error("an unreachable stage must be an error")
	}
}

func TestOAuthAuthorize_PendingCapAndRateLimit(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	env.state.limiter = newGrantMintLimiter(1000, time.Minute)
	for i := 0; i < oauthMaxPending; i++ {
		env.startFlow(t)
	}
	code, body, _ := env.authorize(t, validQuery(), "application/json")
	if code != http.StatusTooManyRequests {
		t.Fatalf("over the cap = %d %s, want 429", code, body)
	}

	env2 := newOAuthAuthzEnv(t)
	env2.state.limiter = newGrantMintLimiter(2, time.Minute)
	env2.startFlow(t)
	env2.startFlow(t)
	if code, _, _ := env2.authorize(t, validQuery(), "application/json"); code != http.StatusTooManyRequests {
		t.Errorf("over the rate limit = %d, want 429", code)
	}
}

// ─── approve / deny / status ─────────────────────────────────────────────────

func TestOAuthApprove_MintsCodeBindsDeviceAndStatusReturnsRedirectOnce(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)

	// Before approval: pending.
	if code, body := env.status(t, res, res.PollSecret); code != http.StatusAccepted || !strings.Contains(body, "pending") {
		t.Fatalf("pre-approval status = %d %s", code, body)
	}

	tok := env.consentToken(t)
	code, body := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", tok,
		map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash})
	if code != 200 {
		t.Fatalf("approve = %d %s", code, body)
	}
	if strings.Contains(body, OAuthAuthCodePrefix) {
		t.Error("the raw authorization code leaked into the approve response")
	}
	var ap struct {
		GrantID        string `json:"grant_id"`
		ApproverDevice string `json:"approver_device"`
	}
	_ = json.Unmarshal([]byte(body), &ap)
	if ap.ApproverDevice != testDeviceHash {
		t.Errorf("approver_device = %q", ap.ApproverDevice)
	}

	// The grant exists, is bound to the device, scope mcp:connector, no tokens yet.
	g, ok := env.srv.oauth.Grant(ap.GrantID)
	if !ok {
		t.Fatal("grant not in the store")
	}
	if !g.ApprovedByDevice(testDeviceHash) || g.Consent.ApprovedVia != "paired-device" || g.Consent.RequestID != res.RequestID {
		t.Errorf("grant consent: %+v", g.Consent)
	}
	if !g.HasScope("mcp:connector") || len(g.Scope) != 1 || g.Resource != testResource || g.ClientID != testClientID {
		t.Errorf("grant: %+v", g)
	}

	// Status returns the redirect with the code + state, once.
	code, body = env.status(t, res, res.PollSecret)
	if code != 200 {
		t.Fatalf("post-approval status = %d %s", code, body)
	}
	var st struct {
		Status   string `json:"status"`
		Redirect string `json:"redirect"`
	}
	_ = json.Unmarshal([]byte(body), &st)
	u, err := url.Parse(st.Redirect)
	if err != nil || st.Status != "approved" {
		t.Fatalf("status body: %s", body)
	}
	if u.Scheme+"://"+u.Host+u.Path != testRedirectURI {
		t.Errorf("redirect target = %s", st.Redirect)
	}
	rawCode := u.Query().Get("code")
	if !strings.HasPrefix(rawCode, OAuthAuthCodePrefix) || u.Query().Get("state") != "st-123" {
		t.Errorf("redirect query: %s", u.RawQuery)
	}
	// The code's hash is what is bound into the grant (Consent.CodeHash).
	if g.Consent.CodeHash != sha256Hex(rawCode) {
		t.Errorf("Consent.CodeHash %q does not hash the delivered code", g.Consent.CodeHash)
	}
	// Collected once.
	if code, _ := env.status(t, res, res.PollSecret); code != http.StatusGone {
		t.Errorf("second collect = %d, want 410", code)
	}

	// K4 hook: the code redeems exactly once; replay is flagged.
	rc, err := env.state.redeemCode(rawCode, nil)
	if err != nil || rc.GrantID != ap.GrantID || rc.CodeChallenge != OAuthPKCEChallengeS256(testVerifier) ||
		!OAuthPKCEVerifyS256(rc.CodeChallenge, testVerifier) {
		t.Fatalf("redeem: %+v %v", rc, err)
	}
	if rc2, err := env.state.redeemCode(rawCode, nil); err != ErrOAuthCodeReplay || rc2 == nil || rc2.GrantID != ap.GrantID {
		t.Errorf("replay = %+v %v", rc2, err)
	}
	if _, err := env.state.redeemCode("cgac_nope", nil); err != ErrOAuthCodeUnknown {
		t.Errorf("unknown code = %v", err)
	}
}

func TestOAuthApprove_ConnectorStyleCallerRefused(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	path := "/v1/oauth/requests/" + res.RequestID + "/approve"
	body := map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash}

	// 1. no credential at all.
	if code, b := env.post(t, path, "", body); code != http.StatusUnauthorized {
		t.Errorf("no grant = %d %s, want 401", code, b)
	}
	// 2. a live kernel grant WITHOUT oauth:consent (the shape of anything a
	//    connector could hold).
	for _, scopes := range [][]string{{ScopeInference}, {ScopeWrite}, {ScopeOAuthIntrospect}, {ScopeManagedSessions}} {
		tok := mintScopedTestGrant(t, env.srv, "connector-"+strings.Join(scopes, ","), scopes...)
		if code, b := env.post(t, path, tok, body); code != http.StatusForbidden || !strings.Contains(b, "insufficient_scope") {
			t.Errorf("grant %v approve = %d %s, want 403 insufficient_scope", scopes, code, b)
		}
		if code, _ := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/deny", tok, nil); code != http.StatusForbidden {
			t.Errorf("grant %v deny = %d, want 403", scopes, code)
		}
	}
	// 3. the poll secret is not a credential for approve.
	if code, _ := env.post(t, path, res.PollSecret, body); code != http.StatusUnauthorized {
		t.Errorf("poll secret as grant = %d, want 401", code)
	}
	// 4. an unknown token.
	if code, _ := env.post(t, path, "cgat_not-a-grant", body); code != http.StatusUnauthorized {
		t.Errorf("garbage token = %d, want 401", code)
	}
	// Nothing was approved by any of that.
	if env.srv.oauth.Len() != 0 {
		t.Error("a refused approve created a grant")
	}
	if code, _ := env.status(t, res, res.PollSecret); code != http.StatusAccepted {
		t.Errorf("request no longer pending after refused approves: %d", code)
	}
}

func TestOAuthApprove_HandlerEnforcesScopeWithoutTheGate(t *testing.T) {
	// A handler wired onto a bare mux (no grant middleware) must still refuse.
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/oauth/requests/"+res.RequestID+"/approve", strings.NewReader(`{"device_hash":"`+testDeviceHash+`"}`))
	req.SetPathValue("id", res.RequestID)
	env.srv.handleOAuthRequestApprove(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("ungated handler call = %d, want 401", rec.Code)
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("ungated approve created a grant")
	}
}

func TestOAuthApprove_RequiresDeviceBinding(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	tok := env.consentToken(t)
	path := "/v1/oauth/requests/" + res.RequestID + "/approve"
	for name, body := range map[string]map[string]string{
		"no device":       {"approved_via": "paired-device"},
		"short device":    {"device_hash": "abc"},
		"uppercase":       {"device_hash": strings.ToUpper(testDeviceHash)},
		"wrong channel":   {"approved_via": "api", "device_hash": testDeviceHash},
		"connector claim": {"approved_via": "claude-web", "device_hash": testDeviceHash},
	} {
		if code, b := env.post(t, path, tok, body); code != http.StatusBadRequest {
			t.Errorf("%s = %d %s, want 400", name, code, b)
		}
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("an invalid approve created a grant")
	}
}

func TestOAuthApprove_UnknownDoubleAndExpired(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	tok := env.consentToken(t)
	body := map[string]string{"device_hash": testDeviceHash}

	if code, _ := env.post(t, "/v1/oauth/requests/oar-nope/approve", tok, body); code != http.StatusNotFound {
		t.Errorf("unknown = %d, want 404", code)
	}

	res := env.startFlow(t)
	path := "/v1/oauth/requests/" + res.RequestID + "/approve"
	if code, b := env.post(t, path, tok, body); code != 200 {
		t.Fatalf("first approve = %d %s", code, b)
	}
	if code, _ := env.post(t, path, tok, body); code != http.StatusConflict {
		t.Errorf("second approve = %d, want 409", code)
	}
	if env.srv.oauth.Len() != 1 {
		t.Errorf("grants = %d, want 1", env.srv.oauth.Len())
	}

	// Expired before approval.
	res2 := env.startFlow(t)
	clock := time.Now().UTC()
	env.state.now = func() time.Time { return clock.Add(oauthRequestLifetime + time.Second) }
	if code, _ := env.post(t, "/v1/oauth/requests/"+res2.RequestID+"/approve", tok, body); code != http.StatusGone {
		t.Errorf("approve after expiry = %d, want 410", code)
	}
}

func TestOAuthDeny_StatusReturnsAccessDenied(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	tok := env.consentToken(t)
	if code, b := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/deny", tok, nil); code != 200 {
		t.Fatalf("deny = %d %s", code, b)
	}
	code, body := env.status(t, res, res.PollSecret)
	if code != 200 {
		t.Fatalf("status = %d %s", code, body)
	}
	var st struct{ Status, Redirect string }
	_ = json.Unmarshal([]byte(body), &st)
	u, _ := url.Parse(st.Redirect)
	if st.Status != "denied" || u.Query().Get("error") != "access_denied" || u.Query().Get("state") != "st-123" || u.Query().Get("code") != "" {
		t.Errorf("denied status: %s", body)
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("deny created a grant")
	}
	if code, _ := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", tok, map[string]string{"device_hash": testDeviceHash}); code != http.StatusConflict {
		t.Errorf("approve after deny = %d, want 409", code)
	}
}

func TestOAuthStatus_ExpiredAndWrongSecret(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)

	for _, secret := range []string{"", "wrong", res.PollSecret + "x"} {
		if code, _ := env.status(t, res, secret); code != http.StatusNotFound {
			t.Errorf("secret %q = %d, want 404", secret, code)
		}
	}
	bogus := res
	bogus.StatusPath = "/oauth/authorize/status/oar-nope"
	if code, _ := env.status(t, bogus, res.PollSecret); code != http.StatusNotFound {
		t.Errorf("unknown id = %d, want 404", code)
	}

	clock := time.Now().UTC()
	env.state.now = func() time.Time { return clock.Add(oauthRequestLifetime + time.Second) }
	code, body := env.status(t, res, res.PollSecret)
	if code != 200 {
		t.Fatalf("expired status = %d %s", code, body)
	}
	var st struct{ Status, Redirect string }
	_ = json.Unmarshal([]byte(body), &st)
	u, _ := url.Parse(st.Redirect)
	if st.Status != "expired" || u.Query().Get("error") != "access_denied" || u.Query().Get("code") != "" {
		t.Errorf("expired status: %s", body)
	}
}

func TestOAuthStatus_LongPollWakesOnApprove(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	env.state.pollWait = 10 * time.Second
	res := env.startFlow(t)
	tok := env.consentToken(t)

	type out struct {
		code int
		body string
	}
	ch := make(chan out, 1)
	go func() { c, b := env.status(t, res, res.PollSecret); ch <- out{c, b} }()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", tok, map[string]string{"device_hash": testDeviceHash})
	select {
	case o := <-ch:
		if o.code != 200 || !strings.Contains(o.body, "code=") {
			t.Errorf("poll result = %d %s", o.code, o.body)
		}
		if time.Since(start) > 3*time.Second {
			t.Error("the poll did not wake promptly")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll never returned")
	}
}

func TestOAuthApprove_UnredeemedCodeExpiryRevokesGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	res := env.startFlow(t)
	tok := env.consentToken(t)
	code, b := env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", tok, map[string]string{"device_hash": testDeviceHash})
	if code != 200 {
		t.Fatalf("approve = %d %s", code, b)
	}
	if env.srv.oauth.Len() != 1 {
		t.Fatal("no grant")
	}
	clock := time.Now().UTC()
	env.state.now = func() time.Time { return clock.Add(oauthCodeLifetime + time.Second) }
	// Any later touch prunes; the browser that never collected sees a denial.
	code, body := env.status(t, res, res.PollSecret)
	if code != 200 || !strings.Contains(body, "access_denied") || strings.Contains(body, "code=cgac") {
		t.Errorf("late status = %d %s", code, body)
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("the grant behind an expired, unredeemed code was not revoked")
	}
}

func TestOAuthRoutes_K3NoLongerPlaceholders(t *testing.T) {
	_, mux := newOAuthTestServer(true)
	for _, c := range []struct{ method, path string }{
		{"GET", "/oauth/authorize"},
		{"GET", "/oauth/authorize/status/abc"},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if rec.Code == http.StatusNotImplemented {
			t.Errorf("%s %s is still a 501 placeholder", c.method, c.path)
		}
	}
}

// TestOAuthLiveRoundtripProof walks the whole K3 flow over real HTTP and logs
// the bytes of every hop (run with -v). Assertions keep it honest.
func TestOAuthLiveRoundtripProof(t *testing.T) {
	env := newOAuthAuthzEnv(t)

	q := validQuery()
	code, body, hdr := env.authorize(t, q, "application/json")
	t.Logf("1. GET /oauth/authorize?...  -> %d  Cache-Control=%s\n%s", code, hdr.Get("Cache-Control"), body)
	var res authzResult
	if code != 200 || json.Unmarshal([]byte(body), &res) != nil {
		t.Fatalf("authorize failed")
	}
	env.mu.Lock()
	notice, _ := json.Marshal(env.notices[0])
	env.mu.Unlock()
	t.Logf("2. kernel -> stage POST /local/oauth-consent body:\n%s", notice)

	c, b := env.status(t, res, res.PollSecret)
	t.Logf("3. GET %s (before consent) -> %d %s", res.StatusPath, c, strings.TrimSpace(b))

	// Connector-style approve: a live kernel grant WITHOUT oauth:consent.
	conn := mintScopedTestGrant(t, env.srv, "claude-web-connector", ScopeInference, ScopeWrite)
	c, b = env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", conn,
		map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash})
	t.Logf("4. CONNECTOR approve (no oauth:consent) -> %d %s", c, strings.TrimSpace(b))
	if c != http.StatusForbidden {
		t.Fatalf("connector approve was not refused: %d", c)
	}
	c, b = env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", "", map[string]string{"device_hash": testDeviceHash})
	t.Logf("4b. approve with NO credential -> %d %s", c, strings.TrimSpace(b))

	studio := env.consentToken(t)
	c, b = env.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", studio,
		map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash})
	t.Logf("5. STAGE approve (oauth:consent) -> %d %s", c, strings.TrimSpace(b))
	if c != 200 {
		t.Fatalf("stage approve failed")
	}

	c, b = env.status(t, res, res.PollSecret)
	t.Logf("6. GET %s (after consent) -> %d %s", res.StatusPath, c, strings.TrimSpace(b))
	if c != 200 || !strings.Contains(b, "code=cgac_") {
		t.Fatalf("no redirect after approve")
	}
	c, b = env.status(t, res, res.PollSecret)
	t.Logf("7. GET %s again -> %d %s", res.StatusPath, c, strings.TrimSpace(b))

	var line []byte
	if raw, err := os.ReadFile(env.srv.cfg.WorkspaceRoot + "/.cog/ledger/identity-oauth/events.jsonl"); err == nil {
		line = raw
	}
	t.Logf("8. ledger identity-oauth/events.jsonl:\n%s", line)
	if strings.Contains(string(line), "cgac_") {
		t.Error("a raw authorization code reached the ledger")
	}
}
