package engine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// tokenPost posts a form to /oauth/token on the test server and returns the
// status and body.
func (e *oauthAuthzEnv) tokenPost(t *testing.T, form url.Values) (int, string) {
	t.Helper()
	return e.formPost(t, "/oauth/token", form)
}

func (e *oauthAuthzEnv) formPost(t *testing.T, path string, form url.Values) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", e.ts.URL+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// approvedCode runs /authorize then approve as the stage and returns the raw
// authorization code (from the browser poll) and the request id.
func (e *oauthAuthzEnv) approvedCode(t *testing.T) (code string, res authzResult) {
	t.Helper()
	res = e.startFlow(t)
	tok := e.consentToken(t)
	if c, b := e.post(t, "/v1/oauth/requests/"+res.RequestID+"/approve", tok,
		map[string]string{"approved_via": "paired-device", "device_hash": testDeviceHash}); c != 200 {
		t.Fatalf("approve = %d %s", c, b)
	}
	c, b := e.status(t, res, res.PollSecret)
	if c != 200 {
		t.Fatalf("status = %d %s", c, b)
	}
	var out struct {
		Redirect string `json:"redirect"`
	}
	if err := json.Unmarshal([]byte(b), &out); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(out.Redirect)
	if err != nil {
		t.Fatal(err)
	}
	code = u.Query().Get("code")
	if !strings.HasPrefix(code, OAuthAuthCodePrefix) {
		t.Fatalf("no code in redirect %q", out.Redirect)
	}
	return code, res
}

func exchangeForm(code string) url.Values {
	return url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"code_verifier": {testVerifier},
		"client_id":     {testClientID},
		"redirect_uri":  {testRedirectURI},
		"resource":      {testResource},
	}
}

// tokenFields decodes a 200 token response.
func tokenFields(t *testing.T, body string) oauthTokenBody {
	t.Helper()
	var tb oauthTokenBody
	if err := json.Unmarshal([]byte(body), &tb); err != nil {
		t.Fatalf("token body: %v: %s", err, body)
	}
	return tb
}

// errorType returns the error kind from either body shape the endpoints emit:
// the RFC 6749 flat {"error":"invalid_grant"} of /oauth/token, or the grant
// gate's nested {"error":{"type":"oauth_token_not_grant"}}.
func errorType(t *testing.T, body string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("error body not JSON: %v: %s", err, body)
	}
	switch e := m["error"].(type) {
	case string:
		return e
	case map[string]any:
		s, _ := e["type"].(string)
		return s
	}
	return ""
}

func TestOAuthToken_AuthorizationCodeHappyPath(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)

	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 200 {
		t.Fatalf("token = %d %s", c, b)
	}
	tb := tokenFields(t, b)
	if tb.TokenType != "Bearer" || tb.ExpiresIn <= 0 {
		t.Errorf("token_type=%q expires_in=%d", tb.TokenType, tb.ExpiresIn)
	}
	if !strings.HasPrefix(tb.AccessToken, OAuthAccessTokenPrefix) || !strings.HasPrefix(tb.RefreshToken, OAuthRefreshTokenPrefix) {
		t.Errorf("token prefixes wrong: access=%q refresh=%q", tb.AccessToken, tb.RefreshToken)
	}
	if tb.Scope != "mcp:connector" {
		t.Errorf("scope = %q", tb.Scope)
	}
}

func TestOAuthToken_CodeIsSingleUseAndReplayRevokesGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)

	if c, b := env.tokenPost(t, exchangeForm(code)); c != 200 {
		t.Fatalf("first exchange = %d %s", c, b)
	}
	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 400 || errorType(t, b) != "invalid_grant" {
		t.Fatalf("replay = %d %s, want 400 invalid_grant", c, b)
	}
	// The first exchange's tokens must now be dead too: its grant was revoked.
	rec := env.introspect(t, "")
	if rec.Active {
		t.Errorf("empty token introspected active")
	}
}

func TestOAuthToken_RejectsWrongVerifierClientRedirectResource(t *testing.T) {
	cases := []struct {
		name string
		mut  func(url.Values)
	}{
		{"wrong verifier", func(f url.Values) { f.Set("code_verifier", strings.Repeat("A", 43)) }},
		{"malformed verifier", func(f url.Values) { f.Set("code_verifier", "short") }},
		{"wrong client_id", func(f url.Values) { f.Set("client_id", "https://claude.ai/other") }},
		{"wrong redirect_uri", func(f url.Values) { f.Set("redirect_uri", "https://claude.ai/evil") }},
		{"wrong resource", func(f url.Values) { f.Set("resource", "https://other.example/mcp") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newOAuthAuthzEnv(t)
			code, _ := env.approvedCode(t)
			f := exchangeForm(code)
			c.mut(f)
			st, b := env.tokenPost(t, f)
			if st != 400 || errorType(t, b) != "invalid_grant" {
				t.Fatalf("%s = %d %s, want 400 invalid_grant", c.name, st, b)
			}
			// The code was consumed and the grant revoked: the honest client's
			// correct retry must fail too.
			if st2, b2 := env.tokenPost(t, exchangeForm(code)); st2 == 200 {
				t.Fatalf("grant survived a failed exchange: %s", b2)
			}
		})
	}
}

func TestOAuthToken_RefreshRotates(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	c, b2 := env.tokenPost(t, url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {first.RefreshToken},
		"client_id": {testClientID}, "resource": {testResource},
	})
	if c != 200 {
		t.Fatalf("refresh = %d %s", c, b2)
	}
	second := tokenFields(t, b2)
	if second.RefreshToken == first.RefreshToken || second.AccessToken == first.AccessToken {
		t.Fatal("refresh did not rotate both tokens")
	}
}

func TestOAuthToken_RefreshReuseIsInvalidGrantAndKillsGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	refresh := func(rt string) (int, string) {
		return env.tokenPost(t, url.Values{
			"grant_type": {"refresh_token"}, "refresh_token": {rt}, "client_id": {testClientID},
		})
	}
	if c, b := refresh(first.RefreshToken); c != 200 {
		t.Fatalf("first rotate = %d %s", c, b)
	}
	c, b := refresh(first.RefreshToken)
	if c != 400 || errorType(t, b) != "invalid_grant" {
		t.Fatalf("reuse = %d %s, want 400 invalid_grant", c, b)
	}
	// The whole family is dead: the newest refresh token must now fail too,
	// and the access token must introspect inactive.
	if c, b := refresh(first.RefreshToken); c != 400 {
		t.Errorf("replay after reuse = %d %s", c, b)
	}
	if env.introspect(t, first.AccessToken).Active {
		t.Error("access token still active after refresh reuse")
	}
}

func TestOAuthToken_RefreshWithWrongClientDoesNotBurnToken(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	c, b := env.tokenPost(t, url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {first.RefreshToken}, "client_id": {"https://evil.example/x"},
	})
	if c != 400 || errorType(t, b) != "invalid_grant" {
		t.Fatalf("wrong client refresh = %d %s", c, b)
	}
	// The legitimate client's refresh still works: the wrong-client attempt
	// did not consume or mark the token.
	if c, b := env.tokenPost(t, url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {first.RefreshToken}, "client_id": {testClientID},
	}); c != 200 {
		t.Fatalf("legitimate refresh after wrong-client attempt = %d %s", c, b)
	}
}

func TestOAuthToken_ErrorShapes(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	cases := []struct {
		name   string
		form   url.Values
		status int
		kind   string
	}{
		{"unknown grant_type", url.Values{"grant_type": {"password"}}, 400, "unsupported_grant_type"},
		{"missing fields", url.Values{"grant_type": {"authorization_code"}}, 400, "invalid_request"},
		{"garbage refresh", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"nope"}, "client_id": {testClientID}}, 400, "invalid_grant"},
		{"garbage code", url.Values{"grant_type": {"authorization_code"}, "code": {"zzz"}, "code_verifier": {testVerifier}, "client_id": {testClientID}, "redirect_uri": {testRedirectURI}}, 400, "invalid_grant"},
	}
	for _, c := range cases {
		st, b := env.tokenPost(t, c.form)
		if st != c.status || errorType(t, b) != c.kind {
			t.Errorf("%s = %d %s, want %d %s", c.name, st, b, c.status, c.kind)
		}
	}
	// Wrong content type is invalid_request 415, not a 500.
	req, _ := http.NewRequest("POST", env.ts.URL+"/oauth/token", strings.NewReader(`{"grant_type":"refresh_token"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("json body = %d, want 415", resp.StatusCode)
	}
}

// introspectResult is the decoded RFC 7662 response.
type introspectResult struct {
	Active bool   `json:"active"`
	Scope  string `json:"scope"`
	Grant  string `json:"grant_id"`
	Raw    string
}

func (e *oauthAuthzEnv) introspect(t *testing.T, token string) introspectResult {
	t.Helper()
	tok := e.consentToken2(t, ScopeOAuthIntrospect)
	c, b := e.post(t, "/v1/oauth/introspect", tok, map[string]string{"token": token})
	if c != 200 {
		t.Fatalf("introspect = %d %s", c, b)
	}
	var r introspectResult
	_ = json.Unmarshal([]byte(b), &r)
	r.Raw = b
	return r
}

// consentToken2 mints a kernel grant carrying exactly the given scope.
func (e *oauthAuthzEnv) consentToken2(t *testing.T, scope string) string {
	return mintScopedTestGrant(t, e.srv, "stage-introspect-test", scope)
}

func TestOAuthIntrospect_ActiveVersusInactiveNoOracle(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	tb := tokenFields(t, b)

	active := env.introspect(t, tb.AccessToken)
	if !active.Active || active.Scope != "mcp:connector" || active.Grant == "" {
		t.Fatalf("live access token = %s", active.Raw)
	}
	// Refresh tokens are not access tokens: they introspect inactive.
	if env.introspect(t, tb.RefreshToken).Active {
		t.Error("refresh token introspected active")
	}
	// Unknown, malformed and empty tokens all give the same inactive body.
	unknown := env.introspect(t, "cgat_"+strings.Repeat("A", 43))
	garbage := env.introspect(t, "nope")
	if unknown.Active || garbage.Active {
		t.Fatal("unknown token introspected active")
	}
	if strings.TrimSpace(unknown.Raw) != strings.TrimSpace(garbage.Raw) || strings.TrimSpace(unknown.Raw) != `{"active":false}` {
		t.Errorf("inactive bodies differ or leak: %q vs %q", unknown.Raw, garbage.Raw)
	}
}

func TestOAuthIntrospect_NeedsScope(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	// A live grant without oauth:introspect is refused, not answered.
	conn := mintScopedTestGrant(t, env.srv, "claude-web-connector", ScopeInference)
	c, b := env.post(t, "/v1/oauth/introspect", conn, map[string]string{"token": "x"})
	if c != http.StatusForbidden {
		t.Fatalf("introspect with wrong scope = %d %s", c, b)
	}
}

func TestOAuthGrants_ListHasNoHashesAndRevokeKillsGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	tb := tokenFields(t, b)

	admin := mintScopedTestGrant(t, env.srv, "ops-admin-test", ScopeAdmin)
	req, _ := http.NewRequest("GET", env.ts.URL+"/v1/oauth/grants", nil)
	req.Header.Set(GrantHeaderName, admin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("grants list = %d %s", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), "_hash") || strings.Contains(string(raw), "access_hash") ||
		strings.Contains(string(raw), "code_hash") || strings.Contains(string(raw), "approver_device\"") ||
		strings.Contains(string(raw), tb.AccessToken) || strings.Contains(string(raw), tb.RefreshToken) {
		t.Fatalf("grant list leaks hashes or tokens: %s", raw)
	}
	var list struct {
		Grants []oauthGrantView `json:"grants"`
	}
	if err := json.Unmarshal(raw, &list); err != nil || len(list.Grants) != 1 {
		t.Fatalf("grants = %s (err %v)", raw, err)
	}
	gid := list.Grants[0].GrantID

	// Revoke: the grant dies and its access token stops introspecting active.
	c, rb := env.post(t, "/v1/oauth/grants/"+gid+"/revoke", admin, map[string]string{"reason": "test"})
	if c != 200 {
		t.Fatalf("revoke = %d %s", c, rb)
	}
	if env.introspect(t, tb.AccessToken).Active {
		t.Fatal("access token still active after revoke")
	}
	// Revoking again is a clean 404, not a 500.
	if c, _ := env.post(t, "/v1/oauth/grants/"+gid+"/revoke", admin, map[string]string{}); c != 404 {
		t.Errorf("second revoke = %d, want 404", c)
	}
}

func TestOAuthGrants_ListNeedsAdminScope(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	req, _ := http.NewRequest("GET", env.ts.URL+"/v1/oauth/grants", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatal("grant list answered without a grant")
	}
	conn := mintScopedTestGrant(t, env.srv, "claude-web-connector", ScopeOAuthConsent, ScopeOAuthIntrospect)
	req, _ = http.NewRequest("GET", env.ts.URL+"/v1/oauth/grants", nil)
	req.Header.Set(GrantHeaderName, conn)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("list with non-admin grant = %d, want 403", resp.StatusCode)
	}
}

// TestOAuthGate_OAuthTokenRefusedAsGrant is the N2/Q7 guarantee: an OAuth
// token presented as a kernel credential on a non-connector route is refused
// by name, while a share-link-free ordinary grant still passes the gate.
func TestOAuthGate_OAuthTokenRefusedAsGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	tb := tokenFields(t, b)

	// The OAuth access token on an ordinary write route (a home-key route).
	c, body := env.post(t, "/v1/attention", tb.AccessToken, map[string]string{})
	if c != http.StatusForbidden || errorType(t, body) != "oauth_token_not_grant" {
		t.Fatalf("oauth token on /v1/attention = %d %s, want 403 oauth_token_not_grant", c, body)
	}
	// The refresh token is refused the same way.
	c, body = env.post(t, "/v1/attention", tb.RefreshToken, map[string]string{})
	if c != http.StatusForbidden || errorType(t, body) != "oauth_token_not_grant" {
		t.Fatalf("refresh token on /v1/attention = %d %s", c, body)
	}
	// The same OAuth token on the internal admin list is refused too.
	req, _ := http.NewRequest("GET", env.ts.URL+"/v1/oauth/grants", nil)
	req.Header.Set(GrantHeaderName, tb.AccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("oauth token on /v1/oauth/grants = %d, want 403", resp.StatusCode)
	}
	// Control: an ordinary kernel grant with the same route still passes the
	// gate (the refusal did not break the normal path).
	ordinary := mintScopedTestGrant(t, env.srv, "ordinary-control", ScopeWrite)
	c, body = env.post(t, "/v1/attention", ordinary, map[string]string{})
	if c == http.StatusForbidden || c == http.StatusUnauthorized {
		t.Fatalf("ordinary grant refused by the gate: %d %s", c, body)
	}
}
