package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newOAuthTestServer(enabled bool) (*Server, *http.ServeMux) {
	cfg := &Config{WorkspaceRoot: "/nonexistent-oauth-test"}
	cfg.OAuth = OAuthConfig{Enabled: enabled, Issuer: "https://stage.example.test/"}
	s := &Server{cfg: cfg}
	mux := http.NewServeMux()
	s.registerOAuthRoutes(mux)
	return s, mux
}

func oauthStrSet(t *testing.T, doc map[string]any, key string) map[string]bool {
	t.Helper()
	raw, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("%s missing or not an array: %#v", key, doc[key])
	}
	out := map[string]bool{}
	for _, v := range raw {
		out[v.(string)] = true
	}
	return out
}

func TestOAuthASMetadata(t *testing.T) {
	_, mux := newOAuthTestServer(true)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", oauthWellKnownASPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("metadata is not JSON: %v", err)
	}
	t.Logf("metadata bytes:\n%s", rec.Body.String())

	if doc["issuer"] != "https://stage.example.test" { // trailing slash trimmed
		t.Errorf("issuer = %v", doc["issuer"])
	}
	if doc["authorization_endpoint"] != "https://stage.example.test/oauth/authorize" {
		t.Errorf("authorization_endpoint = %v", doc["authorization_endpoint"])
	}
	if doc["token_endpoint"] != "https://stage.example.test/oauth/token" {
		t.Errorf("token_endpoint = %v", doc["token_endpoint"])
	}
	if v, ok := doc["client_id_metadata_document_supported"].(bool); !ok || !v {
		t.Errorf("client_id_metadata_document_supported = %v", doc["client_id_metadata_document_supported"])
	}
	pkce := oauthStrSet(t, doc, "code_challenge_methods_supported")
	if len(pkce) != 1 || !pkce["S256"] {
		t.Errorf("code_challenge_methods_supported = %v, want exactly [S256]", pkce)
	}
	if am := oauthStrSet(t, doc, "token_endpoint_auth_methods_supported"); !am["none"] {
		t.Errorf("token_endpoint_auth_methods_supported lacks none: %v", am)
	}
	if sc := oauthStrSet(t, doc, "scopes_supported"); !sc["mcp:connector"] {
		t.Errorf("scopes_supported lacks mcp:connector: %v", sc)
	}
	gt := oauthStrSet(t, doc, "grant_types_supported")
	if !gt["authorization_code"] || !gt["refresh_token"] {
		t.Errorf("grant_types_supported = %v", gt)
	}
	if rt := oauthStrSet(t, doc, "response_types_supported"); !rt["code"] {
		t.Errorf("response_types_supported = %v", rt)
	}
}

func TestOAuthRoutesPlaceholder501(t *testing.T) {
	_, mux := newOAuthTestServer(true)
	cases := []struct{ method, path string }{
		// K3 filled /oauth/authorize, /oauth/authorize/status/{id} and
		// /v1/oauth/requests/{id}/approve|deny; see serve_oauth_authorize_test.go.
		{"POST", "/oauth/token"},
		{"POST", "/v1/oauth/introspect"},
		{"GET", "/v1/oauth/grants"},
		{"POST", "/v1/oauth/grants/abc/revoke"},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s %s = %d, want 501", c.method, c.path, rec.Code)
		}
	}
}

func TestOAuthRoutesOffByDefault(t *testing.T) {
	_, mux := newOAuthTestServer(false)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", oauthWellKnownASPath, nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("oauth disabled: status = %d, want 404", rec.Code)
	}
}

func TestOAuthGateClassification(t *testing.T) {
	exempt := func(method, path string) bool {
		return isGrantExemptRequest(httptest.NewRequest(method, path, nil))
	}
	for _, p := range []string{oauthWellKnownASPath, "/oauth/authorize", "/oauth/token"} {
		if !exempt("GET", p) {
			t.Errorf("%s should be exempt from the grant gate", p)
		}
	}
	for _, p := range []string{"/v1/oauth/grants", "/v1/oauth/introspect", "/v1/oauth/requests/x/approve"} {
		if exempt("GET", p) {
			t.Errorf("%s must be gated", p)
		}
	}
	scope := func(method, path string) string {
		return requiredScopeForRequest(httptest.NewRequest(method, path, nil))
	}
	if got := scope("POST", "/v1/oauth/requests/x/approve"); got != ScopeOAuthConsent {
		t.Errorf("approve scope = %q", got)
	}
	if got := scope("POST", "/v1/oauth/introspect"); got != ScopeOAuthIntrospect {
		t.Errorf("introspect scope = %q", got)
	}
	if got := scope("GET", "/v1/oauth/grants"); got != ScopeAdmin {
		t.Errorf("grants scope = %q", got)
	}
}

func TestOAuthIssuerDefaults(t *testing.T) {
	s := &Server{cfg: &Config{Port: 6931, BindAddr: "127.0.0.1"}}
	st := s.oauthSettings()
	if st.Issuer != "http://127.0.0.1:6931" || st.Resource != "http://127.0.0.1:6931/mcp" {
		t.Errorf("loopback defaults: %+v", st)
	}
	if len(st.AllowedClientHosts) != 1 || st.AllowedClientHosts[0] != "claude.ai" {
		t.Errorf("allowed hosts: %v", st.AllowedClientHosts)
	}
	s.cfg.StageUI.PublicHost = "stage.myrgic.com"
	if got := s.oauthSettings().Issuer; got != "https://stage.myrgic.com" {
		t.Errorf("public_host issuer = %q", got)
	}
}
