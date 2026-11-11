package engine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// K6: RFC 9728 protected-resource metadata for the stage's /mcp resource,
// served on :6932 (the API mux) and on the public :7873 listener. Static JSON,
// same issuer/resource defaulting as the K2 AS metadata (oauthSettings).

// k6WantBody is the document with the issuer https://stage.example.test (the
// issuer the K5 test env and newOAuthTestServer both use), as the JSON encoder
// emits it: sorted keys, two-space indent, trailing newline.
const k6WantBody = `{
  "authorization_servers": [
    "https://stage.example.test"
  ],
  "bearer_methods_supported": [
    "header"
  ],
  "resource": "https://stage.example.test/mcp"
}
`

func TestOAuthPRMetadata_API6932(t *testing.T) {
	_, mux := newOAuthTestServer(true)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", oauthWellKnownPRPath, nil))
	t.Logf(":6932 GET %s -> %d Content-Type=%s\n%s", oauthWellKnownPRPath, rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
	if got := rec.Body.String(); got != k6WantBody {
		t.Errorf("body mismatch:\n got: %q\nwant: %q", got, k6WantBody)
	}
	var doc map[string]any
	mustJSON(t, rec.Body.String(), &doc)
	if len(doc) != 3 {
		t.Errorf("document has %d keys, want exactly 3: %v", len(doc), doc)
	}
	// Other methods are not routed (method pattern).
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("POST", oauthWellKnownPRPath, strings.NewReader("{}")))
	if rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
		t.Errorf("POST status = %d, want 405/404", rec.Code)
	}
}

// resource and issuer come from oauthSettings: an explicit resource wins, and
// the document tracks the AS metadata's issuer (not recomputed).
func TestOAuthPRMetadata_UsesOAuthSettings(t *testing.T) {
	s := &Server{cfg: &Config{}}
	s.cfg.OAuth = OAuthConfig{Enabled: true, Issuer: "https://as.example.test/", Resource: "https://rs.example.test/mcp/"}
	doc := s.oauthPRMetadata()
	if doc["resource"] != "https://rs.example.test/mcp" {
		t.Errorf("resource = %v", doc["resource"])
	}
	as, _ := doc["authorization_servers"].([]string)
	if len(as) != 1 || as[0] != s.oauthASMetadata()["issuer"] || as[0] != "https://as.example.test" {
		t.Errorf("authorization_servers = %v, want [AS metadata issuer]", as)
	}

	// Defaults: no oauth issuer/resource, stage_ui.public_host set.
	s2 := &Server{cfg: &Config{}}
	s2.cfg.OAuth = OAuthConfig{Enabled: true}
	s2.cfg.StageUI.PublicHost = "stage.example.test"
	doc2 := s2.oauthPRMetadata()
	if doc2["resource"] != "https://stage.example.test/mcp" {
		t.Errorf("default resource = %v", doc2["resource"])
	}
	// Defaults: loopback.
	s3 := &Server{cfg: &Config{Port: 6932}}
	s3.cfg.OAuth = OAuthConfig{Enabled: true}
	if got := s3.oauthPRMetadata()["resource"]; got != "http://127.0.0.1:6932/mcp" {
		t.Errorf("loopback resource = %v", got)
	}
}

func TestOAuthPRMetadata_OffByDefault6932(t *testing.T) {
	_, mux := newOAuthTestServer(false)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", oauthWellKnownPRPath, nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("oauth disabled: status = %d, want 404", rec.Code)
	}
}

func TestOAuthPRMetadata_Listener7873(t *testing.T) {
	env := newK5Env(t, true)
	before := env.hits.Load()
	code, body, hdr := env.do(t, "GET", oauthWellKnownPRPath, "203.0.113.7", nil, "")
	t.Logf(":7873 GET %s -> %d Content-Type=%s\n%s", oauthWellKnownPRPath, code, hdr.Get("Content-Type"), body)
	if code != http.StatusOK || env.viaStage(body) {
		t.Fatalf("not served by the kernel: %d %q", code, body)
	}
	if body != k6WantBody {
		t.Errorf("body mismatch:\n got: %q\nwant: %q", body, k6WantBody)
	}
	if hdr.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", hdr.Get("Content-Type"))
	}
	if env.hits.Load() != before {
		t.Error("request reached the proxied stage")
	}
	// Only the exact path: neighbours still proxy to the stage.
	for _, p := range []string{
		"/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/",
		"/.well-known/oauth-protected-resource/mcp/", "/.well-known/oauth-protected-resource/other",
	} {
		c, b, _ := env.do(t, "GET", p, "203.0.113.7", nil, "")
		if c != 200 || !env.viaStage(b) {
			t.Errorf("%s: %d %q, want the proxied stage answer", p, c, b)
		}
	}
	// /v1/oauth/* still never public.
	for _, p := range []string{"/v1/oauth/grants", "/v1/oauth/introspect"} {
		c, b, _ := env.do(t, "GET", p, "203.0.113.7", nil, "")
		if c != 200 || !env.viaStage(b) {
			t.Errorf("%s: %d %q, want the proxied stage answer", p, c, b)
		}
	}
}

func TestOAuthPRMetadata_Listener7873Disabled(t *testing.T) {
	env := newK5Env(t, false)
	code, body, _ := env.do(t, "GET", oauthWellKnownPRPath, "203.0.113.7", nil, "")
	if code != 200 || !env.viaStage(body) {
		t.Errorf("oauth off: %d %q, want the proxied stage answer", code, body)
	}
}

func TestOAuthPRMetadata_ListenerAllowlistAndGate(t *testing.T) {
	if !isOAuthListenerPath(oauthWellKnownPRPath) {
		t.Error("PR metadata path must be on the listener allowlist")
	}
	if !isGrantExemptRequest(httptest.NewRequest("GET", oauthWellKnownPRPath, nil)) {
		t.Error("PR metadata must be exempt from the :6932 grant gate")
	}
	if isOAuthInternalPath(oauthWellKnownPRPath) {
		t.Error("PR metadata is public discovery, not under /v1/oauth")
	}
}
