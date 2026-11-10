package engine

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestOAuthK4LiveRoundtripProof walks the whole K4 loop over real HTTP and logs
// the bytes of each hop (run with -v): authorization code -> token -> introspect
// (active) -> refresh -> reuse of the old refresh token (invalid_grant, grant
// dead) -> introspect (inactive), plus the gate refusal of the access token.
// Assertions keep the log honest.
func TestOAuthK4LiveRoundtripProof(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	t.Logf("0. authorization code from K3 approve+poll: %s...", code[:12])

	c, b := env.tokenPost(t, exchangeForm(code))
	t.Logf("1. POST /oauth/token grant_type=authorization_code -> %d\n%s", c, strings.TrimSpace(b))
	if c != 200 {
		t.Fatalf("code exchange failed")
	}
	first := tokenFields(t, b)

	in := env.introspect(t, first.AccessToken)
	t.Logf("2. POST /v1/oauth/introspect (access token) -> active=%v\n%s", in.Active, strings.TrimSpace(in.Raw))
	if !in.Active {
		t.Fatalf("access token not active after issue")
	}

	c, b = env.tokenPost(t, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {first.RefreshToken},
		"client_id":     {testClientID},
		"resource":      {testResource},
	})
	t.Logf("3. POST /oauth/token grant_type=refresh_token (rotate) -> %d\n%s", c, strings.TrimSpace(b))
	if c != 200 {
		t.Fatalf("rotation failed")
	}

	c, b = env.tokenPost(t, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {first.RefreshToken},
		"client_id":     {testClientID},
	})
	t.Logf("4. POST /oauth/token REUSE of the rotated refresh token -> %d\n%s", c, strings.TrimSpace(b))
	if c != http.StatusBadRequest || errorType(t, b) != "invalid_grant" {
		t.Fatalf("reuse was not invalid_grant: %d %s", c, b)
	}

	in = env.introspect(t, first.AccessToken)
	t.Logf("5. POST /v1/oauth/introspect (access token after reuse) -> active=%v\n%s", in.Active, strings.TrimSpace(in.Raw))
	if in.Active {
		t.Fatalf("access token still active after refresh reuse revoked the grant")
	}

	c, b = env.post(t, "/v1/attention", first.AccessToken, map[string]string{})
	t.Logf("6. GATE: OAuth access token as X-Cogos-Grant on /v1/attention -> %d %s", c, strings.TrimSpace(b))
	if c != http.StatusForbidden || errorType(t, b) != "oauth_token_not_grant" {
		t.Fatalf("gate did not refuse the oauth token")
	}
}
