package engine

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

// The ChatGPT connector client. Both values are public identifiers (the
// client_id is a URL the client hosts; the redirect_uri is the callback it
// registers), not secrets.
const (
	testChatGPTClientID    = "https://chatgpt.com/oauth/xpkWNdN9JZkc/client.json"
	testChatGPTRedirectURI = "https://chatgpt.com/connector/oauth/xpkWNdN9JZkc"
	testChatGPTHost        = "chatgpt.com"
)

// newOAuthChatGPTEnv is newOAuthAuthzEnv with the CIMD host swapped for a
// chatgpt.com fixture serving the ChatGPT client's own document. The default
// oauth.allowed_client_hosts and oauth.redirect_uris (serve_oauth.go) are in
// force: nothing in this env overrides them.
func newOAuthChatGPTEnv(t *testing.T) *oauthAuthzEnv {
	t.Helper()
	env := newOAuthAuthzEnv(t)
	env.cimd = newFakeCIMDHostNamed(t, testChatGPTHost, testChatGPTClientID, testChatGPTRedirectURI)
	env.cimd.wire(t, env.state.cimd)
	return env
}

func chatGPTQuery() url.Values {
	q := validQuery()
	q.Set("client_id", testChatGPTClientID)
	q.Set("redirect_uri", testChatGPTRedirectURI)
	return q
}

// Test 1: the exact ChatGPT client_id + redirect_uri pair passes every check
// (cheap local checks, the CIMD fetch, and the fetched document's own
// redirect_uris match) and leaves a pending consent request.
func TestOAuthAuthorize_ChatGPTConnectorPairAccepted(t *testing.T) {
	env := newOAuthChatGPTEnv(t)
	code, body, _ := env.authorize(t, chatGPTQuery(), "application/json")
	if code != 200 {
		t.Fatalf("authorize = %d: %s", code, body)
	}
	var res authzResult
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("authorize body: %v: %s", err, body)
	}
	if res.Status != "pending" || !strings.HasPrefix(res.RequestID, "oar-") {
		t.Fatalf("unexpected result: %+v", res)
	}
	if env.cimd.hitCount() != 1 {
		t.Errorf("CIMD fetch hits = %d, want 1", env.cimd.hitCount())
	}
	if len(env.notices) != 1 || env.notices[0].ClientName != "Claude" {
		// The fixture document names the client "Claude"; the consent notice
		// carries whatever the document says, so this pins that the fetched
		// document (not a hard-coded name) reached the stage.
		t.Errorf("stage notice = %+v", env.notices)
	}
}

// Test 2: same chatgpt.com client, a different callback id is refused before
// any outbound fetch. Exact matching is not loosened to host or prefix.
func TestOAuthAuthorize_ChatGPTWrongCallbackIDRejected(t *testing.T) {
	env := newOAuthChatGPTEnv(t)
	for _, bad := range []string{
		"https://chatgpt.com/connector/oauth/wrongid",
		testChatGPTRedirectURI + "/extra",
		testChatGPTRedirectURI + "/",
		"https://chatgpt.com/connector/oauth/",
	} {
		q := chatGPTQuery()
		q.Set("redirect_uri", bad)
		code, body, _ := env.authorize(t, q, "application/json")
		if code != 400 {
			t.Errorf("redirect_uri %q = %d, want 400: %s", bad, code, body)
		}
		var e map[string]string
		_ = json.Unmarshal([]byte(body), &e)
		if e["error"] != "invalid_request" {
			t.Errorf("redirect_uri %q error = %q, want invalid_request", bad, e["error"])
		}
	}
	if env.cimd.hitCount() != 0 {
		t.Errorf("CIMD host contacted for a wrong redirect_uri (%d hits)", env.cimd.hitCount())
	}
	if len(env.notices) != 0 {
		t.Errorf("a rejected request reached the stage: %+v", env.notices)
	}
}
