package engine

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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

// testChatGPTDocument is ChatGPT's live Client ID Metadata Document, fetched
// verbatim from https://chatgpt.com/oauth/xpkWNdN9JZkc/client.json (a public,
// unauthenticated GET). Its primary token_endpoint_auth_method is
// private_key_jwt; "none" appears only in token_endpoint_auth_methods_supported.
const testChatGPTDocument = `{"client_id":"https://chatgpt.com/oauth/xpkWNdN9JZkc/client.json","client_uri":"https://chatgpt.com/","redirect_uris":["https://chatgpt.com/connector/oauth/xpkWNdN9JZkc"],"token_endpoint_auth_method":"private_key_jwt","token_endpoint_auth_methods_supported":["none","private_key_jwt"],"grant_types":["authorization_code","refresh_token"],"response_types":["code"],"client_name":"ChatGPT","logo_uri":"https://persistent.oaistatic.com/sonic/misc/openai-logo.png","token_endpoint_auth_signing_alg":"RS256","jwks_uri":"https://chatgpt.com/oauth/jwks.json"}`

// newOAuthChatGPTEnv is newOAuthAuthzEnv with the CIMD host swapped for a
// chatgpt.com fixture serving the ChatGPT client's own document. The default
// oauth.allowed_client_hosts and oauth.redirect_uris (serve_oauth.go) are in
// force: nothing in this env overrides them.
func newOAuthChatGPTEnv(t *testing.T) *oauthAuthzEnv {
	t.Helper()
	env := newOAuthAuthzEnv(t)
	env.cimd = newFakeCIMDHostNamed(t, testChatGPTHost, testChatGPTClientID, testChatGPTRedirectURI)
	env.cimd.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, testChatGPTDocument)
	})
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
	if len(env.notices) != 1 || env.notices[0].ClientName != "ChatGPT" {
		// The fixture document names the client "ChatGPT"; the consent notice
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

// Test 3: the real ChatGPT document (primary private_key_jwt, "none" only in
// token_endpoint_auth_methods_supported) is accepted by the CIMD parser.
func TestOAuthCIMD_ChatGPTRealDocumentAccepted(t *testing.T) {
	m, err := parseClientMetadata(testChatGPTClientID, []byte(testChatGPTDocument))
	if err != nil {
		t.Fatalf("real ChatGPT document rejected: %v", err)
	}
	if m.ClientName != "ChatGPT" || len(m.RedirectURIs) != 1 {
		t.Fatalf("unexpected metadata: %+v", m)
	}
}

// Test 4: a document that supports only a confidential method (no "none"
// anywhere) is still rejected, so the fix did not loosen the check.
func TestOAuthCIMD_ConfidentialOnlyRejected(t *testing.T) {
	cases := map[string]string{
		"primary client_secret_basic, no supported list": `{"client_id":"` + testChatGPTClientID + `","redirect_uris":["` + testChatGPTRedirectURI + `"],"token_endpoint_auth_method":"client_secret_basic"}`,
		"supported list without none":                    `{"client_id":"` + testChatGPTClientID + `","redirect_uris":["` + testChatGPTRedirectURI + `"],"token_endpoint_auth_method":"private_key_jwt","token_endpoint_auth_methods_supported":["client_secret_basic","private_key_jwt"]}`,
	}
	for name, doc := range cases {
		if _, err := parseClientMetadata(testChatGPTClientID, []byte(doc)); !errors.Is(err, ErrOAuthClientMetadata) {
			t.Errorf("%s: err = %v, want ErrOAuthClientMetadata", name, err)
		}
	}
}
