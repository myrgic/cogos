package engine

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pairingHarness is a server with a pairing:approve grant, a trusted-device
// link, and a share link that lacks the scope, plus a clock the test can age.
type pairingHarness struct {
	t      *testing.T
	srv    *Server
	ts     *httptest.Server
	offset atomic.Int64 // seconds added to the pairing clock
	tokens map[string]string
}

func newPairingHarness(t *testing.T) *pairingHarness {
	t.Helper()
	srv := newGrantAuthTestServer(t)
	srv.pairingPollWait = 60 * time.Millisecond
	h := &pairingHarness{t: t, srv: srv, tokens: map[string]string{}}
	srv.pairing.now = func() time.Time { return time.Now().Add(time.Duration(h.offset.Load()) * time.Second) }
	h.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(h.ts.Close)

	h.tokens["grant"] = mintScopedTestGrant(t, srv, "stage-pairing", ScopePairingApprove)
	h.tokens["grant_noscope"] = mintScopedTestGrant(t, srv, "stage-other", ScopeInference)
	imp := func(tok, surface, kind string, scope []string) {
		in := ShareLinkImport{Kind: kind, Surface: surface, Actor: "chaz", Role: "owner", Scope: scope, IntegrityHash: sha256Hex(tok)}
		if _, err := srv.shareLinks.Import(in, ""); err != nil {
			t.Fatal(err)
		}
	}
	h.tokens["link_device"] = "device-token-for-the-test-0123456789"
	imp(h.tokens["link_device"], "stage/device/dev-aaa", ShareLinkKindDevice, defaultDeviceLinkScope)
	h.tokens["link_noscope"] = "room-token-for-the-test-0123456789"
	imp(h.tokens["link_noscope"], "stage/room/r1/owner", ShareLinkKindShare, []string{"stage:room:r1"})
	h.tokens["bogus"] = "not-a-credential"
	return h
}

func (h *pairingHarness) do(method, path, auth string, body any) (int, map[string]any, string) {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, h.ts.URL+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	if tok, ok := h.tokens[auth]; ok {
		req.Header.Set(GrantHeaderName, tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m, string(raw)
}

type pairingVectorFile struct {
	Rows []pairingVectorRow `json:"rows"`
}
type pairingVectorRow struct {
	ID    string              `json:"id"`
	Stage string              `json:"stage"`
	Steps []pairingVectorStep `json:"steps"`
}
type pairingVectorStep struct {
	Op      string            `json:"op"`
	Ref     string            `json:"ref"`
	Secret  string            `json:"secret"`
	Auth    string            `json:"auth"`
	Body    map[string]any    `json:"body"`
	Repeat  int               `json:"repeat"`
	Seconds int               `json:"seconds"`
	Save    map[string]string `json:"save"`
	Name    string            `json:"name"`
	Value   string            `json:"value"`
	Values  []string          `json:"values"`
	Expect  map[string]any    `json:"expect"`
}

func subst(s string, vars map[string]string) string {
	for k, v := range vars {
		s = strings.ReplaceAll(s, "{"+k+"}", v)
	}
	return s
}

func subsetMatch(got, want map[string]any) bool {
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			return false
		}
		if wm, isMap := w.(map[string]any); isMap {
			gm, ok := g.(map[string]any)
			if !ok || !subsetMatch(gm, wm) {
				return false
			}
			continue
		}
		if fmt.Sprint(g) != fmt.Sprint(w) {
			return false
		}
	}
	return true
}

// TestPairing_StageVectors runs the vendored Stage rows (D, P, X01) against the
// kernel flow.
func TestPairing_StageVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/pairing-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vf pairingVectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatal(err)
	}
	if len(vf.Rows) < 15 {
		t.Fatalf("vector file shrank to %d rows", len(vf.Rows))
	}
	for _, row := range vf.Rows {
		row := row
		t.Run(row.ID, func(t *testing.T) {
			t.Parallel()
			h := newPairingHarness(t)
			vars := map[string]string{}
			for i, st := range row.Steps {
				where := fmt.Sprintf("%s step %d (%s)", row.ID, i+1, st.Op)
				n := st.Repeat
				if n == 0 {
					n = 1
				}
				for rep := 0; rep < n; rep++ {
					var code int
					var m map[string]any
					var rawBody string
					ref := subst(st.Ref, vars)
					switch st.Op {
					case "request":
						code, m, rawBody = h.do("POST", "/v1/pairing/requests", "", st.Body)
					case "poll":
						secret := st.Secret
						if secret != "wrong" {
							secret = subst(secret, vars)
						}
						code, m, rawBody = h.do("GET", "/v1/pairing/requests/"+ref+"?secret="+secret, "", nil)
					case "approve", "deny":
						code, m, rawBody = h.do("POST", "/v1/pairing/requests/"+ref+"/"+st.Op, st.Auth, st.Body)
					case "age":
						h.offset.Add(int64(st.Seconds))
						continue
					case "prop":
						switch st.Name {
						case "alphabet":
							if pairingCodeAlphabet != st.Value {
								t.Fatalf("%s: alphabet = %q, want the Stage's %q", where, pairingCodeAlphabet, st.Value)
							}
						case "ledger-hash-only":
							root := h.srv.cfg.WorkspaceRoot
							for _, sess := range []string{pairingLedgerSession, shareLinksLedgerSession} {
								b, err := os.ReadFile(filepath.Join(root, ".cog", "ledger", sess, "events.jsonl"))
								if err != nil {
									t.Fatalf("%s: read %s ledger: %v", where, sess, err)
								}
								for _, v := range st.Values {
									if v = subst(v, vars); v != "" && strings.Contains(string(b), v) {
										t.Fatalf("%s: a raw secret value leaked into the %s ledger", where, sess)
									}
								}
							}
						default:
							t.Fatalf("%s: unknown property %q", where, st.Name)
						}
						continue
					default:
						t.Fatalf("%s: unknown op", where)
					}
					if want, ok := st.Expect["status"]; ok && float64(code) != want.(float64) {
						t.Fatalf("%s: status %d, want %v (%s)", where, code, want, rawBody)
					}
					if want, ok := st.Expect["error"]; ok {
						em, _ := m["error"].(map[string]any)
						if got, _ := em["message"].(string); got != want {
							t.Fatalf("%s: error %q, want %q", where, got, want)
						}
					}
					if keys, ok := st.Expect["json_keys"].([]any); ok {
						for _, k := range keys {
							if _, has := m[k.(string)]; !has {
								t.Fatalf("%s: missing key %q in %s", where, k, redactTokens(rawBody))
							}
						}
					}
					if sub, ok := st.Expect["json_subset"].(map[string]any); ok && !subsetMatch(m, sub) {
						t.Fatalf("%s: body %s lacks subset %v", where, redactTokens(rawBody), sub)
					}
					if rx, ok := st.Expect["json_regex"].(map[string]any); ok {
						for k, pat := range rx {
							if s, _ := m[k].(string); !regexp.MustCompile(pat.(string)).MatchString(s) {
								t.Fatalf("%s: %s=%q does not match %s", where, k, s, pat)
							}
						}
					}
					for v, field := range st.Save {
						s, _ := m[field].(string)
						vars[v] = s
					}
				}
			}
		})
	}
}

func redactTokens(s string) string {
	return regexp.MustCompile(`"token":"[^"]*"`).ReplaceAllString(s, `"token":"<redacted>"`)
}

func (h *pairingHarness) newRequest(body map[string]any) (id, code, secret string) {
	h.t.Helper()
	status, m, raw := h.do("POST", "/v1/pairing/requests", "", body)
	if status != 200 {
		h.t.Fatalf("create = %d %s", status, raw)
	}
	return m["id"].(string), m["code"].(string), m["secret"].(string)
}

// The poll long-polls: an approval that lands while the requester waits ends
// the wait early and delivers the token.
func TestPairing_LongPollWakesOnApproval(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	h.srv.pairingPollWait = 5 * time.Second
	id, code, secret := h.newRequest(map[string]any{"kind": "device", "name": "tv"})
	type res struct {
		status int
		m      map[string]any
	}
	ch := make(chan res, 1)
	go func() {
		s, m, _ := h.do("GET", "/v1/pairing/requests/"+id+"?secret="+secret, "", nil)
		ch <- res{s, m}
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if s, _, raw := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"}); s != 200 {
		t.Fatalf("approve = %d %s", s, raw)
	}
	select {
	case r := <-ch:
		if r.status != 200 || r.m["token"] == nil {
			t.Fatalf("poll = %d", r.status)
		}
		if time.Since(start) > 2*time.Second {
			t.Error("poll did not wake on approval")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("poll never returned")
	}
}

// The token that comes back is a working share link and nothing else.
func TestPairing_IssuedLinkResolvesAndIsNotAGrant(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	id, code, secret := h.newRequest(map[string]any{"kind": "device", "name": "tv"})
	h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"})
	_, m, _ := h.do("GET", "/v1/pairing/requests/"+id+"?secret="+secret, "", nil)
	token, _ := m["token"].(string)
	if len(token) < 40 {
		t.Fatalf("token too short: %d", len(token))
	}
	link, ok := h.srv.shareLinks.Resolve(token)
	if !ok || link.Kind != ShareLinkKindDevice || link.Actor != "chaz" || link.Role != "owner" {
		t.Fatalf("resolve = %+v ok=%v", link, ok)
	}
	if !strings.HasPrefix(link.Surface, "stage/device/dev-") {
		t.Errorf("surface = %q", link.Surface)
	}
	want := map[string]bool{"stage:device": true, "stage:packs": true, "stage:screens": true, ScopePairingApprove: true}
	for _, sc := range link.Scope {
		delete(want, sc)
	}
	if len(want) != 0 {
		t.Errorf("device link missing default scopes %v (has %v)", want, link.Scope)
	}
	if link.Parent == "" {
		t.Error("link has no parent (issuing grant) recorded")
	}
	// it cannot act as a grant
	h.tokens["issued"] = token
	if s, _, _ := h.do("POST", "/v1/stage/keys/list", "issued", map[string]any{}); s != http.StatusForbidden {
		t.Errorf("issued link on a grant route = %d, want 403", s)
	}
}

func TestPairing_ApproveWithoutRequestedKindMismatchOrBadBody(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	_, code, _ := h.newRequest(map[string]any{"kind": "device"})
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"kind": "share", "actor": "chaz", "role": "owner"}); s != 400 {
		t.Errorf("kind mismatch = %d, want 400", s)
	}
	// a share request needs a surface; the request stays open after a 400
	_, scode, _ := h.newRequest(map[string]any{"kind": "share"})
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+scode+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"}); s != 400 {
		t.Errorf("share without surface/scope = %d, want 400", s)
	}
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+scode+"/approve", "grant", map[string]any{
		"surface": "stage/room/r9/owner", "actor": "chaz", "role": "owner", "room": "r9", "scope": []string{"stage:room:r9"}}); s != 200 {
		t.Errorf("approve after a rejected attempt = %d, want 200", s)
	}
	if s, _, _ := h.do("POST", "/v1/pairing/requests", "", map[string]any{"kind": "grant"}); s != 400 {
		t.Errorf("kind=grant = %d, want 400", s)
	}
}

// A trusted device cannot mint more than it is.
func TestPairing_LinkApproverCannotEscalate(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	cases := []struct {
		name string
		body map[string]any
	}{
		{"other actor", map[string]any{"actor": "vroku", "role": "guest"}},
		{"scope outside its own", map[string]any{"scope": []string{"stage:admin"}}},
	}
	for _, c := range cases {
		_, code, _ := h.newRequest(map[string]any{"kind": "device"})
		if s, _, raw := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "link_device", c.body); s != 403 {
			t.Errorf("%s = %d %s, want 403", c.name, s, raw)
		}
	}
	// a lower-privileged approver cannot raise the role
	in := ShareLinkImport{Kind: ShareLinkKindDevice, Surface: "stage/device/dev-guest", Actor: "vroku", Role: "guest",
		Scope: []string{ScopePairingApprove}, IntegrityHash: sha256Hex("guest-device-token-0123456789")}
	if _, err := h.srv.shareLinks.Import(in, ""); err != nil {
		t.Fatal(err)
	}
	h.tokens["guest_dev"] = "guest-device-token-0123456789"
	_, code, _ := h.newRequest(map[string]any{"kind": "device"})
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "guest_dev", map[string]any{"role": "owner", "scope": []string{ScopePairingApprove}}); s != 403 {
		t.Errorf("guest device approving owner = %d, want 403", s)
	}
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "guest_dev", map[string]any{"scope": []string{ScopePairingApprove}}); s != 200 {
		t.Errorf("guest device approving its own level = %d, want 200", s)
	}
}

func TestPairing_ListNeedsApproverAndHidesSecrets(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	_, code, secret := h.newRequest(map[string]any{"kind": "device", "name": "kitchen tv"})
	for _, a := range []string{"", "bogus"} {
		if s, _, _ := h.do("GET", "/v1/pairing/requests", a, nil); s != 401 {
			t.Errorf("list auth=%q = %d, want 401", a, s)
		}
	}
	if s, _, _ := h.do("GET", "/v1/pairing/requests", "link_noscope", nil); s != 403 {
		t.Errorf("list with a scopeless link = %d, want 403", s)
	}
	s, _, raw := h.do("GET", "/v1/pairing/requests", "grant", nil)
	if s != 200 || !strings.Contains(raw, code) || !strings.Contains(raw, "kitchen tv") {
		t.Fatalf("list = %d %s", s, raw)
	}
	if strings.Contains(raw, secret) {
		t.Error("list leaked the poll secret")
	}
}

func TestPairing_CreateIsRateLimited(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	h.srv.pairingLimiter = newGrantMintLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if s, _, _ := h.do("POST", "/v1/pairing/requests", "", map[string]any{"kind": "share"}); s != 200 {
			t.Fatalf("create %d = %d", i, s)
		}
	}
	s, m, _ := h.do("POST", "/v1/pairing/requests", "", map[string]any{"kind": "share"})
	if s != 429 || m["error"].(map[string]any)["type"] != "rate_limited" {
		t.Fatalf("fourth create = %d %v, want 429 rate_limited", s, m)
	}
}

func TestPairing_PublicKeyRecordedByFingerprint(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.MarshalPKIXPublicKey(&k.PublicKey)
	ecdhPub, _ := k.PublicKey.ECDH()
	forms := map[string]string{
		"spki-std":  base64.StdEncoding.EncodeToString(der),
		"spki-url":  base64.RawURLEncoding.EncodeToString(der),
		"point-std": base64.StdEncoding.EncodeToString(ecdhPub.Bytes()),
	}
	for name, form := range forms {
		got, err := parsePairingPublicKey(form)
		if err != nil || !bytes.Equal(got, der) {
			t.Errorf("%s: parse = %v (equal=%v)", name, err, bytes.Equal(got, der))
		}
	}
	k384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	der384, _ := x509.MarshalPKIXPublicKey(&k384.PublicKey)
	for name, bad := range map[string]string{
		"p384":      base64.StdEncoding.EncodeToString(der384),
		"garbage":   base64.StdEncoding.EncodeToString([]byte("not a key")),
		"notb64":    "%%%",
		"bad-point": base64.StdEncoding.EncodeToString(append([]byte{4}, make([]byte, 64)...)),
	} {
		if s, _, _ := h.do("POST", "/v1/pairing/requests", "", map[string]any{"kind": "device", "public_key": bad}); s != 400 {
			t.Errorf("%s accepted (%d)", name, s)
		}
	}

	_, code, _ := h.newRequest(map[string]any{"kind": "device", "public_key": forms["spki-std"]})
	if s, _, raw := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"}); s != 200 {
		t.Fatalf("approve = %d %s", s, raw)
	}
	b, err := os.ReadFile(filepath.Join(h.srv.cfg.WorkspaceRoot, ".cog", "ledger", pairingLedgerSession, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	led := string(b)
	if !strings.Contains(led, "pairing.requested") || !strings.Contains(led, "pairing.approved") {
		t.Fatalf("ledger lacks pairing events: %s", led)
	}
	if !strings.Contains(led, forms["spki-std"]) {
		t.Error("approval event did not record the public key")
	}
	if strings.Contains(led, code) {
		t.Error("the raw code is in the ledger")
	}
	reqLine := strings.SplitN(led, "\n", 2)[0]
	if strings.Contains(reqLine, forms["spki-std"]) {
		t.Error("the request event carries the key, not just its fingerprint")
	}
}

// Two approvers racing on one code: exactly one link is issued.
func TestPairing_ConcurrentApprovalsIssueOneLink(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	_, code, _ := h.newRequest(map[string]any{"kind": "device"})
	before := h.srv.shareLinks.Len()
	var wg sync.WaitGroup
	var ok atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s, _, _ := h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"}); s == 200 {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || h.srv.shareLinks.Len() != before+1 {
		t.Fatalf("approvals ok=%d, links %d -> %d; want exactly one", ok.Load(), before, h.srv.shareLinks.Len())
	}
}

// An approved link nobody collected is revoked when its request expires.
func TestPairing_UncollectedApprovalIsRevokedOnExpiry(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	_, code, _ := h.newRequest(map[string]any{"kind": "device"})
	before := h.srv.shareLinks.Len()
	h.do("POST", "/v1/pairing/requests/"+code+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"})
	if h.srv.shareLinks.Len() != before+1 {
		t.Fatal("approval did not issue a link")
	}
	h.offset.Add(601)
	h.do("POST", "/v1/pairing/requests", "", map[string]any{"kind": "share"}) // triggers a prune
	if h.srv.shareLinks.Len() != before {
		t.Errorf("links = %d, want %d after expiry", h.srv.shareLinks.Len(), before)
	}
}

// Lowercase codes and ids both address a request.
func TestPairing_CodeIsCaseInsensitiveAndIDWorks(t *testing.T) {
	t.Parallel()
	h := newPairingHarness(t)
	id, code, secret := h.newRequest(map[string]any{"kind": "device"})
	if s, _, _ := h.do("POST", "/v1/pairing/requests/"+strings.ToLower(code)+"/approve", "grant", map[string]any{"actor": "chaz", "role": "owner"}); s != 200 {
		t.Errorf("lowercase code approve = %d", s)
	}
	if s, _, _ := h.do("GET", "/v1/pairing/requests/"+id+"?secret="+secret, "", nil); s != 200 {
		t.Errorf("poll by id = %d", s)
	}
}

// The flow must not widen the grant gate: a share link is still refused by name
// on a gated route, and the route list is exactly the five pairing routes.
func TestPairing_GateExemptionIsScopedToPairingPaths(t *testing.T) {
	t.Parallel()
	for path, want := range map[string]bool{
		"/v1/pairing/requests":              true,
		"/v1/pairing/requests/ABCD/approve": true,
		"/v1/pairingx":                      false,
		"/v1/stage/keys/list":               false,
	} {
		r := httptest.NewRequest("POST", path, nil)
		if got := isGrantExemptRequest(r); got != want {
			t.Errorf("exempt(%s) = %v, want %v", path, got, want)
		}
	}
	h := newPairingHarness(t)
	if s, _, _ := h.do("POST", "/v1/stage/keys/list", "link_device", map[string]any{}); s != http.StatusForbidden {
		t.Errorf("share link on the keys route = %d, want 403", s)
	}
}
