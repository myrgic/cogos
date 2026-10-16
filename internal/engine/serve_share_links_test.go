package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// U3 fixture. The hex values are Python's hashlib.sha256(token.encode()).hexdigest()
// for the same tokens, computed independently of this package. The Stage hashes
// device cookies this way (apps/stage/devices.py); the kernel hashes with
// sha256Hex. They must agree byte for byte or imported links would not resolve.
var shareLinkCrossHashVectors = []struct{ token, pyHex string }{
	{"stage-fixture-token-0123456789abcdef", "1b83c20a4fc6809d6559c855899936bd132d11e9cd252bce2357beda166470e8"},
	{"Zm9v_-Ã©", "c8f59502ade975f8e86559a538e6587de668cdc3951ae6f78e3bf64c97817fc9"},
}

func TestShareLink_CrossHashFixture_PythonEqualsKernel(t *testing.T) {
	t.Parallel()
	for _, v := range shareLinkCrossHashVectors {
		if got := sha256Hex(v.token); got != v.pyHex {
			t.Errorf("sha256Hex(%q) = %s; python hashlib gave %s", v.token, got, v.pyHex)
		}
	}
	// Live cross-check when a python3 is on PATH (skipped, not failed, otherwise).
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Log("python3 not on PATH; pinned vectors above still hold")
		return
	}
	for _, v := range shareLinkCrossHashVectors {
		out, err := exec.Command(py, "-c", "import hashlib,sys;print(hashlib.sha256(sys.argv[1].encode()).hexdigest())", v.token).Output()
		if err != nil {
			t.Skipf("python3 failed: %v", err)
		}
		if got := strings.TrimSpace(string(out)); got != sha256Hex(v.token) {
			t.Errorf("live python hash %s != kernel hash %s", got, sha256Hex(v.token))
		}
	}
}

func shareImport(token, surface string) ShareLinkImport {
	return ShareLinkImport{
		Kind: ShareLinkKindShare, Surface: surface, Actor: "chaz", Role: "owner",
		Room: "r1", Scope: []string{"stage:room:r1"}, IntegrityHash: sha256Hex(token),
		Confessed: "pre-B0",
	}
}

func TestShareLink_ImportResolveRevoke(t *testing.T) {
	t.Parallel()
	st := NewShareLinkStoreWithLedger("")
	l, err := st.Import(shareImport("tok-a", "stage/room/r1/owner"), "grant-x")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := st.Resolve("tok-a")
	if !ok || got.LinkID != l.LinkID || got.Role != "owner" || got.Parent != "grant-x" || got.Confessed != "pre-B0" {
		t.Fatalf("resolve = %+v ok=%v", got, ok)
	}
	if _, ok := st.Resolve("tok-b"); ok {
		t.Fatal("unknown token resolved")
	}
	// idempotent
	again, err := st.Import(shareImport("tok-a", "stage/room/r1/owner"), "grant-x")
	if err != nil || again.LinkID != l.LinkID {
		t.Fatalf("re-import = %+v, %v", again, err)
	}
	if _, err := st.Revoke(l.LinkID); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Resolve("tok-a"); ok {
		t.Fatal("revoked link still resolves")
	}
	if _, err := st.Revoke(l.LinkID); err != ErrShareLinkNotFound {
		t.Fatalf("second revoke err = %v", err)
	}
}

func TestShareLink_RekeySupersedesSurface(t *testing.T) {
	t.Parallel()
	st := NewShareLinkStoreWithLedger("")
	if _, err := st.Import(shareImport("old", "stage/home/p1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Import(shareImport("new", "stage/home/p1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Resolve("old"); ok {
		t.Error("superseded token still resolves")
	}
	if _, ok := st.Resolve("new"); !ok {
		t.Error("new token does not resolve")
	}
	if st.Len() != 1 {
		t.Errorf("len = %d; want 1", st.Len())
	}
	// one token, one meaning
	if _, err := st.Import(shareImport("new", "stage/home/p2"), ""); err == nil {
		t.Error("same hash under a second surface was accepted")
	}
}

func TestShareLink_Validation(t *testing.T) {
	t.Parallel()
	st := NewShareLinkStoreWithLedger("")
	bad := []ShareLinkImport{}
	mut := func(f func(*ShareLinkImport)) {
		in := shareImport("t", "stage/room/r1/owner")
		f(&in)
		bad = append(bad, in)
	}
	mut(func(i *ShareLinkImport) { i.Surface = "chat/room" })
	mut(func(i *ShareLinkImport) { i.Surface = "stage/../x " })
	mut(func(i *ShareLinkImport) { i.Kind = "grant" })
	mut(func(i *ShareLinkImport) { i.Role = "root" })
	mut(func(i *ShareLinkImport) { i.Actor = "Chaz!" })
	mut(func(i *ShareLinkImport) { i.Scope = []string{"managed-sessions"} })
	mut(func(i *ShareLinkImport) { i.Scope = nil })
	mut(func(i *ShareLinkImport) { i.IntegrityHash = "ABC" })
	for i, in := range bad {
		if _, err := st.Import(in, ""); err == nil {
			t.Errorf("bad[%d] accepted: %+v", i, in)
		}
	}
}

func TestShareLink_ExpiryHonored(t *testing.T) {
	t.Parallel()
	st := NewShareLinkStoreWithLedger("")
	in := shareImport("short", "stage/device/d1")
	in.Kind = ShareLinkKindDevice
	l, err := st.Import(in, "")
	if err != nil {
		t.Fatal(err)
	}
	if !l.ExpiresAt.IsZero() {
		t.Error("default link must not expire")
	}
	exp := &ShareLink{ExpiresAt: time.Now().Add(-time.Second)}
	if !exp.expired(time.Now()) {
		t.Error("past ExpiresAt must be expired")
	}
}

func TestShareLink_RestartRebuildsFromLedger(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()
	st := NewShareLinkStoreWithLedger(root)
	a, _ := st.Import(shareImport("tok-a", "stage/room/r1/owner"), "g1")
	if _, err := st.Import(shareImport("tok-b", "stage/room/r1/member/p1"), "g1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Import(shareImport("tok-c", "stage/room/r1/member/p1"), "g1"); err != nil { // supersedes tok-b
		t.Fatal(err)
	}
	if _, err := st.Revoke(a.LinkID); err != nil {
		t.Fatal(err)
	}

	resetLedgerCacheForTest()
	re, err := RebuildShareLinkStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := re.Resolve("tok-a"); ok {
		t.Error("revoked link resurrected by rebuild")
	}
	if _, ok := re.Resolve("tok-b"); ok {
		t.Error("superseded link resurrected by rebuild")
	}
	got, ok := re.Resolve("tok-c")
	if !ok || got.Parent != "g1" || got.Confessed != "pre-B0" || got.Role != "owner" {
		t.Fatalf("rebuilt link = %+v ok=%v", got, ok)
	}
	// ledger holds hashes only
	raw, err := os.ReadFile(filepath.Join(root, ".cog", "ledger", shareLinksLedgerSession, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{"tok-a", "tok-b", "tok-c"} {
		if bytes.Contains(raw, []byte(`"`+tok+`"`)) {
			t.Errorf("ledger contains raw token %q", tok)
		}
	}
	if !bytes.Contains(raw, []byte(sha256Hex("tok-c"))) {
		t.Error("ledger does not contain the hash")
	}
	if !bytes.Contains(raw, []byte("identity.share.issued")) || !bytes.Contains(raw, []byte("identity.share.revoked")) {
		t.Error("expected issued and revoked events")
	}
}

func TestShareLink_AppendFailureLeavesStateUntouched(t *testing.T) {
	t.Parallel()
	st := NewShareLinkStoreWithLedger(t.TempDir())
	ok := true
	st.appendEvent = func(string, string, *EventEnvelope) error {
		if ok {
			return nil
		}
		return fmt.Errorf("disk full")
	}
	l, err := st.Import(shareImport("tok", "stage/home/chaz"), "")
	if err != nil {
		t.Fatal(err)
	}
	ok = false
	if _, err := st.Import(shareImport("tok2", "stage/home/chaz"), ""); err == nil {
		t.Fatal("import should fail")
	}
	if _, found := st.Resolve("tok"); !found {
		t.Error("failed supersede dropped the live link")
	}
	if _, err := st.Revoke(l.LinkID); err == nil {
		t.Fatal("revoke should fail")
	}
	if _, found := st.Resolve("tok"); !found {
		t.Error("failed revoke dropped the live link")
	}
}

// U4: 1000 links leave surface-grant minting untouched, in the same process.
func TestShareLink_ThousandLinksDoNotUseGrantCapacity(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	links := make([]ShareLinkImport, 0, 1000)
	for i := 0; i < 1000; i++ {
		links = append(links, shareImport(fmt.Sprintf("bulk-%d", i), fmt.Sprintf("stage/room/r%d/owner", i)))
	}
	root := mintScopedTestGrant(t, srv, "node-root-like", ScopeStageKeys)
	_ = root
	before := len(srv.identityGrants.Snapshot())
	body, _ := json.Marshal(shareLinkImportRequest{Links: links})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/stage/keys/import", bytes.NewReader(body))
	req.Header.Set(GrantHeaderName, root)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("batch import status = %d", resp.StatusCode)
	}
	if srv.shareLinks.Len() != 1000 {
		t.Fatalf("store holds %d links; want 1000", srv.shareLinks.Len())
	}
	if after := len(srv.identityGrants.Snapshot()); after != before {
		t.Errorf("grant surfaces changed %d -> %d", before, after)
	}
	// fill the grant store to its cap; every mint must still succeed.
	for i := before; i < maxLiveGrantSurfaces; i++ {
		if _, err := srv.identityGrants.MintOrReuse(fmt.Sprintf("surf-%d", i), []string{"x"}, time.Hour); err != nil {
			t.Fatalf("mint %d with 1000 links present: %v", i, err)
		}
	}
	if _, ok := srv.shareLinks.Resolve("bulk-999"); !ok {
		t.Error("link lost")
	}
}

func shareDo(t *testing.T, ts *httptest.Server, method, path, header, token string, body any) *http.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, ts.URL+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(header, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// A share link must never act as a grant: x-cogos-grant, Bearer, x-api-key,
// on managed-sessions, /mcp, the grant mint route, and the share routes.
func TestShareLink_NeverActsAsGrant(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const tok = "share-link-secret-xyz"
	in := shareImport(tok, "stage/room/r1/owner")
	in.Scope = []string{"stage:room:r1", "stage:keys"} // even claiming the scope name
	if _, err := srv.shareLinks.Import(in, ""); err != nil {
		t.Fatal(err)
	}
	// An unrelated write-capable grant still works (the gate is not broken).
	okTok := mintScopedTestGrant(t, srv, "ok-surface", ScopeManagedSessions, ScopeWrite, ScopeAdmin)
	if r := shareDo(t, ts, "POST", "/v1/identity/grants", GrantHeaderName, okTok, map[string]any{"surface": "ok-surface", "scope": []string{ScopeManagedSessions, ScopeWrite, ScopeAdmin}}); r.StatusCode != 200 {
		t.Fatalf("control: grant mint status = %d", r.StatusCode)
	}

	routes := []struct{ method, path string }{
		{"POST", "/v1/managed-sessions"},
		{"POST", "/mcp"},
		{"POST", "/v1/identity/grants"},
		{"POST", "/v1/identity/grants/current"},
		{"POST", "/v1/attention"},
		{"POST", "/v1/stage/keys/list"},
		{"POST", "/v1/stage/keys/import"},
	}
	carriers := []string{GrantHeaderName, "Authorization", "x-api-key"}
	for _, rt := range routes {
		for _, c := range carriers {
			tokVal := tok
			if c == "Authorization" {
				tokVal = "Bearer " + tok
			}
			resp := shareDo(t, ts, rt.method, rt.path, c, tokVal, map[string]any{"surface": "x"})
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s via %s = %d; want 403", rt.method, rt.path, c, resp.StatusCode)
			}
		}
	}
	// And verify never reports a share link as a grant.
	resp := shareDo(t, ts, "POST", "/v1/identity/verify", "", "", map[string]any{"surface": "stage/room/r1/owner", "token": tok})
	var v identityVerifyResponse
	identityDecodeBody(t, resp, &v)
	if v.Valid {
		t.Error("/v1/identity/verify reported a share link as a valid grant")
	}
}

func TestShareLink_RoutesNeedStageKeysScope(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	noScope := mintScopedTestGrant(t, srv, "plain", ScopeWrite, ScopeAdmin, ScopeManagedSessions)
	withScope := mintScopedTestGrant(t, srv, "stage-svc", ScopeStageKeys)

	for _, p := range []string{"/v1/stage/keys/import", "/v1/stage/keys/resolve", "/v1/stage/keys/list", "/v1/stage/keys/link-1/revoke"} {
		if r := shareDo(t, ts, "POST", p, GrantHeaderName, noScope, map[string]any{}); r.StatusCode != 403 {
			t.Errorf("%s without stage:keys = %d; want 403", p, r.StatusCode)
		}
		if r := shareDo(t, ts, "POST", p, "", "", map[string]any{}); r.StatusCode != 401 {
			t.Errorf("%s without grant = %d; want 401", p, r.StatusCode)
		}
	}
	// A GET on the prefix is not exempt either.
	if r := shareDo(t, ts, "GET", "/v1/stage/keys/list", "", "", nil); r.StatusCode == 200 {
		t.Error("GET under /v1/stage/keys must not be grant-exempt")
	}

	tokVal := "route-flow-token"
	imp := map[string]any{"links": []ShareLinkImport{shareImport(tokVal, "stage/room/r9/owner")}}
	var out struct{ Links []shareLinkView }
	identityDecodeBody(t, shareDo(t, ts, "POST", "/v1/stage/keys/import", GrantHeaderName, withScope, imp), &out)
	if len(out.Links) != 1 || out.Links[0].Parent == "" {
		t.Fatalf("import response = %+v", out)
	}
	var res struct {
		Valid bool
		Link  shareLinkView
	}
	identityDecodeBody(t, shareDo(t, ts, "POST", "/v1/stage/keys/resolve", GrantHeaderName, withScope, map[string]string{"token": tokVal}), &res)
	if !res.Valid || res.Link.Surface != "stage/room/r9/owner" {
		t.Fatalf("resolve = %+v", res)
	}
	resp := shareDo(t, ts, "POST", "/v1/stage/keys/"+out.Links[0].LinkID+"/revoke", GrantHeaderName, withScope, map[string]any{})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("revoke = %d", resp.StatusCode)
	}
	identityDecodeBody(t, shareDo(t, ts, "POST", "/v1/stage/keys/resolve", GrantHeaderName, withScope, map[string]string{"token": tokVal}), &res)
	if res.Valid {
		t.Error("revoked token still resolves over HTTP")
	}
	// No response carries the hash.
	listResp := shareDo(t, ts, "POST", "/v1/stage/keys/list", GrantHeaderName, withScope, map[string]any{})
	var buf bytes.Buffer
	buf.ReadFrom(listResp.Body)
	listResp.Body.Close()
	if strings.Contains(buf.String(), sha256Hex(tokVal)) {
		t.Error("list leaks an integrity hash")
	}
}
