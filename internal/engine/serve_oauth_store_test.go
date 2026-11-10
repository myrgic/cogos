package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func oauthTestInput() OAuthGrantInput {
	return OAuthGrantInput{
		ClientID: "https://claude.ai/oauth/mcp-oauth-client-metadata",
		Actor:    "chaz",
		Delegate: "claude-web",
		Scope:    []string{OAuthScopeMCPConnector},
		Resource: "https://stage.myrgic.com/mcp",
		Consent: OAuthConsent{
			RequestID:      "req-1",
			CodeHash:       sha256Hex("the-auth-code"),
			ApprovedVia:    "paired-device",
			ApproverDevice: sha256Hex("device-cookie-1"),
		},
	}
}

// oauthClock is a settable clock for the store.
type oauthClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *oauthClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *oauthClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClockedOAuthStore(root string) (*OAuthStore, *oauthClock) {
	st := NewOAuthStoreWithLedger(root)
	// Start at the real time: RebuildOAuthStoreFromLedger evicts against the wall clock.
	c := &oauthClock{t: time.Now().UTC().Truncate(time.Second)}
	st.now = c.now
	return st, c
}

func mustGrantWithTokens(t *testing.T, st *OAuthStore) (*OAuthGrant, *OAuthTokenPair) {
	t.Helper()
	g, err := st.CreateGrant(oauthTestInput())
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.IssueTokens(g.GrantID)
	if err != nil {
		t.Fatal(err)
	}
	return g, p
}

func TestOAuthStore_IssueAndResolve(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, p := mustGrantWithTokens(t, st)
	if !strings.HasPrefix(p.AccessToken, OAuthAccessTokenPrefix) || !strings.HasPrefix(p.RefreshToken, OAuthRefreshTokenPrefix) {
		t.Fatalf("token prefixes wrong: %q %q", p.AccessToken, p.RefreshToken)
	}
	if len(p.AccessToken) != len(OAuthAccessTokenPrefix)+43 {
		t.Errorf("access token length = %d", len(p.AccessToken))
	}
	info, ok := st.Resolve(p.AccessToken)
	if !ok || info.Grant.GrantID != g.GrantID || info.Token.Kind != OAuthTokenKindAccess {
		t.Fatalf("resolve = %+v ok=%v", info, ok)
	}
	if info.Grant.Actor != "chaz" || info.Grant.Delegate != "claude-web" || !info.Grant.HasScope(OAuthScopeMCPConnector) {
		t.Errorf("grant fields wrong: %+v", info.Grant)
	}
	if !info.Token.ExpiresAt.Equal(p.AccessExpiresAt) {
		t.Errorf("access exp %v != pair %v", info.Token.ExpiresAt, p.AccessExpiresAt)
	}
	if _, ok := st.Resolve(p.RefreshToken); ok {
		t.Error("a refresh token resolved as an access token")
	}
	if _, ok := st.Resolve("cgat_nope"); ok {
		t.Error("unknown token resolved")
	}
	if _, ok := st.Resolve(""); ok {
		t.Error("empty token resolved")
	}
	if !st.IsToken(p.AccessToken) || !st.IsToken(p.RefreshToken) || st.IsToken("cgat_nope") || st.IsToken("plain") {
		t.Error("IsToken classification wrong")
	}
}

func TestOAuthStore_InitialTokensOnlyOnce(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, _ := mustGrantWithTokens(t, st)
	if _, err := st.IssueTokens(g.GrantID); !errors.Is(err, ErrOAuthTokensAlreadyIssued) {
		t.Fatalf("second IssueTokens err = %v", err)
	}
	if _, err := st.IssueTokens("oag-missing"); !errors.Is(err, ErrOAuthGrantNotFound) {
		t.Fatalf("unknown grant err = %v", err)
	}
}

func TestOAuthStore_ValidationRejects(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	mut := []func(*OAuthGrantInput){
		func(in *OAuthGrantInput) { in.ClientID = "" },
		func(in *OAuthGrantInput) { in.Actor = "Chaz Spaces" },
		func(in *OAuthGrantInput) { in.Delegate = "" },
		func(in *OAuthGrantInput) { in.Scope = nil },
		func(in *OAuthGrantInput) { in.Scope = []string{"not a scope"} },
		func(in *OAuthGrantInput) { in.Consent.ApproverDevice = "raw-cookie-not-a-hash" },
		func(in *OAuthGrantInput) { in.Consent.CodeHash = "xyz" },
		func(in *OAuthGrantInput) { in.MaxAge = -time.Second },
	}
	for i, m := range mut {
		in := oauthTestInput()
		m(&in)
		if _, err := st.CreateGrant(in); err == nil {
			t.Errorf("case %d: invalid input accepted", i)
		}
	}
	if st.Len() != 0 {
		t.Errorf("len = %d after rejected grants", st.Len())
	}
}

func TestOAuthStore_AccessTokenExpires(t *testing.T) {
	t.Parallel()
	st, clk := newClockedOAuthStore("")
	_, p := mustGrantWithTokens(t, st)
	clk.advance(DefaultOAuthAccessTTL - time.Second)
	if _, ok := st.Resolve(p.AccessToken); !ok {
		t.Fatal("access token dead before its TTL")
	}
	clk.advance(2 * time.Second)
	if _, ok := st.Resolve(p.AccessToken); ok {
		t.Fatal("access token live past its TTL")
	}
}

func TestOAuthStore_RotateIssuesNewPairAndOldRefreshIsConsumed(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, p1 := mustGrantWithTokens(t, st)
	p2, err := st.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if p2.GrantID != g.GrantID || p2.AccessToken == p1.AccessToken || p2.RefreshToken == p1.RefreshToken {
		t.Fatalf("rotation did not mint fresh tokens: %+v", p2)
	}
	if _, ok := st.Resolve(p2.AccessToken); !ok {
		t.Error("new access token does not resolve")
	}
	// The old access token stays valid until its own expiry (standard); it is
	// the refresh token that is single-use.
	if _, ok := st.Resolve(p1.AccessToken); !ok {
		t.Error("old access token died on rotation; only the refresh token is single-use")
	}
	// The new refresh token rotates again.
	if _, err := st.Rotate(p2.RefreshToken); err != nil {
		t.Fatalf("second rotation: %v", err)
	}
}

func TestOAuthStore_RefreshReuseRevokesWholeGrant(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, p1 := mustGrantWithTokens(t, st)
	p2, err := st.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	// Replay of the consumed refresh token.
	_, err = st.Rotate(p1.RefreshToken)
	if !errors.Is(err, ErrOAuthRefreshReuse) {
		t.Fatalf("reuse err = %v; want ErrOAuthRefreshReuse", err)
	}
	if _, ok := st.Grant(g.GrantID); ok {
		t.Error("grant survived refresh reuse")
	}
	for name, tok := range map[string]string{"p1 access": p1.AccessToken, "p2 access": p2.AccessToken} {
		if _, ok := st.Resolve(tok); ok {
			t.Errorf("%s still resolves after family revoke", name)
		}
	}
	// The legitimate newest refresh token is dead too.
	if _, err := st.Rotate(p2.RefreshToken); !errors.Is(err, ErrOAuthInvalidGrant) {
		t.Errorf("p2 refresh err = %v; want ErrOAuthInvalidGrant", err)
	}
	if st.Len() != 0 {
		t.Errorf("len = %d", st.Len())
	}
}

func TestOAuthStore_RotateRejectsBadTokens(t *testing.T) {
	t.Parallel()
	st, clk := newClockedOAuthStore("")
	_, p := mustGrantWithTokens(t, st)
	for name, tok := range map[string]string{
		"empty":        "",
		"unknown":      "cgrt_unknown",
		"access token": p.AccessToken,
		"no prefix":    "whatever",
		"wrong-prefix": strings.Replace(p.RefreshToken, OAuthRefreshTokenPrefix, "xxxxx", 1),
	} {
		if _, err := st.Rotate(tok); !errors.Is(err, ErrOAuthInvalidGrant) {
			t.Errorf("%s: err = %v; want ErrOAuthInvalidGrant", name, err)
		}
	}
	// A bad token never touches the real grant.
	if st.Len() != 1 {
		t.Fatalf("len = %d", st.Len())
	}
	clk.advance(DefaultOAuthRefreshTTL + time.Second)
	if _, err := st.Rotate(p.RefreshToken); !errors.Is(err, ErrOAuthInvalidGrant) {
		t.Errorf("expired refresh err = %v", err)
	}
}

func TestOAuthStore_GrantAbsoluteCapBeatsRotation(t *testing.T) {
	t.Parallel()
	st, clk := newClockedOAuthStore("")
	in := oauthTestInput()
	in.MaxAge = 2 * time.Hour
	g, err := st.CreateGrant(in)
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.IssueTokens(g.GrantID)
	if err != nil {
		t.Fatal(err)
	}
	if p.RefreshExpiresAt.After(g.ExpiresAt) || p.AccessExpiresAt.After(g.ExpiresAt) {
		t.Fatalf("token outlives its grant: %v %v > %v", p.AccessExpiresAt, p.RefreshExpiresAt, g.ExpiresAt)
	}
	clk.advance(90 * time.Minute)
	p2, err := st.Rotate(p.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if p2.AccessExpiresAt.After(g.ExpiresAt) {
		t.Errorf("rotated access token outlives grant cap")
	}
	clk.advance(31 * time.Minute) // past the cap
	if _, ok := st.Resolve(p2.AccessToken); ok {
		t.Error("access token resolves past the grant's absolute cap")
	}
	if _, err := st.Rotate(p2.RefreshToken); !errors.Is(err, ErrOAuthInvalidGrant) {
		t.Errorf("rotate past cap err = %v", err)
	}
	if _, ok := st.Grant(g.GrantID); ok {
		t.Error("expired grant still listed")
	}
}

func TestOAuthStore_RevokeKillsEveryChild(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, p1 := mustGrantWithTokens(t, st)
	p2, err := st.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	// A second, unrelated grant must be untouched.
	other, op := mustGrantWithTokens(t, st)

	if _, err := st.Revoke(g.GrantID, "admin", "test"); err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{p1.AccessToken, p2.AccessToken} {
		if _, ok := st.Resolve(tok); ok {
			t.Error("access token survived grant revoke")
		}
	}
	for _, tok := range []string{p1.RefreshToken, p2.RefreshToken} {
		if _, err := st.Rotate(tok); !errors.Is(err, ErrOAuthInvalidGrant) {
			t.Errorf("refresh after revoke err = %v", err)
		}
		// IsToken still names a revoked grant's token (refused by name at the
		// gate, tombstoned hash only). Resolve/Rotate above are what make it dead.
		if !st.IsToken(tok) {
			t.Error("IsToken lost a revoked grant's token: the gate would stop naming it")
		}
	}
	if _, ok := st.Resolve(op.AccessToken); !ok {
		t.Error("unrelated grant's token died")
	}
	if _, err := st.Revoke(g.GrantID, "admin", "again"); !errors.Is(err, ErrOAuthGrantNotFound) {
		t.Errorf("second revoke err = %v", err)
	}
	if got := st.Snapshot(); len(got) != 1 || got[0].GrantID != other.GrantID {
		t.Errorf("snapshot = %+v", got)
	}
}

func TestOAuthStore_SnapshotOldestFirst(t *testing.T) {
	t.Parallel()
	st, clk := newClockedOAuthStore("")
	var ids []string
	for i := 0; i < 3; i++ {
		g, err := st.CreateGrant(oauthTestInput())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, g.GrantID)
		clk.advance(time.Minute)
	}
	got := st.Snapshot()
	if len(got) != 3 {
		t.Fatalf("len = %d", len(got))
	}
	for i := range ids {
		if got[i].GrantID != ids[i] {
			t.Errorf("snapshot[%d] = %s; want %s", i, got[i].GrantID, ids[i])
		}
	}
}

func TestOAuthStore_DeviceBindingCrossCheck(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	g, _ := mustGrantWithTokens(t, st)
	if !g.ApprovedByDevice(sha256Hex("device-cookie-1")) {
		t.Error("approving device did not match")
	}
	if g.ApprovedByDevice(sha256Hex("device-cookie-2")) || g.ApprovedByDevice("") {
		t.Error("another device matched")
	}

	links := NewShareLinkStoreWithLedger("")
	if g.ApproverDeviceLive(links) || g.ApproverDeviceLive(nil) {
		t.Error("device live with no link in the mirror")
	}
	l, err := links.Import(ShareLinkImport{
		Kind: ShareLinkKindDevice, Surface: "stage/device/d1", Actor: "chaz", Role: "owner",
		Scope: []string{"stage:device"}, IntegrityHash: sha256Hex("device-cookie-1"),
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !g.ApproverDeviceLive(links) {
		t.Error("live device link not recognised")
	}
	if _, err := links.Revoke(l.LinkID); err != nil {
		t.Fatal(err)
	}
	if g.ApproverDeviceLive(links) {
		t.Error("revoked device still counts as live")
	}

	// A share (non-device) link or another actor's link is not a device approval.
	if _, err := links.Import(ShareLinkImport{
		Kind: ShareLinkKindShare, Surface: "stage/home/p1", Actor: "chaz", Role: "owner",
		Scope: []string{"stage:home"}, IntegrityHash: sha256Hex("device-cookie-1"),
	}, ""); err != nil {
		t.Fatal(err)
	}
	if g.ApproverDeviceLive(links) {
		t.Error("a non-device link satisfied the device check")
	}

	in := oauthTestInput()
	in.Consent.ApproverDevice = ""
	noDev, err := st.CreateGrant(in)
	if err != nil {
		t.Fatal(err)
	}
	if noDev.ApproverDeviceLive(links) {
		t.Error("grant with no recorded device passed the device check")
	}
}

func TestOAuthPKCE_S256(t *testing.T) {
	t.Parallel()
	// RFC 7636 appendix B.
	const verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	const challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
	if got := OAuthPKCEChallengeS256(verifier); got != challenge {
		t.Fatalf("challenge = %s; want %s", got, challenge)
	}
	if !OAuthPKCEVerifyS256(challenge, verifier) {
		t.Error("RFC 7636 vector rejected")
	}
	if OAuthPKCEVerifyS256(challenge, verifier+"x") {
		t.Error("wrong verifier accepted")
	}
	if OAuthPKCEVerifyS256(verifier, verifier) {
		t.Error("plain method accepted (challenge == verifier)")
	}
	if OAuthPKCEVerifyS256("", "") || OAuthPKCEVerifyS256(challenge, "short") {
		t.Error("empty / too-short verifier accepted")
	}
}

func oauthLedgerBytes(t *testing.T, root string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, ".cog", "ledger", oauthLedgerSession, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestOAuthStore_LedgerHoldsHashesNeverRawTokens(t *testing.T) {
	root := t.TempDir()
	st, _ := newClockedOAuthStore(root)
	_, p1 := mustGrantWithTokens(t, st)
	p2, err := st.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	raw := oauthLedgerBytes(t, root)
	for name, tok := range map[string]string{
		"access1": p1.AccessToken, "refresh1": p1.RefreshToken,
		"access2": p2.AccessToken, "refresh2": p2.RefreshToken,
	} {
		if strings.Contains(raw, tok) {
			t.Errorf("ledger contains the raw %s", name)
		}
		// The token body (after the prefix) must not appear either.
		if strings.Contains(raw, tok[5:]) {
			t.Errorf("ledger contains the %s secret body", name)
		}
		if !strings.Contains(raw, sha256Hex(tok)) {
			t.Errorf("ledger lacks the hash of %s", name)
		}
	}
	for _, ev := range []string{"identity.oauth.grant.issued", "identity.oauth.token.issued", "identity.oauth.token.refreshed"} {
		if !strings.Contains(raw, ev) {
			t.Errorf("ledger lacks %s", ev)
		}
	}
	if !strings.Contains(raw, sha256Hex("device-cookie-1")) || strings.Contains(raw, "device-cookie-1\"") {
		t.Error("device binding not recorded as a hash")
	}
}

func TestOAuthStore_RestartRebuildsFromLedger(t *testing.T) {
	root := t.TempDir()
	st, clk := newClockedOAuthStore(root)
	g, p1 := mustGrantWithTokens(t, st)
	p2, err := st.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	gone, gp := mustGrantWithTokens(t, st)
	if _, err := st.Revoke(gone.GrantID, "admin", "restart-test"); err != nil {
		t.Fatal(err)
	}

	re, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	re.now = clk.now
	if re.Len() != 1 {
		t.Fatalf("rebuilt len = %d; want 1", re.Len())
	}
	rg, ok := re.Grant(g.GrantID)
	if !ok {
		t.Fatal("grant lost across restart")
	}
	if rg.Actor != "chaz" || rg.Delegate != "claude-web" || rg.Resource != "https://stage.myrgic.com/mcp" ||
		!rg.HasScope(OAuthScopeMCPConnector) || rg.Consent.ApproverDevice != sha256Hex("device-cookie-1") ||
		rg.Consent.ApprovedVia != "paired-device" || rg.Consent.RequestID != "req-1" {
		t.Errorf("rebuilt grant fields wrong: %+v", rg)
	}
	if !rg.ExpiresAt.Equal(g.ExpiresAt) {
		t.Errorf("rebuilt cap %v != %v", rg.ExpiresAt, g.ExpiresAt)
	}
	if _, ok := re.Resolve(p2.AccessToken); !ok {
		t.Error("current access token lost across restart")
	}
	if _, ok := re.Resolve(gp.AccessToken); ok {
		t.Error("revoked grant's token came back after restart")
	}
	if _, err := re.IssueTokens(g.GrantID); !errors.Is(err, ErrOAuthTokensAlreadyIssued) {
		t.Errorf("initial-issue marker lost across restart: %v", err)
	}
	// Rotation state is restart-safe: the consumed refresh token is still
	// recognised as consumed, so replaying it after a restart is detected.
	if _, err := re.Rotate(p1.RefreshToken); !errors.Is(err, ErrOAuthRefreshReuse) {
		t.Fatalf("reuse after restart err = %v; want ErrOAuthRefreshReuse", err)
	}
	if re.Len() != 0 {
		t.Error("grant survived reuse detected after restart")
	}

	// And that reuse was ledgered: a second rebuild agrees the grant is gone.
	re2, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if re2.Len() != 0 {
		t.Errorf("second rebuild len = %d; reuse revocation not durable", re2.Len())
	}
	raw := oauthLedgerBytes(t, root)
	for _, ev := range []string{"identity.oauth.refresh.reuse_detected", "identity.oauth.grant.revoked"} {
		if !strings.Contains(raw, ev) {
			t.Errorf("ledger lacks %s", ev)
		}
	}
}

func TestOAuthStore_RebuildLiveRotationThenRefreshWorks(t *testing.T) {
	root := t.TempDir()
	st, clk := newClockedOAuthStore(root)
	_, p1 := mustGrantWithTokens(t, st)
	re, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	re.now = clk.now
	p2, err := re.Rotate(p1.RefreshToken)
	if err != nil {
		t.Fatalf("rotate after restart: %v", err)
	}
	if _, ok := re.Resolve(p2.AccessToken); !ok {
		t.Error("post-restart rotated token does not resolve")
	}
}

func TestOAuthStore_RebuildMissingLedgerAndMalformedLines(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st, err := RebuildOAuthStoreFromLedger(root)
	if err != nil || st.Len() != 0 {
		t.Fatalf("missing ledger: len=%d err=%v", st.Len(), err)
	}
	dir := filepath.Join(root, ".cog", "ledger", oauthLedgerSession)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte("not json\n\n{\"also\":\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err = RebuildOAuthStoreFromLedger(root)
	if err != nil || st.Len() != 0 {
		t.Fatalf("malformed ledger: len=%d err=%v", st.Len(), err)
	}
}

func TestOAuthStore_LedgerFailureLeavesMemoryUntouched(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("/nonexistent-root-for-test")
	fail := errors.New("disk full")
	st.appendEvent = func(string, string, *EventEnvelope) error { return fail }
	if _, err := st.CreateGrant(oauthTestInput()); !errors.Is(err, ErrOAuthLedgerAppendFailed) {
		t.Fatalf("create err = %v", err)
	}
	if st.Len() != 0 {
		t.Fatal("grant stored despite ledger failure")
	}

	// Let the grant and first pair land, then fail later mutations.
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	g, p := mustGrantWithTokens(t, st)
	st.appendEvent = func(string, string, *EventEnvelope) error { return fail }
	if _, err := st.Rotate(p.RefreshToken); !errors.Is(err, ErrOAuthLedgerAppendFailed) {
		t.Fatalf("rotate err = %v", err)
	}
	// Failed rotation did not consume the refresh token.
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	if _, err := st.Rotate(p.RefreshToken); err != nil {
		t.Fatalf("refresh consumed by a failed rotation: %v", err)
	}
	st.appendEvent = func(string, string, *EventEnvelope) error { return fail }
	if _, err := st.Revoke(g.GrantID, "admin", "x"); !errors.Is(err, ErrOAuthLedgerAppendFailed) {
		t.Fatalf("revoke err = %v", err)
	}
	if _, ok := st.Grant(g.GrantID); !ok {
		t.Error("grant dropped despite failed revoke append")
	}
}

func TestOAuthStore_ReuseFailsClosedEvenWhenLedgerFails(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("/nonexistent-root-for-test")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	g, p1 := mustGrantWithTokens(t, st)
	if _, err := st.Rotate(p1.RefreshToken); err != nil {
		t.Fatal(err)
	}
	st.appendEvent = func(string, string, *EventEnvelope) error { return errors.New("disk full") }
	_, err := st.Rotate(p1.RefreshToken)
	if !errors.Is(err, ErrOAuthRefreshReuse) || !errors.Is(err, ErrOAuthLedgerAppendFailed) {
		t.Fatalf("err = %v; want reuse joined with ledger failure", err)
	}
	if _, ok := st.Grant(g.GrantID); ok {
		t.Error("compromised grant stayed live because the ledger was down")
	}
}

func TestOAuthStore_CapacityBounded(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	for i := 0; i < maxOAuthGrants; i++ {
		if _, err := st.CreateGrant(oauthTestInput()); err != nil {
			t.Fatalf("grant %d: %v", i, err)
		}
	}
	if _, err := st.CreateGrant(oauthTestInput()); !errors.Is(err, ErrOAuthStoreAtCapacity) {
		t.Fatalf("over-capacity err = %v", err)
	}
}

func TestOAuthStore_ConcurrentRotateOnlyOneWins(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("")
	_, p := mustGrantWithTokens(t, st)
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, reuse int
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.Rotate(p.RefreshToken)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrOAuthRefreshReuse), errors.Is(err, ErrOAuthInvalidGrant):
				reuse++
			}
		}()
	}
	wg.Wait()
	if ok != 1 || reuse != n-1 {
		t.Fatalf("ok=%d reuse/invalid=%d; want exactly one winner", ok, reuse)
	}
}
