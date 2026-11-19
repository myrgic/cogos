package engine

// Tests for the three K4 review follow-ups that had to close before OAuth is
// enabled:
//
//  1. D3 device binding is enforced where tokens are used (introspect, refresh,
//     first exchange), not only displayed in the grant list.
//  2. A code-replay revocation fails CLOSED when the ledger append fails.
//  3. A revocation that missed the ledger is queued, retried, and journaled, so
//     the next boot replay agrees with memory.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func refreshForm(refresh string) url.Values {
	return url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {refresh},
		"client_id": {testClientID}, "resource": {testResource},
	}
}

// failLedger makes every identity-oauth ledger append fail until restored.
func failLedger(env *oauthAuthzEnv) (restore func()) {
	prev := env.srv.oauth.appendEvent
	env.srv.oauth.appendEvent = func(string, string, *EventEnvelope) error { return errors.New("disk full") }
	return func() { env.srv.oauth.appendEvent = prev }
}

func (e *oauthAuthzEnv) grantsList(t *testing.T) (grants []map[string]any, unjournaled []map[string]any) {
	t.Helper()
	tok := mintScopedTestGrant(t, e.srv, "k4-followups-admin", ScopeAdmin)
	req, _ := http.NewRequest("GET", e.ts.URL+"/v1/oauth/grants", nil)
	req.Header.Set(GrantHeaderName, tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("grants list = %d %s", resp.StatusCode, b)
	}
	var out struct {
		Grants      []map[string]any `json:"grants"`
		Unjournaled []map[string]any `json:"unjournaled_revocations"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out.Grants, out.Unjournaled
}

// ─── finding 1: device binding enforced ──────────────────────────────────────

func TestOAuthK4F1_RevokedApproverDeviceKillsIntrospectionAndRefresh(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 200 {
		t.Fatalf("exchange = %d %s", c, b)
	}
	tb := tokenFields(t, b)

	// No false negative: the live device's grant introspects active.
	if in := env.introspect(t, tb.AccessToken); !in.Active {
		t.Fatalf("live device's grant introspected inactive: %s", in.Raw)
	}

	// Chaz unpairs the approving device.
	if _, err := env.srv.shareLinks.Revoke(env.deviceLink.LinkID); err != nil {
		t.Fatal(err)
	}

	in := env.introspect(t, tb.AccessToken)
	if in.Active {
		t.Fatalf("grant of a revoked device still introspects active: %s", in.Raw)
	}
	if got := strings.TrimSpace(in.Raw); got != `{"active":false}` {
		t.Errorf("inactive body = %q; a resource server that reads nothing else must see exactly active:false, no oracle", got)
	}

	// The refresh path is the actual exploit: a dead device's grant must not
	// keep minting tokens.
	c, b = env.tokenPost(t, refreshForm(tb.RefreshToken))
	if c != 400 || errorType(t, b) != "invalid_grant" {
		t.Fatalf("refresh under a revoked device = %d %s, want 400 invalid_grant", c, b)
	}
	// ... and the refusal did not consume the refresh token, so re-pairing the
	// same device (same hash) restores the connector without a new consent.
	env.pairDevice(t, testDeviceHash)
	if c, b := env.tokenPost(t, refreshForm(tb.RefreshToken)); c != 200 {
		t.Fatalf("refresh after the device came back = %d %s", c, b)
	}
}

func TestOAuthK4F1_DeviceRevokedBeforeFirstExchangeMintsNothing(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	if _, err := env.srv.shareLinks.Revoke(env.deviceLink.LinkID); err != nil {
		t.Fatal(err)
	}
	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 400 || errorType(t, b) != "invalid_grant" {
		t.Fatalf("exchange with the approver device gone = %d %s, want 400 invalid_grant", c, b)
	}
	if n := env.srv.oauth.Len(); n != 0 {
		t.Errorf("%d grants live after a refused exchange", n)
	}
}

// ─── finding 2: replay revocation fails closed ───────────────────────────────

func TestOAuthK4F2_CodeReplayWithLedgerDownFailsClosedAndLoud(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 200 {
		t.Fatalf("first exchange = %d %s", c, b)
	}
	first := tokenFields(t, b)
	if !env.introspect(t, first.AccessToken).Active {
		t.Fatal("first access token not active before the replay")
	}

	restore := failLedger(env)
	defer restore()
	c, b = env.tokenPost(t, exchangeForm(code)) // attacker replays the code
	// Not a clean "already used": the kernel cannot promise the revocation is
	// durable, and says so with a retryable 5xx.
	if c != http.StatusServiceUnavailable || errorType(t, b) != oauthErrTemporarilyUnavail {
		t.Fatalf("replay with ledger down = %d %s, want 503 temporarily_unavailable", c, b)
	}
	// Fail CLOSED: the grant is dead in memory regardless of the disk.
	if env.introspect(t, first.AccessToken).Active {
		t.Fatal("grant stayed live after a replay because the ledger append failed (fail-open)")
	}
	if c, b := env.tokenPost(t, refreshForm(first.RefreshToken)); c != 400 {
		t.Fatalf("refresh after replay = %d %s, want it refused", c, b)
	}
	// Loud: an operator can see exactly which revocation is not durable.
	_, unj := env.grantsList(t)
	if len(unj) != 1 || unj[0]["reason"] != "authorization code replayed" {
		t.Fatalf("unjournaled_revocations = %v, want the replay revocation", unj)
	}
}

func TestOAuthK4F2_CodeReplayWithLedgerUpIsCleanInvalidGrant(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	c, b := env.tokenPost(t, exchangeForm(code))
	if c != 400 || errorType(t, b) != "invalid_grant" || !strings.Contains(b, "already used") {
		t.Fatalf("replay = %d %s, want 400 invalid_grant 'already used'", c, b)
	}
	if env.introspect(t, first.AccessToken).Active {
		t.Error("replay did not revoke the first exchange's grant")
	}
	if _, unj := env.grantsList(t); len(unj) != 0 {
		t.Errorf("unjournaled = %v on a healthy ledger", unj)
	}
}

func TestOAuthK4F2_PKCEMismatchWithLedgerDownIsNotACleanRefusal(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	code, _ := env.approvedCode(t)
	restore := failLedger(env)
	defer restore()
	form := exchangeForm(code)
	form.Set("redirect_uri", "https://evil.example/cb")
	c, b := env.tokenPost(t, form)
	if c != http.StatusServiceUnavailable {
		t.Fatalf("mismatched exchange with ledger down = %d %s, want 503", c, b)
	}
	if env.srv.oauth.Len() != 0 {
		t.Error("grant left live after a failed exchange with the ledger down")
	}
}

// ─── finding 3: nothing stays un-journaled, and boot replay agrees ───────────

func TestOAuthK4F3_QueuedRevocationLandsAndSurvivesBootReplay(t *testing.T) {
	env := newOAuthAuthzEnv(t)
	root := env.srv.cfg.WorkspaceRoot
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	restore := failLedger(env)
	env.tokenPost(t, exchangeForm(code)) // replay; append fails; queued
	if n := len(env.srv.oauth.UnjournaledRevocations()); n != 1 {
		t.Fatalf("queued = %d, want 1", n)
	}

	// THE RESIDUAL GAP, pinned: a crash here (before any flush succeeds) loses
	// the revocation, because memory was the only record of it. Boot replay sees
	// the grant live. If this assertion starts failing the gap has been closed
	// (e.g. by a local spool); update the kernel doc and this test together.
	crashed, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := crashed.Resolve(first.AccessToken); !ok {
		t.Fatal("boot replay already agrees with memory before any flush: the residual crash-window gap is closed; update this test")
	}

	// While the ledger is still down a flush keeps the entry queued.
	if left := env.srv.oauthFlushJournal(); left != 1 {
		t.Fatalf("flush with ledger down left %d, want 1", left)
	}
	restore()
	if left := env.srv.oauthFlushJournal(); left != 0 {
		t.Fatalf("flush with ledger back left %d, want 0", left)
	}
	if n := len(env.srv.oauth.UnjournaledRevocations()); n != 0 {
		t.Fatalf("still queued: %d", n)
	}

	// After the flush, a restart agrees with memory: the grant stays dead.
	rebuilt, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rebuilt.Resolve(first.AccessToken); ok {
		t.Fatal("revoked grant resurrected on boot replay after the queued revocation was journaled")
	}
	if rebuilt.Len() != 0 {
		t.Errorf("%d grants after replay, want 0", rebuilt.Len())
	}
}

func TestOAuthK4F3_RefreshReuseWithLedgerDownIsQueuedToo(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("/nonexistent-root-for-test")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	g, p1 := mustGrantWithTokens(t, st)
	if _, err := st.Rotate(p1.RefreshToken); err != nil {
		t.Fatal(err)
	}
	st.appendEvent = func(string, string, *EventEnvelope) error { return errors.New("disk full") }
	if _, err := st.Rotate(p1.RefreshToken); !errors.Is(err, ErrOAuthRefreshReuse) {
		t.Fatalf("err = %v", err)
	}
	q := st.UnjournaledRevocations()
	if len(q) != 1 || q[0].GrantID != g.GrantID || q[0].Reason != "refresh_reuse" {
		t.Fatalf("queue = %+v", q)
	}
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	if left := st.FlushUnjournaled(); left != 0 {
		t.Fatalf("left = %d", left)
	}
}

func TestOAuthStore_RevokeFailClosedDropsBeforeLedger(t *testing.T) {
	t.Parallel()
	st, _ := newClockedOAuthStore("/nonexistent-root-for-test")
	st.appendEvent = func(string, string, *EventEnvelope) error { return nil }
	g, p := mustGrantWithTokens(t, st)
	st.appendEvent = func(string, string, *EventEnvelope) error { return errors.New("disk full") }
	if _, err := st.RevokeFailClosed(g.GrantID, "kernel", "x"); !errors.Is(err, ErrOAuthLedgerAppendFailed) {
		t.Fatalf("err = %v, want ErrOAuthLedgerAppendFailed", err)
	}
	if _, ok := st.Resolve(p.AccessToken); ok {
		t.Error("token live after fail-closed revoke")
	}
	if !st.IsToken(p.AccessToken) {
		t.Error("revoked token lost its tombstone")
	}
	if _, err := st.RevokeFailClosed(g.GrantID, "kernel", "x"); !errors.Is(err, ErrOAuthGrantNotFound) {
		t.Errorf("second revoke err = %v, want not found", err)
	}
	if n := len(st.UnjournaledRevocations()); n != 1 {
		t.Errorf("queued = %d, want 1 (a second revoke must not double-queue)", n)
	}
}

// ─── S1: a clean shutdown must not skip the last flush ───────────────────────

func TestOAuthK4S1_ShutdownFlushesAnyStillQueuedRevocation(t *testing.T) {
	// k4-followups-review S1: the flusher only ran oauthFlushJournal() on its 15s ticker, so a CLEAN
	// restart (ctx cancelled, e.g. by an operator restarting to pick up a ledger fix) inside that window
	// left a revocation queued -- exactly the scenario the review reproduced resurrecting a dead grant on
	// boot replay. The fix: one last oauthFlushJournal() on ctx.Done() before the goroutine returns.
	env := newOAuthAuthzEnv(t)
	root := env.srv.cfg.WorkspaceRoot
	code, _ := env.approvedCode(t)
	_, b := env.tokenPost(t, exchangeForm(code))
	first := tokenFields(t, b)

	restore := failLedger(env)
	env.tokenPost(t, exchangeForm(code)) // replay; append fails; queued in memory only
	if n := len(env.srv.oauth.UnjournaledRevocations()); n != 1 {
		t.Fatalf("queued = %d, want 1", n)
	}

	// Ledger recovers, THEN the process is asked to stop before the next 15s tick would have fired --
	// the real startOAuthJournalFlusher ticker interval is far longer than this test should wait.
	restore()
	ctx, cancel := context.WithCancel(context.Background())
	// k4-delta-review4: a WaitGroup must be created fresh per generation, never shared/reused across
	// restarts (a shared one panics: "WaitGroup is reused before previous Wait has returned" if a start
	// races a still-waiting stop). This test creates its own, exactly as the real startBackground now does.
	wg := new(sync.WaitGroup)
	env.srv.startOAuthJournalFlusher(ctx, wg, time.Hour) // a tick that will never fire naturally in this test
	cancel()
	// k4-delta-review: polling with a timeout HID the real race -- stopBackground() used to return the
	// instant cancel() fired, before the goroutine's ctx.Done() branch had actually run its flush, so a
	// real shutdown could exit before the write landed. wg.Wait() is the same join stopBackground() now
	// does (on THIS generation's WaitGroup); asserting immediately after it returns (no sleep, no
	// polling) is the synchronous check that would have caught the original bug.
	wg.Wait()
	if n := len(env.srv.oauth.UnjournaledRevocations()); n != 0 {
		t.Fatalf("still queued immediately after the flusher's WaitGroup was joined: %d, want 0 (the shutdown flush did not land before the join returned)", n)
	}

	rebuilt, err := RebuildOAuthStoreFromLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rebuilt.Resolve(first.AccessToken); ok {
		t.Fatal("revoked grant resurrected on boot replay: the shutdown flush did not actually journal it")
	}
}

// TestOAuthK4_StartStopBackgroundConcurrentRestartDoesNotPanic is k4-delta-review4: a SINGLE shared
// WaitGroup reused across restarts panics ("WaitGroup is reused before previous Wait has returned") if a
// startBackground races a stopBackground still waiting on the PRIOR generation's loops. The fix makes
// each generation's WaitGroup its own *sync.WaitGroup (bgWG), allocated fresh in startBackground and read
// once under reaperMu by stopBackground before it unlocks and waits -- so a concurrent restart's new
// WaitGroup is never the one an in-flight stop is blocked on. Hammer both calls concurrently and confirm
// neither a panic nor a hang occurs across many iterations (the review reproduced the panic within ~15).
func TestOAuthK4_StartStopBackgroundConcurrentRestartDoesNotPanic(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.OAuth = OAuthConfig{Enabled: true, Issuer: "https://stage.example.test", Resource: testResource}
	srv := NewServer(cfg, makeNucleus("Test", "unit-tester"), NewProcess(cfg, makeNucleus("Test", "unit-tester")))

	const iterations = 500
	done := make(chan struct{})
	go func() {
		for i := 0; i < iterations; i++ {
			srv.startBackground()
		}
		close(done)
	}()
	for i := 0; i < iterations; i++ {
		srv.stopBackground()
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("startBackground goroutine did not finish -- possible hang from a shared WaitGroup")
	}
	srv.stopBackground() // final stop, so no background loop is left running when the test ends
}
