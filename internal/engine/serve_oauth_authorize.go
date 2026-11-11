// serve_oauth_authorize.go — GET /oauth/authorize, the consent request flow
// (Phase 3 OAuth, slice K3).
//
// The flow, and where each trust decision is made:
//
//  1. The connector's browser lands on GET /oauth/authorize. The kernel
//     validates the request (response_type=code, S256 PKCE, exact
//     redirect_uri, RFC 8707 resource, scope), fetches the client's CIMD
//     document under the SSRF guard (serve_oauth_cimd.go) and creates a
//     PENDING authorization request. It does NOT consent. It renders a page
//     that shows a short code and says "approve on your paired device".
//  2. The kernel POSTs the pending request to the stage (oauth.stage_notify_url,
//     loopback). The stage raises a "waiting on you" item on Chaz's paired
//     device. If the stage cannot be reached the request is dropped and the
//     page says so: nothing is approved by default, and a request nobody can
//     see is not left to be approved by accident.
//  3. Chaz approves on the paired device. The stage's consent executor calls
//     POST /v1/oauth/requests/{id}/approve with its studio grant. That route
//     needs oauth:consent (serve_grant_auth.go); a connector has no kernel
//     grant and could never hold it. Approve records the paired-device hash,
//     creates the OAuth grant (K1 store) and mints a single-use authorization
//     code whose hash is bound into Consent.CodeHash. The raw code goes only
//     to the browser poller, never back to the stage.
//  4. The page polls GET /oauth/authorize/status/{id} (secret in a header).
//     On approval it receives the redirect (code + state) exactly once and
//     navigates to the client's redirect_uri. /token (K4) redeems the code via
//     oauthASState.redeemCode.
//
// Why the browser cannot approve itself: the poll secret only reads status;
// approval is a different route behind a different credential. Why a cookie
// is not used at /authorize: the stage's device cookie is SameSite=Strict, so
// a cross-site navigation from claude.ai never carries it (plan F4).
//
// Pending requests live in memory only. They are 10-minute, capped and
// rate-limited; nothing is durable until approve creates the grant, and that
// write goes through the ledger-backed OAuthStore.
package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	oauthRequestLifetime = 10 * time.Minute
	oauthCodeLifetime    = 60 * time.Second
	oauthMaxPending      = 50
	oauthMaxRequests     = 200 // pending + settled-but-retained
	oauthRequestRetain   = 5 * time.Minute
	oauthPollWait        = 25 * time.Second
	oauthCreateLimit     = 60
	oauthCreateWindow    = time.Minute
	oauthNotifyTimeout   = 3 * time.Second
	oauthStateMax        = 512
	oauthCodeBytes       = 32
	oauthUserCodeLen     = 6

	// OAuthAuthCodePrefix marks an authorization code (not secret).
	OAuthAuthCodePrefix = "cgac_"

	// OAuthApprovedViaPairedDevice is the only consent channel K3 accepts.
	OAuthApprovedViaPairedDevice = "paired-device"

	// oauthPollSecretHeader carries the browser's poll secret. A header, not a
	// query parameter, so it stays out of access logs.
	oauthPollSecretHeader = "X-Poll-Secret"

	// oauthActor / oauthDelegate: whom the grant acts for, and who holds it.
	oauthActor    = "chaz"
	oauthDelegate = "claude-web"
)

var oauthChallengeRe = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

type oauthReqState int

const (
	oauthReqPending oauthReqState = iota
	oauthReqApproved
	oauthReqDenied
	oauthReqExpired
)

func (s oauthReqState) String() string {
	return [...]string{"pending", "approved", "denied", "expired"}[s]
}

// oauthAuthRequest is one pending (or settled) authorization request.
type oauthAuthRequest struct {
	id          string
	userCode    string
	secretH     [sha256.Size]byte
	clientID    string
	clientName  string
	redirectURI string
	state       string // the client's state param, echoed back
	challenge   string
	resource    string
	scope       []string
	created     time.Time
	expires     time.Time

	status    oauthReqState
	done      chan struct{} // closed on any transition out of pending
	delivered bool          // the outcome has been handed to the poller

	// Set on approve.
	rawCode     string // cleared once delivered
	codeHash    string
	codeExpires time.Time
	grantID     string
	device      string
	approver    string
	redeemed    bool
	revoked     bool // the unredeemed grant has been revoked
	decidedAt   time.Time
	purgeAt     time.Time
}

// oauthASState is the authorization-endpoint state of a Server: pending
// requests, the CIMD fetcher and the knobs tests override.
type oauthASState struct {
	mu       sync.Mutex
	reqs     map[string]*oauthAuthRequest
	byCode   map[string]*oauthAuthRequest // by code hash, approved requests only
	limiter  *grantMintLimiter
	cimd     *oauthClientMetadataFetcher
	now      func() time.Time
	pollWait time.Duration
	// notify delivers the pending request to the stage. nil means the HTTP
	// POST to oauth.stage_notify_url.
	notify func(ctx context.Context, url string, p oauthConsentNotice) error
}

func newOAuthASState() *oauthASState {
	return &oauthASState{
		reqs:     make(map[string]*oauthAuthRequest),
		byCode:   make(map[string]*oauthAuthRequest),
		limiter:  newGrantMintLimiter(oauthCreateLimit, oauthCreateWindow),
		cimd:     newOAuthClientMetadataFetcher(),
		now:      func() time.Time { return time.Now().UTC() },
		pollWait: oauthPollWait,
	}
}

// asState returns the Server's authorization-endpoint state, built on first
// use so a Server assembled without NewServer (tests) still works.
func (s *Server) asState() *oauthASState {
	s.oauthASOnce.Do(func() {
		if s.oauthAS == nil {
			s.oauthAS = newOAuthASState()
		}
	})
	return s.oauthAS
}

// oauthConsentNotice is what the stage receives on stage_notify_url. It holds
// nothing that could complete the flow: no poll secret, no code.
type oauthConsentNotice struct {
	RequestID  string   `json:"request_id"`
	UserCode   string   `json:"user_code"`
	ClientID   string   `json:"client_id"`
	ClientName string   `json:"client_name"`
	Resource   string   `json:"resource"`
	Scope      []string `json:"scope"`
	ExpiresAt  string   `json:"expires_at"`
}

// ─── lifecycle ───────────────────────────────────────────────────────────────

// pruneLocked advances time-driven state: pending requests past their expiry
// become expired, approved codes never redeemed have their grant revoked, and
// retained records past their purge time are dropped. revoke may be nil.
func (st *oauthASState) pruneLocked(revoke func(grantID, reason string)) {
	now := st.now()
	for id, r := range st.reqs {
		if r.status == oauthReqPending && !now.Before(r.expires) {
			r.status = oauthReqExpired
			r.decidedAt = now
			r.purgeAt = now.Add(oauthRequestRetain)
			close(r.done)
		}
		if r.status == oauthReqApproved && !r.redeemed && !r.revoked && !now.Before(r.codeExpires) {
			// An approved grant whose code was never exchanged must not linger.
			r.revoked = true
			r.rawCode = ""
			if revoke != nil && r.grantID != "" {
				revoke(r.grantID, "authorization code expired unredeemed")
			}
		}
		if !r.purgeAt.IsZero() && now.After(r.purgeAt) {
			delete(st.reqs, id)
			if r.codeHash != "" {
				delete(st.byCode, r.codeHash)
			}
		}
	}
}

// oauthRevokeUnredeemed is the pruneLocked / redeemCode callback shape; it drops
// the error because those callers have nobody to report it to. Every failure is
// still loud (oauthRevokeFailClosed logs at Error) and still queued for retry.
func (s *Server) oauthRevokeUnredeemed(grantID, reason string) {
	_ = s.oauthRevokeFailClosed(grantID, reason)
}

// oauthRevokeFailClosed revokes a grant the KERNEL decided to kill (code
// replay, failed exchange, unredeemed code, failed token issue). The grant is
// dead in memory before the ledger is touched (OAuthStore.RevokeFailClosed), so
// a ledger hiccup can never leave it live. An append failure is returned as
// ErrOAuthLedgerAppendFailed so the /token response can refuse to claim a clean
// outcome; the revocation is queued and retried by the journal flusher. A grant
// that is already gone is not an error.
func (s *Server) oauthRevokeFailClosed(grantID, reason string) error {
	if s.oauth == nil {
		return nil
	}
	_, err := s.oauth.RevokeFailClosed(grantID, "kernel", reason)
	switch {
	case err == nil, errors.Is(err, ErrOAuthGrantNotFound):
		return nil
	case errors.Is(err, ErrOAuthLedgerAppendFailed):
		slog.Error("oauth: SECURITY revocation is in force in memory but NOT journaled; queued for retry. "+
			"A restart before the ledger recovers would resurrect this grant.",
			"event", "oauth.revocation_unjournaled", "grant_id", grantID, "reason", reason, "err", err)
	default:
		slog.Error("oauth: revoking a grant failed", "grant_id", grantID, "reason", reason, "err", err)
	}
	return err
}

// oauthJournalFlushInterval is how often queued, unjournaled revocations are
// retried against the ledger.
const oauthJournalFlushInterval = 15 * time.Second

// startOAuthJournalFlusher retries unjournaled revocations until ctx ends.
func (s *Server) startOAuthJournalFlusher(ctx context.Context, interval time.Duration) {
	if s.oauth == nil || interval <= 0 {
		return
	}
	s.reaperWG.Add(1)
	go func() {
		defer s.reaperWG.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				// k4-followups-review S1: a clean shutdown (not just a crash) left any still-queued
				// revocation unflushed, because only the ticker called oauthFlushJournal. The review's
				// probe showed a clean restart inside the 15s tick resurrects a dead grant on boot
				// replay -- exactly the scenario an operator triggers when restarting to pick up a
				// ledger fix. One last pass here closes that: if the ledger has recovered by the time
				// we're asked to stop, the revocation lands before the process actually exits.
				//
				// k4-delta-review follow-up: this flush used to run async with nothing joining it, so
				// stopBackground could return (and the process could exit) before this write landed.
				// reaperWG (added/Done here, waited on in stopBackground) closes that race.
				s.oauthFlushJournal()
				return
			case <-t.C:
				s.oauthFlushJournal()
			}
		}
	}()
}

// oauthFlushJournal runs one retry pass and reports the outcome.
func (s *Server) oauthFlushJournal() int {
	before := len(s.oauth.UnjournaledRevocations())
	if before == 0 {
		return 0
	}
	left := s.oauth.FlushUnjournaled()
	if left > 0 {
		slog.Error("oauth: revocations still not journaled", "event", "oauth.revocation_unjournaled", "pending", left)
	} else {
		slog.Warn("oauth: previously unjournaled revocations are now durable", "flushed", before)
	}
	return left
}

func (st *oauthASState) pendingCountLocked() int {
	n := 0
	for _, r := range st.reqs {
		if r.status == oauthReqPending {
			n++
		}
	}
	return n
}

func (st *oauthASState) userCodeTakenLocked(code string) bool {
	for _, r := range st.reqs {
		if r.status == oauthReqPending && r.userCode == code {
			return true
		}
	}
	return false
}

// ─── error rendering ─────────────────────────────────────────────────────────

// oauthAuthorizeError answers a failed /authorize. These are never redirects:
// the redirect target is only trusted once every check has passed, and the
// user sees the failure instead of being bounced with an opaque error.
func oauthAuthorizeError(w http.ResponseWriter, r *http.Request, status int, code, desc string) {
	w.Header().Set("Cache-Control", "no-store")
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSONResp(w, status, map[string]string{"error": code, "error_description": desc})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	oauthPageHeaders(w, "")
	w.WriteHeader(status)
	fmt.Fprintf(w, oauthErrorPage, html.EscapeString(code), html.EscapeString(desc))
}

func oauthPageHeaders(w http.ResponseWriter, nonce string) {
	script := "'none'"
	if nonce != "" {
		script = "'nonce-" + nonce + "'"
	}
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'unsafe-inline'; script-src "+script+"; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
}

const oauthPageStyle = `<style>body{font:16px/1.5 system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem;color:#222}` +
	`.code{font:700 2.2rem/1.2 ui-monospace,monospace;letter-spacing:.25em;background:#f3f3f3;padding:.6rem 1rem;border-radius:.5rem;display:inline-block}` +
	`.dim{color:#666;font-size:.9rem}</style>`

const oauthErrorPage = `<!doctype html><meta charset="utf-8"><title>Sign-in failed</title>` + oauthPageStyle +
	`<h1>Sign-in failed</h1><p><b>%s</b></p><p>%s</p><p class="dim">Nothing was approved. Close this tab and start again from the connector.</p>`

const oauthConsentPage = `<!doctype html><meta charset="utf-8"><title>Approve on your paired device</title>` + oauthPageStyle +
	`<h1>Approve on your paired device</h1>` +
	`<p><b>%s</b> wants to connect to your stage.</p>` +
	`<p>Open your stage on a paired device and approve the request showing this code:</p>` +
	`<p><span class="code">%s</span></p>` +
	`<p id="s" class="dim">Waiting for approval&hellip; this request expires in 10 minutes.</p>` +
	`<script nonce="%s">
const id=%s,secret=%s;
const out=document.getElementById("s");
async function poll(){
  for(;;){
    let r;
    try{ r=await fetch("/oauth/authorize/status/"+encodeURIComponent(id),{headers:{"X-Poll-Secret":secret},cache:"no-store"}); }
    catch(e){ await new Promise(t=>setTimeout(t,2000)); continue; }
    if(r.status===202) continue;
    if(r.status!==200){ out.textContent="This request is no longer valid. Close this tab and start again."; return; }
    const j=await r.json();
    if(j.redirect){ out.textContent=j.status==="approved"?"Approved. Returning you to the app\u2026":"Not approved. Returning you to the app\u2026"; location.replace(j.redirect); return; }
    out.textContent="Unexpected response."; return;
  }
}
poll();
</script>`

// ─── GET /oauth/authorize ────────────────────────────────────────────────────

func (s *Server) handleOAuthAuthorize(w http.ResponseWriter, r *http.Request) {
	st := s.asState()
	set := s.oauthSettings()
	q := r.URL.Query()

	// A parameter sent twice is ambiguous; refuse rather than pick one.
	for _, k := range []string{"client_id", "redirect_uri", "response_type", "code_challenge",
		"code_challenge_method", "resource", "scope", "state"} {
		if len(q[k]) > 1 {
			oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "parameter "+k+" was sent more than once")
			return
		}
	}
	clientID := q.Get("client_id")
	redirectURI := q.Get("redirect_uri")
	if clientID == "" || redirectURI == "" {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "client_id and redirect_uri are required")
		return
	}
	if !oauthRedirectAllowed(set.RedirectURIs, redirectURI) {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "redirect_uri is not registered")
		return
	}
	if q.Get("response_type") != "code" {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "unsupported_response_type", "response_type must be code")
		return
	}
	if m := q.Get("code_challenge_method"); m != "S256" {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "code_challenge_method must be S256")
		return
	}
	if !oauthChallengeRe.MatchString(q.Get("code_challenge")) {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "code_challenge is required (S256, 43 base64url characters)")
		return
	}
	if q.Get("resource") != set.Resource {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_target", "resource does not name this server's protected resource")
		return
	}
	if !oauthScopeAcceptable(q.Get("scope")) {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_scope", "scope must include "+OAuthScopeMCPConnector)
		return
	}
	clientState := q.Get("state")
	if len(clientState) > oauthStateMax || strings.ContainsAny(clientState, "\r\n\x00") {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "state is too long or malformed")
		return
	}

	if !st.limiter.Allow() {
		oauthAuthorizeError(w, r, http.StatusTooManyRequests, "temporarily_unavailable", "too many authorization requests; try again shortly")
		return
	}

	// The CIMD fetch is the expensive, outward-facing step: it runs last,
	// after every cheap local check has passed.
	meta, err := st.cimd.Fetch(r.Context(), clientID, set.AllowedClientHosts)
	if err != nil {
		slog.Warn("oauth: client metadata rejected", "client_id", clientID, "err", err)
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_client", "the client could not be verified")
		return
	}
	if !meta.hasRedirect(redirectURI) {
		oauthAuthorizeError(w, r, http.StatusBadRequest, "invalid_request", "redirect_uri is not registered by the client")
		return
	}

	secret, err := randomURLToken(18)
	if err != nil {
		oauthAuthorizeError(w, r, http.StatusInternalServerError, "server_error", "entropy unavailable")
		return
	}
	req := &oauthAuthRequest{
		id:          "oar-" + mustHex(8),
		secretH:     sha256.Sum256([]byte(secret)),
		clientID:    clientID,
		clientName:  meta.ClientName,
		redirectURI: redirectURI,
		state:       clientState,
		challenge:   q.Get("code_challenge"),
		resource:    set.Resource,
		scope:       []string{OAuthScopeMCPConnector},
		done:        make(chan struct{}),
	}
	if req.clientName == "" {
		req.clientName = clientID
	}

	st.mu.Lock()
	st.pruneLocked(s.oauthRevokeUnredeemed)
	if st.pendingCountLocked() >= oauthMaxPending || len(st.reqs) >= oauthMaxRequests {
		st.mu.Unlock()
		oauthAuthorizeError(w, r, http.StatusTooManyRequests, "temporarily_unavailable", "too many authorization requests are waiting")
		return
	}
	for i := 0; ; i++ {
		code, cerr := randomPairingCode(oauthUserCodeLen)
		if cerr != nil {
			st.mu.Unlock()
			oauthAuthorizeError(w, r, http.StatusInternalServerError, "server_error", "entropy unavailable")
			return
		}
		if !st.userCodeTakenLocked(code) || i > 20 {
			req.userCode = code
			break
		}
	}
	req.created = st.now()
	req.expires = req.created.Add(oauthRequestLifetime)
	req.purgeAt = req.expires.Add(oauthRequestRetain)
	st.reqs[req.id] = req
	notice := oauthConsentNotice{
		RequestID: req.id, UserCode: req.userCode, ClientID: req.clientID, ClientName: req.clientName,
		Resource: req.resource, Scope: req.scope, ExpiresAt: req.expires.Format(time.RFC3339),
	}
	st.mu.Unlock()

	// Tell the stage so it can raise the paired item. Per the plan, an
	// unreachable stage means no one can approve: drop the request and say so.
	if err := st.notifyStage(r.Context(), set.StageNotifyURL, notice); err != nil {
		slog.Error("oauth: stage consent notify failed; request dropped", "request_id", req.id, "url", set.StageNotifyURL, "err", err)
		st.mu.Lock()
		delete(st.reqs, req.id)
		st.mu.Unlock()
		oauthAuthorizeError(w, r, http.StatusServiceUnavailable, "temporarily_unavailable", "stage unreachable: the approval request could not be delivered")
		return
	}
	slog.Info("oauth: authorization request pending", "request_id", req.id, "client_id", req.clientID, "user_code", req.userCode)

	w.Header().Set("Cache-Control", "no-store")
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"request_id":  req.id,
			"user_code":   req.userCode,
			"poll_secret": secret,
			"status_path": oauthAuthorizeStatusPath + req.id,
			"expires_at":  req.expires.Format(time.RFC3339),
			"status":      "pending",
		})
		return
	}
	nonce, _ := randomURLToken(12)
	idJS, _ := json.Marshal(req.id)
	secretJS, _ := json.Marshal(secret)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	oauthPageHeaders(w, nonce)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, oauthConsentPage, html.EscapeString(req.clientName), html.EscapeString(req.userCode), nonce, idJS, secretJS)
}

func oauthRedirectAllowed(allow []string, uri string) bool {
	for _, a := range allow {
		if a == uri { // exact, byte for byte: no normalisation, no prefix
			return true
		}
	}
	return false
}

// oauthScopeAcceptable: an absent scope means the one scope this AS issues; a
// present scope must include mcp:connector. Extra scopes are ignored (the
// grant is always exactly mcp:connector); a scope list that does not name it is
// refused.
func oauthScopeAcceptable(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return true
	}
	for _, f := range strings.Fields(raw) {
		if f == OAuthScopeMCPConnector {
			return true
		}
	}
	return false
}

// notifyStage POSTs the notice to the stage over loopback.
func (st *oauthASState) notifyStage(ctx context.Context, target string, p oauthConsentNotice) error {
	if st.notify != nil {
		return st.notify(ctx, target, p)
	}
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, oauthNotifyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Timeout:       oauthNotifyTimeout,
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("stage answered %d", resp.StatusCode)
	}
	return nil
}

// ─── GET /oauth/authorize/status/{id} ────────────────────────────────────────

func oauthBuildRedirect(base string, params map[string]string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// handleOAuthAuthorizeStatus is the browser's long-poll. Unknown id and a
// wrong secret answer the same 404. The outcome (code, denial, expiry) is
// handed over exactly once.
func (s *Server) handleOAuthAuthorizeStatus(w http.ResponseWriter, r *http.Request) {
	st := s.asState()
	w.Header().Set("Cache-Control", "no-store")
	id := r.PathValue("id")
	secretH := sha256.Sum256([]byte(r.Header.Get(oauthPollSecretHeader)))

	st.mu.Lock()
	st.pruneLocked(s.oauthRevokeUnredeemed)
	req := st.reqs[id]
	if req == nil || subtle.ConstantTimeCompare(req.secretH[:], secretH[:]) != 1 {
		st.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown request")
		return
	}
	done := req.done
	pending := req.status == oauthReqPending
	wait := st.pollWait
	st.mu.Unlock()

	if pending {
		ctx, cancel := context.WithTimeout(r.Context(), wait)
		defer cancel()
		select {
		case <-done:
		case <-ctx.Done():
			if r.Context().Err() != nil {
				return
			}
			writeJSONResp(w, http.StatusAccepted, map[string]any{"status": "pending"})
			return
		}
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	st.pruneLocked(s.oauthRevokeUnredeemed)
	if st.reqs[id] != req {
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown request")
		return
	}
	if req.status == oauthReqPending { // woke without a decision (should not happen)
		writeJSONResp(w, http.StatusAccepted, map[string]any{"status": "pending"})
		return
	}
	if req.delivered {
		writeJSONResp(w, http.StatusGone, map[string]any{"status": "collected"})
		return
	}
	var params map[string]string
	switch req.status {
	case oauthReqApproved:
		if req.revoked || !st.now().Before(req.codeExpires) {
			req.delivered = true
			params = map[string]string{"error": "access_denied", "error_description": "authorization code expired", "state": req.state}
			break
		}
		params = map[string]string{"code": req.rawCode, "state": req.state}
	case oauthReqDenied:
		params = map[string]string{"error": "access_denied", "state": req.state}
	default: // expired
		params = map[string]string{"error": "access_denied", "error_description": "the request expired", "state": req.state}
	}
	redirect, err := oauthBuildRedirect(req.redirectURI, params)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "could not build redirect")
		return
	}
	req.delivered = true
	req.rawCode = "" // the raw code is held only until the browser has it
	writeJSONResp(w, http.StatusOK, map[string]any{"status": req.status.String(), "redirect": redirect})
}

// ─── POST /v1/oauth/requests/{id}/approve | deny ─────────────────────────────

// oauthConsentCaller enforces oauth:consent inside the handler as well as at
// the gate, so a handler wired onto a bare mux can never be driven without it.
// grantHasScope is fail-closed (no scope, no pass; node-root retains all).
func (s *Server) oauthConsentCaller(w http.ResponseWriter, r *http.Request) (*IdentityGrant, bool) {
	g, ok := grantFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "missing_grant", "a kernel grant carrying "+ScopeOAuthConsent+" is required")
		return nil, false
	}
	if !grantHasScope(g, ScopeOAuthConsent) {
		writeJSONError(w, http.StatusForbidden, "insufficient_scope", "grant does not carry the '"+ScopeOAuthConsent+"' scope")
		return nil, false
	}
	return g, true
}

type oauthApproveRequest struct {
	ApprovedVia string `json:"approved_via"`
	DeviceHash  string `json:"device_hash"`
}

func (s *Server) handleOAuthRequestApprove(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.oauthConsentCaller(w, r)
	if !ok {
		return
	}
	if s.oauth == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth store is not available")
		return
	}
	var body oauthApproveRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON {approved_via, device_hash}")
		return
	}
	if body.ApprovedVia == "" {
		body.ApprovedVia = OAuthApprovedViaPairedDevice
	}
	if body.ApprovedVia != OAuthApprovedViaPairedDevice {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "approved_via must be "+OAuthApprovedViaPairedDevice)
		return
	}
	// The device binding is mandatory: a grant with no recorded approver
	// device fails the D3 cross-check forever (ApproverDeviceLive).
	if !oauthHashRe.MatchString(body.DeviceHash) {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "device_hash must be the paired device's sha256 (64 lowercase hex)")
		return
	}

	st := s.asState()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pruneLocked(s.oauthRevokeUnredeemed)
	req := st.reqs[r.PathValue("id")]
	if req == nil {
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown request")
		return
	}
	if req.status == oauthReqExpired {
		writeJSONError(w, http.StatusGone, "expired", "the request expired")
		return
	}
	if req.status != oauthReqPending {
		writeJSONError(w, http.StatusConflict, "not_pending", "the request is already "+req.status.String())
		return
	}

	rawCode, err := newOAuthToken(OAuthAuthCodePrefix)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "entropy unavailable")
		return
	}
	codeHash := sha256Hex(rawCode)
	grant, err := s.oauth.CreateGrant(OAuthGrantInput{
		ClientID: req.clientID,
		Actor:    oauthActor,
		Delegate: oauthDelegate,
		Scope:    req.scope,
		Resource: req.resource,
		Consent: OAuthConsent{
			RequestID:      req.id,
			CodeHash:       codeHash,
			ApprovedVia:    body.ApprovedVia,
			ApproverDevice: body.DeviceHash,
		},
	})
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, ErrOAuthStoreAtCapacity):
			status = http.StatusTooManyRequests
		case errors.Is(err, ErrOAuthLedgerAppendFailed):
			status = http.StatusServiceUnavailable
		}
		writeJSONError(w, status, "grant_failed", err.Error())
		return
	}

	now := st.now()
	req.status = oauthReqApproved
	req.rawCode = rawCode
	req.codeHash = codeHash
	req.codeExpires = now.Add(oauthCodeLifetime)
	req.grantID = grant.GrantID
	req.device = body.DeviceHash
	req.approver = caller.GrantID
	req.decidedAt = now
	req.purgeAt = req.codeExpires.Add(oauthRequestRetain)
	st.byCode[codeHash] = req
	close(req.done)
	slog.Info("oauth: authorization request approved", "request_id", req.id, "grant_id", grant.GrantID, "approver", caller.GrantID)

	// The raw code is NOT in this response: it goes to the browser poller only.
	writeJSONResp(w, http.StatusOK, map[string]any{
		"request_id":      req.id,
		"status":          "approved",
		"grant_id":        grant.GrantID,
		"approved_via":    body.ApprovedVia,
		"approver_device": body.DeviceHash,
		"device_live":     grant.ApproverDeviceLive(s.shareLinks),
		"code_expires_at": req.codeExpires.Format(time.RFC3339),
	})
}

func (s *Server) handleOAuthRequestDeny(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.oauthConsentCaller(w, r)
	if !ok {
		return
	}
	st := s.asState()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pruneLocked(s.oauthRevokeUnredeemed)
	req := st.reqs[r.PathValue("id")]
	if req == nil {
		writeJSONError(w, http.StatusNotFound, "not_found", "unknown request")
		return
	}
	if req.status == oauthReqExpired {
		writeJSONError(w, http.StatusGone, "expired", "the request expired")
		return
	}
	if req.status != oauthReqPending {
		writeJSONError(w, http.StatusConflict, "not_pending", "the request is already "+req.status.String())
		return
	}
	now := st.now()
	req.status = oauthReqDenied
	req.approver = caller.GrantID
	req.decidedAt = now
	req.purgeAt = now.Add(oauthRequestRetain)
	close(req.done)
	slog.Info("oauth: authorization request denied", "request_id", req.id, "by", caller.GrantID)
	writeJSONResp(w, http.StatusOK, map[string]any{"request_id": req.id, "status": "denied"})
}

// ─── for /token (K4) ─────────────────────────────────────────────────────────

var (
	// ErrOAuthCodeUnknown: no such code, or it has expired.
	ErrOAuthCodeUnknown = errors.New("oauth authorization code unknown or expired")
	// ErrOAuthCodeReplay: the code was already redeemed. The caller should
	// revoke the grant it produced (RFC 6749 4.1.2).
	ErrOAuthCodeReplay = errors.New("oauth authorization code already redeemed")
)

// oauthRedeemedCode is what /token needs to finish the exchange.
type oauthRedeemedCode struct {
	RequestID     string
	GrantID       string
	ClientID      string
	RedirectURI   string
	CodeChallenge string
	Resource      string
}

// redeemCode consumes an authorization code exactly once. A second call with
// the same code returns ErrOAuthCodeReplay together with the grant id so the
// endpoint can revoke it. /token still has to check client_id, redirect_uri
// and the PKCE verifier against the returned record.
func (st *oauthASState) redeemCode(code string, revoke func(grantID, reason string)) (*oauthRedeemedCode, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pruneLocked(revoke)
	req := st.byCode[sha256Hex(code)]
	if req == nil || req.status != oauthReqApproved {
		return nil, ErrOAuthCodeUnknown
	}
	out := &oauthRedeemedCode{
		RequestID: req.id, GrantID: req.grantID, ClientID: req.clientID,
		RedirectURI: req.redirectURI, CodeChallenge: req.challenge, Resource: req.resource,
	}
	if req.redeemed {
		return out, ErrOAuthCodeReplay
	}
	if req.revoked || !st.now().Before(req.codeExpires) {
		return nil, ErrOAuthCodeUnknown
	}
	req.redeemed = true
	return out, nil
}
