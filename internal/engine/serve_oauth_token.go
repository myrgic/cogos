// serve_oauth_token.go — the token endpoint and the internal grant/token
// management routes for the kernel's OAuth AS (Phase 3, K4).
//
//	POST /oauth/token                       public, self-authenticating (PKCE)
//	POST /v1/oauth/introspect               internal, scope oauth:introspect (RFC 7662)
//	GET  /v1/oauth/grants                   internal, scope admin; never returns hashes
//	POST /v1/oauth/grants/{id}/revoke       internal, scope admin
//
// The public client has no secret: the authorization code is bound to the
// client_id, redirect_uri, resource and PKCE challenge recorded at /authorize,
// and each of those is checked here before tokens are issued. A failure after
// the code has been consumed revokes the grant it produced; that grant can
// never yield tokens, so leaving it live would only hold a dead consent open.
package engine

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"
)

// RFC 6749 section 5.2 error codes used by the token endpoint.
const (
	oauthErrInvalidRequest       = "invalid_request"
	oauthErrInvalidGrant         = "invalid_grant"
	oauthErrUnsupportedGrantType = "unsupported_grant_type"
	oauthErrTemporarilyUnavail   = "temporarily_unavailable"
	oauthErrServerError          = "server_error"
)

// oauthTokenMaxBody bounds the form body. Tokens are ~50 bytes; a few KB is
// generous for a legitimate exchange.
const oauthTokenMaxBody = 8 << 10

// oauthTokenError writes an RFC 6749 section 5.2 error body. Not cacheable.
func oauthTokenError(w http.ResponseWriter, status int, code, desc string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSONResp(w, status, map[string]string{"error": code, "error_description": desc})
}

// oauthTokenBody is the RFC 6749 section 5.2 success body.
type oauthTokenBody struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
}

// handleOAuthToken serves POST /oauth/token.
func (s *Server) handleOAuthToken(w http.ResponseWriter, r *http.Request) {
	if s.oauth == nil {
		oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth store is not available")
		return
	}
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != "application/x-www-form-urlencoded" {
		oauthTokenError(w, http.StatusUnsupportedMediaType, oauthErrInvalidRequest,
			"Content-Type must be application/x-www-form-urlencoded")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, oauthTokenMaxBody)
	if err := r.ParseForm(); err != nil {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidRequest, "malformed form body")
		return
	}
	form := r.PostForm
	switch form.Get("grant_type") {
	case "authorization_code":
		s.oauthExchangeCode(w, form)
	case "refresh_token":
		s.oauthRotateRefresh(w, form)
	default:
		oauthTokenError(w, http.StatusBadRequest, oauthErrUnsupportedGrantType,
			"grant_type must be authorization_code or refresh_token")
	}
}

// oauthExchangeCode redeems an authorization code for the grant's first pair.
func (s *Server) oauthExchangeCode(w http.ResponseWriter, form map[string][]string) {
	get := func(k string) string {
		if v := form[k]; len(v) == 1 {
			return v[0]
		}
		return ""
	}
	code, verifier := get("code"), get("code_verifier")
	clientID, redirectURI, resource := get("client_id"), get("redirect_uri"), get("resource")
	if code == "" || verifier == "" || clientID == "" || redirectURI == "" {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidRequest,
			"code, code_verifier, client_id and redirect_uri are required")
		return
	}
	if !strings.HasPrefix(code, OAuthAuthCodePrefix) {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "authorization code is invalid")
		return
	}

	st := s.asState()
	rec, err := st.redeemCode(code, s.oauthRevokeUnredeemed)
	if err != nil {
		switch {
		case errors.Is(err, ErrOAuthCodeReplay):
			// RFC 6749 4.1.2: a second use revokes what the first use produced.
			// The revocation is in force in memory either way; if it could not be
			// journaled the answer must not read as a clean "already used".
			rerr := s.oauthRevokeFailClosed(rec.GrantID, "authorization code replayed")
			slogOAuthFail("code_replay", rec)
			if rerr != nil {
				oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth ledger unavailable")
				return
			}
			oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "authorization code already used")
		default:
			oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "authorization code is invalid or expired")
		}
		return
	}

	// The code is consumed. From here every failure revokes the grant.
	fail := func(reason string) {
		rerr := s.oauthRevokeFailClosed(rec.GrantID, reason)
		slogOAuthFail(reason, rec)
		if rerr != nil {
			oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth ledger unavailable")
			return
		}
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "authorization code does not match this exchange")
	}
	if clientID != rec.ClientID {
		fail("client_id mismatch")
		return
	}
	if redirectURI != rec.RedirectURI {
		fail("redirect_uri mismatch")
		return
	}
	if resource != rec.Resource {
		fail("resource mismatch")
		return
	}
	if !OAuthPKCEVerifyS256(rec.CodeChallenge, verifier) {
		fail("pkce verifier mismatch")
		return
	}

	// D3: the device that approved this grant must still be trusted when its
	// first tokens are minted (it may have been revoked since approval).
	if g, ok := s.oauth.Grant(rec.GrantID); !ok || !g.ApproverDeviceLive(s.shareLinks) {
		fail("approver device not live")
		return
	}

	pair, err := s.oauth.IssueTokens(rec.GrantID)
	if err != nil {
		s.oauthTokenIssueError(w, err, rec)
		return
	}
	s.oauthWriteTokens(w, pair)
}

// oauthRotateRefresh rotates a refresh token. client_id is checked against the
// grant before Rotate, so a wrong client never consumes the token.
func (s *Server) oauthRotateRefresh(w http.ResponseWriter, form map[string][]string) {
	get := func(k string) string {
		if v := form[k]; len(v) == 1 {
			return v[0]
		}
		return ""
	}
	refresh, clientID, resource := get("refresh_token"), get("client_id"), get("resource")
	if refresh == "" || clientID == "" {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidRequest, "refresh_token and client_id are required")
		return
	}
	g, ok := s.oauth.ResolveRefreshGrant(refresh)
	if !ok || g.ClientID != clientID {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "refresh token is invalid")
		return
	}
	if resource != "" && resource != g.Resource {
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "resource does not match the grant")
		return
	}
	// D3: a grant whose approving device is no longer a live device link must
	// not mint fresh tokens. Refused before Rotate, so the refresh token is not
	// consumed (a mirror that lags is recoverable; a revoked grant is not).
	if !g.ApproverDeviceLive(s.shareLinks) {
		slog.Warn("oauth: refresh refused; approver device is not live", "grant_id", g.GrantID)
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "refresh token is invalid")
		return
	}
	pair, err := s.oauth.Rotate(refresh)
	if err != nil {
		switch {
		case errors.Is(err, ErrOAuthRefreshReuse):
			// The store has already revoked the grant and every token under it.
			slog.Warn("oauth: refresh token reuse; grant revoked", "grant_id", g.GrantID)
			if errors.Is(err, ErrOAuthLedgerAppendFailed) {
				oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth ledger unavailable")
				return
			}
			oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "refresh token already used; grant revoked")
		case errors.Is(err, ErrOAuthLedgerAppendFailed):
			oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth ledger unavailable")
		case errors.Is(err, ErrOAuthEntropyUnavailable):
			oauthTokenError(w, http.StatusInternalServerError, oauthErrServerError, "entropy unavailable")
		default:
			oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "refresh token is invalid")
		}
		return
	}
	s.oauthWriteTokens(w, pair)
}

// oauthTokenIssueError maps IssueTokens failures onto the token endpoint.
func (s *Server) oauthTokenIssueError(w http.ResponseWriter, err error, rec *oauthRedeemedCode) {
	switch {
	case errors.Is(err, ErrOAuthTokensAlreadyIssued), errors.Is(err, ErrOAuthGrantNotFound):
		oauthTokenError(w, http.StatusBadRequest, oauthErrInvalidGrant, "authorization code is invalid")
	case errors.Is(err, ErrOAuthLedgerAppendFailed):
		// Nothing was issued (memory is untouched). The code is spent, so the
		// client must restart the flow; the grant is revoked so it cannot linger.
		_ = s.oauthRevokeFailClosed(rec.GrantID, "token issue failed")
		oauthTokenError(w, http.StatusServiceUnavailable, oauthErrTemporarilyUnavail, "oauth ledger unavailable")
	case errors.Is(err, ErrOAuthEntropyUnavailable):
		oauthTokenError(w, http.StatusInternalServerError, oauthErrServerError, "entropy unavailable")
	default:
		oauthTokenError(w, http.StatusInternalServerError, oauthErrServerError, "token issue failed")
	}
}

// oauthWriteTokens sends the RFC 6749 token response. The scope is always the
// one the connector grant carries (risk 10: unknown scopes are not echoed).
func (s *Server) oauthWriteTokens(w http.ResponseWriter, pair *OAuthTokenPair) {
	ttl := pair.AccessExpiresAt.Sub(s.oauth.now())
	expires := int64(ttl / time.Second)
	if expires < 0 {
		expires = 0
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSONResp(w, http.StatusOK, oauthTokenBody{
		AccessToken:  pair.AccessToken,
		TokenType:    "Bearer",
		ExpiresIn:    expires,
		RefreshToken: pair.RefreshToken,
		Scope:        OAuthScopeMCPConnector,
	})
}

// slogOAuthFail logs a refused exchange without the code or any token.
func slogOAuthFail(reason string, rec *oauthRedeemedCode) {
	slog.Warn("oauth: token exchange refused", "reason", reason, "grant_id", rec.GrantID, "request_id", rec.RequestID)
}

// ─── POST /v1/oauth/introspect (RFC 7662) ────────────────────────────────────

type oauthIntrospectRequest struct {
	Token string `json:"token"`
}

// handleOAuthIntrospect answers an access-token check for the stage's resource
// server. Every non-live token gets the same {"active":false}: no oracle
// separates unknown, expired, revoked and rotated.
func (s *Server) handleOAuthIntrospect(w http.ResponseWriter, r *http.Request) {
	if !s.oauthCallerHasScope(w, r, ScopeOAuthIntrospect) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if s.oauth == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth store is not available")
		return
	}
	var body oauthIntrospectRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON {token}")
		return
	}
	info, ok := s.oauth.Resolve(body.Token)
	// D3: a token is only active while the device that approved its grant is
	// still a live device link. Same {"active":false} as every other dead
	// token (no oracle), so a resource server that reads nothing else is safe.
	if !ok || !info.Grant.ApproverDeviceLive(s.shareLinks) {
		writeJSONResp(w, http.StatusOK, map[string]any{"active": false})
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"active":    true,
		"scope":     strings.Join(info.Grant.Scope, " "),
		"client_id": info.Grant.ClientID,
		"sub":       info.Grant.Delegate,
		"act":       map[string]string{"sub": info.Grant.Actor},
		"aud":       info.Grant.Resource,
		"exp":       info.Token.ExpiresAt.Unix(),
		"iat":       info.Token.IssuedAt.Unix(),
		"grant_id":  info.Grant.GrantID,
	})
}

// ─── GET /v1/oauth/grants and POST /v1/oauth/grants/{id}/revoke ──────────────

// oauthGrantView is the operator's view of a grant. It deliberately carries no
// token hash, no authorization-code hash and no approver device hash: the list
// is for humans deciding what to revoke, not for correlation.
type oauthGrantView struct {
	GrantID          string    `json:"grant_id"`
	ClientID         string    `json:"client_id"`
	Actor            string    `json:"actor"`
	Delegate         string    `json:"delegate"`
	Scope            []string  `json:"scope"`
	Resource         string    `json:"resource"`
	IssuedAt         time.Time `json:"issued_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	RequestID        string    `json:"request_id"`
	ApprovedVia      string    `json:"approved_via"`
	ApproverDeviceOK bool      `json:"approver_device_live"`
}

func (s *Server) handleOAuthGrantList(w http.ResponseWriter, r *http.Request) {
	if !s.oauthCallerHasScope(w, r, ScopeAdmin) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if s.oauth == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth store is not available")
		return
	}
	grants := s.oauth.Snapshot()
	out := make([]oauthGrantView, 0, len(grants))
	for _, g := range grants {
		out = append(out, oauthGrantView{
			GrantID:          g.GrantID,
			ClientID:         g.ClientID,
			Actor:            g.Actor,
			Delegate:         g.Delegate,
			Scope:            g.Scope,
			Resource:         g.Resource,
			IssuedAt:         g.IssuedAt,
			ExpiresAt:        g.ExpiresAt,
			RequestID:        g.Consent.RequestID,
			ApprovedVia:      g.Consent.ApprovedVia,
			ApproverDeviceOK: g.ApproverDeviceLive(s.shareLinks),
		})
	}
	// unjournaled_revocations is empty when healthy. Anything listed is a grant
	// that is dead in memory but whose revocation is not yet in the ledger.
	writeJSONResp(w, http.StatusOK, map[string]any{
		"grants":                  out,
		"unjournaled_revocations": s.oauth.UnjournaledRevocations(),
	})
}

type oauthRevokeRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleOAuthGrantRevoke(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.oauthCallerWithScope(w, r, ScopeAdmin)
	if !ok {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if s.oauth == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "oauth_unavailable", "oauth store is not available")
		return
	}
	var body oauthRevokeRequest
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body)
	}
	reason := strings.TrimSpace(body.Reason)
	if reason == "" {
		reason = "admin_revoke"
	}
	id := r.PathValue("id")
	g, err := s.oauth.Revoke(id, caller.GrantID, reason)
	if err != nil {
		switch {
		case errors.Is(err, ErrOAuthGrantNotFound):
			writeJSONError(w, http.StatusNotFound, "not_found", "no live oauth grant with that id")
		case errors.Is(err, ErrOAuthLedgerAppendFailed):
			writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", "identity-oauth ledger append failed; grant left live")
		default:
			writeJSONError(w, http.StatusInternalServerError, "server_error", "revoke failed")
		}
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"grant_id": g.GrantID, "status": "revoked", "reason": reason})
}

// oauthCallerHasScope enforces scope inside the handler as well as at the gate,
// as the K3 consent routes do, so a handler wired onto a bare mux cannot be
// driven without it.
func (s *Server) oauthCallerHasScope(w http.ResponseWriter, r *http.Request, scope string) bool {
	_, ok := s.oauthCallerWithScope(w, r, scope)
	return ok
}

func (s *Server) oauthCallerWithScope(w http.ResponseWriter, r *http.Request, scope string) (*IdentityGrant, bool) {
	g, ok := grantFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "missing_grant", "a kernel grant carrying "+scope+" is required")
		return nil, false
	}
	if !grantHasScope(g, scope) {
		writeJSONError(w, http.StatusForbidden, "insufficient_scope", "grant does not carry the '"+scope+"' scope")
		return nil, false
	}
	return g, true
}
