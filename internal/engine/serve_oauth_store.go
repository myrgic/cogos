// serve_oauth_store.go — the OAuth grant/token store (Phase 3 OAuth, slice K1).
//
// The kernel is the OAuth authorization server for the Stage's MCP connector.
// This file is ONLY the durable store: types, ledger events, replay. It has no
// HTTP routes (those arrive in later slices) and no metadata document.
//
// The revocable unit is an OAuthGrant: one consent, "delegate (claude-web)
// acts for actor (chaz), scope mcp:connector". Access and refresh tokens are
// CHILDREN of the grant; revoking the grant kills every one of them at once.
//
// Why a store of its own, not IdentityGrantRegistry:
//
//   - grantAuthMiddleware calls VerifyAny ("does the caller hold SOME live
//     grant") and requiredScopeForRequest returns "" for most routes, so an
//     OAuth access token stored as an IdentityGrant would pass every unscoped
//     route on the :6932 API. Same reasoning as the share-link store
//     (serve_share_links.go): bearer leaves live apart from kernel grants. A
//     later slice adds the matching refusal in the gate via IsToken.
//
// Durability follows the share-link store: every mutation appends a ledger
// event BEFORE touching memory (write-ahead) in the "identity-oauth" bucket
// (.cog/ledger/identity-oauth/events.jsonl). The ledger holds sha256 hashes
// and never a raw token. RebuildOAuthStoreFromLedger replays it at boot, so a
// restart loses no grant and no rotation state (a rotated refresh token stays
// recognisable as rotated, which is what makes reuse detectable).
//
// Rotating refresh: a refresh token is single-use. Rotate consumes it and
// issues a new pair. Presenting an already-rotated refresh token is reuse: the
// whole grant is revoked and the caller gets ErrOAuthRefreshReuse. The /token
// endpoint (a later slice) maps ErrOAuthInvalidGrant and ErrOAuthRefreshReuse
// both to {"error":"invalid_grant"}; the store keeps them distinct so the
// endpoint can log which one it was.
//
// Device binding (D3): a grant records the paired-device hash the consent came
// from (OAuthConsent.ApproverDevice, a sha256 hex like a share-link device
// link's IntegrityHash). OAuthGrant.ApproverDeviceLive cross-checks it against
// the share-link mirror of trusted devices, so a revoked device cannot have its
// OAuth grants honoured.
package engine

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// oauthLedgerSession is the ledger bucket for OAuth events, at
// .cog/ledger/identity-oauth/events.jsonl.
const oauthLedgerSession = "identity-oauth"

// OAuth token string prefixes. They make a token recognisable to the gate, to
// logs and to the Stage without a lookup. The prefix is not secret.
const (
	OAuthAccessTokenPrefix  = "cgat_"
	OAuthRefreshTokenPrefix = "cgrt_"
)

// oauthTokenBytes is the entropy behind a token: 32 bytes, 43 url-safe base64
// characters after the prefix.
const oauthTokenBytes = 32

// OAuthScopeMCPConnector is the one scope the connector grant carries.
const OAuthScopeMCPConnector = "mcp:connector"

// Defaults. The store holds them per instance so tests and config can override.
const (
	DefaultOAuthAccessTTL  = time.Hour
	DefaultOAuthRefreshTTL = 30 * 24 * time.Hour
	// DefaultOAuthGrantMaxAge is the absolute cap on a grant; after it the
	// connector must be re-consented, rotation or not.
	DefaultOAuthGrantMaxAge = 180 * 24 * time.Hour
)

// Safety nets against an authorized caller looping; not working limits.
const (
	maxOAuthGrants = 1000
	maxOAuthTokens = 20000
)

// Token kinds.
const (
	OAuthTokenKindAccess  = "access"
	OAuthTokenKindRefresh = "refresh"
)

var (
	// ErrOAuthGrantNotFound: no live grant with that id.
	ErrOAuthGrantNotFound = errors.New("no live oauth grant with that id")
	// ErrOAuthInvalidGrant: the presented token is unknown, expired, of the
	// wrong kind, or belongs to a dead grant. Maps to invalid_grant.
	ErrOAuthInvalidGrant = errors.New("oauth token invalid")
	// ErrOAuthRefreshReuse: an already-rotated refresh token was presented. The
	// grant has been revoked. Maps to invalid_grant.
	ErrOAuthRefreshReuse = errors.New("oauth refresh token reuse detected; grant revoked")
	// ErrOAuthTokensAlreadyIssued: IssueTokens ran twice for one grant. An
	// authorization code is single-use, so a second exchange is a replay.
	ErrOAuthTokensAlreadyIssued = errors.New("oauth grant already has its initial tokens")
	// ErrOAuthStoreAtCapacity maps to 429.
	ErrOAuthStoreAtCapacity = errors.New("oauth store at capacity")
	// ErrOAuthLedgerAppendFailed maps to 503; memory is untouched.
	ErrOAuthLedgerAppendFailed = errors.New("identity-oauth ledger append failed")
	// ErrOAuthEntropyUnavailable maps to 500; nothing was written.
	ErrOAuthEntropyUnavailable = errors.New("oauth entropy unavailable")
)

var (
	oauthHashRe      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	oauthScopeRe     = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,31}:[A-Za-z0-9._:-]{1,100}$`)
	oauthRequestIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{0,64}$`)
)

// OAuthConsent records how the grant was approved. Hashes only.
type OAuthConsent struct {
	RequestID      string // the pending authorization request
	CodeHash       string // sha256 of the authorization code that was exchanged ("" until known)
	ApprovedVia    string // e.g. "paired-device"
	ApproverDevice string // sha256 of the paired device's credential ("" if unknown)
}

// OAuthGrant is the revocable consent. Immutable once published in the store.
type OAuthGrant struct {
	GrantID   string
	ClientID  string
	Actor     string // whom the grant acts for, e.g. "chaz"
	Delegate  string // who holds the tokens, e.g. "claude-web"
	Scope     []string
	Resource  string // RFC 8707 resource indicator
	IssuedAt  time.Time
	ExpiresAt time.Time // absolute cap; never zero for a grant made through CreateGrant
	Consent   OAuthConsent
}

func (g *OAuthGrant) expired(now time.Time) bool {
	return !g.ExpiresAt.IsZero() && !now.Before(g.ExpiresAt)
}

// HasScope reports whether the grant carries scope.
func (g *OAuthGrant) HasScope(scope string) bool {
	for _, s := range g.Scope {
		if s == scope {
			return true
		}
	}
	return false
}

// ApprovedByDevice reports whether deviceHash is the device that approved the
// consent (constant-time). An empty hash never matches.
func (g *OAuthGrant) ApprovedByDevice(deviceHash string) bool {
	if deviceHash == "" || g.Consent.ApproverDevice == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(deviceHash), []byte(g.Consent.ApproverDevice)) == 1
}

// ApproverDeviceLive is the D3 cross-check: the device that approved this grant
// must still be a live device link, of the same actor, in the share-link
// mirror. A grant with no recorded device fails the check.
func (g *OAuthGrant) ApproverDeviceLive(links *ShareLinkStore) bool {
	if links == nil || g.Consent.ApproverDevice == "" {
		return false
	}
	l, ok := links.ResolveHash(g.Consent.ApproverDevice)
	return ok && l.Kind == ShareLinkKindDevice && l.Actor == g.Actor
}

// OAuthToken is the stored form of one token: its hash, never the value.
// Immutable once published; rotation replaces the pointer.
type OAuthToken struct {
	Hash       string
	Kind       string
	GrantID    string
	IssuedAt   time.Time
	ExpiresAt  time.Time
	RotatedAt  time.Time // refresh only; non-zero = consumed, any later use is reuse
	ReplacedBy string    // refresh only; hash of the refresh token that replaced it
}

func (t *OAuthToken) expired(now time.Time) bool { return !now.Before(t.ExpiresAt) }

// OAuthTokenPair is what the store hands the caller exactly once, at issue or
// rotation. The raw values are not retained.
type OAuthTokenPair struct {
	GrantID          string
	AccessToken      string
	RefreshToken     string
	AccessExpiresAt  time.Time
	RefreshExpiresAt time.Time
}

// OAuthTokenInfo is the result of resolving a token.
type OAuthTokenInfo struct {
	Token OAuthToken
	Grant *OAuthGrant
}

// OAuthGrantInput is the request to create a grant.
type OAuthGrantInput struct {
	ClientID string
	Actor    string
	Delegate string
	Scope    []string
	Resource string
	Consent  OAuthConsent
	// MaxAge is the absolute cap; zero means DefaultOAuthGrantMaxAge (the store's
	// configured value).
	MaxAge time.Duration
}

func (in *OAuthGrantInput) validate() error {
	if in.ClientID == "" || len(in.ClientID) > 512 {
		return fmt.Errorf("client_id must be 1 to 512 characters")
	}
	if !validActorID(in.Actor) {
		return fmt.Errorf("actor must match %s", actorIDPattern)
	}
	if !validActorID(in.Delegate) {
		return fmt.Errorf("delegate must match %s", actorIDPattern)
	}
	if len(in.Scope) == 0 || len(in.Scope) > 8 {
		return fmt.Errorf("scope must hold 1 to 8 entries")
	}
	for _, sc := range in.Scope {
		if !oauthScopeRe.MatchString(sc) {
			return fmt.Errorf("scope entry %q must match %s", sc, oauthScopeRe)
		}
	}
	if len(in.Resource) > 512 {
		return fmt.Errorf("resource must be at most 512 characters")
	}
	if !oauthRequestIDRe.MatchString(in.Consent.RequestID) {
		return fmt.Errorf("consent request_id must match %s", oauthRequestIDRe)
	}
	if in.Consent.CodeHash != "" && !oauthHashRe.MatchString(in.Consent.CodeHash) {
		return fmt.Errorf("consent code_hash must be 64 lowercase hex characters")
	}
	if in.Consent.ApproverDevice != "" && !oauthHashRe.MatchString(in.Consent.ApproverDevice) {
		return fmt.Errorf("consent approver_device must be 64 lowercase hex characters")
	}
	if len(in.Consent.ApprovedVia) > 64 {
		return fmt.Errorf("consent approved_via must be at most 64 characters")
	}
	if in.MaxAge < 0 {
		return fmt.Errorf("max_age must not be negative")
	}
	return nil
}

// OAuthStore is the in-memory, ledger-backed store. Revoked grants (and their
// tokens) are dropped from memory; the ledger keeps the history.
type OAuthStore struct {
	mu            sync.RWMutex
	grants        map[string]*OAuthGrant
	tokens        map[string]*OAuthToken // by hash, every kind, rotated refresh included
	issued        map[string]bool        // grants whose initial pair has been minted
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
	now           func() time.Time
	accessTTL     time.Duration
	refreshTTL    time.Duration
	grantMaxAge   time.Duration
}

// NewOAuthStoreWithLedger returns an empty store. workspaceRoot "" means
// in-memory only.
func NewOAuthStoreWithLedger(workspaceRoot string) *OAuthStore {
	return &OAuthStore{
		grants:        make(map[string]*OAuthGrant),
		tokens:        make(map[string]*OAuthToken),
		issued:        make(map[string]bool),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
		now:           func() time.Time { return time.Now().UTC() },
		accessTTL:     DefaultOAuthAccessTTL,
		refreshTTL:    DefaultOAuthRefreshTTL,
		grantMaxAge:   DefaultOAuthGrantMaxAge,
	}
}

// SetTTLs overrides the token and grant lifetimes. Zero values keep the
// current setting. Call before the store is shared.
func (st *OAuthStore) SetTTLs(access, refresh, grantMaxAge time.Duration) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if access > 0 {
		st.accessTTL = access
	}
	if refresh > 0 {
		st.refreshTTL = refresh
	}
	if grantMaxAge > 0 {
		st.grantMaxAge = grantMaxAge
	}
}

// CreateGrant records a consent. No tokens exist yet; IssueTokens mints the
// first pair.
func (st *OAuthStore) CreateGrant(in OAuthGrantInput) (*OAuthGrant, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()
	st.evictExpiredLocked(now)
	if len(st.grants) >= maxOAuthGrants {
		return nil, fmt.Errorf("%w: %d live grants", ErrOAuthStoreAtCapacity, maxOAuthGrants)
	}
	maxAge := in.MaxAge
	if maxAge == 0 {
		maxAge = st.grantMaxAge
	}
	g := &OAuthGrant{
		GrantID:   "oag-" + mustHex(8),
		ClientID:  in.ClientID,
		Actor:     in.Actor,
		Delegate:  in.Delegate,
		Scope:     append([]string(nil), in.Scope...),
		Resource:  in.Resource,
		IssuedAt:  now,
		ExpiresAt: now.Add(maxAge),
		Consent:   in.Consent,
	}
	if err := st.appendLocked("identity.oauth.grant.issued", grantData(g), now); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOAuthLedgerAppendFailed, err)
	}
	st.grants[g.GrantID] = g
	return g, nil
}

// IssueTokens mints the grant's first access+refresh pair (the authorization
// code exchange). It succeeds once per grant; a second call is a code replay
// and returns ErrOAuthTokensAlreadyIssued.
func (st *OAuthStore) IssueTokens(grantID string) (*OAuthTokenPair, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()
	g, ok := st.grants[grantID]
	if !ok || g.expired(now) {
		return nil, ErrOAuthGrantNotFound
	}
	if st.issued[grantID] {
		return nil, ErrOAuthTokensAlreadyIssued
	}
	pair, at, rt, err := st.mintPairLocked(g, now)
	if err != nil {
		return nil, err
	}
	d := map[string]interface{}{
		"grant_id":     g.GrantID,
		"access_hash":  at.Hash,
		"refresh_hash": rt.Hash,
		"access_exp":   at.ExpiresAt.Format(time.RFC3339),
		"refresh_exp":  rt.ExpiresAt.Format(time.RFC3339),
		"issued_at":    now.Format(time.RFC3339),
	}
	if err := st.appendLocked("identity.oauth.token.issued", d, now); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOAuthLedgerAppendFailed, err)
	}
	st.issued[grantID] = true
	st.tokens[at.Hash] = at
	st.tokens[rt.Hash] = rt
	return pair, nil
}

// Rotate consumes a refresh token and issues a new pair. Outcomes:
//
//   - unknown / expired / not a refresh token / dead grant: ErrOAuthInvalidGrant
//   - an already-rotated refresh token: the grant is revoked, every token under
//     it dies, and ErrOAuthRefreshReuse is returned. The revocation takes
//     effect in memory even if the ledger append fails (fail closed); that
//     failure is joined onto the returned error.
func (st *OAuthStore) Rotate(refreshToken string) (*OAuthTokenPair, error) {
	if !strings.HasPrefix(refreshToken, OAuthRefreshTokenPrefix) {
		return nil, ErrOAuthInvalidGrant
	}
	hash := sha256Hex(refreshToken)

	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()
	old, ok := st.tokens[hash]
	if !ok || old.Kind != OAuthTokenKindRefresh {
		return nil, ErrOAuthInvalidGrant
	}
	g, ok := st.grants[old.GrantID]
	if !ok || g.expired(now) {
		return nil, ErrOAuthInvalidGrant
	}
	if !old.RotatedAt.IsZero() {
		// Reuse. Detect even if the token has since expired: it was valid once and
		// somebody replayed it.
		var lerr error
		if err := st.appendLocked("identity.oauth.refresh.reuse_detected",
			map[string]interface{}{"grant_id": g.GrantID, "presented_hash": hash}, now); err != nil {
			lerr = err
		}
		if err := st.appendLocked("identity.oauth.grant.revoked",
			revokedData(g.GrantID, "system", "refresh_reuse", now), now); err != nil && lerr == nil {
			lerr = err
		}
		st.dropGrantLocked(g.GrantID)
		if lerr != nil {
			return nil, errors.Join(ErrOAuthRefreshReuse, fmt.Errorf("%w: %w", ErrOAuthLedgerAppendFailed, lerr))
		}
		return nil, ErrOAuthRefreshReuse
	}
	if old.expired(now) {
		return nil, ErrOAuthInvalidGrant
	}
	pair, at, rt, err := st.mintPairLocked(g, now)
	if err != nil {
		return nil, err
	}
	d := map[string]interface{}{
		"grant_id":         g.GrantID,
		"old_refresh_hash": old.Hash,
		"new_access_hash":  at.Hash,
		"new_refresh_hash": rt.Hash,
		"access_exp":       at.ExpiresAt.Format(time.RFC3339),
		"refresh_exp":      rt.ExpiresAt.Format(time.RFC3339),
		"issued_at":        now.Format(time.RFC3339),
	}
	if err := st.appendLocked("identity.oauth.token.refreshed", d, now); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOAuthLedgerAppendFailed, err)
	}
	st.tokens[old.Hash] = markRotated(old, rt.Hash, now)
	st.tokens[at.Hash] = at
	st.tokens[rt.Hash] = rt
	st.evictExpiredLocked(now)
	return pair, nil
}

// Resolve returns the live access token and grant for a raw access token.
// Refresh tokens, rotated or expired tokens, and tokens of dead grants do not
// resolve. This is what introspection answers from.
func (st *OAuthStore) Resolve(token string) (*OAuthTokenInfo, bool) {
	if !strings.HasPrefix(token, OAuthAccessTokenPrefix) {
		return nil, false
	}
	return st.ResolveHash(sha256Hex(token))
}

// ResolveHash is Resolve for a caller that already holds the hash.
func (st *OAuthStore) ResolveHash(hash string) (*OAuthTokenInfo, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	now := st.now()
	t, ok := st.tokens[hash]
	if !ok || t.Kind != OAuthTokenKindAccess || t.expired(now) {
		return nil, false
	}
	g, ok := st.grants[t.GrantID]
	if !ok || g.expired(now) {
		return nil, false
	}
	return &OAuthTokenInfo{Token: *t, Grant: g}, true
}

// IsToken reports whether token is a token this store knows (any kind, rotated
// or not) under a live grant. The grant gate uses it to refuse OAuth tokens
// presented as kernel credentials, as it does for share links.
func (st *OAuthStore) IsToken(token string) bool {
	if !strings.HasPrefix(token, OAuthAccessTokenPrefix) && !strings.HasPrefix(token, OAuthRefreshTokenPrefix) {
		return false
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	t, ok := st.tokens[sha256Hex(token)]
	if !ok {
		return false
	}
	_, live := st.grants[t.GrantID]
	return live
}

// Revoke ends a grant and, with it, every access and refresh token under it.
// The ledger append happens first; on append failure the grant stays live and
// the error wraps ErrOAuthLedgerAppendFailed.
func (st *OAuthStore) Revoke(grantID, by, reason string) (*OAuthGrant, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	g, ok := st.grants[grantID]
	if !ok {
		return nil, ErrOAuthGrantNotFound
	}
	now := st.now()
	if err := st.appendLocked("identity.oauth.grant.revoked", revokedData(grantID, by, reason, now), now); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOAuthLedgerAppendFailed, err)
	}
	st.dropGrantLocked(grantID)
	return g, nil
}

// Grant returns the live grant with that id.
func (st *OAuthStore) Grant(grantID string) (*OAuthGrant, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	g, ok := st.grants[grantID]
	if !ok || g.expired(st.now()) {
		return nil, false
	}
	return g, true
}

// Snapshot returns the live grants, oldest first. No token or token hash is
// reachable from a grant.
func (st *OAuthStore) Snapshot() []*OAuthGrant {
	st.mu.RLock()
	defer st.mu.RUnlock()
	now := st.now()
	out := make([]*OAuthGrant, 0, len(st.grants))
	for _, g := range st.grants {
		if !g.expired(now) {
			out = append(out, g)
		}
	}
	sortGrantsByIssued(out)
	return out
}

// Len returns the number of tracked grants.
func (st *OAuthStore) Len() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.grants)
}

// ─── internals ───────────────────────────────────────────────────────────────

func sortGrantsByIssued(gs []*OAuthGrant) {
	for i := 1; i < len(gs); i++ {
		for j := i; j > 0 && (gs[j].IssuedAt.Before(gs[j-1].IssuedAt) ||
			(gs[j].IssuedAt.Equal(gs[j-1].IssuedAt) && gs[j].GrantID < gs[j-1].GrantID)); j-- {
			gs[j], gs[j-1] = gs[j-1], gs[j]
		}
	}
}

// mintPairLocked generates a fresh access+refresh pair for g. It does not
// store anything. Refresh expiry never outlives the grant.
func (st *OAuthStore) mintPairLocked(g *OAuthGrant, now time.Time) (*OAuthTokenPair, *OAuthToken, *OAuthToken, error) {
	if len(st.tokens)+2 > maxOAuthTokens {
		st.evictExpiredLocked(now)
		if len(st.tokens)+2 > maxOAuthTokens {
			return nil, nil, nil, fmt.Errorf("%w: %d tokens", ErrOAuthStoreAtCapacity, maxOAuthTokens)
		}
	}
	rawA, err := newOAuthToken(OAuthAccessTokenPrefix)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %w", ErrOAuthEntropyUnavailable, err)
	}
	rawR, err := newOAuthToken(OAuthRefreshTokenPrefix)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %w", ErrOAuthEntropyUnavailable, err)
	}
	accessExp := minTime(now.Add(st.accessTTL), g.ExpiresAt)
	refreshExp := minTime(now.Add(st.refreshTTL), g.ExpiresAt)
	at := &OAuthToken{Hash: sha256Hex(rawA), Kind: OAuthTokenKindAccess, GrantID: g.GrantID, IssuedAt: now, ExpiresAt: accessExp}
	rt := &OAuthToken{Hash: sha256Hex(rawR), Kind: OAuthTokenKindRefresh, GrantID: g.GrantID, IssuedAt: now, ExpiresAt: refreshExp}
	pair := &OAuthTokenPair{
		GrantID: g.GrantID, AccessToken: rawA, RefreshToken: rawR,
		AccessExpiresAt: accessExp, RefreshExpiresAt: refreshExp,
	}
	return pair, at, rt, nil
}

func newOAuthToken(prefix string) (string, error) {
	v, err := randomURLToken(oauthTokenBytes)
	if err != nil {
		return "", err
	}
	return prefix + v, nil
}

func minTime(a, b time.Time) time.Time {
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}

func markRotated(old *OAuthToken, replacedBy string, now time.Time) *OAuthToken {
	c := *old
	c.RotatedAt = now
	c.ReplacedBy = replacedBy
	return &c
}

// dropGrantLocked removes the grant and every token under it.
func (st *OAuthStore) dropGrantLocked(grantID string) {
	delete(st.grants, grantID)
	delete(st.issued, grantID)
	for h, t := range st.tokens {
		if t.GrantID == grantID {
			delete(st.tokens, h)
		}
	}
}

// evictExpiredLocked drops expired grants (with their tokens) and expired
// tokens. A rotated refresh token is kept until its own expiry: that is the
// window in which its reuse is still detectable.
func (st *OAuthStore) evictExpiredLocked(now time.Time) {
	for id, g := range st.grants {
		if g.expired(now) {
			st.dropGrantLocked(id)
		}
	}
	for h, t := range st.tokens {
		if t.expired(now) {
			delete(st.tokens, h)
		}
	}
}

// ─── PKCE ────────────────────────────────────────────────────────────────────

var oauthVerifierRe = regexp.MustCompile(`^[A-Za-z0-9._~-]{43,128}$`)

// OAuthPKCEChallengeS256 returns BASE64URL(SHA256(verifier)) without padding
// (RFC 7636 section 4.2).
func OAuthPKCEChallengeS256(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// OAuthPKCEVerifyS256 reports whether verifier is a well-formed code_verifier
// (RFC 7636 section 4.1) whose S256 challenge equals challenge. Constant-time
// comparison. Only S256 exists here: "plain" is deliberately not implemented.
func OAuthPKCEVerifyS256(challenge, verifier string) bool {
	if challenge == "" || !oauthVerifierRe.MatchString(verifier) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(challenge), []byte(OAuthPKCEChallengeS256(verifier))) == 1
}

// ─── ledger ──────────────────────────────────────────────────────────────────

func grantData(g *OAuthGrant) map[string]interface{} {
	return map[string]interface{}{
		"grant_id":   g.GrantID,
		"client_id":  g.ClientID,
		"actor":      g.Actor,
		"delegate":   g.Delegate,
		"scope":      g.Scope,
		"resource":   g.Resource,
		"issued_at":  g.IssuedAt.Format(time.RFC3339),
		"expires_at": g.ExpiresAt.Format(time.RFC3339),
		"consent": map[string]interface{}{
			"request_id":      g.Consent.RequestID,
			"code_hash":       g.Consent.CodeHash,
			"approved_via":    g.Consent.ApprovedVia,
			"approver_device": g.Consent.ApproverDevice,
		},
	}
}

func revokedData(grantID, by, reason string, now time.Time) map[string]interface{} {
	return map[string]interface{}{
		"grant_id":   grantID,
		"by":         by,
		"reason":     reason,
		"revoked_at": now.Format(time.RFC3339),
	}
}

func (st *OAuthStore) appendLocked(eventType string, data map[string]interface{}, now time.Time) error {
	if st.workspaceRoot == "" {
		return nil
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: now.Format(time.RFC3339),
			SessionID: oauthLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-oauth"},
	}
	return st.appendEvent(st.workspaceRoot, oauthLedgerSession, env)
}

// RebuildOAuthStoreFromLedger replays the identity-oauth ledger. A missing file
// is an empty store; malformed lines are skipped. Expired tokens and grants are
// dropped once replay ends.
func RebuildOAuthStoreFromLedger(workspaceRoot string) (*OAuthStore, error) {
	st := NewOAuthStoreWithLedger(workspaceRoot)
	if workspaceRoot == "" {
		return st, nil
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", oauthLedgerSession, "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, fmt.Errorf("open oauth ledger: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			continue
		}
		st.applyLedgerEvent(&env)
	}
	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("scan oauth ledger: %w", err)
	}
	st.evictExpiredLocked(st.now())
	return st, nil
}

// applyLedgerEvent runs only during rebuild, before the store is published.
func (st *OAuthStore) applyLedgerEvent(env *EventEnvelope) {
	d := env.HashedPayload.Data
	str := func(m map[string]interface{}, k string) string { s, _ := m[k].(string); return s }
	gid := str(d, "grant_id")
	if gid == "" {
		return
	}
	switch env.HashedPayload.Type {
	case "identity.oauth.grant.issued":
		g := &OAuthGrant{
			GrantID:   gid,
			ClientID:  str(d, "client_id"),
			Actor:     str(d, "actor"),
			Delegate:  str(d, "delegate"),
			Scope:     stringSliceFromAny(d["scope"]),
			Resource:  str(d, "resource"),
			IssuedAt:  parseRFC3339Lenient(d["issued_at"]),
			ExpiresAt: parseRFC3339Lenient(d["expires_at"]),
		}
		if c, ok := d["consent"].(map[string]interface{}); ok {
			g.Consent = OAuthConsent{
				RequestID:      str(c, "request_id"),
				CodeHash:       str(c, "code_hash"),
				ApprovedVia:    str(c, "approved_via"),
				ApproverDevice: str(c, "approver_device"),
			}
		}
		if g.ClientID == "" || g.ExpiresAt.IsZero() {
			return // malformed: a grant with no expiry would never end
		}
		st.grants[gid] = g
	case "identity.oauth.token.issued":
		if _, ok := st.grants[gid]; !ok {
			return
		}
		issued := parseRFC3339Lenient(d["issued_at"])
		st.insertTokenReplay(gid, OAuthTokenKindAccess, str(d, "access_hash"), issued, d["access_exp"])
		st.insertTokenReplay(gid, OAuthTokenKindRefresh, str(d, "refresh_hash"), issued, d["refresh_exp"])
		st.issued[gid] = true
	case "identity.oauth.token.refreshed":
		if _, ok := st.grants[gid]; !ok {
			return
		}
		issued := parseRFC3339Lenient(d["issued_at"])
		newR := str(d, "new_refresh_hash")
		if old, ok := st.tokens[str(d, "old_refresh_hash")]; ok && old.GrantID == gid {
			st.tokens[old.Hash] = markRotated(old, newR, issued)
		}
		st.insertTokenReplay(gid, OAuthTokenKindAccess, str(d, "new_access_hash"), issued, d["access_exp"])
		st.insertTokenReplay(gid, OAuthTokenKindRefresh, newR, issued, d["refresh_exp"])
	case "identity.oauth.refresh.reuse_detected", "identity.oauth.grant.revoked":
		// Reuse revokes even if the follow-up revoked event never reached the
		// ledger (the Rotate path fails closed in memory; replay matches it).
		st.dropGrantLocked(gid)
	}
}

func (st *OAuthStore) insertTokenReplay(grantID, kind, hash string, issued time.Time, exp interface{}) {
	if !oauthHashRe.MatchString(hash) {
		return
	}
	st.tokens[hash] = &OAuthToken{
		Hash: hash, Kind: kind, GrantID: grantID, IssuedAt: issued,
		ExpiresAt: parseRFC3339Lenient(exp),
	}
}
