// serve_grant_auth.go — kernel HTTP write-route grant authentication.
//
// Closes the open CSRF gap documented at length in serve_cors.go's file
// header ("Write-side CSRF is a known, PRE-EXISTING gap"): corsMiddleware
// only ever decided which Access-Control-* headers a RESPONSE carries, never
// whether a request runs — a cross-origin page's CORS-simple POST (or any
// same-origin blind request) reached every mutating handler with no gate at
// all. This file is that gate, per the sealed design in
// cog://mem/procedural/plan-hermes-kernel-loop-wiring (step 5) and the
// operator's move (2026-08-12, verbatim: "can we use the kernel to mint a
// token and store it in the vault and use that to sign the http") — which
// turns out to complete existing machinery rather than add new: board task
// 60 already built kernel-issued ledger-backed identity grants
// (serve_identity_grants.go). This file only wires that machinery onto the
// HTTP request path as middleware, reusing its Verify/VerifyAny logic
// in-process rather than reimplementing any hashing.
//
// Mechanism:
//
//   - Every request whose method is not GET, OR whose path is /mcp
//     (regardless of method — see below), must carry a valid
//     "X-Cogos-Grant: <token>" header. Missing or invalid → 401 with a terse
//     JSON body. Valid means VerifyAny(token) finds a live, unexpired grant
//     anywhere in the registry — this middleware asks "does the caller hold
//     SOME kernel-issued credential", not "which one", so any surface's
//     grant (node-root, a dashboard's own, mod3's own) satisfies it.
//   - /mcp is gated on EVERY method, not just POST/PUT/DELETE, because it is
//     a single JSON-RPC/streamable-HTTP multiplexer — GET /mcp resumes an
//     SSE session, DELETE /mcp tears one down, and POST /mcp carries every
//     tool call including cog_write_cogdoc, the membrane-bypass companion
//     gap this build was explicitly asked to close (see the test file).
//     Carving /mcp out of the blanket "GET routes are exempt" rule below is
//     deliberate, not an oversight.
//   - EXEMPT, unconditionally: GET on any path other than /mcp and
//     /v1/identity/*, GET/HEAD /health, and POST /v1/identity/verify (the
//     verification authority itself — gating it would be circular: a caller
//     with no grant could never learn whether ITS token is valid).
//   - /v1/identity/* is gated on EVERY method, not just writes (ledger L03,
//     audit F1 HIGH). The blanket "GET is exempt" rule above used to cover
//     the identity read routes too, and one of them — the zero-paste
//     primitive — returns a live grant's RAW TOKEN in its response body. The
//     node-root grant is the kernel's admin credential: any unauthenticated
//     process on 127.0.0.1 could GET it and then mint, revoke, or write with
//     full authority, which makes every other check in this file decorative.
//     Carving /v1/identity/* out of the GET exemption closes that. The
//     zero-paste primitive itself is no longer a GET at all — it is
//     POST /v1/identity/grants/current behind this gate (see
//     handleIdentityGrantCurrent), so a raw token never travels in a GET
//     response body regardless of what a future exemption edit does.
//     Bootstrap is unaffected: ensureNodeRootGrant (boot_node_root_grant.go)
//     persists the node-root token to ~/.cog/vault/node-root-grant (0600) at
//     boot, which is where a local consumer with no grant yet reads its
//     first credential — a same-user filesystem read rather than an
//     unauthenticated HTTP read by anything that can open a loopback socket.
//   - SCOPE ENFORCEMENT (ledger L02, audit R-C1). Past this point the
//     presented grant's Scope is no longer documentary: routes classified by
//     requiredScopeForRequest demand a matching scope, and a live grant
//     without it gets 403 insufficient_scope. See that function and
//     grantHasScope for the mapping and the node-root carve-out.
//   - NO bootstrap exemption for POST /v1/identity/grants (mint). An earlier
//     version of this file left the mint route reachable without a grant,
//     reasoning that a brand-new local consumer has no token yet and needs
//     some way to get its first one. That reasoning only ever covered
//     CONFIDENTIALITY (corsLoopbackOnly hides the response body from a
//     cross-origin reader) and missed integrity/availability: an
//     unauthenticated caller — including a blind cross-origin CSRF POST that
//     never reads the response — could mint a superseding grant for
//     surface="node-root" with a different scope than the live one, which
//     invalidates that grant for every OTHER local consumer already caching
//     it (cog-review finding, PR #551, this file:160 as of commit
//     00bc7b2 — see also VerifyAny's doc comment). The exemption was never
//     actually necessary: ensureNodeRootGrant (boot_node_root_grant.go)
//     mints the node-root credential IN-PROCESS at boot, with no HTTP call
//     involved, and every local consumer acquires it from the vault file
//     ~/.cog/vault/node-root-grant (0600, written at boot by ensureNodeRootGrant)
//     or, holding a grant, via POST /v1/identity/grants/current behind the gate
//     (the GET was removed under ledger L03; see below). So POST /v1/identity/grants is gated exactly
//     like every other write route below: it requires a valid presented
//     grant. What used to be the "bootstrap" case is now just "present the
//     node-root token you already fetched via that GET" — indistinguishable
//     from any other authenticated write. grantMintLimiter still runs on
//     this route as defense-in-depth against a script that mints excessively
//     once it does hold a valid grant (cheap to keep, harmless now that the
//     route requires authentication to reach at all).
//   - SURFACE-MATCH on the two admin-shaped routes (mint, revoke): even with
//     the bootstrap exemption gone, VerifyAny alone would let ANY live grant
//     — including one for an unrelated, throwaway surface an authenticated
//     caller minted for itself — mint a grant for a DIFFERENT surface, or
//     revoke node-root's own grant outright (its grant_id was visible via the
//     then-gate-exempt GET /v1/identity/grants; ledger L03 has since put all
//     of /v1/identity/* behind this gate). Closing that residual (cog-review
//     unverified note, PR #551) means handleIdentityGrantMint and
//     handleIdentityGrantRevoke (serve_identity_grants.go) additionally
//     require the presented grant's surface to be "node-root" (the admin
//     surface) or to match the target surface (self-service rotation of a
//     surface's own grant). This is a minimal rule, not a full scope model —
//     Wave 6b is where per-surface authorization gets designed properly; see
//     each handler's doc comment for the exact check. The check applies ONLY
//     while grantAuthDisabled() reports false — an earlier version enforced
//     it unconditionally, which meant Config.WriteRouteGrantAuthDisabled
//     could not actually restore mint/revoke to their pre-gate behavior (the
//     context grant it reads is only ever populated when this middleware
//     runs the real check below), contradicting the disable knob's own
//     contract (cog-review finding, PR #551 round 3).
//
// CSRF threat model: a custom header (X-Cogos-Grant) is not one of the
// Fetch spec's CORS-safelisted headers, so any browser request carrying it
// forces a CORS preflight — and corsMiddleware's OPTIONS handling only
// echoes Access-Control-Allow-Origin for a loopback (or otherwise permitted)
// Origin, so a REMOTE page's preflight for a write route fails at the
// browser and the real request never goes out. That is what actually closes
// the CSRF hole this middleware exists for; the 401 body is defense for the
// same-origin/curl/blind case where no preflight ever ran.
//
// Enforcement can be turned off entirely via
// Config.WriteRouteGrantAuthDisabled (see config.go's doc comment on that
// field for the default-ON / fail-safe rationale) — Boot() logs loudly when
// it is.
package engine

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// grantContextKey is the unexported context-value key grantAuthMiddleware
// uses to hand the presented (already-verified) *IdentityGrant down to
// handlers that need to know WHO is calling, not just THAT the call is
// authenticated — currently handleIdentityGrantMint and
// handleIdentityGrantRevoke's surface-match check (see the file header's
// "SURFACE-MATCH" section). A struct{} key type (rather than a string) so no
// other package can collide with it by accident.
type grantContextKey struct{}

// contextWithGrant returns a copy of ctx carrying grant as the presented,
// verified identity for this request. Called only from grantAuthMiddleware
// once VerifyAny has already succeeded — never set speculatively or before
// verification.
func contextWithGrant(ctx context.Context, grant *IdentityGrant) context.Context {
	return context.WithValue(ctx, grantContextKey{}, grant)
}

// grantFromContext returns the *IdentityGrant grantAuthMiddleware verified
// for this request, if any. Returns (nil, false) for a request that never
// passed through the middleware (e.g. an exempt route, or a unit test that
// wires handlers onto a bare mux without the middleware) — callers must
// treat that the same as "no grant presented," never assume "so anything is
// allowed."
func grantFromContext(ctx context.Context) (*IdentityGrant, bool) {
	grant, ok := ctx.Value(grantContextKey{}).(*IdentityGrant)
	return grant, ok && grant != nil
}

// GrantHeaderName is the header a caller presents a kernel-issued identity
// grant token in. Exported so hook scripts, the dashboard, and tests share
// one literal rather than re-typing "X-Cogos-Grant".
const GrantHeaderName = "X-Cogos-Grant"

// grantTokenFromRequest returns the grant token a request presents, from
// either accepted carrier:
//
//  1. X-Cogos-Grant: <token>            — the kernel's own header (canonical)
//  2. Authorization: Bearer <token>     — the OpenAI-compatible convention
//     (and Anthropic's ANTHROPIC_AUTH_TOKEN path).
//  3. x-api-key: <token>                — the Anthropic SDK's API-key header.
//     A client's Anthropic-provider "API key" field lands HERE, not in
//     Authorization; without this, pointing an Anthropic-shaped client at
//     /v1/messages can never authenticate.
//
// An external client (dsh, Zed, any OpenAI/Anthropic SDK) has exactly one
// auth knob — an "API key" field — and cannot be taught a custom header.
// Pasting the grant into that field is the whole onboarding story for a
// client the kernel did not write.
//
// X-Cogos-Grant wins when both are present. The CSRF threat model in the
// file header is unchanged: neither Authorization nor x-api-key is one of
// the Fetch spec's CORS-safelisted request headers (only Accept,
// Accept-Language, Content-Language, Content-Type-with-simple-values are), so
// a browser request carrying either forces the same preflight that
// corsMiddleware refuses for non-loopback origins. Only the literal "Bearer"
// scheme is honored in Authorization; anything else is treated as absent.
func grantTokenFromRequest(r *http.Request) string {
	if t := r.Header.Get(GrantHeaderName); t != "" {
		return t
	}
	auth := r.Header.Get("Authorization")
	const scheme = "Bearer "
	if len(auth) > len(scheme) && strings.EqualFold(auth[:len(scheme)], scheme) {
		if t := strings.TrimSpace(auth[len(scheme):]); t != "" {
			return t
		}
	}
	return strings.TrimSpace(r.Header.Get("x-api-key"))
}

// grantMintRequestPath is the one route the bootstrap exemption applies to.
const grantMintRequestPath = "/v1/identity/grants"

// grantVerifyRequestPath is exempt for a different reason (see file header):
// gating the verification authority itself would be circular.
const grantVerifyRequestPath = "/v1/identity/verify"

// mcpRequestPath is gated on every method — see the file header for why it
// is carved out of the blanket GET exemption.
const mcpRequestPath = "/mcp"

// identityRoutePrefix covers every route registered by
// registerIdentityGrantRoutes. Requests under it are gated on EVERY method
// (ledger L03) — the blanket GET exemption does not reach them, because one
// of these routes hands back a live grant's raw token and the rest are the
// mint/revoke/list admin surface.
const identityRoutePrefix = "/v1/identity"

// ─── scopes (ledger L02) ─────────────────────────────────────────────────────
//
// IdentityGrant.Scope existed before this and was purely documentary:
// VerifyAny asked "is this token live" and never looked at Scope, so a grant
// minted for one purpose carried the authority of every purpose. These three
// names are the enforced vocabulary; requiredScopeForRequest maps routes onto
// them and grantAuthMiddleware denies a live-but-wrong-scope grant with 403.
const (
	// ScopeInference authorizes the model-inference routes
	// (POST /v1/chat/completions, POST /v1/messages) — the routes that spend
	// provider tokens and reach an external API on the operator's account.
	ScopeInference = "inference"

	// ScopeWrite authorizes mutation of kernel/workspace state: the config
	// mutation routes and /mcp (which multiplexes cog_write_cogdoc and every
	// other write-capable tool — see requiredScopeForRequest on why the
	// whole multiplexer takes the write requirement rather than per-tool).
	ScopeWrite = "write"

	// ScopeAdmin authorizes the identity/grant surface itself: minting,
	// listing, revoking, and reading a live raw token. This is the
	// credential-issuing authority, so it is deliberately separate from
	// ScopeWrite — a surface that may write cogdocs should not thereby be
	// able to mint itself a grant for any other surface.
	ScopeAdmin = "admin"
)

// requiredScopeForRequest returns the scope a caller's grant must carry to
// reach r, or "" when the route carries no scope requirement (liveness alone
// suffices, exactly as before this change — this function only ever ADDS a
// requirement, it never relaxes one).
//
// The classification is intentionally coarse and path-prefix-driven, because
// this runs in middleware that must not consume r.Body: /mcp carries a
// JSON-RPC envelope naming the tool, but reading it here would break the
// downstream handler, so the whole multiplexer takes the strictest
// requirement of the tools it can dispatch (ScopeWrite). A read-only MCP
// caller therefore needs a write-scoped grant; that is the conservative
// direction of the error, and the per-tool split belongs in the MCP dispatch
// layer where the tool name is already parsed.
func requiredScopeForRequest(r *http.Request) string {
	path := r.URL.Path

	// Identity/grant surface — the credential-issuing authority.
	// POST /v1/identity/verify is exempt from the gate entirely (see
	// isGrantExemptRequest) so it never reaches this function; listed here
	// only so the mapping reads as complete.
	if isIdentityRoutePath(path) {
		return ScopeAdmin
	}

	// Service control: start/stop/restart/enable/disable declared services
	// (serve_services.go). These act on processes outside the kernel —
	// including agent gateways — so they are admin-shaped, not plain writes.
	// GET /v1/services* stays exempt via the blanket GET rule.
	if strings.HasPrefix(path, "/v1/services/") && r.Method != http.MethodGet {
		return ScopeAdmin
	}

	// Inference: the routes that spend provider tokens.
	if path == "/v1/chat/completions" || path == "/v1/messages" {
		return ScopeInference
	}

	// Writes: config mutation, and the MCP multiplexer (cogdoc writes and
	// every other mutating tool arrive here).
	if path == mcpRequestPath {
		return ScopeWrite
	}
	if strings.HasPrefix(path, "/v1/config") {
		return ScopeWrite
	}
	if strings.HasPrefix(path, "/v1/settings/") {
		return ScopeWrite
	}

	return ""
}

// grantHasScope reports whether g may act under want.
//
// Fail-closed: a grant whose Scope list does not contain want is denied,
// including a grant with an empty Scope list. The one carve-out is
// nodeRootScope — the kernel's own bootstrap credential (see
// boot_node_root_grant.go) is the root authority and retains every scope.
// That carve-out is matched on the SCOPE STRING rather than the surface name
// so a node-root grant reconstructed from a pre-L02 ledger (minted with
// scope ["node-root"] and reused, not re-minted, across restarts by
// MintOrReuse) keeps working without a ledger migration.
func grantHasScope(g *IdentityGrant, want string) bool {
	if want == "" {
		return true
	}
	if g == nil {
		return false
	}
	for _, have := range g.Scope {
		if have == want || have == nodeRootScope {
			return true
		}
	}
	return false
}

// defaultGrantMintRateLimit and defaultGrantMintRateWindow bound
// POST /v1/identity/grants throughput (see the file header's BOOTSTRAP
// EXEMPTION note). 20/minute comfortably covers every legitimate local
// bootstrap burst (a handful of surfaces each minting once at their own
// startup) while making a blind flood-the-ledger script land well below
// "fills the disk" territory long before an operator would notice the rate
// limit at all.
const (
	defaultGrantMintRateLimit  = 20
	defaultGrantMintRateWindow = time.Minute
)

// grantMintLimiter is a coarse fixed-window rate limiter guarding the
// bootstrap-exempt POST /v1/identity/grants route from being used to flood
// the identity-grants ledger by an unauthenticated (but loopback-bound)
// caller varying surface/scope on every request. Deliberately simple: this
// is a safety net for the loopback threat model, not a general-purpose rate
// limiter — see MintOrReuse's own maxLiveGrantSurfaces cap for the
// complementary "don't grow the live-surface count without bound" defense
// this limiter does not replace.
type grantMintLimiter struct {
	mu          sync.Mutex
	limit       int
	window      time.Duration
	windowStart time.Time
	count       int
	now         func() time.Time
}

func newGrantMintLimiter(limit int, window time.Duration) *grantMintLimiter {
	return &grantMintLimiter{limit: limit, window: window, now: time.Now}
}

// Allow reports whether one more mint is permitted in the current window,
// consuming one slot if so. Resets the window lazily on first call after it
// elapses rather than running a background ticker.
func (l *grantMintLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= l.window {
		l.windowStart = now
		l.count = 0
	}
	if l.count >= l.limit {
		return false
	}
	l.count++
	return true
}

// grantAuthMiddleware wraps mux (or any handler further down the chain)
// with the X-Cogos-Grant gate described in the file header. It is installed
// INSIDE corsMiddleware — i.e. handler := corsMiddleware(s.grantAuthMiddleware(mux))
// in NewServer — so CORS still owns the OPTIONS preflight short-circuit and
// this middleware never has to special-case OPTIONS itself (corsMiddleware
// never calls next for that method).
// grantAuthDisabled reports whether the write-route grant-auth gate is
// turned off (Config.WriteRouteGrantAuthDisabled — see that field's doc
// comment for the default-ON / fail-safe rationale). This is the single
// source of truth grantAuthMiddleware and the surface-match checks in
// handleIdentityGrantMint/handleIdentityGrantRevoke (serve_identity_grants.go)
// both consult, rather than each re-deriving "is the gate on" from s.cfg
// independently — the two had drifted apart once already (cog-review
// finding, PR #551 round 3): the surface-match check required a
// context-attached grant unconditionally, but grantAuthMiddleware only ever
// populates that context when the gate is ENABLED, so with the knob flipped
// the mint/revoke handlers 403'd every request instead of the disable knob's
// documented "restores pre-grant-auth behavior on every write route"
// contract. A shared accessor makes that class of drift structurally harder:
// there is exactly one place that decides "is the gate on," and everything
// else calls it instead of reading s.cfg directly. s.cfg == nil (test paths
// that construct a bare *Server) counts as "not disabled" — same fail-safe
// default the field's own doc comment describes.
func (s *Server) grantAuthDisabled() bool {
	return s.cfg != nil && s.cfg.WriteRouteGrantAuthDisabled
}

func (s *Server) grantAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.grantAuthDisabled() {
			next.ServeHTTP(w, r)
			return
		}

		if isGrantExemptRequest(r) {
			next.ServeHTTP(w, r)
			return
		}

		token := grantTokenFromRequest(r)
		if token == "" {
			writeJSONError(w, http.StatusUnauthorized, "missing_grant",
				GrantHeaderName+" header (or Authorization: Bearer <grant>, or x-api-key: <grant>) required for this route")
			return
		}
		if s.identityGrants == nil {
			// Nil registry should be structurally impossible (NewServer always
			// constructs one), but fail closed rather than panic if it ever
			// happens — a missing registry is not a green light to skip auth.
			writeJSONError(w, http.StatusUnauthorized, "invalid_grant", "no identity grant registry available")
			return
		}
		grant, ok := s.identityGrants.VerifyAny(token)
		if !ok {
			writeJSONError(w, http.StatusUnauthorized, "invalid_grant",
				"grant token is missing, expired, or revoked")
			return
		}

		// Scope enforcement (ledger L02). VerifyAny above answers liveness
		// only; this is where IdentityGrant.Scope stops being documentary.
		// 403 rather than 401: the credential is genuine and the caller is
		// authenticated, it simply lacks authority for THIS route, so
		// re-presenting or refreshing the same grant will not help.
		if want := requiredScopeForRequest(r); !grantHasScope(grant, want) {
			writeJSONError(w, http.StatusForbidden, "insufficient_scope",
				"grant does not carry the '"+want+"' scope required for this route")
			return
		}

		// The mint route (see the file header's "NO bootstrap exemption"
		// section) is no longer reachable without a valid grant, so the rate
		// limiter here is defense-in-depth against an authenticated caller
		// minting excessively — not the primary guard it used to stand in
		// for.
		if r.Method == http.MethodPost && r.URL.Path == grantMintRequestPath {
			if s.grantMintLimiter != nil && !s.grantMintLimiter.Allow() {
				writeJSONError(w, http.StatusTooManyRequests, "rate_limited",
					"too many grant mint requests; see grantMintLimiter in serve_grant_auth.go")
				return
			}
		}

		// Hand the verified grant down to handlers that need to know WHO is
		// calling (the surface-match check in handleIdentityGrantMint /
		// handleIdentityGrantRevoke) — see grantFromContext's doc comment.
		next.ServeHTTP(w, r.WithContext(contextWithGrant(r.Context(), grant)))
	})
}

// isGrantExemptRequest reports whether r may bypass the grant gate
// unconditionally (i.e. before the bootstrap-exemption / rate-limit branch
// even runs). See the file header for the full rationale per case.
func isGrantExemptRequest(r *http.Request) bool {
	// /mcp is NEVER exempt, on any method — carved out of the GET exemption
	// below before it can apply.
	if r.URL.Path == mcpRequestPath {
		return false
	}
	if r.URL.Path == "/health" {
		return true
	}
	if r.Method == http.MethodPost && r.URL.Path == grantVerifyRequestPath {
		return true
	}
	// /v1/identity/* is NEVER exempt on any method (ledger L03) — same
	// reasoning as /mcp above, carved out before the GET rule can apply.
	// POST /v1/identity/verify already returned true above; that is the one
	// deliberate hole and it is method- and path-exact.
	if isIdentityRoutePath(r.URL.Path) {
		return false
	}
	// Every other GET (and HEAD, which ServeMux treats as GET for handler
	// dispatch) is a read — exempt. Everything else (POST/PUT/PATCH/DELETE
	// on any other path) falls through to the grant check.
	return r.Method == http.MethodGet || r.Method == http.MethodHead
}

// isIdentityRoutePath reports whether path is one of the identity/grant
// routes. Matches the prefix exactly or as a path segment boundary so that a
// hypothetical future "/v1/identity-something" route does not get silently
// swept into the gate (or, worse, a "/v1/identityfoo" route silently escape
// a check someone believed was prefix-wide).
func isIdentityRoutePath(path string) bool {
	if path == identityRoutePrefix {
		return true
	}
	return strings.HasPrefix(path, identityRoutePrefix+"/")
}
