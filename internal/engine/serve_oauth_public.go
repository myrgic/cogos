// serve_oauth_public.go — the OAuth AS on the public stage_ui listener (K5).
//
// The kernel is one process with two listeners. :6932 is the API mux
// (grant-gated, loopback). :7873 is the stage_ui listener, the ONLY public door
// (cloudflared -> stage.myrgic.com); everything on it that is not kernel-owned
// is proxied to the Python stage on :7872. claude.ai reaches the authorization
// server through :7873, so the four public AS paths must be answered by the
// kernel there, BEFORE the proxy. This file owns that, and its limits:
//
//   - EXACT allowlist, not a prefix. isOAuthListenerPath admits precisely
//     GET /.well-known/oauth-authorization-server, GET /oauth/authorize,
//     GET /oauth/authorize/status/{id} (one id segment, a closed charset) and
//     POST /oauth/token. Anything else, including every other /oauth/... path
//     and the whole of /v1/oauth/*, falls through to the proxy exactly as it
//     did before K5. isOAuthPublicPath (serve_oauth.go) is a PREFIX predicate
//     for the :6932 grant-gate exemption and must not be used here.
//   - A DEDICATED mux holds the four handlers. The main API mux is never
//     reachable from this listener, so /v1/oauth/introspect, approve/deny and
//     the grant list/revoke cannot be served here by construction, not just by
//     the predicate. Both layers are tested.
//   - /oauth/token is rate limited per client IP (and under a global ceiling),
//     with 429 + Retry-After. The IP is the one the stage guard already keys
//     on: Cf-Connecting-Ip when present, else the TCP peer (stageClassify).
//     That header is trustworthy only because the listener is loopback-bound
//     and the tunnel is the single remote path in (validateStageUIAddr).
//   - The kernel never decides what an OAuth access token may do on the
//     proxied stage routes. It does not verify one there; the stage's /mcp
//     resource server introspects it via /v1/oauth/introspect on :6932.
package engine

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// oauthTokenIPLimit is /oauth/token requests allowed per client IP per
	// window. A real client calls it once per authorization and once per
	// access-token lifetime; 30/min is far above that and far below a guessing
	// attack on a 256-bit code or refresh token.
	oauthTokenIPLimit  = 30
	oauthTokenIPWindow = time.Minute
	// oauthTokenGlobalLimit caps /oauth/token across all IPs so a spray from
	// many addresses cannot make the endpoint a CPU or ledger-write hot spot.
	oauthTokenGlobalLimit = 600
	// oauthTokenMaxTrackedIPs bounds the per-IP table. When full, expired
	// entries are swept; if still full, new IPs share one overflow bucket
	// rather than growing memory or evicting an active client.
	oauthTokenMaxTrackedIPs = 4096

	// oauthStatusIDMax bounds the {id} segment of the status path.
	oauthStatusIDMax = 64
)

// oauthTokenReadTimeout bounds how long the listener waits for a token request
// body. The listener has no server-wide ReadTimeout (WebSocket and SSE are
// long-lived), so without this a slow-body POST pins a goroutine. A var only so
// the test can shorten it.
var oauthTokenReadTimeout = 15 * time.Second

// isOAuthListenerPath is the exact allowlist of public OAuth paths served on
// the stage_ui listener. It matches r.URL.Path as received (never a cleaned
// form): "//oauth/token", "/oauth/token/", "/oauth/../oauth/token" and
// "/OAuth/token" are not on the list and fall through to the proxy.
func isOAuthListenerPath(p string) bool {
	switch p {
	case oauthWellKnownASPath, oauthWellKnownPRPath, oauthAuthorizePath, oauthTokenPath:
		return true
	}
	if !strings.HasPrefix(p, oauthAuthorizeStatusPath) {
		return false
	}
	id := p[len(oauthAuthorizeStatusPath):]
	if id == "" || len(id) > oauthStatusIDMax {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false // includes '/', '%', '.', and anything after the id
		}
	}
	return true
}

// oauthPublicLimiter is the /oauth/token limiter: a fixed-window grantMintLimiter
// per IP, an overflow bucket, and a global ceiling.
type oauthPublicLimiter struct {
	mu       sync.Mutex
	perIP    map[string]*grantMintLimiter
	overflow *grantMintLimiter
	global   *grantMintLimiter
	ipLimit  int
	window   time.Duration
	maxIPs   int
	now      func() time.Time
}

func newOAuthPublicLimiter(ipLimit, globalLimit int, window time.Duration, maxIPs int) *oauthPublicLimiter {
	l := &oauthPublicLimiter{
		perIP:   make(map[string]*grantMintLimiter),
		ipLimit: ipLimit,
		window:  window,
		maxIPs:  maxIPs,
		now:     time.Now,
	}
	l.overflow = l.newBucket(ipLimit)
	l.global = l.newBucket(globalLimit)
	return l
}

// newBucket builds a grantMintLimiter on the limiter's clock.
func (l *oauthPublicLimiter) newBucket(limit int) *grantMintLimiter {
	b := newGrantMintLimiter(limit, l.window)
	b.now = func() time.Time { return l.now() }
	return b
}

// allow reports whether a request from ip may proceed. When it may not,
// retryAfter is how long until the limiting window resets (at least 1s).
// The per-IP bucket is charged first so one abusive IP cannot burn the global
// budget with requests it was already going to be refused.
func (l *oauthPublicLimiter) allow(ip string) (ok bool, retryAfter time.Duration) {
	if len(ip) > 64 { // longer than any IPv6 literal; bound the table's key size
		ip = ip[:64]
	}
	b := l.bucketFor(ip)
	if !b.Allow() {
		return false, b.retryAfter()
	}
	if !l.global.Allow() {
		return false, l.global.retryAfter()
	}
	return true, 0
}

func (l *oauthPublicLimiter) bucketFor(ip string) *grantMintLimiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.perIP[ip]; ok {
		return b
	}
	if len(l.perIP) >= l.maxIPs {
		now := l.now()
		for k, b := range l.perIP {
			if b.idle(now) {
				delete(l.perIP, k)
			}
		}
		if len(l.perIP) >= l.maxIPs {
			return l.overflow
		}
	}
	b := l.newBucket(l.ipLimit)
	l.perIP[ip] = b
	return b
}

// retryAfter is the time left in the bucket's current window, rounded up to a
// whole second and never below one.
func (l *grantMintLimiter) retryAfter() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	left := l.windowStart.Add(l.window).Sub(l.now())
	if left < time.Second {
		return time.Second
	}
	return left
}

// idle reports whether the bucket's window has fully elapsed (nothing to remember).
func (l *grantMintLimiter) idle(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.windowStart.IsZero() || now.Sub(l.windowStart) >= l.window
}

// oauthPublicHandler builds the handler for the four public AS paths on the
// stage_ui listener. It returns nil when oauth.enabled is false, so a kernel
// without the oauth: block leaves the listener exactly as it was.
//
// It is only ever invoked for paths that passed isOAuthListenerPath; its own
// mux is a second, independent allowlist (and carries the method patterns).
func (s *Server) oauthPublicHandler() http.Handler {
	if s.cfg == nil || !s.cfg.OAuth.Enabled {
		return nil
	}
	mux := http.NewServeMux()
	reg := func(pattern string, h http.HandlerFunc) {
		method, p := splitRoutePattern(pattern)
		mux.HandleFunc(pattern, withSpan(method+" "+p, h, s.spanEmitter))
	}
	reg("GET "+oauthWellKnownASPath, s.handleOAuthASMetadata)
	reg("GET "+oauthWellKnownPRPath, s.handleOAuthPRMetadata)
	reg("GET "+oauthAuthorizePath, s.handleOAuthAuthorize)
	reg("GET "+oauthAuthorizeStatusPath+"{id}", s.handleOAuthAuthorizeStatus)
	reg("POST "+oauthTokenPath, s.handleOAuthToken)

	lim := newOAuthPublicLimiter(oauthTokenIPLimit, oauthTokenGlobalLimit, oauthTokenIPWindow, oauthTokenMaxTrackedIPs)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == oauthTokenPath {
			// Every method counts, before the body is read or parsed.
			if ok, retry := lim.allow(stageClassify(r).ip); !ok {
				w.Header().Set("Retry-After", strconv.Itoa(int((retry+time.Second-1)/time.Second)))
				oauthTokenError(w, http.StatusTooManyRequests, "temporarily_unavailable",
					"too many token requests; retry after the interval in Retry-After")
				return
			}
			// Not supported by every ResponseWriter (httptest); the real
			// listener's is, which is the case that matters.
			_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(oauthTokenReadTimeout))
		}
		mux.ServeHTTP(w, r)
	})
}
