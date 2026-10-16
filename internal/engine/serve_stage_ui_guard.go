// serve_stage_ui_guard.go — the Stage guard's request-level protections,
// enforced by the kernel's Stage UI listener before anything is proxied.
//
// This is a port of the request-level half of apps/stage/guard.py (P3a of the
// stage-into-kernel campaign). Evaluation order is part of the contract and
// matches guard.check: lock, host, websocket Origin, then the /local rules
// (404 not local, 403 cross-site, 415 json only).
//
// Scope. Key VALIDATION stays in the Python Stage for now; the kernel only
// enforces what guard.py enforces at the request level. The Python guard keeps
// running behind this one (defense in depth), so a request must satisfy both.
//
// Deliberate deviations from guard.py (each is stricter, never looser):
//   - Host header must be exactly host[:port] ("localhost:80@evil.example" is
//     refused here; guard.py's first-colon split lets it through).
//   - A non-GET/HEAD /local request with a chunked body (no Content-Length) is
//     treated as having a body, so text/plain chunked cannot bypass the 415.
//   - The /local rules also apply when the CLEANED path is under /local
//     (//local/x, /./local/x), spellings Python would only 404 later. The
//     lockout exemption still uses the raw path, as guard.py does.
//   - Websocket refusals are a bare HTTP 403 (what the ASGI close 4403 looks
//     like on the wire), whatever the reason.
//
// Key-guess lockout. guard.py counts a 404 (or a websocket close 4404) on a
// keyed path (/r/ /k/ /h/ /pair/) only when the key segment is unknown. The
// kernel cannot see keys, so on such a response it asks the Stage whether the
// key opens something (GET /r/<k>/device or GET /h/<k>/ answers non-404) and
// counts a miss only when it does not. The probe runs as the Stage host itself
// (no cf-connecting-ip), so it is never counted by the Python guard.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Lockout constants, identical to guard.py.
const (
	stageGuardLimit   = 20
	stageGuardWindowS = 600
	stageGuardLockS   = 3600
)

var stageGuardLoopbackHosts = map[string]bool{"localhost": true, "127.0.0.1": true, "[::1]": true}

var (
	stageGuardPortRe   = regexp.MustCompile(`^(:[0-9]*)?$`)
	stageGuardSchemeRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.\-]*:$`)
	stageGuardSegRe    = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	stageGuardKeyLogRe = regexp.MustCompile(`/(r|k|h)/[A-Za-z0-9_\-]{12,}`)
)

// stageGuard holds the front-door state: public host, lockout tables, clock.
type stageGuard struct {
	publicHost string // lower-case host, no port; "" = loopback hosts only
	now        func() time.Time
	// keyKnown reports whether the key segment of a keyed path opens something
	// upstream. Replaced in tests.
	keyKnown func(ctx context.Context, seg string) bool

	mu     sync.Mutex
	fails  map[string][]time.Time
	locked map[string]time.Time
}

func newStageGuard(publicHost string) *stageGuard {
	return &stageGuard{
		publicHost: stageGuardHostOnly(normalizeStagePublicHost(publicHost)),
		now:        time.Now,
		fails:      map[string][]time.Time{},
		locked:     map[string]time.Time{},
	}
}

// normalizeStagePublicHost accepts "stage.myrgic.com", "stage.myrgic.com:443"
// or "https://stage.myrgic.com/" and returns the netloc.
func normalizeStagePublicHost(s string) string {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "://") {
		return stageGuardNetloc(s)
	}
	return strings.TrimRight(s, "/")
}

// stageGuardHostOnly mirrors guard._host_only, plus a strictness check: what
// follows the host must be empty or ":digits". Returns "" when malformed.
func stageGuardHostOnly(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if strings.HasPrefix(h, "[") {
		i := strings.Index(h, "]")
		if i < 0 {
			return ""
		}
		if !stageGuardPortRe.MatchString(h[i+1:]) {
			return ""
		}
		return h[:i+1]
	}
	host, rest, _ := strings.Cut(h, ":")
	if rest != "" && !stageGuardPortRe.MatchString(":"+rest) {
		return ""
	}
	return host
}

// stageGuardNetloc mirrors urlsplit(u).netloc: the authority after "//", only
// when the prefix is empty or a scheme ("null" and "localhost:3000" have none).
func stageGuardNetloc(u string) string {
	i := strings.Index(u, "//")
	if i < 0 {
		return ""
	}
	if pre := u[:i]; pre != "" && !stageGuardSchemeRe.MatchString(pre) {
		return ""
	}
	rest := u[i+2:]
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func (g *stageGuard) allowedHost(h string) bool {
	h = stageGuardHostOnly(h)
	return stageGuardLoopbackHosts[h] || (h != "" && h == g.publicHost)
}

func (g *stageGuard) originAllowed(origin string) bool {
	return g.allowedHost(stageGuardNetloc(origin))
}

// stageReqInfo is the per-request classification, carried to ModifyResponse.
type stageReqInfo struct {
	ip   string
	path string
	self bool
	ws   bool
}

func stageIsWebSocket(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func stageHasHeader(r *http.Request, name string) bool {
	return len(r.Header.Values(name)) > 0
}

func stageClassify(r *http.Request) stageReqInfo {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	cf := r.Header.Get("Cf-Connecting-Ip")
	ip := cf
	if ip == "" {
		ip = peer
	}
	// guard.is_self: a loopback peer that did not come through the tunnel.
	self := (peer == "127.0.0.1" || peer == "::1") && !stageHasHeader(r, "Cf-Connecting-Ip")
	return stageReqInfo{ip: ip, path: r.URL.Path, self: self, ws: stageIsWebSocket(r)}
}

func (g *stageGuard) isLocked(ip string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if t, ok := g.locked[ip]; ok {
		if g.now().Before(t) {
			return true
		}
		delete(g.locked, ip)
	}
	return false
}

func (g *stageGuard) noteMiss(ip, p string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	var xs []time.Time
	for _, t := range g.fails[ip] {
		if now.Sub(t) < stageGuardWindowS*time.Second {
			xs = append(xs, t)
		}
	}
	xs = append(xs, now)
	g.fails[ip] = xs
	if _, locked := g.locked[ip]; len(xs) > stageGuardLimit && !locked {
		g.locked[ip] = now.Add(stageGuardLockS * time.Second)
		slog.Warn("stage-ui: guard-lockout", "ip", ip, "misses", len(xs), "last_path", stageGuardLogPath(p))
	}
}

// unlock lifts one IP (or all when ip is empty), like guard.unlock.
func (g *stageGuard) unlock(ip string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var ips []string
	if ip != "" {
		ips = []string{ip}
	} else {
		seen := map[string]bool{}
		for k := range g.locked {
			seen[k] = true
		}
		for k := range g.fails {
			seen[k] = true
		}
		for k := range seen {
			ips = append(ips, k)
		}
	}
	for _, k := range ips {
		delete(g.locked, k)
		delete(g.fails, k)
	}
	slog.Info("stage-ui: guard-unlock", "ips", ips)
	return ips
}

func stageGuardLogPath(p string) string {
	p = stageGuardKeyLogRe.ReplaceAllString(p, "/$1/…")
	if len(p) > 80 {
		p = p[:80]
	}
	return p
}

// check returns a refusal (status, reason) or (0, "") to let the request on.
// It is guard.check, in the same order.
func (g *stageGuard) check(r *http.Request, info stageReqInfo) (int, string) {
	p := r.URL.Path
	isLocalRaw := strings.HasPrefix(p, "/local")
	if !info.self && g.isLocked(info.ip) && !isLocalRaw {
		return http.StatusTooManyRequests, "locked out"
	}
	if !g.allowedHost(r.Host) {
		return http.StatusMisdirectedRequest, "unknown host"
	}
	origin := r.Header.Get("Origin")
	if info.ws && origin != "" && !g.originAllowed(origin) {
		return http.StatusForbidden, "foreign origin"
	}
	if isLocalRaw || strings.HasPrefix(path.Clean(p), "/local") {
		if stageHasHeader(r, "Cf-Connecting-Ip") || !stageGuardLoopbackHosts[stageGuardHostOnly(r.Host)] {
			return http.StatusNotFound, "not local"
		}
		sfs := r.Header.Get("Sec-Fetch-Site")
		if sfs == "cross-site" || sfs == "same-site" || (origin != "" && !g.originAllowed(origin)) {
			return http.StatusForbidden, "cross-site"
		}
		if !info.ws && r.Method != http.MethodGet && r.Method != http.MethodHead {
			ct, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
			ct = strings.ToLower(strings.TrimSpace(ct))
			// ContentLength != 0 includes -1 (chunked): see the file header.
			if ct != "application/json" && r.ContentLength != 0 {
				return http.StatusUnsupportedMediaType, "json only"
			}
		}
	}
	return 0, ""
}

func stageIsKeyedPath(p string) bool {
	for _, pre := range []string{"/r/", "/k/", "/h/", "/pair/"} {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}
	return false
}

// refuse writes the refusal: JSON for http, a bare 403 for websocket.
func (g *stageGuard) refuse(w http.ResponseWriter, r *http.Request, info stageReqInfo, code int, why string) {
	slog.Warn("stage-ui: guard-refused", "why", why, "code", code, "path", stageGuardLogPath(r.URL.Path),
		"ip", info.ip, "host", truncate80(r.Host))
	if info.ws {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	body, _ := json.Marshal(map[string]string{"error": why})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func truncate80(s string) string {
	if len(s) > 80 {
		return s[:80]
	}
	return s
}

// handle runs the guard. It returns the (possibly body-restored) request and
// true when the request may proceed.
func (g *stageGuard) handle(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	info := stageClassify(r)
	if code, why := g.check(r, info); code != 0 {
		g.refuse(w, r, info, code, why)
		return r, false
	}
	// Owner lift of a lockout also clears the kernel's table.
	if r.Method == http.MethodPost && r.URL.Path == "/local/dev/guard" {
		b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		r.Body = io.NopCloser(bytes.NewReader(b))
		var req struct {
			IP string `json:"ip"`
		}
		_ = json.Unmarshal(b, &req)
		g.unlock(req.IP)
	}
	ctx := context.WithValue(r.Context(), stageReqKey{}, &info)
	return r.WithContext(ctx), true
}

type stageReqKey struct{}

// afterResponse is the proxy's ModifyResponse: count unknown-key guesses.
func (g *stageGuard) afterResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(stageReqKey{}).(*stageReqInfo)
	if info == nil || info.self || !stageIsKeyedPath(info.path) {
		return nil
	}
	// http: 404. websocket: the pre-accept close 4404 is an HTTP 403 on the
	// wire (indistinguishable from 4403), so any non-101 error counts as a
	// candidate and the key probe decides.
	candidate := resp.StatusCode == http.StatusNotFound || (info.ws && resp.StatusCode != http.StatusSwitchingProtocols && resp.StatusCode >= 400)
	if !candidate {
		return nil
	}
	parts := strings.Split(info.path, "/")
	seg := ""
	if len(parts) > 2 {
		seg = parts[2]
	}
	if seg != "" && g.keyKnown(resp.Request.Context(), seg) {
		return nil
	}
	g.noteMiss(info.ip, info.path)
	return nil
}

// probeKeyKnown builds the default keyKnown: a room/member key answers
// GET /r/<k>/device non-404, a home key answers GET /h/<k>/ non-404.
func probeKeyKnown(upstream *url.URL) func(context.Context, string) bool {
	client := &http.Client{Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, seg string) bool {
		if !stageGuardSegRe.MatchString(seg) {
			return false
		}
		for _, p := range []string{"/r/" + seg + "/device", "/h/" + seg + "/"} {
			req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodGet, upstream.ResolveReference(&url.URL{Path: p}).String(), nil)
			if err != nil {
				return true
			}
			req.Host = "127.0.0.1"
			resp, err := client.Do(req)
			if err != nil {
				return true // upstream trouble is not a guess
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				return true
			}
		}
		return false
	}
}
