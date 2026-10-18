// serve_stage_ui.go — the kernel-owned Stage UI listener (campaign observable O4).
//
// Purpose: let the kernel stand in front of the Python Stage so the tunnel can
// later point at the kernel instead of the Stage, with ZERO behavior change
// while it is off or on. It is the strangler seam: the kernel takes Stage URLs
// group by group (static files first) and proxies everything else.
//
// Opt-in. kernel.yaml:
//
//	stage_ui:
//	  addr: 127.0.0.1:7873          # empty (default) = no listener at all
//	  upstream: http://127.0.0.1:7872   # the Python stage (default shown)
//	  static_dir: /path/to/ui       # served from disk, never embedded
//	  static_prefixes: [/app-icons/] # URL prefixes eligible for static serving
//	  owned_prefixes: [/v1/rooms]  # route groups the kernel owns (see below)
//	  public_host: stage.myrgic.com  # extra Host the guard accepts (empty = loopback only)
//	  keys: shadow                  # off (default) | shadow | enforce
//	  local_auth: enforce           # off (default) | enforce
//	  local_actor: <operator>       # the actor a /local grant must name (enforce)
//
// keys (P3b S4): "off" does nothing. "shadow" resolves the key segment of each
// /r/ /k/ /h/ request against the share-link store and compares the kernel's
// verdict with the Stage's, logging "stage-ui: key-disagree" and counting
// agreements/disagreements (GET /v1/stage/keys/shadow, scope stage:keys). It
// never changes a response; the Python Stage stays authoritative. "enforce"
// (S5) additionally answers unknown and revoked keys itself, without reaching
// the Stage; see serve_stage_ui_keys.go.
// An unrecognised value runs as off. See serve_stage_ui_keys.go.
//
// local_auth: "off" (default) trusts the guard's /local rules, exactly as
// before this config existed. "enforce" additionally requires every /local
// request — the raw spelling, the cleaned spellings the guard normalises, and
// websocket upgrades — to present a live grant with scope stage:local naming
// actor "chaz" (X-Cogos-Grant or Authorization: Bearer); otherwise the kernel
// answers 401 itself and nothing is proxied. An unrecognised value runs as
// off. See serve_stage_ui_guard.go (localGrantOK).
//
// Request routing, in order:
//
//  0. The Stage guard's request-level checks (serve_stage_ui_guard.go): lock,
//     Host allowlist, websocket Origin, /local rules, key-guess lockout.
//     A refused request never reaches static serving or the upstream.
//  1. GET /v1/stage-ui/stats is answered by the listener itself (see below).
//     It is never proxied and counts nothing.
//  2. GET/HEAD whose path starts with one of static_prefixes AND resolves to a
//     regular file under static_dir (full request path, os.Root-confined) is
//     served from disk. The files live on disk, not in the binary, so a UI edit
//     needs no kernel release.
//  3. A cleaned path that matches an owned prefix (stage_ui.owned_prefixes) is
//     refused with 502 Bad Gateway instead of being proxied: a route group the
//     kernel claims to own must never quietly fall through to the Python Stage,
//     because that is what hides a migration gap. The match is on the cleaned
//     path and on a path boundary: /v1/rooms owns /v1/rooms and /v1/rooms/a,
//     never /v1/roomsx. An entry that is empty or lacks the leading slash is a
//     config error, so the listener does not start with it.
//  4. Everything else, including a static-prefix path with no such file, is
//     reverse-proxied to the upstream. Falling through on a miss means the
//     static layer can only ever replace a response with a byte-for-byte file,
//     never turn a Stage 404/200 into a different status.
//
// Counters (P4-0b). The listener counts what it proxies by route group and what
// it refused as owned fallthrough by route group, and serves the totals as JSON
// at GET /v1/stage-ui/stats:
//
//	{"upstream": {"/v1/rooms": 4}, "owned_fallthrough": {"/v1/mods": 2}}
//
// The group key is the first two segments of the cleaned path (/v1/rooms), the
// single segment when there is only one (/health), and / for the root. Both
// maps are empty until the listener sees traffic. A non-zero
// owned_fallthrough is the loud signal the migration wants: traffic is asking
// for a route the kernel owns but does not serve.
//
// Why only unkeyed paths are static by default: every Stage page shell
// (/r/{key}/dashboard/*, /h/{hkey}/*) is gated by a room or home key that the
// Python side validates, and the guard counts unknown-key misses. Serving
// those from the kernel before key validation moves into Go would change
// behavior, so they stay proxied. Only /app-icons/ (unkeyed, allowlisted
// icons; stage-protocol.md "Not covered by keys") is static by default.
//
// What the proxy forwards, and why (apps/stage/guard.py must behave exactly as
// if the request had hit the Stage port directly):
//   - Method, path (escaped form preserved), query, body, and all request
//     headers, including Cookie, Origin, Sec-Fetch-*, Content-Type, and
//     cf-connecting-ip. The guard reads Origin (cross-site socket hijack and
//     /local cross-site checks), Sec-Fetch-Site, Content-Type (/local JSON
//     only), and cf-connecting-ip (tunnel caller vs. the Stage host itself, and the
//     per-IP lockout key).
//   - Host is forwarded verbatim from the client (not rewritten to the
//     upstream's host). The guard's allowlist (loopback or the tunnel's own
//     hostname) and its /local loopback-only rule are decided on Host.
//   - WebSocket upgrades are tunneled (Upgrade/Connection and the
//     Sec-WebSocket-* headers survive), so /r/{key}/ws/pad works unchanged.
//   - Responses are streamed with immediate flushing (SSE/long polls).
//
// What it does NOT forward: the proxy adds no X-Forwarded-* / Forwarded
// headers and strips any the client sent (httputil.ReverseProxy Rewrite
// semantics). The guard does not read them; passing client-supplied ones
// through would only let a caller forge provenance.
//
// The one thing the Stage cannot see is the TCP peer: it is always the kernel
// (loopback). guard.is_self() treats a loopback peer with no cf-connecting-ip
// as "the Stage host itself" (never locked out, /local allowed). That is only safe if
// nothing but the Stage host can reach this listener, so the listener REFUSES a
// non-loopback bind address. Tunnel traffic still carries cf-connecting-ip, so
// it is classified exactly as before.
package engine

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Defaults for the Stage UI listener. The address default is empty on purpose:
// the listener is opt-in.
const (
	DefaultStageUIUpstream = "http://127.0.0.1:7872"
)

// DefaultStageUIStaticPrefixes are the URL prefixes served from static_dir
// when none are configured. See the file header for why only this one.
var DefaultStageUIStaticPrefixes = []string{"/app-icons/"}

// StageUIConfig is the resolved config of the Stage UI listener.
type StageUIConfig struct {
	Addr           string   // listen address; empty disables the listener
	Upstream       string   // Python stage base URL
	StaticDir      string   // directory of static front-end files (may be empty)
	StaticPrefixes []string // URL path prefixes eligible for static serving
	OwnedPrefixes  []string // route groups the kernel owns: never proxied (502 instead)
	PublicHost     string   // public hostname (e.g. stage.myrgic.com) the guard accepts besides loopback; empty = loopback only
	Keys           string   // key resolution mode: off (default) | shadow | enforce (S5)
	LocalAuth      string   // /local grant mode: off (default) | enforce
	LocalActor     string   // the actor a /local grant must name under enforce (empty refuses all)

	// shareLinks supplies the share-link store for Keys=shadow. Set by
	// startStageUI; nil in handler-only tests that do not use shadow mode.
	shareLinks func() *ShareLinkStore

	// grants supplies the identity-grant registry for LocalAuth=enforce. Set
	// by startStageUI; nil means no grant can verify (the guard fails closed).
	grants func() *IdentityGrantRegistry
}

// stageUISection is the `stage_ui:` block of kernel.yaml.
type stageUISection struct {
	Addr           string   `yaml:"addr"`
	Upstream       string   `yaml:"upstream"`
	StaticDir      string   `yaml:"static_dir"`
	StaticPrefixes []string `yaml:"static_prefixes"`
	OwnedPrefixes  []string `yaml:"owned_prefixes"`
	PublicHost     string   `yaml:"public_host"`
	Keys           string   `yaml:"keys"`
	LocalAuth      string   `yaml:"local_auth"`
	LocalActor     string   `yaml:"local_actor"`
}

// applyStageUISection overlays a kernel.yaml stage_ui block onto cfg.
func applyStageUISection(cfg *Config, s *stageUISection) {
	if s == nil {
		return
	}
	if s.Addr != "" {
		cfg.StageUI.Addr = s.Addr
	}
	if s.Upstream != "" {
		cfg.StageUI.Upstream = s.Upstream
	}
	if s.StaticDir != "" {
		cfg.StageUI.StaticDir = s.StaticDir
	}
	if s.PublicHost != "" {
		cfg.StageUI.PublicHost = s.PublicHost
	}
	if s.Keys != "" {
		cfg.StageUI.Keys = s.Keys
	}
	if s.LocalAuth != "" {
		cfg.StageUI.LocalAuth = s.LocalAuth
	}
	if s.LocalActor != "" {
		cfg.StageUI.LocalActor = s.LocalActor
	}
	if len(s.StaticPrefixes) > 0 {
		cfg.StageUI.StaticPrefixes = s.StaticPrefixes
	}
	if len(s.OwnedPrefixes) > 0 {
		cfg.StageUI.OwnedPrefixes = s.OwnedPrefixes
	}
}

// validateStageUIOwnedPrefixes rejects owned_prefixes entries that cannot be a
// path prefix on a cleaned request path: an empty entry, or one without a
// leading slash. A bad entry disables the listener rather than silently
// dropping it, because a dropped entry would let an owned group proxy again,
// which is exactly the failure this config exists to prevent.
func validateStageUIOwnedPrefixes(prefixes []string) error {
	for i, p := range prefixes {
		if p == "" {
			return fmt.Errorf("stage_ui.owned_prefixes[%d]: empty", i)
		}
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("stage_ui.owned_prefixes[%d] %q: must start with /", i, p)
		}
	}
	return nil
}

// ownedPrefixMatch reports whether the cleaned path p belongs to an owned
// prefix. The match is on a path boundary: "/v1/rooms" owns "/v1/rooms" and
// "/v1/rooms/a", but not "/v1/roomsx", which is a different route group. A
// trailing slash on the entry is not significant ("/v1/rooms/" is the same
// entry); "/" owns everything.
func ownedPrefixMatch(p, prefix string) bool {
	pre := strings.TrimRight(prefix, "/")
	if pre == "" {
		return true
	}
	return p == pre || strings.HasPrefix(p, pre+"/")
}

// validateStageUIAddr requires a loopback host:port (see the file header).
func validateStageUIAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("stage_ui.addr %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("stage_ui.addr %q: host must be loopback (the Stage guard treats the kernel as the Stage host itself)", addr)
	}
	return nil
}

// Stage UI /local grant modes (stage_ui.local_auth).
const (
	// StageUILocalAuthOff trusts the loopback rule alone: any request the
	// guard already accepts may reach /local. This is the default and exactly
	// the pre-local_auth behaviour.
	StageUILocalAuthOff = "off"
	// StageUILocalAuthEnforce additionally requires every /local request to
	// present a live grant with scope stage:local naming stage_ui.local_actor.
	StageUILocalAuthEnforce = "enforce"
)

// normalizeStageUILocalAuthMode maps the config value to the effective mode.
// ok=false means the value was unrecognised (effective mode is off), matching
// normalizeStageUIKeysMode.
func normalizeStageUILocalAuthMode(v string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", StageUILocalAuthOff:
		return StageUILocalAuthOff, true
	case StageUILocalAuthEnforce:
		return StageUILocalAuthEnforce, true
	}
	return StageUILocalAuthOff, false
}

// newStageUIHandler builds the static-then-proxy handler.
func newStageUIHandler(c StageUIConfig) (http.Handler, error) {
	h, _, err := newStageUIHandlerG(c)
	return h, err
}

// newStageUIHandlerG is newStageUIHandler that also returns the guard (tests).
func newStageUIHandlerG(c StageUIConfig) (http.Handler, *stageGuard, error) {
	upstream := c.Upstream
	if upstream == "" {
		upstream = DefaultStageUIUpstream
	}
	target, err := url.Parse(upstream)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, nil, fmt.Errorf("stage_ui.upstream %q: want scheme://host:port", upstream)
	}
	if err := validateStageUIOwnedPrefixes(c.OwnedPrefixes); err != nil {
		return nil, nil, err
	}
	guard := newStageGuard(c.PublicHost)
	guard.keyKnown = probeKeyKnown(target)
	mode, ok := normalizeStageUIKeysMode(c.Keys)
	if !ok {
		slog.Warn("stage-ui: unknown keys mode, running as off", "keys", c.Keys)
	}
	if (mode == StageUIKeysShadow || mode == StageUIKeysEnforce) && c.shareLinks != nil {
		guard.keysMode = mode
		guard.shadow = newKeyShadow(c.shareLinks)
	} else if mode != StageUIKeysOff {
		slog.Warn("stage-ui: keys mode without a share-link store, running as off", "keys", mode)
	}
	localMode, localOK := normalizeStageUILocalAuthMode(c.LocalAuth)
	if !localOK {
		slog.Warn("stage-ui: unknown local_auth mode, running as off", "local_auth", c.LocalAuth)
	}
	// Unlike keys=shadow (which only observes and so can degrade to off), an
	// authorization gate must never silently open: enforce stays on even with
	// no registry, and localGrantOK then refuses every /local request.
	if localMode == StageUILocalAuthEnforce {
		guard.localAuthEnforce = true
		guard.grants = c.grants
		guard.localActor = strings.TrimSpace(c.LocalActor)
		if guard.localActor == "" {
			slog.Warn("stage-ui: local_auth=enforce without local_actor; every /local request will be refused")
		}
		if c.grants == nil {
			slog.Warn("stage-ui: local_auth=enforce without a grant registry; every /local request will be refused")
		}
	}
	prefixes := c.StaticPrefixes
	if len(prefixes) == 0 {
		prefixes = DefaultStageUIStaticPrefixes
	}
	owned := c.OwnedPrefixes
	tally := newStageUITally()

	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			// SetURL rewrites Host to the upstream's; the guard decides on the
			// client's Host, so put it back.
			r.Out.Host = r.In.Host
		},
		ModifyResponse: guard.afterResponse,
		FlushInterval:  -1, // stream (SSE, long polls) without buffering
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("stage-ui: upstream error", "upstream", upstream, "err", err)
			http.Error(w, "stage upstream unavailable", http.StatusBadGateway)
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ok bool
		if r, ok = guard.handle(w, r); !ok {
			return
		}
		if guard.keysMode == StageUIKeysEnforce && guard.shadow != nil {
			if info, _ := r.Context().Value(stageReqKey{}).(*stageReqInfo); info != nil {
				if refuse, count := guard.shadow.keyRefusal(info.path); refuse {
					if count && !info.self {
						guard.noteMiss(info.ip, info.path)
					}
					writeStageKeyRefusal(w, info.ws)
					return
				}
			}
		}
		clean := path.Clean(r.URL.Path)
		if clean == stageUIStatsPath {
			serveStageUIStats(w, r, tally)
			return
		}
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
			c.StaticDir != "" && hasAnyPrefix(clean, prefixes) &&
			serveStageUIStatic(w, r, c.StaticDir) {
			return
		}
		if hasAnyOwnedPrefix(clean, owned) {
			tally.noteOwned(clean)
			// Path and method only: no query, headers, cookies or body.
			slog.Warn("stage-ui: owned-fallthrough", "path", stageGuardLogPath(clean), "method", r.Method)
			http.Error(w, "stage-ui: route is owned by the kernel but not served here", http.StatusBadGateway)
			return
		}
		tally.noteUpstream(clean)
		proxy.ServeHTTP(w, r)
	}), guard, nil
}

// hasAnyOwnedPrefix reports whether the cleaned path is the owned prefix itself
// or a path under it. See ownedPrefixMatch for the boundary rule.
func hasAnyOwnedPrefix(clean string, owned []string) bool {
	for _, pre := range owned {
		if ownedPrefixMatch(clean, pre) {
			return true
		}
	}
	return false
}

// stageUIStatsPath is served by the listener itself, never proxied.
const stageUIStatsPath = "/v1/stage-ui/stats"

// stageUIRouteStats is the JSON body of GET /v1/stage-ui/stats.
type stageUIRouteStats struct {
	Upstream         map[string]int64 `json:"upstream"`
	OwnedFallthrough map[string]int64 `json:"owned_fallthrough"`
}

// stageUITally counts the listener's traffic by route group: what was proxied
// and what was refused as owned fallthrough. The handler's goroutines share
// one, so every access holds the mutex.
type stageUITally struct {
	mu               sync.Mutex
	upstream         map[string]int64
	ownedFallthrough map[string]int64
}

func newStageUITally() *stageUITally {
	return &stageUITally{
		upstream:         map[string]int64{},
		ownedFallthrough: map[string]int64{},
	}
}

func (t *stageUITally) noteUpstream(clean string) { t.note(t.upstream, clean) }
func (t *stageUITally) noteOwned(clean string)    { t.note(t.ownedFallthrough, clean) }

func (t *stageUITally) note(m map[string]int64, clean string) {
	k := stageUIRouteGroup(clean)
	t.mu.Lock()
	defer t.mu.Unlock()
	m[k]++
}

// snapshot copies the counters for the stats endpoint.
func (t *stageUITally) snapshot() stageUIRouteStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := stageUIRouteStats{
		Upstream:         make(map[string]int64, len(t.upstream)),
		OwnedFallthrough: make(map[string]int64, len(t.ownedFallthrough)),
	}
	for k, v := range t.upstream {
		s.Upstream[k] = v
	}
	for k, v := range t.ownedFallthrough {
		s.OwnedFallthrough[k] = v
	}
	return s
}

// stageUIRouteGroup is the counter key of a cleaned path: its first two
// segments (/v1/rooms), its single segment (/health), or / for the root.
func stageUIRouteGroup(clean string) string {
	rest := strings.Trim(strings.TrimPrefix(clean, "/"), "/")
	if rest == "" {
		return "/"
	}
	segs := strings.SplitN(rest, "/", 3)
	if len(segs) < 2 {
		return "/" + segs[0]
	}
	return "/" + segs[0] + "/" + segs[1]
}

// serveStageUIStats answers GET /v1/stage-ui/stats from the counters. It runs
// behind the guard like every other request, and touches no counter itself.
func serveStageUIStats(w http.ResponseWriter, r *http.Request, t *stageUITally) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "stage-ui: stats is GET only", http.StatusMethodNotAllowed)
		return
	}
	writeJSONResp(w, http.StatusOK, t.snapshot())
}

// hasAnyPrefix reports whether the cleaned path p is under a prefix. The caller
// cleans first: checking the raw path would let /app-icons/../x pass the prefix
// test and then clean to /x.
func hasAnyPrefix(p string, prefixes []string) bool {
	for _, pre := range prefixes {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}
	return false
}

// serveStageUIStatic serves the file at dir + request path. It returns false,
// having written nothing, when there is no such regular file, so the caller
// can fall through to the proxy.
func serveStageUIStatic(w http.ResponseWriter, r *http.Request, dir string) bool {
	clean := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("stage-ui: static_dir unreadable", "dir", dir, "err", err)
		}
		return false
	}
	defer root.Close()
	st, err := root.Stat(clean)
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	f, err := root.Open(clean)
	if err != nil {
		return false
	}
	defer f.Close()
	if ct := mime.TypeByExtension(strings.ToLower(filepath.Ext(clean))); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "no-cache") // UI is edited in place
	http.ServeContent(w, r, path.Base(clean), st.ModTime(), f)
	return true
}

// startStageUI binds and serves the listener if configured. A bad config or a
// bind failure is logged, never fatal: the kernel must come up either way.
func (s *Server) startStageUI() {
	c := s.cfg.StageUI
	if c.Addr == "" {
		return
	}
	if err := validateStageUIAddr(c.Addr); err != nil {
		slog.Error("stage-ui: disabled", "err", err)
		return
	}
	c.shareLinks = func() *ShareLinkStore { return s.shareLinks }
	c.grants = func() *IdentityGrantRegistry { return s.identityGrants }
	h, g, err := newStageUIHandlerG(c)
	if err != nil {
		slog.Error("stage-ui: disabled", "err", err)
		return
	}
	ln, err := net.Listen("tcp", c.Addr)
	if err != nil {
		slog.Error("stage-ui: listen failed", "addr", c.Addr, "err", err)
		return
	}
	// No Read/WriteTimeout: WebSocket and SSE connections are long-lived.
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 30 * time.Second}
	s.stageUIMu.Lock()
	s.stageUISrv = srv
	s.stageUIGuard = g
	s.stageUIAddr = ln.Addr().String()
	s.stageUIMu.Unlock()
	slog.Info("stage-ui: listening", "addr", ln.Addr().String(), "upstream", c.Upstream,
		"static_dir", c.StaticDir, "owned_prefixes", c.OwnedPrefixes)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("stage-ui: serve ended", "err", err)
		}
	}()
}

// stopStageUI closes the listener (idempotent). Close, not graceful Shutdown:
// hijacked WebSockets are not tracked by Shutdown and would block it.
func (s *Server) stopStageUI() {
	s.stageUIMu.Lock()
	srv := s.stageUISrv
	s.stageUISrv = nil
	s.stageUIGuard = nil
	s.stageUIMu.Unlock()
	if srv != nil {
		_ = srv.Close()
	}
}
