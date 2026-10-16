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
//	  public_host: stage.myrgic.com  # extra Host the guard accepts (empty = loopback only)
//
// Request routing, in order:
//
//  0. The Stage guard's request-level checks (serve_stage_ui_guard.go): lock,
//     Host allowlist, websocket Origin, /local rules, key-guess lockout.
//     A refused request never reaches static serving or the upstream.
//  1. GET/HEAD whose path starts with one of static_prefixes AND resolves to a
//     regular file under static_dir (full request path, os.Root-confined) is
//     served from disk. The files live on disk, not in the binary, so a UI edit
//     needs no kernel release.
//  2. Everything else, including a static-prefix path with no such file, is
//     reverse-proxied to the upstream. Falling through on a miss means the
//     static layer can only ever replace a response with a byte-for-byte file,
//     never turn a Stage 404/200 into a different status.
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
	PublicHost     string   // public hostname (e.g. stage.myrgic.com) the guard accepts besides loopback; empty = loopback only
}

// stageUISection is the `stage_ui:` block of kernel.yaml.
type stageUISection struct {
	Addr           string   `yaml:"addr"`
	Upstream       string   `yaml:"upstream"`
	StaticDir      string   `yaml:"static_dir"`
	StaticPrefixes []string `yaml:"static_prefixes"`
	PublicHost     string   `yaml:"public_host"`
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
	if len(s.StaticPrefixes) > 0 {
		cfg.StageUI.StaticPrefixes = s.StaticPrefixes
	}
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
	guard := newStageGuard(c.PublicHost)
	guard.keyKnown = probeKeyKnown(target)
	prefixes := c.StaticPrefixes
	if len(prefixes) == 0 {
		prefixes = DefaultStageUIStaticPrefixes
	}

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
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
			c.StaticDir != "" && hasAnyPrefix(path.Clean(r.URL.Path), prefixes) &&
			serveStageUIStatic(w, r, c.StaticDir) {
			return
		}
		proxy.ServeHTTP(w, r)
	}), guard, nil
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
	h, err := newStageUIHandler(c)
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
	s.stageUIAddr = ln.Addr().String()
	s.stageUIMu.Unlock()
	slog.Info("stage-ui: listening", "addr", ln.Addr().String(), "upstream", c.Upstream, "static_dir", c.StaticDir)
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
	s.stageUIMu.Unlock()
	if srv != nil {
		_ = srv.Close()
	}
}
