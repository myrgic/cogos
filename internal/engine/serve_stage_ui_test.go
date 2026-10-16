package engine

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stageUIFixture starts a fake "python stage" upstream and a kernel handler
// in front of it with a static dir containing app-icons/orb.png.
func stageUIFixture(t *testing.T, upstream http.Handler) (front *httptest.Server, staticDir string) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	staticDir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(staticDir, "app-icons"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staticDir, "app-icons", "orb.png"), []byte("PNGDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staticDir, "secret.txt"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := newStageUIHandler(StageUIConfig{Upstream: up.URL, StaticDir: staticDir, PublicHost: "stage.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	front = httptest.NewServer(h)
	t.Cleanup(front.Close)
	return front, staticDir
}

func stageGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestStageUI_StaticServedFromDisk(t *testing.T) {
	front, dir := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("static file must not reach upstream: %s", r.URL.Path)
	}))
	code, body := stageGet(t, front.URL+"/app-icons/orb.png")
	if code != 200 || body != "PNGDATA" {
		t.Fatalf("got %d %q", code, body)
	}
	// Edits on disk show up with no restart (served from disk, not embedded).
	if err := os.WriteFile(filepath.Join(dir, "app-icons", "orb.png"), []byte("NEW"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, body = stageGet(t, front.URL+"/app-icons/orb.png"); body != "NEW" {
		t.Fatalf("edit not visible: %q", body)
	}
}

func TestStageUI_StaticOnlyForConfiguredPrefixesAndFallsThroughOnMiss(t *testing.T) {
	var seen []string
	front, _ := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		w.WriteHeader(404)
		_, _ = w.Write([]byte("stage-404"))
	}))
	// Exists on disk but outside any static prefix: proxied, not served.
	if code, body := stageGet(t, front.URL+"/secret.txt"); code != 404 || body != "stage-404" {
		t.Fatalf("secret.txt: %d %q", code, body)
	}
	// Under a static prefix but absent on disk: upstream decides.
	if code, body := stageGet(t, front.URL+"/app-icons/missing.png"); code != 404 || body != "stage-404" {
		t.Fatalf("missing: %d %q", code, body)
	}
	// Directory under a static prefix: upstream decides.
	if code, _ := stageGet(t, front.URL+"/app-icons/"); code != 404 {
		t.Fatalf("dir: %d", code)
	}
	// Traversal cannot escape static_dir.
	if code, body := stageGet(t, front.URL+"/app-icons/..%2fsecret.txt"); body == "nope" {
		t.Fatalf("traversal served file: %d", code)
	}
	if len(seen) != 4 {
		t.Fatalf("upstream saw %v", seen)
	}
}

func TestStageUI_StaticIsGetHeadOnly(t *testing.T) {
	var method string
	front, _ := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(204)
	}))
	resp, err := http.Post(front.URL+"/app-icons/orb.png", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if method != "POST" || resp.StatusCode != 204 {
		t.Fatalf("POST should proxy: %q %d", method, resp.StatusCode)
	}
}

func TestStageUI_ProxyPassthrough(t *testing.T) {
	type seen struct{ method, uri, body string }
	var got seen
	front, _ := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = seen{r.Method, r.RequestURI, string(b)}
		w.Header().Set("X-Stage", "yes")
		http.SetCookie(w, &http.Cookie{Name: "sid", Value: "abc"})
		w.WriteHeader(201)
		_, _ = w.Write([]byte("made"))
	}))
	req, _ := http.NewRequest("POST", front.URL+"/local/rooms/r1/post?a=1&b=x%20y", strings.NewReader(`{"t":"hi"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 || string(b) != "made" || resp.Header.Get("X-Stage") != "yes" {
		t.Fatalf("response: %d %q %v", resp.StatusCode, b, resp.Header)
	}
	if len(resp.Cookies()) != 1 || resp.Cookies()[0].Name != "sid" {
		t.Fatalf("Set-Cookie lost: %v", resp.Header)
	}
	want := seen{"POST", "/local/rooms/r1/post?a=1&b=x%20y", `{"t":"hi"}`}
	if got != want {
		t.Fatalf("upstream saw %+v want %+v", got, want)
	}
}

func TestStageUI_PreservesGuardHeaders(t *testing.T) {
	var host string
	var hdr http.Header
	front, _ := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, hdr = r.Host, r.Header.Clone()
	}))
	req, _ := http.NewRequest("GET", front.URL+"/r/key/dashboard/pad.js", nil)
	req.Host = "stage.example.com" // the tunnel hostname, not the upstream's
	req.Header.Set("Origin", "https://stage.example.com")
	req.Header.Set("Cookie", "a=1; b=2")
	req.Header.Set("Cf-Connecting-Ip", "203.0.113.9")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("X-Forwarded-For", "6.6.6.6") // forgeable; must not pass
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if host != "stage.example.com" {
		t.Errorf("Host rewritten: %q", host)
	}
	for k, v := range map[string]string{
		"Origin": "https://stage.example.com", "Cookie": "a=1; b=2",
		"Cf-Connecting-Ip": "203.0.113.9", "Sec-Fetch-Site": "same-origin",
	} {
		if hdr.Get(k) != v {
			t.Errorf("%s = %q want %q", k, hdr.Get(k), v)
		}
	}
	if hdr.Get("X-Forwarded-For") != "" || hdr.Get("Forwarded") != "" {
		t.Errorf("forwarded headers leaked: %v", hdr)
	}
}

func TestStageUI_NoCfHeaderStaysAbsent(t *testing.T) {
	// guard.is_self() depends on cf-connecting-ip being ABSENT for local callers.
	var hdr http.Header
	front, _ := stageUIFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hdr = r.Header.Clone() }))
	stageGet(t, front.URL+"/local/rooms")
	if _, ok := hdr["Cf-Connecting-Ip"]; ok {
		t.Fatalf("cf-connecting-ip invented: %v", hdr)
	}
}

func TestStageUI_UpstreamDownIs502(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	h, err := newStageUIHandler(StageUIConfig{Upstream: url})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/x", nil)
	req.Host, req.RemoteAddr = "localhost", "127.0.0.1:5000"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestStageUI_WebSocketPassthrough(t *testing.T) {
	var upHost, upOrigin string
	up := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "no upgrade", 400)
			return
		}
		upHost, upOrigin = r.Host, r.Header.Get("Origin")
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_ = rw.Flush()
		// Echo raw bytes: the proxy must tunnel the connection, not parse frames.
		buf := make([]byte, 64)
		n, _ := rw.Read(buf)
		_, _ = conn.Write(append([]byte("echo:"), buf[:n]...))
	})
	front, _ := stageUIFixture(t, up)

	conn, err := net.Dial("tcp", strings.TrimPrefix(front.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write([]byte("GET /r/key/ws/pad HTTP/1.1\r\nHost: localhost\r\nOrigin: http://localhost\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"))
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("status %q err %v", status, err)
	}
	for { // drain headers
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	_, _ = conn.Write([]byte("ping"))
	buf := make([]byte, 64)
	n, err := br.Read(buf)
	if err != nil || string(buf[:n]) != "echo:ping" {
		t.Fatalf("echo %q err %v", buf[:n], err)
	}
	if upHost != "localhost" || upOrigin != "http://localhost" {
		t.Errorf("ws host/origin: %q %q", upHost, upOrigin)
	}
}

func TestStageUI_AddrMustBeLoopback(t *testing.T) {
	for addr, ok := range map[string]bool{
		"127.0.0.1:7873": true, "[::1]:7873": true, "localhost:7873": true,
		"0.0.0.0:7873": false, "192.168.1.5:7873": false, ":7873": false, "nonsense": false,
	} {
		if err := validateStageUIAddr(addr); (err == nil) != ok {
			t.Errorf("%s: err=%v want ok=%v", addr, err, ok)
		}
	}
}

func TestStageUI_ConfigDefaultOffAndYAML(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, ".cog", "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StageUI.Addr != "" {
		t.Fatalf("listener must default off, got %q", cfg.StageUI.Addr)
	}
	y := "stage_ui:\n  addr: 127.0.0.1:7873\n  upstream: http://127.0.0.1:7872\n  static_dir: /x\n  static_prefixes: [/app-icons/]\n"
	if err := os.WriteFile(filepath.Join(ws, ".cog", "config", "kernel.yaml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(ws, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StageUI.Addr != "127.0.0.1:7873" || cfg.StageUI.StaticDir != "/x" || cfg.StageUI.Upstream != "http://127.0.0.1:7872" {
		t.Fatalf("yaml not applied: %+v", cfg.StageUI)
	}
}

func TestStageUI_ServerListenerLifecycle(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("py:" + r.URL.Path)) }))
	defer up.Close()

	off := &Server{cfg: &Config{}}
	off.startStageUI()
	if off.stageUISrv != nil {
		t.Fatal("listener started with empty addr")
	}

	bad := &Server{cfg: &Config{StageUI: StageUIConfig{Addr: "0.0.0.0:0", Upstream: up.URL}}}
	bad.startStageUI()
	if bad.stageUISrv != nil {
		t.Fatal("non-loopback addr must be refused")
	}

	s := &Server{cfg: &Config{StageUI: StageUIConfig{Addr: "127.0.0.1:0", Upstream: up.URL}}}
	s.startStageUI()
	if s.stageUISrv == nil {
		t.Fatal("listener not started")
	}
	if code, body := stageGet(t, "http://"+s.stageUIAddr+"/hello"); code != 200 || body != "py:/hello" {
		t.Fatalf("%d %q", code, body)
	}
	s.stopStageUI()
	if _, err := http.Get("http://" + s.stageUIAddr + "/hello"); err == nil {
		t.Fatal("listener still up after stop")
	}
}
