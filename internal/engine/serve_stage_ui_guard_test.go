package engine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Row ids below (G05, R01, ...) cite apps/stage/docs/specs/front-door-test-vectors.md.
// Keys, IPs and hostnames are synthetic.

const (
	guardTH   = "stage.example.com"
	guardPeer = "127.0.0.1:40" // the TCP peer is always the loopback listener side
)

// guardRig is a kernel Stage UI handler in front of a recording upstream.
type guardRig struct {
	h       http.Handler
	g       *stageGuard
	reached int
	clock   time.Time
}

func newGuardRig(t *testing.T, publicHost string) *guardRig {
	t.Helper()
	rig := &guardRig{clock: time.Unix(1_700_000_000, 0)}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rig.reached++
		switch {
		case strings.HasPrefix(r.URL.Path, "/r/REALKEY00000000000000/dashboard/launcher.js"):
			w.WriteHeader(404) // missing file under a real key
		case strings.HasPrefix(r.URL.Path, "/r/REALKEY00000000000000/ws/denied"):
			w.WriteHeader(403) // Stage closed a socket on a known key
		case strings.HasPrefix(r.URL.Path, "/r/REALKEY00000000000000/"):
			w.WriteHeader(200)
		case strings.HasPrefix(r.URL.Path, "/local/"), r.URL.Path == "/healthz":
			w.WriteHeader(200)
		case strings.Contains(r.URL.Path, "/ws/"):
			w.WriteHeader(403) // unknown key: closed before accept
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(up.Close)
	h, g, err := newStageUIHandlerG(StageUIConfig{Upstream: up.URL, PublicHost: publicHost})
	if err != nil {
		t.Fatal(err)
	}
	g.now = func() time.Time { return rig.clock }
	g.keyKnown = func(_ context.Context, seg string) bool { return seg == "REALKEY00000000000000" }
	rig.h, rig.g = h, g
	return rig
}

type gcase struct {
	id      string
	peer    string // "self" (loopback, no cf header), "tun:<ip>" (loopback + cf-connecting-ip), or a raw ip
	method  string
	target  string
	host    string
	hdr     [][2]string
	ctype   string
	bodyLen int  // -1 = chunked
	ws      bool // add Upgrade: websocket
	want    int  // 0 = reaches the upstream
}

func (rig *guardRig) do(c gcase) *httptest.ResponseRecorder {
	method := c.method
	if method == "" {
		method = "GET"
	}
	body := strings.NewReader("")
	if c.bodyLen > 0 {
		body = strings.NewReader(strings.Repeat("x", c.bodyLen))
	}
	req := httptest.NewRequest(method, c.target, body)
	switch {
	case c.peer == "self" || c.peer == "":
		req.RemoteAddr = guardPeer
	case strings.HasPrefix(c.peer, "tun:"):
		req.RemoteAddr = guardPeer
		req.Header.Set("Cf-Connecting-Ip", strings.TrimPrefix(c.peer, "tun:"))
	default:
		req.RemoteAddr = c.peer + ":40"
	}
	req.Host = c.host
	for _, kv := range c.hdr {
		req.Header.Add(kv[0], kv[1])
	}
	if c.ctype != "" {
		req.Header.Set("Content-Type", c.ctype)
	}
	if c.bodyLen < 0 {
		req.ContentLength = -1
	} else {
		req.ContentLength = int64(c.bodyLen)
	}
	if c.ws {
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
	}
	rec := httptest.NewRecorder()
	rig.h.ServeHTTP(rec, req)
	return rec
}

func (rig *guardRig) run(t *testing.T, cases []gcase) {
	t.Helper()
	for _, c := range cases {
		c := c
		t.Run(c.id, func(t *testing.T) {
			before := rig.reached
			rec := rig.do(c)
			if c.want == 0 {
				if rig.reached != before+1 {
					t.Fatalf("%s: wanted pass-through, got %d %q", c.id, rec.Code, rec.Body.String())
				}
				return
			}
			if rec.Code != c.want {
				t.Fatalf("%s: got %d %q want %d", c.id, rec.Code, rec.Body.String(), c.want)
			}
			if rig.reached != before {
				t.Fatalf("%s: refused request still reached the upstream", c.id)
			}
		})
	}
}

func TestStageGuard_HostAllowlist(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	rig.run(t, []gcase{
		{id: "G01", peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/dashboard/orb.html", host: guardTH},
		{id: "G02", peer: "self", target: "/local/rooms", host: "evil.example", want: 421},
		{id: "G03", peer: "self", target: "/local/home", host: "evil.example", want: 421},
		{id: "G04", peer: "tun:203.0.113.9", target: "/r/abc/dashboard/orb.html", host: "evil.example:7872", want: 421},
		{id: "G05", peer: "tun:203.0.113.9", target: "/r/abc/dashboard/orb.html", host: "", want: 421},
		{id: "G06", peer: "self", target: "/local/rooms", host: "LOCALHOST:7872"},
		{id: "G07", peer: "self", target: "/local/rooms", host: "[::1]:7872"},
		{id: "G08-trailing-dot", peer: "self", target: "/local/rooms", host: "localhost.", want: 421},
		{id: "G08-suffix", peer: "self", target: "/local/rooms", host: "127.0.0.1.evil.example", want: 421},
		{id: "G08-127.0.0.2", peer: "self", target: "/local/rooms", host: "127.0.0.2", want: 421},
		{id: "G08-0.0.0.0", peer: "self", target: "/local/rooms", host: "0.0.0.0", want: 421},
		{id: "G09", peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/x", host: guardTH + ":443"},
		// Deliberate tightening vs guard.py (which passes this); see the guard file header.
		{id: "G12", peer: "self", target: "/local/rooms", host: "localhost:80@evil.example", want: 421},
		{id: "G21", peer: "self", target: "/local/rooms", host: "127.0.0.1:7872"},
		{id: "G22", peer: "self", target: "/local/rooms", host: "localhost:7872"},
	})
}

func TestStageGuard_G10_NoPublicHost(t *testing.T) {
	rig := newGuardRig(t, "") // no tunnel recorded: only loopback hosts pass
	rig.run(t, []gcase{
		{id: "G10", peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/x", host: guardTH, want: 421},
		{id: "G10-loopback-still-ok", peer: "self", target: "/local/rooms", host: "127.0.0.1"},
	})
}

func TestStageGuard_PublicHostConfigForms(t *testing.T) {
	for _, in := range []string{"stage.example.com", "STAGE.example.com", "https://stage.example.com/", "stage.example.com:443"} {
		if g := newStageGuard(in); !g.allowedHost(guardTH) {
			t.Errorf("public_host %q: tunnel host not allowed (publicHost=%q)", in, g.publicHost)
		}
	}
}

func TestStageGuard_G13_DuplicateHeaders(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	// Go keeps the first value (Header.Get); the spec recommends first-or-reject.
	rig.run(t, []gcase{
		{id: "G13-origin-first-bad", peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/ws/pad", host: guardTH, ws: true,
			hdr: [][2]string{{"Origin", "https://evil.example"}, {"Origin", "https://" + guardTH}}, want: 403},
		{id: "G13-origin-first-good", peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/ws/pad", host: guardTH, ws: true,
			hdr: [][2]string{{"Origin", "https://" + guardTH}, {"Origin", "https://evil.example"}}},
	})
}

func TestStageGuard_WebSocketOrigin(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	ws := "/r/REALKEY00000000000000/pad"
	origin := func(o string) [][2]string { return [][2]string{{"Origin", o}} }
	rig.run(t, []gcase{
		{id: "G14", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true, hdr: origin("https://" + guardTH)},
		{id: "G15", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true, hdr: origin("https://evil.example"), want: 403},
		{id: "G16", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true},
		{id: "G17", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true, hdr: origin("null"), want: 403},
		{id: "G18", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true, hdr: origin("http://localhost:3000")},
		{id: "G19", peer: "tun:203.0.113.9", target: ws, host: guardTH, ws: true, hdr: origin("https://" + guardTH + "@evil.example"), want: 403},
		// G20 pins an intentional gap: Origin is only checked for websockets and /local.
		{id: "G20", peer: "tun:203.0.113.9", method: "POST", target: "/r/REALKEY00000000000000/upload", host: guardTH,
			hdr: origin("https://evil.example"), ctype: "text/plain", bodyLen: 3},
	})
}

func TestStageGuard_WebSocketRefusalIsBare403(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	// Python closes 4403 before accept: a bare HTTP 403 on the wire for every
	// refusal reason (origin, host, not-local), never a JSON 421/404.
	for id, c := range map[string]gcase{
		"G15-ws": {peer: "tun:203.0.113.9", target: "/r/K/ws/pad", host: guardTH, ws: true, hdr: [][2]string{{"Origin", "https://evil.example"}}},
		"G04-ws": {peer: "tun:203.0.113.9", target: "/r/K/ws/pad", host: "evil.example", ws: true},
		"G23-ws": {peer: "tun:203.0.113.9", target: "/local/ws", host: guardTH, ws: true},
	} {
		rec := rig.do(c)
		if rec.Code != 403 || rec.Body.Len() != 0 {
			t.Errorf("%s: got %d %q want bare 403", id, rec.Code, rec.Body.String())
		}
	}
}

func TestStageGuard_RefusalShape(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	rec := rig.do(gcase{peer: "self", target: "/local/rooms", host: "evil.example"})
	if rec.Code != 421 || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != `{"error":"unknown host"}` {
		t.Fatalf("G02 shape: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	for id, tc := range map[string]struct {
		c    gcase
		body string
	}{
		"G23": {gcase{peer: "tun:203.0.113.9", target: "/local/rooms", host: guardTH}, `{"error":"not local"}`},
		"G26": {gcase{peer: "self", method: "POST", target: "/local/rooms", host: "localhost", ctype: "text/plain", bodyLen: 20}, `{"error":"json only"}`},
		"G33": {gcase{peer: "self", method: "POST", target: "/local/rooms", host: "localhost", ctype: "application/json", bodyLen: 2,
			hdr: [][2]string{{"Sec-Fetch-Site", "cross-site"}}}, `{"error":"cross-site"}`},
	} {
		if r := rig.do(tc.c); r.Body.String() != tc.body {
			t.Errorf("%s: body %q want %q", id, r.Body.String(), tc.body)
		}
	}
}

func TestStageGuard_LocalRules(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	sfs := func(v string) [][2]string { return [][2]string{{"Sec-Fetch-Site", v}} }
	origin := func(v string) [][2]string { return [][2]string{{"Origin", v}} }
	post := func(id, ctype string, n int, want int, hdr [][2]string) gcase {
		return gcase{id: id, peer: "self", method: "POST", target: "/local/rooms", host: "localhost", ctype: ctype, bodyLen: n, hdr: hdr, want: want}
	}
	rig.run(t, []gcase{
		{id: "G23", peer: "tun:203.0.113.9", target: "/local/rooms", host: guardTH, want: 404},
		{id: "G24", peer: "self", target: "/local/home", host: guardTH, want: 404}, // tunnel Host, no cf header
		{id: "G25", peer: "self", target: "/local/rooms", host: "127.0.0.1:7872", hdr: [][2]string{{"Cf-Connecting-Ip", "1.2.3.4"}}, want: 404},
		post("G26", "text/plain", 20, 415, nil),
		post("G27", "application/json; charset=utf-8", 5, 0, nil),
		post("G27-case", "Application/JSON", 5, 0, nil),
		{id: "G28", peer: "self", method: "POST", target: "/local/dev/reload", host: "127.0.0.1:7872", ctype: "application/json", bodyLen: 2},
		post("G29", "", 0, 0, nil),
		// G30: guard.py passes this (a simple-request bypass); the kernel treats chunked as a body.
		post("G30", "text/plain", -1, 415, nil),
		{id: "G31-PUT", peer: "self", method: "PUT", target: "/local/x", host: "localhost", ctype: "text/plain", bodyLen: 3, want: 415},
		{id: "G31-PATCH", peer: "self", method: "PATCH", target: "/local/x", host: "localhost", ctype: "text/plain", bodyLen: 3, want: 415},
		{id: "G31-DELETE", peer: "self", method: "DELETE", target: "/local/x", host: "localhost", ctype: "text/plain", bodyLen: 3, want: 415},
		{id: "G31-OPTIONS", peer: "self", method: "OPTIONS", target: "/local/x", host: "localhost", ctype: "text/plain", bodyLen: 3, want: 415},
		{id: "G32", peer: "self", method: "GET", target: "/local/x", host: "localhost", ctype: "text/plain", bodyLen: 3},
		post("G33", "application/json", 2, 403, sfs("cross-site")),
		post("G34", "application/json", 2, 403, sfs("same-site")),
		post("G35-same-origin", "application/json", 2, 0, sfs("same-origin")),
		post("G35-none", "application/json", 2, 0, sfs("none")),
		post("G35-absent", "application/json", 2, 0, nil),
		post("G36", "application/json", 2, 403, origin("https://evil.example")),
		post("G37", "application/json", 2, 0, origin("https://"+guardTH)),
		post("G38", "application/json", 2, 403, origin("null")),
		// G39: not-local beats cross-site beats json-only.
		{id: "G39-not-local-first", peer: "tun:203.0.113.9", method: "POST", target: "/local/rooms", host: guardTH, ctype: "text/plain", bodyLen: 3,
			hdr: sfs("cross-site"), want: 404},
		post("G39-cross-site-over-415", "text/plain", 3, 403, sfs("cross-site")),
		{id: "G40-bare", peer: "tun:203.0.113.9", target: "/local", host: guardTH, want: 404},
		{id: "G40-slash", peer: "tun:203.0.113.9", target: "/local/", host: guardTH, want: 404},
		{id: "G40-prefix-only", peer: "tun:203.0.113.9", target: "/localfoo", host: guardTH, want: 404},
		{id: "G40-self-prefix-only", peer: "self", method: "POST", target: "/localfoo", host: "localhost", ctype: "text/plain", bodyLen: 2, want: 415},
		// G41: the guard decides on the decoded path.
		{id: "G41-pct-l", peer: "tun:203.0.113.9", target: "/%6Cocal/rooms", host: guardTH, want: 404},
		{id: "G41-pct-slash", peer: "tun:203.0.113.9", target: "/local%2Frooms", host: guardTH, want: 404},
		// G42: no spelling reaches /local from the tunnel (extra-strict: the cleaned path is checked too).
		{id: "G42-double-slash", peer: "tun:203.0.113.9", target: "//local/rooms", host: guardTH, want: 404},
		{id: "G42-dot", peer: "tun:203.0.113.9", target: "/./local/rooms", host: guardTH, want: 404},
		// Cleans to /r/local/rooms, not /local: forwarded untouched, the Stage router 404s it.
		{id: "G42-under-r", peer: "tun:203.0.113.9", target: "/r/K/../local/rooms", host: guardTH},
		{id: "G42-local-dotdot", peer: "tun:203.0.113.9", target: "/local/../local/rooms", host: guardTH, want: 404},
	})
}

func TestStageGuard_Lockout(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	miss := func(ip string, i int) gcase {
		return gcase{peer: "tun:" + ip, target: fmt.Sprintf("/r/BADKEY%017d/dashboard/orb.html", i), host: guardTH}
	}
	// R01: 21 misses answer 404 (the 21st is the one that locks), then 429.
	for i := 1; i <= 21; i++ {
		if c := rig.do(miss("203.0.113.9", i)).Code; c != 404 {
			t.Fatalf("R01: miss %d got %d want 404", i, c)
		}
	}
	for i := 22; i <= 23; i++ {
		if c := rig.do(miss("203.0.113.9", i)).Code; c != 429 {
			t.Fatalf("R01: request %d got %d want 429", i, c)
		}
	}
	if rec := rig.do(miss("203.0.113.9", 30)); rec.Body.String() != `{"error":"locked out"}` {
		t.Errorf("R01 body: %q", rec.Body.String())
	}
	// R02: another IP is untouched.
	if c := rig.do(miss("198.51.100.4", 1)).Code; c != 404 {
		t.Errorf("R02: got %d want 404", c)
	}
	rig.run(t, []gcase{
		{id: "R09", peer: "tun:203.0.113.9", target: "/r/whatever/dashboard/orb.html", host: guardTH, want: 429},
		{id: "R10", peer: "tun:203.0.113.9", target: "/local/rooms", host: guardTH, want: 404},
		{id: "R11", peer: "tun:203.0.113.9", target: "/r/whatever/x", host: "evil.example", want: 429},
		{id: "R08", peer: "self", target: "/r/REALKEY00000000000000/x", host: "localhost"},
	})
	// R03: the lock expires after LOCK_S.
	rig.clock = rig.clock.Add(stageGuardLockS*time.Second + time.Second)
	if c := rig.do(miss("203.0.113.9", 99)).Code; c != 404 {
		t.Errorf("R03: after expiry got %d want 404", c)
	}
}

func TestStageGuard_R04_WindowSlides(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	miss := func(i int) gcase {
		return gcase{peer: "tun:203.0.113.9", target: fmt.Sprintf("/r/BADKEY%017d/x", i), host: guardTH}
	}
	for i := 0; i < 15; i++ {
		rig.do(miss(i))
	}
	rig.clock = rig.clock.Add(stageGuardWindowS*time.Second + time.Second) // those 15 age out
	for i := 100; i < 120; i++ {                                           // 20 more: at the limit, not over
		if c := rig.do(miss(i)).Code; c != 404 {
			t.Fatalf("R04: miss %d got %d want 404 (old misses must have dropped out)", i, c)
		}
	}
}

func TestStageGuard_WhatCounts(t *testing.T) {
	flood := func(c gcase, n int) int {
		rig := newGuardRig(t, guardTH)
		var last int
		for i := 0; i < n; i++ {
			last = rig.do(c).Code
		}
		return last
	}
	tun := "tun:203.0.113.9"
	if got := flood(gcase{peer: tun, target: "/r/REALKEY00000000000000/dashboard/launcher.js", host: guardTH}, 30); got != 404 {
		t.Errorf("R05: real key + missing file must never lock, got %d", got)
	}
	if got := flood(gcase{peer: "self", target: "/r/BADKEY/x", host: "localhost"}, 60); got != 404 {
		t.Errorf("R07: self must never be counted or locked, got %d", got)
	}
	if got := flood(gcase{peer: tun, target: "/r/BADKEY/x", host: "evil.example"}, 30); got != 421 {
		t.Errorf("R12: guard refusals are not misses, got %d", got)
	}
	if got := flood(gcase{peer: tun, target: "/nothing/here", host: guardTH}, 30); got != 404 {
		t.Errorf("R13: unkeyed paths are not counted, got %d", got)
	}
	for _, pre := range []string{"/r/", "/k/", "/h/", "/pair/"} {
		if got := flood(gcase{peer: tun, target: pre + "BADKEY/x", host: guardTH}, 30); got != 429 {
			t.Errorf("R13 %s: keyed prefix must count, got %d", pre, got)
		}
	}
	if got := flood(gcase{peer: tun, target: "/pair/ABCD?secret=wrong", host: guardTH}, 30); got != 429 {
		t.Errorf("R15: pair-code 404s count, got %d", got)
	}
	// R20: cf-connecting-ip from a non-loopback peer is a tunnel caller (counted, keyed on the header).
	rig := newGuardRig(t, guardTH)
	var last int
	for i := 0; i < 30; i++ {
		req := httptest.NewRequest("GET", "/r/BADKEY/x", nil)
		req.Host, req.RemoteAddr = guardTH, "10.0.0.5:40"
		req.Header.Set("Cf-Connecting-Ip", "203.0.113.50")
		rec := httptest.NewRecorder()
		rig.h.ServeHTTP(rec, req)
		last = rec.Code
	}
	if last != 429 {
		t.Errorf("R20: got %d want 429", last)
	}
	if got := flood(gcase{peer: "10.0.0.5", target: "/r/BADKEY/x", host: "localhost"}, 30); got != 429 {
		t.Errorf("R20 (no cf header, non-loopback peer is keyed on the peer): got %d want 429", got)
	}
}

func TestStageGuard_R16_WebSocketUnknownKeyCounts(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	for i := 0; i < 30; i++ {
		rig.do(gcase{peer: "tun:203.0.113.9", target: fmt.Sprintf("/r/BADKEY%017d/ws/pad", i), host: guardTH, ws: true})
	}
	if !rig.g.isLocked("203.0.113.9") {
		t.Error("R16: websocket guesses on unknown keys must lock the IP")
	}
	// A refused socket on a KNOWN key (closed 4403 by the Stage) is not a guess.
	rig2 := newGuardRig(t, guardTH)
	for i := 0; i < 30; i++ {
		rig2.do(gcase{peer: "tun:203.0.113.9", target: "/r/REALKEY00000000000000/ws/denied", host: guardTH, ws: true})
	}
	if rig2.g.isLocked("203.0.113.9") {
		t.Error("R16: refusal on a known key must not count")
	}
}

func TestStageGuard_R18_UnlockLiftsKernelLock(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	for i := 0; i < 25; i++ {
		rig.do(gcase{peer: "tun:203.0.113.9", target: fmt.Sprintf("/r/BADKEY%017d/x", i), host: guardTH})
	}
	if !rig.g.isLocked("203.0.113.9") {
		t.Fatal("setup: not locked")
	}
	// POST /local/dev/guard {"ip": ...} from the Stage host lifts it (the Python route also runs behind).
	req := httptest.NewRequest("POST", "/local/dev/guard", strings.NewReader(`{"ip":"203.0.113.9"}`))
	req.RemoteAddr, req.Host = guardPeer, "localhost"
	req.Header.Set("Content-Type", "application/json")
	rig.h.ServeHTTP(httptest.NewRecorder(), req)
	if rig.g.isLocked("203.0.113.9") {
		t.Error("R18: kernel lock survived the owner's unlock")
	}
}

// A malformed or ambiguous unlock body never lifts anyone's lock (cog-review
// #686); only an explicit {"ip": null} lifts all, as stage-cli sends it.
func TestStageGuard_R18_UnlockBodyIsStrict(t *testing.T) {
	rig := newGuardRig(t, guardTH)
	for i := 0; i < 25; i++ {
		rig.do(gcase{peer: "tun:203.0.113.9", target: fmt.Sprintf("/r/BADKEY%017d/x", i), host: guardTH})
	}
	if !rig.g.isLocked("203.0.113.9") {
		t.Fatal("setup: not locked")
	}
	post := func(body string) int {
		req := httptest.NewRequest("POST", "/local/dev/guard", strings.NewReader(body))
		req.RemoteAddr, req.Host = guardPeer, "localhost"
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rig.h.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, body := range []string{``, `{}`, `{"ip":`, `[]`, `{"ip": 7}`, `{"ip": ""}`, `{"other": 1}`} {
		if code := post(body); code != 400 {
			t.Errorf("body %q: code %d, want 400", body, code)
		}
		if !rig.g.isLocked("203.0.113.9") {
			t.Fatalf("body %q lifted the lock", body)
		}
	}
	post(`{"ip": null}`)
	if rig.g.isLocked("203.0.113.9") {
		t.Error(`{"ip": null} must lift all locks`)
	}
}

func TestStageGuard_K22_LogPathScrub(t *testing.T) {
	got := stageGuardLogPath("/r/abcdefghijklmnopqrstuvwx/dashboard/orb.html")
	if strings.Contains(got, "abcdefghij") || !strings.HasPrefix(got, "/r/…") {
		t.Errorf("K22: key not scrubbed: %q", got)
	}
	if long := stageGuardLogPath("/" + strings.Repeat("a", 200)); len(long) > 80 {
		t.Errorf("path not cut to 80: %d", len(long))
	}
}

// Every keyed prefix is scrubbed in guard logs, including short pairing codes.
func TestStageGuard_K22_LogPathScrubAllKeyedPrefixes(t *testing.T) {
	for _, tc := range []struct{ in, secret string }{
		{"/r/abcdefghijklmnopqrstuvwx/dashboard/orb.html", "abcdefghij"},
		{"/k/abcdefghijklmnopqrstuvwx", "abcdefghij"},
		{"/h/abcdefghijklmnopqrstuvwx/", "abcdefghij"},
		{"/pair/BCDF", "BCDF"},
		{"/pair/BCDFGH?x=1", "BCDFGH"},
	} {
		got := stageGuardLogPath(tc.in)
		if strings.Contains(got, tc.secret) {
			t.Errorf("K22: %q logged as %q (key not scrubbed)", tc.in, got)
		}
	}
}

func TestStageGuard_ConfigPublicHost(t *testing.T) {
	cfg := &Config{}
	applyStageUISection(cfg, &stageUISection{Addr: "127.0.0.1:7873", PublicHost: "stage.myrgic.com"})
	if cfg.StageUI.PublicHost != "stage.myrgic.com" {
		t.Fatalf("public_host not applied: %+v", cfg.StageUI)
	}
}
