// mcp_modality_proxy_test.go — coverage for the mod3 MCP proxy tools.
//
// Strategy: stand up a fake mod3 via httptest.NewServer, point an MCPServer's
// proxy at it, exercise each tool handler directly (handler function +
// typed input, bypassing the MCP SDK's JSON marshal layer). Playback is
// stubbed via disablePlayback or the injectable player field so tests don't
// spawn afplay.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── fake mod3 with synthesize + session routes ──────────────────────────────

type fakeMod3Proxy struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	captured []capturedProxyRequest

	// Overrides for per-endpoint behavior.
	speakHandler       http.HandlerFunc
	synthesizeHandler  http.HandlerFunc
	stopHandler        http.HandlerFunc
	voicesHandler      http.HandlerFunc
	healthHandler      http.HandlerFunc
	regHandler         http.HandlerFunc
	deregHandler       http.HandlerFunc
	listSessionHandler http.HandlerFunc
	chatFlowLogHandler http.HandlerFunc
}

type capturedProxyRequest struct {
	Method string
	Path   string
	Query  string
	Body   []byte
}

// synthWav is a tiny valid-ish WAV header + ~1KB of silence. Not a real
// audio file — the proxy doesn't parse it, it just forwards/plays bytes.
var synthWav = func() []byte {
	b := make([]byte, 1024)
	copy(b, []byte("RIFF\x00\x00\x00\x00WAVEfmt "))
	return b
}()

func newFakeMod3Proxy(t *testing.T) *fakeMod3Proxy {
	t.Helper()
	fm := &fakeMod3Proxy{t: t}
	mux := http.NewServeMux()

	// POST /v1/speak — primary queue-aware speak endpoint (non-blocking, JSON).
	mux.HandleFunc("POST /v1/speak", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.speakHandler != nil {
			fm.speakHandler(w, r)
			return
		}
		// Default: return a "speaking" response (queue_position=0, first call).
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"job_id":         "job-speak-0001",
			"queue_position": 0,
			"status":         "speaking",
		})
	})

	// POST /v1/synthesize — raw-bytes path (skip_playback=true or legacy).
	mux.HandleFunc("POST /v1/synthesize", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.synthesizeHandler != nil {
			fm.synthesizeHandler(w, r)
			return
		}
		// Emit the full mod3 header surface so the proxy can extract it.
		w.Header().Set("X-Mod3-Job-Id", "job-test-0001")
		w.Header().Set("X-Mod3-Voice", "bm_lewis")
		w.Header().Set("X-Mod3-Duration-Sec", "1.23")
		w.Header().Set("X-Mod3-Sample-Rate", "24000")
		w.Header().Set("X-Mod3-Rtf", "9.29")
		w.Header().Set("X-Mod3-Chunks", "1")
		w.Header().Set("Content-Type", "audio/wav")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(synthWav)
	})

	mux.HandleFunc("POST /v1/stop", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.stopHandler != nil {
			fm.stopHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"status":        "stopped",
			"dropped_jobs":  0,
			"interrupted":   true,
			"session_id":    r.URL.Query().Get("session_id"),
			"job_id_target": r.URL.Query().Get("job_id"),
		})
	})

	mux.HandleFunc("GET /v1/voices", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.voicesHandler != nil {
			fm.voicesHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"voices": []map[string]any{
				{"id": "bm_lewis", "language": "en-GB"},
				{"id": "af_bella", "language": "en-US"},
			},
		})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.healthHandler != nil {
			fm.healthHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"status":       "ok",
			"model_loaded": true,
			"engine":       "kokoro",
		})
	})

	mux.HandleFunc("POST /v1/sessions/register", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.regHandler != nil {
			fm.regHandler(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"session_id":     body["session_id"],
			"participant_id": body["participant_id"],
			"assigned_voice": "bm_lewis",
		})
	})

	mux.HandleFunc("POST /v1/sessions/{id}/deregister", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.deregHandler != nil {
			fm.deregHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"status":     "ok",
			"session_id": r.PathValue("id"),
		})
	})

	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.listSessionHandler != nil {
			fm.listSessionHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"sessions":   []any{},
			"voice_pool": []string{"bm_lewis", "af_bella"},
		})
	})

	mux.HandleFunc("GET /v1/logs/chat-flow", func(w http.ResponseWriter, r *http.Request) {
		fm.capture(r)
		if fm.chatFlowLogHandler != nil {
			fm.chatFlowLogHandler(w, r)
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"events": []map[string]any{
				{
					"ts":              "2026-05-15T00:00:00Z",
					"event_type":      "chat.message_received",
					"session_id":      r.URL.Query().Get("session_id"),
					"message_id":      "test-msg",
					"from_seat":       "http",
					"to_seats":        []string{},
					"content_hash":    "b94d27b9",
					"content_preview": "hello world",
					"direction":       "inbound",
				},
			},
			"count": 1,
		})
	})

	fm.srv = httptest.NewServer(mux)
	t.Cleanup(func() { fm.srv.Close() })
	return fm
}

func (fm *fakeMod3Proxy) capture(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.captured = append(fm.captured, capturedProxyRequest{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Body: body,
	})
}

func (fm *fakeMod3Proxy) last() capturedProxyRequest {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if len(fm.captured) == 0 {
		fm.t.Fatalf("no captured requests")
	}
	return fm.captured[len(fm.captured)-1]
}

// newProxyMCP builds a minimal MCPServer whose proxy points at fm, with
// playback fully disabled so tests don't touch the audio stack. A live
// Server is wired in as the channel-session backend so the session-family
// MCP tools (register/deregister/list) flow through the kernel's shared
// minting logic — matching production (ADR-082 Wave 3.5). Synthesis /
// control tools continue calling mod3 directly via the proxy.
func newProxyMCP(t *testing.T, fm *fakeMod3Proxy) *MCPServer {
	t.Helper()
	cfg := &Config{Mod3URL: fm.srv.URL}
	srv := &Server{
		cfg:                    cfg,
		channelSessionRegistry: NewChannelSessionRegistry(),
	}
	m := &MCPServer{
		cfg:                   cfg,
		mod3Proxy:             &modalityProxy{disablePlayback: true},
		channelSessionBackend: srv,
	}
	return m
}

// newProxyMCPWithServer is like newProxyMCP but returns the wired-in Server
// so tests that want to assert on the kernel-side registry state can do so.
func newProxyMCPWithServer(t *testing.T, fm *fakeMod3Proxy) (*MCPServer, *Server) {
	t.Helper()
	cfg := &Config{Mod3URL: fm.srv.URL}
	srv := &Server{
		cfg:                    cfg,
		channelSessionRegistry: NewChannelSessionRegistry(),
	}
	m := &MCPServer{
		cfg:                   cfg,
		mod3Proxy:             &modalityProxy{disablePlayback: true},
		channelSessionBackend: srv,
	}
	return m, srv
}

// decodeToolText parses the JSON text content of a CallToolResult.
func decodeToolText(t *testing.T, res *mcp.CallToolResult) map[string]any {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatalf("empty result")
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected *mcp.TextContent, got %T", res.Content[0])
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &out); err != nil {
		t.Fatalf("decode result text: %v (raw=%q)", err, tc.Text)
	}
	return out
}

// ─── mod3_speak — MCP queue path ─────────────────────────────────────────────

// newSpeakFn builds a deterministic speakFn stub. Each call returns a
// "queued" response whose queue_position increments per call (starting at 1
// for the first call that arrives while something is "playing"). The first
// invocation returns a "speaking" response (queue_position 0); subsequent
// ones simulate the queue growing.
//
// capturedArgs is populated with the args map each call receives, in order.
func newSpeakFn(capturedArgs *[]map[string]any) func(ctx context.Context, in mod3SpeakInput) (map[string]any, error) {
	var mu sync.Mutex
	callCount := 0
	return func(ctx context.Context, in mod3SpeakInput) (map[string]any, error) {
		mu.Lock()
		n := callCount
		callCount++
		mu.Unlock()

		args := map[string]any{"text": in.Text}
		if in.Voice != "" {
			args["voice"] = in.Voice
		}
		if in.Speed > 0 {
			args["speed"] = in.Speed
		}
		if in.Emotion > 0 {
			args["emotion"] = in.Emotion
		}
		if in.SessionID != "" {
			args["session_id"] = in.SessionID
		}
		if capturedArgs != nil {
			mu.Lock()
			*capturedArgs = append(*capturedArgs, args)
			mu.Unlock()
		}

		jobID := fmt.Sprintf("job-mcp-%04d", n)
		if n == 0 {
			// First call: starts immediately, no queue.
			return map[string]any{
				"status":             "speaking",
				"job_id":             jobID,
				"queue_position":     float64(0),
				"estimated_wait_sec": float64(0),
			}, nil
		}
		// Subsequent calls: queued behind the first.
		return map[string]any{
			"status":         "queued",
			"job_id":         jobID,
			"queue_position": float64(n),
			"currently_playing": map[string]any{
				"job_id":        "job-mcp-0000",
				"remaining_sec": float64(3),
			},
			"queue_ahead":        []any{},
			"estimated_wait_sec": float64(n) * 3.0,
			"actions":            fmt.Sprintf("To cancel, call stop(job_id='%s').", jobID),
		}, nil
	}
}

// TestMod3Speak_MCPSuccessPath — the primary happy path: speakFn returns a
// "speaking" response; the result must contain job_id + queue_position.
// With the queue-aware /v1/speak endpoint, mod3 owns playback — the kernel
// returns the JSON queue state directly without setting playback_status.
func TestMod3Speak_MCPSuccessPath(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	m.mod3Proxy.speakFn = newSpeakFn(nil)

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text: "hello world",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError=true: %v", res.Content)
	}

	out := decodeToolText(t, res)
	if status, _ := out["status"].(string); status != "speaking" {
		t.Fatalf("expected status=speaking, got %v", out["status"])
	}
	if jobID, _ := out["job_id"].(string); !strings.HasPrefix(jobID, "job-mcp-") {
		t.Fatalf("expected job_id from mod3, got %v", out["job_id"])
	}
	// queue_position must be present (0 for immediately-playing).
	if qp, ok := out["queue_position"]; !ok {
		t.Fatal("expected queue_position in result")
	} else if qp.(float64) != 0 {
		t.Fatalf("expected queue_position=0, got %v", qp)
	}
	// estimated_wait_sec must be present.
	if _, ok := out["estimated_wait_sec"]; !ok {
		t.Fatal("expected estimated_wait_sec in result")
	}
	// playback_status is NOT set by the primary path — mod3's drain thread
	// owns all audio scheduling; the kernel no longer reports local player status.
	if _, present := out["playback_status"]; present {
		t.Fatalf("playback_status must not appear in primary /v1/speak response, got %v", out["playback_status"])
	}
}

// TestMod3Speak_MCPQueued — second call while first plays: response must
// include queue_position >= 1, currently_playing, and estimated_wait_sec > 0.
func TestMod3Speak_MCPQueued(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	speakFn := newSpeakFn(nil)
	m.mod3Proxy.speakFn = speakFn

	// First call — starts immediately.
	_, _, _ = m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "first"})

	// Second call — should be queued.
	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "second"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError=true: %v", res.Content)
	}
	out := decodeToolText(t, res)
	if status, _ := out["status"].(string); status != "queued" {
		t.Fatalf("expected status=queued, got %v", out["status"])
	}
	qp, ok := out["queue_position"].(float64)
	if !ok || qp < 1 {
		t.Fatalf("expected queue_position >= 1, got %v", out["queue_position"])
	}
	if _, present := out["currently_playing"]; !present {
		t.Fatal("expected currently_playing in queued response")
	}
	if wait, _ := out["estimated_wait_sec"].(float64); wait <= 0 {
		t.Fatalf("expected estimated_wait_sec > 0, got %v", out["estimated_wait_sec"])
	}
	if _, present := out["actions"]; !present {
		t.Fatal("expected actions hint in queued response")
	}
}

// TestMod3Speak_MCPForwardsArgs — speakFn receives all call-site arguments
// (text, voice, speed, session_id) correctly.
func TestMod3Speak_MCPForwardsArgs(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	var captured []map[string]any
	m.mod3Proxy.speakFn = newSpeakFn(&captured)

	_, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:      "hello",
		SessionID: "cs-abc123",
		Voice:     "af_bella",
		Speed:     1.1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(captured) == 0 {
		t.Fatal("speakFn not called")
	}
	args := captured[0]
	if args["text"] != "hello" {
		t.Fatalf("expected text=hello, got %v", args["text"])
	}
	if args["session_id"] != "cs-abc123" {
		t.Fatalf("expected session_id=cs-abc123, got %v", args["session_id"])
	}
	if args["voice"] != "af_bella" {
		t.Fatalf("expected voice=af_bella, got %v", args["voice"])
	}
	if speed, _ := args["speed"].(float64); speed != 1.1 {
		t.Fatalf("expected speed=1.1, got %v", args["speed"])
	}
}

// TestMod3Speak_MCPOmitsSessionIDWhenAbsent — no session_id on the call site
// must not produce a session_id key in the args forwarded to speakFn.
func TestMod3Speak_MCPOmitsSessionIDWhenAbsent(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	var captured []map[string]any
	m.mod3Proxy.speakFn = newSpeakFn(&captured)

	_, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "plain"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(captured) == 0 {
		t.Fatal("speakFn not called")
	}
	if _, present := captured[0]["session_id"]; present {
		t.Fatalf("expected no session_id key forwarded, got %v", captured[0]["session_id"])
	}
}

// TestMod3Speak_ConcurrentCallsSequenced — three concurrent mod3_speak calls
// produce responses with strictly increasing queue_position values (0, 1, 2).
// This is the critical regression test: the old /v1/synthesize+afplay path
// would spawn three concurrent players; the MCP queue path serializes them.
func TestMod3Speak_ConcurrentCallsSequenced(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	m.mod3Proxy.speakFn = newSpeakFn(nil)

	const n = 3
	type result struct {
		pos float64
		err string
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
				Text: fmt.Sprintf("message %d", i),
			})
			if err != nil {
				results[i] = result{err: err.Error()}
				return
			}
			if res.IsError {
				tc := res.Content[0].(*mcp.TextContent)
				results[i] = result{err: tc.Text}
				return
			}
			out := decodeToolText(t, res)
			pos, _ := out["queue_position"].(float64)
			results[i] = result{pos: pos}
		}()
	}
	wg.Wait()

	// Collect positions and assert no errors.
	var positions []float64
	for i, r := range results {
		if r.err != "" {
			t.Fatalf("call %d failed: %s", i, r.err)
		}
		positions = append(positions, r.pos)
	}

	// Sort positions; they should span {0, 1, 2} — each call got a distinct
	// queue slot, proving mod3's serializer (not concurrent afplay) controls order.
	seen := map[float64]bool{}
	for _, p := range positions {
		if seen[p] {
			t.Fatalf("duplicate queue_position %v — calls were NOT serialized; got %v", p, positions)
		}
		seen[p] = true
	}
	for _, expected := range []float64{0, 1, 2} {
		if !seen[expected] {
			t.Fatalf("expected queue_position %v in results, got %v", expected, positions)
		}
	}
}

// ─── mod3_speak — fallback path (MCP unreachable) ────────────────────────────

func TestMod3Speak_EmptyTextRejects(t *testing.T) {
	m := &MCPServer{cfg: &Config{Mod3URL: "http://unused"}}
	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "   "})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// textResult — IsError is false because it's a validation message, but
	// the content must mention the required field.
	if len(res.Content) == 0 {
		t.Fatal("expected content")
	}
	tc := res.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "required") {
		t.Fatalf("expected 'required' in message, got %q", tc.Text)
	}
}

// TestMod3Speak_Mod3UnreachableReturnsCleanError — when mod3's /v1/synthesize
// REST endpoint is unreachable, the handler must return IsError=true with an
// "unreachable" message. (The old MCP-transport approach produced a composite
// "mcp: ... http: ..." error; with the REST transport the error is simpler.)
func TestMod3Speak_Mod3UnreachableReturnsCleanError(t *testing.T) {
	// Port that refuses connections.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // release the port so dials get ECONNREFUSED

	m := &MCPServer{
		cfg:       &Config{Mod3URL: "http://" + addr},
		mod3Proxy: &modalityProxy{disablePlayback: true},
	}
	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "hi"})
	if err != nil {
		t.Fatalf("handler should not return Go error, got %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError=true when mod3 is unreachable")
	}
	tc := res.Content[0].(*mcp.TextContent)
	lower := strings.ToLower(tc.Text)
	if !strings.Contains(lower, "unreachable") {
		t.Fatalf("expected 'unreachable' in error text, got %q", tc.Text)
	}
}

// TestMod3Speak_RESTPathNoSpeakFnInjection — verifies the primary REST path
// works when no speakFn is injected. The kernel posts to the fake mod3's
// /v1/speak (queue-aware) and receives a JSON {job_id, queue_position, status}
// response. Mod3 owns playback — no playback_status set by the kernel.
func TestMod3Speak_RESTPathNoSpeakFnInjection(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)
	// No speakFn injected — kernel uses the real REST path against the fake server.

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text: "rest path test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success (REST path), got IsError=true: %v", res.Content)
	}
	out := decodeToolText(t, res)

	// Verify the queue-state response shape from /v1/speak JSON body.
	if status, _ := out["status"].(string); status != "speaking" {
		t.Fatalf("expected status=speaking from /v1/speak, got %v", out["status"])
	}
	if jobID, _ := out["job_id"].(string); jobID != "job-speak-0001" {
		t.Fatalf("expected job_id=job-speak-0001 from /v1/speak, got %v", out["job_id"])
	}
	if qp, ok := out["queue_position"]; !ok {
		t.Fatal("expected queue_position in /v1/speak response")
	} else if qp.(float64) != 0 {
		t.Fatalf("expected queue_position=0, got %v", qp)
	}
	// _audio_bytes must NOT appear — /v1/speak returns JSON not WAV bytes.
	if _, present := out["_audio_bytes"]; present {
		t.Fatal("_audio_bytes must not appear in /v1/speak response")
	}
	// playback_status is NOT set by the primary path — mod3's drain thread owns audio.
	if _, present := out["playback_status"]; present {
		t.Fatalf("playback_status must not appear in primary /v1/speak response, got %v", out["playback_status"])
	}

	// Verify the fake server received POST to /v1/speak (not /v1/synthesize).
	last := fm.last()
	if last.Path != "/v1/speak" || last.Method != "POST" {
		t.Fatalf("expected POST /v1/speak, got %s %s", last.Method, last.Path)
	}
	var body map[string]any
	if err := json.Unmarshal(last.Body, &body); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	if body["text"] != "rest path test" {
		t.Fatalf("expected text forwarded, got %v", body["text"])
	}
}

// TestMod3Speak_PreservesErrorBody — when /v1/speak returns a non-2xx,
// the mod3 error body is preserved intact in the tool error.
func TestMod3Speak_PreservesErrorBody(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	fm.speakHandler = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":"text must not be empty"}`))
	}
	m := newProxyMCP(t, fm)
	// No speakFn — REST path hits /v1/speak which returns 422.

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{Text: "bad"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError=true on mod3 422")
	}
	tc := res.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "422") {
		t.Fatalf("expected '422' in error text, got %q", tc.Text)
	}
	if !strings.Contains(tc.Text, "text must not be empty") {
		t.Fatalf("expected mod3 body preserved, got %q", tc.Text)
	}
}

// ─── mod3_speak — legacy test names for backward-compat with existing CI ─────

func TestMod3Speak_SuccessPath(t *testing.T) {
	TestMod3Speak_MCPSuccessPath(t)
}

func TestMod3Speak_ForwardsSessionID(t *testing.T) {
	TestMod3Speak_MCPForwardsArgs(t)
}

func TestMod3Speak_OmitsSessionIDWhenAbsent(t *testing.T) {
	TestMod3Speak_MCPOmitsSessionIDWhenAbsent(t)
}

func TestMod3Speak_Mod3DownReturnsCleanError(t *testing.T) {
	TestMod3Speak_Mod3UnreachableReturnsCleanError(t)
}

func TestMod3Speak_FallbackPreservesMod3ErrorBody(t *testing.T) {
	TestMod3Speak_PreservesErrorBody(t)
}

func TestMod3Speak_SkipPlaybackReturnsBase64(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:         "skip",
		SkipPlayback: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := decodeToolText(t, res)
	if got, _ := out["playback_status"].(string); got != "skipped" {
		t.Fatalf("expected playback_status=skipped, got %v", out["playback_status"])
	}
	if b64, _ := out["audio_base64"].(string); b64 == "" {
		t.Fatal("expected audio_base64 populated when skip_playback=true")
	}
}

// ─── mod3_stop / voices / status / sessions ──────────────────────────────────

func TestMod3Stop_ForwardsSessionAndJob(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	res, _, err := m.toolMod3Stop(context.Background(), nil, mod3StopInput{
		SessionID: "cs-abc",
		JobID:     "job-xyz",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatal("expected success")
	}

	cap := fm.last()
	if !strings.Contains(cap.Query, "session_id=cs-abc") {
		t.Fatalf("expected session_id in query, got %q", cap.Query)
	}
	if !strings.Contains(cap.Query, "job_id=job-xyz") {
		t.Fatalf("expected job_id in query, got %q", cap.Query)
	}

	out := decodeToolText(t, res)
	if out["status"] != "stopped" {
		t.Fatalf("expected status=stopped, got %v", out["status"])
	}
}

func TestMod3Voices_ReturnsRawList(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	res, _, err := m.toolMod3Voices(context.Background(), nil, mod3VoicesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatal("expected success")
	}
	out := decodeToolText(t, res)
	voices, _ := out["voices"].([]any)
	if len(voices) != 2 {
		t.Fatalf("expected 2 voices, got %d", len(voices))
	}
}

func TestMod3Voices_ThreadsSessionID(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	_, _, _ = m.toolMod3Voices(context.Background(), nil, mod3VoicesInput{SessionID: "cs-qq"})
	cap := fm.last()
	if !strings.Contains(cap.Query, "session_id=cs-qq") {
		t.Fatalf("expected session_id in query, got %q", cap.Query)
	}
}

func TestMod3Status_HitsHealth(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	res, _, err := m.toolMod3Status(context.Background(), nil, mod3StatusInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatal("expected success")
	}
	out := decodeToolText(t, res)
	if out["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", out["status"])
	}

	cap := fm.last()
	if cap.Path != "/health" {
		t.Fatalf("expected /health, got %q", cap.Path)
	}
}

func TestMod3Status_Mod3DownClean(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	_ = l.Close()

	m := &MCPServer{cfg: &Config{Mod3URL: "http://" + addr}}
	res, _, err := m.toolMod3Status(context.Background(), nil, mod3StatusInput{})
	if err != nil {
		t.Fatalf("handler should not Go-error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError=true")
	}
}

// TestMod3RegisterSession_RoutesThroughKernel verifies that the MCP tool
// goes through the kernel's shared RegisterChannelSession backend — not
// directly to mod3 — and that the response is the merged {kernel, mod3}
// block produced by that shared code path (ADR-082 Wave 3.5).
func TestMod3RegisterSession_RoutesThroughKernel(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m, srv := newProxyMCPWithServer(t, fm)

	res, _, err := m.toolMod3RegisterSession(context.Background(), nil, mod3RegisterSessionInput{
		SessionID:             "cs-regtest",
		ParticipantID:         "cog",
		ParticipantType:       "agent",
		PreferredVoice:        "bm_lewis",
		PreferredOutputDevice: "system-default",
		Priority:              2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError: %v", res.Content)
	}

	// The forward to mod3 must have happened via the kernel's shared
	// method — verify the request body mod3 saw carries the caller-
	// supplied session_id and participant_id unchanged.
	cap := fm.last()
	if cap.Path != "/v1/sessions/register" {
		t.Fatalf("expected mod3 /v1/sessions/register, got %q", cap.Path)
	}
	var body map[string]any
	if err := json.Unmarshal(cap.Body, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["session_id"] != "cs-regtest" {
		t.Fatalf("bad session_id forwarded: %v", body["session_id"])
	}
	if body["participant_id"] != "cog" {
		t.Fatalf("bad participant_id forwarded: %v", body["participant_id"])
	}
	if body["preferred_voice"] != "bm_lewis" {
		t.Fatalf("bad preferred_voice forwarded: %v", body["preferred_voice"])
	}

	// The merged {kernel, mod3} shape must land — verify the kernel's
	// identity record is present in the response.
	out := decodeToolText(t, res)
	kernel, ok := out["kernel"].(map[string]any)
	if !ok {
		t.Fatalf("expected kernel block in response, got %v", out)
	}
	if kernel["session_id"] != "cs-regtest" {
		t.Fatalf("expected kernel.session_id=cs-regtest, got %v", kernel["session_id"])
	}
	if kernel["id_source"] != "caller" {
		t.Fatalf("expected id_source=caller, got %v", kernel["id_source"])
	}

	// Kernel registry must hold the committed record — proves we went
	// through the shared backend and not straight to mod3.
	if _, held := srv.channelSessionRegistry.Get("cs-regtest"); !held {
		t.Fatal("expected kernel registry to hold record after register")
	}
}

// TestMod3RegisterSession_KernelMintsWhenAbsent exercises the minting
// path — caller omits session_id, kernel mints one, mod3 receives it.
func TestMod3RegisterSession_KernelMintsWhenAbsent(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m, srv := newProxyMCPWithServer(t, fm)

	res, _, err := m.toolMod3RegisterSession(context.Background(), nil, mod3RegisterSessionInput{
		ParticipantID: "cog",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError: %v", res.Content)
	}

	out := decodeToolText(t, res)
	kernel, _ := out["kernel"].(map[string]any)
	sid, _ := kernel["session_id"].(string)
	if !strings.HasPrefix(sid, "cs-") {
		t.Fatalf("expected minted cs-* session_id, got %q", sid)
	}
	if kernel["id_source"] != "minted" {
		t.Fatalf("expected id_source=minted, got %v", kernel["id_source"])
	}

	// Mod3 must have seen the kernel-minted ID verbatim.
	cap := fm.last()
	var body map[string]any
	_ = json.Unmarshal(cap.Body, &body)
	if body["session_id"] != sid {
		t.Fatalf("mod3 got session_id=%v, expected %q", body["session_id"], sid)
	}

	if _, held := srv.channelSessionRegistry.Get(sid); !held {
		t.Fatalf("expected kernel registry to hold minted record %q", sid)
	}
}

// TestMod3RegisterSession_ForwardsKindsAndMetadata verifies the Wave 3.5
// schema alignment with the channel-provider RFC — `kinds` and `metadata`
// flow through the kernel's register endpoint and land in mod3's request
// body unchanged.
func TestMod3RegisterSession_ForwardsKindsAndMetadata(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m, _ := newProxyMCPWithServer(t, fm)

	_, _, err := m.toolMod3RegisterSession(context.Background(), nil, mod3RegisterSessionInput{
		SessionID:       "cs-kinds",
		ParticipantID:   "mod3-provider",
		ParticipantType: "provider",
		Kinds:           []string{"audio"},
		Metadata: map[string]any{
			"provider_id": "mod3-local",
			"build":       "0.5.0",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cap := fm.last()
	var body map[string]any
	if err := json.Unmarshal(cap.Body, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	kinds, ok := body["kinds"].([]any)
	if !ok || len(kinds) != 1 || kinds[0] != "audio" {
		t.Fatalf("expected kinds=[\"audio\"] forwarded, got %v", body["kinds"])
	}
	md, ok := body["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata object forwarded, got %v (%T)", body["metadata"], body["metadata"])
	}
	if md["provider_id"] != "mod3-local" {
		t.Fatalf("expected metadata.provider_id=mod3-local, got %v", md["provider_id"])
	}
	if body["participant_type"] != "provider" {
		t.Fatalf("expected participant_type=provider, got %v", body["participant_type"])
	}
}

func TestMod3RegisterSession_RejectsWithoutParticipant(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	res, _, err := m.toolMod3RegisterSession(context.Background(), nil, mod3RegisterSessionInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tc := res.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "participant_id") {
		t.Fatalf("expected validation message, got %q", tc.Text)
	}
}

func TestMod3RegisterSession_NoBackendReturnsCleanError(t *testing.T) {
	// An MCPServer with no channel-session backend wired in must surface
	// a clean "not configured" error rather than a nil deref — important
	// because NewMCPServer (used by tests that only care about memory
	// tools) doesn't wire the backend.
	m := &MCPServer{cfg: &Config{Mod3URL: "http://unused"}}
	res, _, err := m.toolMod3RegisterSession(context.Background(), nil, mod3RegisterSessionInput{
		ParticipantID: "cog",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError=true when backend is nil")
	}
	tc := res.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "backend not configured") {
		t.Fatalf("expected backend-not-configured message, got %q", tc.Text)
	}
}

// TestMod3DeregisterSession_RoutesThroughKernel verifies the deregister
// tool forwards via the kernel's shared path and the kernel drops its
// identity record on success.
func TestMod3DeregisterSession_RoutesThroughKernel(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m, srv := newProxyMCPWithServer(t, fm)

	// Seed the kernel registry so we can see it get dropped.
	srv.channelSessionRegistry.Put(ChannelSessionRecord{
		SessionID: "cs-drop", ParticipantID: "cog",
	})

	res, _, err := m.toolMod3DeregisterSession(context.Background(), nil, mod3DeregisterSessionInput{
		SessionID: "cs-drop",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError: %v", res.Content)
	}
	cap := fm.last()
	if cap.Path != "/v1/sessions/cs-drop/deregister" {
		t.Fatalf("expected /v1/sessions/cs-drop/deregister at mod3, got %q", cap.Path)
	}
	if _, held := srv.channelSessionRegistry.Get("cs-drop"); held {
		t.Fatal("expected kernel registry to drop record after successful deregister")
	}
}

func TestMod3DeregisterSession_RequiresID(t *testing.T) {
	m := &MCPServer{cfg: &Config{Mod3URL: "http://unused"}}
	res, _, err := m.toolMod3DeregisterSession(context.Background(), nil, mod3DeregisterSessionInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tc := res.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "session_id") {
		t.Fatalf("expected session_id message, got %q", tc.Text)
	}
}

// TestMod3ListSessions_RoutesThroughKernel verifies list merges the
// kernel snapshot with mod3's per-channel state (the new Wave 3.5
// merged shape, not mod3's raw payload).
func TestMod3ListSessions_RoutesThroughKernel(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m, srv := newProxyMCPWithServer(t, fm)

	srv.channelSessionRegistry.Put(ChannelSessionRecord{
		SessionID: "cs-seed", ParticipantID: "cog",
	})

	res, _, err := m.toolMod3ListSessions(context.Background(), nil, mod3ListSessionsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got IsError: %v", res.Content)
	}
	out := decodeToolText(t, res)
	kernel, ok := out["kernel"].([]any)
	if !ok {
		t.Fatalf("expected kernel array in merged response, got %v", out)
	}
	if len(kernel) != 1 {
		t.Fatalf("expected 1 kernel record, got %d", len(kernel))
	}
	// Mod3 block must be present (from the fake list handler).
	if _, present := out["mod3"]; !present {
		t.Fatal("expected mod3 block in merged response")
	}
}

// ─── metric extraction unit test ─────────────────────────────────────────────

func TestExtractMod3Metrics_TypesByValue(t *testing.T) {
	h := http.Header{}
	h.Set("X-Mod3-Job-Id", "abc123")
	h.Set("X-Mod3-Duration-Sec", "2.50")
	h.Set("X-Mod3-Sample-Rate", "24000")
	h.Set("X-Mod3-Rtf", "8.1")
	h.Set("Content-Type", "audio/wav") // should be skipped

	out := extractMod3Metrics(h)
	if len(out) != 4 {
		t.Fatalf("expected 4 metrics, got %d: %v", len(out), out)
	}
	if got, _ := out["job-id"].(string); got != "abc123" {
		t.Fatalf("job-id: got %v", out["job-id"])
	}
	if got, ok := out["duration-sec"].(float64); !ok || got != 2.5 {
		t.Fatalf("duration-sec: got %v (%T)", out["duration-sec"], out["duration-sec"])
	}
	if got, ok := out["sample-rate"].(int64); !ok || got != 24000 {
		t.Fatalf("sample-rate should parse to int64, got %v (%T)",
			out["sample-rate"], out["sample-rate"])
	}
	if _, present := out["content-type"]; present {
		t.Fatal("content-type should be skipped")
	}
}

// ─── playback injection test ─────────────────────────────────────────────────

// TestPlayAudio_StubPlayer — validate the playback plumbing by injecting a
// small shell-script player that records the file it received. Proves the
// audio bytes reach the player (the bug the installed binary has today is
// that they don't). Only runs on OSes where a shell is available.
func TestPlayAudio_StubPlayer(t *testing.T) {
	dir := t.TempDir()
	recPath := filepath.Join(dir, "received.log")
	stubPath := filepath.Join(dir, "stub-player.sh")

	// Player writes its last-arg path and the first 4 bytes of the wav to
	// received.log; proves (a) the player got a path, (b) the file exists
	// at that path with our bytes.
	stubBody := `#!/bin/sh
path="$1"
hdr=$(dd if="$path" bs=4 count=1 2>/dev/null)
printf 'path=%s hdr=%s\n' "$path" "$hdr" > "` + recPath + `"
`
	if err := os.WriteFile(stubPath, []byte(stubBody), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	p := &modalityProxy{player: stubPath}
	if err := p.playAudio(synthWav, true); err != nil {
		t.Fatalf("playAudio: %v", err)
	}

	got, err := os.ReadFile(recPath)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	line := string(got)
	if !strings.Contains(line, "hdr=RIFF") {
		t.Fatalf("player did not see RIFF header; got %q", line)
	}
	if !strings.Contains(line, "path=") {
		t.Fatalf("player did not get a path arg; got %q", line)
	}
}

// TestPlayAudio_NonBlockingSpawn — fire-and-forget. Use a sleep-style stub
// and assert playAudio returns before the stub would finish. Prevents the
// regression where speech synthesis blocks the MCP response on playback.
func TestPlayAudio_NonBlockingSpawn(t *testing.T) {
	dir := t.TempDir()
	stubPath := filepath.Join(dir, "sleeper.sh")
	stubBody := `#!/bin/sh
sleep 5
`
	if err := os.WriteFile(stubPath, []byte(stubBody), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	p := &modalityProxy{player: stubPath}

	done := make(chan struct{})
	go func() {
		if err := p.playAudio(synthWav, false); err != nil {
			t.Errorf("playAudio: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		// Expected: playAudio returns immediately in non-blocking mode.
	case <-time.After(2 * time.Second):
		t.Fatal("playAudio(blocking=false) did not return within 2s")
	}
}

// ─── Wave 4.3 — subscriber-check / afplay skip (local-fallback path) ────────
//
// These tests exercise toolMod3SpeakLocalFallback, which is the preserved
// legacy code path for when a caller explicitly needs the kernel to manage
// local audio playback (e.g. testing the audio plumbing independently from
// the primary /v1/speak queue path). The primary toolMod3Speak now routes
// through /v1/speak and mod3's drain thread owns all audio.

// TestMod3Speak_LocalFallback_NoSessionSpawnsPlayer — session_id="" in
// the local-fallback path bypasses the subscriber check and spawns the player.
func TestMod3Speak_LocalFallback_NoSessionSpawnsPlayer(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	stubPath, count := writeStubPlayer(t)
	m.mod3Proxy = &modalityProxy{player: stubPath}

	res, _, err := m.toolMod3SpeakLocalFallback(context.Background(), mod3SpeakInput{
		Text:     "no session",
		Blocking: true,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got %v", res.Content)
	}
	out := decodeToolText(t, res)
	if got, _ := out["playback_status"].(string); got != "played" {
		t.Fatalf("expected playback_status=played, got %v", out["playback_status"])
	}
	if got := count(); got != 1 {
		t.Fatalf("expected stub player invoked once, got %d", got)
	}
}

// TestMod3Speak_LocalFallback_SessionWithSubscriberSkipsPlayer — subscriber-
// check returns true: the local-fallback path skips the player (routed_ws).
func TestMod3Speak_LocalFallback_SessionWithSubscriberSkipsPlayer(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	stubPath, count := writeStubPlayer(t)
	m.mod3Proxy = &modalityProxy{
		player: stubPath,
		subscriberCheck: func(ctx context.Context, sessionID string) (bool, error) {
			if sessionID != "cs-with-sub" {
				t.Errorf("unexpected session_id=%q", sessionID)
			}
			return true, nil
		},
	}

	res, _, err := m.toolMod3SpeakLocalFallback(context.Background(), mod3SpeakInput{
		Text:      "skip me",
		SessionID: "cs-with-sub",
		Blocking:  true,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got %v", res.Content)
	}
	out := decodeToolText(t, res)
	if got, _ := out["playback_status"].(string); got != "routed_ws" {
		t.Fatalf("expected playback_status=routed_ws, got %v", out["playback_status"])
	}
	// Give any stray goroutine a moment to trip the stub — proving non-invocation.
	time.Sleep(100 * time.Millisecond)
	if got := count(); got != 0 {
		t.Fatalf("expected stub player NOT invoked, got %d", got)
	}
}

// TestMod3Speak_LocalFallback_NoSubscriberSpawnsPlayer — subscriber-check
// returns false: kernel falls back to the player path.
func TestMod3Speak_LocalFallback_NoSubscriberSpawnsPlayer(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	stubPath, count := writeStubPlayer(t)
	m.mod3Proxy = &modalityProxy{
		player: stubPath,
		subscriberCheck: func(ctx context.Context, sessionID string) (bool, error) {
			return false, nil
		},
	}

	res, _, err := m.toolMod3SpeakLocalFallback(context.Background(), mod3SpeakInput{
		Text:      "no subscriber",
		SessionID: "cs-no-sub",
		Blocking:  true,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got %v", res.Content)
	}
	out := decodeToolText(t, res)
	if got, _ := out["playback_status"].(string); got != "played" {
		t.Fatalf("expected playback_status=played, got %v", out["playback_status"])
	}
	if got := count(); got != 1 {
		t.Fatalf("expected stub player invoked once, got %d", got)
	}
}

// TestMod3Speak_LocalFallback_SubscriberCheckErrorFallsBack — transient check
// error must not orphan the audio; the local-fallback path still spawns the player.
func TestMod3Speak_LocalFallback_SubscriberCheckErrorFallsBack(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	stubPath, count := writeStubPlayer(t)
	m.mod3Proxy = &modalityProxy{
		player: stubPath,
		subscriberCheck: func(ctx context.Context, sessionID string) (bool, error) {
			return false, fmt.Errorf("mod3 probe timed out")
		},
	}

	res, _, err := m.toolMod3SpeakLocalFallback(context.Background(), mod3SpeakInput{
		Text:      "check failed",
		SessionID: "cs-flaky",
		Blocking:  true,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success despite check error, got %v", res.Content)
	}
	out := decodeToolText(t, res)
	if got, _ := out["playback_status"].(string); got != "played" {
		t.Fatalf("expected playback_status=played, got %v", out["playback_status"])
	}
	if checkErr, _ := out["subscriber_check_error"].(string); !strings.Contains(checkErr, "probe timed out") {
		t.Fatalf("expected subscriber_check_error to surface, got %v", out["subscriber_check_error"])
	}
	if got := count(); got != 1 {
		t.Fatalf("expected stub player invoked once on check error, got %d", got)
	}
}

// TestCheckSessionSubscriber_DefaultImplementationHitsMod3 — wire up a
// stand-alone fake HTTP server that answers the
// /v1/sessions/{id}/subscribers probe and verify the default implementation
// (no injected subscriberCheck) parses its response correctly.
func TestCheckSessionSubscriber_DefaultImplementationHitsMod3(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/cs-yes/subscribers", func(w http.ResponseWriter, r *http.Request) {
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"session_id": "cs-yes", "subscribed": true, "count": 1,
		})
	})
	mux.HandleFunc("GET /v1/sessions/cs-no/subscribers", func(w http.ResponseWriter, r *http.Request) {
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"session_id": "cs-no", "subscribed": false, "count": 0,
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	m := &MCPServer{
		cfg:       &Config{Mod3URL: srv.URL},
		mod3Proxy: &modalityProxy{disablePlayback: true},
	}

	yes, err := m.checkSessionSubscriber(context.Background(), "cs-yes")
	if err != nil {
		t.Fatalf("cs-yes: %v", err)
	}
	if !yes {
		t.Fatal("cs-yes: expected subscribed=true")
	}

	no, err := m.checkSessionSubscriber(context.Background(), "cs-no")
	if err != nil {
		t.Fatalf("cs-no: %v", err)
	}
	if no {
		t.Fatal("cs-no: expected subscribed=false")
	}
}

// TestCheckSessionSubscriber_Mod3UnreachableReturnsError — transport
// error (connection refused, timeout) must surface as a non-nil error so
// toolMod3Speak records subscriber_check_error and falls back to afplay.
func TestCheckSessionSubscriber_Mod3UnreachableReturnsError(t *testing.T) {
	// Bind a port, close it so dials get ECONNREFUSED.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	m := &MCPServer{
		cfg:       &Config{Mod3URL: "http://" + addr},
		mod3Proxy: &modalityProxy{disablePlayback: true},
	}
	subscribed, err := m.checkSessionSubscriber(context.Background(), "cs-anything")
	if err == nil {
		t.Fatal("expected transport error")
	}
	if subscribed {
		t.Fatal("expected subscribed=false on error")
	}
}

// ─── mod3_tail_logs ──────────────────────────────────────────────────────────

// TestMod3TailLogs_BasicQuery verifies that mod3_tail_logs forwards filter
// params to /v1/logs/chat-flow and returns parseable event structs.
func TestMod3TailLogs_BasicQuery(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := &MCPServer{cfg: &Config{Mod3URL: fm.srv.URL}}

	result, _, err := m.toolMod3TailLogs(context.Background(), nil, mod3TailLogsInput{
		SessionID: "cs-abc123",
		EventType: "chat.message_received",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("toolMod3TailLogs: %v", err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
	if result.IsError {
		t.Fatalf("tool returned IsError=true: %v", result.Content)
	}

	// Verify the HTTP layer received the expected query params.
	last := fm.last()
	if last.Path != "/v1/logs/chat-flow" {
		t.Errorf("path = %q; want /v1/logs/chat-flow", last.Path)
	}
	if !strings.Contains(last.Query, "session_id=cs-abc123") {
		t.Errorf("query %q missing session_id", last.Query)
	}
	if !strings.Contains(last.Query, "event_type=chat.message_received") {
		t.Errorf("query %q missing event_type", last.Query)
	}
	if !strings.Contains(last.Query, "limit=10") {
		t.Errorf("query %q missing limit", last.Query)
	}

	// Verify the result is parseable JSON with the expected "events" field.
	if len(result.Content) == 0 {
		t.Fatal("empty content")
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] is %T, want *mcp.TextContent", result.Content[0])
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &parsed); err != nil {
		t.Fatalf("parse result JSON: %v (raw=%q)", err, tc.Text)
	}
	events, ok := parsed["events"].([]any)
	if !ok {
		t.Fatalf("events field missing or wrong type: %T", parsed["events"])
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev, ok := events[0].(map[string]any)
	if !ok {
		t.Fatalf("event is %T, want map", events[0])
	}
	if got := ev["event_type"]; got != "chat.message_received" {
		t.Errorf("event_type = %q; want chat.message_received", got)
	}
}

// TestMod3TailLogs_DefaultLimit verifies that the default limit (50) is sent
// when the caller supplies 0.
func TestMod3TailLogs_DefaultLimit(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := &MCPServer{cfg: &Config{Mod3URL: fm.srv.URL}}

	_, _, err := m.toolMod3TailLogs(context.Background(), nil, mod3TailLogsInput{})
	if err != nil {
		t.Fatalf("toolMod3TailLogs: %v", err)
	}
	last := fm.last()
	if !strings.Contains(last.Query, "limit=50") {
		t.Errorf("query %q: want default limit=50", last.Query)
	}
}

// TestMod3TailLogs_LimitCap verifies that limit is capped at 500.
func TestMod3TailLogs_LimitCap(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := &MCPServer{cfg: &Config{Mod3URL: fm.srv.URL}}

	_, _, err := m.toolMod3TailLogs(context.Background(), nil, mod3TailLogsInput{Limit: 9999})
	if err != nil {
		t.Fatalf("toolMod3TailLogs: %v", err)
	}
	last := fm.last()
	if !strings.Contains(last.Query, "limit=500") {
		t.Errorf("query %q: want capped limit=500", last.Query)
	}
}

// TestMod3TailLogs_RelativeSince verifies that a relative "since" string like
// "5m" is resolved to an ISO timestamp before forwarding to mod3.
func TestMod3TailLogs_RelativeSince(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := &MCPServer{cfg: &Config{Mod3URL: fm.srv.URL}}

	before := time.Now().UTC().Add(-6 * time.Minute)
	after := time.Now().UTC().Add(-4 * time.Minute)

	_, _, err := m.toolMod3TailLogs(context.Background(), nil, mod3TailLogsInput{Since: "5m"})
	if err != nil {
		t.Fatalf("toolMod3TailLogs: %v", err)
	}
	last := fm.last()
	// since= must be present and be a parseable ISO time in the expected window.
	for _, kv := range strings.Split(last.Query, "&") {
		if !strings.HasPrefix(kv, "since=") {
			continue
		}
		raw, _ := url.QueryUnescape(strings.TrimPrefix(kv, "since="))
		ts, parseErr := time.Parse(time.RFC3339, raw)
		if parseErr != nil {
			t.Fatalf("since= value %q is not valid RFC3339: %v", raw, parseErr)
		}
		if ts.Before(before) || ts.After(after) {
			t.Errorf("since=%v not in expected window [%v, %v]", ts, before, after)
		}
		return
	}
	t.Errorf("since= not found in query: %q", last.Query)
}

// TestMod3TailLogs_Unreachable verifies that an unreachable mod3 returns an
// IsError=true result (not a Go error) so the caller sees a tool-level error.
func TestMod3TailLogs_Unreachable(t *testing.T) {
	m := &MCPServer{cfg: &Config{Mod3URL: "http://127.0.0.1:1"}} // port 1: always ECONNREFUSED
	result, _, err := m.toolMod3TailLogs(context.Background(), nil, mod3TailLogsInput{})
	if err != nil {
		t.Fatalf("unexpected Go error (want IsError result): %v", err)
	}
	if result == nil || !result.IsError {
		t.Error("expected IsError=true when mod3 is unreachable")
	}
}

// writeStubPlayer builds a temp shell script that records each invocation
// by appending to a log file. Returns the path of the executable AND a
// getter that returns the current invocation count. The stub exits
// immediately so blocking=true still works in tests.
func writeStubPlayer(t *testing.T) (stubPath string, count func() int) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "invocations.log")
	stubPath = filepath.Join(dir, "stub-player.sh")
	stubBody := `#!/bin/sh
echo "invoked" >> "` + logPath + `"
`
	if err := os.WriteFile(stubPath, []byte(stubBody), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	count = func() int {
		data, err := os.ReadFile(logPath)
		if err != nil {
			return 0
		}
		return strings.Count(string(data), "invoked\n")
	}
	return stubPath, count
}

// ─── format field propagation ─────────────────────────────────────────────────

// TestBuildSynthesizeBody_FormatOgg verifies that buildSynthesizeBody
// includes a "format" key when mod3SpeakInput.Format is set.
func TestBuildSynthesizeBody_FormatOgg(t *testing.T) {
	in := mod3SpeakInput{Text: "hello", Format: "ogg"}
	body := buildSynthesizeBody(in)
	if got, ok := body["format"]; !ok || got != "ogg" {
		t.Fatalf("expected format=ogg in synthesize body, got %v (present=%v)", got, ok)
	}
}

// TestBuildSynthesizeBody_FormatEmpty verifies that buildSynthesizeBody
// omits the "format" key when Format is empty.
func TestBuildSynthesizeBody_FormatEmpty(t *testing.T) {
	in := mod3SpeakInput{Text: "hello"}
	body := buildSynthesizeBody(in)
	if _, ok := body["format"]; ok {
		t.Fatal("expected format key absent when Format is empty")
	}
}

// TestBuildSpeakBody_FormatOgg verifies that buildSpeakBody (queue path)
// includes a "format" key when mod3SpeakInput.Format is set.
func TestBuildSpeakBody_FormatOgg(t *testing.T) {
	in := mod3SpeakInput{Text: "hello", Format: "ogg"}
	body := buildSpeakBody(in)
	if got, ok := body["format"]; !ok || got != "ogg" {
		t.Fatalf("expected format=ogg in speak body, got %v (present=%v)", got, ok)
	}
}

// TestBuildSpeakBody_FormatEmpty verifies that buildSpeakBody omits the
// "format" key when Format is empty.
func TestBuildSpeakBody_FormatEmpty(t *testing.T) {
	in := mod3SpeakInput{Text: "hello"}
	body := buildSpeakBody(in)
	if _, ok := body["format"]; ok {
		t.Fatal("expected format key absent when Format is empty")
	}
}

// TestMod3Speak_SkipPlayback_FormatOgg_SentToSynthesize verifies that when
// format="ogg" and skip_playback=true the request body forwarded to mod3's
// /v1/synthesize contains {"format": "ogg"}.
func TestMod3Speak_SkipPlayback_FormatOgg_SentToSynthesize(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	_, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:         "hello ogg",
		SkipPlayback: true,
		Format:       "ogg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	last := fm.last()
	if last.Path != "/v1/synthesize" {
		t.Fatalf("expected /v1/synthesize, got %q", last.Path)
	}
	var got map[string]any
	if err := json.Unmarshal(last.Body, &got); err != nil {
		t.Fatalf("decode captured body: %v", err)
	}
	if got["format"] != "ogg" {
		t.Fatalf("expected format=ogg in forwarded body, got %v", got["format"])
	}
}

// TestMod3Speak_SkipPlayback_FormatAbsent_NoKeyInSynthesize verifies that
// format is not forwarded when skip_playback=true and Format is unset.
func TestMod3Speak_SkipPlayback_FormatAbsent_NoKeyInSynthesize(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	_, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:         "no format",
		SkipPlayback: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	last := fm.last()
	var got map[string]any
	if err := json.Unmarshal(last.Body, &got); err != nil {
		t.Fatalf("decode captured body: %v", err)
	}
	if _, ok := got["format"]; ok {
		t.Fatalf("expected format key absent in forwarded body, got %v", got["format"])
	}
}

// TestMod3Speak_QueuePath_FormatOgg_SentToSpeak verifies that when
// format="ogg" on the primary queue path the request body forwarded to mod3's
// /v1/speak contains {"format": "ogg"}.
func TestMod3Speak_QueuePath_FormatOgg_SentToSpeak(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	_, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:   "hello ogg queue",
		Format: "ogg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	last := fm.last()
	if last.Path != "/v1/speak" {
		t.Fatalf("expected /v1/speak, got %q", last.Path)
	}
	var got map[string]any
	if err := json.Unmarshal(last.Body, &got); err != nil {
		t.Fatalf("decode captured body: %v", err)
	}
	if got["format"] != "ogg" {
		t.Fatalf("expected format=ogg in forwarded speak body, got %v", got["format"])
	}
}

// TestMod3Speak_OutputPath_WritesFileAndReturnsPath verifies that when
// output_path is set together with skip_playback=true the audio is written
// to disk and the response contains {ok, path, bytes} with no audio_base64.
func TestMod3Speak_OutputPath_WritesFileAndReturnsPath(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	tmpFile := t.TempDir() + "/reply.ogg"

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:         "write to disk",
		SkipPlayback: true,
		Format:       "ogg",
		OutputPath:   tmpFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error: %v", decodeToolText(t, res))
	}

	out := decodeToolText(t, res)

	// Must have ok=true, path=tmpFile, bytes>0.
	if ok, _ := out["ok"].(bool); !ok {
		t.Fatalf("expected ok=true, got %v", out["ok"])
	}
	if p, _ := out["path"].(string); p != tmpFile {
		t.Fatalf("expected path=%q, got %q", tmpFile, p)
	}
	if b, _ := out["bytes"].(float64); b <= 0 {
		t.Fatalf("expected bytes>0, got %v", out["bytes"])
	}

	// Must NOT include audio_base64 (that's the whole point).
	if _, present := out["audio_base64"]; present {
		t.Fatal("audio_base64 must be absent when output_path is set")
	}

	// Verify the file exists on disk and has the expected byte count.
	data, readErr := os.ReadFile(tmpFile)
	if readErr != nil {
		t.Fatalf("file not written: %v", readErr)
	}
	if bytesField, _ := out["bytes"].(float64); int(bytesField) != len(data) {
		t.Fatalf("bytes field %v != file size %d", out["bytes"], len(data))
	}
}

// TestMod3Speak_OutputPath_WriteError_ReturnsToolError verifies that a
// write failure (unwritable path) is surfaced as a tool error, not a Go error.
func TestMod3Speak_OutputPath_WriteError_ReturnsToolError(t *testing.T) {
	fm := newFakeMod3Proxy(t)
	m := newProxyMCP(t, fm)

	// Use an obviously unwritable path: a nonexistent nested dir.
	badPath := t.TempDir() + "/no/such/dir/reply.ogg"

	res, _, err := m.toolMod3Speak(context.Background(), nil, mod3SpeakInput{
		Text:         "bad path",
		SkipPlayback: true,
		OutputPath:   badPath,
	})
	if err != nil {
		t.Fatalf("unexpected Go error (should be tool error): %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError=true for unwritable output_path")
	}
}
