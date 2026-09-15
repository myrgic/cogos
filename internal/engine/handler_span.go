// handler_span.go — per-handler span instrumentation for the kernel HTTP surface.
//
// Every handler registered via Server.route (or Server.routeH) is automatically
// wrapped by withSpan, which:
//
//   - Captures start time, HTTP method, path, session-id header, and origin.
//   - Intercepts the response via a capturing ResponseWriter so it can record
//     the status code and response byte count without buffering the body.
//   - Emits a KernelHandlerSpan to the bus_traces bus when the handler returns.
//
// Wire contract (bus channel: bus_traces, block type: kernel.handler.span.v1):
//
//	The KernelHandlerSpan struct is the stable payload shape. The bus_traces
//	channel bridge (RFC-003, deferred) will surface these as <channel> tags.
//	Fields are chosen for observability without leaking sensitive content:
//	no request body, no response body, no full header dump.
//
// SSE and streaming handlers will still emit a span — the span fires on
// handler return, which for streaming responses is when the stream is fully
// drained. Duration therefore reflects the full streaming wall-clock time,
// which is the correct metric for throughput observability.
package engine

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// BusTraces is the well-known bus channel ID for handler span events.
// Mirrors the naming convention in sessions.go (BusSessions, BusHandoffs).
const BusTraces = "bus_traces"

// KernelHandlerSpan is the wire contract for a single handler invocation.
// It is serialised as the payload of a bus_traces event with
// BlockType = "kernel.handler.span.v1".
//
// Sensitive content is explicitly excluded: no request body, no response body,
// no full header dump. SpanID is a UUID v4 (not ULID — ulid is not in go.mod;
// UUID is already imported and provides equivalent uniqueness for this use).
type KernelHandlerSpan struct {
	SpanID       string            `json:"span_id"`
	Handler      string            `json:"handler"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	StartedAt    time.Time         `json:"started_at"`
	DurationMS   int64             `json:"duration_ms"`
	Status       int               `json:"status"`
	RequestSize  int               `json:"request_size"`
	ResponseSize int               `json:"response_size"`
	SessionID    string            `json:"session_id,omitempty"`
	Origin       string            `json:"origin"`
	Error        string            `json:"error,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// spanEmitter is the interface used by withSpan to persist spans so tests can
// inject a no-op or a recording emitter without needing a live BusSessionManager.
type spanEmitter interface {
	emitSpan(span KernelHandlerSpan)
}

// serverSpanEmitter forwards spans to the server's busSessions manager.
// It is wired once in NewServer so that withSpan closures stay small.
type serverSpanEmitter struct {
	bus *BusSessionManager
}

func (e *serverSpanEmitter) emitSpan(span KernelHandlerSpan) {
	if e.bus == nil {
		return
	}
	payload := map[string]interface{}{
		"span_id":       span.SpanID,
		"handler":       span.Handler,
		"method":        span.Method,
		"path":          span.Path,
		"started_at":    span.StartedAt.UTC().Format(time.RFC3339Nano),
		"duration_ms":   span.DurationMS,
		"status":        span.Status,
		"request_size":  span.RequestSize,
		"response_size": span.ResponseSize,
		"origin":        span.Origin,
	}
	if span.SessionID != "" {
		payload["session_id"] = span.SessionID
	}
	if span.Error != "" {
		payload["error"] = span.Error
	}
	if len(span.Metadata) > 0 {
		payload["metadata"] = span.Metadata
	}

	// Ensure bus_traces exists before appending. EnsureBus is idempotent and
	// cheap (stat + mkdir) so calling it per-span is safe; it only allocates on
	// the first call for this bus ID.
	if err := e.bus.EnsureBus(BusTraces); err != nil {
		slog.Warn("handler_span: ensure bus_traces failed", "err", err)
		return
	}
	// Register the bus the first time it is touched. RegisterBus is also
	// idempotent — subsequent calls update state to "active".
	if err := e.bus.RegisterBus(BusTraces, "kernel", "kernel"); err != nil {
		slog.Warn("handler_span: register bus_traces failed", "err", err)
	}
	if _, err := e.bus.AppendEvent(BusTraces, "kernel.handler.span.v1", "kernel", payload); err != nil {
		slog.Warn("handler_span: AppendEvent failed", "err", err, "handler", span.Handler)
	}
}

// ChatSubSpan carries timing and token data for one phase of a chat turn.
// Emitted to bus_traces as "kernel.chat.subspan.v1" events so operators can
// reconstruct per-phase latency (prompt_eval, thinking_generation,
// answer_generation, tool_call_resolution) from the bus without a Jaeger
// collector.  All durations are wall-clock milliseconds.
//
// To export to Jaeger, set OTEL_EXPORTER_OTLP_ENDPOINT and run:
//
//	docker run -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
//
// The OTel SDK in telemetry.go will pick it up automatically.
type ChatSubSpan struct {
	SpanName      string    `json:"span_name"`      // e.g. "chat.answer_generation"
	ParentSpanID  string    `json:"parent_span_id"` // outer chat.request handler span
	TraceID       string    `json:"trace_id"`       // W3C trace-id (hex, 32 chars) if available
	StartedAt     time.Time `json:"started_at"`
	DurationMS    int64     `json:"duration_ms"`
	TokensThink   int       `json:"tokens_think,omitempty"`  // reasoning token count (est.)
	TokensAnswer  int       `json:"tokens_answer,omitempty"` // answer token count (est.)
	TokensTotal   int       `json:"tokens_total,omitempty"`  // total completion tokens
	ToolCallCount int       `json:"tool_calls,omitempty"`
	Model         string    `json:"model,omitempty"`
	SessionID     string    `json:"session_id,omitempty"`
}

// emitChatSubSpan writes a ChatSubSpan to bus_traces. It is a no-op if bus
// is nil. Errors are logged at WARN level and never returned — telemetry must
// not disrupt the hot path.
func emitChatSubSpan(bus *BusSessionManager, sub ChatSubSpan) {
	if bus == nil {
		return
	}
	payload := map[string]interface{}{
		"span_name":   sub.SpanName,
		"started_at":  sub.StartedAt.UTC().Format(time.RFC3339Nano),
		"duration_ms": sub.DurationMS,
	}
	if sub.ParentSpanID != "" {
		payload["parent_span_id"] = sub.ParentSpanID
	}
	if sub.TraceID != "" {
		payload["trace_id"] = sub.TraceID
	}
	if sub.TokensThink > 0 {
		payload["tokens_think"] = sub.TokensThink
	}
	if sub.TokensAnswer > 0 {
		payload["tokens_answer"] = sub.TokensAnswer
	}
	if sub.TokensTotal > 0 {
		payload["tokens_total"] = sub.TokensTotal
	}
	if sub.ToolCallCount > 0 {
		payload["tool_calls"] = sub.ToolCallCount
	}
	if sub.Model != "" {
		payload["model"] = sub.Model
	}
	if sub.SessionID != "" {
		payload["session_id"] = sub.SessionID
	}
	if err := bus.EnsureBus(BusTraces); err != nil {
		slog.Warn("chat_subspan: ensure bus_traces failed", "err", err)
		return
	}
	if err := bus.RegisterBus(BusTraces, "kernel", "kernel"); err != nil {
		slog.Warn("chat_subspan: register bus_traces failed", "err", err)
	}
	if _, err := bus.AppendEvent(BusTraces, "kernel.chat.subspan.v1", "kernel", payload); err != nil {
		slog.Warn("chat_subspan: AppendEvent failed", "err", err, "span", sub.SpanName)
	}
}

// capturingResponseWriter wraps http.ResponseWriter to intercept the status
// code and count the bytes written to the response body. It implements
// http.Flusher so SSE handlers keep working: if the underlying writer is a
// Flusher, Flush() is forwarded; otherwise it's a no-op.
//
// It also implements http.Hijacker if the underlying writer supports it, so
// WebSocket upgrade paths are not broken.
type capturingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    atomic.Int64
}

func newCapturingResponseWriter(w http.ResponseWriter) *capturingResponseWriter {
	return &capturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (c *capturingResponseWriter) WriteHeader(code int) {
	c.statusCode = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *capturingResponseWriter) Write(b []byte) (int, error) {
	n, err := c.ResponseWriter.Write(b)
	c.written.Add(int64(n))
	return n, err
}

// Flush forwards to the underlying Flusher if available.
func (c *capturingResponseWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying Hijacker if available (WebSocket support).
func (c *capturingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := c.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("handler_span: underlying ResponseWriter does not implement http.Hijacker")
}

// inferOrigin classifies the caller origin from the request. Priority:
//   - X-CogOS-Origin header if set (e.g. "mcp", "kernel-loop").
//   - Presence of a known MCP path prefix.
//   - Falls back to "http".
func inferOrigin(r *http.Request) string {
	if o := r.Header.Get("X-CogOS-Origin"); o != "" {
		return o
	}
	if strings.HasPrefix(r.URL.Path, "/mcp") {
		return "mcp"
	}
	return "http"
}

// withSpan returns an http.HandlerFunc that wraps h with span instrumentation.
// The emitter receives the completed span after h returns. Span emission is
// synchronous but cheap: it only allocates a payload map and calls
// AppendEvent (file append). The handler's hot path is not affected because
// the span write happens after the response is already on the wire.
func withSpan(handlerName string, h http.HandlerFunc, emit spanEmitter) http.HandlerFunc {
	if emit == nil {
		// Safety: if the emitter is nil (e.g. in tests that don't need spans),
		// return the original handler unmodified.
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cw := newCapturingResponseWriter(w)

		// Best-effort request size: Content-Length if available, 0 otherwise.
		// We do NOT buffer the body — just read the header field.
		reqSize := 0
		if r.ContentLength > 0 {
			reqSize = int(r.ContentLength)
		}

		sessionID := r.Header.Get("X-Session-ID")
		origin := inferOrigin(r)

		h(cw, r)

		span := KernelHandlerSpan{
			SpanID:       uuid.New().String(),
			Handler:      handlerName,
			Method:       r.Method,
			Path:         r.URL.Path,
			StartedAt:    start,
			DurationMS:   time.Since(start).Milliseconds(),
			Status:       cw.statusCode,
			RequestSize:  reqSize,
			ResponseSize: int(cw.written.Load()),
			SessionID:    sessionID,
			Origin:       origin,
		}

		// Emit synchronously. For non-streaming handlers the response is
		// already on the wire (bufio/http.ResponseWriter has flushed) before
		// this line runs, so the bus-append adds no user-visible latency.
		// For streaming handlers (SSE) the handler blocks until the stream
		// is fully drained, so the span is always emitted after the session
		// completes regardless. A goroutine would race with test TempDir
		// cleanup and cause spurious test failures.
		emit.emitSpan(span)
	}
}
