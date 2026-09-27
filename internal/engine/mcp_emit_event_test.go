// mcp_emit_event_test.go — unit tests for the cog_emit_event MCP tool.
//
// Covers:
//   - Existing four event types still accepted (regression guard).
//   - peer.utterance: valid and all rejection cases.
//   - from_session: recorded correctly; rejection on unregistered session;
//     rejection on mismatch with payload.from.
//   - Unknown event type is rejected.
//   - Payload coercion (issue #492): string-encoded JSON object payloads
//     from local-model tool calls are parsed and accepted; invalid strings
//     and non-object JSON (array/scalar) are rejected with a clear error.
package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// makeServerWithSessions creates an MCPServer with a live Process and a
// pre-wired session registry. Returns the server and registry so tests can
// seed sessions before calling toolEmitEvent.
func makeServerWithSessions(t *testing.T) (*MCPServer, *SessionRegistry) {
	t.Helper()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewMCPServer(cfg, makeNucleus("Cog", "tester"), process)

	bus := NewBusSessionManager(root)
	registry := NewSessionRegistry()
	handoffs := NewHandoffRegistry()
	server.SetSessionsBackend(bus, registry, handoffs)

	return server, registry
}

// registerTestSession seeds the registry with a live session for use in tests.
func registerTestSession(t *testing.T, registry *SessionRegistry, sessionID string) {
	t.Helper()
	now := time.Now().UTC()
	state := SessionState{
		SessionID:    sessionID,
		Workspace:    "/tmp/test",
		Role:         "peer-cog",
		RegisteredAt: now,
		LastSeen:     now,
	}
	_, _, err := registry.ApplyRegister(state, defaultActiveWithinSeconds*time.Second, now, nil)
	if err != nil {
		t.Fatalf("registerTestSession %q: %v", sessionID, err)
	}
}

// callEmitEvent is a thin wrapper that calls toolEmitEvent and returns the
// decoded text content of the first result element. If the result is a JSON
// object it is returned as-is; if it is a plain error message string, that
// string is returned. The caller can inspect it for expected substrings.
func callEmitEvent(t *testing.T, server *MCPServer, input emitEventInput) string {
	t.Helper()
	result, _, err := server.toolEmitEvent(context.Background(), nil, input)
	if err != nil {
		t.Fatalf("toolEmitEvent returned Go error (unexpected): %v", err)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatal("toolEmitEvent returned nil/empty result")
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] type = %T; want *mcp.TextContent", result.Content[0])
	}
	return tc.Text
}

// ─── existing event types (regression guard) ─────────────────────────────────

func TestToolEmitEvent_ExistingTypesAccepted(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   emitEventInput
		wantKey string // key expected in JSON response
	}{
		{
			name:    "attention.boost",
			input:   emitEventInput{Type: "attention.boost", Payload: map[string]any{"uri": "cog://mem/semantic/test.cog.md", "weight": 1.5}},
			wantKey: "emitted",
		},
		{
			name:    "session.marker",
			input:   emitEventInput{Type: "session.marker", Payload: map[string]any{"label": "checkpoint"}},
			wantKey: "emitted",
		},
		{
			name:    "insight.captured",
			input:   emitEventInput{Type: "insight.captured", Payload: map[string]any{"summary": "test insight", "tags": []string{"test"}}},
			wantKey: "emitted",
		},
		{
			name:    "decision.made",
			input:   emitEventInput{Type: "decision.made", Payload: map[string]any{"decision": "use Go", "rationale": "performance"}},
			wantKey: "emitted",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, _ := makeServerWithSessions(t)
			got := callEmitEvent(t, server, tc.input)
			var resp map[string]any
			if err := json.Unmarshal([]byte(got), &resp); err != nil {
				t.Fatalf("response is not JSON: %q", got)
			}
			if _, ok := resp[tc.wantKey]; !ok {
				t.Errorf("response missing key %q; got %v", tc.wantKey, resp)
			}
		})
	}
}

// ─── unknown event type ───────────────────────────────────────────────────────

func TestToolEmitEvent_UnknownTypeRejected(t *testing.T) {
	t.Parallel()
	server, _ := makeServerWithSessions(t)
	got := callEmitEvent(t, server, emitEventInput{Type: "banana.split"})
	if !strings.Contains(got, "unknown event type") {
		t.Errorf("expected 'unknown event type' in response; got %q", got)
	}
}

func TestToolEmitEvent_EmptyTypeRejected(t *testing.T) {
	t.Parallel()
	server, _ := makeServerWithSessions(t)
	got := callEmitEvent(t, server, emitEventInput{Type: ""})
	if !strings.Contains(got, "event type is required") {
		t.Errorf("expected 'event type is required' in response; got %q", got)
	}
}

// ─── peer.utterance: valid ───────────────────────────────────────────────────

func TestToolEmitEvent_PeerUtteranceValid(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a",
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "What is the eigenform?",
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	var resp map[string]any
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("expected JSON response; got %q", got)
	}
	if emitted, _ := resp["emitted"].(bool); !emitted {
		t.Errorf("expected emitted=true; got %v", resp)
	}
	if resp["from_session"] != "opus-cog-node-a" {
		t.Errorf("from_session = %v; want opus-cog-node-a", resp["from_session"])
	}
}

// ─── peer.utterance: rejection cases ─────────────────────────────────────────

func TestToolEmitEvent_PeerUtterance_MissingFromSession(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type: "peer.utterance",
		// FromSession intentionally omitted
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "Hello",
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "requires from_session") {
		t.Errorf("expected 'requires from_session' in response; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_UnregisteredFrom(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	// Register only the "to" session; "from" is not registered.
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a", // not registered
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "Hello",
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "not a registered session") {
		t.Errorf("expected 'not a registered session' in response; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_UnregisteredTo(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	// "to" session not registered

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a",
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b", // not registered
			"content": "Hello",
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "not a registered session") {
		t.Errorf("expected 'not a registered session' in response; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_FromSessionMismatch(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")
	registerTestSession(t, registry, "haiku-cog-aux")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "haiku-cog-aux", // differs from payload.from
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "Hello",
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "must match payload.from") {
		t.Errorf("expected 'must match payload.from' in response; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_EmptyContent(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a",
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "", // empty
			"turn":    float64(1),
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "non-empty 'content'") {
		t.Errorf("expected content error; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_MissingTurn(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a",
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "Hello",
			// turn missing
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "turn") {
		t.Errorf("expected turn error; got %q", got)
	}
}

func TestToolEmitEvent_PeerUtterance_NonPositiveTurn(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")
	registerTestSession(t, registry, "gemma-cog-node-b")

	input := emitEventInput{
		Type:        "peer.utterance",
		FromSession: "opus-cog-node-a",
		Payload: map[string]any{
			"from":    "opus-cog-node-a",
			"to":      "gemma-cog-node-b",
			"content": "Hello",
			"turn":    float64(0), // invalid: must be >= 1
		},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "positive integer") {
		t.Errorf("expected positive integer error; got %q", got)
	}
}

// ─── from_session: other event types ─────────────────────────────────────────

func TestToolEmitEvent_FromSession_RecordedOnInsightCaptured(t *testing.T) {
	t.Parallel()
	server, registry := makeServerWithSessions(t)
	registerTestSession(t, registry, "opus-cog-node-a")

	input := emitEventInput{
		Type:        "insight.captured",
		FromSession: "opus-cog-node-a",
		Payload:     map[string]any{"summary": "binding event is the smallest unit of maintenance"},
	}
	got := callEmitEvent(t, server, input)
	var resp map[string]any
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("expected JSON response; got %q", got)
	}
	if resp["from_session"] != "opus-cog-node-a" {
		t.Errorf("from_session = %v; want opus-cog-node-a", resp["from_session"])
	}
	if emitted, _ := resp["emitted"].(bool); !emitted {
		t.Errorf("emitted = %v; want true", resp["emitted"])
	}
}

func TestToolEmitEvent_FromSession_UnregisteredSessionRejected(t *testing.T) {
	t.Parallel()
	server, _ := makeServerWithSessions(t)
	// Registry is empty — no sessions registered.

	input := emitEventInput{
		Type:        "session.marker",
		FromSession: "opus-cog-node-a", // not registered
		Payload:     map[string]any{"label": "checkpoint"},
	}
	got := callEmitEvent(t, server, input)
	if !strings.Contains(got, "not a registered session") {
		t.Errorf("expected 'not a registered session' in response; got %q", got)
	}
}

func TestToolEmitEvent_FromSession_OmittedKeepsLegacyBehavior(t *testing.T) {
	t.Parallel()
	server, _ := makeServerWithSessions(t)

	input := emitEventInput{
		Type:    "session.marker",
		Payload: map[string]any{"label": "checkpoint"},
	}
	got := callEmitEvent(t, server, input)
	var resp map[string]any
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("expected JSON response; got %q", got)
	}
	if emitted, _ := resp["emitted"].(bool); !emitted {
		t.Errorf("emitted = %v; want true", resp["emitted"])
	}
	if _, has := resp["from_session"]; has {
		t.Errorf("from_session should be absent when not provided; got %v", resp["from_session"])
	}
}

// ─── payload coercion (issue #492) ───────────────────────────────────────────
//
// Local-model tool-call plumbing (LM Studio-served models observed on
// a peer node's example-35b) stringifies nested object arguments even when
// shown the object form. These tests exercise the same code path the local
// harness uses — json.Unmarshal of raw tool-call argument bytes into
// emitEventInput — rather than constructing emitEventInput literals, since
// the bug lives in that unmarshal step (see emitEventInput.UnmarshalJSON).

func TestEmitEventInput_PayloadCoercion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		args        string
		wantErr     bool
		wantErrSub  string // substring expected in the error, when wantErr
		wantPayload map[string]any
	}{
		{
			name:        "object passes through unchanged",
			args:        `{"type":"attention.boost","payload":{"uri":"cog://mem/x","weight":1.5}}`,
			wantPayload: map[string]any{"uri": "cog://mem/x", "weight": 1.5},
		},
		{
			name:        "stringified object coerces",
			args:        `{"type":"attention.boost","payload":"{\"uri\":\"cog://mem/x\",\"weight\":1.5}"}`,
			wantPayload: map[string]any{"uri": "cog://mem/x", "weight": 1.5},
		},
		{
			name:       "invalid JSON string errors cleanly",
			args:       `{"type":"attention.boost","payload":"not json at all"}`,
			wantErr:    true,
			wantErrSub: "must contain a JSON object",
		},
		{
			name:       "stringified array errors cleanly (non-object JSON, per spec)",
			args:       `{"type":"attention.boost","payload":"[1,2,3]"}`,
			wantErr:    true,
			wantErrSub: "must contain a JSON object",
		},
		{
			name:       "direct JSON array is rejected, not coerced",
			args:       `{"type":"attention.boost","payload":[1,2,3]}`,
			wantErr:    true,
			wantErrSub: "an array",
		},
		{
			name:       "direct JSON scalar is rejected, not coerced",
			args:       `{"type":"attention.boost","payload":42}`,
			wantErr:    true,
			wantErrSub: "a number",
		},
		{
			name:       "direct JSON bool is rejected, not coerced",
			args:       `{"type":"attention.boost","payload":true}`,
			wantErr:    true,
			wantErrSub: "a boolean",
		},
		{
			name:        "payload omitted leaves nil, no error",
			args:        `{"type":"session.marker"}`,
			wantPayload: nil,
		},
		{
			name:        "explicit JSON null leaves nil, no error",
			args:        `{"type":"session.marker","payload":null}`,
			wantPayload: nil,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var input emitEventInput
			err := json.Unmarshal([]byte(tc.args), &input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none (payload=%v)", input.Payload)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Errorf("error = %q; want substring %q", err.Error(), tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tc.wantPayload) == 0 && len(input.Payload) == 0 {
				return // both nil/empty, treat as equal
			}
			if !reflect.DeepEqual(input.Payload, tc.wantPayload) {
				t.Errorf("payload = %#v; want %#v", input.Payload, tc.wantPayload)
			}
		})
	}
}

// TestToolEmitEvent_StringifiedPayloadEndToEnd reproduces the issue #492
// repro path in full: raw tool-call argument bytes (payload string-encoded,
// as a dispatched local model emits it) unmarshal into emitEventInput and
// the resulting event still emits successfully.
func TestToolEmitEvent_StringifiedPayloadEndToEnd(t *testing.T) {
	t.Parallel()
	server, _ := makeServerWithSessions(t)

	args := `{"type":"insight.captured","payload":"{\"summary\":\"stringified payloads now coerce\",\"tags\":[\"492\"]}"}`
	var input emitEventInput
	if err := json.Unmarshal([]byte(args), &input); err != nil {
		t.Fatalf("unmarshal tool arguments: %v", err)
	}

	got := callEmitEvent(t, server, input)
	var resp map[string]any
	if err := json.Unmarshal([]byte(got), &resp); err != nil {
		t.Fatalf("expected JSON response; got %q", got)
	}
	if emitted, _ := resp["emitted"].(bool); !emitted {
		t.Errorf("expected emitted=true; got %v", resp)
	}
}
