package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ── Emission tests ─────────────────────────────────────────────────────────

func TestEmitToolCallWritesLedger(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	p.emitToolCall(ToolCallEvent{
		CallID:    "call-1",
		ToolName:  "cog_read_cogdoc",
		Arguments: json.RawMessage(`{"uri":"cog://mem/x"}`),
		Source:    ToolSourceMCP,
		Ownership: ToolOwnershipKernel,
	})

	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 1 {
		t.Fatalf("event count = %d; want 1", len(events))
	}
	ev := events[0]
	if ev.HashedPayload.Type != "tool.call" {
		t.Errorf("type = %q; want tool.call", ev.HashedPayload.Type)
	}
	if got := ev.HashedPayload.Data["call_id"]; got != "call-1" {
		t.Errorf("call_id = %v; want call-1", got)
	}
	if got := ev.HashedPayload.Data["tool_name"]; got != "cog_read_cogdoc" {
		t.Errorf("tool_name = %v; want cog_read_cogdoc", got)
	}
	if got := ev.HashedPayload.Data["source"]; got != ToolSourceMCP {
		t.Errorf("source = %v; want %s", got, ToolSourceMCP)
	}
	if got := ev.HashedPayload.Data["ownership"]; got != ToolOwnershipKernel {
		t.Errorf("ownership = %v; want %s", got, ToolOwnershipKernel)
	}
}

func TestEmitToolResultWritesLedger(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	p.emitToolResult(ToolResultEvent{
		CallID:        "call-1",
		ToolName:      "cog_read_cogdoc",
		Status:        ToolStatusSuccess,
		OutputLength:  42,
		OutputSummary: "hello world",
		Duration:      125 * time.Millisecond,
		Source:        ToolSourceMCP,
	})

	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 1 {
		t.Fatalf("event count = %d; want 1", len(events))
	}
	ev := events[0]
	if ev.HashedPayload.Type != "tool.result" {
		t.Errorf("type = %q; want tool.result", ev.HashedPayload.Type)
	}
	if got := ev.HashedPayload.Data["status"]; got != ToolStatusSuccess {
		t.Errorf("status = %v; want success", got)
	}
	// Numeric fields come back as float64 after JSON round-trip.
	if got := asInt(ev.HashedPayload.Data["duration_ms"]); got != 125 {
		t.Errorf("duration_ms = %d; want 125", got)
	}
	if got := ev.HashedPayload.Data["output_summary"]; got != "hello world" {
		t.Errorf("output_summary = %v; want hello world", got)
	}
}

func TestWithToolObserverWrapsHandler(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))
	m := NewMCPServer(cfg, makeNucleus("Cog", "tester"), p)

	type fakeIn struct {
		Foo string `json:"foo"`
	}
	var invoked bool
	h := func(ctx context.Context, req *mcp.CallToolRequest, in fakeIn) (*mcp.CallToolResult, any, error) {
		invoked = true
		if in.Foo != "bar" {
			t.Errorf("handler Foo = %q; want bar", in.Foo)
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "ok"}},
		}, nil, nil
	}
	wrapped := withToolObserver(m, "fake_tool", h)
	result, _, err := wrapped(context.Background(), nil, fakeIn{Foo: "bar"})
	if err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	if !invoked {
		t.Fatal("handler not invoked")
	}
	if len(result.Content) != 1 {
		t.Errorf("result content len = %d; want 1", len(result.Content))
	}

	// Expected events: cogblock.ingest (MCP CogBlock normalization, §2.2),
	// then tool.call, then tool.result. The tool.* pair shares a call_id.
	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 3 {
		t.Fatalf("event count = %d; want 3 (cogblock.ingest + tool.call + tool.result)", len(events))
	}
	if events[0].HashedPayload.Type != "cogblock.ingest" {
		t.Errorf("event[0].type = %q; want cogblock.ingest", events[0].HashedPayload.Type)
	}
	if events[1].HashedPayload.Type != "tool.call" {
		t.Errorf("event[1].type = %q; want tool.call", events[1].HashedPayload.Type)
	}
	if events[2].HashedPayload.Type != "tool.result" {
		t.Errorf("event[2].type = %q; want tool.result", events[2].HashedPayload.Type)
	}
	if events[1].HashedPayload.Data["call_id"] != events[2].HashedPayload.Data["call_id"] {
		t.Errorf("call_ids differ between call and result")
	}
	if events[2].HashedPayload.Data["status"] != ToolStatusSuccess {
		t.Errorf("status = %v; want success", events[2].HashedPayload.Data["status"])
	}
	// The tool.call's interaction_id should reference the cogblock.ingest block.
	cogBlockID := events[0].HashedPayload.Data["block_id"]
	if cogBlockID == nil || cogBlockID == "" {
		t.Error("cogblock.ingest missing block_id")
	}
	if events[1].HashedPayload.Data["interaction_id"] != cogBlockID {
		t.Errorf("interaction_id = %v; want cogblock block_id %v",
			events[1].HashedPayload.Data["interaction_id"], cogBlockID)
	}
}

func TestWithToolObserverCapturesError(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))
	m := NewMCPServer(cfg, makeNucleus("Cog", "tester"), p)

	type fakeIn struct{}
	handlerErr := errors.New("kaboom")
	h := func(ctx context.Context, req *mcp.CallToolRequest, in fakeIn) (*mcp.CallToolResult, any, error) {
		return nil, nil, handlerErr
	}
	wrapped := withToolObserver(m, "failing_tool", h)
	_, _, err := wrapped(context.Background(), nil, fakeIn{})
	if err == nil || err.Error() != "kaboom" {
		t.Fatalf("err = %v; want kaboom", err)
	}
	// cogblock.ingest + tool.call + tool.result
	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 3 {
		t.Fatalf("event count = %d; want 3", len(events))
	}
	result := events[2]
	if result.HashedPayload.Type != "tool.result" {
		t.Fatalf("last event type = %q; want tool.result", result.HashedPayload.Type)
	}
	if result.HashedPayload.Data["status"] != ToolStatusError {
		t.Errorf("status = %v; want error", result.HashedPayload.Data["status"])
	}
	if result.HashedPayload.Data["reason"] != "kaboom" {
		t.Errorf("reason = %v; want kaboom", result.HashedPayload.Data["reason"])
	}
}

func TestWithToolObserverCapturesIsErrorResult(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))
	m := NewMCPServer(cfg, makeNucleus("Cog", "tester"), p)

	// Handler returns nil-error but IsError=true (the fallbackResult pattern).
	type fakeIn struct{}
	h := func(ctx context.Context, req *mcp.CallToolRequest, in fakeIn) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "kernel unavailable"}},
			IsError: true,
		}, nil, nil
	}
	wrapped := withToolObserver(m, "degraded_tool", h)
	_, _, err := wrapped(context.Background(), nil, fakeIn{})
	if err != nil {
		t.Fatalf("wrapped err = %v; want nil", err)
	}
	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 3 {
		t.Fatalf("event count = %d; want 3", len(events))
	}
	if events[2].HashedPayload.Data["status"] != ToolStatusError {
		t.Errorf("status = %v; want error", events[2].HashedPayload.Data["status"])
	}
}

// ── Pending-call correlation cache tests ──────────────────────────────────

func TestRegisterPendingToolCall(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	p.registerPendingToolCall("call-1", "search", ToolSourceOpenAI, 0)
	p.registerPendingToolCall("call-2", "edit", ToolSourceOpenAI, 0)

	if got := p.pendingToolCalls.len(); got != 2 {
		t.Errorf("pendingToolCalls len = %d; want 2", got)
	}

	// Re-registering the same call_id should replace the entry (idempotent).
	p.registerPendingToolCall("call-1", "search", ToolSourceOpenAI, 0)
	if got := p.pendingToolCalls.len(); got != 2 {
		t.Errorf("after re-register len = %d; want 2", got)
	}
}

func TestMatchAndResolvePendingSuccess(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	p.registerPendingToolCall("call-X", "lookup", ToolSourceOpenAI, 0)
	if ok := p.resolvePendingToolCall("call-X", "result-body"); !ok {
		t.Fatal("resolvePendingToolCall(call-X) returned false; want true")
	}
	if got := p.pendingToolCalls.len(); got != 0 {
		t.Errorf("after resolve len = %d; want 0", got)
	}
	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 1 {
		t.Fatalf("event count = %d; want 1", len(events))
	}
	if events[0].HashedPayload.Type != "tool.result" {
		t.Errorf("type = %q; want tool.result", events[0].HashedPayload.Type)
	}
	if events[0].HashedPayload.Data["status"] != ToolStatusSuccess {
		t.Errorf("status = %v; want success", events[0].HashedPayload.Data["status"])
	}
}

func TestMatchAndResolvePendingMiss(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	if ok := p.resolvePendingToolCall("never-registered", "x"); ok {
		t.Fatal("resolvePendingToolCall returned true for unknown id; want false")
	}

	// A miss must not emit any events. The ledger file may not exist yet;
	// tolerate ENOENT, but any existing file must be empty.
	path := filepath.Join(root, ".cog", "ledger", p.SessionID(), "events.jsonl")
	if _, err := os.Stat(path); err == nil {
		events := mustReadAllEvents(t, root, p.SessionID())
		if len(events) != 0 {
			t.Errorf("events after miss = %d; want 0", len(events))
		}
	}
}

func TestSweepPendingToolCallsTimeout(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	p.registerPendingToolCall("expired-call", "search", ToolSourceOpenAI, 0)

	// Forcibly age the entry past the TTL.
	p.pendingToolCalls.mu.Lock()
	p.pendingToolCalls.entries["expired-call"].EmittedAt = time.Now().Add(-pendingToolCallTTL - time.Minute)
	p.pendingToolCalls.mu.Unlock()

	p.sweepPendingToolCalls()

	if got := p.pendingToolCalls.len(); got != 0 {
		t.Errorf("after sweep len = %d; want 0", got)
	}
	events := mustReadAllEvents(t, root, p.SessionID())
	if len(events) != 1 {
		t.Fatalf("event count = %d; want 1", len(events))
	}
	if events[0].HashedPayload.Data["status"] != ToolStatusTimeout {
		t.Errorf("status = %v; want timeout", events[0].HashedPayload.Data["status"])
	}
}

func TestPendingToolCallCapEvictsOldest(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	// Fill the cache to capacity with slightly-staggered timestamps so the
	// oldest-first eviction is deterministic.
	for i := 0; i < pendingToolCallMaxEntries; i++ {
		id := fmt.Sprintf("call-%d", i)
		p.registerPendingToolCall(id, "search", ToolSourceOpenAI, 0)
	}
	// Manually advance the "oldest" entry's EmittedAt so the eviction pass
	// removes call-0 deterministically.
	p.pendingToolCalls.mu.Lock()
	p.pendingToolCalls.entries["call-0"].EmittedAt = time.Now().Add(-time.Hour)
	p.pendingToolCalls.mu.Unlock()

	// One more entry triggers an eviction.
	p.registerPendingToolCall("call-new", "search", ToolSourceOpenAI, 0)

	// call-0 should be evicted; call-new should be present.
	if got := p.pendingToolCalls.len(); got != pendingToolCallMaxEntries {
		t.Errorf("after overflow len = %d; want %d", got, pendingToolCallMaxEntries)
	}
	p.pendingToolCalls.mu.Lock()
	_, stillThere := p.pendingToolCalls.entries["call-0"]
	_, newOneThere := p.pendingToolCalls.entries["call-new"]
	p.pendingToolCalls.mu.Unlock()
	if stillThere {
		t.Error("call-0 should have been evicted")
	}
	if !newOneThere {
		t.Error("call-new should have been added")
	}

	// An eviction emits a timeout tool.result (with "evicted" reason).
	events := mustReadAllEvents(t, root, p.SessionID())
	var sawTimeout bool
	for _, ev := range events {
		if ev.HashedPayload.Type == "tool.result" && ev.HashedPayload.Data["status"] == ToolStatusTimeout {
			sawTimeout = true
			reason := asString(ev.HashedPayload.Data["reason"])
			if reason == "" {
				t.Error("evicted tool.result missing reason")
			}
			break
		}
	}
	if !sawTimeout {
		t.Error("eviction did not produce a timeout tool.result event")
	}
}

// TestMCPToolInvocationRecordsBlock guards §2.2 of the Agent S design —
// every MCP tool call should produce a cogblock.ingest event alongside the
// tool.call/tool.result pair, activating the dormant NormalizeMCPRequest
// path that has been sitting unused since introduction.
func TestMCPToolInvocationRecordsBlock(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))
	m := NewMCPServer(cfg, makeNucleus("Cog", "tester"), p)

	type fakeIn struct{}
	h := func(ctx context.Context, req *mcp.CallToolRequest, in fakeIn) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil, nil
	}
	wrapped := withToolObserver(m, "cog_probe", h)
	if _, _, err := wrapped(context.Background(), nil, fakeIn{}); err != nil {
		t.Fatalf("wrapped: %v", err)
	}

	events := mustReadAllEvents(t, root, p.SessionID())
	var sawCogBlock bool
	for _, ev := range events {
		if ev.HashedPayload.Type == "cogblock.ingest" {
			sawCogBlock = true
			// The block kind should be the tool-call variant.
			if ev.HashedPayload.Data["kind"] != string(BlockToolCall) {
				t.Errorf("cogblock.ingest kind = %v; want %s",
					ev.HashedPayload.Data["kind"], BlockToolCall)
			}
			break
		}
	}
	if !sawCogBlock {
		t.Error("no cogblock.ingest event emitted for MCP tool invocation")
	}
}

func TestGateRecognizerAcceptsEmittedEvents(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	// Inject a tool.call gate event directly — this exercises the gate
	// recognizer at gate.go:94, which should now actually see events because
	// the emission paths above are producing them.
	result := p.gate.Process(&GateEvent{Type: "tool.call"})
	if result.StateTransition != StateActive {
		t.Errorf("tool.call gate transition = %v; want StateActive", result.StateTransition)
	}
	result = p.gate.Process(&GateEvent{Type: "tool.result"})
	if result.StateTransition != StateActive {
		t.Errorf("tool.result gate transition = %v; want StateActive", result.StateTransition)
	}
}

// TestMCPCogReadToolCallsRoundTrip exercises the full MCP handler path:
// register observable handlers, invoke one, then invoke cog_read_tool_calls
// via its own handler and verify the result includes the earlier call.
func TestMCPCogReadToolCallsRoundTrip(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))
	m := NewMCPServer(cfg, makeNucleus("Cog", "tester"), p)

	// Trigger one observable tool invocation (direct call through wrapper).
	wrapped := withToolObserver(m, "probe_tool", func(ctx context.Context, req *mcp.CallToolRequest, in struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil, nil
	})
	_, _, err := wrapped(context.Background(), nil, struct{}{})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}

	// Now invoke cog_read_tool_calls filter-free.
	result, _, err := m.toolReadToolCalls(context.Background(), nil, readToolCallsInput{})
	if err != nil {
		t.Fatalf("toolReadToolCalls: %v", err)
	}
	var decoded ToolCallQueryResult
	decodeMCPJSON(t, result, &decoded)
	if decoded.Count < 1 {
		t.Fatalf("count = %d; want >= 1", decoded.Count)
	}
	// The probe row should appear; subsequent cog_read_tool_calls invocation
	// is itself observable and will also emit a pair — hence >= 1.
	var sawProbe bool
	for _, r := range decoded.Calls {
		if r.ToolName == "probe_tool" {
			sawProbe = true
			break
		}
	}
	if !sawProbe {
		t.Error("probe_tool row not found in results")
	}
}

// TestHandleToolCallsHTTP verifies GET /v1/tool-calls returns the stitched
// JSON shape. No inference router is wired; we only exercise the read path.
func TestHandleToolCallsHTTP(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.Port = 0
	p := NewProcess(cfg, makeNucleus("Cog", "tester"))

	// Seed a call+result pair via the emission path.
	p.emitToolCall(ToolCallEvent{
		CallID:    "http-probe",
		ToolName:  "cog_read_cogdoc",
		Source:    ToolSourceMCP,
		Ownership: ToolOwnershipKernel,
	})
	p.emitToolResult(ToolResultEvent{
		CallID:       "http-probe",
		ToolName:     "cog_read_cogdoc",
		Status:       ToolStatusSuccess,
		OutputLength: 10,
		Duration:     25 * time.Millisecond,
		Source:       ToolSourceMCP,
	})

	server := NewServer(cfg, makeNucleus("Cog", "tester"), p)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/tool-calls?limit=10")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
	var decoded ToolCallQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Count != 1 {
		t.Fatalf("count = %d; want 1", decoded.Count)
	}
	if decoded.Calls[0].CallID != "http-probe" {
		t.Errorf("call_id = %q; want http-probe", decoded.Calls[0].CallID)
	}
	if decoded.Calls[0].Status != ToolStatusSuccess {
		t.Errorf("status = %q; want success", decoded.Calls[0].Status)
	}
}
