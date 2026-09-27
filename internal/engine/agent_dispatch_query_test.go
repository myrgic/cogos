// agent_dispatch_query_test.go — coverage for the engine-side validation,
// normalization, and controller-availability checks. The concrete dispatcher
// lives in the root package; here we exercise only the contract that engine
// callers can rely on.
package engine

import (
	"context"
	"strings"
	"testing"
)

// fakeAgentDispatcher is a fakeAgentController extension that satisfies
// AgentDispatcher. It records the last DispatchRequest it observed and
// returns a canned batch.
type fakeAgentDispatcher struct {
	fakeAgentController
	lastReq  DispatchRequest
	canned   *DispatchBatchResult
	cannedOk bool
}

func (f *fakeAgentDispatcher) DispatchToHarness(_ context.Context, req DispatchRequest) (*DispatchBatchResult, error) {
	f.lastReq = req
	if f.canned != nil {
		return f.canned, nil
	}
	if !f.cannedOk {
		return nil, &AgentControllerError{Code: "internal", Message: "no canned response set"}
	}
	return &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true}}}, nil
}

func TestQueryDispatchToHarness_NormalizationDefaults(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "do thing"}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query: %v", err)
	}
	got := disp.lastReq
	if got.AgentID != DefaultAgentID {
		t.Errorf("AgentID default not applied, got %q", got.AgentID)
	}
	if got.Model != DispatchModelE4B {
		t.Errorf("Model default not applied, got %q", got.Model)
	}
	// #432: default raised from 30s to dispatchTimeoutDefault (240s) — a 30s
	// ceiling canceled every request under realistic local-model latency
	// (2-4 min under prefill load), and the non-streaming call underneath
	// didn't even abort server-side on that cancel. Assert against the named
	// constant so this test tracks intentional future changes to the default
	// rather than re-pinning a magic number.
	if got.TimeoutSeconds != dispatchTimeoutDefault {
		t.Errorf("TimeoutSeconds default not applied, got %d, want %d", got.TimeoutSeconds, dispatchTimeoutDefault)
	}
	if got.N != 1 {
		t.Errorf("N default not applied, got %d", got.N)
	}
}

func TestQueryDispatchToHarness_ClampsRanges(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "x", N: 99, TimeoutSeconds: 500}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query: %v", err)
	}
	if disp.lastReq.N != 4 {
		t.Errorf("N not clamped to 4, got %d", disp.lastReq.N)
	}
	// Timeouts are no longer silently clamped: in-cap values pass through
	// verbatim; over-cap values are rejected loudly (see the cap tests below).
	if disp.lastReq.TimeoutSeconds != 500 {
		t.Errorf("in-cap TimeoutSeconds altered, got %d, want 500", disp.lastReq.TimeoutSeconds)
	}
}

// TestQueryDispatchToHarness_TimeoutOverDefaultCapFailsLoudly: with no cap
// stamped (TimeoutCapSeconds zero), the built-in default cap
// (dispatchTimeoutCapDefault, 600s) applies, and a request above it is
// REJECTED with invalid_input naming the config key — never silently clamped.
// A caller that asked for 20 minutes and silently got 10 would misread the
// resulting timeout as a task failure. Operator directive 2026-07-04: the cap
// is an operator parameter (dispatch_timeout_cap_seconds), not a hardcoded
// limit.
func TestQueryDispatchToHarness_TimeoutOverDefaultCapFailsLoudly(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "x", TimeoutSeconds: dispatchTimeoutCapDefault + 1}
	_, err := QueryDispatchToHarness(context.Background(), disp, req)
	if err == nil {
		t.Fatal("expected over-cap timeout to be rejected, got nil error")
	}
	ace, ok := err.(*AgentControllerError)
	if !ok || ace.Code != "invalid_input" {
		t.Fatalf("expected invalid_input AgentControllerError, got %T: %v", err, err)
	}
	if !strings.Contains(ace.Message, "dispatch_timeout_cap_seconds") {
		t.Errorf("over-cap error should point at the config key, got %q", ace.Message)
	}
}

// TestQueryDispatchToHarness_ConfiguredCapHonored: a config-stamped cap
// (TimeoutCapSeconds, from dispatch_timeout_cap_seconds) is the effective
// ceiling in both directions — it admits requests the default cap would
// reject, and rejects requests the default cap would admit.
func TestQueryDispatchToHarness_ConfiguredCapHonored(t *testing.T) {
	// Raised cap: 900s passes under a 1200s configured cap (default would reject).
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "x", TimeoutSeconds: 900, TimeoutCapSeconds: 1200}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query with raised cap: %v", err)
	}
	if disp.lastReq.TimeoutSeconds != 900 {
		t.Errorf("TimeoutSeconds altered, got %d, want 900", disp.lastReq.TimeoutSeconds)
	}
	// Tightened cap: 400s is rejected under a 300s configured cap (default
	// would admit it).
	disp2 := &fakeAgentDispatcher{cannedOk: true}
	req2 := DispatchRequest{Task: "x", TimeoutSeconds: 400, TimeoutCapSeconds: 300}
	if _, err := QueryDispatchToHarness(context.Background(), disp2, req2); err == nil {
		t.Fatal("expected 400s to be rejected under a 300s configured cap")
	}
}

// TestQueryDispatchToHarness_ExplicitModelPreservedAsRequestedModel covers
// issue #430: a model string that isn't one of the recognized routing enum
// values ("", "e4b", "26b") must be preserved as the caller's explicit
// request, not silently collapsed to "e4b". Prior behavior (pre-#430) lost
// the caller's intent right here, before routing ever saw it.
func TestQueryDispatchToHarness_ExplicitModelPreservedAsRequestedModel(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "x", Model: DispatchModel("example-35b")}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := disp.lastReq.RequestedModel; got != "example-35b" {
		t.Errorf("explicit model not preserved in RequestedModel, got %q", got)
	}
}

// TestQueryDispatchToHarness_RecognizedEnumValuesLeaveRequestedModelEmpty
// guards backward compatibility: the legacy "e4b"/"26b"/"" routing values
// must NOT populate RequestedModel — they continue to route through the
// legacy local-LLM probe (Path 3) exactly as before.
func TestQueryDispatchToHarness_RecognizedEnumValuesLeaveRequestedModelEmpty(t *testing.T) {
	for _, m := range []DispatchModel{"", DispatchModelE4B, DispatchModel26B} {
		disp := &fakeAgentDispatcher{cannedOk: true}
		req := DispatchRequest{Task: "x", Model: m}
		if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
			t.Fatalf("query(model=%q): %v", m, err)
		}
		if got := disp.lastReq.RequestedModel; got != "" {
			t.Errorf("model=%q: expected empty RequestedModel, got %q", m, got)
		}
	}
}

func TestQueryDispatchToHarness_EmptyTaskRejected(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	if _, err := QueryDispatchToHarness(context.Background(), disp, DispatchRequest{Task: ""}); err == nil {
		t.Fatal("expected error for empty task")
	}
	if _, err := QueryDispatchToHarness(context.Background(), disp, DispatchRequest{Task: "   "}); err == nil {
		t.Fatal("expected error for whitespace-only task")
	}
}

func TestQueryDispatchToHarness_NilControllerUnavailable(t *testing.T) {
	_, err := QueryDispatchToHarness(context.Background(), nil, DispatchRequest{Task: "x"})
	if err == nil {
		t.Fatal("expected ErrAgentUnavailable")
	}
	ace, ok := err.(*AgentControllerError)
	if !ok || ace.Code != "unavailable" {
		t.Errorf("expected unavailable error, got %v", err)
	}
}

func TestQueryDispatchToHarness_ControllerWithoutDispatcher(t *testing.T) {
	// Plain fakeAgentController doesn't satisfy AgentDispatcher; the
	// query helper should report unavailable rather than panic.
	plain := &fakeAgentController{}
	_, err := QueryDispatchToHarness(context.Background(), plain, DispatchRequest{Task: "x"})
	if err == nil {
		t.Fatal("expected error for non-dispatch controller")
	}
	if !strings.Contains(err.Error(), "does not support dispatch") {
		t.Errorf("expected does-not-support message, got %v", err)
	}
}

// RFC-0007 Layer 1: Provider field flows through Normalize without
// mutation. The Normalize step only trims; unknown-name validation is
// deferred to the dispatcher, which holds the live ProviderResolver.
func TestQueryDispatchToHarness_ProviderFieldFlowsThrough(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "do thing", Provider: "  desktop  "}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := disp.lastReq.Provider; got != "desktop" {
		t.Errorf("Provider not trimmed/preserved, got %q", got)
	}
}

func TestQueryDispatchToHarness_EmptyProviderUnchanged(t *testing.T) {
	disp := &fakeAgentDispatcher{cannedOk: true}
	req := DispatchRequest{Task: "do thing"}
	if _, err := QueryDispatchToHarness(context.Background(), disp, req); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := disp.lastReq.Provider; got != "" {
		t.Errorf("Provider should default empty, got %q", got)
	}
}

func TestDispatchRequest_NormalizeDedupesTools(t *testing.T) {
	req := DispatchRequest{Task: "x", Tools: []string{"a", " ", "a", "b", "", "b"}}
	if err := req.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if got, want := req.Tools, []string{"a", "b"}; !equalStrings(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
