package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalHarnessControllerTriggerAndList(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.LocalModel = "gemma4:e4b"
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)

	var call int
	model := "gemma4:e4b"
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": model, "object": "model"}},
			})
		case "/v1/chat/completions":
			call++
			// #432: assessCycle/executeCycleTaskWithPrompt now route through
			// CompleteCancelSafe (Stream under the hood) so a ctx cancel/
			// timeout actually aborts generation server-side. Honor the
			// request's stream field like a real openai-compat server so
			// this mock exercises the same path production traffic takes.
			var body struct {
				Stream bool `json:"stream"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			content := "local harness executed"
			if call == 1 {
				content = `{"action":"observe","reason":"field changed","urgency":0.4,"target":"memory","task":"summarize current state"}`
			}
			if body.Stream {
				writeSSECompletion(t, w, content)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"message": map[string]any{
						"role":    "assistant",
						"content": content,
					},
					"finish_reason": "stop",
				}},
				"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer llm.Close()
	t.Setenv(localLLMEndpointEnv, llm.URL)

	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	res, err := ctrl.TriggerAgent(context.Background(), DefaultAgentID, "test", true)
	if err != nil {
		t.Fatalf("TriggerAgent: %v", err)
	}
	if !res.Triggered {
		t.Fatalf("expected triggered=true, got %+v", res)
	}
	if res.Action != "observe" {
		t.Fatalf("Action = %q; want observe", res.Action)
	}

	list, err := ctrl.ListAgents(context.Background(), false)
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListAgents len = %d; want 1", len(list))
	}
	if list[0].CycleCount != 1 {
		t.Fatalf("CycleCount = %d; want 1", list[0].CycleCount)
	}

	snap, err := ctrl.GetAgent(context.Background(), DefaultAgentID, true, 5)
	if err != nil {
		t.Fatalf("GetAgent: %v", err)
	}
	if len(snap.Traces) != 1 {
		t.Fatalf("Traces len = %d; want 1", len(snap.Traces))
	}
	if snap.Traces[0].Result != "local harness executed" {
		t.Fatalf("trace result = %q; want local harness executed", snap.Traces[0].Result)
	}
}

// TestLocalHarnessAutonomicConsult_SetsMaxTokensBound verifies #432 item (c):
// internal non-interactive consults (the autonomic assess step here) must
// carry a bounded max_tokens on the wire even when the provider itself would
// otherwise apply a larger default — bounded generation must hold even if
// cancellation fails to propagate for any reason. Asserts the wire-level
// request body, not just the CompletionRequest struct field, so a bug in
// marshaling (e.g. buildOpenAIRequest's maxTokens precedence) would be
// caught here too.
func TestLocalHarnessAutonomicConsult_SetsMaxTokensBound(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.LocalModel = "gemma4:e4b"
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)

	var assessMaxTokens int
	var sawAssessCall bool
	model := "gemma4:e4b"
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": model, "object": "model"}},
			})
		case "/v1/chat/completions":
			var body struct {
				Stream    bool `json:"stream"`
				MaxTokens int  `json:"max_tokens"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if !sawAssessCall {
				sawAssessCall = true
				assessMaxTokens = body.MaxTokens
			}
			content := `{"action":"sleep","reason":"idle","urgency":0.0,"target":"","task":""}`
			if body.Stream {
				writeSSECompletion(t, w, content)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"message":       map[string]any{"role": "assistant", "content": content},
					"finish_reason": "stop",
				}},
				"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer llm.Close()
	t.Setenv(localLLMEndpointEnv, llm.URL)

	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	res, err := ctrl.TriggerAgent(context.Background(), DefaultAgentID, "test", true)
	if err != nil {
		t.Fatalf("TriggerAgent: %v", err)
	}
	if !res.Triggered {
		t.Fatalf("expected triggered=true, got %+v", res)
	}
	if !sawAssessCall {
		t.Fatal("assess call was never observed by the mock server")
	}
	if assessMaxTokens != localHarnessAssessMaxToks {
		t.Errorf("assess call max_tokens on the wire = %d; want %d (localHarnessAssessMaxToks)", assessMaxTokens, localHarnessAssessMaxToks)
	}
	if assessMaxTokens <= 0 {
		t.Error("assess call must carry a bounded (>0) max_tokens even when cancellation fails to propagate")
	}
}

func TestServerLegacyAgentStatusRoute(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	srv.SetAgentController(&fakeAgentController{
		GetResult: &AgentSnapshot{
			Summary: AgentSummary{
				AgentID:     DefaultAgentID,
				Alive:       true,
				CycleCount:  3,
				LastAction:  "sleep",
				LastCycle:   "2026-04-21T12:00:00Z",
				LastUrgency: 0.2,
				LastReason:  "idle",
				LastDurMs:   42,
				Model:       "gemma4:e4b",
				Interval:    "1m0s",
				UptimeSec:   60,
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/status", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rr.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["cycle_count"].(float64) != 3 {
		t.Fatalf("cycle_count = %v; want 3", body["cycle_count"])
	}
	if body["uptime"].(string) != "1m0s" {
		t.Fatalf("uptime = %v; want 1m0s", body["uptime"])
	}
}

// TestLocalHarnessOllamaConcurrencySerialized verifies that ollamaMu prevents
// runCycle and DispatchToHarness from issuing concurrent inference requests.
// It spins up a fake LM Studio server that increments an in-flight counter on
// entry and decrements on exit; any counter value > 1 means concurrent calls
// leaked through the serialization gate.
func TestLocalHarnessOllamaConcurrencySerialized(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.LocalModel = "gemma4:e4b"
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)

	model := "gemma4:e4b"

	var (
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
	)

	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": model, "object": "model"}},
			})
		case "/v1/chat/completions":
			cur := inFlight.Add(1)
			// Record the peak concurrency seen.
			for {
				old := maxInFlight.Load()
				if cur <= old {
					break
				}
				if maxInFlight.CompareAndSwap(old, cur) {
					break
				}
			}
			defer inFlight.Add(-1)

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"message": map[string]any{
						"role":    "assistant",
						"content": `{"action":"sleep","reason":"idle","urgency":0.0,"target":"","task":""}`,
					},
					"finish_reason": "stop",
				}},
				"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer llm.Close()
	t.Setenv(localLLMEndpointEnv, llm.URL)

	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	ctx := context.Background()

	// Fire a metabolic cycle and a dispatch concurrently. Without ollamaMu
	// both would immediately call buildLocalProvider and hit /api/chat at
	// the same time.
	errs := make(chan error, 2)
	go func() {
		_, err := ctrl.TriggerAgent(ctx, DefaultAgentID, "concurrency-test", true)
		errs <- err
	}()
	go func() {
		_, err := ctrl.DispatchToHarness(ctx, DispatchRequest{
			Task:           "concurrency-check",
			N:              1,
			TimeoutSeconds: 10,
		})
		errs <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("goroutine %d error: %v", i, err)
		}
	}

	if got := maxInFlight.Load(); got > 1 {
		t.Errorf("max concurrent /v1/chat/completions calls = %d; want <= 1 (ollamaMu not working)", got)
	}
}

// TestDispatchToHarness_StateRouting_ReceptiveGoesToMLX verifies that when the
// process state is "receptive" and process_state_routing maps "receptive" to a
// configured openai-compat provider (simulating mlx-lm), DispatchToHarness
// without an explicit req.Provider routes to that provider rather than the
// legacy Ollama local-LLM path.
//
// This is the core behavioural requirement for Wave C W2: the autonomic loop's
// harness dispatches must honour process_state_routing, not hardcode Ollama.
func TestDispatchToHarness_StateRouting_ReceptiveGoesToMLX(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	// Stand up a lightweight openai-compat stub (simulates mlx-lm endpoint).
	var mlxCalled atomic.Bool
	mlxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			mlxCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "chatcmpl-test",
				"object":  "chat.completion",
				"model":   "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "mlx response"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
			})
		default:
			t.Errorf("mlxSrv: unexpected path %s", r.URL.Path)
		}
	}))
	defer mlxSrv.Close()

	// Write providers.yaml with process_state_routing: receptive -> mlx-lm.
	// The mlx-lm endpoint points at our httptest server.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: http://localhost:11434
    model: gemma4:e4b
  mlx-lm:
    type: openai
    endpoint: `+mlxSrv.URL+`
    model: gemma-4-e4b
routing:
  default: ollama
  fallback_chain: [mlx-lm, ollama]
  process_state_routing:
    receptive: mlx-lm
`)

	// NewProcess starts in StateReceptive — no transition needed.
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	if got := proc.State().String(); got != "receptive" {
		t.Fatalf("process initial state = %q; want receptive", got)
	}

	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	ctx := context.Background()
	_, dispErr := ctrl.DispatchToHarness(ctx, DispatchRequest{
		Task:           "state-routing test",
		N:              1,
		TimeoutSeconds: 10,
	})
	// Dispatch may fail (mlx stub returns no tool results, etc.) but what
	// matters is whether the mlx-lm endpoint was called.
	_ = dispErr

	if !mlxCalled.Load() {
		t.Errorf("mlx-lm endpoint was NOT called; expected state-routing (receptive -> mlx-lm) to route there instead of Ollama legacy path")
	}
}

// TestDispatchToHarness_StateRouting_UnknownStateFallsBackToLegacy verifies
// that a process in an unrecognised state (ProcessState outside the defined
// iota range) falls back to the legacy Ollama probe path instead of routing
// to process_state_routing["unknown"].
func TestDispatchToHarness_StateRouting_UnknownStateFallsBackToLegacy(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var ollamaCalled atomic.Bool
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			ollamaCalled.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ollama fallback"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	// providers.yaml maps "unknown" state to a provider; this MUST NOT fire.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: `+ollamaSrv.URL+`
    model: gemma4:e4b
routing:
  default: ollama
  process_state_routing:
    unknown: ollama
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	// Force the process into an out-of-range state so State().String() returns "unknown".
	proc.transitionWithReason(ProcessState(99), "test: force unknown state")

	if got := proc.State().String(); got != "unknown" {
		t.Fatalf("proc.State().String() = %q; want \"unknown\"", got)
	}

	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, _ = ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "unknown-state fallback test",
		N:              1,
		TimeoutSeconds: 10,
	})

	// The legacy local-LLM probe should have been hit (via /api/tags Ollama probe
	// as fallback after /v1/models fails), not state-routed.
	if !ollamaCalled.Load() {
		t.Errorf("legacy probe was NOT called; unknown state should fall back to legacy path, not route via process_state_routing[\"unknown\"]")
	}
}

// TestDispatchToHarness_StateRouting_NoMappingFallsBackToLegacy verifies that
// when a process state has no entry in process_state_routing, dispatch falls
// through to the legacy local-LLM probe.
func TestDispatchToHarness_StateRouting_NoMappingFallsBackToLegacy(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var ollamaCalled atomic.Bool
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			ollamaCalled.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ok"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	// process_state_routing has no "receptive" entry — state has no mapping.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: `+ollamaSrv.URL+`
    model: gemma4:e4b
routing:
  default: ollama
  process_state_routing:
    active: ollama
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	// NewProcess starts in StateReceptive — no mapping for "receptive" above.
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, _ = ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "no-mapping fallback test",
		N:              1,
		TimeoutSeconds: 10,
	})

	if !ollamaCalled.Load() {
		t.Errorf("legacy probe was NOT called; receptive state with no mapping should fall back to legacy path")
	}
}

// TestDispatchToHarness_StateRouting_DisabledProviderFallsBackToLegacy verifies
// that when state_routing resolves to a provider that is disabled (enabled: false),
// dispatch falls through to the legacy local-LLM probe.
func TestDispatchToHarness_StateRouting_DisabledProviderFallsBackToLegacy(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var ollamaCalled atomic.Bool
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			ollamaCalled.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ok"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	// mlx-lm is mapped for receptive but marked enabled: false.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: `+ollamaSrv.URL+`
    model: gemma4:e4b
  mlx-lm:
    type: openai
    endpoint: http://127.0.0.1:1
    model: gemma-4-e4b
    enabled: false
routing:
  default: ollama
  process_state_routing:
    receptive: mlx-lm
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, _ = ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "disabled-provider fallback test",
		N:              1,
		TimeoutSeconds: 10,
	})

	if !ollamaCalled.Load() {
		t.Errorf("legacy probe was NOT called; disabled state-routed provider should fall back to legacy path")
	}
}

// TestDispatchToHarness_StateRouting_MissingProviderFallsBackToLegacy verifies
// that when state_routing names a provider that doesn't exist in the providers
// map, dispatch falls through to the legacy local-LLM probe.
func TestDispatchToHarness_StateRouting_MissingProviderFallsBackToLegacy(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var ollamaCalled atomic.Bool
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			ollamaCalled.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ok"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	// process_state_routing points "receptive" at "nonexistent" which is not
	// in the providers map.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: `+ollamaSrv.URL+`
    model: gemma4:e4b
routing:
  default: ollama
  process_state_routing:
    receptive: nonexistent
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, _ = ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "missing-provider fallback test",
		N:              1,
		TimeoutSeconds: 10,
	})

	if !ollamaCalled.Load() {
		t.Errorf("legacy probe was NOT called; missing provider in state_routing should fall back to legacy path")
	}
}

// TestDispatchToHarness_StateRouting_HappyPath_ProviderUsedAndNoError verifies
// the result shape on a successful state-routed dispatch:
// - provider_used is populated with the resolved provider name
// - error is empty on success
// - success is true
func TestDispatchToHarness_StateRouting_HappyPath_ProviderUsedAndNoError(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	mlxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "chatcmpl-test",
				"object":  "chat.completion",
				"model":   "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "done"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
			})
		}
	}))
	defer mlxSrv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  mlx-lm:
    type: openai
    endpoint: `+mlxSrv.URL+`
    model: gemma-4-e4b
routing:
  process_state_routing:
    receptive: mlx-lm
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "happy path provider_used test",
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if len(batch.Results) != 1 {
		t.Fatalf("batch.Results len = %d; want 1", len(batch.Results))
	}
	res := batch.Results[0]

	// provider_used must be populated so callers can distinguish state-routed from legacy.
	if res.ProviderUsed == "" {
		t.Errorf("ProviderUsed is empty; want \"mlx-lm\" on state-routed path")
	}
	if res.ProviderUsed != "mlx-lm" {
		t.Errorf("ProviderUsed = %q; want \"mlx-lm\"", res.ProviderUsed)
	}

	// error must be empty on a successful dispatch — routing notes must NOT
	// bleed into error.
	if res.Error != "" {
		t.Errorf("Error = %q; want empty on successful state-routed dispatch", res.Error)
	}

	if !res.Success {
		t.Errorf("Success = false; want true")
	}
}

// TestDispatchToHarness_StateRouting_LocalProviderSerializesWithCycle verifies
// that a state-routed dispatch to a local OpenAI-compatible provider (mlx-lm,
// vllm, lmstudio, etc.) still acquires ollamaMu and serialises against the
// metabolic cycle. Local providers compete for the same on-device
// accelerator/VRAM as the Ollama metabolic path; concurrent access can cause
// memory pressure. This test pins the contract that provider.Capabilities().IsLocal
// governs the lock, not the provider type name "ollama".
func TestDispatchToHarness_StateRouting_LocalProviderSerializesWithCycle(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	// Track calls across both the mlx-lm dispatch stub and the Ollama cycle
	// stub using a shared in-flight counter.
	recordInFlight := func() func() {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old {
				break
			}
			if maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		return func() { inFlight.Add(-1) }
	}

	mlxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			done := recordInFlight()
			defer done()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "chatcmpl-test",
				"object":  "chat.completion",
				"model":   "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "done"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer mlxSrv.Close()

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			done := recordInFlight()
			defer done()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": `{"action":"sleep","reason":"idle","urgency":0.0,"target":"","task":""}`},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  mlx-lm:
    type: openai
    endpoint: `+mlxSrv.URL+`
    model: gemma-4-e4b
routing:
  process_state_routing:
    receptive: mlx-lm
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	ctx := context.Background()

	// Fire a state-routed dispatch (mlx-lm, IsLocal=true) and a metabolic
	// cycle concurrently. Both should acquire ollamaMu and therefore serialize;
	// max concurrent in-flight should be <= 1.
	errs := make(chan error, 2)
	go func() {
		_, e := ctrl.TriggerAgent(ctx, DefaultAgentID, "cycle", true)
		errs <- e
	}()
	go func() {
		_, e := ctrl.DispatchToHarness(ctx, DispatchRequest{
			Task:           "local non-Ollama serialization test",
			N:              1,
			TimeoutSeconds: 10,
		})
		errs <- e
	}()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("goroutine %d error: %v", i, err)
		}
	}

	if got := maxInFlight.Load(); got > 1 {
		t.Errorf("max concurrent local inference calls = %d; want <= 1 (mlx-lm IsLocal=true should still serialize via ollamaMu)", got)
	}
}

// TestDispatchToHarness_Legacy26bDowngradeWarningPerSlot is a regression test
// ensuring that the "26b route unavailable, degraded to e4b" per-slot warning
// from the legacy path still appears in each DispatchResult — in the Note
// field, NOT Error. The warning was silently dropped in an earlier iteration
// of the state-routing patch; it then rode along in Error, which made
// success=true slots look failed to callers that only check that field
// (observed live 2026-07-04: a successful dispatch carrying the provider-probe
// fallback note in error). This test pins the current contract: warning
// present in Note, Error empty on success.
func TestDispatchToHarness_Legacy26bDowngradeWarningPerSlot(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.LocalModel = "gemma4:e4b"

	// Ollama stub that only lists gemma4:e4b — no 26b-class model available.
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ok"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "26b downgrade test",
		Model:          DispatchModel26B,
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if len(batch.Results) != 1 {
		t.Fatalf("batch.Results len = %d; want 1", len(batch.Results))
	}
	res := batch.Results[0]

	// Dispatch must succeed — it fell back to e4b.
	if !res.Success {
		t.Errorf("Success = false; want true (26b fallback to e4b should still succeed)")
	}
	// Per-slot Note must carry the downgrade warning so callers that inspect
	// individual slot results can see that the requested model was not honored.
	// It must NOT be in Error: this is a success, and informational routing
	// notes in the error field made successful slots look failed.
	if res.Note == "" {
		t.Errorf("Note is empty; want downgrade warning (e.g. %q)", "26b route unavailable, degraded to e4b")
	}
	if res.Error != "" {
		t.Errorf("Error = %q; want empty on a successful slot (warnings belong in Note)", res.Error)
	}
	if res.ModelUsed != DispatchModelE4B {
		t.Errorf("ModelUsed = %q; want DispatchModelE4B", res.ModelUsed)
	}
}

// TestDispatchToHarness_TypelessOllamaStillAcquiresOllamaMu verifies that a
// provider named "ollama" WITHOUT an explicit type: field is still treated as
// Ollama for lock-acquisition purposes. The isOllamaProvider helper must mirror
// makeProvider's inference rule (empty type == provider name) so that the
// documented short-form config shape is not silently misclassified as non-Ollama.
func TestDispatchToHarness_TypelessOllamaStillAcquiresOllamaMu(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			cur := inFlight.Add(1)
			for {
				old := maxInFlight.Load()
				if cur <= old {
					break
				}
				if maxInFlight.CompareAndSwap(old, cur) {
					break
				}
			}
			defer inFlight.Add(-1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": `{"action":"sleep","reason":"idle","urgency":0.0,"target":"","task":""}`},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()

	// Provider named "ollama" with NO explicit type: field — the documented
	// short-form config shape. isOllamaProvider must still return true.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    endpoint: `+ollamaSrv.URL+`
    model: gemma4:e4b
routing:
  process_state_routing:
    receptive: ollama
`)
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	ctx := context.Background()

	errs := make(chan error, 2)
	go func() {
		_, err := ctrl.TriggerAgent(ctx, DefaultAgentID, "cycle", true)
		errs <- err
	}()
	go func() {
		_, err := ctrl.DispatchToHarness(ctx, DispatchRequest{
			Task:           "typeless-ollama lock test",
			N:              1,
			TimeoutSeconds: 10,
		})
		errs <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("goroutine %d error: %v", i, err)
		}
	}

	if got := maxInFlight.Load(); got > 1 {
		t.Errorf("max concurrent Ollama /api/chat = %d; want <= 1 (typeless ollama provider should still hold ollamaMu)", got)
	}
}

// TestDispatchToHarness_HarnessProvider_ResolvesNamedProvider verifies that
// when req.Provider is empty, req.Model resolves to nothing, no
// process_state_routing entry matches, and cfg.HarnessProvider names a provider,
// DispatchToHarness routes to that named provider instead of probing Ollama.
//
// This is the core behaviour for cross-node dispatch: a BEP-received remote
// dispatch arrives with empty Provider and is resolved using the EXECUTING
// node's harness_provider (e.g. a remote node -> its own LM Studio), not the legacy probe.
func TestDispatchToHarness_HarnessProvider_ResolvesNamedProvider(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	// Stand up an openai-compat stub (simulates the lmstudio endpoint).
	var lmsCalled atomic.Bool
	lmsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			lmsCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "chatcmpl-test",
				"object":  "chat.completion",
				"model":   "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "lmstudio response"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
			})
		default:
			t.Errorf("lmsSrv: unexpected path %s", r.URL.Path)
		}
	}))
	defer lmsSrv.Close()

	// No process_state_routing — only a named provider "lmstudio".
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio:
    type: openai
    endpoint: `+lmsSrv.URL+`
    model: gemma-4-e4b
routing:
  default: lmstudio
`)

	// Point the legacy Ollama probe at a dead address so that if the resolution
	// chain wrongly falls through to Path 3, the test fails loudly (or at least
	// does not hit lmstudio). HarnessProvider must take precedence.
	t.Setenv(localLLMEndpointEnv, "http://127.0.0.1:1")

	cfg.HarnessProvider = "lmstudio"

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "harness-provider default test",
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}

	if !lmsCalled.Load() {
		t.Errorf("lmstudio endpoint was NOT called; expected harness_provider to route there instead of the legacy Ollama probe")
	}
	if len(batch.Results) != 1 {
		t.Fatalf("batch.Results len = %d; want 1", len(batch.Results))
	}
	if got := batch.Results[0].ProviderUsed; got != "lmstudio" {
		t.Errorf("ProviderUsed = %q; want \"lmstudio\" (harness_provider path)", got)
	}
}

// TestDispatchToHarness_HarnessProvider_ExplicitProviderWins verifies that an
// explicit req.Provider takes precedence over cfg.HarnessProvider.
func TestDispatchToHarness_HarnessProvider_ExplicitProviderWins(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var explicitCalled atomic.Bool
	var harnessCalled atomic.Bool

	explicitSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			explicitCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "explicit"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer explicitSrv.Close()

	harnessSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			harnessCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "harness"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer harnessSrv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  explicit-provider:
    type: openai
    endpoint: `+explicitSrv.URL+`
    model: gemma-4-e4b
  lmstudio:
    type: openai
    endpoint: `+harnessSrv.URL+`
    model: gemma-4-e4b
routing:
  default: lmstudio
`)

	cfg.HarnessProvider = "lmstudio"

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "explicit-wins test",
		Provider:       "explicit-provider",
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}

	if !explicitCalled.Load() {
		t.Errorf("explicit-provider endpoint was NOT called; explicit req.Provider must win over harness_provider")
	}
	if harnessCalled.Load() {
		t.Errorf("harness_provider endpoint WAS called; explicit req.Provider should have taken precedence")
	}
}

// TestDispatchToHarness_HarnessProvider_StateRoutingWins verifies that when a
// process_state_routing entry matches the current state, it takes precedence
// over cfg.HarnessProvider (state routing is Path 2, harness_provider is the
// Path 2.5 fallback above the legacy probe).
func TestDispatchToHarness_HarnessProvider_StateRoutingWins(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var stateCalled atomic.Bool
	var harnessCalled atomic.Bool

	stateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			stateCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "state"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer stateSrv.Close()

	harnessSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			harnessCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": "gemma-4-e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "harness"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer harnessSrv.Close()

	// receptive -> state-provider via process_state_routing. lmstudio is the
	// harness_provider fallback that must NOT fire because state routing matched.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  state-provider:
    type: openai
    endpoint: `+stateSrv.URL+`
    model: gemma-4-e4b
  lmstudio:
    type: openai
    endpoint: `+harnessSrv.URL+`
    model: gemma-4-e4b
routing:
  process_state_routing:
    receptive: state-provider
`)

	cfg.HarnessProvider = "lmstudio"

	// NewProcess starts in StateReceptive — matches the routing entry.
	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	if got := proc.State().String(); got != "receptive" {
		t.Fatalf("process initial state = %q; want receptive", got)
	}

	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "state-routing-wins test",
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}

	if !stateCalled.Load() {
		t.Errorf("state-provider endpoint was NOT called; process_state_routing must win over harness_provider")
	}
	if harnessCalled.Load() {
		t.Errorf("harness_provider endpoint WAS called; process_state_routing should have taken precedence")
	}
}

// TestDispatchToHarness_EmptyHarnessProvider_FallsBackToLegacy verifies that
// when cfg.HarnessProvider is empty, dispatch falls through to the legacy
// Ollama probe path (unchanged behaviour).
func TestDispatchToHarness_EmptyHarnessProvider_FallsBackToLegacy(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	// HarnessProvider intentionally left empty.

	var ollamaCalled atomic.Bool
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			ollamaCalled.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{{"name": "gemma4:e4b"}},
			})
		case "/api/chat":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": map[string]any{"role": "assistant", "content": "ollama fallback"},
				"done":    true, "prompt_eval_count": 1, "eval_count": 1,
			})
		}
	}))
	defer ollamaSrv.Close()
	t.Setenv(localLLMEndpointEnv, ollamaSrv.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, _ = ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "empty-harness-provider legacy test",
		N:              1,
		TimeoutSeconds: 10,
	})

	if !ollamaCalled.Load() {
		t.Errorf("Ollama /api/tags was NOT called; empty harness_provider should fall back to the legacy probe path")
	}
}

// ── Issue #430: explicit-model-wins-over-config-default + served-model visibility ──

// TestDispatchToHarness_ExplicitProvider_RequestedModelWinsOverConfigDefault
// verifies that when a caller names both an explicit provider AND an
// explicit model, the wire request sent to that provider carries the
// caller's model — not the provider config's declared default. This is
// Path 1 (explicit named provider) in DispatchToHarness.
func TestDispatchToHarness_ExplicitProvider_RequestedModelWinsOverConfigDefault(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-content-hash-id","object":"model"}]}`))
		case "/v1/chat/completions":
			var body struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotModel = body.Model
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": body.Model,
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	// Provider config hardcodes a model, mirroring the local-node
	// providers.local.yaml gemma content-hash id from the issue.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-local:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-content-hash-id
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, server.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "explicit model must win",
		Provider:       "lmstudio-local",
		Model:          DispatchModel("example-35b"),
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if len(batch.Results) != 1 {
		t.Fatalf("batch.Results len = %d; want 1", len(batch.Results))
	}
	res := batch.Results[0]

	if gotModel != "example-35b" {
		t.Errorf("wire request model = %q; want caller's explicit \"example-35b\" (config default must not win)", gotModel)
	}
	if res.ProviderUsed != "lmstudio-local" {
		t.Errorf("ProviderUsed = %q; want \"lmstudio-local\"", res.ProviderUsed)
	}
	if res.ServedModel != "example-35b" {
		t.Errorf("ServedModel = %q; want \"example-35b\" (the model actually sent to the provider)", res.ServedModel)
	}
	if !res.Success {
		t.Errorf("Success = false; want true, error=%q", res.Error)
	}
}

// TestDispatchToHarness_ExplicitProvider_ConfigDefaultAppliesWhenNoModelRequested
// is the control: when the caller names a provider but no model, the
// provider's configured model remains the default — unchanged from prior
// behavior. Also asserts ServedModel reflects that default.
func TestDispatchToHarness_ExplicitProvider_ConfigDefaultAppliesWhenNoModelRequested(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-content-hash-id","object":"model"}]}`))
		case "/v1/chat/completions":
			var body struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotModel = body.Model
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": body.Model,
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-local:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-content-hash-id
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, server.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "no explicit model — config default applies",
		Provider:       "lmstudio-local",
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	res := batch.Results[0]

	if gotModel != "gemma-content-hash-id" {
		t.Errorf("wire request model = %q; want provider's configured default \"gemma-content-hash-id\"", gotModel)
	}
	if res.ServedModel != "gemma-content-hash-id" {
		t.Errorf("ServedModel = %q; want \"gemma-content-hash-id\"", res.ServedModel)
	}
}

// TestDispatchToHarness_HarnessProviderDefault_RequestedModelWins covers Path
// 2.5 (cfg.HarnessProvider default): an explicit caller model must still win
// even when the provider itself came from the harness_provider config
// default rather than an explicit req.Provider.
func TestDispatchToHarness_HarnessProviderDefault_RequestedModelWins(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	var gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-content-hash-id","object":"model"}]}`))
		case "/v1/chat/completions":
			var body struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotModel = body.Model
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": body.Model,
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-local:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-content-hash-id
`)
	cfg.HarnessProvider = "lmstudio-local"

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, server.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "explicit model over harness_provider default",
		Model:          DispatchModel("example-35b"),
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	res := batch.Results[0]

	if gotModel != "example-35b" {
		t.Errorf("wire request model = %q; want caller's explicit \"example-35b\"", gotModel)
	}
	if res.ProviderUsed != "lmstudio-local" {
		t.Errorf("ProviderUsed = %q; want \"lmstudio-local\"", res.ProviderUsed)
	}
	if res.ServedModel != "example-35b" {
		t.Errorf("ServedModel = %q; want \"example-35b\"", res.ServedModel)
	}
}

// TestDispatchToHarness_ProviderCannotServeModel_FailsLoudly verifies that
// when the resolved provider rejects the requested model (simulated here as
// an HTTP 400, mirroring LM Studio's real refusal behavior from the issue's
// ctx-262144 battery), the dispatch surfaces a hard per-slot error rather
// than silently falling back to a different model.
func TestDispatchToHarness_ProviderCannotServeModel_FailsLoudly(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-content-hash-id","object":"model"}]}`))
		case "/v1/chat/completions":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"model 'example-35b' would likely overload your system"}`))
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-local:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-content-hash-id
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, server.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "provider refuses requested model",
		Provider:       "lmstudio-local",
		Model:          DispatchModel("example-35b"),
		N:              1,
		TimeoutSeconds: 10,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	res := batch.Results[0]

	if res.Success {
		t.Errorf("Success = true; want false — provider refused the requested model, must fail loudly not substitute")
	}
	if res.Error == "" {
		t.Errorf("Error is empty; want the provider's refusal surfaced to the caller")
	}
	if res.ServedModel != "" {
		t.Errorf("ServedModel = %q; want empty on a failed dispatch (nothing actually served)", res.ServedModel)
	}
}

// ambientDispatchTestFixture stands up a LocalHarnessController wired to a
// named "lmstudio" provider backed by an httptest server that records the
// system-role message content of every /v1/chat/completions request it
// receives. Shared by the two req.Ambient tests below.
func ambientDispatchTestFixture(t *testing.T) (ctrl *LocalHarnessController, capturedSystemPrompts *[]string) {
	t.Helper()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	prompts := make([]string, 0, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			var body struct {
				Messages []struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				} `json:"messages"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			for _, m := range body.Messages {
				if m.Role == "system" {
					prompts = append(prompts, m.Content)
					break
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"index":         0,
					"message":       map[string]any{"role": "assistant", "content": "ambient-test response"},
					"finish_reason": "stop",
				}},
			})
		default:
			t.Errorf("ambientDispatchTestFixture: unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-4-e4b
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	c, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, server.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}
	return c, &prompts
}

// TestDispatchToHarness_AmbientFalse_MessageSetUnchanged verifies that
// leaving req.Ambient at its zero value (false) — the behavior of every
// caller that predates this field — produces the exact same system prompt
// DispatchToHarness has always composed: the harness orientation block plus
// the default dispatch prompt, with no ambient block anywhere in it. This is
// the "existing callers are byte-identical" half of the opt-in contract.
func TestDispatchToHarness_AmbientFalse_MessageSetUnchanged(t *testing.T) {
	ctrl, prompts := ambientDispatchTestFixture(t)

	want := ctrl.harnessOrientationBlock + "\n\n" + localHarnessDispatchPrompt

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "ambient=false control",
		Provider:       "lmstudio",
		N:              1,
		TimeoutSeconds: 10,
		// Ambient omitted: zero value, i.e. false.
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if !batch.Results[0].Success {
		t.Fatalf("Results[0].Success = false; error=%q", batch.Results[0].Error)
	}
	if len(*prompts) != 1 {
		t.Fatalf("captured %d system prompts; want 1", len(*prompts))
	}
	got := (*prompts)[0]
	if got != want {
		t.Errorf("system prompt with Ambient=false diverged from the pre-existing composition:\ngot:  %q\nwant: %q", got, want)
	}
	if strings.Contains(got, "ambient state of self") {
		t.Errorf("system prompt with Ambient=false unexpectedly contains the ambient block")
	}
}

// TestDispatchToHarness_AmbientTrue_PrependsAmbientBlock verifies that
// req.Ambient=true prepends a block carrying live health/workspace/identity
// state ahead of the harness's normal system prompt, using the same
// AdditionalContext-prepend pattern the PreInference hook already uses on
// the main chat/external-CLI path. The pre-existing prompt content must
// still be present and untouched — only prefixed.
func TestDispatchToHarness_AmbientTrue_PrependsAmbientBlock(t *testing.T) {
	ctrl, prompts := ambientDispatchTestFixture(t)

	base := ctrl.harnessOrientationBlock + "\n\n" + localHarnessDispatchPrompt

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "ambient=true test",
		Provider:       "lmstudio",
		N:              1,
		TimeoutSeconds: 10,
		Ambient:        true,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if !batch.Results[0].Success {
		t.Fatalf("Results[0].Success = false; error=%q", batch.Results[0].Error)
	}
	if len(*prompts) != 1 {
		t.Fatalf("captured %d system prompts; want 1", len(*prompts))
	}
	got := (*prompts)[0]

	if !strings.HasPrefix(got, "=== ambient state of self ===") {
		t.Errorf("system prompt with Ambient=true does not start with the ambient block marker:\n%s", got)
	}
	if !strings.Contains(got, "health_counts:") {
		t.Errorf("ambient block missing health_counts line:\n%s", got)
	}
	if !strings.Contains(got, "workspace=") {
		t.Errorf("ambient block missing workspace= line:\n%s", got)
	}
	if !strings.Contains(got, "identity=Cog") {
		t.Errorf("ambient block missing identity= line:\n%s", got)
	}
	if !strings.HasSuffix(got, base) {
		t.Errorf("ambient-prefixed prompt does not end with the unchanged base prompt:\ngot:  %q\nbase: %q", got, base)
	}
}

// TestDispatchToHarness_AmbientTrue_DoesNotConsumeAbandonedInferenceDelta is
// the #432 regression test for gate finding 1 on the ambient-context PR:
// buildAmbientBlock used to call the CONSUMING buildKernelHealthSnapshot,
// whose abandonedInferenceSnapshot() read swaps-and-resets the process-global
// #432 abandoned-inference watermark. The autonomic ticker (autonomicTick)
// is the production consumer that relies on "delta since my last tick" to
// drive escalateAbandonedInference; a concurrent Ambient=true dispatch that
// read the watermark first would silently zero the delta the ticker needed,
// suppressing #432's own escalation path — exactly the "vitals reading 0anom
// for hours" failure mode #432 exists to catch.
//
// This test proves: (1) an Ambient=true dispatch still SEES a pending
// abandoned-inference delta (the block remains informative — it uses the
// non-consuming buildKernelHealthSnapshotPeek/abandonedInferencePeek), and
// (2) it does NOT consume that delta — a subsequent (consuming)
// buildKernelHealthSnapshot call, standing in for the next autonomic tick,
// still observes the delta and shouldEscalate still returns
// escalateAbandonedInference.
func TestDispatchToHarness_AmbientTrue_DoesNotConsumeAbandonedInferenceDelta(t *testing.T) {
	ctrl, prompts := ambientDispatchTestFixture(t)

	// Baseline the #432 watermark so this test's delta isn't polluted by
	// other tests sharing the process-global counter (same convention as
	// TestRecordAbandonedInferenceIncrementsSnapshotDelta).
	_, _ = abandonedInferenceSnapshot()

	// Simulate a pending #432 abandonment the autonomic ticker has not yet
	// consumed.
	recordAbandonedInference("ambient-nonconsume-test", "", errCanceledForTest{})

	batch, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "ambient nonconsume test",
		Provider:       "lmstudio",
		N:              1,
		TimeoutSeconds: 10,
		Ambient:        true,
	})
	if dispErr != nil {
		t.Fatalf("DispatchToHarness: %v", dispErr)
	}
	if !batch.Results[0].Success {
		t.Fatalf("Results[0].Success = false; error=%q", batch.Results[0].Error)
	}
	if len(*prompts) != 1 {
		t.Fatalf("captured %d system prompts; want 1", len(*prompts))
	}
	got := (*prompts)[0]
	if !strings.Contains(got, "anomalies=1") {
		t.Errorf("ambient block should report the pending abandoned-inference delta (anomalies=1), got:\n%s", got)
	}

	// The critical assertion: the autonomic ticker's own (consuming) snapshot
	// must STILL see the delta after the ambient dispatch ran. Before the
	// fix, buildAmbientBlock's call to the consuming buildKernelHealthSnapshot
	// would already have swapped the watermark, so this read would
	// incorrectly return Anomalies=0 here.
	snap := buildKernelHealthSnapshot(context.Background())
	if snap.Anomalies != 1 {
		t.Fatalf("autonomic ticker's snapshot Anomalies = %d; want 1 (the ambient dispatch must not have consumed the #432 delta)", snap.Anomalies)
	}

	reason := shouldEscalate(snap, false, time.Time{}, AutonomicConfig{IdleRecheckIn: time.Hour})
	if reason != escalateAbandonedInference {
		t.Errorf("shouldEscalate = %q; want %q (a subsequent autonomic tick must still escalate on the abandoned-inference delta)", reason, escalateAbandonedInference)
	}
}

// TestDispatchToHarness_AmbientTrue_ConcurrentSameRequestIDIsDeduped is the
// #432 retry-discipline regression test for gate finding 2 on the
// ambient-context PR: buildAmbientBlock embeds a live time= line and
// fluctuating health_counts data, so before the fix the content-stable
// RequestID hash — computed from systemPrompt+task+tool-names AFTER the
// ambient block was prepended — differed across two otherwise-identical
// Ambient=true dispatches, defeating the in-flight dedup guard
// (beginInflightInference) that
// TestCompleteWithToolLoop_ConcurrentSameRequestIDIsDeduped exercises for the
// non-ambient path.
//
// This test forces the two calls' ambient blocks to differ (distinct
// synthetic time= and health_counts content, the same shape buildAmbientBlock
// itself produces call-to-call) and then asserts the second call still
// collides with the first at the in-flight registry. A collision is only
// possible if both calls produced the same RequestID, which only happens if
// the hash is computed from the pre-ambient base prompt.
//
// Exercised directly against dispatchSlot (rather than through
// DispatchToHarness, whose c.ollamaMu batch-level mutex serializes all
// local-provider dispatches and would make the second call wait for the
// first to fully finish — including its timeout — before even attempting
// registration, masking the very race this test needs to observe) to isolate
// exactly the mechanism the fix touches, the same reasoning
// TestCompleteWithToolLoop_ConcurrentSameRequestIDIsDeduped's own doc comment
// gives for bypassing DispatchToHarness.
func TestDispatchSlot_AmbientTrue_ConcurrentSameRequestIDIsDeduped(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	releaseFirst := make(chan struct{})
	var firstArrived sync.WaitGroup
	firstArrived.Add(1)
	var firstArrivedOnce sync.Once

	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": "gemma4:e4b", "object": "model"}},
			})
		case "/v1/chat/completions":
			firstArrivedOnce.Do(func() { firstArrived.Done() })
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial\"}}]}\n\n")
			flusher.Flush()
			// Hold the stream open until the test releases it, simulating a
			// long-running generation the second (would-be duplicate) call
			// would race against.
			<-releaseFirst
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer llm.Close()
	t.Setenv(localLLMEndpointEnv, llm.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	provider := NewOpenAICompatProvider("agent-local", ProviderConfig{
		Endpoint: llm.URL,
		Model:    "gemma4:e4b",
		Timeout:  10,
	})
	registry := NewKernelToolRegistry(srv.mcpServer)

	req := DispatchRequest{
		Task:           "ambient-dedup dispatchSlot task",
		TimeoutSeconds: 10,
		Ambient:        true,
	}

	// Two synthetic ambient blocks that differ exactly the way two real
	// buildAmbientBlock calls would (different time=, different
	// health_counts anomalies=), standing in for what a real second call
	// would compute a moment later without needing to race real wall-clock
	// or counter state.
	ambientA := "=== ambient state of self ===\ntime=2026-01-01T00:00:00Z\n" +
		"health_counts: healthy=1 degraded=0 missing=0 suspended=0 anomalies=0\n" +
		"=== end ambient state ==="
	ambientB := "=== ambient state of self ===\ntime=2026-01-01T00:00:05Z\n" +
		"health_counts: healthy=1 degraded=0 missing=0 suspended=0 anomalies=1\n" +
		"=== end ambient state ==="
	if ambientA == ambientB {
		t.Fatal("test setup bug: ambientA and ambientB must differ")
	}

	type outcome struct {
		res DispatchResult
	}
	results := make(chan outcome, 1)

	go func() {
		res := ctrl.dispatchSlot(context.Background(), provider, registry, "gemma4:e4b", DispatchModel(""), req, 0, "", "anonymous", ambientA)
		results <- outcome{res}
	}()
	firstArrived.Wait()

	res2 := ctrl.dispatchSlot(context.Background(), provider, registry, "gemma4:e4b", DispatchModel(""), req, 0, "", "anonymous", ambientB)
	if res2.Success {
		t.Fatal("expected the second, concurrent, content-identical Ambient=true dispatchSlot call (differing only in ambient content) to be refused by the in-flight dedup guard, but it succeeded")
	}
	if !strings.Contains(res2.Error, "already in flight") {
		t.Errorf("expected second dispatchSlot call's error to report the in-flight dedup collision, got: %q", res2.Error)
	}

	close(releaseFirst)
	first := <-results
	if !first.res.Success {
		t.Fatalf("first dispatchSlot call should have succeeded once released, got error: %q", first.res.Error)
	}
}

// TestDispatchToHarness_ConcurrentIdenticalDispatchIsDeduped is the #432
// retry-discipline regression test: dispatchSlot's RequestID is a
// content-stable hash of systemPrompt+task+tool-names (for KV-cache sharing
// across fan-out slots per ADR-066), which means two independent
// DispatchToHarness(..., N:1) calls carrying identical content produce the
// same RequestID — exactly the shape of a client resubmitting a request
// whose prior attempt may still be generating server-side. The second
// concurrent call must be refused rather than starting a second server-side
// generation under the same identity.
// TestCompleteWithToolLoop_ConcurrentSameRequestIDIsDeduped is the #432
// retry-discipline regression test at the layer where the guard actually
// lives: completeWithToolLoop refuses a second call under the same
// RequestMetadata.RequestID while the first is still in flight, so a caller
// resubmitting a request whose prior attempt may still be generating
// server-side collides with the guard instead of stacking a second
// generation. Exercised directly against completeWithToolLoop (rather than
// through DispatchToHarness, whose c.ollamaMu batch-level mutex already
// fully serializes local-provider dispatches and so would never let two
// calls race at this layer) to isolate exactly the mechanism under test.
func TestCompleteWithToolLoop_ConcurrentSameRequestIDIsDeduped(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	// releaseFirst gates the first request's stream completion so the second
	// (duplicate) call has a window to arrive while the first is still
	// registered in-flight.
	releaseFirst := make(chan struct{})
	var firstArrived sync.WaitGroup
	firstArrived.Add(1)
	var firstArrivedOnce sync.Once

	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]any{{"id": "gemma4:e4b", "object": "model"}},
			})
		case "/v1/chat/completions":
			firstArrivedOnce.Do(func() { firstArrived.Done() })
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial\"}}]}\n\n")
			flusher.Flush()
			// Hold the stream open until the test releases it, simulating a
			// long-running generation the second call would race against.
			<-releaseFirst
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer llm.Close()
	t.Setenv(localLLMEndpointEnv, llm.URL)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	srv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, srv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	provider := NewOpenAICompatProvider("agent-local", ProviderConfig{
		Endpoint: llm.URL,
		Model:    "gemma4:e4b",
		Timeout:  10,
	})
	registry := NewKernelToolRegistry(srv.mcpServer)

	const sharedRequestID = "local-harness-dispatch-dedup-test-0"
	makeReq := func() *CompletionRequest {
		return &CompletionRequest{
			Messages: []ProviderMessage{{Role: "user", Content: "identical dedup-test task"}},
			Metadata: RequestMetadata{RequestID: sharedRequestID},
		}
	}

	type outcome struct {
		resp *CompletionResponse
		err  error
	}
	results := make(chan outcome, 2)

	go func() {
		resp, _, _, callErr := ctrl.completeWithToolLoop(context.Background(), provider, makeReq(), registry)
		results <- outcome{resp, callErr}
	}()
	firstArrived.Wait()

	go func() {
		resp, _, _, callErr := ctrl.completeWithToolLoop(context.Background(), provider, makeReq(), registry)
		results <- outcome{resp, callErr}
	}()

	// Give the second call a moment to reach the dedup guard before releasing
	// the first stream (avoids a race where the first completes, clears its
	// in-flight registration, and the second no longer collides).
	time.Sleep(150 * time.Millisecond)
	close(releaseFirst)

	first := <-results
	second := <-results

	dedupRefusals := 0
	successes := 0
	for _, o := range []outcome{first, second} {
		switch {
		case o.err != nil && strings.Contains(o.err.Error(), "already in flight"):
			dedupRefusals++
		case o.err == nil:
			successes++
		default:
			t.Errorf("unexpected error: %v", o.err)
		}
	}
	if dedupRefusals != 1 {
		t.Errorf("dedup refusals = %d; want exactly 1 (one of the two concurrent identical-RequestID calls must be refused)", dedupRefusals)
	}
	if successes != 1 {
		t.Errorf("successes = %d; want exactly 1", successes)
	}
}
