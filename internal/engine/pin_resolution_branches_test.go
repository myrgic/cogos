package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// TestDispatchToHarness_ExplicitProvider_RecordsPin is the negative control
// for the cog-review objection on PR #601: the pin-recording instrumentation
// originally only covered the legacy local-LLM probe branch (Path 3) of
// DispatchToHarness's four model-resolution paths. This test exercises the
// explicit-named-provider branch (Path 1, req.Provider != "") and asserts a
// pin resolution is recorded for it under site "dispatch:explicit-provider".
//
// Before the fix: recordPinResolution is never called on this branch, so
// snapshotPinResolutions() contains no "dispatch:explicit-provider" entry
// and this test fails.
// After the fix: the branch calls recordPinResolution, and the test passes.
func TestDispatchToHarness_ExplicitProvider_RecordsPin(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma4:e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "chatcmpl-test",
				"object":  "chat.completion",
				"model":   "gemma4:e4b",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  bench:
    type: openai-compat
    endpoint: `+srv.URL+`
    model: gemma4:e4b
routing:
  default: bench
`)

	resetPinResolutionsForTest()

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	testSrv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, testSrv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "explicit-provider pin test",
		N:              1,
		TimeoutSeconds: 10,
		Provider:       "bench",
	})
	t.Logf("dispatch err (informational): %v", dispErr)

	found := false
	for _, p := range snapshotPinResolutions() {
		if p.Site == "dispatch:explicit-provider" {
			found = true
			if p.Resolved != "gemma4:e4b" {
				t.Errorf("dispatch:explicit-provider pin Resolved = %q, want %q", p.Resolved, "gemma4:e4b")
			}
		}
	}
	if !found {
		t.Fatalf("no pin resolution recorded for site %q; explicit-provider branch is not instrumented", "dispatch:explicit-provider")
	}
}

// TestDispatchToHarness_ModelAlias_PinReasonDeclared is the regression test
// for the second cog-review round on PR #601: dispatch:model-alias always
// builds a descriptive note (for Detail) regardless of outcome, so running it
// through classifyPinNote's note=="" heuristic misclassified every single
// resolution on this branch as "fallback:other" — there is no config default
// to diverge from on this path (the caller explicitly named the alias/model),
// so every resolution here must report "declared".
func TestDispatchToHarness_ModelAlias_PinReasonDeclared(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"codex-model","object":"model"}]}`))
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": "codex-model",
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  codex:
    type: openai-compat
    endpoint: `+srv.URL+`
    model: codex-model
routing:
  default: codex
`)

	resetPinResolutionsForTest()

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	testSrv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, testSrv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	// "codex" is a recognised intentAliases entry (PreferProvider: "codex"),
	// so this exercises Path 0 (dispatch:model-alias) rather than the legacy
	// probe.
	_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
		Task:           "model-alias pin test",
		Model:          DispatchModel("codex"),
		N:              1,
		TimeoutSeconds: 10,
	})
	t.Logf("dispatch err (informational): %v", dispErr)

	found := false
	for _, p := range snapshotPinResolutions() {
		if p.Site == "dispatch:model-alias" {
			found = true
			if p.Reason != PinReasonDeclared {
				t.Errorf("dispatch:model-alias Reason = %q, want %q (this branch never diverges from a config default)", p.Reason, PinReasonDeclared)
			}
		}
	}
	if !found {
		t.Fatalf("no pin resolution recorded for site %q; model-alias branch is not instrumented", "dispatch:model-alias")
	}
}

// TestDispatchToHarness_HarnessProvider_PinReasonTyped covers Path 2.5
// (cfg.HarnessProvider default): the no-override resolution must report
// "declared" and the caller-overriding resolution must report
// PinReasonOverridden — before the fix both reported "fallback:other"
// because this branch's note is unconditionally non-empty.
func TestDispatchToHarness_HarnessProvider_PinReasonTyped(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-content-hash-id","object":"model"}]}`))
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			var body struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": body.Model,
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-darkstar:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-content-hash-id
`)
	cfg.HarnessProvider = "lmstudio-darkstar"

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	testSrv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, testSrv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	t.Run("no override reports declared", func(t *testing.T) {
		resetPinResolutionsForTest()
		_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
			Task:           "harness-provider declared pin test",
			N:              1,
			TimeoutSeconds: 10,
		})
		t.Logf("dispatch err (informational): %v", dispErr)

		found := false
		for _, p := range snapshotPinResolutions() {
			if p.Site == "dispatch:harness-provider" {
				found = true
				if p.Reason != PinReasonDeclared {
					t.Errorf("dispatch:harness-provider Reason = %q, want %q", p.Reason, PinReasonDeclared)
				}
			}
		}
		if !found {
			t.Fatalf("no pin resolution recorded for site %q", "dispatch:harness-provider")
		}
	})

	t.Run("explicit model reports overridden", func(t *testing.T) {
		resetPinResolutionsForTest()
		_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
			Task:           "harness-provider overridden pin test",
			Model:          DispatchModel("ornith-1.0-35b"),
			N:              1,
			TimeoutSeconds: 10,
		})
		t.Logf("dispatch err (informational): %v", dispErr)

		found := false
		for _, p := range snapshotPinResolutions() {
			if p.Site == "dispatch:harness-provider" {
				found = true
				if p.Reason != PinReasonOverridden {
					t.Errorf("dispatch:harness-provider Reason = %q, want %q", p.Reason, PinReasonOverridden)
				}
			}
		}
		if !found {
			t.Fatalf("no pin resolution recorded for site %q", "dispatch:harness-provider")
		}
	})
}

// TestDispatchToHarness_StateRouting_PinReasonTyped covers Path 2
// (process_state_routing): the no-override resolution must report "declared"
// and the caller-overriding resolution must report PinReasonOverridden.
func TestDispatchToHarness_StateRouting_PinReasonTyped(t *testing.T) {
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gemma-4-e4b","object":"model"}]}`))
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			var body struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-test", "object": "chat.completion", "model": body.Model,
				"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "ok"}, "finish_reason": "stop"}},
				"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
		}
	}))
	defer srv.Close()

	writeTestFile(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  ollama:
    type: ollama
    endpoint: http://localhost:11434
    model: gemma4:e4b
  mlx-lm:
    type: openai
    endpoint: `+srv.URL+`
    model: gemma-4-e4b
routing:
  default: ollama
  fallback_chain: [mlx-lm, ollama]
  process_state_routing:
    receptive: mlx-lm
`)

	proc := NewProcess(cfg, makeNucleus("Cog", "tester"))
	testSrv := NewServer(cfg, makeNucleus("Cog", "tester"), proc)
	ctrl, err := NewLocalHarnessController(cfg, makeNucleus("Cog", "tester"), proc, testSrv.mcpServer)
	if err != nil {
		t.Fatalf("NewLocalHarnessController: %v", err)
	}

	t.Run("no override reports declared", func(t *testing.T) {
		resetPinResolutionsForTest()
		_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
			Task:           "state-routing declared pin test",
			N:              1,
			TimeoutSeconds: 10,
		})
		t.Logf("dispatch err (informational): %v", dispErr)

		found := false
		for _, p := range snapshotPinResolutions() {
			if p.Site == "dispatch:state-routing" {
				found = true
				if p.Reason != PinReasonDeclared {
					t.Errorf("dispatch:state-routing Reason = %q, want %q", p.Reason, PinReasonDeclared)
				}
			}
		}
		if !found {
			t.Fatalf("no pin resolution recorded for site %q", "dispatch:state-routing")
		}
	})

	t.Run("explicit model reports overridden", func(t *testing.T) {
		resetPinResolutionsForTest()
		_, dispErr := ctrl.DispatchToHarness(context.Background(), DispatchRequest{
			Task:           "state-routing overridden pin test",
			Model:          DispatchModel("ornith-1.0-35b"),
			N:              1,
			TimeoutSeconds: 10,
		})
		t.Logf("dispatch err (informational): %v", dispErr)

		found := false
		for _, p := range snapshotPinResolutions() {
			if p.Site == "dispatch:state-routing" {
				found = true
				if p.Reason != PinReasonOverridden {
					t.Errorf("dispatch:state-routing Reason = %q, want %q", p.Reason, PinReasonOverridden)
				}
			}
		}
		if !found {
			t.Fatalf("no pin resolution recorded for site %q", "dispatch:state-routing")
		}
	})
}
