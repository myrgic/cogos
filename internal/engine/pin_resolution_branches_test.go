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
