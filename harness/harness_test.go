// harness_test.go tests the harness package's tool pipeline, Claude CLI
// argument construction, and shared test doubles.
package harness

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// execCommand returns an exec.Cmd for the given binary with no arguments.
// It exists as a thin wrapper so tests can call it without importing os/exec directly.
func execCommand(binary string) *exec.Cmd {
	return exec.Command(binary) //nolint:gosec
}

// === BuildClaudeArgs TESTS ===

func TestBuildClaudeArgs_NoTools(t *testing.T) {
	req := &InferenceRequest{
		Prompt: "hello",
		Model:  "claude",
	}
	args := BuildClaudeArgs(req)
	for _, arg := range args {
		if arg == "--allowed-tools" {
			t.Error("--allowed-tools should not be present when no tools set")
		}
	}
}

func TestBuildClaudeArgs_ExplicitAllowedTools(t *testing.T) {
	req := &InferenceRequest{
		Prompt:       "hello",
		AllowedTools: []string{"Bash", "Read", "Write"},
	}
	args := BuildClaudeArgs(req)
	found := false
	for i, arg := range args {
		if arg == "--allowed-tools" && i+1 < len(args) {
			if args[i+1] != "Bash,Read,Write" {
				t.Errorf("expected 'Bash,Read,Write', got %q", args[i+1])
			}
			found = true
		}
	}
	if !found {
		t.Error("--allowed-tools not found in args")
	}
}

func TestBuildClaudeArgs_OpenAITools(t *testing.T) {
	req := &InferenceRequest{
		Prompt: "hello",
		Tools: []json.RawMessage{
			json.RawMessage(`{"type":"function","function":{"name":"bash"}}`),
			json.RawMessage(`{"type":"function","function":{"name":"read"}}`),
		},
	}
	args := BuildClaudeArgs(req)
	found := false
	for i, arg := range args {
		if arg == "--allowed-tools" {
			found = true
			if i+1 < len(args) {
				val := args[i+1]
				if !strings.Contains(val, "Bash") || !strings.Contains(val, "Read") {
					t.Errorf("expected mapped tools Bash,Read, got %q", val)
				}
			}
		}
	}
	if !found {
		t.Error("--allowed-tools should be present when Tools are set")
	}
}

func TestBuildClaudeArgs_AllowedToolsPriority(t *testing.T) {
	req := &InferenceRequest{
		Prompt:       "hello",
		AllowedTools: []string{"Bash"},
		Tools: []json.RawMessage{
			json.RawMessage(`{"type":"function","function":{"name":"read"}}`),
		},
	}
	args := BuildClaudeArgs(req)
	for i, arg := range args {
		if arg == "--allowed-tools" && i+1 < len(args) {
			if args[i+1] != "Bash" {
				t.Errorf("AllowedTools should take priority, got %q", args[i+1])
			}
		}
	}
}

func TestBuildClaudeArgs_MCPConfig(t *testing.T) {
	req := &InferenceRequest{
		Prompt:    "hello",
		MCPConfig: "/tmp/test-mcp.json",
	}
	args := BuildClaudeArgs(req)
	found := false
	for i, arg := range args {
		if arg == "--mcp-config" && i+1 < len(args) {
			if args[i+1] != "/tmp/test-mcp.json" {
				t.Errorf("expected /tmp/test-mcp.json, got %q", args[i+1])
			}
			found = true
		}
	}
	if !found {
		t.Error("--mcp-config not found in args when MCPConfig is set")
	}
}

func TestBuildClaudeArgs_SystemPromptChaining(t *testing.T) {
	req := &InferenceRequest{
		Prompt:       "hello",
		SystemPrompt: "Be helpful",
		ContextState: &ContextState{
			Tier1Identity: &ContextTier{Content: "You are Cog"},
		},
	}
	args := BuildClaudeArgs(req)
	found := false
	for i, arg := range args {
		if arg == "--append-system-prompt" && i+1 < len(args) {
			val := args[i+1]
			if !strings.Contains(val, "You are Cog") || !strings.Contains(val, "Be helpful") {
				t.Errorf("expected chained system prompt, got %q", val)
			}
			found = true
		}
	}
	if !found {
		t.Error("--append-system-prompt not found when SystemPrompt + ContextState set")
	}
}

// === MapToolsToCLINames TESTS ===

func TestMapToolsToCLINames(t *testing.T) {
	tools := []json.RawMessage{
		json.RawMessage(`{"type":"function","function":{"name":"exec","description":"Execute command"}}`),
		json.RawMessage(`{"type":"function","function":{"name":"read","description":"Read file"}}`),
		json.RawMessage(`{"type":"function","function":{"name":"exec","description":"Duplicate"}}`),
	}
	names := MapToolsToCLINames(tools)
	if len(names) != 2 {
		t.Errorf("expected 2 unique names, got %d: %v", len(names), names)
	}
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet["Bash"] {
		t.Error("expected Bash in mapped names")
	}
	if !nameSet["Read"] {
		t.Error("expected Read in mapped names")
	}
}

func TestMapToolsToCLINames_UnknownSkipped(t *testing.T) {
	tools := []json.RawMessage{
		json.RawMessage(`{"type":"function","function":{"name":"bash"}}`),
		json.RawMessage(`{"type":"function","function":{"name":"custom_magic_tool"}}`),
	}
	names := MapToolsToCLINames(tools)
	if len(names) != 1 {
		t.Errorf("expected 1 name (unknown skipped), got %d: %v", len(names), names)
	}
	if len(names) > 0 && names[0] != "Bash" {
		t.Errorf("expected Bash, got %q", names[0])
	}
}

func TestMapToolsToCLINames_Empty(t *testing.T) {
	names := MapToolsToCLINames(nil)
	if len(names) != 0 {
		t.Errorf("expected 0 names for nil input, got %d", len(names))
	}
	names = MapToolsToCLINames([]json.RawMessage{})
	if len(names) != 0 {
		t.Errorf("expected 0 names for empty input, got %d", len(names))
	}
}

// === Test doubles ===

// mockKernel implements KernelServices for testing.
type mockKernel struct {
	workspaceRoot string
	mcpTools      []MCPTool
}

func (m *mockKernel) WorkspaceRoot() string { return m.workspaceRoot }
func (m *mockKernel) DispatchHook(event string, data map[string]any) *HookResult {
	return nil
}
func (m *mockKernel) EmitEvent(eventType string, data map[string]any) error { return nil }
func (m *mockKernel) ReadContinuationState() (*ContinuationState, error) {
	return &ContinuationState{}, nil
}
func (m *mockKernel) DepositSignal(location, signalType, agentID string, halfLife float64, meta map[string]any) error {
	return nil
}
func (m *mockKernel) RemoveSignal(location, signalType string) error { return nil }
func (m *mockKernel) ResolveWorkDir(requestWorkspace string) string {
	if requestWorkspace != "" {
		return requestWorkspace
	}
	return m.workspaceRoot
}
func (m *mockKernel) ConvertOpenAIToolsToMCP(tools []json.RawMessage) []MCPTool {
	return m.mcpTools
}
func (m *mockKernel) GetAgentToolPolicy(agentID string) (*AgentToolPolicy, error) {
	return nil, nil
}

// === Provider routing TESTS ===

func TestParseModelProvider_DefaultClaude(t *testing.T) {
	pt, model, _ := ParseModelProvider("")
	if pt != ProviderClaude {
		t.Errorf("empty model should route to Claude, got %v", pt)
	}
	if model != "claude" {
		t.Errorf("expected 'claude' model name, got %q", model)
	}
}

func TestParseModelProvider_ExplicitClaude(t *testing.T) {
	pt, _, _ := ParseModelProvider("claude")
	if pt != ProviderClaude {
		t.Errorf("'claude' should route to Claude, got %v", pt)
	}
}

func TestParseModelProvider_OpenAI(t *testing.T) {
	pt, model, _ := ParseModelProvider("openai/gpt-4o")
	if pt != ProviderOpenAI {
		t.Errorf("expected OpenAI, got %v", pt)
	}
	if model != "gpt-4o" {
		t.Errorf("expected gpt-4o, got %q", model)
	}
}

func TestParseModelProvider_Ollama(t *testing.T) {
	pt, model, _ := ParseModelProvider("ollama/llama3.2")
	if pt != ProviderOllama {
		t.Errorf("expected Ollama, got %v", pt)
	}
	if model != "llama3.2" {
		t.Errorf("expected llama3.2, got %q", model)
	}
}

func TestParseModelProvider_CustomHTTP(t *testing.T) {
	pt, model, config := ParseModelProvider("http://myhost:8080|my-model")
	if pt != ProviderCustom {
		t.Errorf("expected Custom, got %v", pt)
	}
	if model != "my-model" {
		t.Errorf("expected my-model, got %q", model)
	}
	if config == nil {
		t.Fatal("expected non-nil config for custom provider")
	}
	if config.BaseURL != "http://myhost:8080" {
		t.Errorf("expected http://myhost:8080, got %q", config.BaseURL)
	}
}

// === Config resolution TESTS ===

func TestLoadInferenceConfig_Empty(t *testing.T) {
	cfg := LoadInferenceConfig("/nonexistent/path")
	if cfg == nil {
		t.Fatal("LoadInferenceConfig should always return non-nil")
	}
	// With no config files, fields should be zero-valued
	if cfg.DefaultProvider != "" {
		t.Errorf("expected empty default provider, got %q", cfg.DefaultProvider)
	}
}

// === ErrorType TESTS ===

func TestClassifyError_RateLimit(t *testing.T) {
	tests := []struct {
		msg      string
		expected ErrorType
	}{
		{"429 too many requests", ErrorRateLimit},
		{"rate limit exceeded", ErrorRateLimit},
		{"auth error: unauthorized", ErrorAuth},
		{"connection timeout", ErrorTransient},
		{"some unknown error", ErrorTransient}, // default
	}
	for _, tc := range tests {
		t.Run(tc.msg, func(t *testing.T) {
			got := ClassifyError(errorString(tc.msg))
			if got != tc.expected {
				t.Errorf("ClassifyError(%q) = %v, want %v", tc.msg, got, tc.expected)
			}
		})
	}
}

func TestClassifyHTTPError(t *testing.T) {
	tests := []struct {
		code     int
		expected ErrorType
	}{
		{401, ErrorAuth},
		{403, ErrorAuth},
		{429, ErrorRateLimit},
		{500, ErrorTransient},
		{503, ErrorTransient},
		{400, ErrorFatal},
	}
	for _, tc := range tests {
		got := ClassifyHTTPError(tc.code)
		if got != tc.expected {
			t.Errorf("ClassifyHTTPError(%d) = %v, want %v", tc.code, got, tc.expected)
		}
	}
}

// errorString implements the error interface for test strings.
type errorString string

func (e errorString) Error() string { return string(e) }

// === filteredEnviron / env-isolation TESTS (regression for issue #137) ===

// TestFilteredEnviron_StripsIngressVars asserts that filteredEnviron() drops
// the five routing/proxy keys that can redirect a spawned provider subprocess
// back into the CogOS kernel (the same bug fixed for internal/engine/ in
// issue #59 / PR #130).
func TestFilteredEnviron_StripsIngressVars(t *testing.T) {
	for _, key := range harnessIngressVars {
		t.Setenv(key, "http://should-be-stripped.example.com")
	}

	env := filteredEnviron()

	for _, kv := range env {
		for _, key := range harnessIngressVars {
			if strings.HasPrefix(kv, key+"=") {
				t.Errorf("ingress var %s must be stripped from filteredEnviron, found %q", key, kv)
			}
		}
	}
}

// TestFilteredEnviron_StripsCLAUDECODE asserts that CLAUDECODE is removed so
// that a spawned `claude` CLI can start inside a Claude Code session.
func TestFilteredEnviron_StripsCLAUDECODE(t *testing.T) {
	t.Setenv("CLAUDECODE", "1")

	env := filteredEnviron()

	for _, kv := range env {
		if strings.HasPrefix(kv, "CLAUDECODE=") {
			t.Errorf("CLAUDECODE must be stripped from filteredEnviron, found %q", kv)
		}
	}
}

// TestFilteredEnviron_PreservesUnrelatedVars asserts that PATH and similar
// unrelated variables are retained after filtering.
func TestFilteredEnviron_PreservesUnrelatedVars(t *testing.T) {
	t.Parallel()

	path := os.Getenv("PATH")
	if path == "" {
		t.Skip("PATH not set in test environment")
	}

	env := filteredEnviron()

	found := false
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			found = true
			break
		}
	}
	if !found {
		t.Error("PATH must be preserved in filteredEnviron")
	}
}

// TestFilteredEnviron_InjectsHarnessMarker asserts that COGOS_HARNESS=1 is
// appended so Python hooks know they are running under the kernel.
func TestFilteredEnviron_InjectsHarnessMarker(t *testing.T) {
	env := filteredEnviron()

	found := false
	for _, kv := range env {
		if kv == "COGOS_HARNESS=1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("COGOS_HARNESS=1 must be present in filteredEnviron output")
	}
}

// TestSpawnedProcess_DoesNotInheritIngressVars is the end-to-end regression
// test for issue #137. It spawns a real subprocess via a fake shell script that
// writes its environment to a temp file, then asserts none of the ingress vars
// (nor CLAUDECODE) are present in the child env. This mirrors
// TestClaudeCodeComplete_DoesNotInheritBaseURL in internal/engine/provider_env_test.go
// but exercises the harness filteredEnviron() path used by claude.go and codex.go.
func TestSpawnedProcess_DoesNotInheritIngressVars(t *testing.T) {
	// Set all ingress vars + CLAUDECODE in the parent process.
	for _, key := range harnessIngressVars {
		t.Setenv(key, "http://kernel.local:6931/should-not-leak")
	}
	t.Setenv("CLAUDECODE", "1")

	// Temp file for the child to record its environment into.
	envFile, err := os.CreateTemp(t.TempDir(), "child-env-*.txt")
	if err != nil {
		t.Fatalf("create env file: %v", err)
	}
	envFile.Close()

	// Fake binary: write env to envFile then exit 0.
	script, err := os.CreateTemp(t.TempDir(), "fake-provider-*.sh")
	if err != nil {
		t.Fatalf("create script: %v", err)
	}
	_, _ = script.WriteString("#!/bin/sh\n")
	_, _ = script.WriteString("env > " + envFile.Name() + "\n")
	script.Close()
	if err := os.Chmod(script.Name(), 0o755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	// Verify filteredEnviron() itself contains no forbidden vars (unit check).
	childEnv := filteredEnviron()
	forbidden := append([]string{"CLAUDECODE"}, harnessIngressVars...)
	for _, kv := range childEnv {
		for _, key := range forbidden {
			if strings.HasPrefix(kv, key+"=") {
				t.Errorf("filteredEnviron: var %s must be stripped, got %q", key, kv)
			}
		}
	}

	// Spawn a real subprocess with filteredEnviron() applied and read what it saw.
	cmd := execCommand(script.Name())
	cmd.Env = childEnv
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fake provider failed: %v\n%s", err, out)
	}

	data, err := os.ReadFile(envFile.Name())
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		for _, key := range forbidden {
			if strings.HasPrefix(line, key+"=") {
				t.Errorf("child process received %s: %q", key, line)
			}
		}
	}
}
