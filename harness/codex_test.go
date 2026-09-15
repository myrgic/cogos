package harness

import (
	"strings"
	"testing"
)

// resetCodexHomeDirForTest overrides codexHomeDir to point at a fixture
// directory for the duration of the test, restoring the original on cleanup.
// Mirrors internal/engine/provider_codex_test.go's equivalent helper.
func resetCodexHomeDirForTest(t *testing.T, dir string) {
	t.Helper()
	old := codexHomeDir
	codexHomeDir = func() string { return dir }
	t.Cleanup(func() { codexHomeDir = old })
}

func TestBuildCodexArgs_DefaultAlias(t *testing.T) {
	resetCodexHomeDirForTest(t, "testdata/codex_home_fixture")

	req := &InferenceRequest{
		Prompt: "Summarize the repo",
		Model:  "codex",
	}

	args, err := BuildCodexArgs(req, "", nil)
	if err != nil {
		t.Fatalf("BuildCodexArgs returned error: %v", err)
	}
	joined := strings.Join(args, " ")

	if !strings.Contains(joined, "exec") {
		t.Fatalf("expected exec subcommand, got %v", args)
	}
	if !strings.Contains(joined, "--json") {
		t.Fatalf("expected --json mode, got %v", args)
	}
	if !strings.Contains(joined, "--model gpt-5.6-terra") {
		t.Fatalf("expected codex alias to resolve to gpt-5.6-terra (from testdata/codex_home_fixture/config.toml), got %v", args)
	}
}

// TestDefaultCodexModel_FallsBackWhenConfigMissing guards the "no ~/.codex at
// all" path: defaultCodexModel must still return a non-empty model id
// (fallbackCodexModel) rather than "", so BuildCodexArgs never emits a bare
// `--model` with no value.
func TestDefaultCodexModel_FallsBackWhenConfigMissing(t *testing.T) {
	resetCodexHomeDirForTest(t, "testdata/does-not-exist")

	got := defaultCodexModel()
	if got != fallbackCodexModel {
		t.Fatalf("defaultCodexModel() = %q, want fallback %q when config.toml is missing", got, fallbackCodexModel)
	}
}

// TestDefaultCodexModel_IgnoresNestedTableModel mirrors
// internal/engine/provider_codex_test.go's equivalent: a `model = "..."` key
// nested inside a later [table] must not shadow the real top-level default.
func TestDefaultCodexModel_IgnoresNestedTableModel(t *testing.T) {
	resetCodexHomeDirForTest(t, "testdata/codex_home_fixture")

	got := defaultCodexModel()
	if got != "gpt-5.6-terra" {
		t.Fatalf("defaultCodexModel() = %q, want top-level %q (nested table's model must not shadow it)", got, "gpt-5.6-terra")
	}
}

func TestBuildCodexArgs_SystemPromptEmbedded(t *testing.T) {
	req := &InferenceRequest{
		Prompt:       "Fix the failing test",
		Model:        "codex",
		SystemPrompt: "Only touch test files.",
	}

	args, err := BuildCodexArgs(req, "", nil)
	if err != nil {
		t.Fatalf("BuildCodexArgs returned error: %v", err)
	}
	lastArg := args[len(args)-1]

	if !strings.Contains(lastArg, "System instructions:") {
		t.Fatalf("expected system prompt preamble, got %q", lastArg)
	}
	if !strings.Contains(lastArg, "Only touch test files.") {
		t.Fatalf("expected embedded system prompt, got %q", lastArg)
	}
}

func TestBuildCodexArgs_ResumeUsesSessionID(t *testing.T) {
	req := &InferenceRequest{
		Prompt:          "Continue",
		Model:           "codex",
		ClaudeSessionID: "thread-123",
	}

	args, err := BuildCodexArgs(req, "", nil)
	if err != nil {
		t.Fatalf("BuildCodexArgs returned error: %v", err)
	}
	joined := strings.Join(args, " ")

	if !strings.Contains(joined, "resume") {
		t.Fatalf("expected resume subcommand, got %v", args)
	}
	if !strings.Contains(joined, "resume thread-123") {
		t.Fatalf("expected session id after resume, got %v", args)
	}
}

func TestBuildCodexArgs_WithMCPBridge(t *testing.T) {
	kernel := &mockKernel{workspaceRoot: "/tmp/test-ws"}
	req := &InferenceRequest{
		Prompt:        "Use tools",
		Model:         "codex",
		OpenClawURL:   "http://localhost:18789",
		OpenClawToken: "secret",
		SessionID:     "session-123",
	}

	args, err := BuildCodexArgs(req, "", kernel)
	if err != nil {
		t.Fatalf("BuildCodexArgs returned error: %v", err)
	}
	joined := strings.Join(args, " ")

	if !strings.Contains(joined, "mcp_servers.cogos_bridge.command=") {
		t.Fatalf("expected MCP bridge command override, got %v", args)
	}
	if !strings.Contains(joined, "mcp_servers.cogos_bridge.env=") {
		t.Fatalf("expected MCP bridge env override, got %v", args)
	}
	if !strings.Contains(joined, "session-123") {
		t.Fatalf("expected session id to be forwarded in MCP env, got %v", args)
	}
}

func TestParseCodexOutputJSONL(t *testing.T) {
	output := []byte(strings.Join([]string{
		`{"type":"thread.started","thread_id":"thread-123"}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"hello from codex"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":12,"cached_input_tokens":3,"output_tokens":7}}`,
	}, "\n"))

	state := parseCodexOutput(output)

	if state.sessionID != "thread-123" {
		t.Fatalf("expected session id thread-123, got %q", state.sessionID)
	}
	if state.content != "hello from codex" {
		t.Fatalf("expected final content, got %q", state.content)
	}
	if state.promptTokens != 12 || state.completionTokens != 7 {
		t.Fatalf("unexpected usage: %+v", state)
	}
	if state.cacheReadTokens != 3 {
		t.Fatalf("expected cached input tokens to map to cacheReadTokens, got %+v", state)
	}
}

func TestParseModelProvider_Codex(t *testing.T) {
	pt, model, _ := ParseModelProvider("codex")
	if pt != ProviderCodex || model != "codex" {
		t.Fatalf("expected codex provider, got provider=%q model=%q", pt, model)
	}

	pt, model, _ = ParseModelProvider("codex/codex-mini-latest")
	if pt != ProviderCodex || model != "codex-mini-latest" {
		t.Fatalf("expected codex provider with explicit model, got provider=%q model=%q", pt, model)
	}
}
