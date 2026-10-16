package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t) // has .cog/ structure

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Port != 6931 {
		t.Errorf("Port = %d; want 6931", cfg.Port)
	}
	// Default bind MUST be loopback — security posture for issue #12.
	// Operators opt into 0.0.0.0 explicitly; never by accident.
	if cfg.BindAddr != "127.0.0.1" {
		t.Errorf("BindAddr = %q; want default 127.0.0.1 (loopback-only)", cfg.BindAddr)
	}
	if cfg.ConsolidationInterval != 3600 {
		t.Errorf("ConsolidationInterval = %d; want 3600", cfg.ConsolidationInterval)
	}
	if cfg.HeartbeatInterval != 60 {
		t.Errorf("HeartbeatInterval = %d; want 60", cfg.HeartbeatInterval)
	}
	if cfg.SalienceDaysWindow != 90 {
		t.Errorf("SalienceDaysWindow = %d; want 90", cfg.SalienceDaysWindow)
	}
	if cfg.WorkspaceRoot != root {
		t.Errorf("WorkspaceRoot = %q; want %q", cfg.WorkspaceRoot, root)
	}
	if cfg.CogDir != filepath.Join(root, ".cog") {
		t.Errorf("CogDir = %q; want %q", cfg.CogDir, filepath.Join(root, ".cog"))
	}
	if cfg.LocalModel != defaultOllamaModel {
		t.Errorf("LocalModel = %q; want %q", cfg.LocalModel, defaultOllamaModel)
	}
	if !cfg.ToolCallValidationEnabled {
		t.Error("ToolCallValidationEnabled = false; want true by default")
	}
	if len(cfg.DigestPaths) != 0 {
		t.Errorf("DigestPaths len = %d; want 0", len(cfg.DigestPaths))
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	// Write a kernel.yaml that overrides all defaults.
	kernelYAML := `port: 9999
consolidation_interval: 600
heartbeat_interval: 120
salience_days_window: 30
local_model: gemma4:e2b
tool_call_validation_enabled: false
digest_paths:
  claude-code: ~/.claude/events.jsonl
  headless: /tmp/headless
`
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Port != 9999 {
		t.Errorf("Port = %d; want 9999", cfg.Port)
	}
	if cfg.ConsolidationInterval != 600 {
		t.Errorf("ConsolidationInterval = %d; want 600", cfg.ConsolidationInterval)
	}
	if cfg.HeartbeatInterval != 120 {
		t.Errorf("HeartbeatInterval = %d; want 120", cfg.HeartbeatInterval)
	}
	if cfg.SalienceDaysWindow != 30 {
		t.Errorf("SalienceDaysWindow = %d; want 30", cfg.SalienceDaysWindow)
	}
	if cfg.LocalModel != "gemma4:e2b" {
		t.Errorf("LocalModel = %q; want gemma4:e2b", cfg.LocalModel)
	}
	if cfg.ToolCallValidationEnabled {
		t.Error("ToolCallValidationEnabled = true; want false from file")
	}
	if cfg.DigestPaths["claude-code"] != "~/.claude/events.jsonl" {
		t.Errorf("DigestPaths[claude-code] = %q; want ~/.claude/events.jsonl", cfg.DigestPaths["claude-code"])
	}
	if cfg.DigestPaths["headless"] != "/tmp/headless" {
		t.Errorf("DigestPaths[headless] = %q; want /tmp/headless", cfg.DigestPaths["headless"])
	}
}

// TestLoadConfigDispatchTimeoutCap covers the config-driven dispatch timeout
// cap (operator directive 2026-07-04: the cap is an operator parameter, not
// hardcoded). Default (no key) resolves to DefaultDispatchTimeoutCapSeconds;
// a dispatch_timeout_cap_seconds entry in kernel.yaml overrides it; the
// accessor is nil-receiver-safe because transport adapters call it
// unconditionally.
func TestLoadConfigDispatchTimeoutCap(t *testing.T) {
	t.Parallel()

	// Default: no key in kernel.yaml.
	rootDefault := makeWorkspace(t)
	cfgDefault, err := LoadConfig(rootDefault, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := cfgDefault.DispatchTimeoutCap(); got != DefaultDispatchTimeoutCapSeconds {
		t.Errorf("default DispatchTimeoutCap() = %d; want %d", got, DefaultDispatchTimeoutCapSeconds)
	}

	// Configured: kernel.yaml raises the cap.
	rootRaised := makeWorkspace(t)
	writeTestFile(t, filepath.Join(rootRaised, ".cog", "config", "kernel.yaml"),
		"dispatch_timeout_cap_seconds: 1800\n")
	cfgRaised, err := LoadConfig(rootRaised, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := cfgRaised.DispatchTimeoutCap(); got != 1800 {
		t.Errorf("configured DispatchTimeoutCap() = %d; want 1800", got)
	}

	// Nil receiver: falls back to the default rather than panicking.
	var nilCfg *Config
	if got := nilCfg.DispatchTimeoutCap(); got != DefaultDispatchTimeoutCapSeconds {
		t.Errorf("nil-receiver DispatchTimeoutCap() = %d; want %d", got, DefaultDispatchTimeoutCapSeconds)
	}
}

// TestLoadConfigBindAddrFromYAML verifies that a `bind_addr: 0.0.0.0`
// entry in kernel.yaml parses and applies to Config. Regression test for
// cogos#12 (BindAddr declared but never wired).
func TestLoadConfigBindAddrFromYAML(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	kernelYAML := "bind_addr: 0.0.0.0\n"
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.BindAddr != "0.0.0.0" {
		t.Errorf("BindAddr = %q; want 0.0.0.0 (from YAML)", cfg.BindAddr)
	}
	// Port should still default — bind_addr shouldn't affect anything else.
	if cfg.Port != 6931 {
		t.Errorf("Port = %d; want default 6931", cfg.Port)
	}
}

// TestLoadConfigBindAddrV3SectionOverridesTopLevel mirrors the existing
// v3-override pattern for the bind_addr field.
func TestLoadConfigBindAddrV3SectionOverridesTopLevel(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	kernelYAML := `bind_addr: 127.0.0.1
v3:
  bind_addr: 0.0.0.0
`
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.BindAddr != "0.0.0.0" {
		t.Errorf("BindAddr = %q; want 0.0.0.0 (v3 section)", cfg.BindAddr)
	}
}

// TestLoadConfigBindAddrMissingKeepsLoopback verifies that YAML without a
// bind_addr key does NOT reset the default to empty — the zero-skip pattern
// must preserve the loopback default.
func TestLoadConfigBindAddrMissingKeepsLoopback(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	kernelYAML := "port: 7000\n" // bind_addr absent
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.BindAddr != "127.0.0.1" {
		t.Errorf("BindAddr = %q; want default 127.0.0.1 (YAML omitted bind_addr)", cfg.BindAddr)
	}
}

func TestLoadConfigPortFlagOverridesFile(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), "port: 7777\n")

	cfg, err := LoadConfig(root, 8888)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// Flag (8888) beats file (7777).
	if cfg.Port != 8888 {
		t.Errorf("Port = %d; want 8888 (flag override)", cfg.Port)
	}
}

func TestLoadConfigAutoDetectFails(t *testing.T) {
	t.Parallel()
	// findWorkspaceRoot fails when starting from / (no .cog ancestor).
	_, err := findWorkspaceRoot("/")
	if err == nil {
		t.Error("expected error when no .cog/ is found under /")
	}
}

func TestLoadConfigExplicitPathUsedAsIs(t *testing.T) {
	t.Parallel()
	// When a workspaceRoot is supplied explicitly, LoadConfig uses it without
	// validation (nucleus load will catch bad paths later).
	// Any non-empty string is accepted; kernel.yaml absence is silently ignored.
	cfg, err := LoadConfig(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("LoadConfig with explicit temp dir: %v", err)
	}
	if cfg.Port != 6931 {
		t.Errorf("Port = %d; want default 6931", cfg.Port)
	}
}

func TestLoadConfigMissingKernelYAMLIsOK(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	// kernel.yaml is absent → should use defaults without error.
	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig without kernel.yaml: %v", err)
	}
	if cfg.Port != 6931 {
		t.Errorf("Port = %d; want default 6931", cfg.Port)
	}
}

func TestLoadConfigV3SectionOverridesTopLevel(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	// Top-level port is 5100 (v2); v3: section overrides to 6931.
	kernelYAML := `port: 5100
consolidation_interval: 120
v3:
  port: 6931
  consolidation_interval: 600
`
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// v3: section wins over top-level.
	if cfg.Port != 6931 {
		t.Errorf("Port = %d; want 6931 (v3 section)", cfg.Port)
	}
	if cfg.ConsolidationInterval != 600 {
		t.Errorf("ConsolidationInterval = %d; want 600 (v3 section)", cfg.ConsolidationInterval)
	}
}

func TestLoadConfigV3SectionPartialOverride(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	// v3: only overrides heartbeat; consolidation comes from top level.
	kernelYAML := `consolidation_interval: 180
v3:
  heartbeat_interval: 30
`
	writeTestFile(t, filepath.Join(root, ".cog", "config", "kernel.yaml"), kernelYAML)

	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.ConsolidationInterval != 180 {
		t.Errorf("ConsolidationInterval = %d; want 180 (top level)", cfg.ConsolidationInterval)
	}
	if cfg.HeartbeatInterval != 30 {
		t.Errorf("HeartbeatInterval = %d; want 30 (v3 section)", cfg.HeartbeatInterval)
	}
}

func TestFindWorkspaceRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".cog", "config"), 0755); err != nil {
		t.Fatal(err)
	}

	// Start from a nested subdirectory.
	nested := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatal(err)
	}

	got, err := findWorkspaceRoot(nested)
	if err != nil {
		t.Fatalf("findWorkspaceRoot: %v", err)
	}
	if got != root {
		t.Errorf("findWorkspaceRoot = %q; want %q", got, root)
	}
}

func TestFindWorkspaceRootSkipsNestedCogWithoutConfig(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".cog", "config"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "apps", ".cog", "mem"), 0755); err != nil {
		t.Fatal(err)
	}

	nested := filepath.Join(root, "apps", "cogos-v3")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatal(err)
	}

	got, err := findWorkspaceRoot(nested)
	if err != nil {
		t.Fatalf("findWorkspaceRoot: %v", err)
	}
	if got != root {
		t.Errorf("findWorkspaceRoot = %q; want %q", got, root)
	}
}

func TestFindWorkspaceRootNotFound(t *testing.T) {
	t.Parallel()
	// Filesystem root has no .cog directory.
	_, err := findWorkspaceRoot("/")
	if err == nil {
		t.Error("expected error when .cog not found")
	}
}
