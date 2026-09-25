// config.go — CogOS v3 configuration loading
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/myrgic/cogos/internal/engine/inference"
)

// Default context-gating knobs for the foveated assembler.
//
// See .cog/scratch/audit-dashboard-context/REPORT.md §4 for the rationale —
// without a max-docs cap or salience floor the chat-path admits every
// indexed CogDoc with non-zero salience, which can fill a 32K budget with
// 400+ inbox manifest entries and starve conversation history of room.
const (
	DefaultMaxFovealDocs = 10
	DefaultSalienceFloor = 0.3
	// DefaultBudget is the fallback token budget for context assembly when no
	// per-request override (X-Cogos-Context-Budget header or MCP budget field)
	// or kernel.yaml default_budget is provided. Matches the default provider
	// context_window in providers.yaml.
	DefaultBudget = 32768
	// DefaultDispatchTimeoutCapSeconds is the ceiling on a dispatch's
	// timeout_seconds when kernel.yaml sets no dispatch_timeout_cap_seconds.
	// Aliased by dispatchTimeoutCapDefault (agent_dispatch_query.go) so
	// Normalize stays config-free while sharing this single number.
	// Operator directive 2026-07-04: the cap is an operator parameter, not a
	// hardcoded limit — agentic workflows push dispatch budgets past any
	// fixed number.
	DefaultDispatchTimeoutCapSeconds = 600
)

// hasUsableCogConfig reports whether dir looks like a real workspace root for
// v3 rather than a nested helper directory that happens to contain .cog/.
func hasUsableCogConfig(dir string) bool {
	configDir := filepath.Join(dir, ".cog", "config")
	info, err := os.Stat(configDir)
	return err == nil && info.IsDir()
}

// Config holds all runtime configuration for the v3 kernel.
type Config struct {
	// WorkspaceRoot is the absolute path to the cog-workspace root.
	WorkspaceRoot string

	// CogDir is WorkspaceRoot/.cog
	CogDir string

	// Port the HTTP API listens on. Default: 6931 (ln(2) × 10⁴).
	Port int

	// BindAddr is the interface the HTTP API binds to.
	// Default: "127.0.0.1" (loopback-only). Set to "0.0.0.0" to listen
	// on all interfaces — required for pod/LAN/Tailnet deployments.
	// Users opting in to non-loopback binds are expected to handle the
	// network boundary themselves (trusted network, VPN, firewall).
	BindAddr string

	// ConsolidationInterval is how often the consolidation loop fires (seconds).
	ConsolidationInterval int

	// HeartbeatInterval is the dormant-state heartbeat cadence (seconds).
	HeartbeatInterval int

	// SalienceDaysWindow is the git history window for salience scoring.
	SalienceDaysWindow int

	// OutputReserve is tokens reserved for model generation (subtracted from budget).
	OutputReserve int

	// MaxFovealDocs caps the number of CogDocs admitted into the foveated
	// context window after sorting. 0 means use DefaultMaxFovealDocs.
	// Hot-tunable via PATCH /v1/settings/context; access through
	// ContextGating()/SetContextGating() so concurrent chat requests see
	// a consistent snapshot.
	MaxFovealDocs int

	// SalienceFloor is the minimum salience score a CogDoc must reach to
	// be admitted by the keyword-and-field branch of the assembler. Drops
	// inbox-only enrichment boosts (~0.2) while keeping ordinary workspace
	// files. 0 means use DefaultSalienceFloor.
	SalienceFloor float64

	// DefaultBudget is the token budget used when the caller does not supply
	// a per-request override (X-Cogos-Context-Budget header or MCP budget
	// field). 0 means use the package-level DefaultBudget constant (32768).
	// Set via default_budget in kernel.yaml.
	DefaultBudget int

	// ExcludeSubstrings is a list of path substrings. Any CogDoc whose
	// slash-normalised path contains one of these substrings is excluded from
	// the foveated context window for chat requests. Useful to keep large or
	// sensitive path trees (e.g. /inbox/, /archive/, /vendor/) out of ambient
	// context without removing the files from the corpus entirely.
	// Configured via exclude_substrings in kernel.yaml. Substring (not glob)
	// semantics — implementation uses strings.Contains, not filepath.Match.
	ExcludeSubstrings []string

	// gatingMu guards the gating knobs above for hot-update via the
	// /v1/settings/context endpoints.
	gatingMu sync.RWMutex

	// TRMWeightsPath is the path to the TRM binary weights file.
	// If empty, TRM is disabled and keyword+salience scoring is used.
	TRMWeightsPath string

	// TRMEmbeddingsPath is the path to the TRM embedding index binary.
	TRMEmbeddingsPath string

	// TRMChunksPath is the path to the TRM chunk metadata JSON.
	TRMChunksPath string

	// OllamaEmbedEndpoint is the Ollama /api/embeddings endpoint URL.
	// Default: http://localhost:11434
	OllamaEmbedEndpoint string

	// OllamaEmbedModel is the embedding model name for Ollama.
	// Default (when empty): bge-m3:latest — see trm_context.go defaultEmbedModel.
	// Prefix-aware encoders (nomic-embed-text) automatically receive
	// "search_query: " / "search_document: " prefixes; others receive raw text.
	OllamaEmbedModel string

	// ToolCallValidationEnabled gates runtime validation for model-emitted tool calls.
	// Providers that advertise CapToolUse are trusted and skip this guardrail.
	ToolCallValidationEnabled bool

	// EnableSkillExec gates the POST /v1/skills/{name}/exec HTTP endpoint.
	// Default false: any local process on the same host could otherwise plant
	// a workspace skill and trigger code execution via loopback. The CLI
	// (`cog skill exec`) is unaffected — it runs in the user's own session.
	EnableSkillExec bool

	// EnableServiceControl gates the service mutation endpoints:
	// POST /v1/services/{name}/start|stop|restart|enable|disable.
	// Default false: service mutations via HTTP are disabled by default because
	// any local process on the same host could otherwise trigger launchctl
	// operations. Set enable_service_control: true in kernel.yaml to opt in.
	EnableServiceControl bool

	// EnableConfigMutation gates the config-mutation HTTP endpoints:
	// GET/PATCH /v1/config and POST /v1/config/rollback.
	// Default false: live config read/mutation/rollback via HTTP is disabled by
	// default because any local process on the same host could otherwise read
	// or rewrite kernel.yaml. Set enable_config_mutation: true in kernel.yaml
	// to opt in.
	EnableConfigMutation bool

	// EnableReconcileControl gates the reconcile mutation endpoint:
	// POST /v1/reconcile/{type}/resume (see serve_reconcile_resume.go).
	// Default false: any local process on the same host could otherwise lift
	// a provider's quarantine and force an immediate reconcile cycle over
	// loopback. Set enable_reconcile_control: true in kernel.yaml to opt in.
	// Same opt-in-default-off pattern as EnableSkillExec / EnableServiceControl
	// / EnableConfigMutation.
	EnableReconcileControl bool

	// WriteRouteGrantAuthDisabled is the escape hatch for
	// serve_grant_auth.go's X-Cogos-Grant middleware (board 75 / the kernel
	// HTTP write-route CSRF close). Deliberately inverted polarity versus the
	// Enable* gates above: those default OFF (opt in to a risk), this one
	// must default ON (the CSRF gate is live unless explicitly turned off),
	// and Go's bool zero value is false — so "false" has to mean "auth
	// enabled" for every caller that builds a Config without going through
	// LoadConfig's explicit defaulting (tests, testkernel, any future
	// programmatic boot). A caller that forgets to set this field gets the
	// SAFE behavior (enforced), not the exposed one.
	//
	// Setting this true (disable_write_route_grant_auth: true in kernel.yaml)
	// restores pre-grant-auth behavior on every POST/PUT/PATCH/DELETE route
	// and /mcp — a broken/unavailable identity-grant registry or vault must
	// never brick the kernel loop, so this knob exists as the operator's
	// documented way out. Boot() logs a loud warning when this is true (see
	// boot.go) so a disabled gate is visible in logs, not silent.
	WriteRouteGrantAuthDisabled bool

	// DigestPaths maps stream tailer adapter names to JSONL file/directory paths.
	// Empty map means external digestion is disabled.
	DigestPaths map[string]string

	// KernelLogPath overrides the default per-workspace kernel slog JSONL sink
	// at .cog/run/kernel.log.jsonl. Leave empty for the default.
	KernelLogPath string

	// Mod3URL is the base URL (scheme + host + port) of the mod3 voice service
	// that owns per-channel communication state (voice, output device, queue)
	// keyed on kernel-issued session IDs. The kernel forwards channel-session
	// registration to this URL; mod3 remains the per-channel state owner while
	// the kernel retains identity authority (ADR-082 split).
	//
	// Default: http://localhost:7860. Override via `mod3_url` in kernel.yaml
	// (top-level or under v3:) or via the COGOS_MOD3_URL env var.
	Mod3URL string

	LocalModel string

	// HarnessProvider, when non-empty, names a provider from providers(.local).yaml
	// that the harness uses for inference. Takes precedence over the legacy
	// local_model + detectLocalLLMTarget (Ollama) probe. When empty, the harness
	// falls back to the current Ollama-probe path.
	HarnessProvider string

	// DispatchTimeoutCapSeconds is the maximum timeout_seconds a
	// cog_dispatch_to_harness call (or the HTTP dispatch endpoint) will
	// accept. Requests above the cap are rejected with invalid_input, never
	// silently clamped. 0 means use DefaultDispatchTimeoutCapSeconds (600).
	// Configurable via dispatch_timeout_cap_seconds in kernel.yaml — raise it
	// for long-running agentic dispatch workloads instead of patching code.
	DispatchTimeoutCapSeconds int

	localModelConfigured bool

	// IdentityNakedDefault controls per-session identity embedding at the
	// inference gateway (G1).
	//
	// Code default: FALSE (Go zero value; only kernel.yaml overrides it).
	// When FALSE, unbound and foreign-subject requests receive full
	// embodiment (nucleus card + AssembleContext) — the pre-G2 behaviour.
	//
	// The cog workspace sets identity_naked_default: true as of 2026-09-05:
	// a census found no HTTP client sending X-Cogos-Session-Id, so nothing is
	// bound and naked transport is the honest default there; full embodiment
	// is opt-in via cog_register_session. That is a DEPLOYMENT choice
	// recorded in that workspace's kernel.yaml, not this binary's default.
	// Flipping the code default is a behaviour change with its own test and
	// is deliberately not done in a docs-only change.
	//
	// Only requests bound to the nucleus's own subject keep full embodiment
	// regardless of this flag.
	IdentityNakedDefault bool

	// MaxToolOutputBytes caps the byte length of any cog_* MCP tool text
	// response before it is sent to the agent. Responses that exceed the cap
	// are truncated at a UTF-8 rune boundary and annotated with an explicit
	// truncation marker. 0 means use DefaultMaxToolOutputBytes (32 KiB).
	// A hard floor of MinToolOutputBytes (4 KiB) prevents absurdly low values.
	// Configurable via max_tool_output_bytes in kernel.yaml.
	MaxToolOutputBytes int

	// CoreInference is the node's declared N-tier inference contract.
	// Loaded from .cog/config/core-inference.yaml; falls back to
	// inference.DefaultCoreInferenceConfig() if the file is absent.
	CoreInference inference.CoreInferenceConfig

	// RoutingDeny is the operator-authored per-provider model deny table from
	// the optional `routing.deny:` section of kernel.yaml. Nil/empty means
	// use the hardcoded default. Installed into the package-level deny table
	// by NewServer via SetProviderModelDeny.
	RoutingDeny []ProviderDenyRule
}

// kernelConfigSection holds settings that can appear at the top level or inside v3:.
type kernelConfigSection struct {
	Port                        int      `yaml:"port"`
	BindAddr                    string   `yaml:"bind_addr"`
	ConsolidationInterval       int      `yaml:"consolidation_interval"`
	HeartbeatInterval           int      `yaml:"heartbeat_interval"`
	SalienceDaysWindow          int      `yaml:"salience_days_window"`
	OutputReserve               int      `yaml:"output_reserve"`
	MaxFovealDocs               int      `yaml:"max_foveal_docs"`
	SalienceFloor               *float64 `yaml:"salience_floor"`
	DefaultBudget               int      `yaml:"default_budget"`
	ExcludeSubstrings           []string `yaml:"exclude_substrings"`
	TRMWeightsPath              string   `yaml:"trm_weights_path"`
	TRMEmbeddingsPath           string   `yaml:"trm_embeddings_path"`
	TRMChunksPath               string   `yaml:"trm_chunks_path"`
	OllamaEmbedEndpoint         string   `yaml:"ollama_embed_endpoint"`
	OllamaEmbedModel            string   `yaml:"ollama_embed_model"`
	ToolCallValidation          *bool    `yaml:"tool_call_validation_enabled"`
	EnableSkillExec             *bool    `yaml:"enable_skill_exec"`
	EnableServiceControl        *bool    `yaml:"enable_service_control"`
	EnableConfigMutation        *bool    `yaml:"enable_config_mutation"`
	EnableReconcileControl      *bool    `yaml:"enable_reconcile_control"`
	WriteRouteGrantAuthDisabled *bool    `yaml:"disable_write_route_grant_auth"`
	IdentityNakedDefault        *bool    `yaml:"identity_naked_default,omitempty"`
	LocalModel                  string   `yaml:"local_model"`
	HarnessProvider             string   `yaml:"harness_provider"`
	// DispatchTimeoutCapSeconds caps dispatch timeout_seconds requests.
	// 0 means use DefaultDispatchTimeoutCapSeconds (600).
	DispatchTimeoutCapSeconds int               `yaml:"dispatch_timeout_cap_seconds,omitempty"`
	DigestPaths               map[string]string `yaml:"digest_paths"`
	KernelLogPath             string            `yaml:"kernel_log_path"`
	Mod3URL                   string            `yaml:"mod3_url"`
	// CoreInferencePath is an optional override for the core-inference.yaml file path.
	// When empty, LoadConfig uses workspaceRoot/.cog/config/core-inference.yaml.
	// Reserved for future use — not yet wired into the loader.
	CoreInferencePath string `yaml:"core_inference_path,omitempty"`

	// MaxToolOutputBytes caps MCP tool response text at this many bytes.
	// 0 means use DefaultMaxToolOutputBytes (32 KiB).
	// Floor: MinToolOutputBytes (4 KiB).
	MaxToolOutputBytes int `yaml:"max_tool_output_bytes,omitempty"`

	// Routing holds optional routing knobs from kernel.yaml. Currently only
	// the per-provider model deny table.
	Routing *routingConfig `yaml:"routing"`
}

// routingConfig holds the optional `routing:` section of kernel.yaml.
type routingConfig struct {
	// Deny is a list of per-provider model deny rules. When absent, the
	// kernel uses the hardcoded default (openrouter blocks first-party
	// Anthropic ids). See SetProviderModelDeny in resolve.go.
	Deny []ProviderDenyRule `yaml:"deny"`
}

// kernelConfig is the on-disk YAML shape of .cog/config/kernel.yaml.
// Top-level fields apply to all kernels; the v3: section overrides them
// for the v3 kernel specifically (allowing shared kernel.yaml across v2/v3).
type kernelConfig struct {
	kernelConfigSection `yaml:",inline"`
	V3                  kernelConfigSection `yaml:"v3"`
}

// LoadConfig builds a Config from flags + environment + .cog/config/kernel.yaml.
// Precedence: flag > env > file > default.
func LoadConfig(workspaceRoot string, port int) (*Config, error) {
	if workspaceRoot == "" {
		// Auto-detect: walk up from cwd until we find a .cog directory.
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("getwd: %w", err)
		}
		found, err := findWorkspaceRoot(wd)
		if err != nil {
			return nil, err
		}
		workspaceRoot = found
	}

	cfg := &Config{
		WorkspaceRoot:             workspaceRoot,
		CogDir:                    filepath.Join(workspaceRoot, ".cog"),
		Port:                      6931,
		BindAddr:                  "127.0.0.1",
		ConsolidationInterval:     3600,
		HeartbeatInterval:         60,
		SalienceDaysWindow:        90,
		OutputReserve:             4096,
		MaxFovealDocs:             DefaultMaxFovealDocs,
		SalienceFloor:             DefaultSalienceFloor,
		ToolCallValidationEnabled: true,
		LocalModel:                defaultOllamaModel,
		DigestPaths:               make(map[string]string),
		Mod3URL:                   "http://localhost:7860",
	}

	// Load from file if present.
	kf := filepath.Join(cfg.CogDir, "config", "kernel.yaml")
	if data, err := os.ReadFile(kf); err == nil {
		var kc kernelConfig
		if err := yaml.Unmarshal(data, &kc); err == nil {
			// Apply top-level shared settings first, then v3: section overrides.
			applyKernelSection(cfg, kc.kernelConfigSection)
			applyKernelSection(cfg, kc.V3)
		}
	}

	// Env override for the mod3 URL. Env wins over file; flags stay flag-only
	// (we don't surface `--mod3-url` in CLI; one env var + YAML is enough).
	if v := os.Getenv("COGOS_MOD3_URL"); v != "" {
		cfg.Mod3URL = v
	}

	// Load core inference contract. Non-fatal: falls back to default if absent.
	ci, ciErr := inference.LoadCoreInferenceConfig(workspaceRoot)
	if ciErr != nil {
		// File exists but is unreadable or unparseable — use default and continue.
		ci = inference.DefaultCoreInferenceConfig()
	}
	cfg.CoreInference = ci

	// Flag override.
	if port != 0 {
		cfg.Port = port
	}

	return cfg, nil
}

// applyKernelSection applies non-zero values from a config section to cfg.
func applyKernelSection(cfg *Config, s kernelConfigSection) {
	if s.Port != 0 {
		cfg.Port = s.Port
	}
	if s.BindAddr != "" {
		cfg.BindAddr = s.BindAddr
	}
	if s.ConsolidationInterval != 0 {
		cfg.ConsolidationInterval = s.ConsolidationInterval
	}
	if s.HeartbeatInterval != 0 {
		cfg.HeartbeatInterval = s.HeartbeatInterval
	}
	if s.SalienceDaysWindow != 0 {
		cfg.SalienceDaysWindow = s.SalienceDaysWindow
	}
	if s.OutputReserve != 0 {
		cfg.OutputReserve = s.OutputReserve
	}
	if s.MaxFovealDocs != 0 {
		cfg.MaxFovealDocs = s.MaxFovealDocs
	}
	if s.SalienceFloor != nil {
		cfg.SalienceFloor = *s.SalienceFloor
	}
	if s.DefaultBudget != 0 {
		cfg.DefaultBudget = s.DefaultBudget
	}
	if len(s.ExcludeSubstrings) > 0 {
		cfg.ExcludeSubstrings = s.ExcludeSubstrings
	}
	if s.TRMWeightsPath != "" {
		cfg.TRMWeightsPath = s.TRMWeightsPath
	}
	if s.TRMEmbeddingsPath != "" {
		cfg.TRMEmbeddingsPath = s.TRMEmbeddingsPath
	}
	if s.TRMChunksPath != "" {
		cfg.TRMChunksPath = s.TRMChunksPath
	}
	if s.OllamaEmbedEndpoint != "" {
		cfg.OllamaEmbedEndpoint = s.OllamaEmbedEndpoint
	}
	if s.OllamaEmbedModel != "" {
		cfg.OllamaEmbedModel = s.OllamaEmbedModel
	}
	if s.ToolCallValidation != nil {
		cfg.ToolCallValidationEnabled = *s.ToolCallValidation
	}
	if s.EnableSkillExec != nil {
		cfg.EnableSkillExec = *s.EnableSkillExec
	}
	if s.EnableServiceControl != nil {
		cfg.EnableServiceControl = *s.EnableServiceControl
	}
	if s.EnableConfigMutation != nil {
		cfg.EnableConfigMutation = *s.EnableConfigMutation
	}
	if s.EnableReconcileControl != nil {
		cfg.EnableReconcileControl = *s.EnableReconcileControl
	}
	if s.WriteRouteGrantAuthDisabled != nil {
		cfg.WriteRouteGrantAuthDisabled = *s.WriteRouteGrantAuthDisabled
	}
	if s.IdentityNakedDefault != nil {
		cfg.IdentityNakedDefault = *s.IdentityNakedDefault
	}
	if s.LocalModel != "" {
		cfg.LocalModel = s.LocalModel
		cfg.localModelConfigured = true
	}
	if s.HarnessProvider != "" {
		cfg.HarnessProvider = s.HarnessProvider
	}
	if s.DispatchTimeoutCapSeconds != 0 {
		cfg.DispatchTimeoutCapSeconds = s.DispatchTimeoutCapSeconds
	}
	if len(s.DigestPaths) > 0 {
		if cfg.DigestPaths == nil {
			cfg.DigestPaths = make(map[string]string, len(s.DigestPaths))
		}
		for name, path := range s.DigestPaths {
			cfg.DigestPaths[name] = path
		}
	}
	if s.KernelLogPath != "" {
		cfg.KernelLogPath = s.KernelLogPath
	}
	if s.Mod3URL != "" {
		cfg.Mod3URL = s.Mod3URL
	}
	if s.MaxToolOutputBytes != 0 {
		cfg.MaxToolOutputBytes = s.MaxToolOutputBytes
	}
	if s.Routing != nil && len(s.Routing.Deny) > 0 {
		cfg.RoutingDeny = s.Routing.Deny
	}
}

// ContextGating returns the current foveated-assembler gating knobs as a
// consistent snapshot. Falls back to defaults when fields are zero so callers
// don't need to repeat the defaulting logic.
func (c *Config) ContextGating() (maxDocs int, salienceFloor float64) {
	c.gatingMu.RLock()
	defer c.gatingMu.RUnlock()
	maxDocs = c.MaxFovealDocs
	if maxDocs <= 0 {
		maxDocs = DefaultMaxFovealDocs
	}
	salienceFloor = c.SalienceFloor
	if salienceFloor <= 0 {
		salienceFloor = DefaultSalienceFloor
	}
	return maxDocs, salienceFloor
}

// DispatchTimeoutCap returns the effective ceiling for dispatch
// timeout_seconds. Falls back to DefaultDispatchTimeoutCapSeconds (600) when
// unset. Nil-receiver-safe: transport adapters stamp this onto every
// DispatchRequest, including in tests that run without a Config.
func (c *Config) DispatchTimeoutCap() int {
	if c == nil || c.DispatchTimeoutCapSeconds <= 0 {
		return DefaultDispatchTimeoutCapSeconds
	}
	return c.DispatchTimeoutCapSeconds
}

// EffectiveBudget returns the token budget to use for context assembly when no
// per-request override is provided. Falls back to DefaultBudget (32768) when
// the config field is zero (i.e. not set in kernel.yaml).
func (c *Config) EffectiveBudget() int {
	c.gatingMu.RLock()
	defer c.gatingMu.RUnlock()
	if c.DefaultBudget > 0 {
		return c.DefaultBudget
	}
	return DefaultBudget
}

// ContextExcludeSubstrings returns a snapshot of the configured
// exclude-substring list. The returned slice is safe for the caller to iterate
// without holding a lock.
func (c *Config) ContextExcludeSubstrings() []string {
	c.gatingMu.RLock()
	defer c.gatingMu.RUnlock()
	if len(c.ExcludeSubstrings) == 0 {
		return nil
	}
	out := make([]string, len(c.ExcludeSubstrings))
	copy(out, c.ExcludeSubstrings)
	return out
}

// SetContextGating hot-updates the foveated-assembler gating knobs. Pass a
// non-nil pointer for any field you wish to update; nil leaves that field
// untouched. Returns the post-update snapshot via ContextGating().
//
// Used by PATCH /v1/settings/context to let operators tighten or loosen the
// chat-path admission predicate without restarting the kernel.
func (c *Config) SetContextGating(maxDocs *int, salienceFloor *float64) (int, float64) {
	c.gatingMu.Lock()
	if maxDocs != nil {
		c.MaxFovealDocs = *maxDocs
	}
	if salienceFloor != nil {
		c.SalienceFloor = *salienceFloor
	}
	c.gatingMu.Unlock()
	return c.ContextGating()
}

// findWorkspaceRoot walks up from dir until it finds a directory containing a
// usable .cog/config/ directory.
func findWorkspaceRoot(dir string) (string, error) {
	for {
		if hasUsableCogConfig(dir) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no usable .cog/config directory found from %s upward", dir)
		}
		dir = parent
	}
}
