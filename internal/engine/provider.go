// provider.go — CogOS v3 inference provider interface
//
// Adapted from the workspace PROVIDER-SPEC.md contract.
// All LLM backends satisfy the Provider interface. The kernel never calls
// a model API directly — it always routes through a Provider.
//
// Key design decisions:
//   - Models are organs, not the organism. Swappable, upgradeable.
//   - CompletionRequest carries foveated context, not raw strings.
//   - Router implements the sovereignty gradient: local-first, cloud-escalate.
//   - ProcessState uses string (maps to ProcessState.String() from process.go).
package engine

import (
	"context"
	"time"
)

// ── Provider interface ────────────────────────────────────────────────────────

// Provider is the fundamental abstraction for any LLM backend.
// Anthropic, Ollama, MLX, OpenRouter — all satisfy this interface.
type Provider interface {
	// Complete sends a context package and waits for the full response.
	Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)

	// Stream sends a request and returns a channel of incremental chunks.
	// The channel closes when done or on error. Providers that don't support
	// streaming must fall back to Complete and send a single chunk.
	Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error)

	// Name returns the provider identifier (e.g. "ollama", "anthropic").
	Name() string

	// Model returns the configured model identifier for this provider
	// (e.g. "gemma4:e4b", "sonnet", "gemma-4-e4b-agentic-opus-reasoning-geminicli-mlx").
	// Used by the router to resolve OpenAI-compat `model: X` requests to the
	// provider serving X. Empty string means "no specific model" (stubs/tests).
	Model() string

	// Available reports whether the provider is ready to serve requests.
	// For local providers: checks the model server is running and model loaded.
	Available(ctx context.Context) bool

	// Capabilities returns what this provider supports.
	Capabilities() ProviderCapabilities

	// Ping probes the endpoint and returns measured latency.
	Ping(ctx context.Context) (time.Duration, error)
}

// ModelLister is implemented by providers that can enumerate the concrete model
// IDs they currently serve upstream. handleModels type-asserts this; providers
// that don't implement it contribute only their configured Model().
type ModelLister interface {
	ListModels(ctx context.Context) ([]string, error)
}

// ModelListing is one upstream-enumerated model plus whatever context-window
// metadata the provider was able to obtain for it. ContextLength is 0 when the
// upstream has no comparable field (most OpenAI-compat servers only expose bare
// ids via /v1/models) — callers must treat 0 as "unknown", never as a real
// window, and omit it from any client-facing representation rather than
// guessing a default (#518: a wrong number is worse than no number here).
type ModelListing struct {
	ID            string
	ContextLength int
}

// ModelContextLister is implemented by providers that can enumerate model ids
// together with each model's context window (e.g. LM Studio's native
// GET /api/v0/models, which — unlike the OpenAI-standard /v1/models — exposes
// loaded_context_length / max_context_length per model). handleModels prefers
// this over plain ModelLister so context_length can be propagated to
// /v1/models (#518) without changing the ModelLister contract that
// OllamaProvider and ClaudeOAuthProvider already satisfy with no comparable
// per-model window to report.
type ModelContextLister interface {
	ListModelsWithContext(ctx context.Context) ([]ModelListing, error)
}

// CancelSafeCompleter is implemented by providers whose Complete() cannot
// reliably propagate ctx-cancellation to the server (#432). Such providers
// expose CompleteCancelSafe, which routes the request through their
// streaming path (or another mechanism that does tear down the server-side
// connection on cancel) so that an abandoned request actually aborts
// generation instead of running headless.
//
// Providers that don't implement this (Anthropic HTTP, ClaudeCode subprocess,
// etc.) either already propagate cancellation correctly through their own
// transport or are out of scope for the local-inference zombie-generation
// failure mode this interface exists to close. Callers should use
// CompleteCancelSafeIfSupported rather than asserting this interface directly.
type CancelSafeCompleter interface {
	CompleteCancelSafe(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)
}

// CompleteCancelSafeIfSupported calls p.CompleteCancelSafe when the provider
// implements CancelSafeCompleter, otherwise falls back to plain Complete.
// Non-streaming call sites — dispatch, tool-loop re-calls, the autonomic
// consult, AND the external non-streaming chat/Anthropic-compat HTTP
// handlers (serve.go, serve_anthropic.go) — route through this rather than
// calling Complete directly, so that a ctx cancel/timeout actually aborts
// server-side generation on providers where that matters (#432) without
// changing behavior for providers where it doesn't apply.
//
// This is also the RFC-040 S0 InferenceP50Ms/InferenceQueue tap
// (dispatch_inference_metrics.go): every call is timed and counted in-flight
// exactly once here. See that file's package doc for the exact, corrected
// scope of what these gauges measure and — importantly — what they don't
// (streaming completions bypass this function entirely).
//
// Wrapper providers that need to preserve cancel-safety semantics on their
// own embedded Provider (e.g. countingProvider in local_agent_harness.go,
// whose CompleteCancelSafe must still reach the inner provider's
// cancel-safe path) MUST call completeCancelSafeIfSupportedRaw, not this
// function — calling this function again from inside a CancelSafeCompleter
// implementation that this function itself just dispatched into would
// double-count the same logical call in both the queue-depth and p50
// instrumentation below.
func CompleteCancelSafeIfSupported(ctx context.Context, p Provider, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()
	beginDispatchInferenceSample()
	defer func() {
		endDispatchInferenceSample(time.Since(start))
	}()

	return completeCancelSafeIfSupportedRaw(ctx, p, req)
}

// completeCancelSafeIfSupportedRaw is the uninstrumented fallback logic
// behind CompleteCancelSafeIfSupported, extracted so a CancelSafeCompleter
// wrapper's own CompleteCancelSafe method can delegate to its embedded
// provider without re-entering (and double-instrumenting) the exported,
// metered entry point. See CompleteCancelSafeIfSupported's doc for why this
// matters.
func completeCancelSafeIfSupportedRaw(ctx context.Context, p Provider, req *CompletionRequest) (*CompletionResponse, error) {
	if cs, ok := p.(CancelSafeCompleter); ok {
		return cs.CompleteCancelSafe(ctx, req)
	}
	return p.Complete(ctx, req)
}

// ── Request types ─────────────────────────────────────────────────────────────

// CompletionRequest carries the assembled context package to the model.
// This is the output of the attentional field, not raw chat input.
type CompletionRequest struct {
	// SystemPrompt carries nucleus content: identity, role, self-model.
	SystemPrompt string `json:"system_prompt"`

	// Messages is the conversation history in the current foveal window.
	Messages []ProviderMessage `json:"messages"`

	// Context is the assembled foveal content from the attentional field.
	Context []ContextItem `json:"context,omitempty"`

	// MaxTokens is the generation limit.
	MaxTokens int `json:"max_tokens,omitempty"`

	// Temperature controls randomness [0.0, 1.0].
	Temperature *float64 `json:"temperature,omitempty"`

	// TopP is the nucleus sampling parameter.
	TopP *float64 `json:"top_p,omitempty"`

	// Stop sequences that terminate generation.
	Stop []string `json:"stop,omitempty"`

	// Tools defines MCP tool definitions the model can invoke. For OpenAI /
	// Anthropic HTTP providers these are forwarded to the upstream API so
	// the model can emit tool_use events. The full list includes both
	// CogOS-owned tools (internal) and client-owned tools (external).
	Tools []ToolDefinition `json:"tools,omitempty"`

	// ExternalTools is the subset of Tools whose execution belongs to the
	// calling client (e.g. BrowserOS's `browser_*`). Providers may use this
	// to decide which tool_use events to pass back as `tool_calls` on the
	// response instead of executing server-side. Empty when every tool is
	// kernel-owned. Names are expected to match entries in Tools.
	ExternalTools []ToolDefinition `json:"external_tools,omitempty"`

	// RefusedToolNames records CLIENT-SUPPLIED tool definitions that were
	// dropped at the gateway because they collided with a kernel-owned
	// (MCP-registered) tool name. Ledger L06.
	//
	// Stripping the definition is necessary but not sufficient: the tool-call
	// partitioner classifies by name alone, so a provider that emits a
	// tool_use for a refused name would still have it executed in-process.
	// splitToolCallsByOwnershipFor consults this set and routes any such call
	// away from execution. Not serialized upstream — kernel-local state for
	// the duration of the request.
	RefusedToolNames []string `json:"-"`

	// ToolChoice constrains tool use: "auto", "none", "required", or a name.
	ToolChoice string `json:"tool_choice,omitempty"`

	// ModelOverride, when non-empty, instructs the provider to use this model
	// instead of its configured default. Set by --model flag or request body.
	ModelOverride string `json:"model_override,omitempty"`

	// InteractionID links the request back to the canonical ingress CogBlock.
	InteractionID string `json:"interaction_id,omitempty"`

	// Metadata carries routing/ledger information not sent to the model.
	Metadata RequestMetadata `json:"metadata"`

	// KeepAlive overrides the Ollama keep_alive value for this request only.
	// Accepts a Go duration string ("5m", "1h"), an integer number of seconds,
	// or -1 to keep the model loaded indefinitely. When nil, the provider's
	// default is used (currently -1 to keep the model warm across cycles).
	// Set to 0 to force model eviction after this request completes — useful
	// for cold-start simulation in eval workloads.
	// Non-Ollama providers ignore this field.
	KeepAlive *any `json:"keep_alive,omitempty"`

	// WorkDir is the working directory for the spawned claude-code process (G3
	// Part A). When non-empty, the foreground Complete/Stream spawns set
	// cmd.Dir to this path so the subprocess loads the identity's own
	// CLAUDE.md rather than the daemon's cwd. Empty means "inherit daemon cwd"
	// (today's behavior, and the behavior when IdentityNakedDefault=false).
	// Only ClaudeCodeProvider consults this field; other providers ignore it.
	WorkDir string `json:"work_dir,omitempty"`
}

// ProviderMessage is a single conversation turn.
type ProviderMessage struct {
	Role         string        `json:"role"` // "user", "assistant", "system", "tool"
	Content      string        `json:"content"`
	ContentParts []ContentPart `json:"content_parts,omitempty"`
	Name         string        `json:"name,omitempty"`
	ToolCallID   string        `json:"tool_call_id,omitempty"`
	ToolCalls    []ToolCall    `json:"tool_calls,omitempty"`
}

// ContentPart is a structured content element preserving multi-modal data
// (text and images) that would be lost by the text-only Content field.
type ContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
}

// ContextItem is a piece of foveated context assembled by the attentional field.
type ContextItem struct {
	ID            string          `json:"id"` // cog:// URI or memory address
	Zone          AttentionalZone `json:"zone"`
	Salience      float64         `json:"salience"`
	Content       string          `json:"content"`
	TokenEstimate int             `json:"token_estimate,omitempty"`
}

// AttentionalZone maps to the v3 four-layer attentional field.
type AttentionalZone string

const (
	ZoneNucleus    AttentionalZone = "nucleus"    // Identity, never drops below threshold
	ZoneMomentum   AttentionalZone = "momentum"   // Recent trajectory
	ZoneFoveal     AttentionalZone = "foveal"     // Current focus
	ZoneParafoveal AttentionalZone = "parafoveal" // Background, on demand
)

// RequestMetadata carries routing/ledger data that doesn't go to the model.
type RequestMetadata struct {
	RequestID            string                    `json:"request_id"`
	ProcessState         string                    `json:"process_state"` // from ProcessState.String()
	Priority             RequestPriority           `json:"priority"`
	PreferLocal          bool                      `json:"prefer_local,omitempty"`
	PreferProvider       string                    `json:"prefer_provider,omitempty"` // force-route to named provider
	MaxCostUSD           *float64                  `json:"max_cost_usd,omitempty"`
	RequiredCapabilities []Capability              `json:"required_capabilities,omitempty"`
	Source               string                    `json:"source,omitempty"`
	SalienceSnapshot     *ProviderSalienceSnapshot `json:"salience_snapshot,omitempty"`
	// Attribution carries the dispatch caller's subject for observability
	// (RFC-identity-embedding I1/I2). "anonymous" when no DispatchIdentity.Sub
	// was provided; empty on requests that do not originate from
	// cog_dispatch_to_harness. Providers must not forward this field to the
	// upstream model API.
	Attribution string `json:"attribution,omitempty"`
}

// RequestPriority controls routing urgency.
type RequestPriority int

const (
	PriorityLow      RequestPriority = 0
	PriorityNormal   RequestPriority = 1
	PriorityHigh     RequestPriority = 2
	PriorityCritical RequestPriority = 3
)

// ProviderSalienceSnapshot captures attentional field state at request time.
type ProviderSalienceSnapshot struct {
	TopItems       []ProviderSalienceEntry `json:"top_items"`
	FocalPoint     string                  `json:"focal_point"`
	MomentumVector []float64               `json:"momentum_vector,omitempty"`
}

// ProviderSalienceEntry records a single item's salience score.
type ProviderSalienceEntry struct {
	ID       string          `json:"id"`
	Salience float64         `json:"salience"`
	Zone     AttentionalZone `json:"zone"`
}

// ── Response types ────────────────────────────────────────────────────────────

// CompletionResponse is what a Provider returns from Complete().
type CompletionResponse struct {
	Content          string       `json:"content"`
	ReasoningContent string       `json:"reasoning_content,omitempty"` // populated by reasoning models (LM Studio, Anthropic extended-thinking)
	ToolCalls        []ToolCall   `json:"tool_calls,omitempty"`
	StopReason       string       `json:"stop_reason"` // "end_turn" | "max_tokens" | "tool_use"
	Usage            TokenUsage   `json:"usage"`
	ProviderMeta     ProviderMeta `json:"provider_meta"`
}

// StreamChunk is one piece of a streaming response.
type StreamChunk struct {
	Delta         string         `json:"delta,omitempty"`
	IsReasoning   bool           `json:"is_reasoning,omitempty"` // true when Delta carries reasoning/thinking content
	ToolCallDelta *ToolCallDelta `json:"tool_call_delta,omitempty"`
	Done          bool           `json:"done"`
	StopReason    string         `json:"stop_reason,omitempty"`   // e.g. "end_turn", "max_tokens", "tool_use"
	Usage         *TokenUsage    `json:"usage,omitempty"`         // populated on final chunk
	ProviderMeta  *ProviderMeta  `json:"provider_meta,omitempty"` // populated on final chunk
	Error         error          `json:"-"`
}

// ToolCallDelta carries incremental streaming data for a tool call.
type ToolCallDelta struct {
	Index     int    `json:"index"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	ArgsDelta string `json:"args_delta,omitempty"`
}

// TokenUsage tracks token consumption for cost accounting.
type TokenUsage struct {
	InputTokens      int `json:"input_tokens"`
	OutputTokens     int `json:"output_tokens"`
	CacheReadTokens  int `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int `json:"cache_write_tokens,omitempty"`
}

// ProviderMeta carries provenance for the ledger.
type ProviderMeta struct {
	Provider string        `json:"provider"`
	Model    string        `json:"model"`
	Latency  time.Duration `json:"latency"`
	Region   string        `json:"region,omitempty"`
	Cached   bool          `json:"cached,omitempty"`
}

// mapStopReasonToOpenAI converts provider-native stop reasons (e.g. Anthropic's
// "end_turn", "stop_sequence") to OpenAI finish_reason values. Returns "" for
// unknown/empty values so callers can fall back to a heuristic.
func mapStopReasonToOpenAI(reason string) string {
	switch reason {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default:
		return ""
	}
}

// ── Tool types ────────────────────────────────────────────────────────────────

// ToolDefinition describes an MCP tool the model may invoke.
type ToolDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"input_schema"`
}

// ToolCall is a model's request to invoke a tool.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ── Provider capabilities ─────────────────────────────────────────────────────

// Capability is a single feature a provider may support.
type Capability string

const (
	CapStreaming          Capability = "streaming"
	CapToolUse            Capability = "tool_use"
	CapToolCallValidation Capability = "tool_call_validation"
	CapVision             Capability = "vision"
	CapLongContext        Capability = "long_context"
	CapJSON               Capability = "json_output"
	CapCaching            Capability = "caching"
	CapBatch              Capability = "batch"
)

// ProviderCapabilities describes what a provider can do.
type ProviderCapabilities struct {
	Capabilities       []Capability `json:"capabilities"`
	MaxContextTokens   int          `json:"max_context_tokens"`
	MaxOutputTokens    int          `json:"max_output_tokens"`
	ModelsAvailable    []string     `json:"models_available"`
	IsLocal            bool         `json:"is_local"`
	AgenticHarness     bool         `json:"agentic_harness,omitempty"`
	CostPerInputToken  float64      `json:"cost_per_input_token"`
	CostPerOutputToken float64      `json:"cost_per_output_token"`
}

// HasCapability checks if the provider supports a specific capability.
func (pc ProviderCapabilities) HasCapability(cap Capability) bool {
	for _, c := range pc.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// HasAllCapabilities checks if the provider supports all required capabilities.
func (pc ProviderCapabilities) HasAllCapabilities(required []Capability) bool {
	for _, req := range required {
		if !pc.HasCapability(req) {
			return false
		}
	}
	return true
}

// ── Router interface ──────────────────────────────────────────────────────────

// Router selects which Provider handles a given request.
// Maps to the externalized gating network from the MoE architecture.
type Router interface {
	// Route selects the best provider for a request.
	Route(ctx context.Context, req *CompletionRequest) (Provider, *RoutingDecision, error)

	// RegisterProvider adds a provider to the pool.
	RegisterProvider(p Provider)

	// DeregisterProvider removes a provider.
	DeregisterProvider(name string)

	// ProviderForName returns the registered provider name when `name` is an
	// exact match for a provider's Name(). Used to detect provider aliases
	// in OpenAI-compat `model: X` requests so callers can target a specific
	// provider without forwarding the alias as a ModelOverride. Returns
	// ("", false) when no provider matches.
	ProviderForName(name string) (string, bool)

	// ProviderForModel returns the registered provider name whose Name() or
	// Model() matches the requested model string. Used by the OpenAI-compat
	// chat handler to resolve `model: X` to the provider serving X. Returns
	// ("", false) when no match. Name match takes precedence over model match.
	ProviderForModel(model string) (string, bool)

	// LocalProvider returns the provider the "local" model alias pins to: the
	// configured routing.default_local when it names a registered on-device
	// provider, otherwise the first on-device provider by name. "On-device"
	// means inference runs on hardware the operator controls (IsLocal) and the
	// provider is not an agentic CLI that forwards to a hosted model
	// (AgenticHarness: claude-code, codex, pi). Returns ("", false) when none.
	LocalProvider() (string, bool)

	// RangeProviders calls fn for each registered provider under a read lock.
	// Order is by Name(). fn must not call back into the router (no re-entrancy).
	RangeProviders(fn func(p Provider))

	// Stats returns routing statistics.
	Stats() RouterStats
}

// RoutingDecision records why the router chose a specific provider.
type RoutingDecision struct {
	RequestID        string          `json:"request_id"`
	SelectedProvider string          `json:"selected_provider"`
	Scores           []ProviderScore `json:"scores"`
	Reason           string          `json:"reason"`
	Escalated        bool            `json:"escalated"`
	FallbackUsed     bool            `json:"fallback_used"`
	FallbackFrom     string          `json:"fallback_from,omitempty"`
	Timestamp        time.Time       `json:"timestamp"`
	LatencyNs        int64           `json:"latency_ns"`
}

// ProviderScore records a single provider's routing score.
type ProviderScore struct {
	Provider        string  `json:"provider"`
	RawScore        float64 `json:"raw_score"`
	SwapPenalty     float64 `json:"swap_penalty"`
	AdjustedScore   float64 `json:"adjusted_score"`
	Available       bool    `json:"available"`
	CapabilitiesMet bool    `json:"capabilities_met"`
}

// RouterStats tracks routing patterns for observability.
type RouterStats struct {
	TotalRequests                int64                    `json:"total_requests"`
	RequestsByProvider           map[string]int64         `json:"requests_by_provider"`
	ToolCallRejectionsByProvider map[string]int64         `json:"tool_call_rejections_by_provider,omitempty"`
	EscalationCount              int64                    `json:"escalation_count"`
	FallbackCount                int64                    `json:"fallback_count"`
	SovereigntyRatio             float64                  `json:"sovereignty_ratio"`
	TotalCostUSD                 float64                  `json:"total_cost_usd"`
	TokensByProvider             map[string]TokenUsage    `json:"tokens_by_provider"`
	AvgLatencyByProvider         map[string]time.Duration `json:"avg_latency_by_provider"`
}

// ── Configuration types ───────────────────────────────────────────────────────

// ProvidersConfig is the top-level configuration from .cog/config/providers.yaml.
type ProvidersConfig struct {
	Providers   map[string]ProviderConfig `yaml:"providers" json:"providers"`
	Routing     RoutingConfig             `yaml:"routing" json:"routing"`
	ClaudeOAuth ClaudeOAuthAutoConfig     `yaml:"claude_oauth,omitempty" json:"claude_oauth,omitempty"`
}

// ClaudeOAuthAutoConfig controls discovery-based auto-registration of the
// node-local "claude-oauth" provider (see maybeAutoRegisterClaudeOAuth in
// router.go): every node that already holds Claude Code OAuth credentials
// (keychain / CLAUDE_CODE_OAUTH_TOKEN / .credentials.json) gets a working
// claude-oauth provider with no providers.yaml entry and no token paste —
// discovery, not declaration.
type ClaudeOAuthAutoConfig struct {
	// Auto enables automatic discovery-and-registration. Defaults to true: a
	// nil pointer (the field absent from providers.yaml, or providers.yaml
	// absent entirely) means enabled. Set explicitly to false to opt out.
	Auto *bool `yaml:"auto,omitempty" json:"auto,omitempty"`
}

// IsAutoEnabled reports whether claude-oauth auto-registration is enabled.
// Absent config (nil) defaults to true.
func (c ClaudeOAuthAutoConfig) IsAutoEnabled() bool {
	if c.Auto == nil {
		return true
	}
	return *c.Auto
}

// ProviderConfig configures a single provider instance.
type ProviderConfig struct {
	Type          string                 `yaml:"type,omitempty" json:"type,omitempty"`
	APIKeyEnv     string                 `yaml:"api_key_env,omitempty" json:"api_key_env,omitempty"`
	Endpoint      string                 `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Model         string                 `yaml:"model" json:"model"`
	ContextWindow int                    `yaml:"context_window,omitempty" json:"context_window,omitempty"`
	MaxTokens     int                    `yaml:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	Timeout       int                    `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Headers       map[string]string      `yaml:"headers,omitempty" json:"headers,omitempty"`
	Options       map[string]interface{} `yaml:"options,omitempty" json:"options,omitempty"`
	Enabled       *bool                  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
}

// IsEnabled returns whether the provider is active (default: true).
func (pc ProviderConfig) IsEnabled() bool {
	if pc.Enabled == nil {
		return true
	}
	return *pc.Enabled
}

// RoutingConfig controls Router behaviour.
type RoutingConfig struct {
	Default string `yaml:"default" json:"default"`
	// DefaultLocal names the provider the "local", "kernel-agent" and "ollama"
	// aliases resolve to. Optional: when empty (or not a registered on-device
	// provider) the router picks the first on-device provider by name. Set it
	// whenever a node has more than one, so the choice is declared rather than
	// an accident of alphabetical order.
	DefaultLocal        string            `yaml:"default_local,omitempty" json:"default_local,omitempty"`
	LocalThreshold      float64           `yaml:"local_threshold" json:"local_threshold"`
	FallbackChain       []string          `yaml:"fallback_chain" json:"fallback_chain"`
	MaxCostPerDayUSD    float64           `yaml:"max_cost_per_day_usd,omitempty" json:"max_cost_per_day_usd,omitempty"`
	ProcessStateRouting map[string]string `yaml:"process_state_routing,omitempty" json:"process_state_routing,omitempty"`
}

// ── Ledger integration ────────────────────────────────────────────────────────

// InferenceEvent is the ledger event recorded for every inference request.
type InferenceEvent struct {
	RequestID       string           `json:"request_id"`
	Timestamp       time.Time        `json:"timestamp"`
	Provider        string           `json:"provider"`
	Model           string           `json:"model"`
	ProcessState    string           `json:"process_state"`
	Usage           TokenUsage       `json:"usage"`
	CostUSD         float64          `json:"cost_usd"`
	Latency         time.Duration    `json:"latency"`
	RoutingDecision *RoutingDecision `json:"routing_decision,omitempty"`
	Escalated       bool             `json:"escalated"`
	Source          string           `json:"source"`
	Success         bool             `json:"success"`
	Error           string           `json:"error,omitempty"`
}
