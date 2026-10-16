// types.go defines the shared data types used across the harness package.
//
// Key types and where they flow:
//
//   - InferenceRequest   — input to RunInference / RunInferenceStream
//   - InferenceResponse  — output from RunInference
//   - ContextState       — four-tier context pipeline (identity/temporal/present/semantic)
//   - ChatMessage        — OpenAI-format message (used in ChatCompletionRequest)
//   - ChatCompletionRequest — the HTTP request body for /v1/chat/completions
//
// API response types (ModelListResponse, ProviderListResponse, etc.) are also
// defined here so the kernel's HTTP handlers can return harness-typed responses
// in future waves.
package harness

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// === CONTEXT STATE (Four-Tier Context Pipeline) ===

// ContextTier represents a single tier of context with metadata
type ContextTier struct {
	Content string `json:"content"`
	Tokens  int    `json:"tokens"`
	Source  string `json:"source,omitempty"`
}

// ContextState represents the full context.cog.json structure
// Used for context-aware invocation of the inference engine
type ContextState struct {
	// Tier 1: Identity (stable, ~1/3 of budget)
	Tier1Identity *ContextTier `json:"tier1_identity,omitempty"`

	// Tier 2: Temporal (session state, signals, history)
	Tier2Temporal *ContextTier `json:"tier2_temporal,omitempty"`

	// Tier 3: Present (current message context)
	Tier3Present *ContextTier `json:"tier3_present,omitempty"`

	// Tier 4: Semantic (constellation knowledge graph)
	Tier4Semantic *ContextTier `json:"tier4_semantic,omitempty"`

	// Model selection (optional override)
	Model string `json:"model,omitempty"`

	// Metadata
	TotalTokens    int     `json:"total_tokens,omitempty"`
	CoherenceScore float64 `json:"coherence_score,omitempty"`
	ShouldRefresh  bool    `json:"should_refresh,omitempty"`

	// TAA signals (from Tier 2 temporal analysis)
	Anchor string `json:"anchor,omitempty"` // Current conversation topic
	Goal   string `json:"goal,omitempty"`   // Detected user intent
}

// BuildContextString assembles the full context string from tiers
func (cs *ContextState) BuildContextString() string {
	if cs == nil {
		return ""
	}

	var parts []string

	// Add tiers in order
	if cs.Tier1Identity != nil && cs.Tier1Identity.Content != "" {
		parts = append(parts, cs.Tier1Identity.Content)
	}
	if cs.Tier2Temporal != nil && cs.Tier2Temporal.Content != "" {
		parts = append(parts, cs.Tier2Temporal.Content)
	}
	if cs.Tier3Present != nil && cs.Tier3Present.Content != "" {
		parts = append(parts, cs.Tier3Present.Content)
	}
	if cs.Tier4Semantic != nil && cs.Tier4Semantic.Content != "" {
		parts = append(parts, cs.Tier4Semantic.Content)
	}

	return strings.Join(parts, "\n\n---\n\n")
}

// ContextMetrics captures metrics about context used in inference
type ContextMetrics struct {
	TotalTokens     int            `json:"total_tokens"`
	TierBreakdown   map[string]int `json:"tier_breakdown"`
	CoherenceScore  float64        `json:"coherence_score"`
	CompressionUsed bool           `json:"compression_used"`
}

// InferenceRequest represents input to the inference engine
type InferenceRequest struct {
	ID           string          // Unique request ID (auto-generated if empty)
	Prompt       string          // User prompt
	SystemPrompt string          // Optional system prompt
	Model        string          // Model to use (empty = default)
	Schema       json.RawMessage // Optional JSON schema for structured output
	MaxTokens    *int            // Optional max tokens
	Origin       string          // Where request came from: "cli", "http", "hook", "fleet"
	Stream       bool            // Whether to stream
	Context      context.Context // For cancellation

	// Context pipeline
	ContextState *ContextState // Four-tier context for context-aware invocation

	// Tool definitions
	//
	// Tools is the full set of OpenAI-format tool definitions from the
	// client. The harness classifies each tool as internal or external via
	// ClassifyTool/PartitionTools; internal tools are executed by Claude
	// CLI or the CogOS kernel, external tools are forwarded to the client
	// as `tool_calls` on the response.
	//
	// ExternalTools, when non-nil, is a pre-partitioned list of
	// client-owned tools (equivalent to the `external` return of
	// PartitionTools(Tools)). Callers that have already partitioned can
	// populate this directly; otherwise the harness partitions Tools on
	// its own. This is the plumbing BrowserOS uses to register
	// `browser_*` tools it will execute itself.
	Tools           []json.RawMessage // OpenAI-format tool definitions from client
	ExternalTools   []json.RawMessage // Client-owned tool definitions (subset of Tools)
	AllowedTools    []string          // Claude CLI --allowed-tools patterns (e.g. "Bash", "Bash(git:*)")
	SkipPermissions bool              // Pass --dangerously-skip-permissions to Claude CLI

	// Workspace override — when set, Claude CLI runs in this directory
	// instead of the kernel's workspace.
	WorkspaceRoot string

	// MCP bridge configuration
	MCPConfig     string // Path to generated --mcp-config JSON file
	SessionID     string // Session context for tool execution

	// Claude CLI session continuity
	// When set, the harness passes --resume <ClaudeSessionID> instead of
	// starting a new session. Claude Code loads the prior conversation from
	// disk and continues where it left off.
	ClaudeSessionID string

	// Retry configuration
	MaxRetries int           // Max retry attempts (0 = use default)
	Timeout    time.Duration // Request timeout (0 = use default)
}

// InferenceResponse represents output from the inference engine
type InferenceResponse struct {
	ID               string `json:"id"`
	Content          string `json:"content"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	FinishReason     string `json:"finish_reason"`
	Error            error  `json:"-"`
	ErrorMessage     string `json:"error,omitempty"`

	// Anthropic cache metrics (zero for non-Claude providers)
	CacheReadTokens   int     `json:"cache_read_input_tokens,omitempty"`
	CacheCreateTokens int     `json:"cache_creation_input_tokens,omitempty"`
	CostUSD           float64 `json:"cost_usd,omitempty"`

	// Context metrics (from context pipeline)
	ContextMetrics *ContextMetrics `json:"context_metrics,omitempty"`

	// Error classification (for smart recovery)
	ErrorType ErrorType `json:"error_type,omitempty"`

	// Claude CLI session ID returned by the process. Callers should store
	// this and pass it back as ClaudeSessionID on the next request to
	// enable --resume continuity.
	ClaudeSessionID string `json:"claude_session_id,omitempty"`

	// ToolCalls lists client-owned tool invocations the model asked for
	// but the harness did not execute. The caller (typically an HTTP
	// handler) is expected to surface these as `tool_calls` on the
	// assistant message so a BrowserOS-style client can execute them and
	// send back the result on the next turn. FinishReason is set to
	// "tool_calls" when this slice is non-empty.
	ToolCalls []ToolCallData `json:"tool_calls,omitempty"`
}

// ChatMessage represents a message in the chat format.
// Content is json.RawMessage to handle both string and array-of-parts formats.
type ChatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"` // Can be string or array
}

// GetContent extracts the text content from a ChatMessage.
// Handles both string format and array-of-parts format (OpenAI SDK).
func (m *ChatMessage) GetContent() string {
	if len(m.Content) == 0 {
		return ""
	}

	// Try to unmarshal as a simple string first
	var strContent string
	if err := json.Unmarshal(m.Content, &strContent); err == nil {
		return strContent
	}

	// Try to unmarshal as an array of content parts
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Content, &parts); err == nil {
		var result strings.Builder
		for _, part := range parts {
			if part.Type == "text" || part.Type == "" {
				result.WriteString(part.Text)
			}
		}
		return result.String()
	}

	// Fallback: return raw content as string
	return string(m.Content)
}

// StringToRawContent converts a string to json.RawMessage for ChatMessage.Content
func StringToRawContent(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return json.RawMessage(b)
}

// ChatCompletionRequest represents an OpenAI-compatible chat completion request
type ChatCompletionRequest struct {
	Model          string          `json:"model"`
	Messages       []ChatMessage   `json:"messages"`
	Stream         bool            `json:"stream,omitempty"`
	Temperature    *float64        `json:"temperature,omitempty"`
	MaxTokens      *int            `json:"max_tokens,omitempty"`
	ResponseFormat *ResponseFormat `json:"response_format,omitempty"`
	SystemPrompt   string          `json:"system_prompt,omitempty"` // Extension for explicit system
	TAA            json.RawMessage `json:"taa,omitempty"`           // TAA context: false/absent=none, true=default, "name"=profile
	Tools          []json.RawMessage `json:"tools,omitempty"`       // OpenAI-format tool definitions
}

// GetTAAProfile extracts TAA profile from the request body field
func (r *ChatCompletionRequest) GetTAAProfile() (string, bool) {
	if len(r.TAA) == 0 {
		return "", false
	}

	// Try parsing as boolean
	var boolVal bool
	if err := json.Unmarshal(r.TAA, &boolVal); err == nil {
		if boolVal {
			return "default", true
		}
		return "", false
	}

	// Try parsing as string (profile name)
	var strVal string
	if err := json.Unmarshal(r.TAA, &strVal); err == nil && strVal != "" {
		return strVal, true
	}

	return "", false
}

// GetTAAProfileWithHeader extracts TAA profile with header taking precedence
func (r *ChatCompletionRequest) GetTAAProfileWithHeader(header string) (string, bool) {
	// Header takes precedence
	if header != "" {
		return header, true
	}
	// Fall back to body field
	return r.GetTAAProfile()
}

// ResponseFormat represents the response_format field in a chat completion request
type ResponseFormat struct {
	Type       string          `json:"type"`
	JSONSchema json.RawMessage `json:"json_schema,omitempty"`
}

// ModelListResponse represents the /v1/models response
type ModelListResponse struct {
	Object string      `json:"object"`
	Data   []ModelInfo `json:"data"`
}

// ModelInfo represents a single model entry
type ModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ProviderHealth represents the health status of a provider
type ProviderHealth struct {
	LastCheck *string `json:"last_check"` // ISO8601 timestamp or null
	LatencyMs *int    `json:"latency_ms"` // Latency in ms or null
	Error     *string `json:"error"`      // Error message or null
}

// ProviderInfo represents a single provider in the API response
type ProviderInfo struct {
	ID     string               `json:"id"`
	Name   string               `json:"name"`
	Status string               `json:"status"` // "online", "offline", "unknown", "degraded"
	Active bool                 `json:"active"`
	Models []string             `json:"models"`
	Config ProviderPublicConfig `json:"config"`
	Health ProviderHealth       `json:"health"`
}

// ProviderPublicConfig represents publicly-visible provider configuration
type ProviderPublicConfig struct {
	BaseURL   string `json:"base_url"`
	HasAPIKey bool   `json:"has_api_key"`
}

// ProviderListResponse represents the /v1/providers response
type ProviderListResponse struct {
	Object        string         `json:"object"`
	Data          []ProviderInfo `json:"data"`
	Active        string         `json:"active"`
	FallbackChain []string       `json:"fallback_chain"`
}

// ErrorResponse represents an API error
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error information
type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}
