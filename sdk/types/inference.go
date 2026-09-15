package types

import (
	"encoding/json"
	"time"
)

// InferenceRequest represents a request to the inference engine.
// Used for invoking Claude CLI through the SDK.
type InferenceRequest struct {
	// ID is the unique request ID (auto-generated if empty).
	ID string `json:"id,omitempty"`

	// Prompt is the user prompt to send.
	Prompt string `json:"prompt"`

	// Model is the model to use (sonnet, opus, haiku, or full ID).
	// Empty uses the default model.
	Model string `json:"model,omitempty"`

	// MaxTokens is the maximum tokens in the response.
	// Zero uses the model default.
	MaxTokens int `json:"max_tokens,omitempty"`

	// Temperature controls response randomness (0.0-1.0).
	// Zero uses the model default.
	Temperature float64 `json:"temperature,omitempty"`

	// Context is a list of cog:// URIs to include as context.
	Context []string `json:"context,omitempty"`

	// SystemPrompt is an optional system prompt.
	SystemPrompt string `json:"system_prompt,omitempty"`

	// Stream indicates if the response should be streamed.
	Stream bool `json:"stream,omitempty"`

	// Schema is a JSON schema for structured output.
	Schema json.RawMessage `json:"schema,omitempty"`

	// Origin indicates where the request came from.
	// Values: "cli", "http", "hook", "fleet", "widget"
	Origin string `json:"origin,omitempty"`

	// Timeout is the request timeout duration.
	Timeout time.Duration `json:"timeout,omitempty"`

	// MaxRetries is the maximum retry attempts.
	MaxRetries int `json:"max_retries,omitempty"`

	// AllowedTools restricts which tools can be used.
	AllowedTools []string `json:"allowed_tools,omitempty"`

	// DisallowedTools blocks specific tools.
	DisallowedTools []string `json:"disallowed_tools,omitempty"`
}

// InferenceResponse represents the response from the inference engine.
type InferenceResponse struct {
	// ID is the request ID this response corresponds to.
	ID string `json:"id"`

	// Content is the generated text content.
	Content string `json:"content"`

	// Model is the model that was used.
	Model string `json:"model"`

	// InputTokens is the prompt token count.
	InputTokens int `json:"input_tokens"`

	// OutputTokens is the completion token count.
	OutputTokens int `json:"output_tokens"`

	// StopReason is why generation stopped.
	// Values: "stop", "max_tokens", "tool_use"
	StopReason string `json:"stop_reason"`

	// FinishReason is an alias for StopReason (for compatibility).
	FinishReason string `json:"finish_reason,omitempty"`

	// Error is set if the request failed.
	Error string `json:"error,omitempty"`

	// ErrorType classifies the error for retry logic.
	ErrorType string `json:"error_type,omitempty"`

	// Duration is how long the request took.
	Duration time.Duration `json:"duration,omitempty"`

	// Timestamp is when the response was generated.
	Timestamp time.Time `json:"timestamp"`

	// ContextMetrics contains context usage metrics.
	ContextMetrics *ContextMetrics `json:"context_metrics,omitempty"`
}

// StreamChunk represents a single chunk in a streaming response.
type StreamChunk struct {
	// ID is the request ID.
	ID string `json:"id"`

	// Content is the chunk's text content.
	Content string `json:"content"`

	// Done indicates this is the final chunk.
	Done bool `json:"done"`

	// FinishReason is set on the final chunk.
	FinishReason string `json:"finish_reason,omitempty"`

	// Error is set if streaming failed.
	Error string `json:"error,omitempty"`

	// Seq is the sequence number of this chunk.
	Seq int `json:"seq"`
}

// InferenceErrorType classifies inference errors for smart recovery.
type InferenceErrorType string

const (
	// ErrorNone indicates no error.
	ErrorNone InferenceErrorType = ""

	// ErrorRateLimit indicates a 429 rate limit - retry with backoff.
	ErrorRateLimit InferenceErrorType = "rate_limit"

	// ErrorContextOverflow indicates context too long - compress and retry.
	ErrorContextOverflow InferenceErrorType = "context_overflow"

	// ErrorAuth indicates authentication failure - fail fast.
	ErrorAuth InferenceErrorType = "auth"

	// ErrorTransient indicates transient failure - retry with backoff.
	ErrorTransient InferenceErrorType = "transient"

	// ErrorFatal indicates fatal error - don't retry.
	ErrorFatal InferenceErrorType = "fatal"
)

// ResolveModelAlias previously mapped short names ("sonnet"/"opus"/"haiku")
// to date-pinned model IDs, but that table went stale (see #632) and nothing
// re-derived it from a live catalog like internal/engine/resolve.go does.
// It is now the identity function: the `claude` CLI resolves these aliases
// itself, and kernel-routed calls go through the kernel's own resolve.go.
// Kept exported (not renamed/removed) so existing callers keep compiling.
func ResolveModelAlias(alias string) string {
	return alias
}

// InferenceConfig configures the inference projector.
type InferenceConfig struct {
	// DefaultModel is the default model to use.
	DefaultModel string `json:"default_model"`

	// DefaultMaxTokens is the default max tokens.
	DefaultMaxTokens int `json:"default_max_tokens"`

	// DefaultTimeout is the default request timeout.
	DefaultTimeout time.Duration `json:"default_timeout"`

	// MaxRetries is the default max retry attempts.
	MaxRetries int `json:"max_retries"`

	// BaseRetryDelay is the base delay for exponential backoff.
	BaseRetryDelay time.Duration `json:"base_retry_delay"`

	// ClaudeCommand is the Claude CLI command name.
	ClaudeCommand string `json:"claude_command"`
}

// DefaultInferenceConfig returns sensible defaults.
func DefaultInferenceConfig() *InferenceConfig {
	return &InferenceConfig{
		DefaultModel:     "", // Use CLI default
		DefaultMaxTokens: 0,  // Use CLI default
		DefaultTimeout:   2 * time.Minute,
		MaxRetries:       3,
		BaseRetryDelay:   time.Second,
		ClaudeCommand:    "claude",
	}
}

// InferenceStatus represents the current inference engine status.
type InferenceStatus struct {
	// Available indicates if the inference engine is ready.
	Available bool `json:"available"`

	// ClaudeInstalled indicates if Claude CLI is installed.
	ClaudeInstalled bool `json:"claude_installed"`

	// ClaudePath is the path to the Claude CLI.
	ClaudePath string `json:"claude_path,omitempty"`

	// ActiveRequests is the count of in-flight requests.
	ActiveRequests int `json:"active_requests"`

	// QueuedRequests is the count of queued requests.
	QueuedRequests int `json:"queued_requests"`

	// TotalRequests is the total requests processed.
	TotalRequests int64 `json:"total_requests"`

	// TotalTokens is the total tokens used.
	TotalTokens int64 `json:"total_tokens"`

	// LastError is the most recent error, if any.
	LastError string `json:"last_error,omitempty"`

	// LastRequestAt is when the last request was made.
	LastRequestAt *time.Time `json:"last_request_at,omitempty"`
}

// RequestEntry tracks an in-flight inference request.
type RequestEntry struct {
	// ID is the request ID.
	ID string `json:"id"`

	// Origin is where the request came from.
	Origin string `json:"origin"`

	// Model is the model being used.
	Model string `json:"model"`

	// Started is when the request started.
	Started time.Time `json:"started"`

	// Status is the current status.
	// Values: "running", "completed", "cancelled", "failed"
	Status string `json:"status"`

	// Prompt is a preview of the prompt (first 100 chars).
	Prompt string `json:"prompt,omitempty"`
}
