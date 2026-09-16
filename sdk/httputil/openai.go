package httputil

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	sdk "github.com/myrgic/cogos/sdk"
	"github.com/myrgic/cogos/sdk/types"
)

// OpenAIHandler provides an OpenAI-compatible /v1/chat/completions endpoint.
//
// This allows tools that speak OpenAI API to use the CogOS inference system.
// The handler translates between OpenAI format and cog:inference.
type OpenAIHandler struct {
	kernel *sdk.Kernel
}

// NewOpenAIHandler creates a new OpenAI-compatible handler.
func NewOpenAIHandler(k *sdk.Kernel) *OpenAIHandler {
	return &OpenAIHandler{kernel: k}
}

// ChatCompletionRequest matches the OpenAI chat completion request format.
type ChatCompletionRequest struct {
	// Model is the model to use (e.g., "sonnet", "opus", "claude-sonnet-4-5").
	Model string `json:"model"`

	// Messages is the conversation history.
	Messages []Message `json:"messages"`

	// MaxTokens is the maximum tokens in the response.
	MaxTokens int `json:"max_tokens,omitempty"`

	// Temperature controls randomness (0.0-2.0, default 1.0).
	Temperature *float64 `json:"temperature,omitempty"`

	// TopP controls nucleus sampling.
	TopP *float64 `json:"top_p,omitempty"`

	// N is the number of completions to generate (only 1 supported).
	N int `json:"n,omitempty"`

	// Stream indicates if the response should be streamed.
	Stream bool `json:"stream,omitempty"`

	// Stop sequences.
	Stop []string `json:"stop,omitempty"`

	// PresencePenalty penalizes new tokens based on presence.
	PresencePenalty *float64 `json:"presence_penalty,omitempty"`

	// FrequencyPenalty penalizes new tokens based on frequency.
	FrequencyPenalty *float64 `json:"frequency_penalty,omitempty"`

	// User is an optional end-user identifier.
	User string `json:"user,omitempty"`

	// ResponseFormat specifies the output format.
	ResponseFormat *ResponseFormat `json:"response_format,omitempty"`

	// Seed for deterministic sampling.
	Seed *int `json:"seed,omitempty"`

	// Tools available for the model to call.
	Tools []Tool `json:"tools,omitempty"`

	// ToolChoice controls tool selection.
	ToolChoice any `json:"tool_choice,omitempty"`
}

// Message represents a chat message.
type Message struct {
	// Role is "system", "user", "assistant", or "tool".
	Role string `json:"role"`

	// Content is the message content (string or array for multimodal).
	Content any `json:"content"`

	// Name is an optional name for the participant.
	Name string `json:"name,omitempty"`

	// ToolCalls is present for assistant messages with tool calls.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`

	// ToolCallID is present for tool result messages.
	ToolCallID string `json:"tool_call_id,omitempty"`
}

// ResponseFormat specifies the output format.
type ResponseFormat struct {
	Type string `json:"type"` // "text" or "json_object"
}

// Tool represents a tool the model can use.
type Tool struct {
	Type     string       `json:"type"` // Always "function"
	Function ToolFunction `json:"function"`
}

// ToolFunction describes a function the model can call.
type ToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ToolCall represents a tool call from the assistant.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // Always "function"
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction contains the function call details.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatCompletionResponse matches the OpenAI chat completion response format.
type ChatCompletionResponse struct {
	// ID is a unique identifier for this completion.
	ID string `json:"id"`

	// Object is always "chat.completion".
	Object string `json:"object"`

	// Created is the Unix timestamp of creation.
	Created int64 `json:"created"`

	// Model is the model used.
	Model string `json:"model"`

	// SystemFingerprint is a fingerprint of the system configuration.
	SystemFingerprint string `json:"system_fingerprint,omitempty"`

	// Choices contains the completion choices.
	Choices []Choice `json:"choices"`

	// Usage contains token usage statistics.
	Usage Usage `json:"usage"`
}

// Choice represents a completion choice.
type Choice struct {
	// Index is the choice index.
	Index int `json:"index"`

	// Message is the assistant's response.
	Message Message `json:"message"`

	// Logprobs contains log probabilities if requested.
	Logprobs *Logprobs `json:"logprobs,omitempty"`

	// FinishReason indicates why generation stopped.
	FinishReason string `json:"finish_reason"`
}

// Logprobs contains log probability information.
type Logprobs struct {
	Content []LogprobsContent `json:"content,omitempty"`
}

// LogprobsContent contains token log probabilities.
type LogprobsContent struct {
	Token   string  `json:"token"`
	Logprob float64 `json:"logprob"`
	Bytes   []int   `json:"bytes,omitempty"`
}

// Usage contains token usage statistics.
type Usage struct {
	// PromptTokens is the number of prompt tokens.
	PromptTokens int `json:"prompt_tokens"`

	// CompletionTokens is the number of completion tokens.
	CompletionTokens int `json:"completion_tokens"`

	// TotalTokens is the total tokens used.
	TotalTokens int `json:"total_tokens"`
}

// ErrorResponse matches the OpenAI error response format.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error details.
type ErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param,omitempty"`
	Code    *string `json:"code,omitempty"`
}

// ServeHTTP handles OpenAI-format chat completion requests.
func (h *OpenAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error")
		return
	}

	var req ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), "invalid_request_error")
		return
	}

	// Validate request
	if len(req.Messages) == 0 {
		h.writeError(w, http.StatusBadRequest, "messages array is required", "invalid_request_error")
		return
	}

	// Streaming not yet supported
	if req.Stream {
		h.writeError(w, http.StatusNotImplemented, "streaming not yet supported", "invalid_request_error")
		return
	}

	// Convert to SDK inference request
	sdkReq := h.convertToSDKRequest(&req)

	// Execute inference
	resp, err := h.kernel.Infer(r.Context(), sdkReq.Prompt,
		sdk.WithModel(sdkReq.Model),
		sdk.WithMaxTokens(sdkReq.MaxTokens),
		sdk.WithSystemPrompt(sdkReq.SystemPrompt),
		sdk.WithOrigin("openai-compat"),
	)

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error(), "api_error")
		return
	}

	if resp.Error != "" {
		h.writeError(w, http.StatusInternalServerError, resp.Error, "api_error")
		return
	}

	// Convert to OpenAI response
	openAIResp := h.convertToOpenAIResponse(&req, resp)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(openAIResp)
}

// convertToSDKRequest converts an OpenAI request to an SDK inference request.
func (h *OpenAIHandler) convertToSDKRequest(req *ChatCompletionRequest) *types.InferenceRequest {
	// Extract system prompt and user messages
	var systemPrompt string
	var userContent string

	for _, msg := range req.Messages {
		switch msg.Role {
		case "system":
			if content, ok := msg.Content.(string); ok {
				if systemPrompt != "" {
					systemPrompt += "\n\n"
				}
				systemPrompt += content
			}
		case "user":
			if content, ok := msg.Content.(string); ok {
				userContent = content
			}
		case "assistant":
			// Include assistant messages in the prompt for context
			if content, ok := msg.Content.(string); ok {
				if userContent != "" {
					userContent += "\n\nAssistant: " + content
				}
			}
		}
	}

	// Get the last user message as the main prompt
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			if content, ok := req.Messages[i].Content.(string); ok {
				userContent = content
				break
			}
		}
	}

	sdkReq := &types.InferenceRequest{
		Prompt:       userContent,
		Model:        req.Model,
		MaxTokens:    req.MaxTokens,
		SystemPrompt: systemPrompt,
		Origin:       "openai-compat",
	}

	if req.Temperature != nil {
		sdkReq.Temperature = *req.Temperature
	}

	return sdkReq
}

// convertToOpenAIResponse converts an SDK response to OpenAI format.
func (h *OpenAIHandler) convertToOpenAIResponse(req *ChatCompletionRequest, resp *types.InferenceResponse) *ChatCompletionResponse {
	// Generate completion ID
	id := generateCompletionID()

	// Determine finish reason
	finishReason := "stop"
	if resp.StopReason == "max_tokens" {
		finishReason = "length"
	} else if resp.StopReason == "tool_use" {
		finishReason = "tool_calls"
	}

	return &ChatCompletionResponse{
		ID:      id,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   resp.Model,
		Choices: []Choice{
			{
				Index: 0,
				Message: Message{
					Role:    "assistant",
					Content: resp.Content,
				},
				FinishReason: finishReason,
			},
		},
		Usage: Usage{
			PromptTokens:     resp.InputTokens,
			CompletionTokens: resp.OutputTokens,
			TotalTokens:      resp.InputTokens + resp.OutputTokens,
		},
	}
}

// writeError writes an OpenAI-format error response.
func (h *OpenAIHandler) writeError(w http.ResponseWriter, status int, message, errorType string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorDetail{
			Message: message,
			Type:    errorType,
		},
	})
}

// generateCompletionID creates a unique completion ID.
func generateCompletionID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return "chatcmpl-" + hex.EncodeToString(b)
}
