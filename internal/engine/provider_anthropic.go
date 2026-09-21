// provider_anthropic.go — AnthropicProvider
//
// Implements Provider against the Anthropic Messages API (POST /v1/messages).
// Auth: x-api-key header, read from the env var named by config.APIKeyEnv.
// Streaming: SSE (text/event-stream), reading typed events (message_start,
//
//	content_block_start, content_block_delta, message_delta, message_stop).
//
// Tool calls: streamed incrementally as ToolCallDelta chunks; non-streaming
//
//	responses decode tool_use content blocks directly.
//
// Context items are prepended to the system string as labelled sections.
package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	anthropicDefaultEndpoint = "https://api.anthropic.com"
	anthropicAPIVersion      = "2023-06-01"
	anthropicDefaultMaxToks  = 8192

	// anthropicMaxContextTokens is the standard context window for the
	// claude-sonnet-4+ family (200k). Shared by AnthropicProvider's
	// Capabilities and by /v1/models composition (#518) so every advertised
	// Anthropic model carries a real context_length instead of leaving
	// clients to guess a default.
	anthropicMaxContextTokens = 200_000
)

// AnthropicProvider implements Provider against the Anthropic Messages API.
type AnthropicProvider struct {
	name      string
	endpoint  string
	apiKey    string
	model     string
	maxTokens int
	timeout   time.Duration
	client    *http.Client
}

// NewAnthropicProvider creates an AnthropicProvider from a ProviderConfig.
func NewAnthropicProvider(name string, cfg ProviderConfig) *AnthropicProvider {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = anthropicDefaultEndpoint
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout == 0 {
		timeout = 120 * time.Second
	}
	maxTokens := cfg.MaxTokens
	if maxTokens == 0 {
		maxTokens = anthropicDefaultMaxToks
	}
	apiKey := ""
	if cfg.APIKeyEnv != "" {
		apiKey = os.Getenv(cfg.APIKeyEnv)
	}
	return &AnthropicProvider{
		name:      name,
		endpoint:  strings.TrimRight(endpoint, "/"),
		apiKey:    apiKey,
		model:     cfg.Model,
		maxTokens: maxTokens,
		timeout:   timeout,
		client:    &http.Client{Timeout: timeout},
	}
}

// Name returns the provider identifier.
func (p *AnthropicProvider) Name() string { return p.name }

// Model returns the configured model identifier.
func (p *AnthropicProvider) Model() string { return p.model }

// Available reports whether an API key is configured.
// For cloud providers we avoid a network round-trip on every health check —
// the presence of a non-empty API key is the availability signal.
func (p *AnthropicProvider) Available(_ context.Context) bool {
	return p.apiKey != ""
}

// Capabilities returns what Anthropic supports.
func (p *AnthropicProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities: []Capability{
			CapStreaming, CapToolUse, CapVision, CapLongContext, CapJSON, CapCaching,
		},
		MaxContextTokens:   anthropicMaxContextTokens,
		MaxOutputTokens:    p.maxTokens,
		ModelsAvailable:    []string{p.model},
		IsLocal:            false,
		AgenticHarness:     false,
		CostPerInputToken:  3.0 / 1_000_000,  // approximate Sonnet 4 input price
		CostPerOutputToken: 15.0 / 1_000_000, // approximate Sonnet 4 output price
	}
}

// Ping probes the Anthropic API and returns measured round-trip latency.
// Uses GET /v1/models — lightweight, validates auth without running inference.
func (p *AnthropicProvider) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"/v1/models", nil)
	if err != nil {
		return 0, fmt.Errorf("anthropic: ping: build request: %w", err)
	}
	p.setAuthHeaders(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("anthropic: ping: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return 0, fmt.Errorf("anthropic: ping: invalid API key (401)")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("anthropic: ping: /v1/models returned HTTP %d", resp.StatusCode)
	}
	return time.Since(start), nil
}

// setAuthHeaders applies the required Anthropic headers to every request.
func (p *AnthropicProvider) setAuthHeaders(req *http.Request) {
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("anthropic-version", anthropicAPIVersion)
	req.Header.Set("content-type", "application/json")
}

// effectiveModel returns the model to send: request-level override if set,
// otherwise the provider's configured default.
func (p *AnthropicProvider) effectiveModel(req *CompletionRequest) string {
	if req.ModelOverride != "" {
		return req.ModelOverride
	}
	return p.model
}

// ── Anthropic wire types ──────────────────────────────────────────────────────

type anthropicRequest struct {
	Model     string `json:"model"`
	MaxTokens int    `json:"max_tokens"`
	// System is either a plain string (x-api-key path) or a []anthropicSystemBlock
	// array (OAuth path — the Claude Code client sends system as text blocks,
	// and Anthropic validates that block[0] is the canonical Claude Code prompt).
	System        any                  `json:"system,omitempty"`
	Messages      []anthropicMessage   `json:"messages"`
	Stream        bool                 `json:"stream,omitempty"`
	Tools         []anthropicTool      `json:"tools,omitempty"`
	ToolChoice    *anthropicToolChoice `json:"tool_choice,omitempty"`
	Temperature   *float64             `json:"temperature,omitempty"`
	TopP          *float64             `json:"top_p,omitempty"`
	StopSequences []string             `json:"stop_sequences,omitempty"`
}

type anthropicMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"` // string or []anthropicContentBlock
}

// anthropicContentBlock is a structured content block for tool_use, tool_result, and image.
type anthropicContentBlock struct {
	Type         string                 `json:"type"`                  // "text", "tool_use", "tool_result", "image"
	Text         string                 `json:"text,omitempty"`        // type == "text"
	ID           string                 `json:"id,omitempty"`          // type == "tool_use"
	ToolUseID    string                 `json:"tool_use_id,omitempty"` // type == "tool_result"
	Name         string                 `json:"name,omitempty"`        // type == "tool_use"
	Input        json.RawMessage        `json:"input,omitempty"`       // type == "tool_use"
	Content      string                 `json:"content,omitempty"`     // type == "tool_result"
	Source       *anthropicImageSource  `json:"source,omitempty"`      // type == "image"
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
	// Thinking block fields (type == "thinking" or "redacted_thinking").
	Thinking  string `json:"thinking,omitempty"`  // type == "thinking"
	Signature string `json:"signature,omitempty"` // type == "thinking" signed block (may be present on redacted_thinking too)
}

// anthropicImageSource is the base64 image payload for Anthropic vision requests.
type anthropicImageSource struct {
	Type      string `json:"type"`       // "base64"
	MediaType string `json:"media_type"` // e.g. "image/png", "image/jpeg"
	Data      string `json:"data"`       // raw base64 data (no prefix)
}

type anthropicTool struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	InputSchema  map[string]interface{} `json:"input_schema"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

// anthropicToolChoice maps ToolChoice strings to Anthropic's object form.
type anthropicToolChoice struct {
	Type string `json:"type"`           // "auto", "none", "any", "tool"
	Name string `json:"name,omitempty"` // only when Type == "tool"
}

// Non-streaming response.
type anthropicResponse struct {
	Type       string             `json:"type"`
	Content    []anthropicContent `json:"content"`
	StopReason string             `json:"stop_reason"`
	Usage      anthropicUsage     `json:"usage"`
}

type anthropicContent struct {
	Type  string          `json:"type"`            // "text" | "tool_use"
	Text  string          `json:"text,omitempty"`  // type == "text"
	ID    string          `json:"id,omitempty"`    // type == "tool_use"
	Name  string          `json:"name,omitempty"`  // type == "tool_use"
	Input json.RawMessage `json:"input,omitempty"` // type == "tool_use"
}

type anthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
}

// SSE event envelope — all streaming event types share this shape.
type anthropicSSEEvent struct {
	Type string `json:"type"`
	// message_start
	Message *anthropicSSEMsg `json:"message,omitempty"`
	// content_block_start / content_block_stop
	Index        int               `json:"index"`
	ContentBlock *anthropicContent `json:"content_block,omitempty"`
	// content_block_delta / message_delta
	Delta *anthropicSSEDelta `json:"delta,omitempty"`
	// message_delta usage
	Usage *anthropicSSEUsage `json:"usage,omitempty"`
}

type anthropicSSEMsg struct {
	Usage anthropicUsage `json:"usage"`
}

type anthropicSSEDelta struct {
	Type        string `json:"type"`                   // text_delta | input_json_delta | stop_reason
	Text        string `json:"text,omitempty"`         // text_delta
	PartialJSON string `json:"partial_json,omitempty"` // input_json_delta
	StopReason  string `json:"stop_reason,omitempty"`  // message_delta
}

type anthropicSSEUsage struct {
	OutputTokens int `json:"output_tokens"`
}

// ── Request builder ───────────────────────────────────────────────────────────

// temperatureDeprecatedPrefixes lists Anthropic model-id prefixes that REJECT
// the temperature/top_p sampling parameters with a 400 ("`temperature` is
// deprecated for this model") instead of ignoring them. Prefix-matched so dated
// release variants (e.g. claude-sonnet-5-20260501) are covered. Verified live
// 2026-07-17: claude-opus-4-7 rejects; claude-sonnet-4-6 and
// claude-haiku-4-5-20251001 still accept. The Claude 5 family follows the
// opus-4-7+ behavior.
//
// "claude-haiku-5" and "claude-mythos-5" were pruned 2026-09-15 (#632): no
// catalog lists either model, so their inclusion was an unverified guess
// rather than an observed 400. Add them back only alongside a live
// verification note like the ones above.
var temperatureDeprecatedPrefixes = []string{
	"claude-opus-4-7",
	"claude-opus-4-8",
	"claude-sonnet-5",
	"claude-opus-5",
	"claude-fable-5",
}

// modelDeprecatesTemperature reports whether the given Anthropic model id
// rejects client-set temperature/top_p. The harness sets a default temperature
// on every dispatch (local models want it), so the wire builder must strip it
// for these models or every dispatch to them 400s on arrival.
func modelDeprecatesTemperature(model string) bool {
	for _, p := range temperatureDeprecatedPrefixes {
		if strings.HasPrefix(model, p) {
			return true
		}
	}
	return false
}

// resolveAnthropicMaxTokens picks the output budget for one dispatch.
//
// The caller's per-request budget WINS over the provider's configured default.
// This mirrors the OpenAI-compatible provider, which has always honoured
// req.MaxTokens (provider_openai.go), and the Ollama provider, which maps it to
// num_predict (provider_ollama.go). Before this, both Anthropic-family
// providers passed their own construction-time constant straight to the wire
// builder and dropped req.MaxTokens on the floor: a client asking for 32000
// output tokens through /v1/chat/completions or /v1/messages was silently
// clamped to anthropicDefaultMaxToks (8192) and got finish_reason="length"
// mid-answer with no indication the ceiling was the kernel's, not the model's.
//
// providerDefault is still the floor when a request carries no budget (0), so
// Capabilities().MaxOutputTokens remains the documented default — it is a
// default, not a ceiling. No upper clamp is applied here: the per-model output
// cap is Anthropic's to enforce, and inventing a local ceiling would recreate
// the same silent-clamp defect one layer up. An over-large request surfaces as
// an upstream 400 the caller can read, not as a truncated answer.
func resolveAnthropicMaxTokens(req *CompletionRequest, providerDefault int) int {
	if req != nil && req.MaxTokens > 0 {
		return req.MaxTokens
	}
	return providerDefault
}

// buildAnthropicRequest converts a CompletionRequest to the Anthropic wire format.
// Context items are prepended to the system prompt as labelled sections so the
// model sees full workspace attentional field content.
//
// maxTokens is the PROVIDER DEFAULT, applied only when req carries no
// per-request budget — see resolveAnthropicMaxTokens.
func buildAnthropicRequest(model string, req *CompletionRequest, stream bool, maxTokens int) *anthropicRequest {
	ar := &anthropicRequest{
		Model:         model,
		MaxTokens:     resolveAnthropicMaxTokens(req, maxTokens),
		System:        buildAnthropicSystem(req),
		Stream:        stream,
		Temperature:   req.Temperature,
		TopP:          req.TopP,
		StopSequences: req.Stop,
	}
	if modelDeprecatesTemperature(model) {
		ar.Temperature = nil
		ar.TopP = nil
	}

	// Map conversation messages to the Anthropic wire format.
	// role:"tool" messages become role:"user" with a tool_result content block;
	// each is emitted as a fresh user message (normalizeAnthropicMessages below
	// will merge consecutive user messages and enforce I2/I4 block-order).
	// The old merge-into-preceding-user logic (ad-hoc I2/I4) is subsumed by the
	// normalizer; emit one fresh user message per tool result here.
	ar.Messages = make([]anthropicMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		switch {
		case m.Role == "tool" && m.ToolCallID != "":
			// Tool result: always emit a fresh role:"user" message with one
			// tool_result block. The normalizer (I2) will merge consecutive user
			// messages and (I4) will ensure tool_result blocks lead.
			ar.Messages = append(ar.Messages, anthropicMessage{
				Role: "user",
				Content: []anthropicContentBlock{{
					Type:      "tool_result",
					ToolUseID: m.ToolCallID,
					Content:   m.Content,
				}},
			})

		case m.Role == "assistant" && len(m.ToolCalls) > 0:
			// Assistant message with tool calls: include both text and tool_use blocks.
			var blocks []anthropicContentBlock
			if m.Content != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "text", Text: m.Content})
			}
			for _, tc := range m.ToolCalls {
				blocks = append(blocks, anthropicContentBlock{
					Type:  "tool_use",
					ID:    tc.ID,
					Name:  tc.Name,
					Input: json.RawMessage(tc.Arguments),
				})
			}
			ar.Messages = append(ar.Messages, anthropicMessage{Role: "assistant", Content: blocks})

		default:
			// If the message has multi-modal content parts with images, convert
			// them to Anthropic's structured content block format.
			if hasImageParts(m.ContentParts) {
				blocks := contentPartsToAnthropicBlocks(m.ContentParts)
				ar.Messages = append(ar.Messages, anthropicMessage{Role: m.Role, Content: blocks})
			} else {
				ar.Messages = append(ar.Messages, anthropicMessage{Role: m.Role, Content: m.Content})
			}
		}
	}

	// Map tools.
	if len(req.Tools) > 0 {
		ar.Tools = make([]anthropicTool, len(req.Tools))
		for i, t := range req.Tools {
			ar.Tools[i] = anthropicTool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema}
		}
	}

	// Map tool choice.
	if req.ToolChoice != "" {
		ar.ToolChoice = mapAnthropicToolChoice(req.ToolChoice)
	}

	// Apply the wire-layer normalizer: enforces I1-I6 on the FINAL block
	// structure. This is the universal FORMAT safety net, replacing the
	// pre-conversion repairToolPairing call (which ran on ProviderMessages
	// before conversion and could not see block order — the F7 class of bugs).
	repaired, rpt := normalizeAnthropicMessages(ar.Messages)
	ar.Messages = repaired
	rpt.emit("buildAnthropicRequest")

	// Prompt-cache breakpoints on the final block structure. This is the
	// shared exit for BOTH Anthropic-wire providers (API-key AnthropicProvider
	// and ClaudeOAuthProvider), so neither can ship without them. The OAuth
	// path re-applies after its late mutators (system relocation, tool-name
	// rewrite, second normalize); the call is idempotent.
	applyAnthropicCacheBreakpoints(ar)

	return ar
}

// buildAnthropicSystem assembles the system string: context items first (as
// labelled comment blocks), then the nucleus SystemPrompt.
func buildAnthropicSystem(req *CompletionRequest) string {
	if len(req.Context) == 0 {
		return req.SystemPrompt
	}
	var sb strings.Builder
	for _, item := range req.Context {
		if item.Content == "" {
			continue
		}
		fmt.Fprintf(&sb, "<!-- context id=%q zone=%s salience=%.2f -->\n%s\n\n",
			item.ID, item.Zone, item.Salience, item.Content)
	}
	if req.SystemPrompt != "" {
		sb.WriteString(req.SystemPrompt)
	}
	return sb.String()
}

// mapAnthropicToolChoice converts the provider-agnostic ToolChoice string to
// Anthropic's object format.
func mapAnthropicToolChoice(tc string) *anthropicToolChoice {
	switch tc {
	case "auto":
		return &anthropicToolChoice{Type: "auto"}
	case "none":
		return &anthropicToolChoice{Type: "none"}
	case "required":
		return &anthropicToolChoice{Type: "any"} // Anthropic uses "any" for "required"
	default:
		slog.Warn("unknown tool_choice value, treating as tool name", "value", tc)
		return &anthropicToolChoice{Type: "tool", Name: tc}
	}
}

// ── Image helpers ────────────────────────────────────────────────────────────

// hasImageParts reports whether any ContentPart carries an image_url.
func hasImageParts(parts []ContentPart) bool {
	for _, p := range parts {
		if p.Type == "image_url" && p.ImageURL != "" {
			return true
		}
	}
	return false
}

// contentPartsToAnthropicBlocks converts provider-agnostic ContentParts into
// Anthropic content blocks, translating OpenAI-format image_url data URIs
// (data:image/png;base64,AAAA...) into Anthropic's image source format.
func contentPartsToAnthropicBlocks(parts []ContentPart) []anthropicContentBlock {
	blocks := make([]anthropicContentBlock, 0, len(parts))
	for _, p := range parts {
		switch p.Type {
		case "text":
			if p.Text != "" {
				blocks = append(blocks, anthropicContentBlock{Type: "text", Text: p.Text})
			}
		case "image_url":
			mediaType, data := parseDataURI(p.ImageURL)
			if data != "" {
				blocks = append(blocks, anthropicContentBlock{
					Type: "image",
					Source: &anthropicImageSource{
						Type:      "base64",
						MediaType: mediaType,
						Data:      data,
					},
				})
			}
		}
	}
	return blocks
}

// parseDataURI splits a data URI (e.g. "data:image/png;base64,AAAA...") into
// its media type and base64 payload. Returns ("image/png", "") for unparseable
// input so callers can skip gracefully.
func parseDataURI(uri string) (mediaType, data string) {
	// Expected format: data:<mediaType>;base64,<data>
	if !strings.HasPrefix(uri, "data:") {
		return "image/png", ""
	}
	rest := strings.TrimPrefix(uri, "data:")
	semicolon := strings.Index(rest, ";base64,")
	if semicolon < 0 {
		return "image/png", ""
	}
	mediaType = rest[:semicolon]
	data = rest[semicolon+len(";base64,"):]
	if mediaType == "" {
		mediaType = "image/png"
	}
	return mediaType, data
}

// ── Complete ──────────────────────────────────────────────────────────────────

// Complete sends a non-streaming request and returns the full response.
func (p *AnthropicProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()
	model := p.effectiveModel(req)

	payload := buildAnthropicRequest(model, req, false, p.maxTokens)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	p.setAuthHeaders(httpReq)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("anthropic: status %d: %s", resp.StatusCode, string(data))
	}

	var ar anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, fmt.Errorf("anthropic: decode response: %w", err)
	}

	return parseAnthropicResponse(&ar, model, p.name, time.Since(start)), nil
}

// parseAnthropicResponse converts an anthropicResponse into a provider-agnostic
// CompletionResponse, extracting text and tool_use content blocks.
func parseAnthropicResponse(ar *anthropicResponse, model, providerName string, latency time.Duration) *CompletionResponse {
	var text strings.Builder
	var toolCalls []ToolCall

	for _, block := range ar.Content {
		switch block.Type {
		case "text":
			text.WriteString(block.Text)
		case "tool_use":
			args := "{}"
			if block.Input != nil {
				args = string(block.Input)
			}
			toolCalls = append(toolCalls, ToolCall{
				ID:        block.ID,
				Name:      block.Name,
				Arguments: args,
			})
		}
	}

	stopReason := ar.StopReason
	if stopReason == "" {
		stopReason = "end_turn"
	}

	return &CompletionResponse{
		Content:    text.String(),
		ToolCalls:  toolCalls,
		StopReason: stopReason,
		Usage: TokenUsage{
			InputTokens:      ar.Usage.InputTokens,
			OutputTokens:     ar.Usage.OutputTokens,
			CacheReadTokens:  ar.Usage.CacheReadInputTokens,
			CacheWriteTokens: ar.Usage.CacheCreationInputTokens,
		},
		ProviderMeta: ProviderMeta{
			Provider: providerName,
			Model:    model,
			Latency:  latency,
		},
	}
}

// ── Stream ────────────────────────────────────────────────────────────────────

// Stream sends a streaming request and returns a channel of incremental chunks.
// The channel closes when generation is complete or the context is cancelled.
func (p *AnthropicProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	model := p.effectiveModel(req)

	payload := buildAnthropicRequest(model, req, true, p.maxTokens)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal stream request: %w", err)
	}

	// Use a no-timeout HTTP client for streaming — generation can run long.
	streamClient := &http.Client{}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build stream request: %w", err)
	}
	p.setAuthHeaders(httpReq)

	resp, err := streamClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: stream request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("anthropic: stream status %d: %s", resp.StatusCode, string(data))
	}

	ch := make(chan StreamChunk, 32)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		parseAnthropicSSE(ctx, resp.Body, ch, model, p.name)
	}()

	return ch, nil
}

// parseAnthropicSSE reads the Anthropic SSE stream and sends StreamChunks.
//
// Anthropic SSE format: each event is one or more "data: <json>" lines
// (no "event:" lines with type; the type is inside the JSON).
//
// Event sequence for a text response:
//
//	message_start → content_block_start (text) → content_block_delta (text_delta)...
//	→ content_block_stop → message_delta (stop_reason, usage) → message_stop
//
// Event sequence for a tool_use response:
//
//	message_start → content_block_start (tool_use, id+name) →
//	content_block_delta (input_json_delta)... → content_block_stop →
//	message_delta → message_stop
//
// Tool calls are emitted as ToolCallDelta chunks as they arrive, so the kernel
// can reconstruct complete ToolCall objects from the delta stream.
func parseAnthropicSSE(ctx context.Context, r io.Reader, ch chan<- StreamChunk, model, providerName string) {
	var inputTokens, outputTokens, cacheRead, cacheWrite int
	var stopReason string

	send := func(sc StreamChunk) bool {
		select {
		case ch <- sc:
			return true
		case <-ctx.Done():
			return false
		}
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		// SSE data lines: "data: <payload>"
		// Blank lines and "event:" type lines are ignored (type is in the JSON).
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var event anthropicSSEEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			send(StreamChunk{Error: fmt.Errorf("anthropic: decode SSE event: %w", err)})
			return
		}

		switch event.Type {
		case "message_start":
			if event.Message != nil {
				inputTokens = event.Message.Usage.InputTokens
				cacheRead = event.Message.Usage.CacheReadInputTokens
				cacheWrite = event.Message.Usage.CacheCreationInputTokens
			}

		case "content_block_start":
			if event.ContentBlock == nil {
				continue
			}
			if event.ContentBlock.Type == "tool_use" {
				// Signal the start of a tool call: emit id and name.
				if !send(StreamChunk{
					ToolCallDelta: &ToolCallDelta{
						Index: event.Index,
						ID:    event.ContentBlock.ID,
						Name:  event.ContentBlock.Name,
					},
				}) {
					return
				}
			}

		case "content_block_delta":
			if event.Delta == nil {
				continue
			}
			switch event.Delta.Type {
			case "text_delta":
				if !send(StreamChunk{Delta: event.Delta.Text}) {
					return
				}
			case "input_json_delta":
				// Stream partial JSON for the current tool call.
				if !send(StreamChunk{
					ToolCallDelta: &ToolCallDelta{
						Index:     event.Index,
						ArgsDelta: event.Delta.PartialJSON,
					},
				}) {
					return
				}
			}

		case "message_delta":
			// Carries final stop_reason and output token count.
			if event.Delta != nil && event.Delta.StopReason != "" {
				stopReason = event.Delta.StopReason
			}
			if event.Usage != nil {
				outputTokens = event.Usage.OutputTokens
			}

		case "message_stop":
			// Terminal event: emit the Done chunk with usage and provenance.
			if stopReason == "" {
				stopReason = "end_turn"
			}
			send(StreamChunk{
				Done:       true,
				StopReason: stopReason,
				Usage: &TokenUsage{
					InputTokens:      inputTokens,
					OutputTokens:     outputTokens,
					CacheReadTokens:  cacheRead,
					CacheWriteTokens: cacheWrite,
				},
				ProviderMeta: &ProviderMeta{
					Provider: providerName,
					Model:    model,
				},
			})
			return

		case "error":
			send(StreamChunk{Error: fmt.Errorf("anthropic: stream error event")})
			return

			// "ping", "content_block_stop": no-ops for our purposes.
		}
	}

	if err := scanner.Err(); err != nil {
		send(StreamChunk{Error: fmt.Errorf("anthropic: scan: %w", err)})
	}
}
