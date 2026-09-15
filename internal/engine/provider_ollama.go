// provider_ollama.go — OllamaProvider [DECOMMISSIONED]
//
// Ollama has been removed as the default and supported local inference backend.
// The replacement is LM Studio via the named provider "lmstudio-darkstar"
// (OpenAI-compat at 127.0.0.1:1234, resident gemma-4-26b).
//
// This file is retained so that:
//  1. Nodes with "type: ollama" in providers.yaml continue to compile and
//     produce a clear error rather than a silent type-mismatch crash.
//  2. The buildLocalProvider path still handles LocalLLMBackendOllama if the
//     Ollama probe succeeds on a non-standard node configuration.
//
// New callers must NOT reference OllamaProvider or defaultOllamaModel.
// The Ollama integration test and the "ollama" provider registration in
// defaults/providers.yaml have been removed.
package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// defaultOllamaModel is retained only for backward compat with tests and
// configs that reference it. Ollama is decommissioned; do not use this as
// a routing default. The resident local model is google/gemma-4-26b via
// LM Studio (lmstudio-darkstar).
const defaultOllamaModel = "gemma4:e4b"

type ollamaModelProfile struct {
	Capabilities     []Capability
	MaxContextTokens int
	MaxOutputTokens  int
}

var ollamaModelProfiles = map[string]ollamaModelProfile{
	"gemma4:e4b": {
		Capabilities:     []Capability{CapStreaming, CapJSON, CapToolCallValidation},
		MaxContextTokens: 128000,
		MaxOutputTokens:  4096,
	},
	"gemma4:e2b": {
		Capabilities:     []Capability{CapStreaming, CapJSON, CapToolCallValidation},
		MaxContextTokens: 128000,
		MaxOutputTokens:  4096,
	},
	"qwen3.5:9b": {
		Capabilities:    []Capability{CapStreaming, CapJSON},
		MaxOutputTokens: 4096,
	},
}

func lookupOllamaModelProfile(model string) ollamaModelProfile {
	if profile, ok := ollamaModelProfiles[model]; ok {
		return profile
	}
	return ollamaModelProfile{
		Capabilities:    []Capability{CapStreaming, CapJSON},
		MaxOutputTokens: 4096,
	}
}

// OllamaProvider implements Provider against a local Ollama server.
type OllamaProvider struct {
	name          string
	endpoint      string // e.g. "http://localhost:11434"
	model         string
	contextWindow int // num_ctx to send per request; 0 = Ollama default (4096)
	timeout       time.Duration
	client        *http.Client

	// availMu guards the Available() TTL cache (#441): the router's 10s
	// availability ticker probes this provider with a live GET /api/tags. Cache
	// the result for availCacheTTL, holding the mutex across the probe so
	// concurrent callers collapse into a single request.
	availMu     sync.Mutex
	availResult bool
	availAt     time.Time
}

// NewOllamaProvider creates an OllamaProvider from a ProviderConfig.
func NewOllamaProvider(name string, cfg ProviderConfig) *OllamaProvider {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "http://localhost:11434"
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return &OllamaProvider{
		name:          name,
		endpoint:      normalizeLocalLLMEndpoint(endpoint),
		model:         cfg.Model,
		contextWindow: cfg.ContextWindow,
		timeout:       timeout,
		client:        &http.Client{Timeout: timeout},
	}
}

// Name returns the provider identifier.
func (p *OllamaProvider) Name() string  { return p.name }
func (p *OllamaProvider) Model() string { return p.model }

// Available checks if Ollama is running and the configured model is loaded. The
// result is cached for availCacheTTL so the router's periodic availability probe
// doesn't issue a live GET /api/tags on every call (#441). The mutex is held
// across the probe so concurrent callers collapse into a single request.
func (p *OllamaProvider) Available(ctx context.Context) bool {
	p.availMu.Lock()
	defer p.availMu.Unlock()
	if !p.availAt.IsZero() && time.Since(p.availAt) < availCacheTTL {
		return p.availResult
	}
	fresh := p.probeAvailable(ctx)
	// Skip caching only for a caller-initiated cancellation; a context deadline
	// is the router's probeTimeout on a slow provider and must be cached as
	// unavailable (#441 review).
	if !fresh && ctx.Err() == context.Canceled {
		return p.availResult
	}
	p.availResult = fresh
	p.availAt = time.Now()
	return p.availResult
}

// probeAvailable performs the live check backing Available(): GET /api/tags and
// confirm the configured model is present. Call via Available() for TTL caching.
//
// The probe caps its own HTTP call at probeHTTPTimeout rather than inheriting
// p.client.Timeout. Available() holds availMu across this call so concurrent
// callers collapse into one probe (#441); without this internal bound a
// hung-but-accepting upstream would let one caller hold availMu for the full
// client timeout, blocking the router's probeAll goroutine and Route()'s inline
// fallback behind availMu.Lock() (inference-pipeline-robustness FIX 1; mirrors
// ClaudeOAuthProvider.probeAvailable).
func (p *OllamaProvider) probeAvailable(ctx context.Context) bool {
	pctx, cancel := context.WithTimeout(ctx, probeHTTPTimeout)
	defer cancel()
	models, err := p.listModels(pctx)
	if err != nil {
		return false
	}
	// Accept exact name or prefix (e.g. "qwen2.5:9b" matches "qwen2.5:9b-instruct").
	for _, m := range models {
		if m == p.model || strings.HasPrefix(m, p.model) {
			return true
		}
	}
	return false
}

// Capabilities returns what Ollama supports.
func (p *OllamaProvider) Capabilities() ProviderCapabilities {
	profile := lookupOllamaModelProfile(p.model)
	ctxTokens := p.contextWindow
	if ctxTokens <= 0 {
		ctxTokens = profile.MaxContextTokens
	}
	if ctxTokens <= 0 {
		ctxTokens = 4096
	}
	maxOutputTokens := profile.MaxOutputTokens
	if maxOutputTokens <= 0 {
		maxOutputTokens = 4096
	}
	return ProviderCapabilities{
		Capabilities:       append([]Capability(nil), profile.Capabilities...),
		MaxContextTokens:   ctxTokens,
		MaxOutputTokens:    maxOutputTokens,
		ModelsAvailable:    []string{p.model},
		IsLocal:            true,
		AgenticHarness:     false,
		CostPerInputToken:  0,
		CostPerOutputToken: 0,
	}
}

// ContextWindow returns the configured num_ctx for this provider.
func (p *OllamaProvider) ContextWindow() int {
	return p.contextWindow
}

// Ping measures round-trip latency to the Ollama server.
func (p *OllamaProvider) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"/api/version", nil)
	if err != nil {
		return 0, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ollama: ping: /api/version returned HTTP %d", resp.StatusCode)
	}
	return time.Since(start), nil
}

// ListModels enumerates the locally-available Ollama models (GET /api/tags).
// Satisfies the ModelLister interface used by the /v1/models composition
// handler.
func (p *OllamaProvider) ListModels(ctx context.Context) ([]string, error) {
	return p.listModels(ctx)
}

// listModels queries GET /api/tags and returns the names of all locally
// available Ollama models. Empty names are filtered out. This is the shared
// implementation used by Available() and any future listing callers, extracted
// to avoid duplicating the HTTP + JSON decode logic.
func (p *OllamaProvider) listModels(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama: /api/tags status %d", resp.StatusCode)
	}
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(tags.Models))
	for _, m := range tags.Models {
		if m.Name == "" {
			continue
		}
		names = append(names, m.Name)
	}
	return names, nil
}

// ── Ollama wire types ─────────────────────────────────────────────────────────

type ollamaMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []ollamaToolCall `json:"tool_calls,omitempty"`
}

type ollamaTool struct {
	Type     string             `json:"type"`
	Function ollamaToolFunction `json:"function"`
}

type ollamaToolFunction struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Parameters  map[string]interface{} `json:"parameters"`
}

type ollamaToolCall struct {
	ID       string               `json:"id,omitempty"`
	Type     string               `json:"type"`
	Function ollamaToolCallDetail `json:"function"`
}

type ollamaToolCallDetail struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type ollamaChatRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Tools    []ollamaTool    `json:"tools,omitempty"`
	Stream   bool            `json:"stream"`
	Think    bool            `json:"think"` // false = disable thinking mode (qwen3)
	Options  map[string]any  `json:"options,omitempty"`
	// KeepAlive controls how long Ollama keeps the model resident after the
	// request completes. Accepts a Go duration string ("5m", "1h") or a
	// number of seconds; -1 keeps the model loaded indefinitely, 0 unloads
	// immediately. Defaulted to -1 in buildOllamaRequest so cycles spaced
	// past Ollama's 5-minute default keep-alive don't pay cold-start.
	KeepAlive any `json:"keep_alive,omitempty"`
}

type ollamaChatResponse struct {
	Model     string        `json:"model"`
	CreatedAt string        `json:"created_at"`
	Message   ollamaMessage `json:"message"`
	Done      bool          `json:"done"`
	// Token counts (only in final streaming chunk or non-streaming response).
	PromptEvalCount int `json:"prompt_eval_count"`
	EvalCount       int `json:"eval_count"`
}

// buildOllamaRequest converts a CompletionRequest to Ollama's /api/chat format.
// contextWindow sets num_ctx on the request; 0 means omit (use Ollama default of 4096).
func buildOllamaRequest(model string, req *CompletionRequest, stream bool, contextWindow int) *ollamaChatRequest {
	msgs := make([]ollamaMessage, 0, len(req.Messages)+1)
	if req.SystemPrompt != "" {
		msgs = append(msgs, ollamaMessage{Role: "system", Content: req.SystemPrompt})
	}
	for _, m := range req.Messages {
		msg := ollamaMessage{
			Role:       m.Role,
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
		}
		if m.Role == "assistant" && len(m.ToolCalls) > 0 {
			for _, tc := range m.ToolCalls {
				// Normalize arguments to a stable JSON object before sending
				// back to Ollama. Some models return arguments as a JSON string
				// rather than an object; decodeOllamaToolArguments unwraps both.
				normalizedArgs := decodeOllamaToolArguments(tc.Arguments)
				rawArgs := json.RawMessage(encodeOllamaToolArguments(normalizedArgs))
				msg.ToolCalls = append(msg.ToolCalls, ollamaToolCall{
					ID:   tc.ID,
					Type: "function",
					Function: ollamaToolCallDetail{
						Name:      tc.Name,
						Arguments: rawArgs,
					},
				})
			}
		}
		msgs = append(msgs, msg)
	}

	opts := map[string]any{}
	if contextWindow > 0 {
		opts["num_ctx"] = contextWindow
	}
	if req.Temperature != nil {
		opts["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		opts["top_p"] = *req.TopP
	}
	if req.MaxTokens != 0 {
		opts["num_predict"] = req.MaxTokens
	}

	var tools []ollamaTool
	if len(req.Tools) > 0 && req.ToolChoice != "none" {
		tools = make([]ollamaTool, 0, len(req.Tools))
		for _, t := range req.Tools {
			tools = append(tools, ollamaTool{
				Type: "function",
				Function: ollamaToolFunction{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.InputSchema,
				},
			})
		}
	}

	// Resolve keep_alive: per-request override takes precedence over the
	// global default of -1. A nil KeepAlive on the request means "use the
	// provider default." A non-nil value (including 0 for cold-start
	// simulation) is forwarded verbatim to Ollama.
	keepAlive := any(-1) // provider default: keep model in VRAM indefinitely
	if req.KeepAlive != nil {
		keepAlive = *req.KeepAlive
	}

	return &ollamaChatRequest{
		Model:     model,
		Messages:  msgs,
		Tools:     tools,
		Stream:    stream,
		Think:     false, // prevent silent token burn in qwen3 thinking mode
		Options:   opts,
		KeepAlive: keepAlive,
	}
}

// decodeOllamaToolArguments normalizes the raw argument value that Ollama
// returns for a tool call. Ollama's /api/chat returns tool-call arguments as
// either a JSON object or a JSON string whose content is itself a JSON object
// — the shape varies by model and version. This function handles both:
//
//   - Object: `{"query":"x"}` → map[string]any{"query":"x"}
//   - String: `"{\"query\":\"x\"}"` → map[string]any{"query":"x"}
//   - Empty / blank → empty map
//   - Malformed → the original string value (caller must tolerate this)
func decodeOllamaToolArguments(raw string) interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]interface{}{}
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var value interface{}
	if err := dec.Decode(&value); err != nil {
		// Not valid JSON at all — return as-is; caller sees the raw string.
		return raw
	}
	// If Ollama encoded the arguments as a JSON string, unwrap one layer.
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return map[string]interface{}{}
		}
		dec2 := json.NewDecoder(strings.NewReader(s))
		dec2.UseNumber()
		var inner interface{}
		if err := dec2.Decode(&inner); err != nil {
			// The string content isn't JSON; return the unwrapped string.
			return s
		}
		return inner
	}
	return value
}

// encodeOllamaToolArguments serializes a normalized argument value (as
// returned by decodeOllamaToolArguments) back to a JSON string suitable for
// downstream callers. Returns "{}" on nil or marshal failure.
func encodeOllamaToolArguments(v interface{}) string {
	if v == nil {
		return "{}"
	}
	data, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// ollamaToolCallID returns a deterministic, stable ID for an Ollama tool call.
//
// Ollama's /api/chat responses often omit the ID field on tool calls, especially
// with older or open-weight models. Downstream consumers and OpenAI-style
// clients require every tool call to carry a unique, stable ID so they can
// correlate tool-call → tool-result pairs.
//
// The ID is derived from a SHA-256 hash of the canonicalized (name, arguments,
// seq) triple and formatted as "call_<12-hex-chars>" to be recognisable but not
// cryptographically meaningful. Same inputs always produce the same ID, making
// the round-trip testable without mocking random state.
func ollamaToolCallID(name string, arguments any, seq int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d\x00%s\x00", seq, name)
	if arguments != nil {
		b, err := json.Marshal(arguments)
		if err == nil {
			h.Write(b)
		}
	}
	return fmt.Sprintf("call_%x", h.Sum(nil)[:6])
}

// effectiveModel returns the model to send to Ollama: request override if set,
// otherwise the provider's configured default.
func (p *OllamaProvider) effectiveModel(req *CompletionRequest) string {
	if req.ModelOverride != "" {
		return req.ModelOverride
	}
	return p.model
}

// Complete sends a non-streaming request and returns the full response.
func (p *OllamaProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()
	model := p.effectiveModel(req)

	payload := buildOllamaRequest(model, req, false, p.contextWindow)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ollama: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ollama: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("ollama: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama: status %d: %s", resp.StatusCode, string(data))
	}

	var or ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&or); err != nil {
		return nil, fmt.Errorf("ollama: decode response: %w", err)
	}

	out := &CompletionResponse{
		Content: or.Message.Content,
		Usage: TokenUsage{
			InputTokens:  or.PromptEvalCount,
			OutputTokens: or.EvalCount,
		},
		ProviderMeta: ProviderMeta{
			Provider: p.name,
			Model:    model,
			Latency:  time.Since(start),
		},
	}
	if len(or.Message.ToolCalls) > 0 {
		out.StopReason = "tool_use"
		for i, tc := range or.Message.ToolCalls {
			// Ollama may return arguments as a JSON object or a JSON string
			// wrapping a JSON object depending on the model. Normalize both.
			decoded := decodeOllamaToolArguments(string(tc.Function.Arguments))
			args := encodeOllamaToolArguments(decoded)
			id := strings.TrimSpace(tc.ID)
			if id == "" {
				id = ollamaToolCallID(tc.Function.Name, tc.Function.Arguments, i)
			}
			out.ToolCalls = append(out.ToolCalls, ToolCall{
				ID:        id,
				Name:      tc.Function.Name,
				Arguments: args,
			})
		}
	} else {
		out.StopReason = "end_turn"
	}
	return out, nil
}

// Stream sends a streaming request and returns a channel of chunks.
// The channel closes when generation is complete or the context is cancelled.
func (p *OllamaProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	model := p.effectiveModel(req)
	payload := buildOllamaRequest(model, req, true, p.contextWindow)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ollama: marshal stream request: %w", err)
	}

	// Use a separate client without a timeout — streaming can be long.
	streamClient := &http.Client{}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ollama: build stream request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := streamClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("ollama: stream request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("ollama: stream status %d: %s", resp.StatusCode, string(data))
	}

	ch := make(chan StreamChunk, 32)
	go func() {
		defer close(ch)
		defer resp.Body.Close()

		// Track a chunk-counting fallback in case the final record from Ollama
		// doesn't surface eval_count (older servers, partial streams). Counts
		// content-bearing chunks — coarse but vastly better than reporting 0.
		contentChunks := 0

		// Track whether any tool_calls were emitted during the stream so we can
		// set the correct OpenAI-compatible finish_reason on the final chunk.
		sawToolCalls := false

		// bufio.Scanner has a 64KB default token limit; some Ollama chunks
		// (long tool-call arguments) can exceed it. Allow up to 1 MiB.
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			var chunk ollamaChatResponse
			if err := json.Unmarshal([]byte(line), &chunk); err != nil {
				select {
				case ch <- StreamChunk{Error: fmt.Errorf("ollama: decode chunk: %w", err)}:
				case <-ctx.Done():
				}
				return
			}
			if !chunk.Done && chunk.Message.Content != "" {
				contentChunks++
			}
			if len(chunk.Message.ToolCalls) > 0 {
				sawToolCalls = true
			}
			sc := StreamChunk{
				Delta: chunk.Message.Content,
				Done:  chunk.Done,
			}
			if chunk.Done {
				outputTokens := chunk.EvalCount
				if outputTokens == 0 {
					// Fallback: Ollama didn't surface eval_count. Use the
					// number of content-bearing chunks as an estimate.
					outputTokens = contentChunks
				}
				sc.Usage = &TokenUsage{
					InputTokens:  chunk.PromptEvalCount,
					OutputTokens: outputTokens,
				}
				sc.ProviderMeta = &ProviderMeta{
					Provider: p.name,
					Model:    model,
				}
				if sawToolCalls {
					sc.StopReason = "tool_use"
				}
			}
			select {
			case ch <- sc:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case ch <- StreamChunk{Error: fmt.Errorf("ollama: scan: %w", err)}:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}
