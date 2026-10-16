// Package harness provides the inference execution engine for CogOS.
//
// # Architecture
//
// The harness is a separate Go module that owns all inference logic: model
// routing, Claude CLI execution, HTTP provider dispatch, streaming, retries,
// and tool pipeline management. It connects to the kernel through a single
// [KernelServices] interface — the harness never imports package main.
//
//	┌──────────────────────────────────────────────────┐
//	│  kernel (package main)                           │
//	│                                                  │
//	│  serve.go ──► kernel_harness.go ──► harness pkg  │
//	│  cog.go   ──► kernel_harness.go ──► harness pkg  │
//	│                     │                            │
//	│          kernelServicesAdapter                    │
//	│          implements KernelServices               │
//	└──────────────────────────────────────────────────┘
//
// # Common Paths
//
// HTTP request (POST /v1/chat/completions):
//
//	serve.go:handleChatCompletions
//	  → kernel_harness.go:HarnessRunInference / HarnessRunInferenceStream
//	    → converts kernel types → harness types
//	    → harness.Harness.RunInference / RunInferenceStream
//	      → ParseModelProvider (routes claude vs http)
//	      → Claude path: BuildClaudeArgs → exec claude CLI
//	      → HTTP path:   runHTTPInference → OpenAI-compatible API
//	    → converts harness response → kernel types
//	  → serve.go formats OpenAI-compatible HTTP response
//
// CLI request (cog infer "prompt"):
//
//	inference.go:cmdInfer
//	  → kernel_harness.go:HarnessRunInference
//	    → (same path as above)
//
// # File Layout
//
//	harness.go      Harness struct, New(), RunInference, RunInferenceStream, RunInferenceWithRetry
//	interfaces.go   KernelServices interface — the only bridge contract
//	types.go        InferenceRequest/Response, ContextState, ChatMessage, API response types
//	stream.go       StreamChunkInference, Claude CLI wire types, OpenAI wire types
//	providers.go    ProviderType, ProviderConfig, ParseModelProvider, DefaultProviders
//	claude.go       BuildClaudeArgs, chainSystemPrompt, BuildContextMetrics
//	http.go         runHTTPInference, runHTTPInferenceStream (OpenAI-compatible providers)
//	tools.go        MapToolsToCLINames — OpenAI tool defs → Claude CLI --allowed-tools
//	registry.go     RequestRegistry — tracks in-flight requests for cancellation/listing
//	retry.go        ErrorType classification, ClassifyError, ClassifyHTTPError
//	config.go       Node-level config resolution (~/.cog/etc/ → workspace → env → defaults)
//	otel.go         Package-scoped OTEL tracer
package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// harnessIngressVars are environment variable prefixes that, if inherited by a
// spawned subprocess, can redirect the child's model client back through the
// CogOS kernel (routing loop). These mirror the ingressVars list in
// internal/engine/provider_env.go; keep both lists in sync.
var harnessIngressVars = []string{
	"ANTHROPIC_BASE_URL",
	"ANTHROPIC_AUTH_TOKEN",
	"OPENAI_BASE_URL",
	"OPENAI_API_BASE",
	"OPENAI_COMPAT_BASE_URL",
}

// filteredEnviron returns a copy of os.Environ() with variables that would
// cause routing loops stripped out:
//   - CLAUDECODE: Claude Code sets this in its process env; a child `claude`
//     CLI that inherits it refuses to start ("cannot be launched inside another
//     Claude Code session").
//   - The five ingress/proxy routing vars (ANTHROPIC_BASE_URL, etc.): these
//     can redirect a kernel-spawned provider subprocess back through the kernel,
//     producing recursive inference loops (same bug fixed in internal/engine/
//     via PR #130 / issue #59).
func filteredEnviron() []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+1)
	for _, e := range env {
		if strings.HasPrefix(e, "CLAUDECODE=") {
			continue
		}
		if isHarnessIngressVar(e) {
			continue
		}
		out = append(out, e)
	}
	// Signal to Python hooks that they're running under the CogOS kernel,
	// so they can skip context injection (the kernel handles it natively).
	out = append(out, "COGOS_HARNESS=1")
	return out
}

// isHarnessIngressVar reports whether the KEY=VALUE string kv corresponds to
// one of the known ingress routing variables.
func isHarnessIngressVar(kv string) bool {
	for _, key := range harnessIngressVars {
		if strings.HasPrefix(kv, key+"=") {
			return true
		}
	}
	return false
}

// Harness is the inference execution engine. Create one with [New] and call
// [Harness.RunInference] (sync) or [Harness.RunInferenceStream] (streaming).
//
// The Harness owns a [RequestRegistry] for tracking in-flight requests and
// delegates kernel-specific operations (hooks, events, signals, workspace
// resolution) through the [KernelServices] interface passed to [New].
type Harness struct {
	kernel   KernelServices
	registry *RequestRegistry

	// Active provider state (mutable at runtime)
	activeProvider   ProviderType
	activeProviderMu sync.RWMutex

	// Debug mode
	DebugMode bool
}

// New creates a new Harness connected to the kernel via the KernelServices interface.
func New(kernel KernelServices) *Harness {
	return &Harness{
		kernel:         kernel,
		registry:       NewRequestRegistry(),
		activeProvider: ProviderClaude,
	}
}

// Registry returns the harness's request registry for external use (e.g., HTTP handlers).
func (h *Harness) Registry() *RequestRegistry {
	return h.registry
}

// GetActiveProvider returns the currently active provider
func (h *Harness) GetActiveProvider() ProviderType {
	h.activeProviderMu.RLock()
	defer h.activeProviderMu.RUnlock()
	return h.activeProvider
}

// SetActiveProvider sets the active provider, returns the previous one
func (h *Harness) SetActiveProvider(pt ProviderType) ProviderType {
	h.activeProviderMu.Lock()
	defer h.activeProviderMu.Unlock()
	prev := h.activeProvider
	h.activeProvider = pt
	return prev
}

// injectContinuationContext modifies the request to include continuation context.
// This enables eigenfield persistence across compaction.
func (h *Harness) injectContinuationContext(req *InferenceRequest) {
	if req.Origin == "hook" || req.Origin == "internal" {
		return
	}

	state, err := h.kernel.ReadContinuationState()
	if err != nil {
		return
	}

	if state.ContinuationPrompt == "" {
		return
	}

	continuationContext := fmt.Sprintf("[Eigenfield Continuation] %s\n\n", state.ContinuationPrompt)

	if req.SystemPrompt == "" {
		req.SystemPrompt = continuationContext
	} else {
		req.SystemPrompt = continuationContext + req.SystemPrompt
	}
}

// emitInferenceStart emits an INFERENCE_START event
func (h *Harness) emitInferenceStart(req *InferenceRequest) {
	h.kernel.EmitEvent("INFERENCE_START", map[string]any{
		"request_id": req.ID,
		"model":      req.Model,
		"origin":     req.Origin,
		"prompt":     truncate(req.Prompt, 200),
	})
}

// emitInferenceComplete emits an INFERENCE_COMPLETE event
func (h *Harness) emitInferenceComplete(req *InferenceRequest, resp *InferenceResponse, startTime time.Time) {
	h.kernel.EmitEvent("INFERENCE_COMPLETE", map[string]any{
		"request_id":        req.ID,
		"model":             req.Model,
		"origin":            req.Origin,
		"duration_ms":       time.Since(startTime).Milliseconds(),
		"prompt_tokens":     resp.PromptTokens,
		"completion_tokens": resp.CompletionTokens,
		"finish_reason":     resp.FinishReason,
	})
}

// emitInferenceError emits an INFERENCE_ERROR event
func (h *Harness) emitInferenceError(requestID string, errMsg string) {
	h.kernel.EmitEvent("INFERENCE_ERROR", map[string]any{
		"request_id": requestID,
		"error":      errMsg,
	})
}

// setInferenceActiveSignal sets the signal that inference is active
func (h *Harness) setInferenceActiveSignal(requestID, model, origin string) {
	h.kernel.DepositSignal("inference", "active", "harness", 0.5, map[string]any{
		"request_id": requestID,
		"model":      model,
		"origin":     origin,
	})
}

// clearInferenceActiveSignal clears the inference active signal
func (h *Harness) clearInferenceActiveSignal() {
	h.kernel.RemoveSignal("inference", "active")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// externalToolNameSet returns the normalised set of external (client-owned)
// tool names for a request. Callers use it to decide whether a tool_use
// event from the model should be executed server-side or buffered for the
// client to execute. The set is empty when the request has no external
// tools, making lookups cheap on the common path.
//
// Precedence:
//   - If req.ExternalTools is populated, use it as the authoritative list.
//     This lets the HTTP layer pre-partition and skip re-classification.
//   - Otherwise fall back to partitioning req.Tools on the fly.
func externalToolNameSet(req *InferenceRequest) map[string]bool {
	if req == nil {
		return nil
	}
	source := req.ExternalTools
	if source == nil && len(req.Tools) > 0 {
		_, source = PartitionTools(req.Tools)
	}
	if len(source) == 0 {
		return nil
	}
	names := make(map[string]bool, len(source))
	for _, raw := range source {
		if n := ExtractToolName(raw); n != "" {
			names[n] = true
		}
	}
	return names
}

// RunInference executes a non-streaming inference request and blocks until complete.
//
// Routing is determined by the Model field in the request:
//   - "" or "claude"           → Claude CLI (default path)
//   - "openai/gpt-4o"         → OpenAI API via HTTP
//   - "openrouter/claude-3"   → OpenRouter API via HTTP
//   - "ollama/llama3.2"       → Local Ollama via HTTP
//   - "http://host|model"     → Custom OpenAI-compatible endpoint
//
// The full lifecycle for each request:
//  1. Inject continuation context (eigenfield persistence)
//  2. Dispatch PreInference hook (may block or inject context)
//  3. Register in RequestRegistry (for /v1/requests visibility)
//  4. Route to Claude CLI or HTTP provider
//  5. Emit INFERENCE_START / INFERENCE_COMPLETE / INFERENCE_ERROR events
//  6. Dispatch PostInference hook (artifact extraction, logging)
//  7. Clean up signal field
func (h *Harness) RunInference(req *InferenceRequest) (*InferenceResponse, error) {
	// Inject continuation context for eigenfield persistence
	h.injectContinuationContext(req)

	// Ensure context is set with a timeout
	if req.Context == nil {
		var cancel context.CancelFunc
		timeout := req.Timeout
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		req.Context, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	// Ensure ID is set early for consistent tracking
	if req.ID == "" {
		req.ID = GenerateRequestID(req.Origin)
	}

	// Dispatch PreInference hooks
	preInferenceData := map[string]any{
		"request_id":    req.ID,
		"prompt":        req.Prompt,
		"system_prompt": req.SystemPrompt,
		"model":         req.Model,
		"origin":        req.Origin,
	}
	if hookResult := h.kernel.DispatchHook("PreInference", preInferenceData); hookResult != nil {
		if hookResult.Decision == "block" {
			return nil, fmt.Errorf("inference blocked by hook: %s", hookResult.Message)
		}
		if hookResult.AdditionalContext != "" {
			if req.SystemPrompt == "" {
				req.SystemPrompt = hookResult.AdditionalContext
			} else {
				req.SystemPrompt = hookResult.AdditionalContext + "\n\n" + req.SystemPrompt
			}
		}
	}

	// Parse provider from model string
	providerType, modelName, customConfig := ParseModelProvider(req.Model)

	if providerType == ProviderCodex {
		return h.runCodexInference(req, modelName)
	}

	// Route to HTTP providers for non-Claude models
	if providerType != ProviderClaude {
		startTime := time.Now()

		h.emitInferenceStart(req)
		h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

		ctx, cancel := context.WithCancel(req.Context)
		defer cancel()
		req.Context = ctx

		h.registry.Register(req, cancel)

		resp, err := runHTTPInference(req, providerType, modelName, customConfig)

		if err != nil {
			h.registry.Complete(req.ID, "failed")
			h.emitInferenceError(req.ID, err.Error())
		} else {
			h.registry.Complete(req.ID, "completed")
			h.emitInferenceComplete(req, resp, startTime)
		}
		h.clearInferenceActiveSignal()

		return resp, err
	}

	// === CLAUDE CLI PATH (default) ===

	startTime := time.Now()

	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// OTEL: top-level inference span
	ctx, span := tracer.Start(ctx, "inference.sync",
		trace.WithAttributes(
			attribute.String("model", req.Model),
			attribute.String("origin", req.Origin),
			attribute.Int("tool_count", len(req.Tools)),
		),
	)
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register the request
	var entry *RequestEntry
	entry = h.registry.Register(req, cancel)
	defer func() {
		if entry.Status == "running" {
			h.registry.Complete(req.ID, "completed")
		}
	}()

	h.emitInferenceStart(req)
	h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

	// Classify external tool names so we can capture (not execute) any
	// tool_use events the model emits for client-owned tools. This is the
	// plumbing BrowserOS relies on — without it, browser_* tool calls
	// either fall through silently or fail because Claude CLI doesn't
	// know how to run them.
	externalToolNames := externalToolNameSet(req)

	// Build Claude CLI arguments
	args := BuildClaudeArgs(req)

	// OTEL: child span for CLI execution
	_, cliSpan := tracer.Start(ctx, "claude.cli.exec",
		trace.WithAttributes(
			attribute.Int("arg_count", len(args)),
		),
	)

	// Create command with context for cancellation
	cmd := exec.CommandContext(ctx, ClaudeCommand, args...)
	cmd.Env = filteredEnviron()
	cmd.Dir = h.kernel.ResolveWorkDir(req.WorkspaceRoot)

	// Use Output() instead of StdoutPipe to avoid pipe inheritance issues
	// where child processes (MCP servers) inherit the pipe FD and keep it open,
	// causing bufio.Scanner to block forever waiting for EOF.
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	outputBytes, waitErr := cmd.Output()
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			log.Printf("[harness] Claude CLI failed (exit %d), stderr: %s", exitErr.ExitCode(), stderrBuf.String())
		}
	}

	// Collect output
	var content strings.Builder
	var promptTokens, completionTokens int
	var cacheReadTokens, cacheCreateTokens int
	var costUSD float64
	var finishReason string
	var claudeSessionID string
	// externalCalls buffers tool_use events the model emits for
	// client-owned tools. We return these on the response as ToolCalls
	// rather than executing them; finish_reason becomes "tool_calls".
	var externalCalls []ToolCallData

	// Debug: capture raw stream if COG_DEBUG_INFERENCE is set
	debugFile := os.Getenv("COG_DEBUG_INFERENCE")
	var debugWriter *os.File
	if debugFile != "" {
		if f, err := os.OpenFile(debugFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			debugWriter = f
			defer debugWriter.Close()
			fmt.Fprintf(debugWriter, "\n=== Inference Request %s ===\n", req.ID)
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(outputBytes))
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if debugWriter != nil {
			fmt.Fprintf(debugWriter, "%s\n", line)
		}

		var claudeMsg ClaudeStreamMessage
		if err := json.Unmarshal([]byte(line), &claudeMsg); err != nil {
			continue
		}

		switch claudeMsg.Type {
		case "system":
			// Extract Claude CLI session ID from the init event.
			// Wire format: {"type":"system","subtype":"init","session_id":"<uuid>",...}
			var sysMsg struct {
				Subtype   string `json:"subtype"`
				SessionID string `json:"session_id,omitempty"`
			}
			if err := json.Unmarshal([]byte(line), &sysMsg); err == nil {
				if sysMsg.Subtype == "init" && sysMsg.SessionID != "" {
					claudeSessionID = sysMsg.SessionID
				}
			}
		case "assistant":
			if claudeMsg.Message != nil {
				for _, c := range claudeMsg.Message.Content {
					switch c.Type {
					case "text":
						if c.Text != "" {
							content.WriteString(c.Text)
						}
					case "tool_use":
						if c.Name == "StructuredOutput" && len(c.Input) > 0 {
							content.Write(c.Input)
						} else if externalToolNames[c.Name] {
							// Client-owned tool — buffer the call rather
							// than letting Claude CLI try to execute it.
							externalCalls = append(externalCalls, ToolCallData{
								ID:        c.ID,
								Name:      c.Name,
								Arguments: c.Input,
							})
						}
					}
				}
				if claudeMsg.Message.Usage != nil {
					if claudeMsg.Message.Usage.InputTokens > 0 {
						promptTokens = claudeMsg.Message.Usage.InputTokens
					}
					if claudeMsg.Message.Usage.OutputTokens > 0 {
						completionTokens = claudeMsg.Message.Usage.OutputTokens
					}
					if claudeMsg.Message.Usage.CacheReadTokens > 0 {
						cacheReadTokens = claudeMsg.Message.Usage.CacheReadTokens
					}
					if claudeMsg.Message.Usage.CacheCreateTokens > 0 {
						cacheCreateTokens = claudeMsg.Message.Usage.CacheCreateTokens
					}
					if claudeMsg.Message.Usage.CostUSD > 0 {
						costUSD = claudeMsg.Message.Usage.CostUSD
					}
				}
				if claudeMsg.Message.StopReason != "" {
					finishReason = claudeMsg.Message.StopReason
				}
			}
		case "result":
			if claudeMsg.Usage != nil {
				if claudeMsg.Usage.InputTokens > 0 {
					promptTokens = claudeMsg.Usage.InputTokens
				}
				if claudeMsg.Usage.OutputTokens > 0 {
					completionTokens = claudeMsg.Usage.OutputTokens
				}
				if claudeMsg.Usage.CacheReadTokens > 0 {
					cacheReadTokens = claudeMsg.Usage.CacheReadTokens
				}
				if claudeMsg.Usage.CacheCreateTokens > 0 {
					cacheCreateTokens = claudeMsg.Usage.CacheCreateTokens
				}
				if claudeMsg.Usage.CostUSD > 0 {
					costUSD = claudeMsg.Usage.CostUSD
				}
			}
			if content.Len() == 0 && len(claudeMsg.StructuredOutput) > 0 {
				content.Write(claudeMsg.StructuredOutput)
			}
			if content.Len() == 0 && claudeMsg.Result != "" {
				content.WriteString(claudeMsg.Result)
			}
			finishReason = "stop"
		}
	}

	// cmd.Output() already waits for the process — no separate cmd.Wait() needed

	// OTEL: end CLI span with exit code
	if waitErr != nil {
		cliSpan.SetAttributes(attribute.Int("exit_code", 1))
	} else {
		cliSpan.SetAttributes(attribute.Int("exit_code", 0))
	}
	cliSpan.End()

	// OTEL: record token counts
	span.SetAttributes(
		attribute.Int("tokens.input", promptTokens),
		attribute.Int("tokens.output", completionTokens),
	)

	if ctx.Err() == context.Canceled {
		h.registry.Complete(req.ID, "cancelled")
		h.emitInferenceError(req.ID, "request cancelled")
		h.clearInferenceActiveSignal()
		return nil, fmt.Errorf("request cancelled")
	}

	// If the model emitted any tool_use events for client-owned tools,
	// surface them as ToolCalls and flip finish_reason so the caller
	// knows to propagate them to the upstream OpenAI response.
	if len(externalCalls) > 0 {
		finishReason = "tool_calls"
	}

	response := &InferenceResponse{
		ID:                req.ID,
		Content:           content.String(),
		PromptTokens:      promptTokens,
		CompletionTokens:  completionTokens,
		CacheReadTokens:   cacheReadTokens,
		CacheCreateTokens: cacheCreateTokens,
		CostUSD:           costUSD,
		FinishReason:      finishReason,
		ContextMetrics:    BuildContextMetrics(req.ContextState),
		ClaudeSessionID:   claudeSessionID,
		ToolCalls:         externalCalls,
	}

	if waitErr != nil {
		h.registry.Complete(req.ID, "failed")
		errMsg := waitErr.Error()
		if stderrBuf.Len() > 0 {
			errMsg = fmt.Sprintf("%s: %s", errMsg, strings.TrimSpace(stderrBuf.String()))
		}
		response.Error = waitErr
		response.ErrorMessage = errMsg
		response.ErrorType = ClassifyError(waitErr)
		h.emitInferenceError(req.ID, errMsg)
		h.clearInferenceActiveSignal()
	} else {
		h.emitInferenceComplete(req, response, startTime)
		h.clearInferenceActiveSignal()

		postInferenceData := map[string]any{
			"request_id":        req.ID,
			"prompt":            req.Prompt,
			"response":          response.Content,
			"model":             req.Model,
			"origin":            req.Origin,
			"prompt_tokens":     response.PromptTokens,
			"completion_tokens": response.CompletionTokens,
		}
		h.kernel.DispatchHook("PostInference", postInferenceData)
	}

	return response, waitErr
}

// RunInferenceWithRetry wraps [Harness.RunInference] with automatic retry for
// transient errors. Uses exponential backoff (1s, 2s, 4s…) capped at 30s.
// Rate limit errors (429) get 2x longer delays. Auth and fatal errors are
// never retried. Set InferenceRequest.MaxRetries to override the default (3).
func (h *Harness) RunInferenceWithRetry(req *InferenceRequest) (*InferenceResponse, error) {
	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}

	var lastResp *InferenceResponse
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			req.ID = GenerateRequestID(req.Origin + "-retry")
		}

		resp, err := h.RunInference(req)

		if err == nil && (resp.Error == nil || resp.ErrorMessage == "") {
			return resp, nil
		}

		lastResp = resp
		lastErr = err

		var errType ErrorType
		if err != nil {
			errType = ClassifyError(err)
		} else if resp != nil && resp.ErrorMessage != "" {
			errType = ClassifyError(fmt.Errorf("%s", resp.ErrorMessage))
		}

		if errType == ErrorAuth || errType == ErrorFatal {
			return resp, err
		}

		if attempt == maxRetries-1 {
			break
		}

		delay := BaseRetryDelay * time.Duration(1<<uint(attempt))
		if errType == ErrorRateLimit {
			delay = delay * 2
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		fmt.Fprintf(os.Stderr, "Inference retry %d/%d after %v (error type: %s)\n",
			attempt+1, maxRetries, delay, errType)

		select {
		case <-time.After(delay):
		case <-req.Context.Done():
			return lastResp, fmt.Errorf("cancelled during retry wait: %w", req.Context.Err())
		}
	}

	if lastErr != nil {
		return lastResp, fmt.Errorf("max retries (%d) exceeded: %w", maxRetries, lastErr)
	}
	if lastResp != nil && lastResp.ErrorMessage != "" {
		return lastResp, fmt.Errorf("max retries (%d) exceeded: %s", maxRetries, lastResp.ErrorMessage)
	}
	return lastResp, fmt.Errorf("max retries (%d) exceeded", maxRetries)
}

// RunInferenceStream executes a streaming inference request and returns immediately.
// The returned channel receives [StreamChunkInference] values and closes when the
// inference completes (look for chunk.Done == true as the final message).
//
// Routing follows the same rules as [Harness.RunInference]. For the Claude CLI path,
// the stream includes rich events: text deltas, tool_use start/delta/stop,
// tool_result, session_info, and a final Done chunk with usage data.
//
// The caller should drain the channel fully. Context cancellation is respected —
// cancelling the request context will terminate the underlying CLI process or
// HTTP connection and close the channel.
func (h *Harness) RunInferenceStream(req *InferenceRequest) (<-chan StreamChunkInference, error) {
	// Inject continuation context
	h.injectContinuationContext(req)

	timeoutCancel := func() {}
	if req.Context == nil {
		timeout := req.Timeout
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		var cancel context.CancelFunc
		req.Context, cancel = context.WithTimeout(context.Background(), timeout)
		timeoutCancel = cancel
	}

	if req.ID == "" {
		req.ID = GenerateRequestID(req.Origin)
	}

	// Dispatch PreInference hooks
	preInferenceData := map[string]any{
		"request_id":    req.ID,
		"prompt":        req.Prompt,
		"system_prompt": req.SystemPrompt,
		"model":         req.Model,
		"origin":        req.Origin,
	}
	if hookResult := h.kernel.DispatchHook("PreInference", preInferenceData); hookResult != nil {
		if hookResult.Decision == "block" {
			timeoutCancel()
			return nil, fmt.Errorf("inference blocked by hook: %s", hookResult.Message)
		}
		if hookResult.AdditionalContext != "" {
			if req.SystemPrompt == "" {
				req.SystemPrompt = hookResult.AdditionalContext
			} else {
				req.SystemPrompt = hookResult.AdditionalContext + "\n\n" + req.SystemPrompt
			}
		}
	}

	providerType, modelName, customConfig := ParseModelProvider(req.Model)

	if providerType == ProviderCodex {
		return h.runCodexInferenceStream(req, modelName)
	}

	// Route to HTTP providers for non-Claude models
	if providerType != ProviderClaude {
		h.emitInferenceStart(req)
		h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

		ctx, cancel := context.WithCancel(req.Context)
		req.Context = ctx

		h.registry.Register(req, cancel)

		chunks, err := runHTTPInferenceStream(req, providerType, modelName, customConfig)
		if err != nil {
			cancel()
			timeoutCancel()
			h.registry.Complete(req.ID, "failed")
			h.emitInferenceError(req.ID, err.Error())
			h.clearInferenceActiveSignal()
			return nil, err
		}

		wrappedChunks := make(chan StreamChunkInference, 100)
		go func() {
			defer close(wrappedChunks)
			defer cancel()
			defer timeoutCancel()
			defer h.clearInferenceActiveSignal()

			for {
				select {
				case <-ctx.Done():
					h.registry.Complete(req.ID, "cancelled")
					return
				case chunk, ok := <-chunks:
					if !ok {
						return
					}
					select {
					case wrappedChunks <- chunk:
					case <-ctx.Done():
						h.registry.Complete(req.ID, "cancelled")
						return
					}
					if chunk.Done {
						if chunk.Error != nil {
							h.registry.Complete(req.ID, "failed")
						} else {
							h.registry.Complete(req.ID, "completed")
						}
						return
					}
				}
			}
		}()

		return wrappedChunks, nil
	}

	// === CLAUDE CLI PATH (default) ===

	startTime := time.Now()
	ctx := req.Context

	// OTEL: top-level inference span
	ctx, span := tracer.Start(ctx, "inference.stream",
		trace.WithAttributes(
			attribute.String("model", req.Model),
			attribute.String("origin", req.Origin),
			attribute.Int("tool_count", len(req.Tools)),
		),
	)

	ctx, cancel := context.WithCancel(ctx)

	var entry *RequestEntry
	entry = h.registry.Register(req, cancel)

	h.emitInferenceStart(req)
	h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

	// Classify client-owned tools so the stream loop can capture (not
	// execute) any tool_use events the model emits for them.
	externalToolNames := externalToolNameSet(req)

	args := BuildClaudeArgs(req)

	_, cliSpan := tracer.Start(ctx, "claude.cli.exec",
		trace.WithAttributes(
			attribute.Int("arg_count", len(args)),
		),
	)

	cmd := exec.CommandContext(ctx, ClaudeCommand, args...)
	cmd.Env = filteredEnviron()
	cmd.Dir = h.kernel.ResolveWorkDir(req.WorkspaceRoot)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cliSpan.End()
		span.End()
		cancel()
		timeoutCancel()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, "failed to create stdout pipe: "+err.Error())
		h.clearInferenceActiveSignal()
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		stdout.Close()
		cliSpan.End()
		span.End()
		cancel()
		timeoutCancel()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, "failed to start Claude: "+err.Error())
		h.clearInferenceActiveSignal()
		return nil, fmt.Errorf("failed to start Claude: %w", err)
	}

	chunks := make(chan StreamChunkInference, 100)

	go func() {
		defer close(chunks)
		defer cancel()
		defer timeoutCancel()
		defer span.End()
		defer cliSpan.End()

		safeSend := func(chunk StreamChunkInference) bool {
			select {
			case chunks <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}

		var promptTokens, completionTokens int
		var cacheReadTokens, cacheCreateTokens int
		var costUSD float64
		var finishReason string
		var fullContent strings.Builder

		activeToolCalls := make(map[int]*ToolCallData)
		// externalCalls accumulates tool_use invocations the model emits
		// for client-owned tools. We keep them internally and flush them
		// in the final chunk (see safeSend near the bottom) so the HTTP
		// layer can emit a single `tool_calls` delta event.
		var externalCalls []ToolCallData
		var sessionID, sessionModel string
		var sessionTools []string

		scanner := bufio.NewScanner(stdout)
		const maxScannerSize = 4 * 1024 * 1024
		scanner.Buffer(make([]byte, maxScannerSize), maxScannerSize)

		gotContent := false
		gotStreamContent := false
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				h.registry.Complete(req.ID, "cancelled")
				h.emitInferenceError(req.ID, "request cancelled")
				h.clearInferenceActiveSignal()
				safeSend(StreamChunkInference{
					ID:    req.ID,
					Done:  true,
					Error: ctx.Err(),
				})
				return
			default:
			}

			line := scanner.Text()
			if line == "" {
				continue
			}

			var msgType struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(line), &msgType); err != nil {
				continue
			}

			// Handle stream_event type for rich streaming
			if msgType.Type == "stream_event" {
				var streamEvent struct {
					Type  string          `json:"type"`
					Event json.RawMessage `json:"event"`
				}
				if err := json.Unmarshal([]byte(line), &streamEvent); err != nil {
					continue
				}

				var eventData struct {
					Type         string `json:"type"`
					Index        int    `json:"index,omitempty"`
					ContentBlock *struct {
						Type  string          `json:"type"`
						Text  string          `json:"text,omitempty"`
						ID    string          `json:"id,omitempty"`
						Name  string          `json:"name,omitempty"`
						Input json.RawMessage `json:"input,omitempty"`
					} `json:"content_block,omitempty"`
					Delta *struct {
						Type        string `json:"type"`
						Text        string `json:"text,omitempty"`
						PartialJSON string `json:"partial_json,omitempty"`
					} `json:"delta,omitempty"`
					Message json.RawMessage `json:"message,omitempty"`
					Usage   *struct {
						InputTokens       int `json:"input_tokens,omitempty"`
						OutputTokens      int `json:"output_tokens,omitempty"`
						CacheReadTokens   int `json:"cache_read_input_tokens,omitempty"`
						CacheCreateTokens int `json:"cache_creation_input_tokens,omitempty"`
					} `json:"usage,omitempty"`
				}
				if err := json.Unmarshal(streamEvent.Event, &eventData); err != nil {
					continue
				}

				switch eventData.Type {
				case "content_block_start":
					if eventData.ContentBlock != nil {
						switch eventData.ContentBlock.Type {
						case "tool_use":
							cliSpan.AddEvent("tool_use", trace.WithAttributes(
								attribute.String("tool.name", eventData.ContentBlock.Name),
							))
							activeToolCalls[eventData.Index] = &ToolCallData{
								ID:        eventData.ContentBlock.ID,
								Name:      eventData.ContentBlock.Name,
								Arguments: json.RawMessage(""),
							}
							if !safeSend(StreamChunkInference{
								ID:        req.ID,
								EventType: "tool_use_start",
								ToolCall: &ToolCallData{
									ID:   eventData.ContentBlock.ID,
									Name: eventData.ContentBlock.Name,
								},
								Done: false,
							}) {
								return
							}
						}
					}

				case "content_block_delta":
					if eventData.Delta != nil {
						switch eventData.Delta.Type {
						case "text_delta":
							if eventData.Delta.Text != "" {
								gotContent = true
								gotStreamContent = true
								fullContent.WriteString(eventData.Delta.Text)
								if !safeSend(StreamChunkInference{
									ID:        req.ID,
									Content:   eventData.Delta.Text,
									EventType: "text",
									Done:      false,
								}) {
									return
								}
							}
						case "input_json_delta":
							if tc, ok := activeToolCalls[eventData.Index]; ok {
								tc.Arguments = append(tc.Arguments, []byte(eventData.Delta.PartialJSON)...)
								if !safeSend(StreamChunkInference{
									ID:        req.ID,
									Content:   eventData.Delta.PartialJSON,
									EventType: "tool_use_delta",
									ToolCall: &ToolCallData{
										ID:        tc.ID,
										Name:      tc.Name,
										Arguments: json.RawMessage(eventData.Delta.PartialJSON),
									},
									Done: false,
								}) {
									return
								}
							}
						}
					}

				case "content_block_stop":
					if tc, ok := activeToolCalls[eventData.Index]; ok {
						// Buffer client-owned tool calls so we can emit
						// them on the final chunk as `tool_calls`.
						// Internal tools still pass through as regular
						// tool_use stream events for the MCP bridge or
						// CLI to execute.
						if externalToolNames[tc.Name] {
							externalCalls = append(externalCalls, *tc)
						} else if !safeSend(StreamChunkInference{
							ID:        req.ID,
							EventType: "tool_use",
							ToolCall:  tc,
							Done:      false,
						}) {
							return
						}
						delete(activeToolCalls, eventData.Index)
					}

				case "message_start":
					if len(eventData.Message) > 0 {
						var msgStart struct {
							ID    string `json:"id"`
							Model string `json:"model"`
							Usage *struct {
								InputTokens       int `json:"input_tokens,omitempty"`
								CacheReadTokens   int `json:"cache_read_input_tokens,omitempty"`
								CacheCreateTokens int `json:"cache_creation_input_tokens,omitempty"`
							} `json:"usage,omitempty"`
						}
						if err := json.Unmarshal(eventData.Message, &msgStart); err == nil {
							// Only use message ID as session ID if we don't already have a
							// proper session UUID from the system/init event. message_start IDs
							// are message IDs (msg_...), not session UUIDs, and can't be used
							// with --resume.
							if sessionID == "" {
								sessionID = msgStart.ID
							}
							sessionModel = msgStart.Model
							if msgStart.Usage != nil {
								promptTokens = msgStart.Usage.InputTokens
								cacheReadTokens = msgStart.Usage.CacheReadTokens
								cacheCreateTokens = msgStart.Usage.CacheCreateTokens
							}
							if !safeSend(StreamChunkInference{
								ID:        req.ID,
								EventType: "session_start",
								SessionInfo: &SessionInfo{
									SessionID: sessionID,
									Model:     sessionModel,
									Tools:     sessionTools,
								},
								Done: false,
							}) {
								return
							}
						}
					}

				case "message_delta":
					if eventData.Usage != nil {
						completionTokens = eventData.Usage.OutputTokens
					}

				case "message_stop":
					finishReason = "stop"
				}
				continue
			}

			// Handle system/init for session metadata
			if msgType.Type == "system" {
				var sysMsg struct {
					Type      string   `json:"type"`
					Subtype   string   `json:"subtype,omitempty"`
					SessionID string   `json:"session_id,omitempty"`
					Model     string   `json:"model,omitempty"`
					Tools     []string `json:"tools,omitempty"`
				}
				if err := json.Unmarshal([]byte(line), &sysMsg); err == nil {
					if sysMsg.Subtype == "init" && sysMsg.SessionID != "" {
						sessionID = sysMsg.SessionID
						sessionModel = sysMsg.Model
						sessionTools = sysMsg.Tools
						if !safeSend(StreamChunkInference{
							ID:        req.ID,
							EventType: "session_info",
							SessionInfo: &SessionInfo{
								SessionID: sessionID,
								Model:     sessionModel,
								Tools:     sessionTools,
							},
							Done: false,
						}) {
							return
						}
					}
				}
				continue
			}

			// Fall back to original ClaudeStreamMessage parsing
			var claudeMsg ClaudeStreamMessage
			if err := json.Unmarshal([]byte(line), &claudeMsg); err != nil {
				continue
			}

			switch claudeMsg.Type {
			case "assistant":
				if claudeMsg.Message != nil {
					for _, c := range claudeMsg.Message.Content {
						switch c.Type {
						case "text":
							if c.Text != "" && !gotStreamContent {
								gotContent = true
								fullContent.WriteString(c.Text)
								if !safeSend(StreamChunkInference{
									ID:        req.ID,
									Content:   c.Text,
									EventType: "text",
									Done:      false,
								}) {
									return
								}
							}
						case "tool_use":
							if c.Name == "StructuredOutput" && len(c.Input) > 0 {
								gotContent = true
								fullContent.WriteString(string(c.Input))
								if !safeSend(StreamChunkInference{
									ID:        req.ID,
									Content:   string(c.Input),
									EventType: "text",
									Done:      false,
								}) {
									return
								}
							} else if c.Name != "" {
								cliSpan.AddEvent("tool_use", trace.WithAttributes(
									attribute.String("tool.name", c.Name),
								))
								if externalToolNames[c.Name] {
									externalCalls = append(externalCalls, ToolCallData{
										ID:        c.ID,
										Name:      c.Name,
										Arguments: c.Input,
									})
								} else if !safeSend(StreamChunkInference{
									ID:        req.ID,
									EventType: "tool_use",
									ToolCall: &ToolCallData{
										ID:        c.ID,
										Name:      c.Name,
										Arguments: c.Input,
									},
									Done: false,
								}) {
									return
								}
							}
						}
					}
					if claudeMsg.Message.Usage != nil {
						if claudeMsg.Message.Usage.InputTokens > 0 {
							promptTokens = claudeMsg.Message.Usage.InputTokens
						}
						if claudeMsg.Message.Usage.OutputTokens > 0 {
							completionTokens = claudeMsg.Message.Usage.OutputTokens
						}
					}
					if claudeMsg.Message.StopReason != "" {
						finishReason = claudeMsg.Message.StopReason
					}
				}
			case "user":
				if claudeMsg.Message != nil {
					for _, c := range claudeMsg.Message.Content {
						if c.Type == "tool_result" && c.ToolUseID != "" {
							cliSpan.AddEvent("tool_result", trace.WithAttributes(
								attribute.String("tool.id", c.ToolUseID),
							))
							if h.DebugMode {
								log.Printf("[DEBUG] Received tool_result for tool %s (isError=%v)", c.ToolUseID, c.IsError)
							}
							if !safeSend(StreamChunkInference{
								ID:        req.ID,
								EventType: "tool_result",
								ToolResult: &ToolResultData{
									ToolCallID: c.ToolUseID,
									Content:    c.Content,
									IsError:    c.IsError,
								},
								Done: false,
							}) {
								return
							}
						}
					}
				}
			case "result":
				if h.DebugMode {
					log.Printf("[DEBUG] Received 'result' message from Claude CLI (NOT emitting Done)")
				}
				if claudeMsg.Usage != nil {
					if claudeMsg.Usage.InputTokens > 0 {
						promptTokens = claudeMsg.Usage.InputTokens
					}
					if claudeMsg.Usage.OutputTokens > 0 {
						completionTokens = claudeMsg.Usage.OutputTokens
					}
				}
				var resultMsg struct {
					CostUSD float64 `json:"cost_usd,omitempty"`
				}
				json.Unmarshal([]byte(line), &resultMsg)
				if resultMsg.CostUSD > 0 {
					costUSD = resultMsg.CostUSD
				}

				if !gotContent && len(claudeMsg.StructuredOutput) > 0 {
					if !safeSend(StreamChunkInference{
						ID:        req.ID,
						Content:   string(claudeMsg.StructuredOutput),
						EventType: "text",
						Done:      false,
					}) {
						return
					}
					gotContent = true
				}
				if !gotContent && claudeMsg.Result != "" {
					if !safeSend(StreamChunkInference{
						ID:        req.ID,
						Content:   claudeMsg.Result,
						EventType: "text",
						Done:      false,
					}) {
						return
					}
					gotContent = true
				}
				finishReason = "stop"
			}
		}

		// Check for scanner errors
		if err := scanner.Err(); err != nil {
			log.Printf("[ERROR] Scanner error while reading Claude CLI output: %v", err)
			h.emitInferenceError(req.ID, "scanner error: "+err.Error())
			h.clearInferenceActiveSignal()
			cliSpan.End()
			safeSend(StreamChunkInference{
				ID:    req.ID,
				Done:  true,
				Error: fmt.Errorf("scanner error: %w", err),
			})
			cmd.Process.Kill()
			return
		}

		if h.DebugMode {
			log.Printf("[DEBUG] Scanner loop finished, waiting for Claude CLI to exit...")
		}
		waitErr := cmd.Wait()
		if h.DebugMode {
			log.Printf("[DEBUG] Claude CLI exited (err=%v), will now emit Done chunk", waitErr)
		}

		if waitErr != nil {
			cliSpan.SetAttributes(attribute.Int("exit_code", 1))
		} else {
			cliSpan.SetAttributes(attribute.Int("exit_code", 0))
		}
		cliSpan.End()

		span.SetAttributes(
			attribute.Int("tokens.input", promptTokens),
			attribute.Int("tokens.output", completionTokens),
		)

		if entry.Status == "running" {
			if waitErr != nil {
				h.registry.Complete(req.ID, "failed")
			} else {
				h.registry.Complete(req.ID, "completed")
			}
		}

		if waitErr != nil {
			h.emitInferenceError(req.ID, waitErr.Error())
			h.clearInferenceActiveSignal()
			safeSend(StreamChunkInference{
				ID:    req.ID,
				Done:  true,
				Error: waitErr,
			})
		} else {
			// External tool calls flip finish_reason to "tool_calls" per
			// the OpenAI contract so a BrowserOS-style client knows the
			// turn ended because the model wants it to execute a tool.
			if len(externalCalls) > 0 {
				finishReason = "tool_calls"
			}
			resp := &InferenceResponse{
				ID:               req.ID,
				Content:          fullContent.String(),
				PromptTokens:     promptTokens,
				CompletionTokens: completionTokens,
				FinishReason:     finishReason,
				ToolCalls:        externalCalls,
			}
			h.emitInferenceComplete(req, resp, startTime)
			h.clearInferenceActiveSignal()

			safeSend(StreamChunkInference{
				ID:                req.ID,
				Done:              true,
				FinishReason:      finishReason,
				ExternalToolCalls: externalCalls,
				Usage: &UsageData{
					InputTokens:       promptTokens,
					OutputTokens:      completionTokens,
					CacheReadTokens:   cacheReadTokens,
					CacheCreateTokens: cacheCreateTokens,
					CostUSD:           costUSD,
				},
				SessionInfo: &SessionInfo{
					ClaudeSessionID: sessionID, // Return Claude CLI session for --resume on next call
				},
			})

			postInferenceData := map[string]any{
				"request_id":        req.ID,
				"prompt":            req.Prompt,
				"response":          fullContent.String(),
				"model":             req.Model,
				"origin":            req.Origin,
				"prompt_tokens":     promptTokens,
				"completion_tokens": completionTokens,
			}
			h.kernel.DispatchHook("PostInference", postInferenceData)
		}
	}()

	return chunks, nil
}
