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
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	CodexCommand = "codex"
	// defaultCodexModel is the fallback model id used when a caller passes
	// the bare "codex" alias without a specific model. codex-cli's own
	// catalog (~/.codex/models_cache.json) drifts over time as OpenAI
	// retires/renames slugs (see myrgic/cogos#627, where CodexProvider's
	// equivalent hardcoded default had already gone stale); this harness
	// package is a separate Go module without access to internal/engine's
	// config.toml/models_cache.json discovery, so the id here needs to be
	// re-checked against a live models_cache.json periodically rather than
	// assumed evergreen.
	defaultCodexModel = "gpt-5.6-terra"
)

type codexOutputState struct {
	content          string
	sessionID        string
	promptTokens     int
	completionTokens int
	cacheReadTokens  int
	finishReason     string
	errorMessage     string
}

type codexThreadEvent struct {
	Type     string           `json:"type"`
	ThreadID string           `json:"thread_id,omitempty"`
	Item     *codexThreadItem `json:"item,omitempty"`
	Usage    *codexUsage      `json:"usage,omitempty"`
	Error    *codexError      `json:"error,omitempty"`
	Message  string           `json:"message,omitempty"`
}

type codexThreadItem struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Text   string `json:"text,omitempty"`
	Status string `json:"status,omitempty"`
}

type codexUsage struct {
	InputTokens       int `json:"input_tokens"`
	CachedInputTokens int `json:"cached_input_tokens,omitempty"`
	OutputTokens      int `json:"output_tokens"`
}

type codexError struct {
	Message string `json:"message"`
}

func buildCodexPrompt(req *InferenceRequest) string {
	prompt := req.Prompt
	if systemPrompt := chainSystemPrompt(req); systemPrompt != "" {
		prompt = fmt.Sprintf("System instructions:\n%s\n\nUser request:\n%s", systemPrompt, req.Prompt)
	}
	return prompt
}

func resolveCodexModel(req *InferenceRequest) string {
	model := req.Model
	if req.ContextState != nil && req.ContextState.Model != "" {
		model = req.ContextState.Model
	}

	switch {
	case model == "", model == "codex":
		return defaultCodexModel
	case strings.HasPrefix(model, "codex/"):
		return strings.TrimPrefix(model, "codex/")
	default:
		return model
	}
}

func tomlString(value string) string {
	return strconv.Quote(value)
}

func tomlStringArray(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, tomlString(value))
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

func tomlInlineTable(values map[string]string) string {
	if len(values) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, tomlString(values[key])))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func buildCodexMCPOverrides(req *InferenceRequest, kernel KernelServices) ([]string, error) {
	if req.OpenClawURL == "" {
		return nil, nil
	}

	cogBin, err := os.Executable()
	if err != nil {
		cogBin = "cog"
	}

	env := map[string]string{
		"COG_ROOT":       kernel.WorkspaceRoot(),
		"OPENCLAW_URL":   req.OpenClawURL,
		"OPENCLAW_TOKEN": req.OpenClawToken,
		"SESSION_ID":     req.SessionID,
	}

	if otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otelEndpoint != "" {
		env["OTEL_EXPORTER_OTLP_ENDPOINT"] = otelEndpoint
	}

	if len(req.Tools) > 0 {
		mcpTools := kernel.ConvertOpenAIToolsToMCP(req.Tools)
		if len(mcpTools) > 0 {
			toolsJSON, err := json.Marshal(mcpTools)
			if err != nil {
				return nil, fmt.Errorf("serialize MCP tool registry: %w", err)
			}
			env["TOOL_REGISTRY"] = string(toolsJSON)
		}
	}

	return []string{
		`mcp_servers.cogos_bridge.command=` + tomlString(cogBin),
		`mcp_servers.cogos_bridge.args=` + tomlStringArray([]string{"mcp", "serve", "--bridge"}),
		`mcp_servers.cogos_bridge.env=` + tomlInlineTable(env),
	}, nil
}

func BuildCodexArgs(req *InferenceRequest, schemaPath string, kernel KernelServices) ([]string, error) {
	// All flags must go on `exec` before any subcommand like `resume`.
	// `codex exec --json --sandbox workspace-write resume <id> <prompt>`
	args := []string{"exec"}

	args = append(args, "--json", "--skip-git-repo-check")

	if req.SkipPermissions {
		args = append(args, "--dangerously-bypass-approvals-and-sandbox")
	} else {
		args = append(args, "--sandbox", "workspace-write")
	}

	if model := resolveCodexModel(req); model != "" {
		args = append(args, "--model", model)
	}

	if req.ClaudeSessionID == "" && req.WorkspaceRoot != "" {
		args = append(args, "--cd", req.WorkspaceRoot)
	}

	if schemaPath != "" {
		args = append(args, "--output-schema", schemaPath)
	}

	if kernel != nil {
		overrides, err := buildCodexMCPOverrides(req, kernel)
		if err != nil {
			return nil, err
		}
		for _, override := range overrides {
			args = append(args, "-c", override)
		}
	}

	// Subcommand and positional args come after all flags
	if req.ClaudeSessionID != "" {
		args = append(args, "resume", req.ClaudeSessionID)
	}

	args = append(args, buildCodexPrompt(req))
	return args, nil
}

func writeCodexSchemaFile(schema json.RawMessage) (string, error) {
	if len(schema) == 0 {
		return "", nil
	}

	tmpFile, err := os.CreateTemp("", "cog-codex-schema-*.json")
	if err != nil {
		return "", fmt.Errorf("create temp schema file: %w", err)
	}

	if _, err := tmpFile.Write(schema); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("write schema: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("close schema file: %w", err)
	}

	return tmpFile.Name(), nil
}

func parseCodexOutput(output []byte) codexOutputState {
	state := codexOutputState{}

	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 0, 1024*1024), 4*1024*1024)

	sawJSON := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var event codexThreadEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}
		sawJSON = true

		switch event.Type {
		case "thread.started":
			state.sessionID = event.ThreadID
		case "item.updated", "item.completed":
			if event.Item != nil && event.Item.Type == "agent_message" && event.Item.Text != "" {
				state.content = event.Item.Text
			}
		case "turn.completed":
			if event.Usage != nil {
				state.promptTokens = event.Usage.InputTokens
				state.completionTokens = event.Usage.OutputTokens
				state.cacheReadTokens = event.Usage.CachedInputTokens
			}
			state.finishReason = "stop"
		case "turn.failed":
			if event.Error != nil && event.Error.Message != "" {
				state.errorMessage = event.Error.Message
			}
		case "error":
			if event.Message != "" {
				state.errorMessage = event.Message
			} else if event.Error != nil && event.Error.Message != "" {
				state.errorMessage = event.Error.Message
			}
		}
	}

	if !sawJSON {
		state.content = strings.TrimSpace(string(output))
		if state.content != "" {
			state.finishReason = "stop"
		}
	}

	if state.finishReason == "" && state.content != "" {
		state.finishReason = "stop"
	}

	return state
}

func (h *Harness) runCodexInference(req *InferenceRequest, modelName string) (*InferenceResponse, error) {
	startTime := time.Now()

	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := tracer.Start(ctx, "inference.sync.codex",
		trace.WithAttributes(
			attribute.String("model", modelName),
			attribute.String("origin", req.Origin),
			attribute.Int("tool_count", len(req.Tools)),
		),
	)
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var entry *RequestEntry
	entry = h.registry.Register(req, cancel)
	defer func() {
		if entry.Status == "running" {
			h.registry.Complete(req.ID, "completed")
		}
	}()

	h.emitInferenceStart(req)
	h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

	schemaPath, err := writeCodexSchemaFile(req.Schema)
	if err != nil {
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}
	if schemaPath != "" {
		defer os.Remove(schemaPath)
	}

	args, err := BuildCodexArgs(req, schemaPath, h.kernel)
	if err != nil {
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}

	_, cliSpan := tracer.Start(ctx, "codex.cli.exec",
		trace.WithAttributes(
			attribute.Int("arg_count", len(args)),
		),
	)

	cmd := exec.CommandContext(ctx, CodexCommand, args...)
	cmd.Env = filteredEnviron()
	cmd.Dir = h.kernel.ResolveWorkDir(req.WorkspaceRoot)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	outputBytes, waitErr := cmd.Output()
	if waitErr != nil {
		cliSpan.SetAttributes(attribute.Int("exit_code", 1))
	} else {
		cliSpan.SetAttributes(attribute.Int("exit_code", 0))
	}
	cliSpan.End()

	parsed := parseCodexOutput(outputBytes)

	span.SetAttributes(
		attribute.Int("tokens.input", parsed.promptTokens),
		attribute.Int("tokens.output", parsed.completionTokens),
	)

	if ctx.Err() == context.Canceled {
		h.registry.Complete(req.ID, "cancelled")
		h.emitInferenceError(req.ID, "request cancelled")
		h.clearInferenceActiveSignal()
		return nil, fmt.Errorf("request cancelled")
	}

	response := &InferenceResponse{
		ID:               req.ID,
		Content:          parsed.content,
		PromptTokens:     parsed.promptTokens,
		CompletionTokens: parsed.completionTokens,
		CacheReadTokens:  parsed.cacheReadTokens,
		FinishReason:     parsed.finishReason,
		ContextMetrics:   BuildContextMetrics(req.ContextState),
		ClaudeSessionID:  parsed.sessionID,
	}

	if waitErr != nil {
		// Codex CLI may exit non-zero even when it produced valid output
		// (e.g. exit 2 after a successful turn). If we got content and a
		// finish reason from the JSONL stream, treat it as success.
		if parsed.content != "" && parsed.finishReason != "" {
			log.Printf("[codex] CLI exited with %v but produced valid output; treating as success", waitErr)
		} else {
			h.registry.Complete(req.ID, "failed")
			errMsg := parsed.errorMessage
			if errMsg == "" {
				errMsg = waitErr.Error()
				if stderrBuf.Len() > 0 {
					errMsg = fmt.Sprintf("%s: %s", errMsg, strings.TrimSpace(stderrBuf.String()))
				}
			}
			response.Error = waitErr
			response.ErrorMessage = errMsg
			response.ErrorType = ClassifyError(waitErr)
			h.emitInferenceError(req.ID, errMsg)
			h.clearInferenceActiveSignal()
			return response, waitErr
		}
	}

	if parsed.finishReason == "" {
		response.FinishReason = "stop"
	}

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

	return response, nil
}

func (h *Harness) runCodexInferenceStream(req *InferenceRequest, modelName string) (<-chan StreamChunkInference, error) {
	startTime := time.Now()
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

	ctx := req.Context
	ctx, span := tracer.Start(ctx, "inference.stream.codex",
		trace.WithAttributes(
			attribute.String("model", modelName),
			attribute.String("origin", req.Origin),
			attribute.Int("tool_count", len(req.Tools)),
		),
	)

	ctx, cancel := context.WithCancel(ctx)
	req.Context = ctx

	var entry *RequestEntry
	entry = h.registry.Register(req, cancel)

	h.emitInferenceStart(req)
	h.setInferenceActiveSignal(req.ID, modelName, req.Origin)

	schemaPath, err := writeCodexSchemaFile(req.Schema)
	if err != nil {
		cancel()
		timeoutCancel()
		span.End()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}

	args, err := BuildCodexArgs(req, schemaPath, h.kernel)
	if err != nil {
		if schemaPath != "" {
			os.Remove(schemaPath)
		}
		cancel()
		timeoutCancel()
		span.End()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}

	_, cliSpan := tracer.Start(ctx, "codex.cli.exec",
		trace.WithAttributes(
			attribute.Int("arg_count", len(args)),
		),
	)

	cmd := exec.CommandContext(ctx, CodexCommand, args...)
	cmd.Env = filteredEnviron()
	cmd.Dir = h.kernel.ResolveWorkDir(req.WorkspaceRoot)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		if schemaPath != "" {
			os.Remove(schemaPath)
		}
		cancel()
		timeoutCancel()
		span.End()
		cliSpan.End()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		if schemaPath != "" {
			os.Remove(schemaPath)
		}
		stdout.Close()
		cancel()
		timeoutCancel()
		span.End()
		cliSpan.End()
		h.registry.Complete(req.ID, "failed")
		h.emitInferenceError(req.ID, err.Error())
		h.clearInferenceActiveSignal()
		return nil, err
	}

	chunks := make(chan StreamChunkInference, 32)

	go func() {
		defer close(chunks)
		defer cancel()
		defer timeoutCancel()
		defer span.End()
		defer cliSpan.End()
		if schemaPath != "" {
			defer os.Remove(schemaPath)
		}

		safeSend := func(chunk StreamChunkInference) bool {
			select {
			case chunks <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}

		var fullContent strings.Builder
		var sessionID string
		var promptTokens, completionTokens, cacheReadTokens int
		var finishReason = "stop"
		var cliError string
		var lastAgentText string

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 1024*1024), 4*1024*1024)

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			var event codexThreadEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				continue
			}

			switch event.Type {
			case "thread.started":
				sessionID = event.ThreadID
				if !safeSend(StreamChunkInference{
					ID:        req.ID,
					EventType: "session_start",
					SessionInfo: &SessionInfo{
						SessionID:       sessionID,
						Model:           resolveCodexModel(req),
						ClaudeSessionID: sessionID,
					},
				}) {
					return
				}
			case "item.updated", "item.completed":
				if event.Item != nil && event.Item.Type == "agent_message" && event.Item.Text != "" {
					delta := event.Item.Text
					if strings.HasPrefix(event.Item.Text, lastAgentText) {
						delta = event.Item.Text[len(lastAgentText):]
					}
					lastAgentText = event.Item.Text
					fullContent.Reset()
					fullContent.WriteString(event.Item.Text)
					if delta != "" {
						if !safeSend(StreamChunkInference{
							ID:        req.ID,
							Content:   delta,
							EventType: "text",
						}) {
							return
						}
					}
				}
			case "turn.completed":
				if event.Usage != nil {
					promptTokens = event.Usage.InputTokens
					completionTokens = event.Usage.OutputTokens
					cacheReadTokens = event.Usage.CachedInputTokens
				}
				finishReason = "stop"
			case "turn.failed":
				if event.Error != nil && event.Error.Message != "" {
					cliError = event.Error.Message
				}
			case "error":
				if event.Message != "" {
					cliError = event.Message
				} else if event.Error != nil && event.Error.Message != "" {
					cliError = event.Error.Message
				}
			}
		}

		waitErr := cmd.Wait()
		if waitErr != nil {
			cliSpan.SetAttributes(attribute.Int("exit_code", 1))
		} else {
			cliSpan.SetAttributes(attribute.Int("exit_code", 0))
		}

		span.SetAttributes(
			attribute.Int("tokens.input", promptTokens),
			attribute.Int("tokens.output", completionTokens),
		)

		if scanner.Err() != nil {
			waitErr = scanner.Err()
		}

		// Codex CLI may exit non-zero even after a successful turn.
		// If we streamed content and got a finish reason, treat as success.
		hasValidOutput := fullContent.Len() > 0 && finishReason != ""
		if waitErr != nil && hasValidOutput {
			log.Printf("[codex] CLI exited with %v but produced valid streamed output; treating as success", waitErr)
			waitErr = nil
		}

		if entry.Status == "running" {
			if waitErr != nil {
				h.registry.Complete(req.ID, "failed")
			} else {
				h.registry.Complete(req.ID, "completed")
			}
		}

		if waitErr != nil {
			errMsg := cliError
			if errMsg == "" {
				errMsg = waitErr.Error()
				if stderrBuf.Len() > 0 {
					errMsg = fmt.Sprintf("%s: %s", errMsg, strings.TrimSpace(stderrBuf.String()))
				}
			}
			h.emitInferenceError(req.ID, errMsg)
			h.clearInferenceActiveSignal()
			safeSend(StreamChunkInference{
				ID:    req.ID,
				Done:  true,
				Error: fmt.Errorf("%s", errMsg),
			})
			return
		}

		resp := &InferenceResponse{
			ID:               req.ID,
			Content:          fullContent.String(),
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			FinishReason:     finishReason,
			ClaudeSessionID:  sessionID,
		}
		h.emitInferenceComplete(req, resp, startTime)
		h.clearInferenceActiveSignal()

		safeSend(StreamChunkInference{
			ID:           req.ID,
			Done:         true,
			FinishReason: finishReason,
			Usage: &UsageData{
				InputTokens:     promptTokens,
				OutputTokens:    completionTokens,
				CacheReadTokens: cacheReadTokens,
			},
			SessionInfo: &SessionInfo{
				SessionID:       sessionID,
				Model:           resolveCodexModel(req),
				ClaudeSessionID: sessionID,
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
	}()

	return chunks, nil
}
