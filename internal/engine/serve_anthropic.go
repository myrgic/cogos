package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type anthropicMessagesRequest struct {
	Model      string                  `json:"model"`
	System     json.RawMessage         `json:"system,omitempty"`
	Messages   []anthropicInputMessage `json:"messages"`
	MaxTokens  int                     `json:"max_tokens,omitempty"`
	Stream     bool                    `json:"stream,omitempty"`
	Metadata   anthropicRequestMeta    `json:"metadata,omitempty"`
	Tools      []anthropicTool         `json:"tools,omitempty"`
	ToolChoice *anthropicToolChoice    `json:"tool_choice,omitempty"`
}

// anthropicRequestMeta holds the optional metadata object from the Anthropic
// Messages API. The user_id field mirrors the OpenAI user field: a client-
// supplied string identifying the end-user, used here as the reqUser fallback
// for identity resolution when no X-Cogos-Session-Id header is present.
type anthropicRequestMeta struct {
	UserID string `json:"user_id,omitempty"`
}

type anthropicInputMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// anthropicContentBlock is defined in provider_anthropic.go (superset of fields).

type anthropicMessagesResponse struct {
	ID         string                  `json:"id"`
	Type       string                  `json:"type"`
	Role       string                  `json:"role"`
	Content    []anthropicContentBlock `json:"content"`
	Model      string                  `json:"model"`
	StopReason string                  `json:"stop_reason,omitempty"`
	Usage      anthropicMessagesUsage  `json:"usage,omitempty"`
}

type anthropicMessagesUsage struct {
	// No omitempty: the Anthropic SDK (and pi-ai on top of it) reads
	// usage.input_tokens / usage.output_tokens unconditionally from
	// message_start and message_delta. A zero must serialize as 0, not
	// vanish — an absent key crashes the client with
	// "Cannot read properties of undefined (reading 'input_tokens')".
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	// Cache accounting is optional on the wire (omitempty is correct here —
	// the SDK reads these defensively) but the kernel MUST forward them so an
	// external client can see prompt-cache hits instead of inferring 0%.
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
}

// anthropicInputContentBlock is a lenient decode target for inbound content
// blocks. It differs from anthropicContentBlock in that tool_result content
// may be either a plain string or an array of nested blocks, so Content is
// kept raw and resolved by anthropicToolResultText.
type anthropicInputContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`          // tool_use
	Name      string          `json:"name,omitempty"`        // tool_use
	Input     json.RawMessage `json:"input,omitempty"`       // tool_use
	ToolUseID string          `json:"tool_use_id,omitempty"` // tool_result
	Content   json.RawMessage `json:"content,omitempty"`     // tool_result: string or []blocks
}

// anthropicToolResultText flattens a tool_result content payload (string or
// array of blocks) into plain text for the OpenAI-shape role:"tool" message.
func anthropicToolResultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []anthropicInputContentBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var parts []string
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				parts = append(parts, b.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}

// expandAnthropicMessage translates one Anthropic input message into one or
// more OpenAI-shape messages:
//   - assistant tool_use blocks → assistant message with tool_calls
//     (Arguments = the tool_use input object serialized as a string).
//   - user tool_result blocks → one role:"tool" message per result, each
//     carrying tool_call_id, emitted BEFORE any remaining user text.
//   - plain text (string or text blocks) passes through unchanged.
func expandAnthropicMessage(message anthropicInputMessage) []oaiMessage {
	var blocks []anthropicInputContentBlock
	if err := json.Unmarshal(message.Content, &blocks); err != nil {
		// String (or unrecognized) content — pass through as before.
		return []oaiMessage{{Role: message.Role, Content: normalizeAnthropicContent(message.Content)}}
	}

	var out []oaiMessage
	var textParts []string
	var toolCalls []oaiToolCall
	for _, b := range blocks {
		switch b.Type {
		case "text":
			if b.Text != "" {
				textParts = append(textParts, b.Text)
			}
		case "tool_use":
			args := "{}"
			if len(b.Input) > 0 {
				args = string(b.Input)
			}
			toolCalls = append(toolCalls, oaiToolCall{
				ID:   b.ID,
				Type: "function",
				Function: oaiToolCallFunc{
					Name:      b.Name,
					Arguments: args,
				},
			})
		case "tool_result":
			out = append(out, oaiMessage{
				Role:       "tool",
				Content:    mustMarshalString(anthropicToolResultText(b.Content)),
				ToolCallID: b.ToolUseID,
			})
		}
	}

	text := strings.Join(textParts, "\n")
	if len(toolCalls) > 0 {
		raw, _ := json.Marshal(toolCalls)
		out = append(out, oaiMessage{
			Role:      message.Role,
			Content:   mustMarshalString(text),
			ToolCalls: raw,
		})
	} else if text != "" || len(out) == 0 {
		out = append(out, oaiMessage{Role: message.Role, Content: normalizeAnthropicContent(message.Content)})
	}
	return out
}

func anthropicToOpenAIRequest(req *anthropicMessagesRequest) *oaiChatRequest {
	if req == nil {
		return &oaiChatRequest{}
	}

	messages := make([]oaiMessage, 0, len(req.Messages)+1)
	if system := normalizeAnthropicContent(req.System); len(system) > 0 {
		messages = append(messages, oaiMessage{Role: "system", Content: system})
	}
	for _, message := range req.Messages {
		messages = append(messages, expandAnthropicMessage(message)...)
	}

	out := &oaiChatRequest{
		Model:     req.Model,
		Messages:  messages,
		Stream:    req.Stream,
		MaxTokens: req.MaxTokens,
	}

	// Translate tool definitions to the OpenAI function envelope so the
	// normalized request is complete on both surfaces.
	for _, t := range req.Tools {
		out.Tools = append(out.Tools, oaiToolDefinition{
			Type: "function",
			Function: oaiToolFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.InputSchema,
			},
		})
	}
	if tc := anthropicToolChoiceString(req.ToolChoice); tc != "" {
		out.ToolChoice, _ = json.Marshal(tc)
	}

	return out
}

// anthropicToolChoiceString maps the Anthropic tool_choice object to the
// internal CompletionRequest.ToolChoice string convention (same values the
// OpenAI-shape handler produces): "auto", "none", "required" (from "any"),
// or a specific tool name (from {type:"tool",name}).
func anthropicToolChoiceString(tc *anthropicToolChoice) string {
	if tc == nil {
		return ""
	}
	switch tc.Type {
	case "auto":
		return "auto"
	case "none":
		return "none"
	case "any":
		return "required"
	case "tool":
		return tc.Name
	}
	return ""
}

func normalizeAnthropicContent(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return mustMarshalString("")
	}

	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return mustMarshalString(s)
	}

	var blocks []anthropicContentBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		return raw
	}

	return raw
}

// toolInputJSON parses a stringified tool-call Arguments payload into the
// JSON object Anthropic's tool_use.input field requires. Invalid or empty
// arguments render as {} rather than corrupting the response body.
func toolInputJSON(args string) json.RawMessage {
	trimmed := strings.TrimSpace(args)
	if trimmed == "" {
		return json.RawMessage("{}")
	}
	if json.Valid([]byte(trimmed)) && strings.HasPrefix(trimmed, "{") {
		return json.RawMessage(trimmed)
	}
	return json.RawMessage("{}")
}

func anthropicStopReason(stopReason string) string {
	switch stopReason {
	case "", "stop", "end_turn":
		return "end_turn"
	case "max_tokens":
		return "max_tokens"
	case "tool_use":
		return "tool_use"
	default:
		return stopReason
	}
}

func (s *Server) handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var anthropicReq anthropicMessagesRequest
	if err := json.Unmarshal(body, &anthropicReq); err != nil {
		http.Error(w, "parse body: "+err.Error(), http.StatusBadRequest)
		return
	}

	oaiReq := anthropicToOpenAIRequest(&anthropicReq)
	block := NormalizeAnthropicRequest(body, "http")
	block.SessionID = s.process.SessionID()

	// G1: resolve inbound client session → bound identity for attribution.
	// The Anthropic metadata.user_id mirrors OpenAI's user field and serves
	// as the reqUser fallback when no X-Cogos-Session-Id header is present.
	bound := s.resolveBoundIdentity(r, anthropicReq.Metadata.UserID)
	if bound.Bound {
		block.TargetIdentity = bound.Subject
	} else if s.nucleus != nil {
		block.TargetIdentity = s.nucleus.Name
	}

	block.WorkspaceID = filepath.Base(s.cfg.WorkspaceRoot)
	s.process.RecordBlock(block)

	clientMsgs := block.Messages

	// Resolve any pending client-ownership tool calls whose results are
	// arriving on this turn (tool_result blocks translated to role=tool
	// messages by anthropicToOpenAIRequest). Mirrors handleChat.
	for _, msg := range clientMsgs {
		if msg.Role == "tool" && msg.ToolCallID != "" {
			s.process.resolvePendingToolCall(msg.ToolCallID, msg.Content)
		}
	}

	query := ""
	for i := len(clientMsgs) - 1; i >= 0; i-- {
		if clientMsgs[i].Role == "user" {
			query = clientMsgs[i].Content
			break
		}
	}

	s.process.Send(NewGateEventFromBlock(block, "user.message", query))

	if s.router == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"type":    "not_implemented",
				"message": "no inference router configured; run with a providers.yaml",
			},
		})
		return
	}

	creq := &CompletionRequest{
		MaxTokens:     oaiReq.MaxTokens,
		InteractionID: block.ID,
		Metadata: RequestMetadata{
			RequestID:    uuid.New().String(),
			ProcessState: "active",
			Priority:     PriorityNormal,
			Source:       "http-anthropic",
		},
	}

	// Convert Anthropic tool definitions to internal ToolDefinition and
	// partition by ownership — same three pools as handleChat (serve.go):
	// kernel-classified names, MCP-internal tools, and everything else
	// forwarded back to the client as tool_use blocks.
	//
	// Ledger L06: identical refusal to the OpenAI twin. This is the surface
	// the row names — /v1/messages routed client-named cog_* straight into
	// splitToolCallsByOwnership → MCPServer.CallTool.
	if len(anthropicReq.Tools) > 0 {
		defs := make([]ToolDefinition, 0, len(anthropicReq.Tools))
		for _, t := range anthropicReq.Tools {
			defs = append(defs, ToolDefinition{
				Name:        t.Name,
				Description: t.Description,
				InputSchema: t.InputSchema,
			})
		}
		tools, external, rejected := admitClientSuppliedTools(defs, s.mcpServer)
		creq.Tools = tools
		creq.ExternalTools = append(creq.ExternalTools, external...)
		creq.RefusedToolNames = rejected
		if len(rejected) > 0 {
			slog.Warn("anthropic: refused client-supplied kernel-owned tool definitions",
				"request_id", creq.Metadata.RequestID,
				"rejected", rejected,
			)
		}
	}
	creq.ToolChoice = anthropicToolChoiceString(anthropicReq.ToolChoice)

	// Per-provider deny policy: enforced on THIS endpoint's own model-routing
	// decision below, not by swapping in the OpenAI-compat handler's shared
	// resolver — cog-review round 1 flagged that ResolveModelRequest resolves
	// "claude" to claude-oauth and "local" to an explicit live-provider probe,
	// both of which silently differ from this switch's existing behavior
	// ("claude" -> claude-code; "local" -> no-op/default routing) and would
	// have redirected previously-working /v1/messages traffic to a different
	// provider/auth path with no test coverage catching it. The deny check
	// itself must still run against whatever THIS switch actually resolves,
	// both before (raw composite string) and after (resolved provider/model)
	// the switch runs, so the incident payload this PR exists to block is
	// blocked here exactly as it is on /v1/chat/completions — without
	// changing any route this endpoint already served correctly.
	//
	// Pre-resolution: reject an explicit "<provider>/<model>" composite id
	// whose provider prefix is denied, before the switch below runs.
	if reason, denied := deniedModelProvider(oaiReq.Model); denied {
		provider := oaiReq.Model
		if i := strings.Index(oaiReq.Model, "/"); i > 0 {
			provider = oaiReq.Model[:i]
		}
		slog.Warn("anthropic: rejected policy-denied model at kernel boundary",
			"request_id", creq.Metadata.RequestID,
			"model", oaiReq.Model,
			"provider", provider,
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "policy_denied",
				"message": fmt.Sprintf("model %q is denied on provider %q: %s", oaiReq.Model, provider, reason),
			},
		})
		return
	}

	switch oaiReq.Model {
	case "", "local":
	case "claude":
		creq.Metadata.PreferProvider = "claude-code"
	case "codex":
		creq.Metadata.PreferProvider = "codex"
	case "ollama":
		// "ollama" is kept as a convenience spelling that routes to the live
		// local backend (lmstudio-darkstar) after PR #417 decommissioned the
		// Ollama provider. On a stock install no "ollama" provider exists, so
		// the old value routed to nothing; installs that still declare an
		// "ollama" provider select it by its provider name via the default arm.
		creq.Metadata.PreferProvider = "lmstudio-darkstar"
	default:
		creq.ModelOverride = oaiReq.Model
	}

	// Post-resolution: reject the (provider, model) pair the switch above
	// just resolved, even when the raw client string carried no explicit
	// provider prefix (e.g. the "default" arm's bare model id passing through
	// as ModelOverride with no PreferProvider set, followed by the request's
	// default routing landing on a denied provider).
	if reason, denied := DeniedByPolicy(creq.Metadata.PreferProvider, creq.ModelOverride); denied {
		slog.Warn("anthropic: rejected policy-denied model after resolution at kernel boundary",
			"request_id", creq.Metadata.RequestID,
			"model", oaiReq.Model,
			"resolved_provider", creq.Metadata.PreferProvider,
			"resolved_model", creq.ModelOverride,
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "policy_denied",
				"message": fmt.Sprintf("model %q is denied on provider %q: %s", oaiReq.Model, creq.Metadata.PreferProvider, reason),
			},
		})
		return
	}

	// Allow per-request budget override via the X-Cogos-Context-Budget header.
	// A value of 0 (absent or unparseable) defers to the kernel's configured
	// default_budget (or the package-level DefaultBudget fallback).
	contextBudget := 0
	if hv := r.Header.Get("X-Cogos-Context-Budget"); hv != "" {
		if v, err := strconv.Atoi(hv); err == nil && v > 0 {
			contextBudget = v
		}
	}

	// G1: embodiment gating — mirrors handleChat exactly.
	//
	// When IdentityNakedDefault is false (default), behavior is exactly
	// today's: run AssembleContext + nucleus card on every request. The ONLY
	// observable difference from before G1 is that bound sessions carry their
	// own subject in block.TargetIdentity (set above).
	//
	// When IdentityNakedDefault is true:
	//   • nucleus-bound request (bound.Subject == nucleus.Name): full embodiment.
	//   • foreign-bound or unbound: clean transport — skip AssembleContext,
	//     forward client messages verbatim (including role:system), no nucleus card.
	nucleusName := ""
	if s.nucleus != nil {
		nucleusName = s.nucleus.Name
	}
	useFullEmbodiment := !s.cfg.IdentityNakedDefault ||
		(bound.Bound && bound.Subject == nucleusName)

	// G3 Part A: spawn embodiment — mirrors handleChat exactly.
	// flag OFF → WorkDir empty (today's behavior).
	// flag ON + bound    → resolve cog:// WorkspaceRoot to fs path.
	// flag ON + unbound  → neutral temp dir so no CLAUDE.md loads.
	if s.cfg.IdentityNakedDefault {
		if bound.Bound && bound.WorkspaceRoot != "" {
			if fsPath := resolveWorkspaceRootPath(s.cfg.WorkspaceRoot, bound.WorkspaceRoot); fsPath != "" {
				creq.WorkDir = fsPath
			}
		}
		// Note: unbound anon-temp is handled on the chat path (serve.go);
		// the anthropic path follows the same policy but defers temp creation
		// to avoid resource leak on the simpler BrowserOS / Zed flow.
	}

	// G3 Part B: memory scope — mirrors handleChat exactly.
	var assembleScopeOpts []AssembleOption
	if s.cfg.IdentityNakedDefault && bound.Bound && bound.MemoryNamespace != "" {
		assembleScopeOpts = append(assembleScopeOpts, WithMemoryScope(bound.MemoryNamespace))
	}

	// Foveation / light-cone key: stable per-conversation, never the per-request
	// UUID and never Process.SessionID(). Mirrors handleChat. The Anthropic
	// metadata.user_id is the "user" field equivalent. See foveation_session_key.go.
	foveationKey := foveationSessionKey(r.Header.Get(foveationKeyHeader), anthropicReq.Metadata.UserID, clientMsgs)

	if useFullEmbodiment {
		assembleOpts := []AssembleOption{
			WithContext(r.Context()),
			WithConversationID(foveationKey),
			WithManifestMode(true),
		}
		assembleOpts = append(assembleOpts, assembleScopeOpts...)
		if pkg, err := s.process.AssembleContext(query, clientMsgs, contextBudget, assembleOpts...); err != nil {
			slog.Warn("anthropic: context assembly failed", "err", err)
			// Fallback: preserve role=system messages as the provider SystemPrompt
			// so an explicit user/BrowserOS prompt isn't silently dropped. The
			// Anthropic upstream API rejects role=system inside messages, so we
			// MUST extract them on this path.
			var clientSysParts []string
			var nonSysMsgs []ProviderMessage
			for _, m := range clientMsgs {
				if m.Role == "system" {
					if strings.TrimSpace(m.Content) != "" {
						clientSysParts = append(clientSysParts, m.Content)
					}
					continue
				}
				nonSysMsgs = append(nonSysMsgs, m)
			}
			creq.Messages = nonSysMsgs
			creq.SystemPrompt = mergeSystemPrompts(s.nucleusCard(), clientSysParts)
		} else {
			systemPrompt, managedMsgs := pkg.FormatForProvider()
			creq.SystemPrompt = systemPrompt
			creq.Messages = managedMsgs
		}
	} else {
		// Clean transport path (IdentityNakedDefault=true, foreign or unbound).
		// Forward client messages verbatim; no nucleus card; AssembleContext skipped.
		var clientSysParts []string
		var nonSysMsgs []ProviderMessage
		for _, m := range clientMsgs {
			if m.Role == "system" {
				if strings.TrimSpace(m.Content) != "" {
					clientSysParts = append(clientSysParts, m.Content)
				}
				continue
			}
			nonSysMsgs = append(nonSysMsgs, m)
		}
		creq.Messages = nonSysMsgs
		creq.SystemPrompt = mergeSystemPrompts("", clientSysParts)
	}

	provider, _, err := s.router.Route(r.Context(), creq)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{"type": "no_provider", "message": err.Error()},
		})
		return
	}

	respID := "msg_" + uuid.NewString()
	model := provider.Name()
	if anthropicReq.Model != "" && anthropicReq.Model != "local" {
		model = anthropicReq.Model
	}

	// Prepare the turn record — fills in during complete/stream, persisted below.
	turn := &TurnRecord{
		TurnID:    uuid.NewString(),
		TurnIndex: NextTurnIndex(s.cfg.WorkspaceRoot, block.SessionID),
		SessionID: block.SessionID,
		Timestamp: time.Now().UTC(),
		Prompt:    query,
		Provider:  provider.Name(),
		Model:     model,
		BlockID:   block.ID,
	}

	turnStart := time.Now()
	if anthropicReq.Stream {
		s.streamAnthropicMessages(w, r.Context(), creq, provider, respID, model, turn)
	} else {
		s.completeAnthropicMessages(w, r.Context(), creq, provider, respID, model, turn)
	}

	turn.DurationMs = time.Since(turnStart).Milliseconds()
	if err := s.process.RecordTurn(turn); err != nil {
		slog.Warn("anthropic: RecordTurn failed", "err", err, "session", turn.SessionID)
	}
}

func (s *Server) completeAnthropicMessages(w http.ResponseWriter, ctx context.Context, req *CompletionRequest,
	provider Provider, respID, model string, turn *TurnRecord) {

	// Cancel-safe (#432): same rationale as completeChat in serve.go — the
	// Anthropic-compat non-streaming surface hits the same provider pool and
	// is subject to the same zombie-generation risk on abandoned requests.
	resp, err := CompleteCancelSafeIfSupported(ctx, provider, req)
	if err != nil {
		recordAbandonedInference("anthropic-complete", req.Metadata.RequestID, err)
		slog.Warn("anthropic: complete error", "err", err)
		if turn != nil {
			turn.Status = "error"
			turn.Error = err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{"type": "inference_error", "message": err.Error()},
		})
		return
	}

	// Server-side execution of MCP-internal tools — mirrors completeChat's
	// #94 loop. Kernel-owned tool_use is executed in-process and never
	// rendered to the client; only external tool calls surface below as
	// Anthropic tool_use content blocks.
	if s.mcpServer != nil {
		const maxInternalHops = 8
		for hop := 0; hop < maxInternalHops; hop++ {
			internal, external := splitToolCallsByOwnershipFor(resp.ToolCalls, s.mcpServer, req)
			if len(internal) == 0 {
				break
			}
			req.Messages = appendToolHopMessages(req.Messages, resp, internal)
			for _, tc := range internal {
				s.executeInternalToolCall(ctx, provider.Name(), tc)
				resultText, isErr, callErr := s.mcpServer.CallTool(ctx, tc.Name, []byte(tc.Arguments))
				if callErr != nil {
					slog.Warn("anthropic: internal MCP tool call failed",
						"tool", tc.Name, "err", callErr,
						"request_id", req.Metadata.RequestID,
					)
					resultText = "tool error: " + callErr.Error()
					isErr = true
				}
				s.process.resolvePendingToolCall(tc.ID, resultText)
				req.Messages = append(req.Messages, ProviderMessage{
					Role:       "tool",
					Content:    resultText,
					Name:       tc.Name,
					ToolCallID: tc.ID,
				})
				if turn != nil {
					rec := ToolCallRecord{
						ID:        tc.ID,
						Name:      tc.Name,
						Arguments: tc.Arguments,
						Result:    truncateForTurn(resultText),
					}
					if isErr {
						rec.Rejected = true
						rec.RejectReason = "tool reported error"
					}
					turn.ToolCalls = append(turn.ToolCalls, rec)
				}
			}
			next, nerr := CompleteCancelSafeIfSupported(ctx, provider, req)
			if nerr != nil {
				recordAbandonedInference("anthropic-complete-post-tool", req.Metadata.RequestID, nerr)
				slog.Warn("anthropic: complete after internal tool exec failed", "err", nerr)
				if turn != nil {
					turn.Status = "error"
					turn.Error = nerr.Error()
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]string{"type": "inference_error", "message": nerr.Error()},
				})
				return
			}
			if len(external) > 0 {
				next.ToolCalls = append(next.ToolCalls, external...)
			}
			resp = next
		}
		// Cap exhausted with kernel-owned calls still pending: hard error,
		// mirroring the streaming path. Falling through would hand
		// unexecuted cog_* tool_use to an external client (review round 1).
		if internal, _ := splitToolCallsByOwnershipFor(resp.ToolCalls, s.mcpServer, req); len(internal) > 0 {
			slog.Warn("anthropic: internal tool hop cap exceeded", "cap", maxInternalHops, "pending", len(internal))
			if turn != nil {
				turn.Status = "error"
				turn.Error = "internal tool hop cap exceeded"
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]string{"type": "inference_error", "message": "internal tool hop cap exceeded"},
			})
			return
		}
	}

	if turn != nil {
		turn.Response = resp.Content
		turn.Usage = TurnUsage{
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
			TotalTokens:  resp.Usage.InputTokens + resp.Usage.OutputTokens,
		}
		if len(resp.ToolCalls) > 0 {
			for _, tc := range resp.ToolCalls {
				turn.ToolCalls = append(turn.ToolCalls, ToolCallRecord{
					ID:        tc.ID,
					Name:      tc.Name,
					Arguments: tc.Arguments,
				})
			}
		}
	}

	// Render content blocks: text (when present) followed by one tool_use
	// block per CLIENT-OWNED tool call. The renderer is the aperture: it
	// filters by ownership itself rather than trusting the loop above to
	// have drained every kernel-owned call, so the "kernel tools never reach
	// the client" invariant holds here regardless of how we arrived.
	// tool_use input must be a JSON object, so stringified Arguments are
	// parsed back.
	clientCalls := resp.ToolCalls
	if s.mcpServer != nil {
		_, clientCalls = splitToolCallsByOwnership(resp.ToolCalls, s.mcpServer)
	}
	content := make([]anthropicContentBlock, 0, 1+len(clientCalls))
	if resp.Content != "" || len(clientCalls) == 0 {
		content = append(content, anthropicContentBlock{Type: "text", Text: resp.Content})
	}
	for _, tc := range clientCalls {
		content = append(content, anthropicContentBlock{
			Type:  "tool_use",
			ID:    nonEmptyID(tc.ID),
			Name:  tc.Name,
			Input: toolInputJSON(tc.Arguments),
		})
	}
	stopReason := anthropicStopReason(resp.StopReason)
	if len(resp.ToolCalls) > 0 {
		stopReason = "tool_use"
	}

	response := anthropicMessagesResponse{
		ID:         respID,
		Type:       "message",
		Role:       "assistant",
		Content:    content,
		Model:      model,
		StopReason: stopReason,
		Usage: anthropicMessagesUsage{
			InputTokens:              resp.Usage.InputTokens,
			OutputTokens:             resp.Usage.OutputTokens,
			CacheReadInputTokens:     resp.Usage.CacheReadTokens,
			CacheCreationInputTokens: resp.Usage.CacheWriteTokens,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) streamAnthropicMessages(w http.ResponseWriter, ctx context.Context, req *CompletionRequest,
	provider Provider, respID, model string, turn *TurnRecord) {

	chunks, err := provider.Stream(ctx, req)
	if err != nil {
		slog.Warn("anthropic: stream error", "err", err)
		if turn != nil {
			turn.Status = "error"
			turn.Error = err.Error()
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, canFlush := w.(http.Flusher)
	bw := bufio.NewWriter(w)
	flush := func() {
		_ = bw.Flush()
		if canFlush {
			flusher.Flush()
		}
	}
	writeEvent := func(event string, data any) {
		payload, _ := json.Marshal(data)
		_, _ = fmt.Fprintf(bw, "event: %s\n", event)
		_, _ = fmt.Fprintf(bw, "data: %s\n\n", payload)
		flush()
	}

	writeEvent("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            respID,
			"type":          "message",
			"role":          "assistant",
			"content":       []anthropicContentBlock{},
			"model":         model,
			"stop_reason":   nil,
			"stop_sequence": nil,
			// Real Anthropic sends usage on message_start (input known,
			// output 0). SDK clients dereference it; omitting it is a crash,
			// not a degradation. Input is unknown until the provider reports
			// it, so 0 here and the true figure on message_delta.
			"usage": anthropicMessagesUsage{},
		},
	})

	writeErrorAndStop := func(index int) {
		writeEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
		writeEvent("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": "end_turn"},
			"usage": anthropicMessagesUsage{},
		})
		writeEvent("message_stop", map[string]any{"type": "message_stop"})
	}

	// Hop loop — mirrors streamChat's #94/#95 pattern: drain each upstream
	// stream into a buffer, execute kernel-internal tool calls in-process,
	// and re-issue Stream(). Only the final hop's text + any client-owned
	// (external) tool calls are rendered to the client.
	const maxStreamHops = 8
	var carriedExternal []ToolCall
	var respBuf strings.Builder
	usage := anthropicMessagesUsage{}

	writeEvent("content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": 0,
		"content_block": map[string]any{
			"type": "text",
			"text": "",
		},
	})

	for hop := 0; hop < maxStreamHops; hop++ {
		hopBuf, hopErr := drainStreamHop(chunks)
		if hopErr != nil {
			slog.Warn("anthropic: stream chunk error", "err", hopErr)
			if turn != nil {
				turn.Status = "error"
				turn.Error = hopErr.Error()
				turn.Response = respBuf.String()
			}
			writeErrorAndStop(0)
			return
		}
		if hopBuf.usage != nil {
			usage.InputTokens = hopBuf.usage.InputTokens
			usage.OutputTokens = hopBuf.usage.OutputTokens
			usage.CacheReadInputTokens = hopBuf.usage.CacheReadTokens
			usage.CacheCreationInputTokens = hopBuf.usage.CacheWriteTokens
		}

		toolCalls := hopBuf.assembledToolCalls()
		internal, external := splitToolCallsByOwnershipFor(toolCalls, s.mcpServer, req)
		carriedExternal = append(carriedExternal, external...)

		if len(internal) == 0 {
			// Terminal hop: replay text deltas, then render external
			// tool_use blocks at fresh indices.
			for _, d := range hopBuf.deltas {
				respBuf.WriteString(d)
				writeEvent("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": 0,
					"delta": map[string]any{
						"type": "text_delta",
						"text": d,
					},
				})
			}
			writeEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})

			for i, tc := range carriedExternal {
				idx := i + 1
				writeEvent("content_block_start", map[string]any{
					"type":  "content_block_start",
					"index": idx,
					"content_block": map[string]any{
						"type":  "tool_use",
						"id":    nonEmptyID(tc.ID),
						"name":  tc.Name,
						"input": map[string]any{},
					},
				})
				writeEvent("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": idx,
					"delta": map[string]any{
						"type":         "input_json_delta",
						"partial_json": string(toolInputJSON(tc.Arguments)),
					},
				})
				writeEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
				if turn != nil {
					turn.ToolCalls = append(turn.ToolCalls, ToolCallRecord{
						ID:        tc.ID,
						Name:      tc.Name,
						Arguments: tc.Arguments,
					})
				}
			}

			stopReason := anthropicStopReason(hopBuf.stopReason)
			if len(carriedExternal) > 0 {
				stopReason = "tool_use"
			}
			if turn != nil {
				turn.Response = respBuf.String()
				turn.Usage = TurnUsage{
					InputTokens:  usage.InputTokens,
					OutputTokens: usage.OutputTokens,
					TotalTokens:  usage.InputTokens + usage.OutputTokens,
				}
			}
			writeEvent("message_delta", map[string]any{
				"type": "message_delta",
				"delta": map[string]any{
					"stop_reason": stopReason,
				},
				"usage": usage,
			})
			writeEvent("message_stop", map[string]any{"type": "message_stop"})
			return
		}

		// Internal tool calls: execute in-process, extend the transcript,
		// and re-open the upstream stream. Any text this hop produced is
		// still streamed so the client doesn't lose interleaved reasoning.
		for _, d := range hopBuf.deltas {
			respBuf.WriteString(d)
			writeEvent("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": 0,
				"delta": map[string]any{
					"type": "text_delta",
					"text": d,
				},
			})
		}

		req.Messages = appendToolHopMessages(req.Messages, &CompletionResponse{
			Content:    hopBuf.content.String(),
			ToolCalls:  toolCalls,
			StopReason: hopBuf.stopReason,
		}, internal)
		for _, tc := range internal {
			s.executeInternalToolCall(ctx, provider.Name(), tc)
			resultText, isErr, callErr := s.mcpServer.CallTool(ctx, tc.Name, []byte(tc.Arguments))
			if callErr != nil {
				slog.Warn("anthropic: internal MCP tool call failed (stream)",
					"tool", tc.Name, "err", callErr,
					"request_id", req.Metadata.RequestID,
				)
				resultText = "tool error: " + callErr.Error()
				isErr = true
			}
			s.process.resolvePendingToolCall(tc.ID, resultText)
			req.Messages = append(req.Messages, ProviderMessage{
				Role:       "tool",
				Content:    resultText,
				Name:       tc.Name,
				ToolCallID: tc.ID,
			})
			if turn != nil {
				rec := ToolCallRecord{
					ID:        tc.ID,
					Name:      tc.Name,
					Arguments: tc.Arguments,
					Result:    truncateForTurn(resultText),
				}
				if isErr {
					rec.Rejected = true
					rec.RejectReason = "tool reported error"
				}
				turn.ToolCalls = append(turn.ToolCalls, rec)
			}
		}

		if hop+1 >= maxStreamHops {
			slog.Warn("anthropic: stream tool-loop exceeded hop cap", "hops", maxStreamHops)
			if turn != nil {
				turn.Status = "error"
				turn.Error = fmt.Sprintf("stream tool-loop exceeded %d hops", maxStreamHops)
				turn.Response = respBuf.String()
			}
			writeErrorAndStop(0)
			return
		}

		nextStream, nerr := provider.Stream(ctx, req)
		if nerr != nil {
			slog.Warn("anthropic: stream after internal tool exec failed", "err", nerr)
			if turn != nil {
				turn.Status = "error"
				turn.Error = nerr.Error()
				turn.Response = respBuf.String()
			}
			writeErrorAndStop(0)
			return
		}
		chunks = nextStream
	}
	// In case the loop exited without a terminal hop, preserve what we captured.
	if turn != nil && turn.Response == "" {
		turn.Response = respBuf.String()
	}
}
