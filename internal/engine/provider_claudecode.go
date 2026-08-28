// provider_claudecode.go — ClaudeCodeProvider
//
// Implements Provider by spawning `claude -p` subprocesses. Unlike the
// Anthropic and Ollama providers, ClaudeCodeProvider is agentic: the
// subprocess owns its own tool loop (filesystem, MCP, etc.).
//
// Authentication: uses the host's Claude Max subscription via OAuth
// (keychain). Does NOT use --bare mode, which would require API keys.
//
// Process lifecycle:
//   - Foreground: tied to HTTP request context. Cancelled on disconnect.
//   - Background: outlives the request. Reports back via callback.
//   - Agent: runs in Docker container. Trust-bounded, resource-limited.
//
// Output: parsed from `--output-format stream-json --include-partial-messages`
// which emits NDJSON with Anthropic streaming events.
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// claudeCodeKillGrace is the time we wait for the claude subprocess to exit
// after sending SIGTERM before exec.Cmd escalates to SIGKILL automatically.
// 10 s is enough for a graceful shutdown but short enough to unblock the kernel
// well within the 5-minute autonomic-cycle timeout.
const claudeCodeKillGrace = 10 * time.Second

// ClaudeCodeProvider implements Provider by spawning claude CLI processes.
type ClaudeCodeProvider struct {
	name      string
	model     string // "sonnet", "opus", "haiku", or full model name
	effort    string // "low", "medium", "high", "max"
	timeout   time.Duration
	cliBinary string // path to claude binary (default: "claude")

	// MCP configuration file path for backend processes.
	mcpConfig string

	// Tools to allow/disallow in backend processes.
	allowedTools    []string
	disallowedTools []string

	// Process manager (shared across all providers/callers).
	procMgr *ProcessManager

	// Availability cache (mirrors ClaudeOAuthProvider). The auth-status probe
	// spawns a subprocess, so the result is cached for availCacheTTL and the
	// mutex is held across the probe to collapse concurrent callers.
	availMu     sync.Mutex
	availAt     time.Time
	availResult bool
}

// claudeCodeAuthProbe runs `<binary> auth status --json` and returns its
// combined output and error. Package-level var so tests can stub the
// subprocess. The command's working directory is the user's home so a
// poisoned repo-local config can't skew the result.
var claudeCodeAuthProbe = func(ctx context.Context, binary string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, binary, "auth", "status", "--json")
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cmd.Dir = home
	}
	return cmd.Output()
}

// NewClaudeCodeProvider creates a ClaudeCodeProvider from a ProviderConfig.
func NewClaudeCodeProvider(name string, cfg ProviderConfig, procMgr *ProcessManager) *ClaudeCodeProvider {
	model := cfg.Model
	if model == "" {
		model = "sonnet"
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout == 0 {
		timeout = 300 * time.Second // 5 min default — agentic tasks take longer
	}
	binary := "claude"
	if cfg.Endpoint != "" {
		binary = cfg.Endpoint // abuse Endpoint field for binary path
	}

	var effort string
	var mcpConfig string
	var allowed, disallowed []string
	if cfg.Options != nil {
		if e, ok := cfg.Options["effort"].(string); ok {
			effort = e
		}
		if m, ok := cfg.Options["mcp_config"].(string); ok {
			mcpConfig = m
		}
		if a, ok := cfg.Options["allowed_tools"].(string); ok {
			allowed = strings.Split(a, ",")
		}
		if d, ok := cfg.Options["disallowed_tools"].(string); ok {
			disallowed = strings.Split(d, ",")
		}
	}

	return &ClaudeCodeProvider{
		name:            name,
		model:           model,
		effort:          effort,
		timeout:         timeout,
		cliBinary:       binary,
		mcpConfig:       mcpConfig,
		allowedTools:    allowed,
		disallowedTools: disallowed,
		procMgr:         procMgr,
	}
}

// Name returns the provider identifier.
func (p *ClaudeCodeProvider) Name() string { return p.name }

// Model returns the configured model identifier (e.g. "sonnet", "haiku").
func (p *ClaudeCodeProvider) Model() string { return p.model }

// Available reports whether the claude binary exists AND the CLI is
// authenticated. The result is cached for availCacheTTL so the router's
// periodic availability ticker (and the per-request /v1/providers handler)
// don't spawn a subprocess on every call. The mutex is held across the probe
// so concurrent callers collapse into a single subprocess.
func (p *ClaudeCodeProvider) Available(ctx context.Context) bool {
	p.availMu.Lock()
	defer p.availMu.Unlock()
	if !p.availAt.IsZero() && time.Since(p.availAt) < availCacheTTL {
		return p.availResult
	}
	fresh := p.probeAvailable(ctx)
	// Don't cache a false result caused by caller-initiated cancellation
	// (client disconnect); a deadline is a genuine probe timeout and is cached.
	if !fresh && ctx.Err() == context.Canceled {
		return p.availResult
	}
	p.availResult = fresh
	p.availAt = time.Now()
	return p.availResult
}

// probeAvailable performs the real availability check backing Available():
// binary on PATH, then `claude auth status --json` reporting loggedIn=true.
// Call via Available() for TTL caching.
func (p *ClaudeCodeProvider) probeAvailable(ctx context.Context) bool {
	path, err := exec.LookPath(p.cliBinary)
	if err != nil || path == "" {
		return false
	}

	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	out, err := claudeCodeAuthProbe(probeCtx, p.cliBinary)
	if err != nil {
		slog.Debug("claude-code: Available: auth status probe failed", "err", err)
		return false
	}
	var status struct {
		LoggedIn bool `json:"loggedIn"`
	}
	if err := json.Unmarshal(out, &status); err != nil {
		slog.Debug("claude-code: Available: auth status parse failed", "err", err)
		return false
	}
	return status.LoggedIn
}

// Capabilities returns what this provider supports.
//
// CapVision (#640): the claude-code CLI drives the same claude-* model
// family as claude-oauth/AnthropicProvider (sonnet/opus/haiku, all
// genuinely multimodal), so it declares CapVision alongside them — omitting
// it here would make /v1/models advertise vision for the direct-API
// frontier providers but not for the CLI-backed one serving the identical
// models, which is exactly the "provider name says one thing, upstream says
// another" gap #640 exists to close.
func (p *ClaudeCodeProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities: []Capability{
			CapStreaming,
			CapToolUse,
			CapVision,
			CapLongContext,
			CapCaching,
		},
		MaxContextTokens:   1_000_000, // Opus 4.6 1M
		MaxOutputTokens:    64_000,
		ModelsAvailable:    []string{"sonnet", "opus", "haiku"},
		IsLocal:            true, // runs as local process
		AgenticHarness:     true,
		CostPerInputToken:  0, // Max sub, no per-token cost
		CostPerOutputToken: 0,
	}
}

// Ping checks the binary is available and returns the startup overhead.
func (p *ClaudeCodeProvider) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, p.cliBinary, "--version")
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("claude binary not available: %w", err)
	}
	return time.Since(start), nil
}

// Complete sends a prompt and waits for the full response.
// It uses stream-json internally so that tool_use events emitted by the
// subprocess are captured and returned in CompletionResponse.ToolCalls.
func (p *ClaudeCodeProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()

	prompt := p.buildPrompt(req)
	args := p.buildArgs(req)
	args = append(args,
		"--output-format", "stream-json",
		"--verbose",
		"--include-partial-messages",
	)

	cmd := NewProviderCommandContext(ctx, ManagedCommandOpts{EnvPolicy: EnvPolicyProviderChild, Dir: req.WorkDir}, p.cliBinary, args...)
	setProcessGroup(cmd)
	cmd.WaitDelay = claudeCodeKillGrace
	cmd.Cancel = func() error {
		return killProcessGroup(cmd)
	}
	cmd.Stdin = strings.NewReader(prompt)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	proc := p.procMgr.Track(cmd, ManagedProcessOpts{
		Kind:   ProcessForeground,
		Source: req.Metadata.Source,
	})
	defer p.procMgr.Remove(proc.ID)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start claude: %w", err)
	}

	content, toolCalls, usage, stopReason, parseErr := p.drainStreamJSON(stdout)

	if waitErr := cmd.Wait(); waitErr != nil && ctx.Err() != nil {
		return nil, fmt.Errorf("cancelled: %w", ctx.Err())
	}

	if parseErr != nil {
		return nil, parseErr
	}

	resp := &CompletionResponse{
		Content:    content,
		ToolCalls:  toolCalls,
		StopReason: stopReason,
		Usage:      usage,
		ProviderMeta: ProviderMeta{
			Provider: p.name,
			Model:    p.model,
			Latency:  time.Since(start),
		},
	}
	return resp, nil
}

// drainStreamJSON reads NDJSON lines from r and aggregates them into a
// complete response. It returns the accumulated text content, any tool calls
// that were emitted (finalized at content_block_stop), the token usage from
// the result message, the stop reason, and any parse error.
func (p *ClaudeCodeProvider) drainStreamJSON(r io.Reader) (content string, toolCalls []ToolCall, usage TokenUsage, stopReason string, err error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	state := newCCStreamState()
	var sb strings.Builder

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		chunk, done := p.parseStreamLine(line, state)
		if chunk != nil {
			if chunk.Error != nil {
				return "", nil, TokenUsage{}, "", chunk.Error
			}
			if chunk.Delta != "" {
				sb.WriteString(chunk.Delta)
			}
			if chunk.Done && chunk.Usage != nil {
				usage = *chunk.Usage
			}
		}
		if done {
			break
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return "", nil, TokenUsage{}, "", fmt.Errorf("scan stream: %w", scanErr)
	}

	return sb.String(), state.done, usage, state.stopReason, nil
}

// Stream spawns a claude process and returns incremental chunks.
// The returned channel closes when the process exits or ctx is cancelled.
// On ctx cancellation (client disconnect), the process is killed.
func (p *ClaudeCodeProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	prompt := p.buildPrompt(req)
	args := p.buildArgs(req)
	args = append(args,
		"--output-format", "stream-json",
		"--verbose",
		"--include-partial-messages",
	)

	cmd := NewProviderCommandContext(ctx, ManagedCommandOpts{EnvPolicy: EnvPolicyProviderChild, Dir: req.WorkDir}, p.cliBinary, args...)
	setProcessGroup(cmd)
	cmd.WaitDelay = claudeCodeKillGrace
	cmd.Cancel = func() error {
		return killProcessGroup(cmd)
	}
	cmd.Stdin = strings.NewReader(prompt)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start claude: %w", err)
	}

	proc := p.procMgr.Track(cmd, ManagedProcessOpts{
		Kind:   ProcessForeground,
		Source: req.Metadata.Source,
	})

	ch := make(chan StreamChunk, 32)
	start := time.Now()

	go func() {
		defer close(ch)
		defer p.procMgr.Remove(proc.ID)

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 256*1024), 256*1024) // 256KB line buffer

		state := newCCStreamState()

		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			chunk, done := p.parseStreamLine(line, state)
			if chunk != nil {
				select {
				case ch <- *chunk:
				case <-ctx.Done():
					p.procMgr.Kill(proc.ID)
					ch <- StreamChunk{Error: ctx.Err(), Done: true}
					return
				}
			}
			if done {
				break
			}
		}

		// Wait for process to exit.
		exitErr := cmd.Wait()

		// Send final chunk with usage info.
		finalChunk := StreamChunk{
			Done: true,
			ProviderMeta: &ProviderMeta{
				Provider: p.name,
				Model:    p.model,
				Latency:  time.Since(start),
			},
		}
		if exitErr != nil && ctx.Err() == nil {
			finalChunk.Error = fmt.Errorf("claude process exited: %w", exitErr)
		}

		// If we captured usage from the result message, attach it.
		if proc.Usage != nil {
			finalChunk.Usage = proc.Usage
		}

		select {
		case ch <- finalChunk:
		default:
		}
	}()

	return ch, nil
}

// ── Prompt & argument construction ──────────────────────────────────────────

// buildPrompt assembles the user-facing prompt from the CompletionRequest.
// Context injection is handled via --append-system-prompt. For Claude Code we
// keep the prompt body lightweight: user turns plus minimal continuity markers.
func (p *ClaudeCodeProvider) buildPrompt(req *CompletionRequest) string {
	// claude -p treats stdin as a single user message — it has no concept of
	// multi-turn conversation. We extract the last user message as the prompt
	// and fold prior conversation into a compact history prefix so the model
	// has continuity context without repeating earlier messages verbatim.

	var history []string
	var lastUserMsg string

	for _, m := range req.Messages {
		switch m.Role {
		case "user":
			if lastUserMsg != "" {
				// Push previous user message into history before overwriting.
				history = append(history, fmt.Sprintf("[user]: %s", truncateForHistory(lastUserMsg, 200)))
			}
			lastUserMsg = m.Content
		case "assistant":
			content := strings.TrimSpace(m.Content)
			if content != "" {
				history = append(history, fmt.Sprintf("[assistant]: %s", truncateForHistory(content, 200)))
			}
		}
	}

	if lastUserMsg == "" {
		return ""
	}

	// If there's prior conversation, prepend a compact summary.
	if len(history) > 0 {
		var sb strings.Builder
		sb.WriteString("<conversation_history>\n")
		for _, h := range history {
			sb.WriteString(h)
			sb.WriteByte('\n')
		}
		sb.WriteString("</conversation_history>\n\n")
		sb.WriteString(lastUserMsg)
		return sb.String()
	}

	return lastUserMsg
}

// truncateForHistory trims a message to maxLen characters for the history prefix.
func truncateForHistory(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	// Collapse whitespace runs.
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}

// buildArgs constructs the claude CLI arguments from the request.
func (p *ClaudeCodeProvider) buildArgs(req *CompletionRequest) []string {
	args := []string{"-p", "--dangerously-skip-permissions"}

	// Model selection.
	model := p.model
	if req.ModelOverride != "" {
		model = req.ModelOverride
	}
	args = append(args, "--model", model)

	// Effort level.
	if p.effort != "" {
		args = append(args, "--effort", p.effort)
	}

	// System prompt: inject workspace context.
	if req.SystemPrompt != "" {
		args = append(args, "--append-system-prompt", req.SystemPrompt)
	}

	// MCP configuration.
	if p.mcpConfig != "" {
		args = append(args, "--mcp-config", p.mcpConfig, "--strict-mcp-config")
	}

	// Tool access control.
	if len(p.allowedTools) > 0 {
		args = append(args, "--allowedTools")
		args = append(args, p.allowedTools...)
	}
	if len(p.disallowedTools) > 0 {
		args = append(args, "--disallowedTools")
		args = append(args, p.disallowedTools...)
	}

	// Budget cap (per-request).
	if req.Metadata.MaxCostUSD != nil && *req.Metadata.MaxCostUSD > 0 {
		args = append(args, "--max-budget-usd", fmt.Sprintf("%.2f", *req.Metadata.MaxCostUSD))
	}

	// Session management — allow resuming conversations.
	if req.Metadata.RequestID != "" {
		// Don't persist sessions for one-off requests.
		args = append(args, "--no-session-persistence")
	}

	return args
}

// ── NDJSON stream parsing ───────────────────────────────────────────────────

// ccStreamMessage is the top-level NDJSON envelope from claude --output-format stream-json.
type ccStreamMessage struct {
	Type    string          `json:"type"`    // "system", "assistant", "stream_event", "result"
	Subtype string          `json:"subtype"` // "init" for system
	Event   json.RawMessage `json:"event"`   // for stream_event
	// Result fields (present when type == "result")
	Result     string          `json:"result"`
	IsError    bool            `json:"is_error"`
	StopReason string          `json:"stop_reason"`
	Usage      json.RawMessage `json:"usage"`
	ModelUsage json.RawMessage `json:"modelUsage"`
	SessionID  string          `json:"session_id"`
}

// ccResult is the final JSON output from claude --output-format json.
type ccResult struct {
	Type       string          `json:"type"`
	Result     string          `json:"result"`
	IsError    bool            `json:"is_error"`
	StopReason string          `json:"stop_reason"`
	SessionID  string          `json:"session_id"`
	Usage      json.RawMessage `json:"usage"`
	ModelUsage json.RawMessage `json:"modelUsage"`
	TotalCost  float64         `json:"total_cost_usd"`
}

// ccStreamEvent wraps an Anthropic SSE event inside stream-json output.
type ccStreamEvent struct {
	Type  string `json:"type"` // content_block_start, content_block_delta, content_block_stop, message_delta, etc.
	Index int    `json:"index"`
	// ContentBlock is populated for content_block_start events.
	ContentBlock struct {
		Type string `json:"type"` // "text" or "tool_use"
		ID   string `json:"id"`   // tool call id (tool_use only)
		Name string `json:"name"` // tool name (tool_use only)
	} `json:"content_block"`
	// Delta is populated for content_block_delta events.
	Delta struct {
		Type        string `json:"type"`         // "text_delta" or "input_json_delta"
		Text        string `json:"text"`         // populated for text_delta
		PartialJSON string `json:"partial_json"` // populated for input_json_delta
	} `json:"delta"`
}

// partialToolCall accumulates streamed tool_use content block data.
type partialToolCall struct {
	id   string
	name string
	args strings.Builder
}

// ccStreamState holds mutable state across a sequence of parseStreamLine calls.
type ccStreamState struct {
	// pending maps content_block index → in-progress tool call.
	pending map[int]*partialToolCall
	// done is the list of finalized tool calls.
	done []ToolCall
	// stopReason from the result message.
	stopReason string
}

func newCCStreamState() *ccStreamState {
	return &ccStreamState{pending: make(map[int]*partialToolCall)}
}

// parseStreamLine parses a single NDJSON line from claude's stream output.
// State carries mutable tool-call aggregation across calls; pass the same
// pointer for every line in a stream. Returns a StreamChunk (or nil if the
// line should be skipped) and whether this is the final line.
func (p *ClaudeCodeProvider) parseStreamLine(line []byte, state *ccStreamState) (*StreamChunk, bool) {
	var msg ccStreamMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		slog.Debug("claudecode: unparseable stream line", "err", err)
		return nil, false
	}

	switch msg.Type {
	case "stream_event":
		var evt ccStreamEvent
		if err := json.Unmarshal(msg.Event, &evt); err != nil {
			return nil, false
		}

		switch evt.Type {
		case "content_block_start":
			if evt.ContentBlock.Type == "tool_use" {
				state.pending[evt.Index] = &partialToolCall{
					id:   evt.ContentBlock.ID,
					name: evt.ContentBlock.Name,
				}
				return &StreamChunk{
					ToolCallDelta: &ToolCallDelta{
						Index: evt.Index,
						ID:    evt.ContentBlock.ID,
						Name:  evt.ContentBlock.Name,
					},
				}, false
			}
			return nil, false

		case "content_block_delta":
			switch evt.Delta.Type {
			case "text_delta":
				if evt.Delta.Text != "" {
					return &StreamChunk{Delta: evt.Delta.Text}, false
				}
			case "input_json_delta":
				if ptc, ok := state.pending[evt.Index]; ok && evt.Delta.PartialJSON != "" {
					ptc.args.WriteString(evt.Delta.PartialJSON)
					return &StreamChunk{
						ToolCallDelta: &ToolCallDelta{
							Index:     evt.Index,
							ArgsDelta: evt.Delta.PartialJSON,
						},
					}, false
				}
			}
			return nil, false

		case "content_block_stop":
			// Finalize any pending tool call at this index.
			if ptc, ok := state.pending[evt.Index]; ok {
				state.done = append(state.done, ToolCall{
					ID:        ptc.id,
					Name:      ptc.name,
					Arguments: ptc.args.String(),
				})
				delete(state.pending, evt.Index)
			}
			return nil, false

		default:
			return nil, false
		}

	case "result":
		if state != nil {
			state.stopReason = msg.StopReason
		}
		usage := p.extractUsageFromRaw(msg.Usage)
		chunk := &StreamChunk{
			Done:       true,
			StopReason: msg.StopReason,
			Usage:      &usage,
			ProviderMeta: &ProviderMeta{
				Provider: p.name,
				Model:    p.model,
			},
		}
		if msg.IsError {
			chunk.Error = fmt.Errorf("claude error: %s", msg.Result)
		}
		return chunk, true

	case "system", "assistant":
		// system/init and full assistant messages — skip for streaming.
		return nil, false

	default:
		return nil, false
	}
}

// ── Usage extraction ────────────────────────────────────────────────────────

func (p *ClaudeCodeProvider) extractUsage(result *ccResult) TokenUsage {
	return p.extractUsageFromRaw(result.Usage)
}

func (p *ClaudeCodeProvider) extractUsageFromRaw(raw json.RawMessage) TokenUsage {
	if raw == nil {
		return TokenUsage{}
	}
	var usage struct {
		InputTokens              int `json:"input_tokens"`
		OutputTokens             int `json:"output_tokens"`
		CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	}
	if err := json.Unmarshal(raw, &usage); err != nil {
		return TokenUsage{}
	}
	return TokenUsage{
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		CacheReadTokens:  usage.CacheReadInputTokens,
		CacheWriteTokens: usage.CacheCreationInputTokens,
	}
}

func (p *ClaudeCodeProvider) resolveModel(result *ccResult) string {
	if result.ModelUsage == nil {
		return p.model
	}
	// modelUsage is map[modelName]stats — extract the key.
	var mu map[string]json.RawMessage
	if err := json.Unmarshal(result.ModelUsage, &mu); err != nil {
		return p.model
	}
	for name := range mu {
		return name // first key is the model used
	}
	return p.model
}

// ── Background task support ─────────────────────────────────────────────────

// BackgroundTaskOpts configures a fire-and-forget Claude Code task.
type BackgroundTaskOpts struct {
	Prompt          string
	Model           string
	Effort          string
	MCPConfig       string
	AllowedTools    []string
	Source          string // "discord", "signal", "http", etc.
	CallbackChannel string // channel to report results to
	Identity        string // NodeID of the requestor
	MaxBudgetUSD    float64
	Timeout         time.Duration
	WorkDir         string // working directory for the process
	SystemPrompt    string
}

// SpawnBackground starts a Claude Code process that outlives the HTTP request.
// Results are delivered via the process manager's callback mechanism.
//
// Deprecated: SpawnBackground is deprecated as a substrate primitive per
// ADR-093 §8 (Migration plan, Commit 1). It spawns a one-shot
// `claude -p --no-session-persistence` process with no lifecycle tracking
// beyond the process manager's callback — no state machine, no
// resumability, no cancellation contract. New substrate callers must use
// ManagedSession (internal/engine/managed_session.go) instead, which spawns
// via internal/acp.Subprocess and exposes the ADR-093 §4
// Starting/Live/Stalled/Detached/Crashed lifecycle plus a Cancel() that
// encodes the L1 cancellation spike's verdict (gate SIGINT on having
// observed system.init; stdin-close is "no more turns", not abort).
//
// This method is not yet removed: its sole current caller,
// serve_claude_code.go's POST /v1/claude-code/spawn handler, has not been
// migrated (that migration is ADR-093 §8 Commits 3-4, a later lane — this
// commit only marks the primitive deprecated and adds audit_callers.go to
// flag any *additional* caller before that migration lands). Do not add
// new SpawnBackground call sites; wire new spawn paths through
// ManagedSessionRegistry.
func (p *ClaudeCodeProvider) SpawnBackground(opts BackgroundTaskOpts) (string, error) {
	args := []string{"-p"}
	args = append(args, "--output-format", "json")

	model := opts.Model
	if model == "" {
		model = p.model
	}
	args = append(args, "--model", model)

	if opts.Effort != "" {
		args = append(args, "--effort", opts.Effort)
	}
	if opts.SystemPrompt != "" {
		args = append(args, "--append-system-prompt", opts.SystemPrompt)
	}
	if opts.MCPConfig != "" {
		args = append(args, "--mcp-config", opts.MCPConfig, "--strict-mcp-config")
	}
	if len(opts.AllowedTools) > 0 {
		args = append(args, "--allowedTools")
		args = append(args, opts.AllowedTools...)
	}
	if opts.MaxBudgetUSD > 0 {
		args = append(args, "--max-budget-usd", fmt.Sprintf("%.2f", opts.MaxBudgetUSD))
	}
	args = append(args, "--no-session-persistence")

	ctx, cancel := context.WithCancel(context.Background())
	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), opts.Timeout)
	}

	cmd := NewProviderCommandContext(ctx, ManagedCommandOpts{EnvPolicy: EnvPolicyProviderChild, Dir: opts.WorkDir}, p.cliBinary, args...)
	setProcessGroup(cmd)
	cmd.WaitDelay = claudeCodeKillGrace
	cmd.Cancel = func() error {
		return killProcessGroup(cmd)
	}
	cmd.Stdin = strings.NewReader(opts.Prompt)

	proc := p.procMgr.Track(cmd, ManagedProcessOpts{
		Kind:            ProcessBackground,
		Source:          opts.Source,
		CallbackChannel: opts.CallbackChannel,
		Identity:        opts.Identity,
		Cancel:          cancel,
	})

	if err := cmd.Start(); err != nil {
		cancel()
		p.procMgr.Remove(proc.ID)
		return "", fmt.Errorf("start background claude: %w", err)
	}

	// Monitor in a goroutine — capture output and fire callback.
	go func() {
		defer cancel()
		defer p.procMgr.Finish(proc.ID)

		err := cmd.Wait()
		if err != nil {
			proc.SetError(err)
		}
	}()

	return proc.ID, nil
}
