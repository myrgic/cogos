// provider_pi.go — PiProvider
//
// Implements Provider by spawning `pi -p` subprocesses for local agentic
// inference. Pi handles the tool loop (read, bash, edit, write) against local
// models, while the kernel handles context assembly.
//
// This is the local counterpart to ClaudeCodeProvider:
//   - ClaudeCodeProvider: cloud agentic inference (Claude Max via OAuth)
//   - PiProvider: local agentic inference via Pi
//
// The kernel assembles foveated context and injects it via --system-prompt.
// Pi runs the agent loop; the local backend runs the model. defaultLocalPiProvider
// below is configured as "lmstudio" (PR #417) — but that is only a config
// default, not a guarantee that pi's own provider registry (agent/models.json,
// what `pi --list-models` reads) actually has an "lmstudio" entry. Available()
// verifies the configured --provider exists in that registry before reporting
// the provider usable. Earlier comments here claimed PR #417 already made this
// safe; it did not — see #629.
//
// Output: parsed from `--mode json` which emits NDJSON AgentSessionEvents.
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// defaultLocalPiProvider is the pi `--provider` value used when a PiProvider
// config does not specify one. Set to "lmstudio" by PR #417 to match the
// engine's intended local backend. This is a config default only — whether
// pi's own registry (agent/models.json) actually has an "lmstudio" provider
// entry is verified at runtime by Available() via piRegistryHas (#629). A
// mismatch is reported as unavailable, not silently repointed to whatever
// provider the registry does have.
const defaultLocalPiProvider = "lmstudio"

// defaultLocalModel is the model used when a PiProvider config does not specify
// one. The resident local model is google/gemma-4-26b-a4b via LM Studio
// (lmstudio-darkstar), consistent with defaults/providers.yaml (PR #417).
const defaultLocalModel = "google/gemma-4-26b-a4b"

// PiProvider implements Provider by spawning pi CLI processes.
type PiProvider struct {
	name     string
	provider string // lmstudio, openrouter, etc.
	model    string // e.g. google/gemma-4-26b-a4b
	thinking string // off, minimal, low, medium, high, xhigh
	timeout  time.Duration
	piBinary string // path to pi binary (default: "pi")
	tools    string // comma-separated tools (default: "read,bash,edit,write")
	procMgr  *ProcessManager

	// backend overrides the local server URL probed by Available (tests /
	// non-default LM Studio ports). Empty means defaultPiBackendURL.
	backend string

	// Availability cache — same shape as ClaudeCodeProvider / ClaudeOAuthProvider.
	availMu     sync.Mutex
	availAt     time.Time
	availResult bool
}

// NewPiProvider creates a PiProvider from a ProviderConfig.
func NewPiProvider(name string, cfg ProviderConfig, procMgr *ProcessManager) *PiProvider {
	model := cfg.Model
	if model == "" {
		model = defaultLocalModel
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout == 0 {
		timeout = 300 * time.Second
	}
	binary := "pi"
	if cfg.Endpoint != "" {
		binary = cfg.Endpoint
	}

	provider := defaultLocalPiProvider
	thinking := "off"
	tools := "read,bash,edit,write"

	if cfg.Options != nil {
		if p, ok := cfg.Options["provider"].(string); ok {
			provider = p
		}
		if t, ok := cfg.Options["thinking"].(string); ok {
			thinking = t
		}
		if t, ok := cfg.Options["tools"].(string); ok {
			tools = t
		}
	}

	return &PiProvider{
		name:     name,
		provider: provider,
		model:    model,
		thinking: thinking,
		timeout:  timeout,
		piBinary: binary,
		tools:    tools,
		procMgr:  procMgr,
	}
}

func (p *PiProvider) Name() string  { return p.name }
func (p *PiProvider) Model() string { return p.model }

// piLookPath and piBackendProbe are package-level so tests can stub them.
// Mirrors codexLookPath / claudeCodeAuthProbe.
var piLookPath = exec.LookPath

// piBackendProbe asks the local inference backend pi will spawn against whether
// it is actually serving. pi has no credential of its own — it fronts an
// OpenAI-compatible local server (LM Studio by default, PR #417) — so the
// honest availability question is not "is pi installed" but "is the backend pi
// will talk to up". A pi binary with no backend is exactly as unavailable as
// a claude binary with no login.
var piBackendProbe = func(ctx context.Context, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/models", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("backend %s returned %d", baseURL, resp.StatusCode)
	}
	return nil
}

// defaultPiBackendURL is where pi's default local provider (lmstudio) listens.
// Matches the resident LM Studio server documented in the file header.
const defaultPiBackendURL = "http://127.0.0.1:1234"

// piHomeDir resolves pi's home directory (where agent/models.json lives).
// Package-level var so tests can stub it. Mirrors PI's own resolution order:
// $PI_HOME if set, else ~/.pi.
var piHomeDir = func() (string, error) {
	if h := os.Getenv("PI_HOME"); h != "" {
		return h, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".pi"), nil
}

// piModelsRegistry is the shape of $PI_HOME/agent/models.json that matters
// here: a map of configured provider name -> provider config. We only need
// to know whether a given provider key exists, so the value is left opaque.
type piModelsRegistry struct {
	Providers map[string]json.RawMessage `json:"providers"`
}

// piRegistryHas reports whether pi's own provider registry — the same file
// `pi --list-models` reads — has an entry for the given --provider value.
// Configuring PiProvider with provider="lmstudio" (the engine's default,
// PR #417) does not mean pi itself knows about an "lmstudio" provider; the
// two are independently configured, and a mismatch makes every pi invocation
// fail with `Error: Unknown provider "..."` despite Available() historically
// reporting true. The second return value is the registry path checked, for
// logging/diagnostics.
func piRegistryHas(provider string) (bool, string, error) {
	home, err := piHomeDir()
	if err != nil {
		return false, "", err
	}
	path := filepath.Join(home, "agent", "models.json")

	data, err := os.ReadFile(path)
	if err != nil {
		return false, path, err
	}

	var reg piModelsRegistry
	if err := json.Unmarshal(data, &reg); err != nil {
		return false, path, fmt.Errorf("parse %s: %w", path, err)
	}

	_, ok := reg.Providers[provider]
	return ok, path, nil
}

// Available reports whether pi can actually serve a request: the binary must be
// on PATH AND the local backend it fronts must answer. Before this, Available
// was exec.LookPath alone — the same defect class as claude-code and codex
// (a binary on PATH is not a working provider). Result is cached for
// availCacheTTL under availMu, the pattern validated in ClaudeOAuthProvider.
func (p *PiProvider) Available(ctx context.Context) bool {
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

func (p *PiProvider) probeAvailable(ctx context.Context) bool {
	path, err := piLookPath(p.piBinary)
	if err != nil || path == "" {
		return false
	}

	// pi maintains its own provider registry (agent/models.json, the source
	// `pi --list-models` reads) independent of any config default the engine
	// applies. A configured provider missing from that registry makes every
	// pi invocation fail with `Error: Unknown provider "..."` regardless of
	// which --provider is configured — this check applies to every provider,
	// not just the local lmstudio default, otherwise the exact defect class
	// #629 fixes reopens for any non-default --provider (e.g. "ollama"). Do
	// not silently repoint to whatever provider the registry does have;
	// report unavailable and say why. This was the gap left by PR #417.
	has, registryPath, err := piRegistryHas(p.provider)
	if err != nil {
		slog.Warn("pi: could not read provider registry", "provider", p.provider, "registry", registryPath, "err", err)
		return false
	}
	if !has {
		slog.Warn("pi: configured --provider not in pi registry", "provider", p.provider, "registry", registryPath, "hint", "pi --list-models")
		return false
	}

	// Only the local lmstudio backend has an HTTP probe target we own. For
	// any other pi provider (openrouter, etc.) we cannot vouch for the
	// remote's auth/serving state from here, so registry presence plus
	// binary presence is the best available signal — a known limitation,
	// not a claim of verified end-to-end availability.
	if p.provider != defaultLocalPiProvider {
		return true
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := piBackendProbe(probeCtx, p.backendURL()); err != nil {
		slog.Debug("pi: Available: backend probe failed", "backend", p.backendURL(), "err", err)
		return false
	}
	return true
}

// backendURL is the local server pi's default provider connects to.
func (p *PiProvider) backendURL() string {
	if p.backend != "" {
		return p.backend
	}
	return defaultPiBackendURL
}

func (p *PiProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities: []Capability{
			CapStreaming,
			CapToolUse,
		},
		MaxContextTokens:   32768,
		MaxOutputTokens:    8192,
		ModelsAvailable:    []string{p.model},
		IsLocal:            true,
		AgenticHarness:     true,
		CostPerInputToken:  0,
		CostPerOutputToken: 0,
	}
}

func (p *PiProvider) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, p.piBinary, "--help")
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("pi binary not available: %w", err)
	}
	return time.Since(start), nil
}

// Complete sends a prompt and waits for the full response.
func (p *PiProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()

	prompt := p.buildPrompt(req)
	args := p.buildArgs(req)

	cmd := NewProviderCommandContext(ctx, ManagedCommandOpts{EnvPolicy: EnvPolicyProviderChild}, p.piBinary, args...)
	cmd.Stdin = strings.NewReader(prompt)

	proc := p.procMgr.Track(cmd, ManagedProcessOpts{
		Kind:   ProcessForeground,
		Source: req.Metadata.Source,
	})
	defer p.procMgr.Remove(proc.ID)

	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("pi exited with error: %w", err)
	}

	// Pi print mode outputs the response text directly to stdout.
	content := strings.TrimSpace(string(out))

	return &CompletionResponse{
		Content:    content,
		StopReason: "end_turn",
		ProviderMeta: ProviderMeta{
			Provider: p.name,
			Model:    p.model,
			Latency:  time.Since(start),
		},
	}, nil
}

// Stream spawns a pi process in JSON mode and returns incremental chunks.
func (p *PiProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	prompt := p.buildPrompt(req)
	args := p.buildArgs(req)
	// Replace --print with --mode json for streaming NDJSON output.
	for i, a := range args {
		if a == "-p" || a == "--print" {
			args[i] = "--mode"
			args = append(args[:i+1], append([]string{"json"}, args[i+1:]...)...)
			break
		}
	}

	cmd := NewProviderCommandContext(ctx, ManagedCommandOpts{EnvPolicy: EnvPolicyProviderChild}, p.piBinary, args...)
	cmd.Stdin = strings.NewReader(prompt)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start pi: %w", err)
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
		scanner.Buffer(make([]byte, 256*1024), 256*1024)

		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			chunk, done := p.parseStreamLine(line)
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

		exitErr := cmd.Wait()

		finalChunk := StreamChunk{
			Done: true,
			ProviderMeta: &ProviderMeta{
				Provider: p.name,
				Model:    p.model,
				Latency:  time.Since(start),
			},
		}
		if exitErr != nil && ctx.Err() == nil {
			finalChunk.Error = fmt.Errorf("pi process exited: %w", exitErr)
		}

		select {
		case ch <- finalChunk:
		default:
		}
	}()

	return ch, nil
}

// ── Prompt & argument construction ──────────────────────────────────────────

func (p *PiProvider) buildPrompt(req *CompletionRequest) string {
	var history []string
	var lastUserMsg string

	for _, m := range req.Messages {
		switch m.Role {
		case "user":
			if lastUserMsg != "" {
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

func (p *PiProvider) buildArgs(req *CompletionRequest) []string {
	args := []string{"-p"}

	args = append(args, "--provider", p.provider)

	model := p.model
	if req.ModelOverride != "" {
		model = req.ModelOverride
	}
	args = append(args, "--model", model)

	if p.thinking != "" && p.thinking != "off" {
		args = append(args, "--thinking", p.thinking)
	}

	if p.tools != "" {
		args = append(args, "--tools", p.tools)
	}

	// Kernel-assembled context injected as system prompt.
	if req.SystemPrompt != "" {
		args = append(args, "--system-prompt", req.SystemPrompt)
	}

	// No session persistence for kernel-routed requests.
	args = append(args, "--no-session")

	// No extensions — the kernel handles context and tools.
	args = append(args, "--no-extensions")

	return args
}

// ── NDJSON stream parsing ───────────────────────────────────────────────────

// piStreamEvent is a single NDJSON line from pi --mode json.
type piStreamEvent struct {
	Type    string          `json:"type"`
	Message json.RawMessage `json:"message,omitempty"`
	// message_update fields
	AssistantMessageEvent *piAssistantEvent `json:"assistantMessageEvent,omitempty"`
	// agent_end fields
	Messages json.RawMessage `json:"messages,omitempty"`
}

type piAssistantEvent struct {
	Type  string `json:"type"`  // text_delta, tool_call_delta, etc.
	Delta string `json:"delta"` // text content for text_delta
}

func (p *PiProvider) parseStreamLine(line []byte) (*StreamChunk, bool) {
	var evt piStreamEvent
	if err := json.Unmarshal(line, &evt); err != nil {
		slog.Debug("pi: unparseable stream line", "err", err)
		return nil, false
	}

	switch evt.Type {
	case "message_update":
		if evt.AssistantMessageEvent != nil && evt.AssistantMessageEvent.Type == "text_delta" {
			delta := evt.AssistantMessageEvent.Delta
			if delta != "" {
				return &StreamChunk{Delta: delta}, false
			}
		}
		return nil, false

	case "agent_end":
		return &StreamChunk{
			Done: true,
			ProviderMeta: &ProviderMeta{
				Provider: p.name,
				Model:    p.model,
			},
		}, true

	case "session", "agent_start", "turn_start", "turn_end",
		"message_start", "message_end", "queue_update",
		"tool_execution_start", "tool_execution_update", "tool_execution_end":
		return nil, false

	default:
		return nil, false
	}
}
