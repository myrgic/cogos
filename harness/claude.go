// claude.go handles Claude CLI argument construction and MCP config passthrough.
//
// BuildClaudeArgs is the main entry point — it takes an InferenceRequest and
// produces the argument list for `claude -p ... --output-format stream-json`.
// This includes system prompt chaining (TAA context + client prompt), model
// mapping, tool forwarding (--allowed-tools), and MCP config.
package harness

import (
	"encoding/json"
	"strings"

)

// ClaudeCommand is the name of the Claude CLI binary.
const ClaudeCommand = "claude"

// chainSystemPrompt combines TAA context and client system prompt into a single
// header chain, separated by ---. TAA context comes first (identity, temporal,
// present, semantic), followed by any client-provided system instructions.
func chainSystemPrompt(req *InferenceRequest) string {
	var taaBlock string
	if req.ContextState != nil {
		taaBlock = req.ContextState.BuildContextString()
	}
	switch {
	case taaBlock != "" && req.SystemPrompt != "":
		return taaBlock + "\n\n---\n\n" + req.SystemPrompt
	case taaBlock != "":
		return taaBlock
	default:
		return req.SystemPrompt
	}
}

// BuildClaudeArgs constructs the Claude CLI arguments from an InferenceRequest.
// Supports both legacy mode (SystemPrompt) and new context-aware mode (ContextState).
func BuildClaudeArgs(req *InferenceRequest) []string {
	args := []string{
		"-p", req.Prompt,
		"--output-format", "stream-json",
		"--include-partial-messages", // Enable rich streaming with content_block_delta events
		"--verbose",
	}

	// Session continuity: --resume reloads a prior Claude Code session from
	// disk, so Claude sees the full conversation history without re-injection.
	if req.ClaudeSessionID != "" {
		args = append(args, "--resume", req.ClaudeSessionID)
	}

	// Always skip permissions in harness mode — the subprocess has no TTY
	// to prompt for approval. Tool restriction is enforced via --allowed-tools.
	// (The CRD's dangerouslySkipPermissions field is for direct Claude Code
	// shell usage, not for the CogOS harness subprocess.)
	args = append(args, "--dangerously-skip-permissions")

	// Build system prompt: chain TAA context + client system prompt
	if sp := chainSystemPrompt(req); sp != "" {
		args = append(args, "--append-system-prompt", sp)
	}

	// Use request-level schema if provided
	var schema json.RawMessage
	if len(req.Schema) > 0 {
		schema = req.Schema
	}

	// Add JSON schema if requested
	if len(schema) > 0 {
		args = append(args, "--json-schema", string(schema))
	}

	// Determine model source
	// Priority: ContextState.Model > req.Model
	model := req.Model
	if req.ContextState != nil && req.ContextState.Model != "" {
		model = req.ContextState.Model
	}

	// Map model IDs to Claude CLI aliases
	if model != "" && model != "claude" {
		switch model {
		case "claude-opus-4-5-20250929", "opus-4-5", "opus":
			model = "opus"
		case "claude-sonnet-4-5-20250929", "sonnet-4-5", "sonnet":
			model = "sonnet"
		}
		args = append(args, "--model", model)
	}

	// Forward tool control to Claude CLI
	if len(req.AllowedTools) > 0 {
		// Explicit allowed-tools list takes priority
		args = append(args, "--allowed-tools", strings.Join(req.AllowedTools, ","))
	} else if len(req.Tools) > 0 {
		// Map OpenAI-format tool definitions to Claude CLI tool names
		if mapped := MapToolsToCLINames(req.Tools); len(mapped) > 0 {
			args = append(args, "--allowed-tools", strings.Join(mapped, ","))
		}
	}

	// MCP bridge configuration
	if req.MCPConfig != "" {
		args = append(args, "--mcp-config", req.MCPConfig)
	}

	return args
}

// BuildContextMetrics extracts metrics from ContextState for response
func BuildContextMetrics(ctx *ContextState) *ContextMetrics {
	if ctx == nil {
		return nil
	}

	tierBreakdown := make(map[string]int)
	totalTokens := 0

	if ctx.Tier1Identity != nil {
		tierBreakdown["tier1_identity"] = ctx.Tier1Identity.Tokens
		totalTokens += ctx.Tier1Identity.Tokens
	}
	if ctx.Tier2Temporal != nil {
		tierBreakdown["tier2_temporal"] = ctx.Tier2Temporal.Tokens
		totalTokens += ctx.Tier2Temporal.Tokens
	}
	if ctx.Tier3Present != nil {
		tierBreakdown["tier3_present"] = ctx.Tier3Present.Tokens
		totalTokens += ctx.Tier3Present.Tokens
	}

	// Use provided total if available, otherwise use computed
	if ctx.TotalTokens > 0 {
		totalTokens = ctx.TotalTokens
	}

	return &ContextMetrics{
		TotalTokens:     totalTokens,
		TierBreakdown:   tierBreakdown,
		CoherenceScore:  ctx.CoherenceScore,
		CompressionUsed: false, // Set by caller if compression was applied
	}
}
