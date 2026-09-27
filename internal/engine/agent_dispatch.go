// agent_dispatch.go — Phase 2 transport: task-parameterized dispatch into the
// kernel-interior agent harness, with concurrency, structured return, per-call
// scope narrowing, and pluggable model routing.
//
// The cog_dispatch_to_harness MCP tool surfaces this transport. It is the
// foveal -> peripheral handoff: a big external Claude session can offload a
// piece of cognitive work (validation, rewriting, modality matching) onto the
// resident LM Studio instance (lmstudio-local, gemma-4-26b at 127.0.0.1:1234)
// without burning Anthropic tokens.
//
// This file owns the *contract* — the request and result types and the
// AgentController extension. The concrete dispatcher lives in the root
// package (agent_dispatch.go in main) so it can reach the *AgentHarness
// instance the kernel runs.
//
// Identity propagation note: per the Phase 2 plan, identity claims travel
// through DispatchRequest.Identity as opaque OIDC-shaped fields (iss/sub/aud
// + claims map). Today the controller adapter copies these through to the
// harness as cycle-trace metadata; full CRD-based identity binding waits for
// the Wave 6b migration. The field is wired now so that integration is
// additive — no schema break later.
package engine

import "context"

// DispatchModel selects the inference backend. "e4b" and "26b" are legacy
// enum values retained for backward compatibility; both now route to the
// LM Studio provider (lmstudio-local, 127.0.0.1:1234). The preferred
// path is to set DispatchRequest.Provider = "lmstudio-local" explicitly.
// Empty string routes through process-state or harness_provider config, which
// should also resolve to lmstudio-local on this node.
type DispatchModel string

const (
	// DispatchModelE4B is retained for backward compatibility. Ollama has been
	// decommissioned; dispatches using this value route through the legacy
	// local-LLM probe which resolves to the LM Studio provider when Ollama is
	// absent. Prefer Provider="lmstudio-local" for new callers.
	DispatchModelE4B DispatchModel = "e4b"
	DispatchModel26B DispatchModel = "26b"
)

// DispatchIdentity carries OIDC-shaped identity claims propagated from the
// caller. All fields are optional — when absent, the dispatcher records
// "anonymous" in the trace and lets the harness operate under its default
// envelope. Once Wave 6b lands, these claims will be checked against the
// Identity CRD reconciler before tool selection; today they are observability
// metadata only.
type DispatchIdentity struct {
	Iss    string                 `json:"iss,omitempty"`    // issuer (e.g. "anthropic.claude-code")
	Sub    string                 `json:"sub,omitempty"`    // subject (e.g. session id, user handle)
	Aud    string                 `json:"aud,omitempty"`    // audience (e.g. "cogos.kernel")
	Claims map[string]interface{} `json:"claims,omitempty"` // free-form claim bag
}

// DispatchRequest is the normalized input one DispatchToHarness call accepts.
// Validation and clamping happen in the controller adapter; engine callers
// can pass through unchecked since the controller re-normalizes.
type DispatchRequest struct {
	// AgentID names the harness instance. Defaults to "primary" when empty.
	AgentID string

	// Task is the user-role prompt sent into the harness's Execute loop.
	// Required and non-empty.
	Task string

	// Scope selects the named tool scope for this dispatch. Empty string
	// means "use the harness's default scope" (consolidation). Unknown scope
	// names are rejected before the dispatch runs. The Tools field, if set,
	// further narrows within the resolved scope.
	Scope string

	// Tools is the optional allowlist for this dispatch. nil or empty means
	// "use the full scope set". Names that don't match any tool in the
	// chosen scope surface as an error rather than silently dropping.
	Tools []string

	// Model selects the inference backend. Recognized enum values ("e4b",
	// "26b", "") route through the legacy local-LLM probe (see
	// resolveDispatchLocalModel). Any other value is treated as an explicit
	// model id requested by the caller (see RequestedModel) rather than
	// being silently collapsed to "e4b" — Normalize() preserves it.
	Model DispatchModel

	// RequestedModel is the caller's explicit model id, populated by
	// Normalize() whenever Model is not one of the recognized routing enum
	// values ("", "e4b", "26b"). Per issue #430: a model explicitly named by
	// the caller must win over the resolved provider's configured default
	// (ProviderConfig.Model) and must never be silently substituted. Empty
	// means the caller did not name a specific model — the resolved
	// provider's configured model applies as the default, unchanged from
	// prior behavior.
	RequestedModel string

	// Provider, when non-empty, names a provider declared in providers.yaml
	// or providers.local.yaml. The dispatcher resolves the name to a
	// concrete backend (URL, kind, model, api-key) via the wired
	// ProviderResolver. The provider's declared model is used as the
	// default; RequestedModel, when set, overrides it (see Normalize /
	// RequestedModel doc, RFC-0007, issue #430).
	Provider string

	// TimeoutSeconds is the per-dispatch wall-clock budget. Defaults to
	// dispatchTimeoutDefault (240s, see #432) when zero. Values above the
	// effective cap (TimeoutCapSeconds, or dispatchTimeoutCapDefault when
	// unset) are rejected at Normalize time with invalid_input — never
	// silently clamped.
	TimeoutSeconds int

	// TimeoutCapSeconds is the maximum TimeoutSeconds this dispatch accepts,
	// stamped from kernel config (dispatch_timeout_cap_seconds in
	// kernel.yaml, default 600) by the transport adapters (MCP tool, HTTP
	// handler) and re-stamped by the controller so the executing node's own
	// policy is what's ultimately enforced. Zero means "use the built-in
	// default cap" (dispatchTimeoutCapDefault). This is kernel policy, not a
	// caller parameter — boundary adapters overwrite any caller-supplied
	// value.
	TimeoutCapSeconds int

	// N controls the parallel fan-out. Clamped to [1,4]. Default 1. Each
	// dispatch in the batch gets its own Index, its own context, its own
	// deadline; failures in one do not abort siblings.
	N int

	// SystemPrompt overrides the harness's default system prompt for this
	// dispatch. Empty string keeps the harness default. Used by the output-
	// alignment layer to swap in role-specific prompts (validator, rewriter,
	// modality-matcher) without persistent config changes.
	SystemPrompt string

	// Thinking optionally overrides the harness's "think" flag. nil keeps
	// the harness default (false today, JSON-mode-friendly). Pass &true to
	// let the model emit a reasoning trace before answering.
	Thinking *bool

	// Temperature overrides the sampling temperature for this dispatch. nil
	// keeps the harness default (0.1). Per ADR-066 §models-always-swappable:
	// inference parameters must be caller-overridable, not hardcoded.
	Temperature *float64

	// MaxTokens overrides the completion token ceiling for this dispatch.
	// 0 (the zero value) keeps the harness default (localHarnessExecuteMaxToks=1024).
	// Per ADR-066 §models-always-swappable: callers must be able to widen
	// or narrow the window without a harness change.
	MaxTokens int

	// Identity propagates OIDC-shaped caller claims for trace metadata.
	// Optional; see DispatchIdentity for forward-compat notes.
	Identity DispatchIdentity

	// TargetNode, when non-empty, routes this dispatch to the named peer node
	// over the authenticated BEP channel instead of running locally. The
	// remote daemon receives a MessageTypeDispatch, executes the request
	// against its own harness, and returns a MessageTypeDispatchResult.
	// Requires cluster.enabled=true and the named peer to be connected.
	// Empty string (the default) runs locally — unchanged behavior.
	TargetNode string `json:"target_node,omitempty"`

	// Ambient opts this dispatch batch into the ambient-state-of-self context
	// block (cogdoc 16, drafted 2026-05-11): a prepended system-prompt block
	// carrying live kernel health, workspace, and identity state, built once
	// per batch by LocalHarnessController.buildAmbientBlock and injected into
	// every fan-out slot via the same AdditionalContext-prepend pattern the
	// PreInference hook uses on the main chat/external-CLI path
	// (harness/harness.go RunInference) — a pattern that path has had since
	// early on, but that LocalHarnessController.DispatchToHarness (the live
	// backer of cog_dispatch_to_harness) never received.
	//
	// Default false so every existing caller of DispatchToHarness is
	// byte-identical: the field defaults to the zero value, dispatchSlot's
	// ambientBlock is empty, and the system prompt is composed exactly as
	// before. Looped kernel-interior callers (ralph runners, RFC-036 seats)
	// are expected to set this explicitly; the default may flip once the
	// receipts-up half of the seam (myrgic/cogos#478) lands and the ambient
	// block earns its keep across the general dispatch population.
	Ambient bool `json:"ambient,omitempty"`
}

// DispatchToolCallSummary is the digest of one tool invocation made during a
// dispatch's Execute loop. Args and result are summarized to keep the result
// shape small (full transcripts are still recoverable via the kernel's
// ledger using the dispatch's cycle id).
type DispatchToolCallSummary struct {
	Name         string `json:"name"`
	ArgsDigest   string `json:"args_digest,omitempty"`   // first 200 chars of the JSON args
	ResultDigest string `json:"result_digest,omitempty"` // first 200 chars of the JSON result
	Error        string `json:"error,omitempty"`
}

// DispatchResult is one slot in the batch — the outcome of a single dispatch
// invocation. Index is its position in the batch (0..N-1).
type DispatchResult struct {
	Index     int                       `json:"index"`
	Success   bool                      `json:"success"`
	Content   string                    `json:"content,omitempty"`
	ToolCalls []DispatchToolCallSummary `json:"tool_calls,omitempty"`
	Error     string                    `json:"error,omitempty"`
	// Note carries per-slot informational warnings (e.g. the local-model
	// probe's fallback note: "configured local model X not loaded, using Y").
	// Distinct from Error — a populated Note on a success=true slot does NOT
	// indicate failure. Previously these warnings rode along in Error, which
	// made successful slots look failed to callers that only check that field.
	Note        string  `json:"note,omitempty"`
	DurationSec float64 `json:"duration_sec"`
	Turns       int     `json:"turns"`
	// ModelUsed reports the legacy routing enum ("e4b"/"26b") the local-LLM
	// probe path (Path 3) resolved to. Empty on every other routing path —
	// ServedModel is the canonical "what actually served" signal across all
	// paths (see below, issue #430).
	ModelUsed DispatchModel `json:"model_used,omitempty"`
	// ProviderUsed is the resolved provider name when DispatchRequest.Provider
	// fired, otherwise empty. Surfaced for observability so a caller can tell
	// which named provider handled the slot. Per RFC-0007 Layer 1.
	ProviderUsed string `json:"provider_used,omitempty"`
	// ServedModel is the concrete model id actually sent to the provider on
	// the wire for this slot (CompletionResponse.ProviderMeta.Model), whether
	// it came from the caller's RequestedModel, a config default, or the
	// legacy local-LLM probe. Populated on every successful slot regardless
	// of routing path. Per issue #430: the kernel trace/session record and
	// the dispatch result must both expose the served model, not just
	// ProviderUsed.
	ServedModel string `json:"served_model,omitempty"`
	// Degraded is true when the model returned no final text and the slot fell
	// back to summarizeToolTranscript. Success is still true — the tool loop
	// completed — but the output contract (ADR-eigen output-contract) was not met.
	Degraded bool `json:"degraded,omitempty"`
}

// ResolvedProvider is the materialized backend a ProviderResolver returns
// for a named provider lookup. Mirrors the four fields the harness needs
// at ExecuteScoped time. APIKey is pre-resolved from the provider's
// api_key_env at lookup time so the dispatcher does not have to know
// which env var any given provider uses.
type ResolvedProvider struct {
	BackendURL  string // e.g. http://127.0.0.1:1234 (no /v1 suffix; harness appends)
	BackendKind string // "openai" | "openai-compat"
	Model       string // e.g. google/gemma-4-26b-a4b
	APIKey      string // already materialized; empty when the provider has no api_key_env
}

// ProviderResolver translates a dispatch-time provider name into the
// concrete backend the harness needs. ok=false signals the name is not
// in the providers registry; the dispatcher surfaces that as
// invalid_input rather than silently falling through to the Model-based
// path. Implementations are typically thin wrappers over the live router
// config. See RFC-0007 Layer 1.
type ProviderResolver interface {
	ResolveDispatchProvider(name string) (ResolvedProvider, bool)
}

// DispatchBatchResult is the envelope returned to the caller. Results is
// always len(N), filled in dispatch index order, even when some slots
// failed. TotalDurationSec is wall-clock from dispatch start to last slot
// finishing.
type DispatchBatchResult struct {
	Results          []DispatchResult `json:"results"`
	TotalDurationSec float64          `json:"total_duration_sec"`
	// Notes carries batch-level diagnostic strings (e.g. state-routing path
	// taken). Per-slot warnings live in the individual Note fields.
	Notes []string `json:"notes,omitempty"`
}

// AgentDispatcher is the AgentController extension that surfaces the Phase 2
// transport. The interface lives separate from AgentController so older
// implementations can satisfy the latter without growing the dispatch
// surface. The MCP layer type-asserts to detect availability.
type AgentDispatcher interface {
	// DispatchToHarness runs the request as a fan-out batch and returns
	// once every slot has either completed, errored, or timed out. The
	// returned batch is always non-nil when the error is nil.
	DispatchToHarness(ctx context.Context, req DispatchRequest) (*DispatchBatchResult, error)
}

// RemoteDispatchRouter is satisfied by BEPEngine (Phase 2 S4). It is the
// narrow surface QueryDispatchToHarness uses to route a dispatch with a
// non-empty TargetNode over the authenticated BEP channel to the named peer.
// Separate from AgentDispatcher so the engine can implement it without
// becoming an AgentController.
type RemoteDispatchRouter interface {
	// RemoteDispatch sends req to the peer named targetNode and blocks until
	// the result arrives or ctx is cancelled. Returns the batch result or an
	// AgentControllerError describing the failure.
	RemoteDispatch(ctx context.Context, targetNode string, req DispatchRequest) (*DispatchBatchResult, error)
}
