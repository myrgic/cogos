// gen_ai_attrs.go — gen_ai.* OpenTelemetry semantic convention aliasing layer
// for the cogos kernel.
//
// Context: H1 of the mod3-pipecat-otel-batch (2026-05-16).
//
// The OTel GenAI semantic conventions (semconv v1.30.0) define a standard
// attribute namespace for LLM/AI system spans. This file provides a thin
// aliasing layer that:
//
//  1. Exports named attribute constructors for every gen_ai.* key the kernel
//     emits — callers never hard-code the raw string key.
//  2. Centralises the semconv import so the rest of the engine doesn't need to
//     import semconv directly.
//  3. Adds cogos-kernel–specific extensions (gen_ai.cogos.*) for substrate
//     fields that have no upstream semconv equivalent.
//
// Architectural note:
//
//	H1/H2/H3/H4 is one OTel convention applied at multiple call sites — not
//	four separate tracks. This file is the foundation; H2 wires it into
//	ProjectionReconciler spans; H3 adds the conversation/turn/service span
//	hierarchy; H4 wires ProjectionReconciler spans into that hierarchy.
//
// Wire compatibility:
//
//	These attributes are emitted on OpenTelemetry spans that may be exported
//	to Jaeger, Prometheus, or any OTel-compatible backend. The cogos.* extension
//	keys are namespaced to avoid collisions with future upstream GenAI semconv
//	additions.
//
// See: https://opentelemetry.io/docs/specs/semconv/gen-ai/ (v1.30.0)
package engine

import (
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

// ─── Standard gen_ai.* attribute constructors ────────────────────────────────
//
// These wrap the semconv key constants to provide typed, named constructors
// that read clearly at the call site. All names follow the pattern
// GenAI<PascalCaseFieldName>(value).

// GenAISystem returns the gen_ai.system attribute identifying the AI provider.
// Values: "anthropic", "openai", "gemma", "ollama", etc.
func GenAISystem(system string) attribute.KeyValue {
	return semconv.GenAISystemKey.String(system)
}

// GenAIOperationName returns the gen_ai.operation.name attribute.
// Values: "chat", "text_completion", "embeddings", "reconcile", etc.
func GenAIOperationName(name string) attribute.KeyValue {
	return semconv.GenAIOperationNameKey.String(name)
}

// GenAIRequestModel returns the gen_ai.request.model attribute.
// The model identifier as requested by the caller (may differ from response model).
func GenAIRequestModel(model string) attribute.KeyValue {
	return semconv.GenAIRequestModelKey.String(model)
}

// GenAIResponseModel returns the gen_ai.response.model attribute.
// The model identifier as reported in the response (may include version suffix).
func GenAIResponseModel(model string) attribute.KeyValue {
	return semconv.GenAIResponseModelKey.String(model)
}

// GenAIResponseID returns the gen_ai.response.id attribute.
// Opaque response identifier from the provider.
func GenAIResponseID(id string) attribute.KeyValue {
	return semconv.GenAIResponseIDKey.String(id)
}

// GenAIUsageInputTokens returns the gen_ai.usage.input_tokens attribute.
func GenAIUsageInputTokens(n int) attribute.KeyValue {
	return semconv.GenAIUsageInputTokensKey.Int(n)
}

// GenAIUsageOutputTokens returns the gen_ai.usage.output_tokens attribute.
func GenAIUsageOutputTokens(n int) attribute.KeyValue {
	return semconv.GenAIUsageOutputTokensKey.Int(n)
}

// GenAIRequestMaxTokens returns the gen_ai.request.max_tokens attribute.
func GenAIRequestMaxTokens(n int) attribute.KeyValue {
	return semconv.GenAIRequestMaxTokensKey.Int(n)
}

// GenAIRequestTemperature returns the gen_ai.request.temperature attribute.
func GenAIRequestTemperature(t float64) attribute.KeyValue {
	return semconv.GenAIRequestTemperatureKey.Float64(t)
}

// GenAIRequestTopP returns the gen_ai.request.top_p attribute.
func GenAIRequestTopP(p float64) attribute.KeyValue {
	return semconv.GenAIRequestTopPKey.Float64(p)
}

// GenAIResponseFinishReasons returns the gen_ai.response.finish_reasons attribute.
// reasons is a slice of finish-reason strings (e.g. ["stop"], ["end_turn"]).
func GenAIResponseFinishReasons(reasons []string) attribute.KeyValue {
	return semconv.GenAIResponseFinishReasonsKey.StringSlice(reasons)
}

// ─── cogos-kernel extension attributes (gen_ai.cogos.*) ──────────────────────
//
// Fields with no upstream semconv equivalent. Namespaced under gen_ai.cogos.*
// to (a) signal they are cogos-specific and (b) stay in the gen_ai.* namespace
// so they group naturally with the standard attrs in OTel UIs.

const (
	// cogosSessionIDKey is the cogos session identifier that scopes this AI operation.
	cogosSessionIDKey = attribute.Key("gen_ai.cogos.session_id")

	// cogosOperationPhaseKey identifies the processing phase within a session turn.
	// Values: "prompt_eval", "thinking", "answer", "tool_call", "reconcile".
	cogosOperationPhaseKey = attribute.Key("gen_ai.cogos.operation.phase")

	// cogosProjectionKindKey identifies the projection type for reconcile operations.
	// Values: "bibliography", "lineage-chain", "convergence-map", etc.
	cogosProjectionKindKey = attribute.Key("gen_ai.cogos.projection.kind")

	// cogosProjectionNodeCountKey is the number of lineage nodes processed.
	cogosProjectionNodeCountKey = attribute.Key("gen_ai.cogos.projection.node_count")

	// cogosWorkspaceRootKey is the workspace root path for this operation.
	cogosWorkspaceRootKey = attribute.Key("gen_ai.cogos.workspace_root")

	// cogosSpanKindKey classifies the span within the conversation hierarchy.
	// Values: "conversation", "turn", "service".
	cogosSpanKindKey = attribute.Key("gen_ai.cogos.span.kind")
)

// CogosSessionID returns the gen_ai.cogos.session_id attribute.
func CogosSessionID(sessionID string) attribute.KeyValue {
	return cogosSessionIDKey.String(sessionID)
}

// CogosOperationPhase returns the gen_ai.cogos.operation.phase attribute.
func CogosOperationPhase(phase string) attribute.KeyValue {
	return cogosOperationPhaseKey.String(phase)
}

// CogosProjectionKind returns the gen_ai.cogos.projection.kind attribute.
func CogosProjectionKind(kind string) attribute.KeyValue {
	return cogosProjectionKindKey.String(kind)
}

// CogosProjectionNodeCount returns the gen_ai.cogos.projection.node_count attribute.
func CogosProjectionNodeCount(n int) attribute.KeyValue {
	return cogosProjectionNodeCountKey.Int(n)
}

// CogosWorkspaceRoot returns the gen_ai.cogos.workspace_root attribute.
func CogosWorkspaceRoot(root string) attribute.KeyValue {
	return cogosWorkspaceRootKey.String(root)
}

// CogosSpanKind returns the gen_ai.cogos.span.kind attribute.
// Values: "conversation", "turn", "service".
func CogosSpanKind(kind string) attribute.KeyValue {
	return cogosSpanKindKey.String(kind)
}

// ─── Span kind constants ──────────────────────────────────────────────────────
//
// These are the valid values for CogosSpanKind, matching the H3 span hierarchy.
// The conversation > turn > service nesting mirrors Pipecat's GenAI span shape.

const (
	// SpanKindConversation is the root span for a complete conversation session.
	SpanKindConversation = "conversation"
	// SpanKindTurn is a child of conversation — one user-turn + assistant-response.
	SpanKindTurn = "turn"
	// SpanKindService is a child of turn — one service call (LLM, STT, TTS, reconcile).
	SpanKindService = "service"
)
