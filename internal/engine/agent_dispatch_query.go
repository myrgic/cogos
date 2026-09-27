// agent_dispatch_query.go — validation, clamping, and orchestration glue for
// DispatchRequest. Pure functions over the AgentDispatcher contract; the
// concrete dispatcher lives in the root package because it needs *AgentHarness.
package engine

import (
	"context"
	"fmt"
	"strings"
)

// dispatchTimeoutDefault is the per-slot wall-clock budget when the caller
// passed 0 or omitted the field.
//
// #432 forensics: the prior value here was 30s, sized for the resident E4B's
// 10-turn worst case at ~3s/turn. That assumption broke once larger local
// models joined the resident mix (a 35B MoE, gemma under concurrent-slot
// prefill load per #430/#432) — real single-turn latency under load runs
// 2-4 minutes, so a 30s ceiling canceled every such request client-side while
// LM Studio kept generating server-side (non-streaming completions don't
// abort on client cancel — see CompleteCancelSafeIfSupported/provider_openai.go),
// and the retry that followed stacked a second zombie generation on top.
// 240s covers the observed 2-4 min realistic worst case with headroom while
// staying under the dispatch timeout cap (600s default, config-driven via
// dispatch_timeout_cap_seconds) and h.httpClient.Timeout (which
// providers.yaml should set to at least this value for local endpoints —
// see NewOpenAICompatProvider's timeout resolution).
const dispatchTimeoutDefault = 240

// dispatchTimeoutCapDefault is the per-slot budget ceiling applied when no
// dispatch_timeout_cap_seconds is configured in kernel.yaml. Aliases the
// exported config default so there is exactly one number; Normalize itself
// stays config-free — transport adapters stamp the configured cap onto
// DispatchRequest.TimeoutCapSeconds (see Config.DispatchTimeoutCap).
//
// Operator directive (2026-07-04): "expand the timeout caps to at least 5m.
// With agentic workflows that will likely get pushed even further out, so the
// timeout should be a parameter, not hardcoded." 600s covers cold-start load
// of larger local models plus multi-turn tool-loop dispatches; anything
// beyond it is an operator decision expressed in config, not a code change.
// The previous hardcoded caps (120s lifted from the legacy TriggerAgent wait
// limit, then 300s post-#432) both aged out the same way — a fixed number
// can't track the workload mix.
//
// Requests above the effective cap are REJECTED loudly at Normalize time
// (invalid_input) rather than silently clamped: a caller that asked for 20
// minutes and silently got 10 would misinterpret the resulting timeout as a
// task failure. Same fail-loud posture as #430's unservable-model handling.
const dispatchTimeoutCapDefault = DefaultDispatchTimeoutCapSeconds

// dispatchNDefault is the parallel fan-out when 0 is passed. Mirrors the
// "single dispatch" baseline so a minimal call shape behaves like a normal
// trigger.
const dispatchNDefault = 1

// dispatchNMax caps fan-out. The 48 GB VRAM box comfortably runs 4 concurrent
// E4B requests against the resident weights; tighter caps belong upstream.
const dispatchNMax = 4

// Normalize fills defaults, clamps ranges, and returns the first invalid-input
// error it finds (or nil). Mutates the receiver — callers pass by pointer.
func (r *DispatchRequest) Normalize() error {
	if r.AgentID == "" {
		r.AgentID = DefaultAgentID
	}
	if err := ValidateAgentID(r.AgentID); err != nil {
		return err
	}
	r.Task = strings.TrimSpace(r.Task)
	if r.Task == "" {
		return &AgentControllerError{Code: "invalid_input", Message: "task is required"}
	}
	r.Provider = strings.TrimSpace(r.Provider)
	// Provider unknown-name validation happens in the dispatcher: only it
	// holds the live ProviderResolver. Normalize merely trims and lets a
	// non-empty value through; the dispatcher fails fast on unknown names
	// before any slot starts.
	switch r.Model {
	case "":
		r.Model = DispatchModelE4B
	case DispatchModelE4B, DispatchModel26B:
		// keep — recognized legacy routing enum values.
	default:
		// Not a recognized routing enum value: treat it as an explicit
		// model id the caller is requesting (e.g. "example-35b"),
		// per issue #430. Preserve it in RequestedModel so the dispatcher
		// can honor it end-to-end instead of silently coercing to "e4b" —
		// the prior behavior masked the caller's intent before routing
		// ever saw it. Model itself still carries the raw string through
		// for Path 0's alias-table lookup (ResolveModelRequest); Path 1/2.5
		// consult RequestedModel directly.
		r.RequestedModel = string(r.Model)
	}
	if r.TimeoutSeconds <= 0 {
		r.TimeoutSeconds = dispatchTimeoutDefault
	}
	timeoutCap := r.TimeoutCapSeconds
	if timeoutCap <= 0 {
		timeoutCap = dispatchTimeoutCapDefault
	}
	if r.TimeoutSeconds > timeoutCap {
		return &AgentControllerError{
			Code: "invalid_input",
			Message: fmt.Sprintf(
				"timeout_seconds=%d exceeds the dispatch timeout cap (%ds); raise dispatch_timeout_cap_seconds in kernel.yaml or lower the request",
				r.TimeoutSeconds, timeoutCap),
		}
	}
	if r.N <= 0 {
		r.N = dispatchNDefault
	}
	if r.N > dispatchNMax {
		r.N = dispatchNMax
	}
	// Tools are validated against the live registry by the dispatcher
	// (only it knows what's registered). Trim/dedupe here to keep the
	// downstream adapter simple.
	if len(r.Tools) > 0 {
		r.Tools = dedupeStrings(r.Tools)
	}
	return nil
}

// QueryDispatchToHarness wraps an AgentDispatcher with normalization and the
// "controller installed?" check. Returns ErrAgentUnavailable when the
// controller is nil or doesn't implement AgentDispatcher.
//
// When req.TargetNode is non-empty and a RemoteDispatchRouter is wired (Phase 2
// S4), the request is forwarded to the named peer over the authenticated BEP
// channel instead of running locally. Gated on cluster.enabled: when no router
// is wired and TargetNode is set, the call fails fast with a clear error.
func QueryDispatchToHarness(ctx context.Context, ctrl AgentController, req DispatchRequest) (*DispatchBatchResult, error) {
	return QueryDispatchToHarnessRouted(ctx, ctrl, nil, req)
}

// QueryDispatchToHarnessRouted is the cluster-aware variant used by
// toolDispatchToHarness (MCPServer) and the HTTP serve_agents path. When
// router is non-nil and req.TargetNode is non-empty the dispatch is forwarded
// to the named peer over BEP; otherwise it behaves identically to
// QueryDispatchToHarness.
func QueryDispatchToHarnessRouted(ctx context.Context, ctrl AgentController, router RemoteDispatchRouter, req DispatchRequest) (*DispatchBatchResult, error) {
	// Remote path: TargetNode set + a live cluster router available.
	if req.TargetNode != "" {
		if router == nil {
			return nil, &AgentControllerError{
				Code:    "cluster_disabled",
				Message: fmt.Sprintf("target_node=%q requested but cluster transport is not running (cluster.enabled=false or BEP engine not started)", req.TargetNode),
			}
		}
		// Normalize before forwarding so the remote receives a clean request.
		if err := req.Normalize(); err != nil {
			return nil, err
		}
		return router.RemoteDispatch(ctx, req.TargetNode, req)
	}

	// Local path — unchanged from before.
	if ctrl == nil {
		return nil, ErrAgentUnavailable
	}
	disp, ok := ctrl.(AgentDispatcher)
	if !ok {
		return nil, &AgentControllerError{
			Code:    "unavailable",
			Message: fmt.Sprintf("agent %q does not support dispatch (controller missing AgentDispatcher)", req.AgentID),
		}
	}
	if err := req.Normalize(); err != nil {
		return nil, err
	}
	return disp.DispatchToHarness(ctx, req)
}

// dedupeStrings returns ss with empty strings removed and order-preserving
// dedupe. Allocates only when the input has duplicates or empties.
func dedupeStrings(ss []string) []string {
	if len(ss) == 0 {
		return ss
	}
	seen := make(map[string]struct{}, len(ss))
	out := ss[:0]
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
