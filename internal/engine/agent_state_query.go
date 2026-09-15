// agent_state_query.go — query helpers for the agent-state API.
//
// Pure functions that operate on the AgentController interface. Keeping
// them in a separate file makes it clear what logic lives in the engine
// layer (validation, clamping, tool/HTTP marshaling) versus the root
// package's adapter (which actually reads *ServeAgent state).
package engine

import (
	"context"
	"fmt"
	"regexp"
)

// agentIDRegex matches the accepted agent_id format: lowercase start,
// alphanumeric/dash/underscore, up to 64 chars. Today only "primary" is
// valid; the regex is forward-compatible for future handles like
// "digestion" or "identity-cog".
var agentIDRegex = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// DefaultAgentID is the stable handle for the singleton ServeAgent. The
// API pluralises from day one (§2.5 of Agent T's spec); callers that
// omit agent_id should default to this value.
const DefaultAgentID = "primary"

// ValidateAgentID checks the agent_id against agentIDRegex. Returns
// nil when valid, ErrAgentInvalidInput wrapped with the reason
// otherwise. Empty id is treated as invalid — callers must substitute
// DefaultAgentID before validating.
func ValidateAgentID(id string) error {
	if id == "" {
		return &AgentControllerError{Code: "invalid_input", Message: "agent_id is required"}
	}
	if !agentIDRegex.MatchString(id) {
		return &AgentControllerError{Code: "invalid_input", Message: fmt.Sprintf("invalid agent_id: %q (must match [a-z][a-z0-9_-]{0,63})", id)}
	}
	return nil
}

// ClampTraceLimit enforces the [1, 20] range on trace_limit. Returns the
// clamped value and an error when the input was clearly out of range
// (>20 or <0). A value of 0 is treated as "use the default of 1".
func ClampTraceLimit(n int) (int, error) {
	if n < 0 {
		return 0, &AgentControllerError{Code: "invalid_input", Message: fmt.Sprintf("trace_limit must be >= 0 (got %d)", n)}
	}
	if n > 20 {
		return 0, &AgentControllerError{Code: "invalid_input", Message: fmt.Sprintf("trace_limit must be <= 20 (got %d)", n)}
	}
	if n == 0 {
		return 1, nil
	}
	return n, nil
}

// ListAgentsRequest is the normalized input to the list-agents helper.
type ListAgentsRequest struct {
	IncludeStopped bool
}

// ListAgentsResponse is the normalized output.
type ListAgentsResponse struct {
	Count  int            `json:"count"`
	Agents []AgentSummary `json:"agents"`
}

// QueryListAgents wraps AgentController.ListAgents with uniform error
// handling + an envelope shape suitable for direct JSON marshal.
func QueryListAgents(ctx context.Context, ctrl AgentController, req ListAgentsRequest) (*ListAgentsResponse, error) {
	if ctrl == nil {
		return nil, ErrAgentUnavailable
	}
	agents, err := ctrl.ListAgents(ctx, req.IncludeStopped)
	if err != nil {
		return nil, err
	}
	if agents == nil {
		agents = []AgentSummary{}
	}
	return &ListAgentsResponse{
		Count:  len(agents),
		Agents: agents,
	}, nil
}

// GetAgentRequest is the normalized input to the get-agent helper.
type GetAgentRequest struct {
	AgentID      string
	IncludeTrace bool
	TraceLimit   int
}

// Normalize fills in defaults and validates the request. Returns the
// original ErrAgentInvalidInput for obvious malformed input, else nil.
func (r *GetAgentRequest) Normalize() error {
	if r.AgentID == "" {
		r.AgentID = DefaultAgentID
	}
	if err := ValidateAgentID(r.AgentID); err != nil {
		return err
	}
	if r.IncludeTrace {
		clamped, err := ClampTraceLimit(r.TraceLimit)
		if err != nil {
			return err
		}
		r.TraceLimit = clamped
	} else {
		// When include_trace is false, trace_limit is ignored — but
		// don't let a bad value pollute the response.
		if r.TraceLimit != 0 {
			r.TraceLimit = 0
		}
	}
	return nil
}

// QueryGetAgent wraps AgentController.GetAgent with normalization.
func QueryGetAgent(ctx context.Context, ctrl AgentController, req GetAgentRequest) (*AgentSnapshot, error) {
	if ctrl == nil {
		return nil, ErrAgentUnavailable
	}
	if err := req.Normalize(); err != nil {
		return nil, err
	}
	return ctrl.GetAgent(ctx, req.AgentID, req.IncludeTrace, req.TraceLimit)
}

// TriggerAgentRequest is the normalized input to the trigger-agent helper.
type TriggerAgentRequest struct {
	AgentID string
	Reason  string
	Wait    bool
}

// Normalize fills defaults and validates.
func (r *TriggerAgentRequest) Normalize() error {
	if r.AgentID == "" {
		r.AgentID = DefaultAgentID
	}
	return ValidateAgentID(r.AgentID)
}

// QueryTriggerAgent wraps AgentController.TriggerAgent with normalization.
func QueryTriggerAgent(ctx context.Context, ctrl AgentController, req TriggerAgentRequest) (*AgentTriggerResult, error) {
	if ctrl == nil {
		return nil, ErrAgentUnavailable
	}
	if err := req.Normalize(); err != nil {
		return nil, err
	}
	return ctrl.TriggerAgent(ctx, req.AgentID, req.Reason, req.Wait)
}
