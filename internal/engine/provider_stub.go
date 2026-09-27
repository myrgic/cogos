// provider_stub.go — StubProvider for testing
//
// In-memory provider with configurable responses, error injection, and
// latency simulation. Used in unit tests and for offline development.
package engine

import (
	"context"
	"time"
)

// StubProvider is an in-memory Provider for testing.
type StubProvider struct {
	name  string
	model string // reported by Model(); empty by default
	// endpoint is reported by Endpoint(). Loopback by default, matching the
	// default IsLocal=true: an in-memory stub runs on this machine.
	endpoint string
	response string
	// usage, when set, is reported on Complete() and on the final Stream chunk
	// so tests can prove the kernel forwards provider accounting (incl. cache).
	usage        *TokenUsage
	err          error
	latency      time.Duration
	available    bool
	capabilities ProviderCapabilities
	chunks       []string // if set, Stream sends these chunks instead of response
	// toolCalls, when non-empty, is returned on Complete() so tests can
	// exercise the tool-calls-as-response-content path used by BrowserOS
	// passthrough. Stream() attaches them to the Done chunk via the
	// StreamChunk.ToolCallDelta channel (sent eagerly before Done).
	toolCalls []ToolCall
	// lastRequest captures the most recent CompletionRequest so tests can
	// assert on what the handler actually passed down to the provider
	// (e.g. that ExternalTools was partitioned correctly).
	lastRequest *CompletionRequest
}

// NewStubProvider creates a StubProvider that returns the given response.
func NewStubProvider(name, response string) *StubProvider {
	return &StubProvider{
		name:      name,
		endpoint:  "http://127.0.0.1",
		response:  response,
		available: true,
		capabilities: ProviderCapabilities{
			Capabilities:     []Capability{CapStreaming, CapToolUse, CapVision, CapJSON},
			MaxContextTokens: 128000,
			MaxOutputTokens:  4096,
			IsLocal:          true,
		},
	}
}

func (s *StubProvider) Name() string                       { return s.name }
func (s *StubProvider) Model() string                      { return s.model }
func (s *StubProvider) Endpoint() string                   { return s.endpoint }
func (s *StubProvider) Available(_ context.Context) bool   { return s.available }
func (s *StubProvider) Capabilities() ProviderCapabilities { return s.capabilities }

func (s *StubProvider) Ping(_ context.Context) (time.Duration, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.latency, nil
}

func (s *StubProvider) Complete(_ context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	s.lastRequest = req
	if s.err != nil {
		return nil, s.err
	}
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	stop := "end_turn"
	if len(s.toolCalls) > 0 {
		stop = "tool_use"
	}
	resp := &CompletionResponse{
		Content:    s.response,
		ToolCalls:  s.toolCalls,
		StopReason: stop,
		ProviderMeta: ProviderMeta{
			Provider: s.name,
			Model:    "stub",
		},
	}
	if s.usage != nil {
		resp.Usage = *s.usage
	}
	return resp, nil
}

// WithUsage makes the stub report the given accounting on both paths.
func (s *StubProvider) WithUsage(u TokenUsage) *StubProvider { s.usage = &u; return s }

func (s *StubProvider) Stream(_ context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	s.lastRequest = req
	if s.err != nil {
		return nil, s.err
	}
	chunks := s.chunks
	if len(chunks) == 0 {
		chunks = []string{s.response}
	}
	extra := 1
	if len(s.toolCalls) > 0 {
		extra += len(s.toolCalls)
	}
	ch := make(chan StreamChunk, len(chunks)+extra)
	for _, c := range chunks {
		ch <- StreamChunk{Delta: c}
	}
	// Emit any stub-configured tool calls as ToolCallDelta events. Each
	// call is emitted as a single delta carrying the full ID/Name/args
	// — StreamChunk doesn't distinguish multi-part deltas from atomic
	// ones; serve.go's tool_call forwarder is tolerant of both.
	for i, tc := range s.toolCalls {
		ch <- StreamChunk{
			ToolCallDelta: &ToolCallDelta{
				Index:     i,
				ID:        tc.ID,
				Name:      tc.Name,
				ArgsDelta: tc.Arguments,
			},
		}
	}
	stop := ""
	if len(s.toolCalls) > 0 {
		stop = "tool_use"
	}
	ch <- StreamChunk{
		Done:         true,
		StopReason:   stop,
		Usage:        s.usage,
		ProviderMeta: &ProviderMeta{Provider: s.name, Model: "stub"},
	}
	close(ch)
	return ch, nil
}
