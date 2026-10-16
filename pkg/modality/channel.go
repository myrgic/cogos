// channel.go — Channel capability declarations and registry.
//
// Each channel declares which modalities it can receive and deliver. The
// ChannelRegistry provides thread-safe lookup and session binding so the
// kernel can route output to every channel that supports a given modality.

package modality

import (
	"fmt"
	"sync"
)

// ChannelDescriptor declares a channel's identity and capabilities.
//
// Mode controls pipeline composition:
//
//   - "intentional" (default): session-scoped, explicit participation. The
//     user turns the mic on deliberately; the pipeline is mic → VAD → STT →
//     emit. This is the current default for all Claude Code / MCP channels.
//
//   - "ambient": always-on mic, VAD-gated attention, continuous diarization.
//     The pipeline is mic → VAD → diarize → ecapa_match → STT → attribute →
//     mention_detect → emit. Cog stays silent unless mentioned or there is
//     "appropriate silence." This is the multi-human-attendee mode (e.g. two people
//     sharing a hot mic where Cog listens and chimes in when addressed).
//
// Pipeline overrides the ordered stage list derived from Mode. When empty,
// the channel runtime uses DEFAULT_PIPELINES[Mode]. Each entry is a stage
// name; the runtime resolves each name to a registered PipelineStage
// implementation and skips (with a warning log) any name that is not yet
// registered, so ambient mode is safe to declare even before all its stages
// are implemented.
type ChannelDescriptor struct {
	ID         string         `json:"id"`                 // "discord-text", "claude-code", etc.
	Transport  string         `json:"transport"`          // "mcp", "http", "stdio"
	Input      []ModalityType `json:"input"`              // modalities this channel can receive
	Output     []ModalityType `json:"output"`             // modalities this channel can deliver
	SessionKey string         `json:"session_key"`        // pattern for session binding, e.g. "discord:{guild}:{channel}:{user}"
	// Mode governs pipeline composition. "intentional" = session-scoped,
	// explicit participation (default). "ambient" = always-on, VAD-gated
	// attention, continuous diarization. Empty string resolves to "intentional".
	Mode string `json:"mode,omitempty"` // "intentional" | "ambient"
	// Pipeline declares the ordered stage graph. Empty = use mode default.
	// Each entry is a stage name; the channel runtime resolves each name to
	// a registered PipelineStage implementation.
	Pipeline []string       `json:"pipeline,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"` // transport-specific data (guild ID, thread ID, etc.)
}

// SupportsOutput reports whether this channel can deliver the given modality.
func (d *ChannelDescriptor) SupportsOutput(m ModalityType) bool {
	for _, o := range d.Output {
		if o == m {
			return true
		}
	}
	return false
}

// ChannelAdapter is the interface a channel transport must implement.
type ChannelAdapter interface {
	// Connect joins a session, declaring supported modalities.
	Connect(sessionID string, descriptor *ChannelDescriptor) error

	// Disconnect leaves a session.
	Disconnect(sessionID string) error

	// HandleIncoming sends raw input from the channel to the kernel.
	HandleIncoming(channelID string, modality ModalityType, raw []byte) error

	// Deliver sends output from the kernel to the channel.
	Deliver(channelID string, output *EncodedOutput) error
}

// ChannelRegistry manages active channel connections. All methods are
// safe for concurrent use.
type ChannelRegistry struct {
	mu       sync.RWMutex
	channels map[string]*ChannelDescriptor // channelID -> descriptor
	sessions map[string][]string           // sessionID -> []channelID
}

// NewChannelRegistry creates an empty registry.
func NewChannelRegistry() *ChannelRegistry {
	return &ChannelRegistry{
		channels: make(map[string]*ChannelDescriptor),
		sessions: make(map[string][]string),
	}
}

// Register adds a channel descriptor. Returns an error if a channel
// with the same ID is already registered.
func (r *ChannelRegistry) Register(desc *ChannelDescriptor) error {
	if desc == nil {
		return fmt.Errorf("channel descriptor must not be nil")
	}
	if desc.ID == "" {
		return fmt.Errorf("channel descriptor ID must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.channels[desc.ID]; exists {
		return fmt.Errorf("channel %q already registered", desc.ID)
	}
	r.channels[desc.ID] = desc
	return nil
}

// Unregister removes a channel and unbinds it from all sessions.
func (r *ChannelRegistry) Unregister(channelID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.channels[channelID]; !exists {
		return fmt.Errorf("channel %q not found", channelID)
	}
	delete(r.channels, channelID)

	// Remove from all session bindings.
	for sid, cids := range r.sessions {
		for i, cid := range cids {
			if cid == channelID {
				r.sessions[sid] = append(cids[:i], cids[i+1:]...)
				break
			}
		}
	}
	return nil
}

// Get returns a channel descriptor by ID.
func (r *ChannelRegistry) Get(channelID string) (*ChannelDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	desc, ok := r.channels[channelID]
	return desc, ok
}

// ChannelsForSession returns all channel descriptors bound to a session.
func (r *ChannelRegistry) ChannelsForSession(sessionID string) []*ChannelDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cids := r.sessions[sessionID]
	out := make([]*ChannelDescriptor, 0, len(cids))
	for _, cid := range cids {
		if desc, ok := r.channels[cid]; ok {
			out = append(out, desc)
		}
	}
	return out
}

// BindToSession associates a channel with a session.
func (r *ChannelRegistry) BindToSession(channelID, sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.channels[channelID]; !ok {
		return fmt.Errorf("channel %q not registered", channelID)
	}

	// Avoid duplicate bindings.
	for _, cid := range r.sessions[sessionID] {
		if cid == channelID {
			return nil
		}
	}
	r.sessions[sessionID] = append(r.sessions[sessionID], channelID)
	return nil
}

// UnbindFromSession removes a channel from a session.
func (r *ChannelRegistry) UnbindFromSession(channelID, sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cids := r.sessions[sessionID]
	for i, cid := range cids {
		if cid == channelID {
			r.sessions[sessionID] = append(cids[:i], cids[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("channel %q not bound to session %q", channelID, sessionID)
}

// SupportsModality returns channels in a session that support a given
// modality for output. Used by the kernel to fan out responses.
func (r *ChannelRegistry) SupportsModality(sessionID string, modality ModalityType) []*ChannelDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cids := r.sessions[sessionID]
	var out []*ChannelDescriptor
	for _, cid := range cids {
		if desc, ok := r.channels[cid]; ok && desc.SupportsOutput(modality) {
			out = append(out, desc)
		}
	}
	return out
}

// Snapshot returns a copy of all registered channels, keyed by ID.
// Intended for the agent HUD and diagnostic views.
func (r *ChannelRegistry) Snapshot() map[string]*ChannelDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snap := make(map[string]*ChannelDescriptor, len(r.channels))
	for id, desc := range r.channels {
		snap[id] = desc
	}
	return snap
}
