package modality_test

import (
	"encoding/json"
	"testing"

	"github.com/myrgic/cogos/pkg/modality"
)

func TestChannelRegistry_RegisterAndGet(t *testing.T) {
	reg := modality.NewChannelRegistry()

	desc := &modality.ChannelDescriptor{
		ID:        "discord-text",
		Transport: "websocket-gateway",
		Input:     []modality.ModalityType{modality.Text},
		Output:    []modality.ModalityType{modality.Text, modality.Voice},
	}

	if err := reg.Register(desc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := reg.Get("discord-text")
	if !ok {
		t.Fatal("Get returned false")
	}
	if got.Transport != "websocket-gateway" {
		t.Errorf("Transport = %q, want %q", got.Transport, "websocket-gateway")
	}
}

func TestChannelRegistry_RegisterDuplicate(t *testing.T) {
	reg := modality.NewChannelRegistry()
	desc := &modality.ChannelDescriptor{ID: "ch1"}
	reg.Register(desc)
	if err := reg.Register(&modality.ChannelDescriptor{ID: "ch1"}); err == nil {
		t.Fatal("expected error on duplicate registration")
	}
}

func TestChannelRegistry_SessionBinding(t *testing.T) {
	reg := modality.NewChannelRegistry()

	desc := &modality.ChannelDescriptor{
		ID:     "ch-voice",
		Output: []modality.ModalityType{modality.Voice},
	}
	reg.Register(desc)

	// Bind to session.
	if err := reg.BindToSession("ch-voice", "session-1"); err != nil {
		t.Fatalf("BindToSession: %v", err)
	}

	// Check SupportsModality.
	matches := reg.SupportsModality("session-1", modality.Voice)
	if len(matches) != 1 {
		t.Fatalf("SupportsModality returned %d, want 1", len(matches))
	}

	// Text should not match.
	textMatches := reg.SupportsModality("session-1", modality.Text)
	if len(textMatches) != 0 {
		t.Errorf("SupportsModality(text) returned %d, want 0", len(textMatches))
	}

	// Unbind.
	if err := reg.UnbindFromSession("ch-voice", "session-1"); err != nil {
		t.Fatalf("UnbindFromSession: %v", err)
	}
	matches = reg.SupportsModality("session-1", modality.Voice)
	if len(matches) != 0 {
		t.Errorf("SupportsModality after unbind returned %d, want 0", len(matches))
	}
}

func TestChannelRegistry_Unregister(t *testing.T) {
	reg := modality.NewChannelRegistry()
	desc := &modality.ChannelDescriptor{ID: "ch1"}
	reg.Register(desc)
	reg.BindToSession("ch1", "s1")

	if err := reg.Unregister("ch1"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	if _, ok := reg.Get("ch1"); ok {
		t.Error("channel should be removed after Unregister")
	}
}

func TestChannelDescriptor_SupportsOutput(t *testing.T) {
	desc := &modality.ChannelDescriptor{
		Output: []modality.ModalityType{modality.Text, modality.Voice},
	}
	if !desc.SupportsOutput(modality.Voice) {
		t.Error("should support voice")
	}
	if desc.SupportsOutput(modality.Vision) {
		t.Error("should not support vision")
	}
}

func TestChannelRegistry_Snapshot(t *testing.T) {
	reg := modality.NewChannelRegistry()
	reg.Register(&modality.ChannelDescriptor{ID: "a"})
	reg.Register(&modality.ChannelDescriptor{ID: "b"})

	snap := reg.Snapshot()
	if len(snap) != 2 {
		t.Errorf("Snapshot has %d entries, want 2", len(snap))
	}
}

func TestChannelRegistry_ChannelsForSession(t *testing.T) {
	reg := modality.NewChannelRegistry()
	reg.Register(&modality.ChannelDescriptor{ID: "a"})
	reg.Register(&modality.ChannelDescriptor{ID: "b"})
	reg.BindToSession("a", "s1")
	reg.BindToSession("b", "s1")

	chs := reg.ChannelsForSession("s1")
	if len(chs) != 2 {
		t.Errorf("ChannelsForSession returned %d, want 2", len(chs))
	}
}

// ---------------------------------------------------------------------------
// Primitive 4: Mode + Pipeline fields
// ---------------------------------------------------------------------------

func TestChannelDescriptor_AmbientModeRoundTrip(t *testing.T) {
	// Construct a descriptor with Mode="ambient" and a Pipeline list.
	desc := &modality.ChannelDescriptor{
		ID:        "mic-ambient",
		Transport: "http",
		Mode:      "ambient",
		Pipeline:  []string{"denoise", "vad", "diarize", "ecapa_match", "stt", "attribute", "mention_detect", "emit"},
	}

	data, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got modality.ChannelDescriptor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.Mode != "ambient" {
		t.Errorf("Mode = %q, want %q", got.Mode, "ambient")
	}
	if len(got.Pipeline) != len(desc.Pipeline) {
		t.Fatalf("Pipeline len = %d, want %d", len(got.Pipeline), len(desc.Pipeline))
	}
	for i, stage := range desc.Pipeline {
		if got.Pipeline[i] != stage {
			t.Errorf("Pipeline[%d] = %q, want %q", i, got.Pipeline[i], stage)
		}
	}
}

func TestChannelDescriptor_IntentionalModeRoundTrip(t *testing.T) {
	// Explicit intentional mode round-trips correctly.
	desc := &modality.ChannelDescriptor{
		ID:       "mic-intentional",
		Mode:     "intentional",
		Pipeline: []string{"denoise", "vad", "stt", "emit"},
	}

	data, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got modality.ChannelDescriptor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Mode != "intentional" {
		t.Errorf("Mode = %q, want %q", got.Mode, "intentional")
	}
}

func TestChannelDescriptor_DefaultModeIsEmpty(t *testing.T) {
	// When Mode is not set, the JSON field is absent (omitempty) and the
	// zero value round-trips as empty string. Resolution to "intentional"
	// is the consumer's responsibility.
	desc := &modality.ChannelDescriptor{
		ID:        "default-mode",
		Transport: "stdio",
	}

	data, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// "mode" key must not appear in the JSON when Mode is empty.
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal raw: %v", err)
	}
	if _, present := raw["mode"]; present {
		t.Error("mode key should be absent from JSON when Mode is empty (omitempty)")
	}

	var got modality.ChannelDescriptor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Mode != "" {
		t.Errorf("Mode = %q, want empty string", got.Mode)
	}
	if len(got.Pipeline) != 0 {
		t.Errorf("Pipeline = %v, want empty", got.Pipeline)
	}
}
