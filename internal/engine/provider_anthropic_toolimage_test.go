package engine

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildAnthropicRequest_ToolResultCarriesImage(t *testing.T) {
	req := &CompletionRequest{Messages: []ProviderMessage{
		{Role: "user", Content: "look"},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "toolu_1", Name: "vision_analyze", Arguments: "{}"}}},
		{Role: "tool", ToolCallID: "toolu_1", Content: "Image loaded",
			ContentParts: []ContentPart{
				{Type: "text", Text: "Image loaded"},
				{Type: "image_url", ImageURL: "data:image/png;base64,AAAA"},
			}},
	}}
	ar := buildAnthropicRequest("claude-sonnet-5-5", req, false, 0)
	raw, err := json.Marshal(ar)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if !strings.Contains(s, `"type":"tool_result"`) || !strings.Contains(s, `"type":"image"`) ||
		!strings.Contains(s, `"media_type":"image/png"`) || !strings.Contains(s, `"data":"AAAA"`) {
		t.Fatalf("image missing from tool_result: %s", s)
	}
}

func TestBuildAnthropicRequest_ToolResultTextOnlyUnchanged(t *testing.T) {
	req := &CompletionRequest{Messages: []ProviderMessage{
		{Role: "user", Content: "x"},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "toolu_1", Name: "t", Arguments: "{}"}}},
		{Role: "tool", ToolCallID: "toolu_1", Content: "plain"},
	}}
	raw, _ := json.Marshal(buildAnthropicRequest("claude-sonnet-5-5", req, false, 0))
	if !strings.Contains(string(raw), `"content":"plain"`) {
		t.Fatalf("text tool_result changed: %s", raw)
	}
}
