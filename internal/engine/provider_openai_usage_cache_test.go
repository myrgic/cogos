package engine

import "testing"

// OpenAI reports cached prompt tokens inside prompt_tokens and breaks the cached
// share out in prompt_tokens_details. TokenUsage.InputTokens is the uncached
// tail, so the cached share must land in CacheReadTokens, not be double counted.
func TestOpenAIUsage_CachedTokensSplitOut(t *testing.T) {
	cases := []struct {
		name string
		in   openaiUsageResponse
		want TokenUsage
	}{
		{"no details", openaiUsageResponse{PromptTokens: 5000, CompletionTokens: 50},
			TokenUsage{InputTokens: 5000, OutputTokens: 50}},
		{"cached share", openaiUsageResponse{PromptTokens: 5000, CompletionTokens: 50,
			PromptTokensDetails: &openaiPromptTokensDetails{CachedTokens: 4000}},
			TokenUsage{InputTokens: 1000, OutputTokens: 50, CacheReadTokens: 4000}},
		{"cached exceeds prompt clamps", openaiUsageResponse{PromptTokens: 10,
			PromptTokensDetails: &openaiPromptTokensDetails{CachedTokens: 12}},
			TokenUsage{InputTokens: 0, CacheReadTokens: 12}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.in.tokenUsage(); got != c.want {
				t.Fatalf("got %+v want %+v", got, c.want)
			}
		})
	}
}
