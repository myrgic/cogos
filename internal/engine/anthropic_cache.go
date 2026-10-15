package engine

import "strings"

// anthropic_cache.go — prompt-cache breakpoints for the Anthropic wire.
//
// Anthropic's prompt cache is opt-in per request: a block carrying
// `cache_control: {type: "ephemeral"}` marks a breakpoint, and every token
// before it is served as a cache read (~10% of input price) when the prefix
// matches a recent request. The kernel sent no breakpoints, so an agentic
// client replaying its full transcript every step paid cold price for the
// whole window each time (observed: 60K input/step, 0% cache hit, dsh
// session 3c189c37 — 2.85M input tokens across 63 steps).
//
// Placement rule (ADR-103 stability order, deferred TODO in
// context_assembly.go): mark the END of each stable region so the prefix up
// to it is reusable across steps.
//
//   1. last system block           — nucleus + client system, stable per session
//   2. last tool definition        — stable per session
//   3. the final content block of the SECOND-TO-LAST message — i.e. everything
//      the model has already seen this turn. The final message (the newest
//      tool_result or operator text) is the only cold region.
//
// Anthropic allows at most 4 breakpoints; we use 3. Breakpoints are applied
// AFTER normalizeAnthropicMessages, on the final block structure, so I2 merges
// and I4 reorders cannot move them.
//
// Lifetimes. A breakpoint lives 5 minutes by default; `ttl: "1h"` keeps it an
// hour. The STABLE prefix (system, tools, the OAuth-relocated system prefix)
// takes the lifetime resolved by resolveCacheTTL; the conversation-tail
// breakpoint (3) always keeps the default, because it moves every step and a
// longer lifetime on a region that is rewritten next turn buys nothing.
// Anthropic requires longer-lived breakpoints to precede shorter ones in
// prompt order (tools, system, messages); the placement above satisfies that.
//
// Cost model: a 1h write costs 2x base input once per hour of silence; a 5m
// write costs 1.25x after EVERY pause longer than five minutes, which is every
// human turn. Reads cost 0.1x either way. A human-paced client therefore pays
// less with 1h as soon as it pauses past five minutes twice an hour.

// anthropicCacheControl is the wire shape for a cache breakpoint.
type anthropicCacheControl struct {
	Type string `json:"type"`          // "ephemeral"
	TTL  string `json:"ttl,omitempty"` // "" (API default, 5m) or "1h"
}

// Cache lifetimes accepted by the policy. cacheTTL5m is the API default and is
// sent as an absent ttl field so the 5m wire shape is byte-identical to the
// pre-TTL kernel.
const (
	cacheTTL5m = "5m"
	cacheTTL1h = "1h"
)

// CacheTTLHeader is the per-request hint header ("5m" or "1h"). It wins over
// the provider default.
const CacheTTLHeader = "X-Cogos-Cache-Ttl"

// ephemeral is the default-lifetime (5m) breakpoint, used for the tail.
var ephemeral = &anthropicCacheControl{Type: "ephemeral"}

// ephemeral1h is the one-hour breakpoint for the stable prefix.
var ephemeral1h = &anthropicCacheControl{Type: "ephemeral", TTL: cacheTTL1h}

// normalizeCacheTTL maps a user-supplied lifetime to "5m", "1h" or "" (unset or
// unrecognized, so the next policy tier decides).
func normalizeCacheTTL(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case cacheTTL5m:
		return cacheTTL5m
	case cacheTTL1h:
		return cacheTTL1h
	}
	return ""
}

// resolveCacheTTL applies the lifetime policy for the stable prefix. No model
// id is consulted. Order: per-request hint, then the provider's configured
// default (ProviderConfig.CacheTTL), then fallback (the provider type's
// built-in default: "1h" for claude-oauth, whose clients are human-paced;
// "5m" for everything else).
func resolveCacheTTL(hint, providerDefault, fallback string) string {
	for _, v := range []string{hint, providerDefault, fallback} {
		if t := normalizeCacheTTL(v); t != "" {
			return t
		}
	}
	return cacheTTL5m
}

// stableCacheControl returns the breakpoint marker for the stable prefix at
// the given lifetime.
func stableCacheControl(ttl string) *anthropicCacheControl {
	if normalizeCacheTTL(ttl) == cacheTTL1h {
		return ephemeral1h
	}
	return ephemeral
}

// applyAnthropicCacheBreakpoints mutates payload in place. Idempotent; calling
// it again with a different ttl re-stamps the stable-prefix markers. ttl is the
// resolved lifetime of the stable prefix (see resolveCacheTTL).
func applyAnthropicCacheBreakpoints(payload *anthropicRequest, ttl string) {
	if payload == nil {
		return
	}
	stable := stableCacheControl(ttl)
	// 1. system: last block. The API-key path builds System as a plain
	// string; convert it to a single text block so it can carry the marker
	// (Anthropic accepts either form). The OAuth path already sends blocks.
	switch sys := payload.System.(type) {
	case string:
		if sys != "" {
			payload.System = []anthropicSystemBlock{{Type: "text", Text: sys, CacheControl: stable}}
		}
	case []anthropicSystemBlock:
		if len(sys) > 0 {
			sys[len(sys)-1].CacheControl = stable
			payload.System = sys
		}
	}
	// 2. tools: last definition.
	if n := len(payload.Tools); n > 0 {
		payload.Tools[n-1].CacheControl = stable
	}
	// 3. history: final block of the second-to-last message.
	if n := len(payload.Messages); n >= 2 {
		markLastBlock(&payload.Messages[n-2])
	}
}

// applyOAuthRelocatedCacheBreakpoint marks the OAuth-relocated system text as a
// breakpoint for single-turn requests.
//
// The OAuth path moves operator/agent system content into the first user turn
// (prependOAuthSystemToUserTurn). For a single-turn request that turn is the only
// message, so applyAnthropicCacheBreakpoints has no history mark and the relocated
// prefix was never cached. Here the relocated text is kept as its own leading
// content block (passAlternation concatenates blocks, so it stays block[0] after
// the I2 merge) and gets the breakpoint. The volatile user text sits after it.
//
// Scope: only when the request is exactly one message whose first block is the
// relocated text. Multi-message requests are left untouched. Total breakpoints stay
// at most 3 (system, tools, relocated). Idempotent. Call after
// applyAnthropicCacheBreakpoints.
func applyOAuthRelocatedCacheBreakpoint(payload *anthropicRequest, relocated, ttl string) {
	if payload == nil || relocated == "" || len(payload.Messages) != 1 {
		return
	}
	stable := stableCacheControl(ttl)
	m := &payload.Messages[0]
	if m.Role != "user" {
		return
	}
	switch c := m.Content.(type) {
	case string:
		if c != relocated {
			return
		}
		m.Content = []anthropicContentBlock{{Type: "text", Text: c, CacheControl: stable}}
	case []anthropicContentBlock:
		if len(c) == 0 || c[0].Type != "text" || c[0].Text != relocated {
			return
		}
		c[0].CacheControl = stable
	}
}

// markLastBlock attaches cache_control to the final content block of m,
// converting string content to a single text block if needed.
func markLastBlock(m *anthropicMessage) {
	switch c := m.Content.(type) {
	case string:
		if c == "" {
			return
		}
		m.Content = []anthropicContentBlock{{Type: "text", Text: c, CacheControl: ephemeral}}
	case []anthropicContentBlock:
		if len(c) == 0 {
			return
		}
		c[len(c)-1].CacheControl = ephemeral
		m.Content = c
	}
}
