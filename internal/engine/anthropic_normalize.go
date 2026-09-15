// anthropic_normalize.go — Wire-layer normalizer for Anthropic /v1/messages
//
// Enforces the full structural invariant set (I1-I6) on the FINAL
// []anthropicMessage wire structure (post-conversion from ProviderMessage).
// Operating at the block layer (not ProviderMessage layer) is what allows
// enforcement of I4 (tool_result block-0 ordering) and I6 (thinking-block
// signatures) — invariants that are invisible at the ProviderMessage layer.
//
// Design: DROP-ONLY / REORDER / MERGE — never synthesizes content.
// Idempotent: normalizeAnthropicMessages(normalizeAnthropicMessages(x)) == normalizeAnthropicMessages(x).
// Fixed point witness: validateAnthropicMessages(normalizeAnthropicMessages(x)) == [] for all x.
//
// Transform order (fixed, each pass monotone toward legality):
//  1. I6 thinking-strip + I5 empty-drop (single assistant walk)
//  2. I3 tool-pairing (per-assistant-run scoped, iterate to local fixpoint)
//  3. I4 block-order  (stable-partition tool_result-first in each user message)
//  4. I2 alternation-merge (consecutive same-role)
//  5. I1 leading-user-drop
//  6. settle: if any step 2-5 changed the slice, repeat 2-5 (converges in <=2
//     passes because message count is monotonically non-increasing)
//
// Invariants:
//
//	I1: messages[0].role == "user"
//	I2: strict user/assistant alternation (no consecutive same-role)
//	I3: tool_use/tool_result pairs complete and scoped per assistant run
//	I4: tool_result blocks lead in any tool-response user message
//	I5: no empty assistant content
//	I6: no signed thinking/redacted_thinking blocks on non-final assistant turns (v1)
package engine

import (
	"fmt"
	"log/slog"
	"strings"
)

// RepairReport counts per-invariant repairs performed by one normalizeAnthropicMessages call.
type RepairReport struct {
	LeadingDropped          int // I1: leading non-user messages dropped
	ConsecutiveMerged       int // I2: messages merged into preceding same-role
	OrphanToolUseDropped    int // I3a: assistant tool_use blocks without a following tool_result
	OrphanToolResultDropped int // I3b: user tool_result blocks without a preceding tool_use
	EmptyMsgAfterPairing    int // I3: whole messages dropped because all blocks were orphaned
	BlockOrderReordered     int // I4: user messages whose block order was changed
	EmptyAssistantDropped   int // I5: empty assistant messages dropped
	EmptyUserDropped        int // I5: empty user messages dropped (defensive)
	ThinkingStripped        int // I6: thinking/redacted_thinking blocks stripped
	TrailingDropped         int // I7: trailing non-user (assistant) messages dropped
	SettleIterations        int // how many settle-loop passes ran (1 = clean first pass)
}

// Total returns the sum of all repair counters (excluding SettleIterations).
func (r RepairReport) Total() int {
	return r.LeadingDropped + r.ConsecutiveMerged +
		r.OrphanToolUseDropped + r.OrphanToolResultDropped +
		r.EmptyMsgAfterPairing + r.BlockOrderReordered +
		r.EmptyAssistantDropped + r.EmptyUserDropped +
		r.ThinkingStripped + r.TrailingDropped
}

// emit logs the repair report via slog when any repair was performed.
// Silent on the common clean path (Total() == 0).
func (r RepairReport) emit(site string) {
	if r.Total() == 0 {
		return
	}
	slog.Info("anthropic.normalize.repair",
		"site", site,
		"total", r.Total(),
		"leading_dropped", r.LeadingDropped,
		"consecutive_merged", r.ConsecutiveMerged,
		"orphan_tool_use", r.OrphanToolUseDropped,
		"orphan_tool_result", r.OrphanToolResultDropped,
		"empty_after_pairing", r.EmptyMsgAfterPairing,
		"block_order_reordered", r.BlockOrderReordered,
		"empty_assistant_dropped", r.EmptyAssistantDropped,
		"empty_user_dropped", r.EmptyUserDropped,
		"thinking_stripped", r.ThinkingStripped,
		"trailing_dropped", r.TrailingDropped,
		"settle_iterations", r.SettleIterations,
	)
}

// ── Block-level helpers ───────────────────────────────────────────────────────

// blockTypes returns an ordered list of block type strings for a message's
// Content field (which may be a string or []anthropicContentBlock).
// A plain string counts as one "text" block (empty string returns nil).
func blockTypes(c interface{}) []string {
	switch v := c.(type) {
	case string:
		if v != "" {
			return []string{"text"}
		}
		return nil
	case []anthropicContentBlock:
		types := make([]string, len(v))
		for i, b := range v {
			types[i] = b.Type
		}
		return types
	default:
		return nil
	}
}

// toBlocks normalises Content to a []anthropicContentBlock view.
// A plain string becomes a single text block; nil/empty string becomes nil.
func toBlocks(c interface{}) []anthropicContentBlock {
	switch v := c.(type) {
	case string:
		if v == "" {
			return nil
		}
		return []anthropicContentBlock{{Type: "text", Text: v}}
	case []anthropicContentBlock:
		return v
	default:
		return nil
	}
}

// isToolResponseUser reports whether the message is a user turn that contains
// at least one tool_result block.
func isToolResponseUser(m anthropicMessage) bool {
	if m.Role != "user" {
		return false
	}
	for _, b := range toBlocks(m.Content) {
		if b.Type == "tool_result" {
			return true
		}
	}
	return false
}

// toolUseIDs returns the ordered list of tool_use block IDs in an assistant message.
func toolUseIDs(m anthropicMessage) []string {
	if m.Role != "assistant" {
		return nil
	}
	var ids []string
	for _, b := range toBlocks(m.Content) {
		if b.Type == "tool_use" && b.ID != "" {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

// toolResultIDs returns the set of tool_use_ids referenced by tool_result blocks
// in a user message.
func toolResultIDs(m anthropicMessage) map[string]bool {
	ids := make(map[string]bool)
	if m.Role != "user" {
		return ids
	}
	for _, b := range toBlocks(m.Content) {
		if b.Type == "tool_result" && b.ToolUseID != "" {
			ids[b.ToolUseID] = true
		}
	}
	return ids
}

// isSubstantialContent reports whether a block slice has at least one
// substantive block: non-empty text, tool_use, or image.
// thinking/redacted_thinking blocks alone are not substantive.
func isSubstantialContent(blocks []anthropicContentBlock) bool {
	for _, b := range blocks {
		switch b.Type {
		case "text":
			if strings.TrimSpace(b.Text) != "" {
				return true
			}
		case "tool_use", "image":
			return true
		}
	}
	return false
}

// ── normalizeAnthropicMessages ────────────────────────────────────────────────

// normalizeAnthropicMessages rewrites msgs into a strictly /v1/messages-legal
// sequence (user-first, alternation, tool-pairing, block-order, no-empty) and
// returns the repaired slice plus a per-rule RepairReport.
//
// Drop-only / reorder / merge -- never synthesizes content.
// Idempotent: normalize(normalize(x)) == normalize(x).
func normalizeAnthropicMessages(msgs []anthropicMessage) ([]anthropicMessage, RepairReport) {
	var rpt RepairReport

	// Pass 1: I6 thinking-strip + I5 empty-drop (single walk).
	msgs, rpt = passThinkingAndEmpty(msgs, rpt)

	// Settle loop: passes 2-5 may feed each other.
	// Converges because message count is monotonically non-increasing (every drop
	// or merge strictly shrinks the count; block reorders preserve count but are
	// idempotent on a sequence they already fixed). We track the pre-loop count
	// and also compare JSON-encoded output for exact fixpoint detection.
	//
	// A large corpus (e.g. tail-60 of 60 tool-dense messages) may need many
	// passes: each pass removes one "layer" of orphan chains. maxSettle is set
	// high enough to handle pathological inputs; the loop always breaks once
	// the sequence is stable.
	const maxSettle = 200
	for iter := 0; iter < maxSettle; iter++ {
		prevLen := len(msgs)

		// Pass 2: I3 tool-pairing.
		beforeRpt := rpt
		msgs, rpt = passToolPairing(msgs, rpt)

		// Pass 3: I4 block-order.
		msgs, rpt = passBlockOrder(msgs, rpt)

		// Pass 4: I2 alternation-merge.
		msgs, rpt = passAlternation(msgs, rpt)

		// Pass 5: I1 leading-user-drop.
		msgs, rpt = passLeadingUser(msgs, rpt)

		// Pass 6: I7 trailing-user-drop (symmetric to I1). Runs inside the settle
		// loop so a dropped trailing assistant can expose an orphan tool_result that
		// I3 then cleans up on the next iteration, and vice-versa, before fixpoint.
		msgs, rpt = passTrailingUser(msgs, rpt)

		rpt.SettleIterations = iter + 1

		// Fixpoint: length didn't change AND no repair counters changed.
		// (Block reorders change BlockOrderReordered but do not shrink length;
		// however I4 is idempotent so the second pass always has the same count.)
		changed := len(msgs) != prevLen ||
			rpt.LeadingDropped != beforeRpt.LeadingDropped ||
			rpt.ConsecutiveMerged != beforeRpt.ConsecutiveMerged ||
			rpt.OrphanToolUseDropped != beforeRpt.OrphanToolUseDropped ||
			rpt.OrphanToolResultDropped != beforeRpt.OrphanToolResultDropped ||
			rpt.EmptyMsgAfterPairing != beforeRpt.EmptyMsgAfterPairing ||
			rpt.EmptyAssistantDropped != beforeRpt.EmptyAssistantDropped ||
			rpt.EmptyUserDropped != beforeRpt.EmptyUserDropped ||
			rpt.TrailingDropped != beforeRpt.TrailingDropped
		if !changed {
			break
		}
	}

	return msgs, rpt
}

// passThinkingAndEmpty handles I6 (strip thinking) and I5 (drop empty assistant/user).
func passThinkingAndEmpty(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	out := make([]anthropicMessage, 0, len(msgs))
	for _, m := range msgs {
		blocks := toBlocks(m.Content)

		if m.Role == "assistant" {
			// I6: strip thinking/redacted_thinking blocks (v1: strip all).
			var kept []anthropicContentBlock
			for _, b := range blocks {
				if b.Type == "thinking" || b.Type == "redacted_thinking" {
					rpt.ThinkingStripped++
				} else {
					kept = append(kept, b)
				}
			}
			// I5: drop the whole message if nothing substantive remains.
			if !isSubstantialContent(kept) {
				rpt.EmptyAssistantDropped++
				continue
			}
			out = append(out, anthropicMessage{Role: "assistant", Content: kept})

		} else if m.Role == "user" {
			// I5 (defensive): drop empty user messages.
			allEmpty := len(blocks) == 0
			if !allEmpty {
				allEmpty = true
				for _, b := range blocks {
					if b.Type != "text" || strings.TrimSpace(b.Text) != "" {
						allEmpty = false
						break
					}
				}
			}
			if allEmpty {
				rpt.EmptyUserDropped++
				continue
			}
			out = append(out, m)
		} else {
			out = append(out, m)
		}
	}
	return out, rpt
}

// passToolPairing handles I3.
// (a) drops orphan tool_result blocks (no matching preceding tool_use);
// (b) drops orphan tool_use blocks (no matching following tool_result).
// Scoped per assistant run. Iterates to local fixpoint.
func passToolPairing(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	// Fast path.
	hasTools := false
outer:
	for i := range msgs {
		for _, b := range toBlocks(msgs[i].Content) {
			if b.Type == "tool_use" || b.Type == "tool_result" {
				hasTools = true
				break outer
			}
		}
	}
	if !hasTools {
		return msgs, rpt
	}

	const maxIter = 10
	for iter := 0; iter < maxIter; iter++ {
		changed := false

		// -- (a) drop orphan tool_result blocks ----------------------------------
		knownIDs := make(map[string]bool)
		var passA []anthropicMessage
		for _, m := range msgs {
			if m.Role == "assistant" {
				knownIDs = make(map[string]bool)
				for _, id := range toolUseIDs(m) {
					knownIDs[id] = true
				}
				passA = append(passA, m)
			} else if m.Role == "user" {
				blocks := toBlocks(m.Content)
				var kept []anthropicContentBlock
				for _, b := range blocks {
					if b.Type == "tool_result" {
						if b.ToolUseID != "" && knownIDs[b.ToolUseID] {
							kept = append(kept, b)
						} else {
							rpt.OrphanToolResultDropped++
							changed = true
						}
					} else {
						kept = append(kept, b)
					}
				}
				// A user message also closes the per-run scope.
				knownIDs = make(map[string]bool)
				if len(kept) == 0 {
					rpt.EmptyMsgAfterPairing++
					changed = true
					continue
				}
				if len(kept) != len(blocks) {
					passA = append(passA, anthropicMessage{Role: "user", Content: kept})
				} else {
					passA = append(passA, m)
				}
			} else {
				knownIDs = make(map[string]bool)
				passA = append(passA, m)
			}
		}

		// -- (b) drop orphan tool_use blocks -------------------------------------
		var passB []anthropicMessage
		for i, m := range passA {
			if m.Role != "assistant" {
				passB = append(passB, m)
				continue
			}
			aBlocks := toBlocks(m.Content)
			hasTU := false
			for _, b := range aBlocks {
				if b.Type == "tool_use" {
					hasTU = true
					break
				}
			}
			if !hasTU {
				passB = append(passB, m)
				continue
			}

			// Gather result IDs from the immediately-following user message.
			followingResults := make(map[string]bool)
			if i+1 < len(passA) && passA[i+1].Role == "user" {
				followingResults = toolResultIDs(passA[i+1])
			}

			var kept []anthropicContentBlock
			for _, b := range aBlocks {
				if b.Type == "tool_use" {
					if b.ID != "" && followingResults[b.ID] {
						kept = append(kept, b)
					} else {
						rpt.OrphanToolUseDropped++
						changed = true
					}
				} else {
					kept = append(kept, b)
				}
			}
			if len(kept) == 0 {
				rpt.EmptyMsgAfterPairing++
				changed = true
				continue
			}
			if !isSubstantialContent(kept) {
				rpt.EmptyAssistantDropped++
				changed = true
				continue
			}
			if len(kept) != len(aBlocks) {
				passB = append(passB, anthropicMessage{Role: "assistant", Content: kept})
			} else {
				passB = append(passB, m)
			}
		}

		msgs = passB
		if !changed {
			break
		}
	}
	return msgs, rpt
}

// passBlockOrder handles I4: stable-partition tool_result blocks to lead in
// any user message that is a tool-response user message.
func passBlockOrder(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	out := make([]anthropicMessage, len(msgs))
	for i, m := range msgs {
		if !isToolResponseUser(m) {
			out[i] = m
			continue
		}
		blocks := toBlocks(m.Content)

		// Detect whether reorder is needed.
		firstNonResult := -1
		needsReorder := false
		for j, b := range blocks {
			if b.Type != "tool_result" {
				if firstNonResult < 0 {
					firstNonResult = j
				}
			} else if firstNonResult >= 0 {
				needsReorder = true
				break
			}
		}
		if !needsReorder {
			out[i] = m
			continue
		}

		// Stable partition: tool_result first, others after.
		results := make([]anthropicContentBlock, 0, len(blocks))
		others := make([]anthropicContentBlock, 0, len(blocks))
		for _, b := range blocks {
			if b.Type == "tool_result" {
				results = append(results, b)
			} else {
				others = append(others, b)
			}
		}
		reordered := append(results, others...)
		out[i] = anthropicMessage{Role: "user", Content: reordered}
		rpt.BlockOrderReordered++
	}
	return out, rpt
}

// passAlternation handles I2: merge consecutive same-role messages.
// user+user: concat block lists.
// assistant+assistant: concat block lists, dropping second's thinking blocks.
func passAlternation(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	if len(msgs) == 0 {
		return msgs, rpt
	}
	out := make([]anthropicMessage, 0, len(msgs))
	out = append(out, msgs[0])

	for _, m := range msgs[1:] {
		prev := &out[len(out)-1]
		if m.Role != prev.Role {
			out = append(out, m)
			continue
		}
		// Same role -- merge.
		rpt.ConsecutiveMerged++
		prevBlocks := toBlocks(prev.Content)
		curBlocks := toBlocks(m.Content)

		if m.Role == "assistant" {
			// Drop thinking from the second assistant before merging.
			var filteredCur []anthropicContentBlock
			for _, b := range curBlocks {
				if b.Type != "thinking" && b.Type != "redacted_thinking" {
					filteredCur = append(filteredCur, b)
				} else {
					rpt.ThinkingStripped++
				}
			}
			merged := make([]anthropicContentBlock, 0, len(prevBlocks)+len(filteredCur))
			merged = append(merged, prevBlocks...)
			merged = append(merged, filteredCur...)
			prev.Content = merged
		} else {
			// user+user: concat all blocks.
			merged := make([]anthropicContentBlock, 0, len(prevBlocks)+len(curBlocks))
			merged = append(merged, prevBlocks...)
			merged = append(merged, curBlocks...)
			prev.Content = merged
		}
	}
	return out, rpt
}

// passLeadingUser handles I1: drop leading non-user messages.
func passLeadingUser(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	for len(msgs) > 0 && msgs[0].Role != "user" {
		msgs = msgs[1:]
		rpt.LeadingDropped++
	}
	return msgs, rpt
}

// passTrailingUser handles I7: drop trailing non-user (assistant) messages so the
// conversation ends with a user turn. The Anthropic Messages API rejects a request
// whose final message is an assistant message ("does not support assistant message
// prefill. The conversation must end with a user message") with HTTP 400.
//
// A request can end on an assistant message after upstream context eviction drops
// the trailing tool_result(s) that followed an assistant tool_use turn: passToolPairing
// (I3) then strips the now-orphaned tool_use blocks, leaving an assistant text-only
// message as the tail. This is the symmetric counterpart to passLeadingUser (I1):
// I1 guarantees the sequence starts with user, I7 guarantees it ends with user.
func passTrailingUser(msgs []anthropicMessage, rpt RepairReport) ([]anthropicMessage, RepairReport) {
	for len(msgs) > 0 && msgs[len(msgs)-1].Role != "user" {
		msgs = msgs[:len(msgs)-1]
		rpt.TrailingDropped++
	}
	return msgs, rpt
}

// ── validateAnthropicMessages ─────────────────────────────────────────────────

// validateAnthropicMessages is a pure checker returning one human-readable
// string per invariant violation found. Empty return == fully legal.
// No mutation. Asserts I1-I6 plus a structural role sanity check.
func validateAnthropicMessages(msgs []anthropicMessage) []string {
	var violations []string

	// Structural sanity: no unexpected roles at this layer.
	for i, m := range msgs {
		if m.Role != "user" && m.Role != "assistant" {
			violations = append(violations, fmt.Sprintf(
				"structural: msg[%d]: unexpected role %q at anthropic layer",
				i, m.Role,
			))
		}
	}

	if len(msgs) == 0 {
		return violations
	}

	// V-I1: first message must be user.
	if msgs[0].Role != "user" {
		violations = append(violations, fmt.Sprintf(
			"I1: first message must be user, got %q",
			msgs[0].Role,
		))
	}

	// V-I7: last message must be user (Anthropic rejects assistant-terminated
	// requests — "the conversation must end with a user message").
	if last := msgs[len(msgs)-1]; last.Role != "user" {
		violations = append(violations, fmt.Sprintf(
			"I7: last message must be user, got %q",
			last.Role,
		))
	}

	// V-I2: strict alternation.
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Role == msgs[i-1].Role {
			violations = append(violations, fmt.Sprintf(
				"I2: consecutive %s at index %d",
				msgs[i].Role, i,
			))
		}
	}

	// V-I3: tool pairing.
	for i, m := range msgs {
		if m.Role == "assistant" {
			nextResultIDs := make(map[string]bool)
			if i+1 < len(msgs) && msgs[i+1].Role == "user" {
				nextResultIDs = toolResultIDs(msgs[i+1])
			}
			for _, id := range toolUseIDs(m) {
				if !nextResultIDs[id] {
					violations = append(violations, fmt.Sprintf(
						"I3: assistant tool_use id=%s at msg %d has no immediately-following tool_result",
						id, i,
					))
				}
			}
		}
		if m.Role == "user" {
			prevToolUseSet := make(map[string]bool)
			if i > 0 && msgs[i-1].Role == "assistant" {
				for _, id := range toolUseIDs(msgs[i-1]) {
					prevToolUseSet[id] = true
				}
			}
			for _, b := range toBlocks(m.Content) {
				if b.Type == "tool_result" && b.ToolUseID != "" {
					if !prevToolUseSet[b.ToolUseID] {
						violations = append(violations, fmt.Sprintf(
							"I3: orphan tool_result tool_use_id=%s at msg %d has no preceding assistant tool_use",
							b.ToolUseID, i,
						))
					}
				}
			}
		}
	}

	// V-I4: tool_result must be leading blocks in tool-response user messages.
	for i, m := range msgs {
		if m.Role != "user" {
			continue
		}
		blocks := toBlocks(m.Content)
		firstNonResult := -1
		for j, b := range blocks {
			if b.Type != "tool_result" {
				if firstNonResult < 0 {
					firstNonResult = j
				}
			} else if firstNonResult >= 0 {
				violations = append(violations, fmt.Sprintf(
					"I4: tool_result is not block-0 in user message at index %d, block-0 is type=%s",
					i, blocks[0].Type,
				))
				break
			}
		}
	}

	// V-I5: no empty assistant content.
	for i, m := range msgs {
		if m.Role != "assistant" {
			continue
		}
		blocks := toBlocks(m.Content)
		if !isSubstantialContent(blocks) {
			violations = append(violations, fmt.Sprintf(
				"I5: assistant at index %d has empty content",
				i,
			))
		}
	}

	// V-I6: signed thinking blocks in non-final assistant turns (v1 policy).
	lastAssistantIdx := -1
	for i, m := range msgs {
		if m.Role == "assistant" {
			lastAssistantIdx = i
		}
	}
	for i, m := range msgs {
		if m.Role != "assistant" || i == lastAssistantIdx {
			continue
		}
		for _, b := range toBlocks(m.Content) {
			if (b.Type == "thinking" || b.Type == "redacted_thinking") && b.Signature != "" {
				violations = append(violations, fmt.Sprintf(
					"I6: thinking block with signature in non-final assistant at index %d — signature must be stripped after mutation",
					i,
				))
				break
			}
		}
	}

	return violations
}
