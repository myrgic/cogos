// chunk.go — Turn-aware document chunking for CogOS v3
//
// Splits CogDoc content into chunks suitable for embedding. Conversations
// (ChatGPT exports, Claude sessions, etc.) are chunked by turn boundaries
// so that no chunk splits mid-turn. Non-conversation content falls back to
// paragraph/character-based chunking.
//
// The target chunk size is in characters (~500 tokens at 4 chars/token).
// A single oversized turn becomes its own chunk regardless of target.
package engine

import (
	"regexp"
	"strings"
)

// DefaultChunkSize is the target chunk size in characters (~500 tokens).
const DefaultChunkSize = 2000

// turnHeaderRe matches conversation turn markers in CogDoc format:
//
//	## [user]      ## [assistant]     ## [system]
//	## User        ## Assistant       ## System
//	**User:**      **Assistant:**     **user:**
//
// Also matches role markers from ChatGPT JSON exports rendered as markdown.
var turnHeaderRe = regexp.MustCompile(
	`(?m)^(?:#{1,3}\s*\[?\s*(?i:user|assistant|system|human|ai)\s*\]?\s*$|` +
		`\*\*(?i:user|assistant|system|human|ai)\s*:\*\*\s*)`,
)

// ChunkDocument splits a document body into chunks for embedding.
// It detects conversation format and uses turn-aware chunking when
// appropriate, falling back to paragraph-based chunking otherwise.
//
// The body should have frontmatter already stripped.
func ChunkDocument(body string, targetSize int) []string {
	if targetSize <= 0 {
		targetSize = DefaultChunkSize
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}

	if isConversation(body) {
		return chunkConversation(body, targetSize)
	}
	return chunkByParagraphs(body, targetSize)
}

// isConversation returns true if the text looks like a conversation transcript.
// Heuristic: at least 2 turn markers found in the first ~4000 characters.
func isConversation(text string) bool {
	sample := text
	if len(sample) > 4000 {
		sample = sample[:4000]
	}
	matches := turnHeaderRe.FindAllStringIndex(sample, -1)
	return len(matches) >= 2
}

// chunkConversation splits conversation text by turn boundaries, grouping
// consecutive turns into chunks that fit within targetSize. A single turn
// that exceeds targetSize becomes its own chunk (never split mid-turn).
func chunkConversation(text string, targetSize int) []string {
	turns := splitTurns(text)
	if len(turns) == 0 {
		// Fallback if splitting produced nothing useful.
		return chunkByParagraphs(text, targetSize)
	}
	return groupTurns(turns, targetSize)
}

// splitTurns splits text at turn boundaries, returning each turn as a
// trimmed string. Any preamble before the first turn marker is included
// as the first element if non-empty.
func splitTurns(text string) []string {
	locs := turnHeaderRe.FindAllStringIndex(text, -1)
	if len(locs) == 0 {
		return []string{strings.TrimSpace(text)}
	}

	var turns []string

	// Preamble before first turn marker (title, metadata, etc.)
	if locs[0][0] > 0 {
		pre := strings.TrimSpace(text[:locs[0][0]])
		if pre != "" {
			turns = append(turns, pre)
		}
	}

	// Each turn runs from its header to the start of the next header (or EOF).
	for i, loc := range locs {
		var end int
		if i+1 < len(locs) {
			end = locs[i+1][0]
		} else {
			end = len(text)
		}
		turn := strings.TrimSpace(text[loc[0]:end])
		if turn != "" {
			turns = append(turns, turn)
		}
	}

	return turns
}

// groupTurns packs turns into chunks that stay within targetSize.
// Turns are joined with a blank line separator. If a single turn
// exceeds targetSize, it becomes its own chunk without splitting.
func groupTurns(turns []string, targetSize int) []string {
	var chunks []string
	var current strings.Builder
	sep := "\n\n"

	for _, turn := range turns {
		turnLen := len(turn)

		if current.Len() == 0 {
			// Start a new chunk with this turn.
			current.WriteString(turn)
			continue
		}

		// Would adding this turn exceed the target?
		newLen := current.Len() + len(sep) + turnLen
		if newLen <= targetSize {
			current.WriteString(sep)
			current.WriteString(turn)
		} else {
			// Flush current chunk, start a new one.
			chunks = append(chunks, current.String())
			current.Reset()
			current.WriteString(turn)
		}
	}

	// Flush final chunk.
	if current.Len() > 0 {
		chunks = append(chunks, current.String())
	}

	return chunks
}

// chunkByParagraphs splits non-conversation text at paragraph boundaries
// (double newlines), grouping paragraphs to fill targetSize. Falls back
// to hard character splits only when a single paragraph exceeds the target.
func chunkByParagraphs(text string, targetSize int) []string {
	// Split on blank lines (one or more empty lines).
	paragraphs := splitParagraphs(text)
	if len(paragraphs) == 0 {
		return nil
	}

	var chunks []string
	var current strings.Builder
	sep := "\n\n"

	for _, para := range paragraphs {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}

		if current.Len() == 0 {
			current.WriteString(para)
			continue
		}

		newLen := current.Len() + len(sep) + len(para)
		if newLen <= targetSize {
			current.WriteString(sep)
			current.WriteString(para)
		} else {
			chunks = append(chunks, current.String())
			current.Reset()
			current.WriteString(para)
		}
	}

	if current.Len() > 0 {
		chunks = append(chunks, current.String())
	}

	return chunks
}

// splitParagraphs splits text on blank-line boundaries.
func splitParagraphs(text string) []string {
	// Normalize line endings.
	text = strings.ReplaceAll(text, "\r\n", "\n")
	// Split on one or more blank lines.
	re := regexp.MustCompile(`\n{2,}`)
	parts := re.Split(text, -1)
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
