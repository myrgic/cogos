// memory_transcripts.go — transcript-side fusion for GET /memory/search
// (myrgic/cogos#650).
//
// /memory/search used to search only the cogdoc corpus. The Conversations
// Observatory index holds every ingested turn, but it lives in the
// internal/conversations package, which this package must not import
// (package-boundary guard, see cogdoc_service.go). So the observatory is
// reached through a hook, ConversationSearcher, set from
// internal/providers/all.RegisterConversations, the same shape as
// SetConversationsResolver.
//
// Fusion rules:
//   - Curated outranks raw. Cogdoc hits come first, in their existing
//     relevance order. Transcript hits follow. A fixed share of the result
//     window (transcriptReserveDivisor) is reserved for transcripts, so a
//     flood of cogdoc hits cannot hide the transcript side entirely.
//   - Transcript hits are ordered newest first, not by session-id order.
//   - Fail loud. The response's "sources" map reports each side as ok,
//     unavailable, error, or excluded. A caller can always tell a narrow
//     result set from a broken transcript side.
package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// TranscriptHit is one observatory turn matched by a transcript search.
type TranscriptHit struct {
	URI          string    // cog:conversations/... URI that resolves the turn
	SessionID    string    //
	TurnIndex    int       //
	Role         string    //
	Excerpt      string    //
	Source       string    // observer source id, empty for CC sessions
	SessionTitle string    //
	Timestamp    time.Time //
}

// ConversationSearcher is the hook the conversations provider implements.
// SearchTranscripts returns at most limit matches, newest first. limit 0 means
// no cap. It returns an error
// when the observatory index is not loaded, so the caller can report the
// transcript side as unavailable instead of an empty list.
type ConversationSearcher interface {
	SearchTranscripts(query string, limit int) ([]TranscriptHit, error)
}

// conversationSearcher is the kernel-global searcher. Nil until wired at boot.
var conversationSearcher ConversationSearcher

// SetConversationSearcher wires the transcript searcher. Call once at boot from
// internal/providers/all.RegisterConversations.
func SetConversationSearcher(s ConversationSearcher) {
	conversationSearcher = s
}

// WiredConversationSearcher returns the wired searcher, or nil. Exposed so
// binary-assembly regression tests can assert the wiring reaches the engine.
func WiredConversationSearcher() ConversationSearcher {
	return conversationSearcher
}

const (
	memSearchKindCogdoc     = "cogdoc"
	memSearchKindTranscript = "transcript"

	// transcriptReserveDivisor: when transcripts match, they hold limit/divisor
	// slots of the result window (at least 1, capped by the match count).
	transcriptReserveDivisor = 4
)

// transcriptReserve returns how many window slots are held for transcripts
// when there are transcriptMatches to show. With no transcript matches the
// reserve is 0, so cogdoc results are never narrowed for nothing.
func transcriptReserve(limit, transcriptMatches int) int {
	reserve := limit / transcriptReserveDivisor
	if reserve < 1 {
		reserve = 1
	}
	if transcriptMatches < reserve {
		reserve = transcriptMatches
	}
	if reserve > limit {
		reserve = limit
	}
	return reserve
}

// parseMemorySearchSources reads the optional sources= query parameter.
// "" or "all" includes both sides. "cogdoc" is the opt-out: cogdoc-only
// results, with the transcript side reported as excluded.
func parseMemorySearchSources(raw string) (includeTranscripts bool, err error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "all", "cogdoc,transcript", "transcript,cogdoc":
		return true, nil
	case "cogdoc":
		return false, nil
	}
	return false, fmt.Errorf("invalid sources %q: want \"cogdoc\" or \"all\"", raw)
}

// fuseMemorySearch merges the cogdoc output of SearchMemory with transcript
// hits from the wired searcher. It is pure apart from the searcher call.
func fuseMemorySearch(query string, cogOut any, limit int, includeTranscripts bool) map[string]any {
	var cogResults []map[string]any
	if m, ok := cogOut.(map[string]any); ok {
		cogResults, _ = m["results"].([]map[string]any)
	}
	for _, r := range cogResults {
		r["kind"] = memSearchKindCogdoc
	}

	sources := map[string]any{"cogdoc": "ok"}
	var transcriptResults []map[string]any
	transcriptMatches := 0

	switch {
	case !includeTranscripts:
		sources["transcripts"] = "excluded"
	default:
		sr := WiredConversationSearcher()
		if sr == nil {
			sources["transcripts"] = "unavailable"
			break
		}
		hits, err := sr.SearchTranscripts(query, limit)
		if err != nil {
			sources["transcripts"] = "unavailable"
			sources["transcripts_error"] = err.Error()
			break
		}
		sources["transcripts"] = "ok"
		transcriptMatches = len(hits)

		// Reserve window slots for transcripts so cogdoc hits cannot fill
		// the whole window. The reserve is 0 when nothing matched.
		if c := limit - transcriptReserve(limit, len(hits)); len(cogResults) > c {
			cogResults = cogResults[:c]
		}

		// Newest first. Ties fall back to session and turn for a stable order.
		sort.SliceStable(hits, func(i, j int) bool {
			if !hits[i].Timestamp.Equal(hits[j].Timestamp) {
				return hits[i].Timestamp.After(hits[j].Timestamp)
			}
			if hits[i].SessionID != hits[j].SessionID {
				return hits[i].SessionID < hits[j].SessionID
			}
			return hits[i].TurnIndex < hits[j].TurnIndex
		})

		slots := limit - len(cogResults)
		if slots > len(hits) {
			slots = len(hits)
		}
		for _, h := range hits[:max(slots, 0)] {
			transcriptResults = append(transcriptResults, map[string]any{
				"kind":          memSearchKindTranscript,
				"uri":           h.URI,
				"session_id":    h.SessionID,
				"turn_index":    h.TurnIndex,
				"role":          h.Role,
				"source":        h.Source,
				"session_title": h.SessionTitle,
				"timestamp":     h.Timestamp.Format(time.RFC3339),
				"excerpt":       h.Excerpt,
			})
		}
	}

	results := make([]map[string]any, 0, len(cogResults)+len(transcriptResults))
	results = append(results, cogResults...)
	results = append(results, transcriptResults...)

	return map[string]any{
		"query":              query,
		"count":              len(results),
		"results":            results,
		"sources":            sources,
		"transcript_matches": transcriptMatches,
	}
}
