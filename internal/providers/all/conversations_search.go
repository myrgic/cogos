// conversations_search.go — adapts the conversations provider to
// engine.ConversationSearcher for GET /memory/search (myrgic/cogos#650).
//
// The adapter lives here, not in internal/engine, because engine must not
// import internal/conversations. RegisterConversations wires it.
package all

import (
	"fmt"
	"strings"

	"github.com/myrgic/cogos/internal/conversations"
	"github.com/myrgic/cogos/internal/engine"
)

// conversationsSearcher adapts conversations.Provider to
// engine.ConversationSearcher.
type conversationsSearcher struct {
	p *conversations.Provider
}

// SearchTranscripts implements engine.ConversationSearcher.
func (s *conversationsSearcher) SearchTranscripts(query string, limit int) ([]engine.TranscriptHit, error) {
	hits, err := s.p.SearchTurns(query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]engine.TranscriptHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, engine.TranscriptHit{
			URI:          transcriptURI(h.Source, bareSessionID(h.Source, h.SessionID), h.UUID),
			SessionID:    h.SessionID,
			TurnIndex:    h.TurnIndex,
			Role:         string(h.Role),
			Excerpt:      h.Excerpt,
			Source:       h.Source,
			SessionTitle: h.SessionTitle,
			Timestamp:    h.Timestamp,
		})
	}
	return out, nil
}

// bareSessionID strips the "<source>/" prefix the index adds to source-scoped
// session keys. cog:conversations URIs take the bare id after the source
// segment, so an un-stripped key would double the source in the resolved URI.
func bareSessionID(source, key string) string {
	if source != "" {
		if rest, ok := strings.CutPrefix(key, source+"/"); ok {
			return rest
		}
	}
	return key
}

// transcriptURI builds the cog:conversations URI that resolves one turn. The
// #id-<uuid> fragment fully bounds the slice, so the URI addresses exactly
// this turn. Source-less (Claude Code) sessions use the whole-observatory
// form, because a source segment there would be read as a source name.
func transcriptURI(source, sessionID, uuid string) string {
	if source != "" {
		return fmt.Sprintf("cog:conversations/%s/%s#id-%s", source, sessionID, uuid)
	}
	return fmt.Sprintf("cog:conversations#id-%s", uuid)
}
