// transcript_search.go — provider-level turn search for the cogdoc-fused
// memory search (myrgic/cogos#650).
//
// Index.Search is unranked: it walks sessions in lexical session-id order and
// stops at the first `limit` hits. A merged result list needs recency order,
// so SearchTurns collects every match and sorts it newest first before it
// truncates. The scan is linear over the in-memory index. That matches the
// MCP path's cost. A hot-path FTS table over turns is a follow-up.
package conversations

import (
	"errors"
	"sort"
	"strings"
	"time"
)

// ErrIndexNotInitialised is returned by SearchTurns before the observatory
// index is loaded. Callers must report this as "unavailable", never as an
// empty result.
var ErrIndexNotInitialised = errors.New("conversations index not initialised")

// SearchTurns returns turns matching query, newest first, capped at limit
// (limit <= 0 means no cap). An empty or all-whitespace query matches nothing.
func (p *Provider) SearchTurns(query string, limit int) ([]SearchHit, error) {
	p.mu.Lock()
	idx := p.index
	p.mu.Unlock()

	if idx == nil {
		return nil, ErrIndexNotInitialised
	}
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	hits := idx.Search(query, time.Time{}, time.Time{}, "", "", 0)
	sort.SliceStable(hits, func(i, j int) bool {
		if !hits[i].Timestamp.Equal(hits[j].Timestamp) {
			return hits[i].Timestamp.After(hits[j].Timestamp)
		}
		if hits[i].SessionID != hits[j].SessionID {
			return hits[i].SessionID < hits[j].SessionID
		}
		return hits[i].TurnIndex < hits[j].TurnIndex
	})
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}
