// search_recent.go — recency-ordered, top-k transcript search for callers
// that fuse observatory hits into another result list (GET /memory/search,
// myrgic/cogos#650).
//
// (*Index).Search is the MCP tool's search: it walks sessions in lexical
// session-id order and stops at the first `limit` matches, so the hits it
// returns are an arbitrary slice of the match set. That is acceptable for an
// explicit conversation-search tool, but a fused memory search needs a
// defined order. SearchRecent returns the `limit` MOST RECENT matching turns.
//
// Cost: still a linear scan over every indexed turn (AND substring match,
// same semantics as Search), but memory is bounded to O(limit) by a min-heap
// keyed on timestamp, and excerpts are only built for the survivors. The scan
// honours ctx between sessions so an abandoned HTTP request stops early.
package conversations

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrIndexNotReady is returned when the provider's index has not been
// initialised or has never been loaded from disk. Callers must surface this
// as "transcripts unavailable" — an empty result from an unloaded index is
// indistinguishable from "no matches" and would silently narrow a search.
var ErrIndexNotReady = errors.New("conversations index not initialised or not yet loaded")

// recentCand is a lightweight match reference; excerpts are built only for
// the final top-k.
type recentCand struct {
	sid string
	pos int // index into idx.turns[sid]
	ts  time.Time
}

// recentHeap is a min-heap on timestamp (oldest at the root), so the root is
// the candidate to evict when a newer match arrives.
type recentHeap []recentCand

func (h recentHeap) Len() int { return len(h) }
func (h recentHeap) Less(i, j int) bool {
	if !h[i].ts.Equal(h[j].ts) {
		return h[i].ts.Before(h[j].ts)
	}
	// Deterministic tiebreak: the lexically larger (sid, pos) is "older".
	if h[i].sid != h[j].sid {
		return h[i].sid > h[j].sid
	}
	return h[i].pos > h[j].pos
}
func (h recentHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *recentHeap) Push(x any)   { *h = append(*h, x.(recentCand)) }
func (h *recentHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// SearchRecent returns up to limit turns matching query (AND substring, same
// parsing as Search), ordered newest first. since, when non-zero, drops turns
// older than it. limit <= 0 returns nil: an unbounded fused search is exactly
// the hot-path cost this function exists to avoid.
func (idx *Index) SearchRecent(ctx context.Context, query string, since time.Time, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		return nil, nil
	}
	terms := parseSearchQuery(query)
	if len(terms) == 0 {
		// An empty query would match every turn; that is a listing, not a
		// search, and never what a fused memory search means.
		return nil, nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	h := make(recentHeap, 0, limit)
	for sid, turns := range idx.turns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for i := range turns {
			t := &turns[i]
			if !since.IsZero() && t.Timestamp.Before(since) {
				continue
			}
			// Cheap reject before the substring scan: a full heap only
			// admits turns strictly newer than its oldest member.
			if len(h) == limit && !t.Timestamp.After(h[0].ts) {
				continue
			}
			if !matchesAllTerms(t.Text, terms) {
				continue
			}
			c := recentCand{sid: sid, pos: i, ts: t.Timestamp}
			if len(h) < limit {
				heap.Push(&h, c)
			} else {
				h[0] = c
				heap.Fix(&h, 0)
			}
		}
	}

	cands := []recentCand(h)
	sort.Slice(cands, func(i, j int) bool { return h.Less(j, i) }) // newest first

	anchor := query
	if len(terms) > 0 {
		anchor = terms[0]
	}
	hits := make([]SearchHit, 0, len(cands))
	for _, c := range cands {
		t := idx.turns[c.sid][c.pos]
		meta, hasMeta := idx.sessions[c.sid]
		hit := SearchHit{
			SessionID: c.sid,
			TurnIndex: t.TurnIndex,
			UUID:      t.UUID,
			Timestamp: t.Timestamp,
			Role:      t.Role,
			Excerpt:   makeExcerpt(t.Text, anchor, 300),
		}
		if hasMeta {
			hit.SessionTitle = meta.Title
			hit.Source = meta.Source
			hit.Identity = meta.Identity
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

// HitURI returns a cog:conversations URI that ResolveConversationURI resolves
// to exactly this hit's turn.
//
//   - Ingest sessions are indexed under "<source>/<session_id>"; the path form
//     cog:conversations/<source>/<session_id> recomposes that key.
//   - Sessions without a source (the native transcript parser) have no path
//     form that recomposes their key, so they are addressed by the canonical
//     turn-UUID fragment over the whole observatory.
//
// The fragment is the canonical #id-<uuid> when the turn has a UUID, else the
// positional #turn-N (only unambiguous inside a session path). Returns "" when
// no resolvable form exists (no source and no UUID); callers must still report
// the hit, just without a URI.
func HitURI(h SearchHit) string {
	const scheme = "cog:conversations"
	if h.Source != "" && strings.HasPrefix(h.SessionID, h.Source+"/") {
		base := scheme + "/" + h.SessionID // == <source>/<raw session id>
		if h.UUID != "" {
			return base + "#id-" + h.UUID
		}
		return base + "#turn-" + strconv.Itoa(h.TurnIndex)
	}
	if h.UUID != "" {
		return scheme + "#id-" + h.UUID
	}
	return ""
}

// SearchRecent is the provider-level entry point used by the kernel's fused
// memory search. It returns ErrIndexNotReady when the index is nil or has
// never completed a Load — an unloaded index must read as "unavailable", not
// as "zero matches".
func (p *Provider) SearchRecent(ctx context.Context, query string, since time.Time, limit int) ([]SearchHit, error) {
	if p == nil {
		return nil, ErrIndexNotReady
	}
	p.mu.Lock()
	idx := p.index
	p.mu.Unlock()
	if idx == nil || !idx.Loaded() {
		return nil, ErrIndexNotReady
	}
	return idx.SearchRecent(ctx, query, since, limit)
}
