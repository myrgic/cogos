// memory_search_fusion.go — fuse curated cogdoc hits with raw conversation
// transcript hits for GET /memory/search (myrgic/cogos#650).
//
// Before this, memory search read only the cogdoc corpus (constellation FTS,
// grep fallback). The Conversations Observatory ingests every turn from every
// surface, but its index was reachable only through the MCP conversation
// tools — so "have we talked about this?" could not see what was said, only
// what someone later wrote down.
//
// Package boundary: internal/engine must not import internal/conversations.
// The transcript side is therefore a hook (TranscriptSearcher) that the daemon
// binary sets from internal/providers/all.RegisterConversations, the same
// pattern as SetConversationsResolver.
//
// Ranking contract:
//
//   - Curated outranks raw. Every returned cogdoc hit precedes every returned
//     transcript hit, so a chat turn can never push out the cogdoc written
//     from it.
//   - Transcripts get a reserved quota (transcriptQuota) so a query that
//     saturates the cogdoc corpus still shows some of what was said; slots
//     neither kind uses are backfilled by the other.
//   - Transcript hits are ordered newest first (the observatory's own search
//     is unranked and walks sessions in lexical id order).
//   - Scores are NOT comparable across kinds. Cogdoc scores are normalised
//     bm25 in (0,1] (0 on the grep fallback); transcript hits carry score 0
//     because substring matching has no relevance signal. Result order is the
//     authoritative ranking.
//
// Fail loud: the response always carries a per-source status map. A missing
// hook, an index that has not loaded, or a failed scan reports
// "unavailable"/"error" for transcripts instead of silently returning a
// narrower cogdoc-only result.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Memory-search result kinds.
const (
	MemoryKindCogdoc     = "cogdoc"
	MemoryKindTranscript = "transcript"
)

// Per-source status values reported under "sources" in the response.
const (
	SourceStatusOK          = "ok"
	SourceStatusUnavailable = "unavailable" // not wired, or index not loaded
	SourceStatusError       = "error"       // wired and loaded, but the search failed
	SourceStatusExcluded    = "excluded"    // caller asked for other kinds only
)

// transcriptSearchTimeout bounds the transcript scan on the request path. The
// observatory search is a linear substring scan over every indexed turn; on a
// large corpus the cogdoc half must not wait indefinitely on it. A timeout is
// reported as sources.transcripts = "error", never as zero hits.
var transcriptSearchTimeout = 3 * time.Second

// ErrTranscriptsUnavailable is returned by a TranscriptSearcher when the
// transcript index exists in the binary but is not ready to answer (not
// initialised, or never loaded from disk). Distinct from a search failure.
var ErrTranscriptsUnavailable = errors.New("transcript index unavailable")

// TranscriptHit is one conversation turn matching a memory-search query.
type TranscriptHit struct {
	SessionID    string
	TurnIndex    int
	Source       string
	Role         string
	Timestamp    time.Time
	Excerpt      string
	SessionTitle string
	// URI resolves to exactly this turn via /v1/uri/resolve (cog:conversations
	// scheme). May be empty when the turn has no resolvable form.
	URI string
}

// TranscriptSearcher is the engine-side view of the conversation observatory
// index. Implementations must return hits newest first, at most limit of them,
// and must return ErrTranscriptsUnavailable (possibly wrapped) when the index
// is not ready rather than an empty slice.
type TranscriptSearcher interface {
	SearchTranscripts(ctx context.Context, query string, limit int) ([]TranscriptHit, error)
}

// transcriptSearcher is wired at boot by the daemon binary. Nil means the
// conversations provider is not linked into this binary.
var transcriptSearcher TranscriptSearcher

// SetTranscriptSearcher wires the transcript half of memory search. Call from
// the package that assembles the daemon (internal/providers/all), before the
// server starts listening.
func SetTranscriptSearcher(s TranscriptSearcher) { transcriptSearcher = s }

// WiredTranscriptSearcher returns the wired searcher, or nil. Exposed for
// binary-assembly regression tests in cmd/cogos.
func WiredTranscriptSearcher() TranscriptSearcher { return transcriptSearcher }

// parseMemoryKinds parses the optional `kind` query parameter: a comma list of
// "cogdoc" and/or "transcript". Empty means both (the default — the point of
// #650 is that the default path can see what was said).
func parseMemoryKinds(raw string) (cogdoc, transcript bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true, true, nil
	}
	for _, part := range strings.Split(raw, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case MemoryKindCogdoc:
			cogdoc = true
		case MemoryKindTranscript:
			transcript = true
		case "":
			// tolerate "cogdoc," style trailing commas
		default:
			return false, false, fmt.Errorf("unknown kind %q (want %q and/or %q)",
				part, MemoryKindCogdoc, MemoryKindTranscript)
		}
	}
	if !cogdoc && !transcript {
		return false, false, fmt.Errorf("kind must name at least one of %q, %q",
			MemoryKindCogdoc, MemoryKindTranscript)
	}
	return cogdoc, transcript, nil
}

// transcriptQuota is the number of result slots reserved for transcript hits
// when both kinds have more matches than fit: a quarter of the page, at least
// one. Cogdocs keep the rest and always rank first.
func transcriptQuota(limit int) int {
	q := limit / 4
	if q < 1 {
		q = 1
	}
	return q
}

// fuseMemoryResults applies the ranking contract (see file comment). Inputs
// are each already in their own rank order; the output never exceeds limit.
func fuseMemoryResults(cogdocs, transcripts []map[string]any, limit int) []map[string]any {
	if limit <= 0 {
		return []map[string]any{}
	}
	tReserve := transcriptQuota(limit)
	if len(transcripts) < tReserve {
		tReserve = len(transcripts)
	}
	cSlots := limit - tReserve
	if len(cogdocs) < cSlots {
		cSlots = len(cogdocs)
	}
	tSlots := limit - cSlots
	if len(transcripts) < tSlots {
		tSlots = len(transcripts)
	}
	out := make([]map[string]any, 0, cSlots+tSlots)
	out = append(out, cogdocs[:cSlots]...)
	out = append(out, transcripts[:tSlots]...)
	return out
}

// transcriptHitsToResults converts hits to response items, newest first. The
// sort is defensive: the ordering contract belongs to the engine, not to
// whichever searcher happens to be wired.
func transcriptHitsToResults(hits []TranscriptHit) []map[string]any {
	sorted := make([]TranscriptHit, len(hits))
	copy(sorted, hits)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.After(sorted[j].Timestamp)
	})
	out := make([]map[string]any, 0, len(sorted))
	for _, h := range sorted {
		title := h.SessionTitle
		if title == "" {
			title = h.SessionID
		}
		ts := ""
		if !h.Timestamp.IsZero() {
			ts = h.Timestamp.UTC().Format(time.RFC3339)
		}
		out = append(out, map[string]any{
			// Backward-compatible fields every existing caller parses.
			"uri":   h.URI,
			"path":  "", // transcripts are not files under .cog/mem
			"title": title,
			"score": 0.0, // substring match: no relevance signal (see file comment)
			// Provenance.
			"kind":       MemoryKindTranscript,
			"session_id": h.SessionID,
			"turn_index": h.TurnIndex,
			"source":     h.Source,
			"role":       h.Role,
			"timestamp":  ts,
			"excerpt":    h.Excerpt,
		})
	}
	return out
}

// cogdocResults extracts and kind-tags the results slice from a SearchMemory
// response (both the FTS and grep paths return map[string]any with
// "results": []map[string]any).
func cogdocResults(raw any) []map[string]any {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	items, _ := m["results"].([]map[string]any)
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		tagged := make(map[string]any, len(it)+1)
		for k, v := range it {
			tagged[k] = v
		}
		tagged["kind"] = MemoryKindCogdoc
		out = append(out, tagged)
	}
	return out
}

// searchTranscriptsForMemory runs the transcript half and classifies the
// outcome into (results, status, reason).
func searchTranscriptsForMemory(ctx context.Context, s TranscriptSearcher, query string, limit int) ([]map[string]any, string, string) {
	if s == nil {
		return nil, SourceStatusUnavailable, "conversation observatory not wired into this binary"
	}
	ctx, cancel := context.WithTimeout(ctx, transcriptSearchTimeout)
	defer cancel()
	hits, err := s.SearchTranscripts(ctx, query, limit)
	if err != nil {
		if errors.Is(err, ErrTranscriptsUnavailable) {
			return nil, SourceStatusUnavailable, err.Error()
		}
		return nil, SourceStatusError, err.Error()
	}
	return transcriptHitsToResults(hits), SourceStatusOK, ""
}

// SearchMemoryFused is the kernel's memory search across both corpora. It
// keeps SearchMemory's response fields (query, count, results[uri,path,title,
// score]) and adds, per result, "kind" plus transcript provenance, and at the
// top level "sources" (per-source status) and, when any source is degraded,
// "source_errors" (per-source reason).
//
// A cogdoc search error is returned as an error (the caller must be able to
// tell "no matches" from "I am broken"; unchanged from before). A transcript
// failure is reported in-band, because the cogdoc half is still a valid answer
// — but it is never silent.
func SearchMemoryFused(ctx context.Context, workspaceRoot, query string, limit int, wantCogdoc, wantTranscript bool) (map[string]any, error) {
	return searchMemoryFusedWith(ctx, transcriptSearcher, workspaceRoot, query, limit, wantCogdoc, wantTranscript)
}

func searchMemoryFusedWith(ctx context.Context, ts TranscriptSearcher, workspaceRoot, query string, limit int, wantCogdoc, wantTranscript bool) (map[string]any, error) {
	sources := map[string]string{}
	reasons := map[string]string{}

	var cogdocs []map[string]any
	if wantCogdoc {
		raw, err := SearchMemory(workspaceRoot, query, limit, "")
		if err != nil {
			return nil, err
		}
		cogdocs = cogdocResults(raw)
		sources[MemoryKindCogdoc] = SourceStatusOK
	} else {
		sources[MemoryKindCogdoc] = SourceStatusExcluded
	}

	var transcripts []map[string]any
	if wantTranscript {
		var status, reason string
		transcripts, status, reason = searchTranscriptsForMemory(ctx, ts, query, limit)
		sources[MemoryKindTranscript] = status
		if reason != "" {
			reasons[MemoryKindTranscript] = reason
		}
	} else {
		sources[MemoryKindTranscript] = SourceStatusExcluded
	}

	results := fuseMemoryResults(cogdocs, transcripts, limit)
	out := map[string]any{
		"query":   query,
		"count":   len(results),
		"results": results,
		"sources": sources,
	}
	if len(reasons) > 0 {
		out["source_errors"] = reasons
	}
	return out, nil
}
