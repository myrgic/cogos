// memory_search_fusion_test.go — myrgic/cogos#650: GET /memory/search must
// also see conversation transcripts, rank curated above raw, order transcripts
// by recency, and never silently narrow when the transcript index is absent.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// fakeTranscripts is a TranscriptSearcher with canned behaviour. It returns
// hits deliberately OLDEST first so the engine's recency ordering is tested,
// not assumed.
type fakeTranscripts struct {
	hits  []TranscriptHit
	err   error
	calls int
	block bool // block until ctx is done (timeout test)
}

func (f *fakeTranscripts) SearchTranscripts(ctx context.Context, _ string, limit int) ([]TranscriptHit, error) {
	f.calls++
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	if len(f.hits) > limit {
		return f.hits[:limit], nil
	}
	return f.hits, nil
}

func transcriptFixture(n int) []TranscriptHit {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	out := make([]TranscriptHit, n)
	for i := 0; i < n; i++ {
		out[i] = TranscriptHit{
			SessionID: fmt.Sprintf("src/s%02d", i),
			TurnIndex: i,
			Source:    "src",
			Role:      "user",
			Timestamp: base.Add(time.Duration(i) * time.Hour), // oldest first
			Excerpt:   "said claude out loud",
			URI:       fmt.Sprintf("cog:conversations/src/s%02d#turn-%d", i, i),
		}
	}
	return out
}

// withTranscriptSearcher swaps the package hook for the test's duration.
func withTranscriptSearcher(t *testing.T, s TranscriptSearcher) {
	t.Helper()
	prev := transcriptSearcher
	transcriptSearcher = s
	t.Cleanup(func() { transcriptSearcher = prev })
}

func doSearchURL(t *testing.T, s *Server, rawQuery string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/memory/search?"+rawQuery, nil)
	rec := httptest.NewRecorder()
	s.handleMemorySearch(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return res.StatusCode, nil
	}
	var out map[string]any
	decodeJSON(t, res, &out)
	return res.StatusCode, out
}

func resultItems(t *testing.T, out map[string]any) []map[string]any {
	t.Helper()
	raw, _ := out["results"].([]any)
	items := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		items = append(items, r.(map[string]any))
	}
	return items
}

func sourcesOf(out map[string]any) map[string]any {
	m, _ := out["sources"].(map[string]any)
	return m
}

// ── Acceptance: a transcript-only phrase is found, with provenance ─────────

func TestMemorySearchFused_TranscriptOnlyPhraseIsFound(t *testing.T) {
	s := newMemorySearchServer(t) // FTS fixture; "zzz…" matches no cogdoc
	withTranscriptSearcher(t, &fakeTranscripts{hits: transcriptFixture(1)})

	_, out := doSearchURL(t, s, "query="+url.QueryEscape("zzzznotinthecorpuszzzz"))
	items := resultItems(t, out)
	if len(items) != 1 {
		t.Fatalf("want 1 transcript hit, got %d: %v", len(items), items)
	}
	it := items[0]
	if it["kind"] != MemoryKindTranscript {
		t.Errorf("kind = %v, want transcript", it["kind"])
	}
	for _, k := range []string{"session_id", "turn_index", "source", "timestamp", "excerpt", "uri"} {
		if v, ok := it[k]; !ok || v == "" || v == nil {
			t.Errorf("transcript hit missing %q: %v", k, it)
		}
	}
	// Backward-compatible fields are present on every item.
	for _, k := range []string{"path", "score", "title", "uri"} {
		if _, ok := it[k]; !ok {
			t.Errorf("transcript hit missing legacy field %q", k)
		}
	}
	if got := sourcesOf(out); got["cogdoc"] != "ok" || got["transcript"] != "ok" {
		t.Errorf("sources = %v, want both ok", got)
	}
	if out["count"].(float64) != 1 {
		t.Errorf("count = %v, want 1", out["count"])
	}
}

// ── Acceptance: curated outranks raw ───────────────────────────────────────

func TestMemorySearchFused_CogdocsRankBeforeTranscripts(t *testing.T) {
	s := newMemorySearchServer(t)
	withTranscriptSearcher(t, &fakeTranscripts{hits: transcriptFixture(3)})

	// "claude" matches cogdocs in the FTS fixture AND every fake transcript.
	_, out := doSearchURL(t, s, "query=claude")
	items := resultItems(t, out)
	seenTranscript := false
	nCogdoc := 0
	for i, it := range items {
		switch it["kind"] {
		case MemoryKindCogdoc:
			nCogdoc++
			if seenTranscript {
				t.Fatalf("cogdoc at position %d ranked below a transcript: %v", i, items)
			}
			if p, _ := it["path"].(string); p == "" {
				t.Errorf("cogdoc hit lost its path: %v", it)
			}
		case MemoryKindTranscript:
			seenTranscript = true
		default:
			t.Fatalf("item %d has no/unknown kind: %v", i, it)
		}
	}
	if nCogdoc == 0 || !seenTranscript {
		t.Fatalf("want both kinds present, got %d cogdocs, transcripts=%v", nCogdoc, seenTranscript)
	}
}

// ── Transcript ordering is recency, not the searcher's order ───────────────

func TestMemorySearchFused_TranscriptsNewestFirst(t *testing.T) {
	s := newMemorySearchServer(t)
	withTranscriptSearcher(t, &fakeTranscripts{hits: transcriptFixture(4)})

	_, out := doSearchURL(t, s, "query=zzzznotinthecorpuszzzz&kind=transcript")
	items := resultItems(t, out)
	if len(items) != 4 {
		t.Fatalf("want 4, got %d", len(items))
	}
	var prev time.Time
	for i, it := range items {
		ts, err := time.Parse(time.RFC3339, it["timestamp"].(string))
		if err != nil {
			t.Fatalf("bad timestamp %v", it["timestamp"])
		}
		if i > 0 && ts.After(prev) {
			t.Fatalf("transcripts not newest-first at %d: %v", i, items)
		}
		prev = ts
	}
	if items[0]["session_id"] != "src/s03" {
		t.Errorf("newest hit should be src/s03, got %v", items[0]["session_id"])
	}
}

// ── Fail loud: unwired / not ready / erroring transcript index ─────────────

func TestMemorySearchFused_UnwiredReportsUnavailable(t *testing.T) {
	s := newMemorySearchServer(t)
	withTranscriptSearcher(t, nil)

	code, out := doSearchURL(t, s, "query=claude")
	if code != http.StatusOK {
		t.Fatalf("status %d; cogdoc half must still answer", code)
	}
	src := sourcesOf(out)
	if src["transcript"] != SourceStatusUnavailable {
		t.Errorf("sources.transcript = %v, want unavailable", src["transcript"])
	}
	if src["cogdoc"] != SourceStatusOK {
		t.Errorf("sources.cogdoc = %v, want ok", src["cogdoc"])
	}
	errs, _ := out["source_errors"].(map[string]any)
	if errs["transcript"] == nil || errs["transcript"] == "" {
		t.Errorf("source_errors.transcript must explain the degradation, got %v", out["source_errors"])
	}
	if len(resultItems(t, out)) == 0 {
		t.Error("cogdoc results must still be returned")
	}
}

func TestMemorySearchFused_NotReadyAndErrorAreDistinct(t *testing.T) {
	s := newMemorySearchServer(t)

	withTranscriptSearcher(t, &fakeTranscripts{err: fmt.Errorf("wrap: %w", ErrTranscriptsUnavailable)})
	_, out := doSearchURL(t, s, "query=claude")
	if got := sourcesOf(out)["transcript"]; got != SourceStatusUnavailable {
		t.Errorf("not-ready index: sources.transcript = %v, want unavailable", got)
	}

	withTranscriptSearcher(t, &fakeTranscripts{err: errors.New("disk on fire")})
	_, out = doSearchURL(t, s, "query=claude")
	if got := sourcesOf(out)["transcript"]; got != SourceStatusError {
		t.Errorf("failing index: sources.transcript = %v, want error", got)
	}
}

func TestMemorySearchFused_TranscriptTimeoutIsReportedNotSilent(t *testing.T) {
	s := newMemorySearchServer(t)
	prev := transcriptSearchTimeout
	transcriptSearchTimeout = 20 * time.Millisecond
	t.Cleanup(func() { transcriptSearchTimeout = prev })
	withTranscriptSearcher(t, &fakeTranscripts{block: true})

	_, out := doSearchURL(t, s, "query=claude")
	if got := sourcesOf(out)["transcript"]; got != SourceStatusError {
		t.Errorf("timed-out scan: sources.transcript = %v, want error", got)
	}
}

// ── kind= opt-out ──────────────────────────────────────────────────────────

func TestMemorySearchFused_KindParam(t *testing.T) {
	s := newMemorySearchServer(t)
	fake := &fakeTranscripts{hits: transcriptFixture(2)}
	withTranscriptSearcher(t, fake)

	_, out := doSearchURL(t, s, "query=claude&kind=cogdoc")
	for _, it := range resultItems(t, out) {
		if it["kind"] != MemoryKindCogdoc {
			t.Errorf("kind=cogdoc returned %v", it)
		}
	}
	if got := sourcesOf(out)["transcript"]; got != SourceStatusExcluded {
		t.Errorf("kind=cogdoc: sources.transcript = %v, want excluded", got)
	}
	if fake.calls != 0 {
		t.Errorf("kind=cogdoc must not scan transcripts (calls=%d)", fake.calls)
	}

	_, out = doSearchURL(t, s, "query=claude&kind=transcript")
	for _, it := range resultItems(t, out) {
		if it["kind"] != MemoryKindTranscript {
			t.Errorf("kind=transcript returned %v", it)
		}
	}
	if got := sourcesOf(out)["cogdoc"]; got != SourceStatusExcluded {
		t.Errorf("kind=transcript: sources.cogdoc = %v, want excluded", got)
	}

	if code, _ := doSearchURL(t, s, "query=claude&kind=bogus"); code != http.StatusBadRequest {
		t.Errorf("kind=bogus: status %d, want 400", code)
	}
	if code, _ := doSearchURL(t, s, "query=claude&kind=cogdoc,transcript"); code != http.StatusOK {
		t.Errorf("kind=cogdoc,transcript: status %d, want 200", code)
	}
}

// ── fuseMemoryResults unit tests (pure ranking arithmetic) ─────────────────

func mkItems(kind string, n int) []map[string]any {
	out := make([]map[string]any, n)
	for i := range out {
		out[i] = map[string]any{"kind": kind, "i": i}
	}
	return out
}

func countKinds(items []map[string]any) (c, tr int) {
	for _, it := range items {
		if it["kind"] == MemoryKindCogdoc {
			c++
		} else {
			tr++
		}
	}
	return
}

func TestFuseMemoryResults(t *testing.T) {
	cases := []struct {
		name          string
		nc, nt, limit int
		wantC, wantT  int
	}{
		{"both saturated: transcripts get their reserved quarter", 50, 50, 20, 15, 5},
		{"few transcripts: cogdocs backfill", 50, 2, 20, 18, 2},
		{"few cogdocs: transcripts backfill", 3, 50, 20, 3, 17},
		{"no transcripts", 30, 0, 20, 20, 0},
		{"no cogdocs", 0, 30, 20, 0, 20},
		{"tiny limit still reserves one transcript slot", 10, 10, 2, 1, 1},
		{"under limit: everything", 4, 3, 20, 4, 3},
		{"zero limit", 5, 5, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fuseMemoryResults(mkItems(MemoryKindCogdoc, tc.nc), mkItems(MemoryKindTranscript, tc.nt), tc.limit)
			c, tr := countKinds(got)
			if c != tc.wantC || tr != tc.wantT {
				t.Fatalf("got %d cogdoc + %d transcript, want %d + %d", c, tr, tc.wantC, tc.wantT)
			}
			if len(got) > tc.limit && tc.limit >= 0 {
				t.Fatalf("len %d exceeds limit %d", len(got), tc.limit)
			}
			// Curated-first invariant and per-kind order preserved.
			seenT := false
			for i, it := range got {
				if it["kind"] == MemoryKindTranscript {
					seenT = true
				} else if seenT {
					t.Fatalf("cogdoc after transcript at %d", i)
				}
			}
			for i := 0; i < c; i++ {
				if got[i]["i"] != i {
					t.Fatalf("cogdoc order not preserved: %v", got)
				}
			}
		})
	}
}

func TestParseMemoryKinds(t *testing.T) {
	for raw, want := range map[string][2]bool{
		"":                   {true, true},
		"cogdoc":             {true, false},
		"transcript":         {false, true},
		"Transcript, cogdoc": {true, true},
		"cogdoc,":            {true, false},
	} {
		c, tr, err := parseMemoryKinds(raw)
		if err != nil || c != want[0] || tr != want[1] {
			t.Errorf("parseMemoryKinds(%q) = %v,%v,%v want %v", raw, c, tr, err, want)
		}
	}
	for _, bad := range []string{"bogus", ",", "cogdoc,nope"} {
		if _, _, err := parseMemoryKinds(bad); err == nil {
			t.Errorf("parseMemoryKinds(%q): want error", bad)
		}
	}
}
