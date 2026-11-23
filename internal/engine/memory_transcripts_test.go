// memory_transcripts_test.go — tests for transcript fusion in GET /memory/search
// (myrgic/cogos#650). The searcher here is a fake, so these tests cover the
// fusion rules only. The real observatory wiring is covered in
// internal/providers/all and cmd/cogos.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeSearcher returns canned hits and records the query it was given.
type fakeSearcher struct {
	hits  []TranscriptHit
	err   error
	calls int
	query string
}

func (f *fakeSearcher) SearchTranscripts(query string, limit int) ([]TranscriptHit, error) {
	f.calls++
	f.query = query
	if f.err != nil {
		return nil, f.err
	}
	out := make([]TranscriptHit, len(f.hits))
	copy(out, f.hits)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// withSearcher installs s for one test and restores the previous value after.
func withSearcher(t *testing.T, s ConversationSearcher) {
	t.Helper()
	prev := conversationSearcher
	SetConversationSearcher(s)
	t.Cleanup(func() { SetConversationSearcher(prev) })
}

// writeMemSearchCogdoc writes a cogdoc under .cog/mem so the grep fallback finds it.
func writeMemSearchCogdoc(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, ".cog", "mem", rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func doMemorySearch(t *testing.T, root, query, sources string) (int, map[string]any) {
	t.Helper()
	s := &Server{cfg: &Config{WorkspaceRoot: root}}
	q := "/memory/search?query=" + url.QueryEscape(query)
	if sources != "" {
		q += "&sources=" + url.QueryEscape(sources)
	}
	rec := httptest.NewRecorder()
	s.handleMemorySearch(rec, httptest.NewRequest(http.MethodGet, q, nil))
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return rec.Code, out
}

func resultsOf(t *testing.T, out map[string]any) []map[string]any {
	t.Helper()
	raw, _ := out["results"].([]any)
	var rs []map[string]any
	for _, r := range raw {
		rs = append(rs, r.(map[string]any))
	}
	return rs
}

func sourcesOf(t *testing.T, out map[string]any) map[string]any {
	t.Helper()
	s, ok := out["sources"].(map[string]any)
	if !ok {
		t.Fatalf("response has no sources map: %v", out)
	}
	return s
}

// Acceptance 1: a phrase present only in an indexed transcript returns at least
// one hit with kind "transcript" and a cog:conversations URI.
func TestMemorySearchTranscript_KindAndURI(t *testing.T) {
	root := t.TempDir()
	fs := &fakeSearcher{hits: []TranscriptHit{{
		URI:       "cog:conversations/hermes-cog/sess-1#id-u-1",
		SessionID: "sess-1", TurnIndex: 3, Role: "user",
		Excerpt: "temperature-gapped decoding is the plan",
		Source:  "hermes-cog", Timestamp: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC),
	}}}
	withSearcher(t, fs)

	code, out := doMemorySearch(t, root, "temperature-gapped decoding", "")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	rs := resultsOf(t, out)
	if len(rs) != 1 {
		t.Fatalf("want 1 transcript hit, got %d: %v", len(rs), out)
	}
	if rs[0]["kind"] != "transcript" {
		t.Errorf("kind = %v, want transcript", rs[0]["kind"])
	}
	uri, _ := rs[0]["uri"].(string)
	if !strings.HasPrefix(uri, "cog:conversations/") {
		t.Errorf("uri = %q, want a cog:conversations URI", uri)
	}
	if rs[0]["session_id"] != "sess-1" || rs[0]["turn_index"] != float64(3) {
		t.Errorf("provenance missing: %v", rs[0])
	}
	if fs.query != "temperature-gapped decoding" {
		t.Errorf("searcher got query %q", fs.query)
	}
	if sourcesOf(t, out)["transcripts"] != "ok" {
		t.Errorf("sources.transcripts = %v, want ok", sourcesOf(t, out)["transcripts"])
	}
}

// Acceptance 2: a phrase in both a cogdoc and the transcript it came from ranks
// the cogdoc first.
func TestMemorySearchTranscript_CuratedOutranksRaw(t *testing.T) {
	root := t.TempDir()
	writeMemSearchCogdoc(t, root, "semantic/insight.cog.md",
		"# Insight\n\nthe harness attestation policy keeps sessions honest\n")
	withSearcher(t, &fakeSearcher{hits: []TranscriptHit{{
		URI: "cog:conversations#id-u-9", SessionID: "sess-9", TurnIndex: 0,
		Role: "user", Excerpt: "the harness attestation policy keeps sessions honest",
		Timestamp: time.Now(),
	}}})

	code, out := doMemorySearch(t, root, "harness attestation policy", "")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	rs := resultsOf(t, out)
	if len(rs) < 2 {
		t.Fatalf("want cogdoc + transcript, got %d results", len(rs))
	}
	if rs[0]["kind"] != "cogdoc" {
		t.Errorf("first result kind = %v, want cogdoc (curated outranks raw)", rs[0]["kind"])
	}
	if rs[len(rs)-1]["kind"] != "transcript" {
		t.Errorf("last result kind = %v, want transcript", rs[len(rs)-1]["kind"])
	}
}

// Acceptance 3: with the conversations provider unwired, the response reports
// transcripts: unavailable. It must not silently return a narrower set.
func TestMemorySearchTranscript_UnwiredReportsUnavailable(t *testing.T) {
	root := t.TempDir()
	writeMemSearchCogdoc(t, root, "semantic/a.cog.md", "alpha bravo charlie\n")
	withSearcher(t, nil)

	code, out := doMemorySearch(t, root, "alpha bravo", "")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if got := sourcesOf(t, out)["transcripts"]; got != "unavailable" {
		t.Errorf("sources.transcripts = %v, want unavailable", got)
	}
	if got := sourcesOf(t, out)["cogdoc"]; got != "ok" {
		t.Errorf("sources.cogdoc = %v, want ok", got)
	}
	if n := len(resultsOf(t, out)); n != 1 {
		t.Errorf("cogdoc results = %d, want 1 (the cogdoc must still be returned)", n)
	}
}

// A searcher error (index not loaded) is reported as unavailable, with the
// reason attached, and the cogdoc results are still returned.
func TestMemorySearchTranscript_SearcherErrorReportsUnavailable(t *testing.T) {
	root := t.TempDir()
	writeMemSearchCogdoc(t, root, "semantic/a.cog.md", "alpha bravo charlie\n")
	withSearcher(t, &fakeSearcher{err: errors.New("conversations index not initialised")})

	_, out := doMemorySearch(t, root, "alpha bravo", "")
	src := sourcesOf(t, out)
	if src["transcripts"] != "unavailable" {
		t.Errorf("sources.transcripts = %v, want unavailable", src["transcripts"])
	}
	if msg, _ := src["transcripts_error"].(string); !strings.Contains(msg, "not initialised") {
		t.Errorf("transcripts_error = %q, want the searcher error", msg)
	}
	if n := len(resultsOf(t, out)); n != 1 {
		t.Errorf("cogdoc results = %d, want 1", n)
	}
}

// Opt-out: sources=cogdoc returns cogdoc-only results and never calls the
// searcher.
func TestMemorySearchTranscript_CogdocOnlyOptOut(t *testing.T) {
	root := t.TempDir()
	writeMemSearchCogdoc(t, root, "semantic/a.cog.md", "alpha bravo charlie\n")
	fs := &fakeSearcher{hits: []TranscriptHit{{URI: "cog:conversations#id-x", Timestamp: time.Now()}}}
	withSearcher(t, fs)

	code, out := doMemorySearch(t, root, "alpha bravo", "cogdoc")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if fs.calls != 0 {
		t.Errorf("searcher called %d times under sources=cogdoc, want 0", fs.calls)
	}
	if sourcesOf(t, out)["transcripts"] != "excluded" {
		t.Errorf("sources.transcripts = %v, want excluded", sourcesOf(t, out)["transcripts"])
	}
	for _, r := range resultsOf(t, out) {
		if r["kind"] == "transcript" {
			t.Errorf("transcript hit leaked into cogdoc-only response: %v", r)
		}
	}
}

func TestMemorySearchTranscript_InvalidSourcesIs400(t *testing.T) {
	code, _ := doMemorySearch(t, t.TempDir(), "x", "bogus")
	if code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for sources=bogus", code)
	}
}

// Transcript hits are ordered newest first, not by session-id order.
func TestFuseMemorySearch_TranscriptsNewestFirst(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	fs := &fakeSearcher{hits: []TranscriptHit{
		{URI: "u-a", SessionID: "aaa", Timestamp: base},                  // oldest, lexically first
		{URI: "u-z", SessionID: "zzz", Timestamp: base.AddDate(0, 0, 5)}, // newest
		{URI: "u-m", SessionID: "mmm", Timestamp: base.AddDate(0, 0, 2)},
	}}
	withSearcher(t, fs)

	out := fuseMemorySearch("q", map[string]any{"results": []map[string]any(nil)}, 20, true)
	rs := resultsOf(t, map[string]any{"results": anySlice(out["results"])})
	var got []string
	for _, r := range rs {
		got = append(got, r["uri"].(string))
	}
	want := []string{"u-z", "u-m", "u-a"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want newest first %v", got, want)
	}
}

// Transcripts keep a reserved share of the window when cogdocs fill it, so the
// curated side cannot push the raw side out entirely.
func TestFuseMemorySearch_TranscriptReserveSurvivesCogdocFlood(t *testing.T) {
	var cog []map[string]any
	for i := 0; i < 20; i++ {
		cog = append(cog, map[string]any{"uri": fmt.Sprintf("cog:mem/%d", i), "path": "p", "title": "t", "score": 0.5})
	}
	var hits []TranscriptHit
	for i := 0; i < 10; i++ {
		hits = append(hits, TranscriptHit{URI: fmt.Sprintf("cog:conversations#id-%d", i),
			SessionID: "s", TurnIndex: i, Timestamp: time.Now().Add(time.Duration(i) * time.Second)})
	}
	withSearcher(t, &fakeSearcher{hits: hits})

	out := fuseMemorySearch("q", map[string]any{"results": cog}, 20, true)
	rs := resultsOf(t, map[string]any{"results": anySlice(out["results"])})
	var nT int
	for _, r := range rs {
		if r["kind"] == "transcript" {
			nT++
		}
	}
	if nT != 5 {
		t.Errorf("transcript slots = %d, want 5 (limit 20 / divisor 4)", nT)
	}
	if len(rs) != 20 {
		t.Errorf("total = %d, want the full window of 20", len(rs))
	}
}

// With no transcript matches, the reserve is 0 and no cogdoc result is dropped.
func TestFuseMemorySearch_NoTranscriptMatchesDoesNotNarrowCogdocs(t *testing.T) {
	var cog []map[string]any
	for i := 0; i < 20; i++ {
		cog = append(cog, map[string]any{"uri": fmt.Sprintf("cog:mem/%d", i)})
	}
	withSearcher(t, &fakeSearcher{})

	out := fuseMemorySearch("q", map[string]any{"results": cog}, 20, true)
	if n := len(anySlice(out["results"])); n != 20 {
		t.Errorf("cogdoc results = %d, want all 20 kept when no transcripts match", n)
	}
	if out["sources"].(map[string]any)["transcripts"] != "ok" {
		t.Errorf("sources.transcripts should be ok even with zero matches")
	}
}

// The wired searcher is the one the engine calls. A nil hook is a nil searcher.
func TestSetConversationSearcher_RoundTrip(t *testing.T) {
	fs := &fakeSearcher{}
	withSearcher(t, fs)
	if WiredConversationSearcher() != ConversationSearcher(fs) {
		t.Fatal("WiredConversationSearcher did not return the installed searcher")
	}
}

// anySlice converts the results value (typed []map[string]any) to []any for
// resultsOf, which expects a decoded JSON shape.
func anySlice(v any) []any {
	rs, _ := v.([]map[string]any)
	out := make([]any, 0, len(rs))
	for _, r := range rs {
		out = append(out, r)
	}
	return out
}
