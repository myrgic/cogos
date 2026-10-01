// memory_search_575_test.go — regression guards for myrgic/cogos#575.
//
// #575: "memory search returns confident results that do not contain the
// query". The handler-level salience blend was removed earlier (#578/#580),
// but two defects kept producing results that the index never vouched for:
//
//  1. buildFTSQuery emitted any single bare term UNQUOTED. A term containing a
//     byte outside FTS5's bareword alphabet ("serve_compat.go", "v0.16.31",
//     "foo-bar", "c++") is then a MATCH syntax error.
//  2. SearchMemory answered ANY FTS error by walking .cog/mem with a substring
//     grep: unranked (every score 0), filesystem-ordered, blind to the index's
//     deprecated filter, and slow on a large corpus. The caller could not tell
//     the index had never answered.
//
// The fixture below deliberately plants "grep bait": markdown files under
// .cog/mem that contain the query terms but are NOT in the index. Any result
// carrying a bait path proves the grep fallback ran instead of FTS5.
package engine

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const baitMarker = "grep-bait"

type doc575 struct {
	id, rel, title, status, content string
	nullStatus                      bool
}

// corpus575 is a small corpus with ranking-relevant structure: one
// document per distinctive term, a strong-vs-weak pair for ordering, and
// unrelated filler that a salience-style ranker would have returned.
var corpus575 = []doc575{
	{id: "prior-art", rel: "semantic/reading/prior-art.cog.md", title: "Prior art index",
		status: "active", content: "Change prediction surveys include BugCache and FixCache heuristics."},
	{id: "compat-note", rel: "semantic/notes/compat.cog.md", title: "Compat routes",
		status: "active", content: "The deprecated handler lives in serve_compat.go and is scheduled for removal."},
	{id: "release-note", rel: "episodic/release.cog.md", title: "Release notes",
		status: "active", content: "Published v0.16.31 with the binary guard."},
	{id: "hyphen-note", rel: "semantic/notes/hyphen.cog.md", title: "Hyphenated terms",
		status: "active", content: "The foo-bar adapter joins two halves."},
	{id: "strong", rel: "semantic/strong.cog.md", title: "Stigmergy stigmergy",
		status: "active", content: "stigmergy stigmergy stigmergy: coordination through the environment."},
	{id: "weak", rel: "semantic/weak.cog.md", title: "Assorted notes",
		status: "active", content: "A long note about many unrelated topics, gardening, weather, " +
			"music, travel, cooking, and one passing mention of stigmergy near the end."},
	{id: "no-status", rel: "semantic/no-status.cog.md", title: "Untagged",
		nullStatus: true, content: "Only this document mentions quokkapheme."},
	{id: "filler-1", rel: "semantic/filler-1.cog.md", title: "Weekly review",
		status: "active", content: "Plans, commitments, and next steps."},
	{id: "filler-2", rel: "semantic/filler-2.cog.md", title: "Reading list",
		status: "active", content: "Books and articles to read this month."},
}

// newIndexedWorkspace builds a workspace root with a real FTS5-backed
// constellation.db at the canonical path plus unindexed grep-bait files.
// Rows are inserted into documents and documents_fts in OPPOSITE orders so
// rowids never line up — a rowid join would pair the wrong documents.
func newIndexedWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	memDir := filepath.Join(root, ".cog", "mem")
	stateDir := filepath.Join(root, ".cog", ".state")
	for _, d := range []string{memDir, stateDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	db, err := sql.Open("sqlite3", filepath.Join(stateDir, "constellation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
CREATE TABLE documents (
    id TEXT PRIMARY KEY, path TEXT NOT NULL UNIQUE, type TEXT NOT NULL,
    title TEXT NOT NULL, created TEXT NOT NULL, updated TEXT, sector TEXT,
    status TEXT, content TEXT NOT NULL, content_hash TEXT NOT NULL,
    word_count INTEGER, line_count INTEGER, indexed_at TEXT NOT NULL,
    file_mtime TEXT NOT NULL
);
CREATE VIRTUAL TABLE documents_fts USING fts5(
    id UNINDEXED, title, content, tags, sector, type,
    tokenize='porter unicode61'
);`)
	if err != nil {
		if strings.Contains(err.Error(), "no such module: fts5") {
			t.Skip("FTS5 not available (build with -tags fts5)")
		}
		t.Fatal(err)
	}

	for _, d := range corpus575 {
		var status any = d.status
		if d.nullStatus {
			status = nil
		}
		if _, err := db.Exec(`INSERT INTO documents (id, path, type, title, created, sector, status,
			content, content_hash, indexed_at, file_mtime)
			VALUES (?, ?, 'insight', ?, '2026-01-01', 'semantic', ?, ?, 'h', '2026-01-01', '2026-01-01')`,
			d.id, filepath.Join(memDir, d.rel), d.title, status, d.content); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(corpus575) - 1; i >= 0; i-- {
		d := corpus575[i]
		if _, err := db.Exec(`INSERT INTO documents_fts (id, title, content, tags, sector, type)
			VALUES (?, ?, ?, '', 'semantic', 'insight')`, d.id, d.title, d.content); err != nil {
			t.Fatal(err)
		}
	}

	// Grep bait: on disk, NOT indexed, containing every query term used below.
	bait := "---\ntitle: " + baitMarker + "\n---\nBugCache serve_compat.go v0.16.31 foo-bar " +
		"stigmergy or and c++ corruptionterm\n"
	for _, name := range []string{"a-" + baitMarker + ".md", "b-" + baitMarker + ".md"} {
		if err := os.WriteFile(filepath.Join(memDir, name), []byte(bait), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

type hit struct {
	path  string
	score float64
}

// search575 runs the real HTTP handler and returns status + ordered hits.
func search575(t *testing.T, root, query string) (int, []hit) {
	t.Helper()
	s := &Server{cfg: &Config{WorkspaceRoot: root}}
	req := httptest.NewRequest(http.MethodGet, "/memory/search?query="+url.QueryEscape(query), nil)
	rec := httptest.NewRecorder()
	s.handleMemorySearch(rec, req)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	_, out := doSearchDecode(t, rec)
	var hits []hit
	items, _ := out["results"].([]any)
	for _, it := range items {
		m := it.(map[string]any)
		p, _ := m["path"].(string)
		sc, _ := m["score"].(float64)
		hits = append(hits, hit{p, sc})
	}
	if c, _ := out["count"].(float64); int(c) != len(hits) {
		t.Fatalf("count=%v but %d results", out["count"], len(hits))
	}
	return rec.Code, hits
}

func doSearchDecode(t *testing.T, rec *httptest.ResponseRecorder) (int, map[string]any) {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range []string{"count", "query", "results"} {
		if _, ok := out[k]; !ok {
			t.Fatalf("response missing backward-compatible field %q: %v", k, out)
		}
	}
	return rec.Code, out
}

func assertNoBait(t *testing.T, query string, hits []hit) {
	t.Helper()
	for _, h := range hits {
		if strings.Contains(h.path, baitMarker) {
			t.Fatalf("query %q returned unindexed grep-bait %s (score %v): the substring "+
				"grep fallback answered instead of FTS5 — myrgic/cogos#575", query, h.path, h.score)
		}
	}
}

// A term absent from the corpus returns zero results — not salience filler,
// not grep hits.
func TestMemorySearch575_ZeroMatchReturnsZero(t *testing.T) {
	root := newIndexedWorkspace(t)
	code, hits := search575(t, root, "zzqxnonceterm")
	if code != http.StatusOK || len(hits) != 0 {
		t.Fatalf("absent term: status %d, %d hits %v; want 200 with 0 hits", code, len(hits), hits)
	}
}

// A body-only term returns exactly the document that contains it.
func TestMemorySearch575_BodyOnlyTermFound(t *testing.T) {
	root := newIndexedWorkspace(t)
	code, hits := search575(t, root, "BugCache")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	assertNoBait(t, "BugCache", hits)
	if len(hits) != 1 || !strings.HasSuffix(hits[0].path, "prior-art.cog.md") {
		t.Fatalf("BugCache: got %v; want exactly the prior-art document", hits)
	}
	if hits[0].score <= 0 {
		t.Fatalf("BugCache hit has score %v; an FTS hit must carry a positive bm25 score", hits[0].score)
	}
}

// Single terms with punctuation must be answered by the index. Before the fix
// each of these was an FTS5 syntax error that silently fell back to grep.
func TestMemorySearch575_PunctuatedTermsUseIndex(t *testing.T) {
	root := newIndexedWorkspace(t)
	cases := map[string]string{
		"serve_compat.go": "compat.cog.md",
		"v0.16.31":        "release.cog.md",
		"foo-bar":         "hyphen.cog.md",
	}
	for q, want := range cases {
		code, hits := search575(t, root, q)
		if code != http.StatusOK {
			t.Errorf("%q: status %d; want 200", q, code)
			continue
		}
		assertNoBait(t, q, hits)
		if len(hits) == 0 || !strings.HasSuffix(hits[0].path, want) {
			t.Errorf("%q: got %v; want %s at rank 1", q, hits, want)
			continue
		}
		if hits[0].score <= 0 {
			t.Errorf("%q: rank-1 score %v; want > 0 (grep hits score 0)", q, hits[0].score)
		}
	}
}

// Results are ordered by relevance with non-increasing scores: the document
// saturated with the term outranks the one with a passing mention.
func TestMemorySearch575_RelevanceOrder(t *testing.T) {
	root := newIndexedWorkspace(t)
	_, hits := search575(t, root, "stigmergy")
	assertNoBait(t, "stigmergy", hits)
	if len(hits) != 2 {
		t.Fatalf("stigmergy: got %v; want the strong and weak documents only", hits)
	}
	if !strings.HasSuffix(hits[0].path, "strong.cog.md") || !strings.HasSuffix(hits[1].path, "weak.cog.md") {
		t.Fatalf("stigmergy order: %v; want strong before weak", hits)
	}
	if !(hits[0].score > hits[1].score) {
		t.Fatalf("scores not strictly decreasing with relevance: %v", hits)
	}
}

// A query with nothing searchable after sanitising is a clean zero, not an
// FTS syntax error and not a grep walk for "or".
func TestMemorySearch575_UnsearchableQueryIsEmpty(t *testing.T) {
	root := newIndexedWorkspace(t)
	for _, q := range []string{"OR", "-", "OR OR"} {
		code, hits := search575(t, root, q)
		if code != http.StatusOK || len(hits) != 0 {
			t.Errorf("%q: status %d hits %v; want 200 with 0 hits", q, code, hits)
		}
	}
}

// A document whose status column is NULL is still searchable. The old filter
// `d.status != 'deprecated'` evaluates to NULL for such rows and dropped them.
func TestMemorySearch575_NullStatusDocIsSearchable(t *testing.T) {
	root := newIndexedWorkspace(t)
	_, hits := search575(t, root, "quokkapheme")
	if len(hits) != 1 || !strings.HasSuffix(hits[0].path, "no-status.cog.md") {
		t.Fatalf("quokkapheme: got %v; want the NULL-status document", hits)
	}
}

// When the index exists but cannot answer, the caller gets an error — never
// grep results dressed up as search results.
func TestMemorySearch575_IndexErrorSurfaces(t *testing.T) {
	root := newIndexedWorkspace(t)
	dbPath := filepath.Join(root, ".cog", ".state", "constellation.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE documents_fts`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := SearchMemory(root, "corruptionterm", 20, ""); err == nil {
		t.Fatal("SearchMemory on a broken index returned no error; retrieval failure was masked")
	}
	code, hits := search575(t, root, "corruptionterm")
	if code != http.StatusInternalServerError {
		t.Fatalf("handler status %d hits %v; want 500 when the index cannot answer", code, hits)
	}
}

// With no index at all, the substring walk is still allowed — and it only
// returns documents that literally contain the query.
func TestMemorySearch575_NoIndexUsesGrepMatchesOnly(t *testing.T) {
	root := newIndexedWorkspace(t)
	if err := os.Remove(filepath.Join(root, ".cog", ".state", "constellation.db")); err != nil {
		t.Fatal(err)
	}
	out, err := SearchMemory(root, "zzqxnonceterm", 20, "")
	if err != nil {
		t.Fatal(err)
	}
	if c := out.(map[string]any)["count"].(int); c != 0 {
		t.Fatalf("grep path returned %d results for an absent term; want 0", c)
	}
}

// Unit coverage for the quoting rule itself.
func TestBuildFTSQuery575_SingleTermQuoting(t *testing.T) {
	cases := []struct{ in, want string }{
		{"BugCache", "BugCache"},
		{"claude_code", "claude_code"},
		{"naïve", "naïve"},
		{"Bug*", "Bug*"},
		{"serve_compat.go", `"serve_compat.go"`},
		{"v0.16.31", `"v0.16.31"`},
		{"foo-bar", `"foo-bar"`},
		{"c++", `"c++"`},
		{"a/b", `"a/b"`},
		{"*", `"*"`},
		{"^foo", `"^foo"`},
	}
	for _, c := range cases {
		if got := buildFTSQuery(c.in); got != c.want {
			t.Errorf("buildFTSQuery(%q) = %s; want %s", c.in, got, c.want)
		}
	}
}
