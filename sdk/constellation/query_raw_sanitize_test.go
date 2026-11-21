package constellation

import (
	"testing"
)

// TestSanitizeFTSQueryNeutersSyntaxAndColumnFilterHarm pins the exact FTS5
// text sanitizeFTSQuery hands to MATCH for the raw queries real callers type:
// paths with dots ("serve_compat.go"), versions ("v0.16.31"), hyphenated
// words ("foo-bar") and unterminated quotes.
//
// The harm this guards: before the sanitizer, those strings reached
// documents_fts MATCH verbatim. Dots and stray quotes were FTS5 SYNTAX
// ERRORS -- the search failed instead of matching -- and "foo-bar" was WORSE
// than an error: FTS5 read it as a column filter `foo` minus `bar` and
// returned a confident answer to a question nobody asked. An empty query was
// a syntax error too; it must now be the valid, zero-match empty phrase "".
func TestSanitizeFTSQueryNeutersSyntaxAndColumnFilterHarm(t *testing.T) {
	cases := []struct{ in, want string }{
		{`serve_compat.go`, `"serve_compat.go"`},
		{`v0.16.31`, `"v0.16.31"`},
		{`foo-bar`, `"foo-bar"`},
		{`bar.baz qux`, `"bar.baz" AND "qux"`},
		{`a.b`, `"a.b"`},
		{`foo`, `foo`},
		{`foo OR bar`, `"foo" OR "bar"`},
		{`foo bar`, `"foo" AND "bar"`},
		{`"exact phrase" tail`, `"exact phrase" AND "tail"`},
		{`unterminated "quote`, `"unterminated" AND "quote"`},
		{`AND`, `""`},
		{`foo NOT bar`, `"foo" NOT "bar"`},
		{`foo OR`, `foo`},
		{``, `""`},
		{`   `, `""`},
	}
	for _, tc := range cases {
		if got := sanitizeFTSQuery(tc.in); got != tc.want {
			t.Errorf("sanitizeFTSQuery(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestSanitizeFTSQueryIsIdempotentBecauseQueryRelevantResanitizes guards a
// double-sanitization harm: QueryRelevant calls
// c.Search(buildKeywordFTSQuery(keywords)), whose output is already quoted and
// OR-joined, and Search sanitizes again. If a second pass changed the text,
// every keyword search would be silently rewritten -- broadening an OR into
// an AND, or quoting an already-quoted phrase into a literal-quote mismatch.
func TestSanitizeFTSQueryIsIdempotentBecauseQueryRelevantResanitizes(t *testing.T) {
	inputs := []string{
		`serve_compat.go`, `v0.16.31`, `foo-bar`, `bar.baz qux`, `a.b`, `foo`,
		`foo OR bar`, `foo bar`, `"exact phrase" tail`, `unterminated "quote`,
		`AND`, `foo NOT bar`, `foo OR`, ``, `   `,
	}
	for _, in := range inputs {
		once := sanitizeFTSQuery(in)
		if twice := sanitizeFTSQuery(once); twice != once {
			t.Errorf("sanitizeFTSQuery not idempotent for %q: once=%q twice=%q", in, once, twice)
		}
	}
}

// seedRawQueryFixture inserts a real FTS5 fixture (documents + documents_fts,
// populated separately as query_join_test.go does) covering the raw-query
// harms: a document holding a path-like literal, one holding a hyphenated
// literal, one whose status was never set (NULL), and a deprecated one.
func seedRawQueryFixture(t *testing.T, c *Constellation) {
	t.Helper()
	docs := []struct {
		id, title, content string
		status             interface{}
	}{
		{"doc-raw-dot", "Raw dot", "The file serve_compat.go is an FTS5 syntax error when passed to MATCH unquoted", "active"},
		{"doc-raw-hyphen", "Raw hyphen", "Quoting foo-bar turns a column filter misparse into a literal phrase match", "active"},
		{"doc-null-status", "Null status", "A document whose status was never set must not be silently dropped from search", nil},
		{"doc-deprecated", "Deprecated", "Deprecated scaffolding that a caller must never see in search results", "deprecated"},
	}
	for _, d := range docs {
		if _, err := c.DB().Exec(`INSERT INTO documents (id, path, type, title, created, sector, status,
			content, content_hash, indexed_at, file_mtime, substance_ratio)
			VALUES (?, ?, 'insight', ?, '2026-01-01', 'semantic', ?, ?, 'h', '2026-01-01', '2026-01-01', 1.0)`,
			d.id, "/mem/"+d.id+".md", d.title, d.status, d.content); err != nil {
			t.Fatal(err)
		}
		if _, err := c.DB().Exec(`INSERT INTO documents_fts (id, title, content, tags, sector, type)
			VALUES (?, ?, ?, '', '', 'insight')`, d.id, d.title, d.content); err != nil {
			t.Fatal(err)
		}
	}
}

func rawQueryIDs(nodes []Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func rawQueryHas(nodes []Node, id string) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// TestRawQuerySearchFindsLiteralTextInsteadOfFailingOnDotsAndHyphens guards
// the caller-visible harm: a search for a real file name ("serve_compat.go")
// or a hyphenated word ("foo-bar") returned an FTS5 syntax error or, for the
// hyphen, a column-filter misparse -- instead of the document that literally
// contains that text.
func TestRawQuerySearchFindsLiteralTextInsteadOfFailingOnDotsAndHyphens(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()
	seedRawQueryFixture(t, c)

	got, err := c.Search("serve_compat.go", 10)
	if err != nil {
		t.Fatalf("Search(%q) errored: %v (want the literal document, not a syntax error)", "serve_compat.go", err)
	}
	if !rawQueryHas(got, "doc-raw-dot") {
		t.Fatalf("Search(%q) = %v; want doc-raw-dot (the dot made the query a syntax error)", "serve_compat.go", rawQueryIDs(got))
	}

	got, err = c.SearchWithFilters("foo-bar", nil, "", 10)
	if err != nil {
		t.Fatalf("SearchWithFilters(%q) errored: %v (want the literal document, not a column-filter misparse)", "foo-bar", err)
	}
	if !rawQueryHas(got, "doc-raw-hyphen") {
		t.Fatalf("SearchWithFilters(%q) = %v; want doc-raw-hyphen (the hyphen made it column foo minus bar)", "foo-bar", rawQueryIDs(got))
	}
}

// TestNullStatusDocumentIsNotSilentlyDroppedFromSearch guards the silent-loss
// harm: a bare `status != 'deprecated'` evaluates to NULL for a document whose
// status was never set, so SQL drops it and the document becomes invisible to
// every search even though nobody deprecated it. With COALESCE it is returned.
func TestNullStatusDocumentIsNotSilentlyDroppedFromSearch(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()
	seedRawQueryFixture(t, c)

	got, err := c.Search("silently", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !rawQueryHas(got, "doc-null-status") {
		t.Fatalf("Search(%q) = %v; want doc-null-status included (NULL status must not exclude a document)", "silently", rawQueryIDs(got))
	}
}

// TestDeprecatedDocumentIsStillHiddenFromSearch guards the opposite harm: the
// NULL fix must not accidentally un-hide deprecated documents, and the fixture
// document would match the query were its status filter not applied.
func TestDeprecatedDocumentIsStillHiddenFromSearch(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()
	seedRawQueryFixture(t, c)

	got, err := c.Search("deprecated", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("Search(%q) = %v; want no rows (a deprecated document must never be returned)", "deprecated", rawQueryIDs(got))
	}
}

// TestEmptyQueryIsNoMatchNotAFailedSearch guards the harm that an empty search
// string was an FTS5 syntax error -- an error where the honest answer is "no
// document matches" -- and that the fix never hands FTS5 the empty Go string.
func TestEmptyQueryIsNoMatchNotAFailedSearch(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()
	seedRawQueryFixture(t, c)

	got, err := c.Search("", 10)
	if err != nil {
		t.Fatalf("Search(%q) errored: %v (want a valid zero-match, not a syntax error)", "", err)
	}
	if len(got) != 0 {
		t.Fatalf("Search(%q) = %v; want no rows", "", rawQueryIDs(got))
	}
}
