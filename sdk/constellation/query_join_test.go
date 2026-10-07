package constellation

import (
	"strings"
	"testing"
)

// TestSearchJoinsOnIDNotRowid guards the documents <-> documents_fts join in
// every FTS query in this file (myrgic/cogos#575, secondary finding).
//
// documents has a TEXT primary key and documents_fts is populated separately
// (rebuildFTS / per-file upsert), so their integer rowids are unrelated. A
// `d.rowid = documents_fts.rowid` join therefore returns whatever document
// happens to share the rowid — a confident, wrong answer. The fixture inserts
// the two tables in opposite orders so rowids never line up.
func TestSearchJoinsOnIDNotRowid(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	docs := []struct{ id, title, content string }{
		{"doc-alpha", "Alpha", "alphaonlyterm appears here and nowhere else"},
		{"doc-beta", "Beta", "betaonlyterm appears here and nowhere else"},
		{"doc-gamma", "Gamma", "gammaonlyterm appears here and nowhere else"},
	}
	for _, d := range docs {
		if _, err := c.DB().Exec(`INSERT INTO documents (id, path, type, title, created, sector, status,
			content, content_hash, indexed_at, file_mtime, substance_ratio)
			VALUES (?, ?, 'insight', ?, '2026-01-01', 'semantic', 'active', ?, 'h', '2026-01-01', '2026-01-01', 1.0)`,
			d.id, "/mem/"+d.id+".md", d.title, d.content); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(docs) - 1; i >= 0; i-- {
		d := docs[i]
		if _, err := c.DB().Exec(`INSERT INTO documents_fts (id, title, content, tags, sector, type)
			VALUES (?, ?, ?, '', '', 'insight')`, d.id, d.title, d.content); err != nil {
			t.Fatal(err)
		}
	}

	check := func(name string, got []Node, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != 1 || got[0].ID != "doc-alpha" || !strings.Contains(got[0].Content, "alphaonlyterm") {
			ids := make([]string, len(got))
			for i, n := range got {
				ids[i] = n.ID
			}
			t.Fatalf("%s: got %v; want exactly doc-alpha (rowid join returns the wrong document)", name, ids)
		}
	}

	got, err := c.Search("alphaonlyterm", 10)
	check("Search", got, err)

	got, err = c.SearchWithFilters("alphaonlyterm", nil, "", 10)
	check("SearchWithFilters", got, err)

	filter := DefaultSubstanceFilter()
	filter.MinSubstanceRatio = 0
	got, err = c.QueryRelevantWithSubstance("alphaonlyterm", "", 10, 10, filter)
	check("QueryRelevantWithSubstance", got, err)

	scored, err := c.QueryRelevantWithEmbedding("alphaonlyterm", "", 10, 10, filter, nil)
	nodes := make([]Node, len(scored))
	for i, s := range scored {
		nodes[i] = s.Node
	}
	check("QueryRelevantWithEmbedding", nodes, err)
}
