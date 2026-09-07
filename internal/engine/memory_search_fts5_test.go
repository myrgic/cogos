package engine

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMemorySearch_FTS5CompiledIn is a BUILD guard, not a behaviour test.
//
// The kernel's memory search has two paths: FTS5 (ranked, phrase/AND/OR) and a
// substring-grep fallback (unranked, returns 0 for any multi-word query). The
// fallback exists for corrupt DBs, but it ALSO silently activates when the
// binary was built without `-tags fts5` — which is what `go build` does by
// default, what CI does, and what the deploy scripts did until 2026-09-05. The
// live kernel served grep-fallback results for every multi-word query with no
// signal to the caller.
//
// This test opens an in-memory sqlite and creates an fts5 table. Without the
// tag it fails with "no such module: fts5" — the exact error the fallback
// swallows — so a tagless build cannot pass the suite.
func TestMemorySearch_FTS5CompiledIn(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE VIRTUAL TABLE t USING fts5(x)`); err != nil {
		t.Fatalf("FTS5 is not compiled into this binary (build with -tags fts5): %v", err)
	}
}

// TestMemorySearch_ScoreIsMonotoneInRelevance pins the score direction.
// FTS5 bm25() is negative-better; the SQL orders ASC (best first). The score
// the caller sees must be HIGHER for the better match, or callers that sort by
// score invert the kernel's own ranking. Negative control: a doc matching the
// query once must score BELOW a doc matching it many times.
func TestMemorySearch_ScoreIsMonotoneInRelevance(t *testing.T) {
	ws := t.TempDir()
	mem := filepath.Join(ws, ".cog", "mem")
	if err := os.MkdirAll(filepath.Join(ws, ".cog", ".state"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(mem, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(ws, ".cog", ".state", "constellation.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(`CREATE TABLE documents (id TEXT PRIMARY KEY, path TEXT, title TEXT, type TEXT, sector TEXT, status TEXT, file_mtime INTEGER)`)
	mustExec(`CREATE VIRTUAL TABLE documents_fts USING fts5(id UNINDEXED, title, content, tags, sector, type, tokenize='porter unicode61')`)
	// bm25 IDF is ~0 when the term appears in EVERY document, which collapses
	// all ranks toward 0 and makes the direction untestable. Add unrelated
	// documents so the term is rare corpus-wide, and vary term frequency
	// between the two matching docs.
	weak := "proprioception is mentioned once. " + strings.Repeat("unrelated filler text about other topics. ", 40)
	strong := strings.Repeat("proprioception grounds proprioception in proprioception. ", 10)
	docs := []struct{ id, title, body string }{{"weak", "Weak", weak}, {"strong", "Strong", strong}}
	for i := 0; i < 8; i++ {
		docs = append(docs, struct{ id, title, body string }{
			"noise" + string(rune('a'+i)), "Noise", strings.Repeat("kernel ledger grant surface embodiment gate. ", 20)})
	}
	for _, d := range docs {
		p := filepath.Join(mem, d.id+".cog.md")
		if err := os.WriteFile(p, []byte(d.body), 0o644); err != nil {
			t.Fatal(err)
		}
		mustExec(`INSERT INTO documents VALUES (?,?,?,?,?,?,?)`, d.id, p, d.title, "note", "semantic", "active", 0)
		mustExec(`INSERT INTO documents_fts (id,title,content,tags,sector,type) VALUES (?,?,?,?,?,?)`, d.id, d.title, d.body, "", "semantic", "note")
	}
	db.Close()

	out, err := searchMemoryFTS(dbPath, ws, "proprioception", 10, "")
	if err != nil {
		t.Fatalf("searchMemoryFTS: %v", err)
	}
	res, _ := out["results"].([]map[string]any)
	if len(res) != 2 {
		t.Fatalf("want 2 results, got %d: %v", len(res), out)
	}
	score := map[string]float64{}
	for _, r := range res {
		score[r["title"].(string)] = r["score"].(float64)
	}
	if !(score["Strong"] > score["Weak"]) {
		t.Fatalf("score direction inverted: strong=%.3f weak=%.3f (bm25 negative-better must map to higher score)", score["Strong"], score["Weak"])
	}
	if res[0]["title"] != "Strong" {
		t.Fatalf("SQL order and score disagree: first result %v", res[0]["title"])
	}
	for _, r := range res {
		s := r["score"].(float64)
		if s <= 0 || s > 1 {
			t.Fatalf("score out of (0,1]: %v", s)
		}
	}
}
