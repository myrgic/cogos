package constellation

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExcludePathsSkipsWalkPrunesAndRefusesIndexFile covers cogdocs.yaml
// excludePaths end to end: a full IndexWorkspace never indexes files under an
// excluded directory, a row indexed there BEFORE the exclusion is pruned on
// the next run, IndexFile is a no-op for an excluded path, and files outside
// the exclusion are indexed as usual.
func TestExcludePathsSkipsWalkPrunesAndRefusesIndexFile(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	fm := "type: note\ntitle: T"
	keep := writeFileAt(t, c, ".cog/mem/semantic/keep.cog.md", fm, "keep body")
	worker := writeFileAt(t, c, ".cog/workers/r-1/.cog/mem/semantic/copy.cog.md", fm, "worker copy")

	count := func(path string) int {
		t.Helper()
		var n int
		if err := c.db.QueryRow("SELECT COUNT(*) FROM documents WHERE path = ?", path).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", path, err)
		}
		return n
	}
	real := func(p string) string {
		r, err := filepath.EvalSymlinks(p)
		if err != nil {
			t.Fatalf("EvalSymlinks(%s): %v", p, err)
		}
		return r
	}

	// Before any exclusion: the worker copy IS indexed (today's behaviour).
	writeCogdocsYaml(t, c, nil)
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace (no exclusion): %v", err)
	}
	if count(real(worker)) != 1 {
		t.Fatalf("precondition: worker copy should be indexed without excludePaths")
	}

	// Declare the exclusion and reindex: the old row is pruned, keep survives.
	cfg := "version: \"1.0\"\nrequiredPaths: []\nexcludePaths:\n  - .cog/workers/\n"
	if err := os.WriteFile(filepath.Join(c.root, ".cog", "config", "cogdocs.yaml"), []byte(cfg), 0644); err != nil {
		t.Fatalf("write cogdocs.yaml: %v", err)
	}
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace (excluded): %v", err)
	}
	if n := count(real(worker)); n != 0 {
		t.Errorf("worker copy under excluded .cog/workers/ still indexed after reindex: %d rows", n)
	}
	if n := count(real(keep)); n != 1 {
		t.Errorf("non-excluded doc should stay indexed, got %d rows", n)
	}
	var fts int
	c.db.QueryRow("SELECT COUNT(*) FROM documents_fts").Scan(&fts)
	var docs int
	c.db.QueryRow("SELECT COUNT(*) FROM documents").Scan(&docs)
	if fts != docs {
		t.Errorf("documents_fts (%d) out of step with documents (%d) after prune", fts, docs)
	}

	// IndexFile on an excluded path is a silent no-op.
	if err := c.IndexFile(worker); err != nil {
		t.Errorf("IndexFile(excluded) returned error: %v", err)
	}
	if n := count(real(worker)); n != 0 {
		t.Errorf("IndexFile indexed an excluded path: %d rows", n)
	}
	// ...and still works for a normal path.
	other := writeFileAt(t, c, ".cog/mem/semantic/other.cog.md", fm, "other body")
	if err := c.IndexFile(other); err != nil {
		t.Fatalf("IndexFile(normal): %v", err)
	}
	if n := count(real(other)); n != 1 {
		t.Errorf("IndexFile(normal) should index, got %d rows", n)
	}
}
