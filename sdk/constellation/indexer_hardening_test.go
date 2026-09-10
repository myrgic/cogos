package constellation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestIndexFileConcurrentNoLoss is the regression test for the BLOCKER:
// concurrent IndexFile calls previously raced on the single-writer connection
// (one goroutine's SAVEPOINT in upsertFTSRow issued while another held an open
// db.Begin() transaction) and produced "cannot start a transaction within a
// transaction", a failure that was swallowed by a slog.Warn — silently dropping
// index updates. With indexMu serialising the whole operation, 20 goroutines
// hammering IndexFile must produce zero errors and full row coverage.
func TestIndexFileConcurrentNoLoss(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	const n = 20

	// Pre-create N distinct cogdocs so every goroutine indexes a real file.
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("semantic/hammer-%02d.cog.md", i)
		paths[i] = writeCogdocInWorkspace(t, c,
			rel,
			fmt.Sprintf("id: hammer-%02d\ntype: note\ntitle: Hammer %02d\ncreated: 2026-01-01", i, i),
			fmt.Sprintf("Concurrent index body number %02d with token hammertoken%02d.", i, i),
		)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			<-start // maximise contention: release all goroutines at once
			if err := c.IndexFile(p); err != nil {
				errCh <- fmt.Errorf("IndexFile(%s): %w", p, err)
			}
		}(paths[i])
	}

	close(start)
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		t.Fatalf("expected zero IndexFile errors from %d concurrent goroutines, got %d: %v", n, len(errs), errs)
	}

	// Full row coverage: every document row present, and every FTS row present.
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("hammer-%02d", i)
		var docCount, ftsCount int
		if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = ?`, id).Scan(&docCount); err != nil {
			t.Fatalf("documents count for %s: %v", id, err)
		}
		if docCount != 1 {
			t.Errorf("expected exactly 1 documents row for %s, got %d", id, docCount)
		}
		if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents_fts WHERE id = ?`, id).Scan(&ftsCount); err != nil {
			t.Fatalf("fts count for %s: %v", id, err)
		}
		if ftsCount != 1 {
			t.Errorf("expected exactly 1 documents_fts row for %s, got %d (index update dropped)", id, ftsCount)
		}
	}

	// And each unique token must be searchable — the FTS row content is correct.
	for i := 0; i < n; i++ {
		token := fmt.Sprintf("hammertoken%02d", i)
		var found int
		if err := c.DB().QueryRow(
			`SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH ?`, token,
		).Scan(&found); err != nil {
			t.Fatalf("FTS search for %s: %v", token, err)
		}
		if found != 1 {
			t.Errorf("expected token %s searchable in exactly 1 doc, got %d", token, found)
		}
	}
}

// TestIndexFileConcurrentSamePath hammers IndexFile on a SINGLE shared path from
// many goroutines. This is the tightest form of the transaction-state race
// (every goroutine contends for the same row + FTS entry). It must produce zero
// errors and leave exactly one document row and one FTS row.
func TestIndexFileConcurrentSamePath(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	path := writeCogdocInWorkspace(t, c,
		"semantic/shared.cog.md",
		"id: shared-doc\ntype: note\ntitle: Shared\ncreated: 2026-01-01",
		"Shared body content with token sharedtoken.",
	)

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := c.IndexFile(path); err != nil {
				errCh <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("IndexFile error on shared path: %v", err)
	}

	var docCount, ftsCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'shared-doc'`).Scan(&docCount); err != nil {
		t.Fatalf("documents count: %v", err)
	}
	if docCount != 1 {
		t.Errorf("expected 1 documents row for shared-doc, got %d", docCount)
	}
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents_fts WHERE id = 'shared-doc'`).Scan(&ftsCount); err != nil {
		t.Fatalf("fts count: %v", err)
	}
	if ftsCount != 1 {
		t.Errorf("expected 1 documents_fts row for shared-doc, got %d", ftsCount)
	}
}

// TestIndexCogdocHashSkipRefreshesMtime is the regression test for the MAJOR
// hash-skip bug: indexCogdoc's early return on a content-hash match previously
// preceded the only file_mtime write, so a touched-but-unchanged file kept a
// stale stored mtime and was flagged as drifted by the engine's drift repair on
// every search, forever. After the fix, a hash-match path must still refresh the
// stored file_mtime in place.
func TestIndexCogdocHashSkipRefreshesMtime(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	path := writeCogdocInWorkspace(t, c,
		"semantic/touchme.cog.md",
		"id: touch-doc\ntype: note\ntitle: Touch Me\ncreated: 2026-01-01",
		"Unchanging body content.",
	)

	if err := c.IndexFile(path); err != nil {
		t.Fatalf("initial IndexFile: %v", err)
	}

	var mtime1 string
	if err := c.DB().QueryRow(`SELECT file_mtime FROM documents WHERE id = 'touch-doc'`).Scan(&mtime1); err != nil {
		t.Fatalf("read mtime1: %v", err)
	}

	// Touch the file: bump mtime forward by 5 minutes without changing content.
	future := time.Now().Add(5 * time.Minute)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Re-index. Content hash matches, so indexCogdoc takes the early-return
	// branch — but it must now refresh file_mtime.
	if err := c.IndexFile(path); err != nil {
		t.Fatalf("re-IndexFile after touch: %v", err)
	}

	var mtime2 string
	if err := c.DB().QueryRow(`SELECT file_mtime FROM documents WHERE id = 'touch-doc'`).Scan(&mtime2); err != nil {
		t.Fatalf("read mtime2: %v", err)
	}
	if mtime2 == mtime1 {
		t.Fatalf("stored file_mtime not refreshed on touched-but-unchanged file: still %q", mtime2)
	}

	// The refreshed stored mtime must equal the on-disk mtime (no residual
	// drift): a second-pass drift check would report clean.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	wantMtime := info.ModTime().Format(time.RFC3339)
	if mtime2 != wantMtime {
		t.Errorf("refreshed stored mtime %q != on-disk mtime %q (drift would persist)", mtime2, wantMtime)
	}
}

// TestIndexCogdocFrontmatterChangeInvalidatesRefs is the regression test for
// the MAJOR hash-scope bug (board #138): indexCogdoc's content-hash skip
// decision previously hashed doc.Content (body only), so a frontmatter-only
// edit — refs, tags, title, status, etc. — left the stored content_hash
// unchanged. That tripped the unchanged-hash early return, which never
// reaches the DELETE+INSERT of tags/doc_references, so the graph edges went
// stale forever even though the frontmatter had genuinely changed. After the
// fix, the hash is computed over the full raw file bytes (frontmatter +
// body), so a refs-only edit with an identical body must still refresh
// doc_references.
func TestIndexCogdocFrontmatterChangeInvalidatesRefs(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	body := "Unchanging body content."
	path := writeCogdocInWorkspace(t, c,
		"semantic/refswap.cog.md",
		"id: refswap-doc\ntype: note\ntitle: Ref Swap\ncreated: 2026-01-01\nrefs:\n  - target-a",
		body,
	)

	if err := c.IndexFile(path); err != nil {
		t.Fatalf("initial IndexFile: %v", err)
	}

	var target string
	if err := c.DB().QueryRow(
		`SELECT target_uri FROM doc_references WHERE source_id = 'refswap-doc'`,
	).Scan(&target); err != nil {
		t.Fatalf("read initial doc_references: %v", err)
	}
	if target != "target-a" {
		t.Fatalf("expected initial ref target-a, got %q", target)
	}

	// Rewrite with an identical body but a changed frontmatter ref (and a new
	// tag) — the body-only hash used before the fix would not change here.
	rewritten := "---\nid: refswap-doc\ntype: note\ntitle: Ref Swap\ncreated: 2026-01-01\nrefs:\n  - target-b\ntags:\n  - swapped\n---\n\n" + body
	if err := os.WriteFile(path, []byte(rewritten), 0644); err != nil {
		t.Fatalf("rewrite cogdoc: %v", err)
	}

	if err := c.IndexFile(path); err != nil {
		t.Fatalf("re-IndexFile after frontmatter change: %v", err)
	}

	var count int
	if err := c.DB().QueryRow(
		`SELECT COUNT(*) FROM doc_references WHERE source_id = 'refswap-doc'`,
	).Scan(&count); err != nil {
		t.Fatalf("count doc_references: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 doc_references row after frontmatter change, got %d (stale ref not cleared)", count)
	}

	if err := c.DB().QueryRow(
		`SELECT target_uri FROM doc_references WHERE source_id = 'refswap-doc'`,
	).Scan(&target); err != nil {
		t.Fatalf("read updated doc_references: %v", err)
	}
	if target != "target-b" {
		t.Fatalf("expected updated ref target-b after frontmatter-only change, got %q (frontmatter edit not detected)", target)
	}
}

// TestIndexWorkspacePrunesGhostCogdocs is the regression test for the additive-
// only reindex path leaving permanent ghost rows: a cogdoc whose file is deleted
// from disk previously kept its documents/FTS/tags/refs rows forever. After the
// fix, IndexWorkspace (the reindex path) prunes rows for managed cogdocs
// (path LIKE '%/.cog/mem/%.cog.md') whose files are gone, while leaving rows for
// still-present files untouched.
func TestIndexWorkspacePrunesGhostCogdocs(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	keepPath := writeCogdocInWorkspace(t, c,
		"semantic/keep.cog.md",
		"id: keep-doc\ntype: note\ntitle: Keep\ncreated: 2026-01-01\ntags:\n  - keeptag",
		"Body that stays. Token keepstoken.",
	)
	_ = keepPath
	ghostPath := writeCogdocInWorkspace(t, c,
		"semantic/ghost.cog.md",
		"id: ghost-doc\ntype: note\ntitle: Ghost\ncreated: 2026-01-01\ntags:\n  - ghosttag",
		"Body that will vanish. Token ghoststoken.",
	)

	// First index: both docs present.
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("first IndexWorkspace: %v", err)
	}
	for _, id := range []string{"keep-doc", "ghost-doc"} {
		var n int
		if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = ?`, id).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", id, err)
		}
		if n != 1 {
			t.Fatalf("expected %s indexed after first pass, got %d", id, n)
		}
	}

	// Delete the ghost file on disk.
	if err := os.Remove(ghostPath); err != nil {
		t.Fatalf("remove ghost file: %v", err)
	}

	// Re-index: ghost row (and its cascaded tags) must be pruned; keep row stays.
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("second IndexWorkspace: %v", err)
	}

	var ghostDocs, ghostFTS, ghostTags int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'ghost-doc'`).Scan(&ghostDocs); err != nil {
		t.Fatalf("ghost doc count: %v", err)
	}
	if ghostDocs != 0 {
		t.Errorf("expected ghost-doc pruned from documents, got %d rows", ghostDocs)
	}
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents_fts WHERE id = 'ghost-doc'`).Scan(&ghostFTS); err != nil {
		t.Fatalf("ghost fts count: %v", err)
	}
	if ghostFTS != 0 {
		t.Errorf("expected ghost-doc pruned from documents_fts, got %d rows", ghostFTS)
	}
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM tags WHERE document_id = 'ghost-doc'`).Scan(&ghostTags); err != nil {
		t.Fatalf("ghost tags count: %v", err)
	}
	if ghostTags != 0 {
		t.Errorf("expected ghost-doc tags cascade-deleted, got %d rows", ghostTags)
	}

	// The surviving doc must be untouched and still searchable.
	var keepDocs int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'keep-doc'`).Scan(&keepDocs); err != nil {
		t.Fatalf("keep doc count: %v", err)
	}
	if keepDocs != 1 {
		t.Errorf("expected keep-doc to survive prune, got %d rows", keepDocs)
	}
	var keepFound int
	if err := c.DB().QueryRow(
		`SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'keepstoken'`,
	).Scan(&keepFound); err != nil {
		t.Fatalf("keep search: %v", err)
	}
	if keepFound != 1 {
		t.Errorf("expected keep-doc still searchable after prune, got %d", keepFound)
	}
}

// writeFileAt writes an arbitrary cogdoc under c.root at the given
// workspace-relative path, creating parent directories as needed. Unlike
// writeCogdocInWorkspace (which is hardcoded to .cog/mem/), this lets tests
// place a doc anywhere in the workspace tree — e.g. a workspace-root
// cogdoc directory declared via cogdocs.yaml requiredPaths.
func writeFileAt(t *testing.T, c *Constellation, relPath, frontmatter, body string) string {
	t.Helper()
	absPath := filepath.Join(c.root, relPath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(absPath), err)
	}
	content := "---\n" + frontmatter + "\n---\n\n" + body
	if err := os.WriteFile(absPath, []byte(content), 0644); err != nil {
		t.Fatalf("write cogdoc %s: %v", absPath, err)
	}
	return absPath
}

// writeCogdocsYaml writes a minimal .cog/config/cogdocs.yaml declaring the
// given requiredPaths, following the workspace's real cogdocs.yaml shape
// (see .cog/config/cogdocs.yaml's requiredPaths list).
func writeCogdocsYaml(t *testing.T, c *Constellation, requiredPaths []string) {
	t.Helper()
	configDir := filepath.Join(c.root, ".cog", "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", configDir, err)
	}
	var sb strings.Builder
	sb.WriteString("version: \"1.0\"\nrequiredPaths:\n")
	for _, p := range requiredPaths {
		sb.WriteString("  - " + p + "\n")
	}
	path := filepath.Join(configDir, "cogdocs.yaml")
	if err := os.WriteFile(path, []byte(sb.String()), 0644); err != nil {
		t.Fatalf("write cogdocs.yaml: %v", err)
	}
}

// TestWalkRootsDerivesExtraRootsFromCogdocsYaml verifies walkRoots reads
// requiredPaths from .cog/config/cogdocs.yaml to add workspace-root roots
// beyond .cog/, and skips declared paths that are already nested under .cog/
// (the base walk covers those already).
func TestWalkRootsDerivesExtraRootsFromCogdocsYaml(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	archDir := filepath.Join(c.root, "architecture")
	if err := os.MkdirAll(archDir, 0755); err != nil {
		t.Fatalf("mkdir architecture: %v", err)
	}
	memSemanticDir := filepath.Join(c.root, ".cog", "mem", "semantic")
	if err := os.MkdirAll(memSemanticDir, 0755); err != nil {
		t.Fatalf("mkdir .cog/mem/semantic: %v", err)
	}

	writeCogdocsYaml(t, c, []string{"architecture/", ".cog/mem/semantic/"})

	resolvedArch, err := filepath.EvalSymlinks(archDir)
	if err != nil {
		t.Fatalf("EvalSymlinks(architecture): %v", err)
	}
	resolvedCog, err := filepath.EvalSymlinks(filepath.Join(c.root, ".cog"))
	if err != nil {
		t.Fatalf("EvalSymlinks(.cog): %v", err)
	}

	roots := c.walkRoots()

	foundArch := false
	nestedCount := 0
	for _, r := range roots {
		if r == resolvedArch {
			foundArch = true
		}
		if r != resolvedCog && strings.HasPrefix(r, resolvedCog+string(filepath.Separator)) {
			nestedCount++
		}
	}
	if !foundArch {
		t.Errorf("expected walkRoots to include workspace-root architecture/ dir %q, got %v", resolvedArch, roots)
	}
	if nestedCount != 0 {
		t.Errorf("expected requiredPaths nested under .cog/ to be skipped (already covered by base walk), got %d extra nested roots in %v", nestedCount, roots)
	}
}

// TestWalkRootsSkipsDeclaredPathThatDoesNotExist verifies a requiredPaths
// entry with no backing directory on disk is silently skipped rather than
// producing a walk error — a config declaration is not a guarantee.
func TestWalkRootsSkipsDeclaredPathThatDoesNotExist(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	if err := os.MkdirAll(filepath.Join(c.root, ".cog"), 0755); err != nil {
		t.Fatalf("mkdir .cog: %v", err)
	}
	writeCogdocsYaml(t, c, []string{"nonexistent-dir/"})

	roots := c.walkRoots()
	if len(roots) != 1 {
		t.Errorf("expected only the base .cog/ root when the declared path doesn't exist, got %v", roots)
	}
}

// TestIndexWorkspaceWidensRootsToWorkspaceRootCogdocDir is the regression test
// for the reindex-widening half of this change: a cogdoc living under a
// workspace-root directory declared in cogdocs.yaml's requiredPaths (e.g.
// architecture/, per the v2 migration moving the ADR/RFC corpus out of
// .cog/adr) must be indexed by IndexWorkspace even though it sits entirely
// outside .cog/.
func TestIndexWorkspaceWidensRootsToWorkspaceRootCogdocDir(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	if err := os.MkdirAll(filepath.Join(c.root, ".cog"), 0755); err != nil {
		t.Fatalf("mkdir .cog: %v", err)
	}
	writeCogdocsYaml(t, c, []string{"architecture/"})

	writeFileAt(t, c, "architecture/adrs/001-widen-roots.cog.md",
		"id: adr-001-widen-roots\ntype: adr\ntitle: Widen Roots\ncreated: 2026-01-01",
		"Body with unique token archroottoken.",
	)

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}

	var count int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'adr-001-widen-roots'`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected workspace-root architecture/ cogdoc indexed, got %d rows", count)
	}

	var ftsCount int
	if err := c.DB().QueryRow(
		`SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'archroottoken'`,
	).Scan(&ftsCount); err != nil {
		t.Fatalf("FTS query: %v", err)
	}
	if ftsCount == 0 {
		t.Error("expected workspace-root architecture/ cogdoc to be FTS-searchable, got 0 matches")
	}
}

// TestIndexWorkspaceSymlinkAliasNoDuplicateRow is the regression test for the
// symlink-resolved-path half of this change: the same real file reached via
// two different literal path strings — a symlink under .cog/mem/ and its
// target under the widened workspace-root architecture/ root — must produce
// exactly one documents row, not two. Before symlink resolution, the two
// alias paths would compute different auto-generated IDs (the ID derivation
// in parseCogdoc keys off finding ".cog/" in the path) and neither the `id`
// PRIMARY KEY nor the `path` UNIQUE constraint would catch the resulting
// duplicate.
func TestIndexWorkspaceSymlinkAliasNoDuplicateRow(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	writeCogdocsYaml(t, c, []string{"architecture/"})

	realPath := writeFileAt(t, c, "architecture/foo.cog.md",
		"id: arch-foo\ntype: adr\ntitle: Arch Foo\ncreated: 2026-01-01",
		"Body with unique token symlinktoken.",
	)

	memSemanticDir := filepath.Join(c.root, ".cog", "mem", "semantic")
	if err := os.MkdirAll(memSemanticDir, 0755); err != nil {
		t.Fatalf("mkdir .cog/mem/semantic: %v", err)
	}
	aliasPath := filepath.Join(memSemanticDir, "foo-alias.cog.md")
	if err := os.Symlink(realPath, aliasPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}

	// Exactly one row for the doc's ID, regardless of which alias path
	// discovered it first.
	var docCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'arch-foo'`).Scan(&docCount); err != nil {
		t.Fatalf("documents count: %v", err)
	}
	if docCount != 1 {
		t.Errorf("expected exactly 1 documents row for the doc reached via both a symlink and its real path, got %d", docCount)
	}

	// And no stray second row under the alias's own (unresolved) path or any
	// other id — total .cog.md rows in the workspace should be exactly 1.
	var totalCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE path LIKE '%.cog.md'`).Scan(&totalCount); err != nil {
		t.Fatalf("total count: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("expected exactly 1 total cogdoc row (symlink alias must not produce a second), got %d", totalCount)
	}
}

// TestIndexWorkspacePrunesWholesaleDeletedDirectoryOutsideMem is the
// regression test for issue #552: the previous ghost-prune pass scoped
// candidates to '%/.cog/mem/%.cog.md' only, so deleting an entire non-mem
// managed directory (e.g. .cog/adr/, before it moved to architecture/adrs/)
// left every row under it behind forever — the walk simply never visited
// those rows again (the directory is gone) and the old prune query never even
// considered them, because they weren't under .cog/mem/. After the fix, a
// from-scratch rebuild drops every documents row not visited this run,
// regardless of which managed directory it lived under.
func TestIndexWorkspacePrunesWholesaleDeletedDirectoryOutsideMem(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	adrDir := filepath.Join(c.root, ".cog", "adr")
	writeFileAt(t, c, ".cog/adr/001-example.cog.md",
		"id: adr-001-example\ntype: adr\ntitle: Example ADR\ncreated: 2026-01-01",
		"Body that will be removed wholesale. Token wholesaletoken.",
	)
	writeFileAt(t, c, ".cog/mem/semantic/keep.cog.md",
		"id: mem-keep\ntype: note\ntitle: Keep\ncreated: 2026-01-01",
		"Body that survives.",
	)

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("first IndexWorkspace: %v", err)
	}
	for _, id := range []string{"adr-001-example", "mem-keep"} {
		var n int
		if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = ?`, id).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", id, err)
		}
		if n != 1 {
			t.Fatalf("expected %s indexed after first pass, got %d", id, n)
		}
	}

	// Remove the whole .cog/adr/ directory, not just the one file inside it.
	if err := os.RemoveAll(adrDir); err != nil {
		t.Fatalf("remove adr dir: %v", err)
	}

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("second IndexWorkspace: %v", err)
	}

	var adrCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'adr-001-example'`).Scan(&adrCount); err != nil {
		t.Fatalf("adr count: %v", err)
	}
	if adrCount != 0 {
		t.Errorf("expected adr-001-example pruned after its whole directory was removed, got %d rows", adrCount)
	}

	var keepCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'mem-keep'`).Scan(&keepCount); err != nil {
		t.Fatalf("keep count: %v", err)
	}
	if keepCount != 1 {
		t.Errorf("expected mem-keep to survive the prune, got %d rows", keepCount)
	}
}

// TestParseCogdocAutoIDPortableUnderSymlinkedWorkspaceRoot is the regression
// test for the cog-review finding on parseCogdoc's auto-generated-ID
// fallback: it computed filepath.Rel against the unresolved workspace root
// (c.root, stored verbatim by Open) while every path actually reaching
// parseCogdoc from the walk is symlink-RESOLVED. On any workspace whose root
// itself sits behind a symlink (a very ordinary situation — macOS temp dirs,
// Nix store paths, symlinked checkouts/bind mounts), filepath.Rel returned a
// ".."-prefixed string, the portability guard rejected it, and the code
// silently fell back to baking the raw resolved absolute path into the ID —
// exactly the non-portable outcome the fallback exists to prevent. Re-running
// IndexWorkspace from a different absolute checkout location must produce the
// same ID for the same logical file.
func TestParseCogdocAutoIDPortableUnderSymlinkedWorkspaceRoot(t *testing.T) {
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real-workspace")
	if err := os.MkdirAll(realRoot, 0755); err != nil {
		t.Fatalf("mkdir real workspace root: %v", err)
	}
	linkRoot := filepath.Join(parent, "workspace-link")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Fatalf("symlink workspace root: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(linkRoot, ".cog", ".state"), 0755); err != nil {
		t.Fatalf("mkdir .cog/.state: %v", err)
	}
	c, err := Open(linkRoot)
	if err != nil {
		if err.Error() == "failed to initialize schema: no such module: fts5" {
			t.Skip("FTS5 not available (build with -tags fts5)")
		}
		t.Fatalf("Open(symlinked root): %v", err)
	}
	defer c.Close()

	writeCogdocsYaml(t, c, []string{"architecture/"})

	// No `id:` field — exercises the auto-generated-ID fallback.
	writeFileAt(t, c, "architecture/adrs/002-foo.cog.md",
		"type: adr\ntitle: Foo\ncreated: 2026-01-01",
		"Body with unique token symlinkedroottoken.",
	)

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}

	// The ID must be derived from the path relative to the workspace root
	// ("architecture-adrs-002-foo"), not from the raw absolute filesystem
	// path (which would embed the symlink-resolved parent-dir segments and
	// differ across checkouts/CI runners).
	const wantID = "architecture-adrs-002-foo"
	var count int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = ?`, wantID).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		var allIDs []string
		rows, qerr := c.DB().Query(`SELECT id FROM documents`)
		if qerr == nil {
			defer rows.Close()
			for rows.Next() {
				var id string
				if rows.Scan(&id) == nil {
					allIDs = append(allIDs, id)
				}
			}
		}
		t.Errorf("expected exactly 1 row with portable id %q, got %d (all ids: %v)", wantID, count, allIDs)
	}
}

// TestIndexWorkspacePrunesGhostRowAfterExternalSymlinkAliasRemoved is the
// regression test for the cog-review finding on pruneGhostCogdocs's
// root-scoping: a cogdoc reached only through a symlink alias (under a
// walked root) whose target resolves OUTSIDE every walked root must still be
// recognized and pruned once that alias is removed. Before the fix,
// documents.path stored the symlink-resolved EXTERNAL path (outside every
// root), so isUnderAnyRoot rejected the row as "not a managed cogdoc" and it
// became a permanent ghost — even though its only access path from within
// the workspace no longer existed.
func TestIndexWorkspacePrunesGhostRowAfterExternalSymlinkAliasRemoved(t *testing.T) {
	c, cleanup := openTestDB(t)
	defer cleanup()

	externalDir := t.TempDir()
	externalPath := filepath.Join(externalDir, "external.cog.md")
	if err := os.WriteFile(externalPath,
		[]byte("---\nid: external-doc\ntype: note\ntitle: External\ncreated: 2026-01-01\n---\n\nBody outside the workspace entirely."),
		0644,
	); err != nil {
		t.Fatalf("write external file: %v", err)
	}

	memSemanticDir := filepath.Join(c.root, ".cog", "mem", "semantic")
	if err := os.MkdirAll(memSemanticDir, 0755); err != nil {
		t.Fatalf("mkdir .cog/mem/semantic: %v", err)
	}
	aliasPath := filepath.Join(memSemanticDir, "external-alias.cog.md")
	if err := os.Symlink(externalPath, aliasPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("first IndexWorkspace: %v", err)
	}
	var firstCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'external-doc'`).Scan(&firstCount); err != nil {
		t.Fatalf("count after first index: %v", err)
	}
	if firstCount != 1 {
		t.Fatalf("expected external-doc indexed via the alias, got %d rows", firstCount)
	}

	// Remove only the alias. The external target file at externalPath still
	// exists on disk, but it is no longer reachable through any root this
	// workspace walks.
	if err := os.Remove(aliasPath); err != nil {
		t.Fatalf("remove alias: %v", err)
	}

	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("second IndexWorkspace: %v", err)
	}

	var secondCount int
	if err := c.DB().QueryRow(`SELECT COUNT(*) FROM documents WHERE id = 'external-doc'`).Scan(&secondCount); err != nil {
		t.Fatalf("count after second index: %v", err)
	}
	if secondCount != 0 {
		t.Errorf("expected external-doc pruned as a ghost after its only alias was removed, got %d rows (permanent ghost row)", secondCount)
	}
}
