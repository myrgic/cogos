package constellation

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ftsMu serialises all FTS mutations (rebuildFTS and upsertFTSRow).
// The underlying SQLite connection is single-writer, but multi-statement
// sequences (delete + insert) must not interleave with a concurrent full
// rebuild.
var ftsMu sync.Mutex

// indexMu serialises whole indexing operations (IndexFile / IndexWorkspace).
//
// The connection pool is MaxOpenConns(1), so at most one *statement* runs at a
// time — but a single indexing operation is a MULTI-statement unit: db.Begin()
// opens a transaction, indexCogdoc runs several Exec/Query calls, tx.Commit()
// closes it, and then upsertFTSRow issues its own SAVEPOINT/DELETE/INSERT/RELEASE
// sequence as autocommit statements. If two IndexFile calls interleave on the
// single connection, one goroutine's SAVEPOINT (a transaction-control statement)
// can be issued while another goroutine holds an open db.Begin() transaction on
// the same connection, producing "cannot start a transaction within a
// transaction" — a failure that was previously swallowed by a slog.Warn, silently
// dropping the index update. Holding indexMu for the whole operation makes
// concurrent IndexFile calls safe and lossless (they serialise instead of
// racing). ftsMu still guards upsertFTSRow vs. rebuildFTS for callers that hold
// neither (none today, but the invariant is preserved).
var indexMu sync.Mutex

// Cogdoc represents a parsed cogdoc with frontmatter and content.
type Cogdoc struct {
	ID               string
	Path             string
	Type             string
	Title            string
	Created          string
	Updated          string
	Sector           string
	Status           string
	Salience         string
	Confidence       string
	Ingested         string
	Tags             []string
	Refs             []Reference
	Content          string
	FrontmatterBytes int // Size of YAML frontmatter in bytes
}

// Reference represents a document reference from frontmatter.
type Reference struct {
	URI string
	Rel string
}

// IndexWorkspace scans the workspace and indexes all cogdocs.
func (c *Constellation) IndexWorkspace() error {
	// Serialise against concurrent IndexFile calls: both open a db.Begin()
	// transaction on the single-writer connection, and interleaving them
	// corrupts transaction state (see indexMu). A full rebuild must own the
	// connection for the duration of its transaction + FTS rebuild.
	indexMu.Lock()
	defer indexMu.Unlock()

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Fix 3: Transaction Rollback Safety
	// Defer rollback with proper error handling
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			fmt.Fprintf(os.Stderr, "Warning: transaction rollback failed: %v\n", err)
		}
	}()

	indexed := 0
	skipped := 0

	// NOTE: an earlier revision purged "stale" rows whose path did not begin with the
	// current workspace root (DELETE ... WHERE path NOT LIKE <root>/%). That was unsafe:
	// the documents table also holds rows indexed from OUTSIDE the workspace root (e.g.
	// conversation/session documents under ~/.claude), so a blanket prefix-negation DELETE
	// would remove legitimately-indexed rows — and an unescaped LIKE pattern from the root
	// path could match unintended rows via the _/% metacharacters. Orphan cleanup (removing
	// rows whose underlying file no longer exists) belongs in a dedicated stat-based sweep,
	// not a path-prefix DELETE; re-indexing below is idempotent and does not require it.

	// Walk roots: the workspace .cog/ directory, plus any workspace-root cogdoc
	// directories declared in .cog/config/cogdocs.yaml's requiredPaths (e.g.
	// architecture/, introduced by the v2 migration moving the ADR/RFC corpus
	// out of .cog/adr into architecture/adrs/ — see walkRoots for the derivation
	// and issue #552's "Related" note on the transitional .cog/architecture
	// symlink). visited tracks every *.cog.md file actually encountered this
	// run, keyed by its symlink-RESOLVED real path, so:
	//   - the same real file reached via two different literal path strings
	//     (a symlink alias and its target, e.g. .cog/architecture/foo.cog.md
	//     and architecture/foo.cog.md) is indexed exactly once instead of
	//     producing two documents rows for one file (both id and path would
	//     differ between the aliases, so neither UNIQUE constraint would catch
	//     it); and
	//   - pruneGhostCogdocs below can tell "not visited" apart from "failed to
	//     parse" (a malformed-but-still-present doc must keep its stale row,
	//     not be treated as a ghost — see TestIndexWorkspaceToleratesMalformedFrontmatter).
	roots := c.walkRoots()
	visited := make(map[string]bool)
	for _, walkRoot := range roots {
		err = filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			// Skip .state directory
			if d.IsDir() && d.Name() == ".state" {
				return fs.SkipDir
			}

			// Index *.cog.md files
			if !d.IsDir() && strings.HasSuffix(d.Name(), ".cog.md") {
				realPath, rerr := filepath.EvalSymlinks(path)
				if rerr != nil {
					// Resolution failure (e.g. a broken symlink) — fall back to the
					// walked path; indexCogdoc's os.ReadFile will surface the real
					// error if the file truly can't be read.
					realPath = path
				}
				if visited[realPath] {
					// Already indexed via another walk root or a different alias
					// path for the same real file this run.
					return nil
				}
				visited[realPath] = true

				// Storage path: normally the symlink-resolved realPath, so two
				// alias paths reaching the same real file (both under a walked
				// root) collapse to one row — the dedup this PR's widened roots
				// depend on. But when realPath resolves OUTSIDE every walked
				// root (an alias under a managed root pointing at an external
				// file), storing the external realPath would make the row
				// permanently invisible to pruneGhostCogdocs's isUnderAnyRoot
				// scoping: if this alias is later removed, no root reaches that
				// external path again, visited[] would never contain it on a
				// later run, yet isUnderAnyRoot(realPath, roots) is false so
				// the ghost sweep would skip it as "not managed" rather than
				// pruning it -- a permanent ghost row, the exact defect class
				// this PR exists to eliminate. Store the walked alias path
				// instead in that case: it IS under a root, so removing the
				// alias correctly makes the row a prunable ghost on the next
				// run, same as any other deleted in-root file.
				storagePath := realPath
				if !isUnderAnyRoot(realPath, roots) {
					storagePath = path
				}
				visited[storagePath] = true

				if err := c.indexCogdoc(tx, storagePath); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: failed to index %s: %v\n", storagePath, err)
					skipped++
				} else {
					indexed++
				}
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to walk workspace root %s: %w", walkRoot, err)
		}
	}

	// Prune ghost rows: cogdoc rows under any walked root whose backing file was
	// not visited this run — either because the individual file was deleted, or
	// because a whole directory was removed wholesale (issue #552: deleting
	// .cog/adr/ entirely left 93 rows behind because the old sweep only scoped
	// to '%/.cog/mem/%.cog.md' and only ever considered rows the walk revisited).
	// Re-indexing is additive (INSERT OR REPLACE) and never removes rows for
	// deleted files, so without this sweep a deleted cogdoc's row (and its FTS
	// entry, tags, refs) persist forever. Scope is derived from the same roots
	// list the walk just used, so rows indexed from outside the workspace's
	// managed cogdoc directories (e.g. conversation/session documents) are never
	// touched. tags / doc_references / backlinks cascade on delete; documents_fts
	// is rebuilt from documents below.
	pruned, err := c.pruneGhostCogdocs(tx, roots, visited)
	if err != nil {
		return fmt.Errorf("failed to prune ghost cogdocs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Fix 1: Two-pass URI resolution
	// After all documents are indexed, resolve unresolved references
	// This fixes the chicken-and-egg problem where doc A references doc B
	// but B hasn't been indexed yet during A's indexing
	fmt.Printf("Resolving unresolved references (second pass)...\n")
	resolved, err := c.resolveUnresolvedRefs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to resolve refs: %v\n", err)
	} else {
		fmt.Printf("Resolved %d additional references\n", resolved)
	}

	// Fix 2: Rebuild FTS index to sync tags
	// Tags are inserted AFTER documents, so we need to rebuild FTS after commit
	fmt.Printf("Rebuilding FTS index with tags...\n")
	if err := c.rebuildFTS(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to rebuild FTS: %v\n", err)
	}

	if skipped > 0 {
		fmt.Printf("Indexed %d cogdocs (%d skipped — see warnings above)\n", indexed, skipped)
	} else {
		fmt.Printf("Indexed %d cogdocs\n", indexed)
	}
	if pruned > 0 {
		fmt.Printf("Pruned %d ghost cogdoc rows (files removed from disk)\n", pruned)
	}

	// Async: trigger embedding backfill if embed client is configured
	if c.embedClient != nil {
		go func() {
			indexer := NewEmbedIndexer(c, c.embedClient)
			n, err := indexer.BackfillAll(20)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[embed-indexer] backfill error: %v\n", err)
			} else if n > 0 {
				fmt.Fprintf(os.Stderr, "[embed-indexer] backfilled %d documents\n", n)
			}
		}()
	}

	// Per-doc parse/index failures are warnings, not command failures: the
	// walk, commit, ref-resolution, and FTS rebuild all succeeded, so the index
	// is consistent for every doc that could be parsed. Return nil so callers
	// (e.g. cogos reindex) exit 0 on a successful-with-skips run. The warnings
	// printed above are the signal for the skipped documents.
	return nil
}

// pruneGhostCogdocs removes documents rows for managed cogdocs that this run's
// walk did not visit — i.e. their backing file no longer exists, whether
// because the individual file was deleted or because an entire directory was
// removed wholesale. It is the orphan sweep referenced in IndexWorkspace's
// design note: re-indexing is additive and never deletes rows, so deleted
// cogdocs would otherwise leave permanent ghost rows in the index.
//
// Scope: a row is a ghost candidate only if its path falls under one of the
// roots this run walked (isUnderAnyRoot) AND ends in ".cog.md" — the exact set
// of files the walk claims to own. Rows indexed from outside that scope (e.g.
// conversation/session documents under ~/.claude) never match a root prefix
// and are left untouched. Fix for issue #552: the previous version scoped
// candidates to the SQL pattern '%/.cog/mem/%.cog.md' only, so rows under any
// other managed directory (.cog/adr/, the newly-widened workspace-root roots)
// were never even considered — deleting .cog/adr/ wholesale left 93 ghost rows
// behind. Membership in `visited` (populated by the walk in IndexWorkspace,
// keyed by symlink-resolved real path) replaces the old per-row os.Stat call:
// a row not visited this run is a ghost by definition, whether the underlying
// file was individually deleted or its whole parent directory is gone.
//
// Deletes cascade to tags / doc_references / backlinks (schema FKs, ON DELETE
// CASCADE); the caller rebuilds documents_fts from documents afterward, so no
// explicit FTS delete is needed here. Returns the number of rows pruned.
func (c *Constellation) pruneGhostCogdocs(tx *sql.Tx, roots []string, visited map[string]bool) (int, error) {
	rows, err := tx.Query(
		`SELECT id, path FROM documents WHERE path LIKE '%.cog.md'`,
	)
	if err != nil {
		return 0, fmt.Errorf("select cogdoc rows: %w", err)
	}

	type ghost struct {
		id   string
		path string
	}
	var ghosts []ghost
	for rows.Next() {
		var id, path string
		if err := rows.Scan(&id, &path); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan cogdoc row: %w", err)
		}
		if !isUnderAnyRoot(path, roots) {
			// Not a managed cogdoc under any root this run walked. Usually
			// this is a legitimate out-of-workspace row (e.g. conversation/
			// session documents) that isUnderAnyRoot correctly leaves alone.
			// But a row can also land here because it was indexed through a
			// symlink alias inside a walked root whose target resolves
			// OUTSIDE every root (documents.path stores the symlink-resolved
			// real path, per IndexFile/IndexWorkspace) — if that alias is
			// later removed, the row is permanently invisible to
			// isUnderAnyRoot and would never be pruned, even though its
			// backing file access path is gone. Distinguish the two cases
			// with a direct stat: only a row whose file no longer exists at
			// all is a ghost here — a legitimate out-of-workspace row whose
			// file is still present is left untouched, same as before.
			if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
				ghosts = append(ghosts, ghost{id: id, path: path})
			}
			continue
		}
		if !visited[path] {
			ghosts = append(ghosts, ghost{id: id, path: path})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate cogdoc rows: %w", err)
	}
	// Close before issuing DELETEs: the single-writer connection cannot run a
	// mutation while this cursor is still open.
	rows.Close()

	pruned := 0
	for _, g := range ghosts {
		if _, err := tx.Exec("DELETE FROM documents WHERE id = ?", g.id); err != nil {
			return pruned, fmt.Errorf("delete ghost %s (%s): %w", g.id, g.path, err)
		}
		pruned++
	}
	return pruned, nil
}

// isUnderAnyRoot reports whether path is exactly one of roots, or nested
// under one of them. roots are expected to already be absolute, cleaned
// (filepath.Join / filepath.EvalSymlinks output), matching how documents.path
// is stored (see indexCogdoc, which writes the walk-encountered — and now
// symlink-resolved — path verbatim).
func isUnderAnyRoot(path string, roots []string) bool {
	for _, root := range roots {
		if path == root || strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// cogdocsConfig is the declared shape of .cog/config/cogdocs.yaml relevant to
// indexing. The file also carries exemptPatterns / conventionalFiles /
// validTypes for cogdoc *validation*, which indexing has no use for and does
// not parse.
type cogdocsConfig struct {
	// RequiredPaths lists workspace-relative cogdoc directories the workspace
	// declares as canonical (e.g. "architecture/", ".cog/mem/semantic/") — see
	// e.g. .cog/config/cogdocs.yaml's requiredPaths. walkRoots uses this to
	// derive extra walk roots outside .cog/ rather than hardcoding them.
	RequiredPaths []string `yaml:"requiredPaths"`
}

// loadCogdocsConfig reads the optional .cog/config/cogdocs.yaml declaration,
// following the .cog/config/<name>.yaml convention used elsewhere in this
// codebase (see internal/providers/vitalsretention.LoadConfig for the sibling
// pattern: missing file → zero value, not an error). A workspace with no
// cogdocs.yaml simply declares no extra required paths.
func loadCogdocsConfig(workspaceRoot string) (cogdocsConfig, error) {
	path := filepath.Join(workspaceRoot, ".cog", "config", "cogdocs.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cogdocsConfig{}, nil
		}
		return cogdocsConfig{}, err
	}
	var cfg cogdocsConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cogdocsConfig{}, err
	}
	return cfg, nil
}

// walkRoots returns the absolute, symlink-resolved directories IndexWorkspace
// should walk: the workspace's .cog/ directory (always — indexing has always
// required this to exist), plus any cogdocs.yaml requiredPaths entries that
// fall OUTSIDE .cog/ — e.g. the workspace-root architecture/ directory the v2
// migration moved the ADR/RFC corpus into (issue #552's "Related" note: the
// walker previously covered only .cog/, so a corpus living at the workspace
// root was invisible to it once the transitional .cog/architecture symlink
// retires).
//
// requiredPaths entries already nested under .cog/ (e.g. ".cog/mem/semantic/")
// are skipped: the base .cog/ walk already covers them, and adding them again
// would just be wasted work (each root is walked once; recall indexCogdoc is
// idempotent per real path, so it wouldn't be incorrect — just redundant).
//
// Every returned root is passed through filepath.EvalSymlinks so a root that
// is itself reached via a symlink resolves to the same canonical form
// indexCogdoc will store for files under it, keeping pruneGhostCogdocs'
// root-prefix membership check (isUnderAnyRoot) consistent with what's
// actually in the documents table.
//
// A missing or unreadable cogdocs.yaml is not an error — it just means no
// extra roots are declared; the walk still covers .cog/.
func (c *Constellation) walkRoots() []string {
	cogRoot := filepath.Join(c.root, ".cog")
	resolvedCogRoot, err := filepath.EvalSymlinks(cogRoot)
	if err != nil {
		// .cog/ doesn't exist yet (e.g. a brand-new workspace) — the walk below
		// will simply find nothing there; fall back to the unresolved form so
		// there's still a root to attempt (and a meaningful error if it's
		// genuinely absent, matching pre-existing behavior).
		resolvedCogRoot = cogRoot
	}

	roots := []string{resolvedCogRoot}
	seen := map[string]bool{resolvedCogRoot: true}

	cfg, err := loadCogdocsConfig(c.root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to read .cog/config/cogdocs.yaml: %v\n", err)
		return roots
	}

	for _, rel := range cfg.RequiredPaths {
		abs := rel
		if !filepath.IsAbs(rel) {
			abs = filepath.Join(c.root, rel)
		}
		resolved, evalErr := filepath.EvalSymlinks(abs)
		if evalErr != nil {
			// Declared path doesn't exist on disk (not yet created, or since
			// removed) — a config declaration is not a guarantee, and a missing
			// directory here is not a walk error.
			continue
		}
		if seen[resolved] {
			continue
		}
		if resolved == resolvedCogRoot || strings.HasPrefix(resolved, resolvedCogRoot+string(filepath.Separator)) {
			// Already covered by the base .cog/ walk.
			continue
		}
		seen[resolved] = true
		roots = append(roots, resolved)
	}
	return roots
}

// ExtraCogdocRoots returns the workspace-root cogdoc directories declared via
// .cog/config/cogdocs.yaml requiredPaths that fall OUTSIDE .cog/ — the same
// extra roots walkRoots() adds to the batch IndexWorkspace walk, minus the
// always-present .cog/ base root. Exported so live incremental watchers
// (internal/engine's MemWatcher, which only observes .cog/mem by default)
// can also watch these declared roots, keeping live indexing consistent with
// what a full reindex covers. Does not require an open database — only reads
// cogdocs.yaml from disk — so callers need not go through Open().
func ExtraCogdocRoots(workspaceRoot string) []string {
	c := &Constellation{root: workspaceRoot}
	cogRoot := filepath.Join(workspaceRoot, ".cog")
	resolvedCogRoot, err := filepath.EvalSymlinks(cogRoot)
	if err != nil {
		resolvedCogRoot = cogRoot
	}
	var extra []string
	for _, root := range c.walkRoots() {
		if root == resolvedCogRoot {
			continue
		}
		extra = append(extra, root)
	}
	return extra
}

// IndexFile indexes a single cogdoc file into the constellation.
// This is the public entry point for incremental indexing (e.g., after
// a decomposition stores a new CogDoc). It handles its own transaction,
// FTS rebuild for the affected document, and optional async embedding.
func (c *Constellation) IndexFile(path string) error {
	// Serialise the whole operation (tx + commit + FTS upsert) against other
	// IndexFile / IndexWorkspace callers. On the MaxOpenConns(1) pool an
	// interleaved SAVEPOINT (from upsertFTSRow) and an open db.Begin() tx on the
	// same connection otherwise trigger "cannot start a transaction within a
	// transaction", which was silently swallowed and dropped the index update.
	indexMu.Lock()
	defer indexMu.Unlock()

	// Resolve symlinks before indexing, matching IndexWorkspace's walk loop.
	// fsnotify callers (mem_watcher.go) pass the raw, unresolved event path,
	// which for a symlink alias (e.g. .cog/mem/semantic/foo-alias.cog.md ->
	// ../../../architecture/foo.cog.md, the migration pattern this package's
	// widened walkRoots exists to support) differs from the path a later
	// IndexWorkspace walk will derive for the same real file. Without this
	// resolution, parseCogdoc's auto-ID fallback (keyed off the literal path
	// string) can mint two different IDs for one real file — the alias path
	// contains ".cog/" and the resolved target path does not — producing two
	// permanent documents rows for a single file that neither the id PRIMARY
	// KEY nor the path UNIQUE constraint would catch.
	realPath, rerr := filepath.EvalSymlinks(path)
	if rerr != nil {
		// Resolution failure (e.g. a broken symlink, or the file was removed
		// between the fsnotify event and this call) — fall back to the raw
		// path; indexCogdoc's os.ReadFile will surface the real error if the
		// file truly can't be read.
		realPath = path
	}

	// Storage path: see the matching comment in IndexWorkspace's walk loop.
	// When realPath resolves outside every managed root, store the walked
	// alias path instead so pruneGhostCogdocs's root-scoping can still
	// recognize and prune the row once the alias is removed.
	storagePath := realPath
	if !isUnderAnyRoot(realPath, c.walkRoots()) {
		storagePath = path
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			fmt.Fprintf(os.Stderr, "Warning: IndexFile rollback: %v\n", err)
		}
	}()

	if err := c.indexCogdoc(tx, storagePath); err != nil {
		return err
	}

	// Look up the doc id inside the transaction so we get the row even if it
	// was just inserted (not yet committed to the reader connection).
	var docID string
	idErr := tx.QueryRow("SELECT id FROM documents WHERE path = ?", storagePath).Scan(&docID)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	committed = true

	if idErr != nil {
		// Row absent should not happen: indexCogdoc always upserts a row for the
		// path (even on unchanged-hash it now refreshes file_mtime in place).
		// Treat as "nothing more to do" rather than an error.
		return nil
	}

	// Targeted O(1) FTS upsert — avoids full table rebuild on every write.
	if err := c.upsertFTSRow(docID); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: FTS upsert after IndexFile: %v\n", err)
	}

	// Async embedding if configured
	if c.embedClient != nil {
		go func() {
			indexer := NewEmbedIndexer(c, c.embedClient)
			if _, err := indexer.BackfillAll(1); err != nil {
				fmt.Fprintf(os.Stderr, "[embed-indexer] single-doc backfill error: %v\n", err)
			}
		}()
	}

	return nil
}

// indexCogdoc parses and indexes a single cogdoc file.
func (c *Constellation) indexCogdoc(tx *sql.Tx, path string) error {
	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Get file mtime
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	mtime := info.ModTime().Format(time.RFC3339)

	// Parse cogdoc
	doc, err := parseCogdoc(data, path, c.root)
	if err != nil {
		return err
	}

	// Compute content hash over the full raw file bytes (frontmatter + body),
	// not just doc.Content (body only). Hashing the body alone let a
	// frontmatter-only edit (refs, tags, title, status, ...) hit the
	// unchanged-hash early return below and skip the tags/doc_references
	// DELETE+INSERT, silently leaving stale graph edges. The stored `content`
	// column below still uses doc.Content (body only) — only the
	// staleness-detection hash changed.
	contentHash := fmt.Sprintf("%x", sha256.Sum256(data))

	// Check if already indexed with same hash
	var existingHash string
	err = tx.QueryRow("SELECT content_hash FROM documents WHERE path = ?", path).Scan(&existingHash)
	if err == nil && existingHash == contentHash {
		// Content unchanged, but the file may have been touched (mtime bumped
		// with identical bytes — e.g. an editor rewrite or `touch`). The stored
		// file_mtime is the ONLY signal the engine's lazy drift-repair uses to
		// decide a row is stale; if we return here without refreshing it, the
		// drift check flags this document as drifted on every search, forever,
		// and re-indexes it needlessly. Cheaply update the stored mtime so a
		// touched-but-unchanged file reports no drift on the next pass.
		if _, uerr := tx.Exec(
			"UPDATE documents SET file_mtime = ? WHERE path = ?", mtime, path,
		); uerr != nil {
			return fmt.Errorf("failed to refresh file_mtime on unchanged doc: %w", uerr)
		}
		return nil
	}

	// Calculate stats
	wordCount := len(strings.Fields(doc.Content))
	lineCount := strings.Count(doc.Content, "\n") + 1

	// Calculate substance metrics
	frontmatterBytes := doc.FrontmatterBytes
	contentBytes := len(doc.Content)
	substanceRatio := 0.0
	if frontmatterBytes+contentBytes > 0 {
		substanceRatio = float64(contentBytes) / float64(contentBytes+frontmatterBytes)
	}
	refCount := len(doc.Refs)
	refDensity := 0.0
	if contentBytes > 0 {
		refDensity = float64(refCount) / (float64(contentBytes) / 1024.0) // refs per KB
	}

	// CRITICAL-6: check for ID collision before insert.
	// If another document at a different path already claims this ID, log it to
	// migration_conflicts for human review. We still proceed with INSERT OR REPLACE
	// (last-write wins) so indexing is not blocked, but the collision is recorded.
	var existingPath string
	if collErr := tx.QueryRow(
		"SELECT path FROM documents WHERE id = ? AND path != ?", doc.ID, path,
	).Scan(&existingPath); collErr == nil {
		// Collision detected: record in migration_conflicts
		_, _ = tx.Exec(
			"INSERT INTO migration_conflicts (candidate_path, existing_path, detected_at) VALUES (?, ?, ?)",
			path, existingPath, time.Now().Format(time.RFC3339),
		)
		fmt.Fprintf(os.Stderr, "Warning: ID collision for %q: %s conflicts with %s\n",
			doc.ID, path, existingPath)
	}

	// Insert or replace document (includes new canonical schema columns)
	_, err = tx.Exec(`
		INSERT OR REPLACE INTO documents (
			id, path, type, title, created, updated, sector, status,
			content, content_hash, word_count, line_count,
			indexed_at, file_mtime,
			frontmatter_bytes, content_bytes, substance_ratio, ref_count, ref_density,
			ingested, salience, confidence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, doc.ID, path, doc.Type, doc.Title, doc.Created, doc.Updated, doc.Sector, doc.Status,
		doc.Content, contentHash, wordCount, lineCount, time.Now().Format(time.RFC3339), mtime,
		frontmatterBytes, contentBytes, substanceRatio, refCount, refDensity,
		doc.Ingested, doc.Salience, doc.Confidence)

	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	// Delete old tags and doc_references
	if _, err := tx.Exec("DELETE FROM tags WHERE document_id = ?", doc.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM doc_references WHERE source_id = ?", doc.ID); err != nil {
		return err
	}

	// Insert tags
	for _, tag := range doc.Tags {
		_, err := tx.Exec("INSERT INTO tags (document_id, tag) VALUES (?, ?)", doc.ID, tag)
		if err != nil {
			return fmt.Errorf("failed to insert tag: %w", err)
		}
	}

	// Insert doc_references
	for _, ref := range doc.Refs {
		// Try to resolve target_id from URI
		targetID := resolveURIToID(tx, ref.URI)

		_, err := tx.Exec(`
			INSERT INTO doc_references (source_id, target_uri, target_id, relation)
			VALUES (?, ?, ?, ?)
		`, doc.ID, ref.URI, targetID, ref.Rel)

		if err != nil {
			return fmt.Errorf("failed to insert reference: %w", err)
		}
	}

	return nil
}

// parseCogdoc parses frontmatter and content from a cogdoc file. workspaceRoot
// is used only for the auto-generated-ID fallback below (a path outside
// .cog/, e.g. a widened cogdocs.yaml requiredPaths root, has no ".cog/"
// substring to key off) — pass "" when no workspace root is known (e.g. unit
// tests exercising parsing in isolation), which preserves the pre-existing
// filename-derived fallback for that case.
func parseCogdoc(data []byte, path string, workspaceRoot string) (*Cogdoc, error) {
	// Split frontmatter and content
	parts := strings.SplitN(string(data), "---", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid cogdoc: missing frontmatter")
	}

	frontmatterYAML := parts[1]
	content := strings.TrimSpace(parts[2])

	// Parse frontmatter
	var fm struct {
		ID           string        `yaml:"id"`
		Type         string        `yaml:"type"`
		Title        string        `yaml:"title"`
		Created      string        `yaml:"created"`
		Updated      string        `yaml:"updated"`
		Modified     string        `yaml:"modified"` // CRITICAL-1: parse-time alias for updated
		Revised      string        `yaml:"revised"`  // CRITICAL-1: parse-time alias for updated
		Sector       string        `yaml:"sector"`
		MemorySector string        `yaml:"memory_sector"` // CRITICAL-1: parse-time alias for sector
		Status       string        `yaml:"status"`
		Salience     string        `yaml:"salience"`
		Confidence   string        `yaml:"confidence"`
		Ingested     string        `yaml:"ingested"`
		Tags         []string      `yaml:"tags"`
		Refs         []interface{} `yaml:"refs"`
		Authors      []string      `yaml:"authors"`
		Author       string        `yaml:"author"` // CRITICAL-1: singular alias for authors
		// CRITICAL-2: nested cog submap for RFC/spec frontmatter (cog.type → type, cog.id → id)
		Cog struct {
			Type string `yaml:"type"`
			ID   string `yaml:"id"`
		} `yaml:"cog"`
	}

	// Fix 8: YAML Parsing Robustness
	// Keep strict parsing but provide better error context
	if err := yaml.Unmarshal([]byte(frontmatterYAML), &fm); err != nil {
		// Enhanced error message with file context
		return nil, fmt.Errorf("failed to parse frontmatter in %s: %w", filepath.Base(path), err)
	}

	// CRITICAL-1: resolve memory_sector alias → sector
	if fm.Sector == "" && fm.MemorySector != "" {
		fm.Sector = fm.MemorySector
	}

	// CRITICAL-1: resolve updated field aliases
	if fm.Updated == "" && fm.Modified != "" {
		fm.Updated = fm.Modified
	}
	if fm.Updated == "" && fm.Revised != "" {
		fm.Updated = fm.Revised
	}

	// CRITICAL-2: lift cog submap fields to top-level for nested RFC/spec frontmatter
	if fm.Type == "" && fm.Cog.Type != "" {
		fm.Type = fm.Cog.Type
	}
	if fm.ID == "" && fm.Cog.ID != "" {
		fm.ID = fm.Cog.ID
	}

	// CRITICAL-3: lowercase status at parse time (D-10: no author burden)
	fm.Status = strings.ToLower(strings.TrimSpace(fm.Status))

	// CRITICAL-5: type-aware status enum validation — emit warning, not error
	humanStatuses := map[string]bool{
		"draft": true, "active": true, "canonical": true,
		"superseded": true, "retired": true, "": true,
	}
	machineStatuses := map[string]bool{
		"raw": true, "enriched": true, "completed": true, "": true,
	}
	machineTypes := map[string]bool{
		"conversation": true, "link": true, "session": true, "working-memory": true,
	}
	if machineTypes[fm.Type] {
		if !machineStatuses[fm.Status] {
			fmt.Fprintf(os.Stderr, "Warning: %s: machine-type %q doc has non-machine status %q\n",
				filepath.Base(path), fm.Type, fm.Status)
		}
	} else if fm.Type != "" {
		if !humanStatuses[fm.Status] {
			fmt.Fprintf(os.Stderr, "Warning: %s: human-type %q doc has non-canonical status %q\n",
				filepath.Base(path), fm.Type, fm.Status)
		}
	}

	// Fix 9: Empty Title Fallback
	// Implement title fallback cascade: frontmatter → H1 → filename
	if fm.Title == "" {
		// Try to extract from first H1 heading in content
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "# ") {
				fm.Title = strings.TrimSpace(strings.TrimPrefix(trimmed, "# "))
				break
			}
		}

		// If still empty, use filename without .cog.md extension
		if fm.Title == "" {
			fm.Title = strings.TrimSuffix(filepath.Base(path), ".cog.md")
		}
	}

	// Fix 10: Auto-generate ID from path if missing
	// This prevents all files without IDs from colliding on empty string
	if fm.ID == "" {
		// Generate ID from path relative to .cog/ (the common case).
		// Example: /path/to/.cog/mem/semantic/foo.cog.md → mem-semantic-foo
		relPath := path
		if idx := strings.Index(path, ".cog/"); idx != -1 {
			// Find .cog/ in path and take everything after it.
			relPath = path[idx+5:] // Skip ".cog/"
		} else if workspaceRoot != "" {
			// Widened walkRoots (cogdocs.yaml requiredPaths, e.g.
			// architecture/) puts files outside .cog/ with no ".cog/"
			// substring to key off. Falling back to the raw absolute path
			// would bake the checkout location into the ID — a different
			// clone or CI runner produces a different ID for the same
			// logical file, breaking cog: URI resolution and the id-keyed
			// dedup this same walk-widening depends on. Derive the ID from
			// the path relative to the workspace root instead, which is
			// portable across checkouts.
			//
			// path has already been through filepath.EvalSymlinks (both
			// IndexWorkspace's walk loop and IndexFile resolve it before
			// calling indexCogdoc), but workspaceRoot (c.root, stored
			// verbatim by Open) has not. On any workspace whose root
			// involves a symlink component (macOS /tmp, a Nix store path,
			// a symlinked checkout or bind mount) that mismatch makes
			// filepath.Rel return a ".."-prefixed string, which silently
			// falls through below and bakes the raw resolved absolute path
			// into the ID anyway — exactly the non-portable outcome this
			// fallback exists to avoid. Resolve workspaceRoot the same way
			// before comparing so both sides refer to the same real path.
			relRoot := workspaceRoot
			if resolvedRoot, rrErr := filepath.EvalSymlinks(workspaceRoot); rrErr == nil {
				relRoot = resolvedRoot
			}
			if rel, rerr := filepath.Rel(relRoot, path); rerr == nil && !strings.HasPrefix(rel, "..") {
				relPath = rel
			}
		}
		relPath = strings.TrimSuffix(relPath, ".cog.md")
		// Replace slashes and dots with dashes for a valid ID
		fm.ID = strings.ReplaceAll(strings.ReplaceAll(relPath, "/", "-"), ".", "-")
	}

	// Parse refs (can be simple strings or typed objects)
	var refs []Reference
	for _, refRaw := range fm.Refs {
		switch ref := refRaw.(type) {
		case string:
			refs = append(refs, Reference{URI: ref, Rel: "refs"})
		case map[string]interface{}:
			uri, _ := ref["uri"].(string)
			rel, _ := ref["rel"].(string)
			if rel == "" {
				rel = "refs"
			}
			refs = append(refs, Reference{URI: uri, Rel: rel})
		}
	}

	return &Cogdoc{
		ID:               fm.ID,
		Path:             path,
		Type:             fm.Type,
		Title:            fm.Title,
		Created:          fm.Created,
		Updated:          fm.Updated,
		Sector:           fm.Sector,
		Status:           fm.Status,
		Salience:         fm.Salience,
		Confidence:       fm.Confidence,
		Ingested:         fm.Ingested,
		Tags:             fm.Tags,
		Refs:             refs,
		Content:          content,
		FrontmatterBytes: len(frontmatterYAML),
	}, nil
}

// Fix 1: Implement URI Resolution
// resolveURIToID attempts to resolve a cog: URI to a document ID.
// Accepts both bare (cog:X/Y) and legacy authority form (cog://X/Y).
//
// Supported URI patterns:
//   - cog:mem/semantic/path/to/doc → .cog/mem/semantic/path/to/doc.cog.md
//   - cog:adr/004 → .cog/adr/004-*.cog.md (glob pattern)
//   - cog:kernel/path → .cog/kernel/path.cog.md
//   - cog:type/identifier → ID lookup
func resolveURIToID(tx *sql.Tx, uri string) sql.NullString {
	if !strings.HasPrefix(uri, "cog://") {
		return sql.NullString{Valid: false}
	}

	// PREREQ-2: normalize legacy cog://memory/ → cog://mem/ (D-14 canonical prefix)
	uri = strings.Replace(uri, "cog://memory/", "cog://mem/", 1)

	// Strip cog:// prefix
	path := strings.TrimPrefix(uri, "cog://")

	// Strip incorrect .cog.md suffix if present in URI
	path = strings.TrimSuffix(path, ".cog.md")

	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		return sql.NullString{Valid: false}
	}

	uriType := parts[0]

	switch uriType {
	case "mem":
		// cog:mem/semantic/insights/foo
		// → .cog/mem/semantic/insights/foo.cog.md
		return resolveByPath(tx, filepath.Join(".cog", path)+".cog.md")

	case "adr":
		// cog:adr/004 → .cog/adr/004-*.cog.md (glob pattern)
		// cog:adr/004-cogdoc-format → .cog/adr/004-cogdoc-format.cog.md (exact)
		if len(parts) < 2 {
			return sql.NullString{Valid: false}
		}
		adrID := parts[1]
		// If adrID contains hyphen, it's the full filename (exact match)
		// Otherwise it's just the number (use glob pattern)
		if strings.Contains(adrID, "-") {
			return resolveByPath(tx, filepath.Join(".cog/adr", adrID)+".cog.md")
		}
		return resolveByPattern(tx, ".cog/adr", adrID+"-*")

	case "kernel":
		// cog:kernel/path
		return resolveByPath(tx, filepath.Join(".cog", path)+".cog.md")

	case "term":
		// cog:term/thermal-time-world → .cog/ontology/vocabulary.cog.md (all terms in one file)
		// For now, try to resolve by ID (term name)
		if len(parts) < 2 {
			return sql.NullString{Valid: false}
		}
		termName := parts[1]
		return resolveByID(tx, termName)

	case "work":
		// cog:work/councils/xyz/synthesis → .cog/work/councils/xyz/synthesis.cog.md
		return resolveByPath(tx, filepath.Join(".cog", path)+".cog.md")

	case "rfc":
		// PREREQ-2: cog:rfc/NNN → .cog/conf/spec/rfc/RFC-NNN-*.cog.md (glob pattern)
		// cog:rfc/030 → .cog/conf/spec/rfc/RFC-030-*.cog.md
		// cog:rfc/30 → .cog/conf/spec/rfc/RFC-030-*.cog.md (zero-padded)
		if len(parts) < 2 {
			return sql.NullString{Valid: false}
		}
		rfcNum := parts[1]
		// Attempt numeric zero-pad to 3 digits; fall back to literal if not numeric
		var rfcPrefix string
		var n int
		if _, parseErr := fmt.Sscanf(rfcNum, "%d", &n); parseErr == nil {
			rfcPrefix = fmt.Sprintf("RFC-%03d", n)
		} else {
			rfcPrefix = "RFC-" + strings.ToUpper(rfcNum)
		}
		return resolveByPattern(tx, ".cog/conf/spec/rfc", rfcPrefix+"-*")

	case "conf":
		// cog:conf/spec/foo → .cog/conf/spec/foo.cog.md
		return resolveByPath(tx, filepath.Join(".cog", path)+".cog.md")

	default:
		// Generic: try ID lookup first, then path
		identifier := parts[len(parts)-1]
		return resolveByID(tx, identifier)
	}
}

// resolveByPath resolves a URI by path suffix match.
// Matches paths ending with the given suffix (e.g., ".cog/mem/path.cog.md")
// Handles date-prefixed filenames (e.g., 2026-01-14-name.cog.md)
func resolveByPath(tx *sql.Tx, path string) sql.NullString {
	var id string
	// First try exact suffix match
	likePattern := "%" + path
	err := tx.QueryRow("SELECT id FROM documents WHERE path LIKE ? LIMIT 1", likePattern).Scan(&id)
	if err == nil {
		return sql.NullString{String: id, Valid: true}
	}

	// If that fails, try with date prefix wildcard for episodic documents
	// Example: .cog/mem/episodic/handoffs/foo.cog.md
	//       → %.cog/mem/episodic/handoffs/%-foo.cog.md
	if strings.Contains(path, "/episodic/") {
		// Split path to insert wildcard before filename
		dir := filepath.Dir(path)
		filename := filepath.Base(path)
		dateWildcardPattern := "%" + dir + "/%-" + filename

		err = tx.QueryRow("SELECT id FROM documents WHERE path LIKE ? LIMIT 1", dateWildcardPattern).Scan(&id)
		if err == nil {
			return sql.NullString{String: id, Valid: true}
		}
	}

	return sql.NullString{Valid: false}
}

// resolveByPattern resolves a URI using LIKE pattern matching.
// Used for ADRs where the full filename isn't known (e.g., 004-*.cog.md).
func resolveByPattern(tx *sql.Tx, dir, pattern string) sql.NullString {
	// Convert glob pattern to SQL LIKE pattern with % prefix for absolute paths
	// Pattern: 004-* → %/.cog/adr/004-%.cog.md
	likePattern := "%" + filepath.Join(dir, pattern) + ".cog.md"
	likePattern = strings.ReplaceAll(likePattern, "*", "%")

	var id string
	err := tx.QueryRow(
		"SELECT id FROM documents WHERE path LIKE ? LIMIT 1",
		likePattern,
	).Scan(&id)
	if err == nil {
		return sql.NullString{String: id, Valid: true}
	}
	return sql.NullString{Valid: false}
}

// resolveByID resolves a URI by document ID.
func resolveByID(tx *sql.Tx, identifier string) sql.NullString {
	var id string
	err := tx.QueryRow("SELECT id FROM documents WHERE id = ?", identifier).Scan(&id)
	if err == nil {
		return sql.NullString{String: id, Valid: true}
	}
	return sql.NullString{Valid: false}
}

// resolveUnresolvedRefs performs a second pass to resolve references that
// failed during initial indexing (due to target documents not being indexed yet).
func (c *Constellation) resolveUnresolvedRefs() (int, error) {
	// Query all unresolved references
	rows, err := c.db.Query(`
		SELECT source_id, target_uri
		FROM doc_references
		WHERE target_id IS NULL AND target_uri LIKE 'cog://%'
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	// Collect unresolved refs
	type UnresolvedRef struct {
		SourceID  string
		TargetURI string
	}
	var unresolvedRefs []UnresolvedRef
	for rows.Next() {
		var ref UnresolvedRef
		if err := rows.Scan(&ref.SourceID, &ref.TargetURI); err != nil {
			return 0, err
		}
		unresolvedRefs = append(unresolvedRefs, ref)
	}

	if len(unresolvedRefs) == 0 {
		return 0, nil
	}

	// Start transaction for updates
	tx, err := c.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			fmt.Fprintf(os.Stderr, "Warning: transaction rollback failed: %v\n", err)
		}
	}()

	resolved := 0
	for _, ref := range unresolvedRefs {
		// Try to resolve now that all documents are indexed
		targetID := resolveURIToID(tx, ref.TargetURI)

		if targetID.Valid {
			// Update the reference with resolved target_id
			_, err := tx.Exec(`
				UPDATE doc_references
				SET target_id = ?
				WHERE source_id = ? AND target_uri = ?
			`, targetID.String, ref.SourceID, ref.TargetURI)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to update reference: %v\n", err)
				continue
			}

			// Manually create backlink (trigger only fires on INSERT, not UPDATE)
			_, err = tx.Exec(`
				INSERT OR IGNORE INTO backlinks(target_id, source_id, relation)
				VALUES (?, ?, ?)
			`, targetID.String, ref.SourceID, "refs")

			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to create backlink: %v\n", err)
			}

			resolved++
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return resolved, nil
}

// upsertFTSRow performs a targeted FTS upsert for a single document.
// It deletes the existing FTS row (if any) then inserts a fresh one that
// joins the latest tags from the tags table. The two-statement sequence is
// wrapped in a savepoint so a crash between delete and insert leaves the
// FTS index intact rather than missing the row. ftsMu is held for the
// duration so a concurrent rebuildFTS cannot interleave.
func (c *Constellation) upsertFTSRow(docID string) error {
	ftsMu.Lock()
	defer ftsMu.Unlock()

	// Use a savepoint for crash-consistency within the single-writer connection.
	if _, err := c.db.Exec("SAVEPOINT fts_upsert"); err != nil {
		return fmt.Errorf("fts_upsert savepoint: %w", err)
	}

	if _, err := c.db.Exec("DELETE FROM documents_fts WHERE id = ?", docID); err != nil {
		_, _ = c.db.Exec("ROLLBACK TO SAVEPOINT fts_upsert")
		_, _ = c.db.Exec("RELEASE SAVEPOINT fts_upsert")
		return fmt.Errorf("fts_upsert delete: %w", err)
	}

	_, err := c.db.Exec(`
		INSERT INTO documents_fts(id, title, content, tags, sector, type)
		SELECT
			d.id,
			d.title,
			d.content,
			COALESCE((SELECT group_concat(tag, ' ') FROM tags WHERE document_id = d.id), ''),
			d.sector,
			d.type
		FROM documents d
		WHERE d.id = ?
	`, docID)
	if err != nil {
		_, _ = c.db.Exec("ROLLBACK TO SAVEPOINT fts_upsert")
		_, _ = c.db.Exec("RELEASE SAVEPOINT fts_upsert")
		return fmt.Errorf("fts_upsert insert: %w", err)
	}

	if _, err := c.db.Exec("RELEASE SAVEPOINT fts_upsert"); err != nil {
		return fmt.Errorf("fts_upsert release: %w", err)
	}
	return nil
}

// rebuildFTS manually populates the FTS index with current documents and aggregated tags.
// This is called after indexing completes to sync tags into the FTS table.
// Nuclear fix: We don't use triggers anymore - manual population after all docs + tags indexed.
func (c *Constellation) rebuildFTS() error {
	ftsMu.Lock()
	defer ftsMu.Unlock()

	// Clear existing FTS data
	if _, err := c.db.Exec("DELETE FROM documents_fts"); err != nil {
		return fmt.Errorf("failed to clear FTS: %w", err)
	}

	// Manually populate FTS with all documents and their aggregated tags
	_, err := c.db.Exec(`
		INSERT INTO documents_fts(id, title, content, tags, sector, type)
		SELECT
			d.id,
			d.title,
			d.content,
			COALESCE((SELECT group_concat(tag, ' ') FROM tags WHERE document_id = d.id), ''),
			d.sector,
			d.type
		FROM documents d
	`)

	if err != nil {
		return fmt.Errorf("failed to populate FTS: %w", err)
	}

	return nil
}
