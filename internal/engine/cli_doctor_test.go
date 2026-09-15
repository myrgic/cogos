// cli_doctor_test.go — tests for RunDoctor and its check groups.
//
// RunDoctor is the testable orchestration entry point (no os.Exit, no direct
// stdout/stderr writes); runDoctorCmd is the thin CLI wrapper around it and is
// exercised indirectly through these tests via the same fixtures a real
// invocation would see.
package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/myrgic/cogos/sdk/constellation"

	_ "github.com/mattn/go-sqlite3"
)

// writeCogdoc writes a minimal indexable cogdoc under workspaceRoot/.cog/mem.
func writeCogdoc(t *testing.T, workspaceRoot, relPath, title, body string) {
	t.Helper()
	full := filepath.Join(workspaceRoot, relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	content := "---\ntitle: " + title + "\ntype: note\ncreated: 2026-01-01\n---\n\n" + body + "\n"
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatalf("write cogdoc: %v", err)
	}
}

// buildFixtureWorkspace creates a workspace with an indexed constellation.db
// containing at least one document with a distinctive title, plus an
// unindexed .cog/hooks tree (a *.cog.md cogdoc written AFTER IndexWorkspace
// runs, so it genuinely never got indexed) to exercise the documents-vs-files
// check. It must be *.cog.md, not a plain .md: IndexWorkspace only ever
// indexes files with that exact suffix (indexer.go:129), so a plain .md
// sibling is invisible to the indexer for a structural reason (never
// eligible in the first place) rather than the "on disk but not yet
// reindexed" staleness this fixture means to simulate -- and, since
// IndexWorkspace's base walk covers the whole .cog/ tree, writing a *.cog.md
// file BEFORE indexing would just get it indexed, defeating the fixture.
func buildFixtureWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeCogdoc(t, root, ".cog/mem/semantic/distinctive-sentinel-doc.cog.md",
		"Distinctive Sentinel Document", "This document contains the word RECOGNIZABLE for the negative control.")

	c, err := constellation.Open(root)
	if err != nil {
		skipIfNoFTS5(t, err)
		t.Fatalf("constellation.Open: %v", err)
	}
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Written after the index above ran, so it is a real, indexable cogdoc
	// that genuinely never got indexed -- simulating the .cog/hooks /
	// .cog/lib unindexed-tree finding from the issue.
	writeCogdoc(t, root, ".cog/hooks/note.cog.md", "Unindexed Hook Note", "never reindexed")

	return root
}

// skipIfNoFTS5 skips the current test when err is the well-known
// constellation.Open failure produced by a build without the fts5 tag ("go
// test ./..." with no build tags, as CI runs it). Any other error still
// fails the test via the caller's own t.Fatalf. Mirrors the pattern in
// mcp_stubs_test.go and sdk/constellation/indexer_hardening_test.go.
func skipIfNoFTS5(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "no such module: fts5") {
		t.Skip("FTS5 not available (build with -tags fts5)")
	}
}

func findCheck(t *testing.T, report *DoctorReport, group, name string) *DoctorCheck {
	t.Helper()
	for i := range report.Groups {
		if report.Groups[i].Name != group {
			continue
		}
		for j := range report.Groups[i].Checks {
			if report.Groups[i].Checks[j].Name == name {
				return &report.Groups[i].Checks[j]
			}
		}
	}
	t.Fatalf("check %q not found in group %q; groups=%+v", name, group, report.Groups)
	return nil
}

// TestNegativeControlPassesOnHealthyIndex is the primary design-constraint
// test: a sentinel term derived from an indexed document must return >=1 hit
// through the real search path, and the check must report OK.
func TestNegativeControlPassesOnHealthyIndex(t *testing.T) {
	root := buildFixtureWorkspace(t)
	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})

	check := findCheck(t, report, "index health", "negative control (sentinel query)")
	if check.Status != StatusOK {
		t.Fatalf("negative control on a healthy index = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestNegativeControlNeverReportsOKOnEmptyDB pins the never-OK-when-unknown
// discipline: an empty documents table cannot produce a trustworthy sentinel,
// so the check must be UNKNOWN, never OK, and never silently pass.
func TestNegativeControlNeverReportsOKOnEmptyDB(t *testing.T) {
	root := t.TempDir()
	c, err := constellation.Open(root) // creates an empty, schema-only db
	if err != nil {
		skipIfNoFTS5(t, err)
		t.Fatalf("constellation.Open: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "index health", "negative control (sentinel query)")
	if check.Status == StatusOK {
		t.Fatalf("negative control on an empty index reported OK; must be UNKNOWN (nothing to derive a sentinel from)")
	}
	if check.Status != StatusUnknown {
		t.Errorf("negative control on empty index = %s, want UNKNOWN; detail=%s", check.Status, check.Detail)
	}
}

// TestDoctorAgainstNonexistentWorkspaceNeverReportsOK is the UNKNOWN-
// discipline end-to-end check the ground rules call out explicitly: doctor
// run against a workspace root that does not exist must never claim OK for
// any of the workspace-scoped checks (index health, store liveness) — there
// is nothing there to have verified. Install integrity and config coherence
// are machine-scoped (PATH, ~/.claude, ~/.hermes), not workspace-scoped, so
// they legitimately keep reporting on the real machine regardless of whether
// the target workspace directory exists; this test does not constrain them.
func TestDoctorAgainstNonexistentWorkspaceNeverReportsOK(t *testing.T) {
	// Isolate PATH/HOME so the "inference surface" group's "argv vs CLI"
	// check (myrgic/cogos#631) can't inject a real, machine-state FAIL
	// (e.g. genuine codex CLI flag drift on the host running this test)
	// into this workspace-absence invariant -- that check is intentionally
	// machine-scoped, same as install integrity/config coherence above it,
	// and a real CLI-drift FAIL there is correct behavior, just orthogonal
	// to what this test asserts. t.Setenv (not os.Setenv) per this repo's
	// own TestNoTestSetsHOMEGlobally rule: process-wide os.Setenv("HOME",
	// ...) in a test is exactly the 2026-08 ~/.zshrc incident's root cause.
	t.Setenv("PATH", "/nonexistent-doctor-test-path")
	t.Setenv("HOME", t.TempDir())

	root := filepath.Join(t.TempDir(), "does-not-exist", "nested")
	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})

	workspaceScoped := map[string]bool{"index health": true, "store liveness": true}
	for _, g := range report.Groups {
		if !workspaceScoped[g.Name] {
			continue
		}
		for _, c := range g.Checks {
			if c.Status == StatusOK {
				t.Errorf("group %q check %q reported OK against a nonexistent workspace (detail=%s); should be UNKNOWN or WARN",
					g.Name, c.Name, c.Detail)
			}
		}
	}
	if report.ExitCode() != 0 {
		t.Errorf("nonexistent workspace produced FAIL-driven exit code %d; absence of a workspace is UNKNOWN, not a proven defect",
			report.ExitCode())
	}
}

// TestDocsVsFilesFlagsUnindexedTree exercises the documents-vs-files check
// against the .cog/hooks fixture tree that has files on disk but zero FTS
// rows — the exact shape of Finding 5 in the issue.
func TestDocsVsFilesFlagsUnindexedTree(t *testing.T) {
	root := buildFixtureWorkspace(t)
	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})

	check := findCheck(t, report, "index health", "documents vs files on disk")
	if check.Status != StatusWarn {
		t.Fatalf("documents vs files on disk = %s, want WARN (unindexed .cog/hooks tree present); detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "hooks") || !strings.Contains(check.Detail, "UNINDEXED") {
		t.Errorf("expected detail to call out unindexed hooks tree, got:\n%s", check.Detail)
	}
}

// TestDocsVsFilesCountsCorrectlyThroughSymlinkedRoot is the regression test
// for the countDocsUnderPrefix symlink-resolution bug: documents.path in the
// constellation DB is stored only after filepath.EvalSymlinks resolution
// (walkRoots in sdk/constellation/indexer.go), so doctorDocsVsFiles must
// resolve the workspace root itself before building its LIKE prefix, not
// rely on the caller having already done so. Deliberately passes the RAW,
// unresolved t.TempDir() root -- unlike TestDocsVsFilesDoesNotMergeSiblingPrefixSubtrees
// below and TestIndexFreshnessCatchesStaleNonMemSubtree, which resolve up
// front only so their own path-string assertions are exact -- so this test
// exercises the resolution the fix adds. On macOS, t.TempDir() itself
// traverses a symlink (/var/folders -> /private/var/folders), which is
// exactly the failure shape a symlinked workspace mount, NFS home, or macOS
// /tmp hits in production.
func TestDocsVsFilesCountsCorrectlyThroughSymlinkedRoot(t *testing.T) {
	root := t.TempDir() // intentionally NOT resolved -- see comment above

	writeCogdoc(t, root, ".cog/mem/semantic/sentinel.cog.md", "Sentinel", "content")

	c, err := constellation.Open(root)
	if err != nil {
		skipIfNoFTS5(t, err)
		t.Fatalf("constellation.Open: %v", err)
	}
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "index health", "documents vs files on disk")

	var memLine string
	for _, line := range strings.Split(check.Detail, "\n") {
		if strings.HasPrefix(line, ".cog/mem:") {
			memLine = line
			break
		}
	}
	if memLine == "" {
		t.Fatalf("no .cog/mem: line found in detail:\n%s", check.Detail)
	}
	if strings.Contains(memLine, "0 indexed") || strings.Contains(memLine, "UNINDEXED") {
		t.Errorf(".cog/mem line = %q, want a nonzero indexed count -- the sentinel doc IS indexed; an unresolved-symlink prefix mismatch would falsely report 0. Full detail:\n%s", memLine, check.Detail)
	}
}

// TestLikeEscapeCharDoesNotCollideWithBackslashSeparator pins the choice of
// '!' (not the conventional '\') as countDocsUnderPrefix's SQL ESCAPE
// character. On Windows, filepath.Separator IS '\', so if '\' were also the
// ESCAPE char, the pattern's own trailing separator-then-wildcard ('\' +
// '%') would parse as an ESCAPE'd literal '%' rather than "separator,
// then anything" -- making the query match nothing on Windows regardless of
// index health. This can't literally run under GOOS=windows here, so it
// isolates the SQL mechanism directly against a document path containing a
// literal backslash (as any real Windows path would), built the same way
// countDocsUnderPrefix builds its pattern.
func TestLikeEscapeCharDoesNotCollideWithBackslashSeparator(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "escape-fixture-*.db")
	if err != nil {
		t.Fatalf("create temp db: %v", err)
	}
	f.Close()
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		if strings.Contains(err.Error(), "no such module: fts5") {
			t.Skip("FTS5 not available (build with -tags fts5)")
		}
		t.Fatalf("open fixture db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE documents (id TEXT PRIMARY KEY, path TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// Shaped like a real Windows document path: backslash separators, same
	// as filepath.Join produces under GOOS=windows.
	winPath := `C:\workspace\.cog\mem\note.cog.md`
	if _, err := db.Exec(`INSERT INTO documents (id, path) VALUES ('d1', ?)`, winPath); err != nil {
		t.Fatalf("insert: %v", err)
	}

	prefix := `C:\workspace\.cog\mem`
	// Mirrors countDocsUnderPrefix's own pattern construction, with a
	// literal '\' standing in for filepath.Separator on Windows.
	pattern := escapeLikePattern(prefix) + `\` + "%"

	var gotFixed int
	if err := db.QueryRow(`SELECT COUNT(*) FROM documents WHERE path LIKE ? ESCAPE '!'`, pattern).Scan(&gotFixed); err != nil {
		t.Fatalf("query with ESCAPE '!': %v", err)
	}
	if gotFixed != 1 {
		t.Errorf("Windows-shaped prefix match with ESCAPE '!' = %d, want 1 (separator+wildcard must not be swallowed as an escaped literal '%%')", gotFixed)
	}

	// Documents the regression this guards against: the SAME pattern under
	// the conventional ESCAPE '\' collides, because '\' is both the
	// appended path separator and the declared escape character.
	var gotBroken int
	if err := db.QueryRow(`SELECT COUNT(*) FROM documents WHERE path LIKE ? ESCAPE '\'`, pattern).Scan(&gotBroken); err != nil {
		t.Fatalf("query with ESCAPE '\\': %v", err)
	}
	if gotBroken != 0 {
		t.Fatalf("test assumption broken: ESCAPE '\\' unexpectedly matched %d row(s) -- the collision this test documents may no longer reproduce this way", gotBroken)
	}
}

// TestDocsVsFilesDoesNotMergeSiblingPrefixSubtrees is the regression test for
// the countDocsUnderPrefix LIKE-pattern bug: a bare `prefix+"%"` pattern
// matches any sibling subtree whose name merely starts with the same
// characters (".cog/adr" matches ".cog/adr-legacy/..."), merging their
// document counts and masking a genuinely unindexed tree.
func TestDocsVsFilesDoesNotMergeSiblingPrefixSubtrees(t *testing.T) {
	root := t.TempDir()
	// t.TempDir() on macOS resolves under a /var/folders symlink to
	// /private/var/folders; the indexer resolves .cog/'s walk root via
	// filepath.EvalSymlinks (see walkRoots), so document paths land in the
	// database already symlink-resolved. Resolve root the same way here so
	// the prefixes this test builds actually match what got indexed --
	// otherwise every countDocsUnderPrefix lookup silently returns 0
	// regardless of the LIKE-pattern fix under test, for an unrelated
	// path-identity reason.
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve tempdir symlinks: %v", err)
	}
	root = resolvedRoot

	// .cog/adr-legacy is a distinct, indexed subtree that shares "adr" as a
	// literal name prefix. Under the pre-fix bare-prefix LIKE pattern, this
	// row's path also matches `path LIKE '.../.cog/adr%'` and would get
	// double-counted against .cog/adr, masking the fact that .cog/adr itself
	// has zero indexed documents.
	writeCogdoc(t, root, ".cog/adr-legacy/real.cog.md", "Legacy ADR", "indexed sibling content")

	c, err := constellation.Open(root)
	if err != nil {
		skipIfNoFTS5(t, err)
		t.Fatalf("constellation.Open: %v", err)
	}
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// .cog/adr gets its *.cog.md file AFTER the index above ran, so it is a
	// real, indexable cogdoc that genuinely has zero rows in the DB -- not a
	// plain .md, which IndexWorkspace was never going to index regardless
	// (indexer.go:129 only ever considers the *.cog.md suffix) and so
	// wouldn't be counted as "on disk" at all by the fix under test here.
	writeCogdoc(t, root, ".cog/adr/new.cog.md", "New ADR", "not yet reindexed")

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "index health", "documents vs files on disk")

	var adrLine string
	for _, line := range strings.Split(check.Detail, "\n") {
		if strings.HasPrefix(line, ".cog/adr:") {
			adrLine = line
			break
		}
	}
	if adrLine == "" {
		t.Fatalf("no .cog/adr: line found in detail (want it distinct from .cog/adr-legacy):\n%s", check.Detail)
	}
	if !strings.Contains(adrLine, "0 indexed") || !strings.Contains(adrLine, "UNINDEXED") {
		t.Errorf(".cog/adr line = %q, want 0 indexed/UNINDEXED -- .cog/adr-legacy's indexed row must not be counted against .cog/adr; full detail:\n%s", adrLine, check.Detail)
	}
}

// TestIndexFreshnessCatchesStaleNonMemSubtree is the regression test for
// doctorIndexFreshness's original .cog/mem-only scan scope: IndexWorkspace's
// actual walk covers the whole .cog/ tree (walkRoots in
// sdk/constellation/indexer.go), so an edit landing in a different indexed
// subtree (.cog/adr here) after the last reindex must still surface as a
// freshness gap, not silently OK because .cog/mem itself looks fine.
func TestIndexFreshnessCatchesStaleNonMemSubtree(t *testing.T) {
	root := t.TempDir()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve tempdir symlinks: %v", err)
	}
	root = resolvedRoot

	writeCogdoc(t, root, ".cog/mem/semantic/sentinel.cog.md", "Sentinel", "content")

	c, err := constellation.Open(root)
	if err != nil {
		skipIfNoFTS5(t, err)
		t.Fatalf("constellation.Open: %v", err)
	}
	if err := c.IndexWorkspace(); err != nil {
		t.Fatalf("IndexWorkspace: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// An edit lands in .cog/adr (indexed by the base .cog/ walk, no
	// cogdocs.yaml required) well after the reindex above, without
	// triggering a reindex.
	adrPath := filepath.Join(root, ".cog", "adr", "new.cog.md")
	if err := os.MkdirAll(filepath.Dir(adrPath), 0755); err != nil {
		t.Fatalf("mkdir adr: %v", err)
	}
	if err := os.WriteFile(adrPath, []byte("# new adr\n"), 0644); err != nil {
		t.Fatalf("write adr file: %v", err)
	}
	future := time.Now().Add(3 * time.Hour)
	if err := os.Chtimes(adrPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "index health", "index freshness")
	if check.Status != StatusWarn {
		t.Fatalf("index freshness = %s, want WARN (a .cog/adr file postdates the last reindex by 3h); detail=%s", check.Status, check.Detail)
	}
}

// TestStoreLivenessFlagsDeadStore verifies a SQLite store whose mtime is
// older than the stale threshold is flagged WARN/DEAD, and a fresh one is OK.
func TestStoreLivenessFlagsDeadStore(t *testing.T) {
	root := buildFixtureWorkspace(t) // already has a fresh constellation.db

	deadPath := filepath.Join(root, ".cog", ".state", "old_events.db")

	// Build a minimal real sqlite file via the stdlib driver so store liveness
	// can open it read-only and enumerate its (empty) table set.
	createEmptySQLiteFile(t, deadPath)
	oldTime := time.Now().Add(-40 * 24 * time.Hour)
	if err := os.Chtimes(deadPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true, StaleDays: 30})
	check := findCheck(t, report, "store liveness", "store: "+deadPath)
	if check.Status != StatusWarn {
		t.Fatalf("store liveness for a 40d-stale store = %s, want WARN(DEAD); detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "DEAD") {
		t.Errorf("expected DEAD label in detail, got: %s", check.Detail)
	}
}

// TestStoreLivenessReportsUnknownOnUnreadableStore pins the UNKNOWN-not-OK
// contract this command advertises: a store whose row count cannot be
// established (corrupt file here; permission-denied is the same code path)
// must never render OK, even though its last-write age looks fresh.
func TestStoreLivenessReportsUnknownOnUnreadableStore(t *testing.T) {
	root := buildFixtureWorkspace(t)

	corruptPath := filepath.Join(root, ".cog", ".state", "corrupt.db")
	if err := os.MkdirAll(filepath.Dir(corruptPath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(corruptPath, []byte("not a sqlite database"), 0644); err != nil {
		t.Fatalf("write corrupt db: %v", err)
	}
	// Fresh mtime so the staleness case never masks the row-count failure.
	now := time.Now()
	if err := os.Chtimes(corruptPath, now, now); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true, StaleDays: 30})
	check := findCheck(t, report, "store liveness", "store: "+corruptPath)
	if check.Status == StatusOK {
		t.Fatalf("store liveness reported OK for an unreadable store (detail=%s); must be UNKNOWN, never OK", check.Detail)
	}
	if check.Status != StatusUnknown {
		t.Errorf("store liveness for an unreadable store = %s, want UNKNOWN; detail=%s", check.Status, check.Detail)
	}
}

func createEmptySQLiteFile(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open sqlite file: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE placeholder (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestExitCodeReflectsOnlyFail(t *testing.T) {
	r := &DoctorReport{Groups: []DoctorGroup{
		{Name: "g", Checks: []DoctorCheck{
			{Name: "a", Status: StatusOK},
			{Name: "b", Status: StatusWarn},
			{Name: "c", Status: StatusUnknown},
		}},
	}}
	if r.ExitCode() != 0 {
		t.Errorf("OK/WARN/UNKNOWN only: exit code = %d, want 0", r.ExitCode())
	}
	r.Groups[0].Checks = append(r.Groups[0].Checks, DoctorCheck{Name: "d", Status: StatusFail})
	if r.ExitCode() != 1 {
		t.Errorf("with a FAIL present: exit code = %d, want 1", r.ExitCode())
	}
}

func TestGroupByPrefixDetectsGenerationDrift(t *testing.T) {
	groups := groupByPrefix([]string{"cogos", "cogos-http", "cogos-v3", "unrelated"})
	var driftedFound bool
	for _, g := range groups {
		if len(g) > 1 {
			driftedFound = true
			if len(g) != 3 {
				t.Errorf("expected the cogos family to cluster to 3 names, got %v", g)
			}
		}
	}
	if !driftedFound {
		t.Fatalf("groupByPrefix did not cluster cogos/cogos-http/cogos-v3: %v", groups)
	}
}

func TestGroupByPrefixLeavesUnrelatedNamesSingleton(t *testing.T) {
	groups := groupByPrefix([]string{"alpha", "beta", "gamma"})
	for _, g := range groups {
		if len(g) != 1 {
			t.Errorf("expected all-singleton groups for unrelated names, got %v", groups)
		}
	}
}

func TestLongestWordPicksDistinctiveTerm(t *testing.T) {
	if got := longestWord("A B Recognizable"); got != "recognizable" {
		t.Errorf("longestWord = %q, want %q", got, "recognizable")
	}
	if got := longestWord("a b c"); got != "" {
		t.Errorf("longestWord on all-short-words = %q, want empty", got)
	}
}

func TestExpandHome(t *testing.T) {
	home := "/Users/tester"
	if got := expandHome("~/foo/bar", home); got != "/Users/tester/foo/bar" {
		t.Errorf("expandHome(~/foo/bar) = %q", got)
	}
	if got := expandHome("/already/absolute", home); got != "/already/absolute" {
		t.Errorf("expandHome should pass through absolute paths unchanged, got %q", got)
	}
}

// TestBinarySprawlScansWorkspaceRootAndDotCog pins cogos#568 finding 1: a
// binary sitting at <workspace>/.cog/cog (the exact shape of the 79-day-old
// binary a workspace-local `scripts/cog` wrapper resolved to) must be
// detected without requiring an explicit --scan-dir, because it is the
// doctor target workspace itself, not an arbitrary extra location.
func TestBinarySprawlScansWorkspaceRootAndDotCog(t *testing.T) {
	root := t.TempDir()
	dotCog := filepath.Join(root, ".cog")
	if err := os.MkdirAll(dotCog, 0755); err != nil {
		t.Fatalf("mkdir .cog: %v", err)
	}
	stray := filepath.Join(dotCog, "cog")
	if err := os.WriteFile(stray, []byte("not a real binary, just needs to exist+exec"), 0755); err != nil {
		t.Fatalf("write stray binary: %v", err)
	}
	old := time.Now().Add(-79 * 24 * time.Hour)
	if err := os.Chtimes(stray, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "install integrity", "binary sprawl")
	if !strings.Contains(check.Detail, stray) {
		t.Errorf("binary sprawl did not report %s (workspace/.cog binary); detail=%s", stray, check.Detail)
	}
	if !strings.Contains(check.Detail, "age=79d") {
		t.Errorf("expected age=79d for the stray binary in detail:\n%s", check.Detail)
	}
}

// TestBinarySprawlIgnoresNonExecutableAndUnrelatedFiles verifies the name/
// executable-bit filter does not over-match: a non-executable file named
// "cog.md" or an executable named "cogfield" must not be reported as a
// stray cogos/cog binary.
func TestBinarySprawlIgnoresNonExecutableAndUnrelatedFiles(t *testing.T) {
	root := t.TempDir()
	dotCog := filepath.Join(root, ".cog")
	if err := os.MkdirAll(dotCog, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dotCog, "cog.prev"), []byte("x"), 0644); err != nil { // no exec bit
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dotCog, "cogfield-notes.md"), []byte("x"), 0755); err != nil { // exec but wrong name
		t.Fatalf("write: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "install integrity", "binary sprawl")
	if strings.Contains(check.Detail, "cog.prev") {
		t.Errorf("non-executable cog.prev should not be reported as a binary: %s", check.Detail)
	}
	if strings.Contains(check.Detail, "cogfield-notes.md") {
		t.Errorf("cogfield-notes.md should not match the cogos/cog binary name pattern: %s", check.Detail)
	}
}

// TestPathLikeRegexIgnoresURLsAndModelIDs pins the false-positive fix: MCP/
// Hermes configs are full of "/segment/segment"-shaped strings that are NOT
// filesystem paths (API base URLs, HuggingFace model IDs, MCP route
// strings), and the nonexistent-path-reference check must not flag them.
func TestPathLikeRegexIgnoresURLsAndModelIDs(t *testing.T) {
	notPaths := []string{
		"https://api.anthropic.com/v1",
		"lmstudio-eclipse/google/gemma-4-e4b",
		"/v1/synthesize",
		"nikolaik/python-nodejs:python3.11-nodejs20",
	}
	for _, s := range notPaths {
		if m := pathLikeRe.FindAllString(s, -1); len(m) > 0 {
			t.Errorf("pathLikeRe matched non-path string %q as %v; these must not be flagged as filesystem paths", s, m)
		}
	}

	realPaths := []string{
		"/Users/tester/workspaces/cogos-dev/mod3",
		"~/workspaces/cog",
	}
	for _, s := range realPaths {
		if m := pathLikeRe.FindAllString(s, -1); len(m) == 0 {
			t.Errorf("pathLikeRe failed to match real absolute path %q", s)
		}
	}
}

// TestMCPConfigsPointAtOneBinaryDetectsDrift exercises the structured
// "command" field walk end-to-end: two config files naming different cogos
// binaries must produce a WARN naming both; a single shared binary must
// produce OK.
func TestMCPConfigsPointAtOneBinaryDetectsDrift(t *testing.T) {
	root := t.TempDir()
	binA := filepath.Join(root, "bin-a", "cogos")
	binB := filepath.Join(root, "bin-b", "cogos")
	for _, p := range []string{binA, binB} {
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(p, []byte("x"), 0755); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	cfg1 := filepath.Join(root, "cfg1.mcp.json")
	cfg2 := filepath.Join(root, "cfg2.mcp.json")
	writeJSON(t, cfg1, map[string]any{"mcpServers": map[string]any{"cogos": map[string]any{"command": binA}}})
	writeJSON(t, cfg2, map[string]any{"mcpServers": map[string]any{"cogos": map[string]any{"command": binB}}})

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true, ExtraConfigFiles: []string{cfg1, cfg2}})
	check := findCheck(t, report, "config coherence", "MCP configs point at one binary")
	if check.Status != StatusWarn {
		t.Fatalf("two distinct cogos binaries referenced = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, binA) || !strings.Contains(check.Detail, binB) {
		t.Errorf("expected both binaries named in detail:\n%s", check.Detail)
	}
}

func TestLooksLikeRealPath(t *testing.T) {
	if !looksLikeRealPath("/Users/tester/workspaces/cogos-dev") {
		t.Error("expected a two+-segment absolute path to look real")
	}
	if looksLikeRealPath("https://example.com/foo") {
		t.Error("a URL should not be treated as a filesystem path")
	}
}

// ---------------------------------------------------------------------------
// #571 item 1: lint exit contract
// ---------------------------------------------------------------------------

// findCheckPrefix is like findCheck but matches a check whose name starts
// with prefix -- used for checks whose full name embeds a filesystem path
// generated by the test (e.g. "quick check: <tempdir>/store.db").
func findCheckPrefix(t *testing.T, report *DoctorReport, group, prefix string) *DoctorCheck {
	t.Helper()
	for i := range report.Groups {
		if report.Groups[i].Name != group {
			continue
		}
		for j := range report.Groups[i].Checks {
			if strings.HasPrefix(report.Groups[i].Checks[j].Name, prefix) {
				return &report.Groups[i].Checks[j]
			}
		}
	}
	t.Fatalf("no check with name prefix %q found in group %q; groups=%+v", prefix, group, report.Groups)
	return nil
}

// TestLintFindingsTableDriven pins lintMeetsThreshold/LintFindings against a
// synthesized report for every (status-present, severity-min) combination:
// the warn threshold must trip on WARN, UNKNOWN, and FAIL; the fail
// threshold must trip on FAIL only, explicitly NOT on UNKNOWN (an
// unperformed observation is not itself proof of breakage).
func TestLintFindingsTableDriven(t *testing.T) {
	reportWith := func(status DoctorStatus) *DoctorReport {
		return &DoctorReport{Groups: []DoctorGroup{
			{Name: "g", Checks: []DoctorCheck{{Name: "c", Status: status}}},
		}}
	}
	cleanReport := &DoctorReport{Groups: []DoctorGroup{
		{Name: "g", Checks: []DoctorCheck{{Name: "c", Status: StatusOK}}},
	}}

	cases := []struct {
		name   string
		report *DoctorReport
		min    DoctorStatus
		want   bool
	}{
		{"OK-only never meets warn", cleanReport, StatusWarn, false},
		{"OK-only never meets fail", cleanReport, StatusFail, false},
		{"WARN meets warn", reportWith(StatusWarn), StatusWarn, true},
		{"WARN does not meet fail", reportWith(StatusWarn), StatusFail, false},
		{"UNKNOWN meets warn", reportWith(StatusUnknown), StatusWarn, true},
		{"UNKNOWN does NOT meet fail", reportWith(StatusUnknown), StatusFail, false},
		{"FAIL meets warn", reportWith(StatusFail), StatusWarn, true},
		{"FAIL meets fail", reportWith(StatusFail), StatusFail, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.report.LintFindings(tc.min); got != tc.want {
				t.Errorf("LintFindings(min=%s) = %v, want %v", tc.min, got, tc.want)
			}
		})
	}
}

// TestExitCodeUnaffectedByWarnOrUnknown pins the default ADVISORY contract
// (#570, unchanged by #571): only FAIL flips the default exit code; WARN and
// UNKNOWN, no matter how many, must not.
func TestExitCodeUnaffectedByWarnOrUnknown(t *testing.T) {
	r := &DoctorReport{Groups: []DoctorGroup{
		{Name: "g", Checks: []DoctorCheck{
			{Name: "a", Status: StatusWarn},
			{Name: "b", Status: StatusUnknown},
			{Name: "c", Status: StatusWarn},
		}},
	}}
	if r.ExitCode() != 0 {
		t.Errorf("WARN/UNKNOWN-only report: default ExitCode() = %d, want 0 (advisory contract)", r.ExitCode())
	}
}

// doctorLintHelperEnv gates TestHelperProcessDoctorLint so a normal `go test`
// run treats it as a no-op, matching the os/exec_test.go TestHelperProcess
// idiom already used by cli_reconcile_test.go's runReconcileSnapshotHelper.
const doctorLintHelperEnv = "CLI_DOCTOR_LINT_HELPER"

// TestHelperProcessDoctorLint re-execs runDoctorCmd (which calls os.Exit
// directly on every path and so cannot be exercised in-process) against
// scenarios selected by argv, all built to be deterministic regardless of
// host state:
//   - "default": no --lint, against an EMPTY, isolated temp workspace with
//     --skip-network. Every FAIL in this codebase's doctor checks requires
//     either a real SQLite store (index health) or --deep (store liveness);
//     neither is present, so this can never produce a FAIL and must exit 0
//     under the default any-FAIL contract regardless of what else is on the
//     host running the test.
//   - "lint-warn": same empty workspace, --lint (default --severity-min
//     warn). --skip-network guarantees the "version vs published tag" check
//     is UNKNOWN, and UNKNOWN always meets the warn threshold, so this must
//     exit 1 on every host.
//   - "lint-fail-threshold": same empty workspace, --lint --severity-min
//     fail. No FAIL is possible (see "default" above), so this must exit 0
//     even though "lint-warn" against the identical workspace exits 1 --
//     the two thresholds disagreeing on the same report is the point of the
//     test.
//   - "lint-bad-severity": --lint --severity-min bogus, no workspace flag at
//     all. Validation happens before workspace resolution or any report is
//     produced, so this must exit 2 (pre-report failure) unconditionally.
//   - "bad-flag-no-lint" / "bad-flag-lint": an actually-unrecognized flag
//     (--nonexistent-flag), without and with --lint respectively. Both must
//     exit 2, and identically so: fs is a flag.ExitOnError FlagSet, and the
//     stdlib's own Parse calls os.Exit(2) directly on any parse error before
//     ever returning one to our code (ErrHelp/"-h" is the sole exception,
//     exiting 0) -- see flag.FlagSet.Parse in the standard library. Bad
//     flags have therefore always exited 2 regardless of --lint, since
//     #570; this pins that as the ACTUAL bad-flags path, distinct from
//     "lint-bad-severity" above, which exercises the separate --severity-min
//     value-validation code this PR adds (a cog-review finding on an
//     earlier revision of this PR: --severity-min bad-value coverage does
//     NOT exercise the genuine-parse-failure path, and vice versa).
func TestHelperProcessDoctorLint(t *testing.T) {
	if os.Getenv(doctorLintHelperEnv) != "1" {
		return
	}
	args := os.Args
	var scenario, root string
	for i, a := range args {
		if a == "--" && i+1 < len(args) {
			rest := args[i+1:]
			if len(rest) > 0 {
				scenario = rest[0]
			}
			if len(rest) > 1 {
				root = rest[1]
			}
			break
		}
	}
	switch scenario {
	case "default":
		runDoctorCmd([]string{"--workspace", root, "--skip-network"}, root)
	case "lint-warn":
		runDoctorCmd([]string{"--workspace", root, "--skip-network", "--lint"}, root)
	case "lint-fail-threshold":
		runDoctorCmd([]string{"--workspace", root, "--skip-network", "--lint", "--severity-min", "fail"}, root)
	case "lint-json":
		runDoctorCmd([]string{"--workspace", root, "--skip-network", "--lint", "--json"}, root)
	case "lint-bad-severity":
		runDoctorCmd([]string{"--lint", "--severity-min", "bogus"}, root)
	case "bad-flag-no-lint":
		runDoctorCmd([]string{"--workspace", root, "--nonexistent-flag"}, root)
	case "bad-flag-lint":
		runDoctorCmd([]string{"--workspace", root, "--lint", "--nonexistent-flag"}, root)
	}
	os.Exit(99) // unreached: runDoctorCmd always calls os.Exit itself
}

// minimalHelperEnv returns the parent's environment minus HOME and PATH,
// which runDoctorLintHelper below sets explicitly to deterministic,
// isolated values. Filtering here (rather than just appending overrides
// after os.Environ()) matters because whichever HOME/PATH entry appears
// LAST wins when a process reads its environment for a duplicate key on
// some platforms' exec implementations -- filtering removes the ambiguity
// instead of relying on override-via-append ordering.
func minimalHelperEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "PATH=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func runDoctorLintHelper(t *testing.T, scenario, root string) (exitCode int, stdout, stderr string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessDoctorLint$", "--", scenario, root)
	// Isolate PATH and HOME from the host running this test: the new
	// "inference surface" group's "argv vs CLI" check (myrgic/cogos#631)
	// probes whatever codex/pi/claude binaries are actually installed on
	// PATH (plus ~/.nvm/versions/node/*/bin) and reports FAIL on real CLI
	// flag drift -- exactly its job, but it means this end-to-end test's
	// hardcoded exit-code expectations (deliberately zero FAILs possible
	// against an empty workspace) would otherwise flip on any host with a
	// stale/drifted CLI installed, or with no CLI installed at all vs. one
	// installed with matching flags. A deterministic, PATH-less, HOME-less
	// subprocess environment makes that check report UNKNOWN ("not found")
	// on every host, keeping this test about the exit-code contract, not
	// about the state of this host's node_modules/PATH.
	isolatedHome := t.TempDir()
	cmd.Env = append(minimalHelperEnv(), doctorLintHelperEnv+"=1", "HOME="+isolatedHome, "PATH=/nonexistent-doctor-test-path")
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	stdout, stderr = outBuf.String(), errBuf.String()
	if err == nil {
		return 0, stdout, stderr
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), stdout, stderr
	}
	t.Fatalf("running doctor lint helper: %v (stdout=%q stderr=%q)", err, stdout, stderr)
	return -1, stdout, stderr
}

// TestDoctorLintExitCodesEndToEnd exercises every exit path of the real CLI
// entry point (runDoctorCmd), table-driven, per the ground rules for this
// deliverable. See TestHelperProcessDoctorLint's doc comment for why each
// scenario's expected exit code is deterministic across hosts.
func TestDoctorLintExitCodesEndToEnd(t *testing.T) {
	root := t.TempDir() // empty; shared read-only across subtests below

	cases := []struct {
		scenario string
		wantExit int
	}{
		{"default", 0},
		{"lint-warn", 1},
		{"lint-fail-threshold", 0},
		{"lint-json", 1},
		{"lint-bad-severity", 2},
		{"bad-flag-no-lint", 2},
		{"bad-flag-lint", 2},
	}
	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			exitCode, stdout, stderr := runDoctorLintHelper(t, tc.scenario, root)
			if exitCode != tc.wantExit {
				t.Errorf("scenario %q: exit code = %d, want %d\nstdout=%s\nstderr=%s", tc.scenario, exitCode, tc.wantExit, stdout, stderr)
			}
		})
	}
}

// TestDoctorLintJSONNeverCorruptsStdout pins a cog-review-flagged defect:
// combining --json with --lint used to write the "lint: severity-min=..."
// status line to stdout AFTER the JSON-encoded report, on the same stream --
// exactly the machine-consumption scenario (CI/cron piping through jq)
// --lint is pitched for. stdout under --json --lint must be nothing but the
// single JSON-encoded report, and the lint status line must still be
// observable, just on stderr instead.
func TestDoctorLintJSONNeverCorruptsStdout(t *testing.T) {
	root := t.TempDir()
	_, stdout, stderr := runDoctorLintHelper(t, "lint-json", root)

	var report DoctorReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("--json --lint stdout is not valid JSON (%v); stdout=%q", err, stdout)
	}
	if strings.Contains(stdout, "lint:") {
		t.Errorf("lint status text leaked into --json stdout:\n%s", stdout)
	}
	if !strings.Contains(stderr, "lint:") {
		t.Errorf("expected the lint status line on stderr; stderr=%q", stderr)
	}
}

// ---------------------------------------------------------------------------
// #571 item 2: --deep (PRAGMA quick_check) + *.corrupt-* enumeration
// ---------------------------------------------------------------------------

// createSQLiteFileWithRows builds a real, multi-page SQLite file (large
// enough that corrupting a byte outside the first page lands inside real
// data rather than empty free space) via the stdlib driver, no FTS5
// required.
func createSQLiteFileWithRows(t *testing.T, path string, rows int) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open sqlite file: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	filler := strings.Repeat("x", 200)
	for i := 0; i < rows; i++ {
		if _, err := db.Exec(`INSERT INTO t (data) VALUES (?)`, fmt.Sprintf("row-%d-%s", i, filler)); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// corruptByteAtOffset flips a run of bytes at off to 0xFF, deliberately past
// the 100-byte SQLite header and page 1 so the file still opens (the header
// stays intact) but a later page's content is corrupted -- exactly what
// PRAGMA quick_check is meant to catch, as opposed to a header-level
// "this isn't a SQLite file at all" open failure.
func corruptByteAtOffset(t *testing.T, path string, off int64, n int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	defer f.Close()
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 0xFF
	}
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("write corruption: %v", err)
	}
}

// TestDeepQuickCheckPassesOnHealthyStore verifies --deep's PRAGMA
// quick_check reports OK against a genuinely healthy, multi-page store, and
// that this never happens unless Deep is explicitly requested (deep off by
// default: no "quick check:" entry appears at all).
func TestDeepQuickCheckPassesOnHealthyStore(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, ".cog", ".state")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dbPath := filepath.Join(dbDir, "healthy.db")
	createSQLiteFileWithRows(t, dbPath, 200)

	reportShallow := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	for _, c := range findGroup(t, reportShallow, "store liveness").Checks {
		if strings.HasPrefix(c.Name, "quick check:") {
			t.Errorf("quick check ran without --deep: %+v", c)
		}
	}

	reportDeep := RunDoctor(root, DoctorOptions{SkipNetwork: true, Deep: true})
	check := findCheckPrefix(t, reportDeep, "store liveness", "quick check: "+dbPath)
	if check.Status != StatusOK {
		t.Fatalf("quick check on a healthy store = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestDeepQuickCheckFailsOnCorruptStore verifies a byte-level-corrupted page
// is reported as FAIL (not WARN, not UNKNOWN) -- corruption is evidence the
// system is misbehaving, not merely untidy -- and that this FAIL flips the
// default advisory ExitCode() to 1.
func TestDeepQuickCheckFailsOnCorruptStore(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, ".cog", ".state")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dbPath := filepath.Join(dbDir, "corrupt.db")
	createSQLiteFileWithRows(t, dbPath, 2000) // large enough to span several pages
	corruptByteAtOffset(t, dbPath, 8192, 200) // page 3 (4096-byte pages), well past the header

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true, Deep: true})
	check := findCheckPrefix(t, report, "store liveness", "quick check: "+dbPath)
	if check.Status != StatusFail {
		t.Fatalf("quick check on a byte-corrupted store = %s, want FAIL; detail=%s", check.Status, check.Detail)
	}
	if report.ExitCode() != 1 {
		t.Errorf("report with a corrupt-store FAIL: ExitCode() = %d, want 1", report.ExitCode())
	}
}

// TestDeepQuickCheckTimesOutAsUnknownNeverOK pins the "bounded... report
// UNKNOWN on timeout, never OK" requirement directly: an effectively-zero
// timeout must never let quick_check report OK just because it didn't get a
// chance to actually check anything.
func TestDeepQuickCheckTimesOutAsUnknownNeverOK(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "t.db")
	createSQLiteFileWithRows(t, dbPath, 50)

	db, err := sql.Open("sqlite3", dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	g := &DoctorGroup{Name: "store liveness"}
	doctorQuickCheck(g, db, dbPath, 1*time.Nanosecond)
	if len(g.Checks) != 1 {
		t.Fatalf("expected exactly one check added, got %d", len(g.Checks))
	}
	check := g.Checks[0]
	if check.Status == StatusOK {
		t.Fatalf("quick check with a ~0 timeout reported OK (detail=%s); must be UNKNOWN, never OK", check.Detail)
	}
	if check.Status != StatusUnknown {
		t.Errorf("quick check with a ~0 timeout = %s, want UNKNOWN; detail=%s", check.Status, check.Detail)
	}
}

// findGroup is a small helper mirroring findCheck's lookup-or-fail shape,
// for tests that need to assert over an entire group's checks.
func findGroup(t *testing.T, report *DoctorReport, name string) *DoctorGroup {
	t.Helper()
	for i := range report.Groups {
		if report.Groups[i].Name == name {
			return &report.Groups[i]
		}
	}
	t.Fatalf("group %q not found; groups=%+v", name, report.Groups)
	return nil
}

// TestCorruptFileEnumerationWarns pins #571 item 3: a *.corrupt-* file
// preserved by a corruption-safe reindex-replace must be enumerated as WARN
// so it does not rot silently, unconditionally (no --deep required).
func TestCorruptFileEnumerationWarns(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, ".cog", ".state")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	corpsePath := filepath.Join(stateDir, "constellation.db.corrupt-20260101120000")
	if err := os.WriteFile(corpsePath, []byte("preserved corpse"), 0644); err != nil {
		t.Fatalf("write corpse fixture: %v", err)
	}

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "store liveness", "preserved corrupt stores")
	if check.Status != StatusWarn {
		t.Fatalf("preserved corrupt stores with a *.corrupt-* file present = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, corpsePath) {
		t.Errorf("expected corpse path %s named in detail:\n%s", corpsePath, check.Detail)
	}
}

// TestCorruptFileEnumerationOKWhenNoneFound is the negative case: an
// otherwise-empty .cog tree must report OK, not WARN, so the check is only
// ever noisy when there is genuinely something to look at.
func TestCorruptFileEnumerationOKWhenNoneFound(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".cog"), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "store liveness", "preserved corrupt stores")
	if check.Status != StatusOK {
		t.Fatalf("preserved corrupt stores with none present = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestCorruptFileEnumerationUnknownOnUnreadableSubdir pins the fix for a
// defect the exact class this command exists to eliminate: doctorCorruptFiles
// used to swallow filepath.WalkDir errors (`if err != nil { return nil }`),
// so a *.cog* subtree it could not fully read -- one directory alone chmod
// 0o000'd, say -- still produced "OK, no *.corrupt-* files found", even
// though the walk never actually looked inside that subtree. A partial walk
// must report UNKNOWN, naming the unreadable path, never OK.
func TestCorruptFileEnumerationUnknownOnUnreadableSubdir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits are not enforced, cannot fixture an unreadable directory")
	}

	root := t.TempDir()
	stateDir := filepath.Join(root, ".cog", ".state")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	blocked := filepath.Join(stateDir, "blocked")
	if err := os.MkdirAll(blocked, 0755); err != nil {
		t.Fatalf("mkdir blocked: %v", err)
	}
	if err := os.Chmod(blocked, 0o000); err != nil {
		t.Fatalf("chmod 000: %v", err)
	}
	t.Cleanup(func() {
		// t.TempDir()'s own cleanup needs to remove blocked's contents;
		// restore permissions first or that removal fails too.
		_ = os.Chmod(blocked, 0o755)
	})

	report := RunDoctor(root, DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "store liveness", "preserved corrupt stores")
	if check.Status != StatusUnknown {
		t.Fatalf("corrupt-file check with an unreadable subdirectory = %s, want UNKNOWN (never OK); detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, blocked) {
		t.Errorf("expected the unreadable path %s named in detail:\n%s", blocked, check.Detail)
	}
}

// ---------------------------------------------------------------------------
// #571 item 3: context-construction check group
//
// Every test below sets HOME to an isolated, per-test temp directory via
// t.Setenv so os.UserHomeDir() (which the context-construction functions
// call directly, matching every other doctor check group's convention of
// reading the real machine) resolves to fixture files instead of whatever
// happens to be on the host running the test. t.Setenv auto-restores HOME
// when the subtest completes.
// ---------------------------------------------------------------------------

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestDuplicateToolsetRegistrationsAcrossScopes reproduces the live shape
// #571 asks doctor to catch: the SAME target under the SAME literal name
// registered in two independent scopes (~/.claude.json user scope and a
// Hermes profile) -- the case a single-file inspection cannot see.
func TestDuplicateToolsetRegistrationsAcrossScopes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"browseros": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}
	}`)
	writeFile(t, filepath.Join(home, ".hermes", "profiles", "darkstar", "config.yaml"), `
mcp_servers:
  browseros:
    url: http://127.0.0.1:9000/mcp
`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("same target registered in two scopes = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "http://127.0.0.1:9000/mcp") {
		t.Errorf("expected the shared target URL named in detail:\n%s", check.Detail)
	}
	if !strings.Contains(check.Detail, "~/.claude.json (user)") || !strings.Contains(check.Detail, "config.yaml") {
		t.Errorf("expected both scope labels named in detail:\n%s", check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsOKAcrossDifferentProjects pins a
// cog-review-flagged false positive: two DIFFERENT ~/.claude.json projects
// independently registering the same MCP server under the same name is a
// normal, intentional pattern (a team checking the same server into
// multiple project configs) -- and it is never actually "double context
// cost per session", because only one project's mcpServers block ever
// applies to a given session. This must not WARN.
func TestDuplicateToolsetRegistrationsOKAcrossDifferentProjects(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"projects": {
			"/repo-a": {"mcpServers": {"github": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}},
			"/repo-b": {"mcpServers": {"github": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusOK {
		t.Fatalf("same target registered independently in two DIFFERENT projects = %s, want OK (never co-loaded in one session); detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsWarnsWhenProjectDuplicatesUserScope is
// the paired positive case: a project-scoped registration DOES genuinely
// collide with a non-project-scoped registration of the same target,
// because every session rooted in that project has BOTH the project's own
// mcpServers block AND the always-active user-level one loaded together.
func TestDuplicateToolsetRegistrationsWarnsWhenProjectDuplicatesUserScope(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"github": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}},
		"projects": {
			"/repo-a": {"mcpServers": {"github-again": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("project scope duplicating the always-active user scope = %s, want WARN (genuinely co-loaded); detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "~/.claude.json (user)") || !strings.Contains(check.Detail, "~/.claude.json (project: /repo-a)") {
		t.Errorf("expected both scope labels named in detail:\n%s", check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsWarnsWithinSameProject is the third
// case: two DIFFERENTLY-NAMED entries within the SAME project's mcpServers
// block resolving to the same target genuinely co-load within that
// project's own sessions, regardless of whether any other project or scope
// is involved at all.
func TestDuplicateToolsetRegistrationsWarnsWithinSameProject(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"projects": {
			"/repo-a": {"mcpServers": {
				"github": {"type": "http", "url": "http://127.0.0.1:9000/mcp"},
				"github-dup": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}
			}}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("two differently-named entries within the SAME project = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsNormalizesNpxMcpRemoteWrapper pins the
// harder normalization case: a stdio `npx mcp-remote <url>` bridge (the
// shape Claude Desktop's config uses to mount an http MCP server) must
// resolve to the SAME target as a plain {"type":"http","url":...}
// registration for the identical address, even though neither the command
// string nor the JSON shape matches literally.
func TestDuplicateToolsetRegistrationsNormalizesNpxMcpRemoteWrapper(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"browseros": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}
	}`)
	writeFile(t, claudeDesktopConfigPath(home), `{
		"mcpServers": {"browserOS": {"command": "npx", "args": ["mcp-remote", "http://127.0.0.1:9000/mcp"]}}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("npx-mcp-remote wrapper vs direct url registration = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, `"browseros"`) || !strings.Contains(check.Detail, `"browserOS"`) {
		t.Errorf("expected both names (browseros, browserOS) named in detail:\n%s", check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsNormalizesNpxMcpRemoteWrapperWithYesFlag
// pins a cog-review-flagged gap in the fix above: mcpEntryTarget's
// mcp-remote detection originally required args[0] == "mcp-remote"
// literally, missing the extremely common `npx -y mcp-remote <url>` form
// (the "-y"/"--yes" flag skips npx's install-confirmation prompt, so this
// shape is what a real generated config is more likely to contain than the
// bare no-flag form the test above covers). This must normalize to the
// same target as a plain {"url": ...} registration exactly like the
// no-flag case does.
func TestDuplicateToolsetRegistrationsNormalizesNpxMcpRemoteWrapperWithYesFlag(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"browseros": {"type": "http", "url": "http://127.0.0.1:9000/mcp"}}
	}`)
	writeFile(t, claudeDesktopConfigPath(home), `{
		"mcpServers": {"browserOS": {"command": "npx", "args": ["-y", "mcp-remote", "http://127.0.0.1:9000/mcp"]}}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("npx -y mcp-remote wrapper vs direct url registration = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, `"browseros"`) || !strings.Contains(check.Detail, `"browserOS"`) {
		t.Errorf("expected both names (browseros, browserOS) named in detail:\n%s", check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsOKWhenAllDistinct is the negative case:
// two genuinely different targets must not be flagged.
func TestDuplicateToolsetRegistrationsOKWhenAllDistinct(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {
			"alpha": {"type": "http", "url": "http://127.0.0.1:9001/mcp"},
			"beta": {"type": "http", "url": "http://127.0.0.1:9002/mcp"}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusOK {
		t.Fatalf("two distinct targets = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsDoesNotCollapseGenericLaunchers is the
// regression test for a false-positive this check almost shipped with:
// "uvx", "npx", "python3" and similar generic package-runner commands are
// legitimately shared by many unrelated MCP servers. Two registrations that
// share the bare command but launch DIFFERENT packages via different args
// (uvx blender-mcp vs uvx some-other-tool) must NOT be flagged as the same
// toolset just because mcpEntryTarget once used the bare command alone.
func TestDuplicateToolsetRegistrationsDoesNotCollapseGenericLaunchers(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {
			"blender": {"command": "uvx", "args": ["blender-mcp"]},
			"some-other-tool": {"command": "uvx", "args": ["some-other-tool-mcp"]}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusOK {
		t.Fatalf("two DIFFERENT uvx-launched packages = %s, want OK (bare-command target would be a false positive); detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsDoesNotCollapseNpxLaunchersOnIncidentalURL
// pins a cog-review-flagged false positive: mcpEntryTarget's npx-wrapper
// normalization is documented (and, per
// TestDuplicateToolsetRegistrationsNormalizesNpxMcpRemoteWrapper above,
// tested) to apply ONLY to the `npx mcp-remote <url>` bridge shape. It must
// not treat ANY URL-shaped argument of ANY npx-launched server as the
// mount target -- two unrelated npx packages that each happen to take a
// URL-shaped flag of their own (--api-base, --callback, ...) sharing that
// incidental value must not collapse onto the same target and get flagged
// as a duplicate registration.
func TestDuplicateToolsetRegistrationsDoesNotCollapseNpxLaunchersOnIncidentalURL(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {
			"tool-a": {"command": "npx", "args": ["some-tool-mcp", "--api-base", "https://api.example.com/v1"]},
			"tool-b": {"command": "npx", "args": ["unrelated-tool-mcp", "--callback", "https://api.example.com/v1"]}
		}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusOK {
		t.Fatalf("two DIFFERENT npx-launched packages sharing an incidental URL-shaped flag = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicateToolsetRegistrationsCatchesIdenticalGenericLauncherArgs is the
// paired positive case: the SAME command AND args registered twice (the
// live-evidence "blender" shape -- ~/.claude.json's project scope and Claude
// Desktop's config both mounting `uvx blender-mcp` under the same name) IS a
// genuine duplicate and must still be caught.
func TestDuplicateToolsetRegistrationsCatchesIdenticalGenericLauncherArgs(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"blender": {"command": "uvx", "args": ["blender-mcp"]}}
	}`)
	writeFile(t, claudeDesktopConfigPath(home), `{
		"mcpServers": {"blender": {"command": "uvx", "args": ["blender-mcp"]}}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("identical command+args registered in two scopes = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
}

// TestClaudeDesktopConfigPathIsPlatformAware pins the cog-review-flagged
// gap: Claude Desktop's config path was hardcoded to the macOS location
// with no runtime.GOOS branching, so the cross-scope duplicate this check
// exists to catch went silently unscanned on Windows/Linux. Exercises all
// three OS branches directly via the *ForGOOS variant rather than relying
// on the host's actual GOOS, so this test's coverage doesn't itself depend
// on which platform CI happens to run it on.
func TestClaudeDesktopConfigPathIsPlatformAware(t *testing.T) {
	home := "/home/tester"
	cases := []struct {
		goos string
		env  map[string]string
		want string
	}{
		{"darwin", nil, filepath.Join(home, "Library", "Application Support", "Claude", "claude_desktop_config.json")},
		{"linux", nil, filepath.Join(home, ".config", "Claude", "claude_desktop_config.json")},
		{"windows", map[string]string{"APPDATA": `C:\Users\tester\AppData\Roaming`}, filepath.Join(`C:\Users\tester\AppData\Roaming`, "Claude", "claude_desktop_config.json")},
	}
	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := claudeDesktopConfigPathForGOOS(home, tc.goos); got != tc.want {
				t.Errorf("claudeDesktopConfigPathForGOOS(%q, %q) = %q, want %q", home, tc.goos, got, tc.want)
			}
		})
	}
}

// TestClaudeCodeManagedSettingsPathIsPlatformAware is the same coverage for
// the machine-wide managed-settings.json scope.
func TestClaudeCodeManagedSettingsPathIsPlatformAware(t *testing.T) {
	cases := []struct {
		goos string
		env  map[string]string
		want string
	}{
		{"darwin", nil, filepath.Join(string(filepath.Separator), "Library", "Application Support", "ClaudeCode", "managed-settings.json")},
		{"linux", nil, filepath.Join(string(filepath.Separator), "etc", "claude-code", "managed-settings.json")},
		{"windows", map[string]string{"ProgramData": `C:\ProgramData`}, filepath.Join(`C:\ProgramData`, "ClaudeCode", "managed-settings.json")},
	}
	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := claudeCodeManagedSettingsPathForGOOS(tc.goos); got != tc.want {
				t.Errorf("claudeCodeManagedSettingsPathForGOOS(%q) = %q, want %q", tc.goos, got, tc.want)
			}
		})
	}
}

// TestRedactMCPTargetStripsQueryAndUserinfo pins the cog-review-flagged
// credential leak: an http MCP registration's URL can carry an auth token
// in its query string or userinfo, and doctor's report is built to be
// shared/logged/pasted for diagnosis, so the raw target must never reach
// the report text.
func TestRedactMCPTargetStripsQueryAndUserinfo(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "query string token stripped",
			input: "https://mcp.example.com/sse?token=SECRET123",
			want:  "https://mcp.example.com/sse",
		},
		{
			name:  "userinfo stripped",
			input: "https://user:hunter2@mcp.example.com/sse",
			want:  "https://mcp.example.com/sse",
		},
		{
			name:  "no query, no userinfo -- unchanged",
			input: "http://127.0.0.1:9000/mcp",
			want:  "http://127.0.0.1:9000/mcp",
		},
		{
			name:  "non-URL stdio target passes through unchanged",
			input: "uvx blender-mcp",
			want:  "uvx blender-mcp",
		},
		{
			// cog-review-flagged regression: a malformed percent-escape
			// inside the userinfo itself (an invalid escape in the
			// password) makes net/url.Parse fail on this string entirely
			// (its own userinfo unescaper returns an EscapeError), so the
			// old code's Parse-then-clear-User approach fell through to
			// `return s` with the raw credential still attached. The fix
			// strips userinfo via string slicing before Parse is ever
			// attempted, independent of whether Parse subsequently
			// succeeds.
			name:  "userinfo with malformed percent-escape stripped even though url.Parse fails",
			input: "https://user:p%2@host/mcp",
			want:  "https://host/mcp",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactMCPTarget(tc.input)
			if got != tc.want {
				t.Errorf("redactMCPTarget(%q) = %q, want %q", tc.input, got, tc.want)
			}
			if strings.Contains(got, "p%2") || strings.Contains(got, "hunter2") {
				t.Errorf("redactMCPTarget(%q) = %q, credential leaked into output", tc.input, got)
			}
		})
	}
}

// TestDuplicateToolsetRegistrationsNeverPrintsRawSecretToken is the
// end-to-end regression test for the cog-review credential-leak finding:
// two registrations of the same server, each embedding a DIFFERENT
// query-string token (the realistic per-registration-credential shape),
// must still collide as the same target (grouping happens on the redacted
// form) AND the report text must never contain either raw token.
func TestDuplicateToolsetRegistrationsNeverPrintsRawSecretToken(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude.json"), `{
		"mcpServers": {"remote-a": {"type": "http", "url": "https://mcp.example.com/sse?token=SECRET_TOKEN_ONE"}}
	}`)
	writeFile(t, claudeDesktopConfigPath(home), `{
		"mcpServers": {"remote-b": {"type": "http", "url": "https://mcp.example.com/sse?token=SECRET_TOKEN_TWO"}}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate toolset registrations")
	if check.Status != StatusWarn {
		t.Fatalf("same endpoint, different query tokens, in two scopes = %s, want WARN (redacted grouping should still collide them); detail=%s", check.Status, check.Detail)
	}
	if strings.Contains(check.Detail, "SECRET_TOKEN_ONE") || strings.Contains(check.Detail, "SECRET_TOKEN_TWO") {
		t.Fatalf("report detail contains a raw query-string token -- credential leak:\n%s", check.Detail)
	}
	if !strings.Contains(check.Detail, "https://mcp.example.com/sse") {
		t.Errorf("expected the redacted (no-query) target named in detail:\n%s", check.Detail)
	}
}

// TestDuplicatePermissionEntriesWarns reproduces #568's finding: the exact
// same permission string present in both settings.json and
// settings.local.json's allow lists.
func TestDuplicatePermissionEntriesWarns(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude", "settings.json"), `{
		"permissions": {"allow": ["mcp__cogos-v3__cog_dispatch_to_harness", "WebSearch"]}
	}`)
	writeFile(t, filepath.Join(home, ".claude", "settings.local.json"), `{
		"permissions": {"allow": ["mcp__cogos-v3__cog_dispatch_to_harness", "Bash(ls)"]}
	}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate permission entries")
	if check.Status != StatusWarn {
		t.Fatalf("duplicated permission entry = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "mcp__cogos-v3__cog_dispatch_to_harness") {
		t.Errorf("expected the duplicated entry named in detail:\n%s", check.Detail)
	}
	if strings.Contains(check.Detail, "WebSearch") || strings.Contains(check.Detail, "Bash(ls)") {
		t.Errorf("non-duplicated entries should not appear in detail:\n%s", check.Detail)
	}
}

// TestDuplicatePermissionEntriesOKWhenNoOverlap is the negative case.
func TestDuplicatePermissionEntriesOKWhenNoOverlap(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude", "settings.json"), `{"permissions": {"allow": ["WebSearch"]}}`)
	writeFile(t, filepath.Join(home, ".claude", "settings.local.json"), `{"permissions": {"allow": ["Bash(ls)"]}}`)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate permission entries")
	if check.Status != StatusOK {
		t.Fatalf("no overlapping entries = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestDuplicatePermissionEntriesUnknownOnMalformedSettings pins the same
// never-OK-when-unverified contract for loadPermissionEntries /
// doctorDuplicatePermissions: a settings file that exists but is not valid
// JSON cannot be compared for duplicates -- "no duplicates found" would be a
// claim about content this check never actually read. A missing file is
// fine (nothing to compare, so it degenerates to an empty entry set); a
// present-but-corrupt one must report UNKNOWN and name the broken file,
// never fall through to OK.
func TestDuplicatePermissionEntriesUnknownOnMalformedSettings(t *testing.T) {
	table := []struct {
		name           string
		settingsJSON   string
		settingsLocal  string
		writeLocalFile bool
	}{
		{
			name:           "malformed settings.json, missing settings.local.json",
			settingsJSON:   `{"permissions": {"allow": ["WebSearch",`, // truncated/invalid JSON
			writeLocalFile: false,
		},
		{
			name:           "valid settings.json, malformed settings.local.json",
			settingsJSON:   `{"permissions": {"allow": ["WebSearch"]}}`,
			settingsLocal:  `not json at all`,
			writeLocalFile: true,
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)

			sjPath := filepath.Join(home, ".claude", "settings.json")
			writeFile(t, sjPath, tc.settingsJSON)
			var slPath string
			if tc.writeLocalFile {
				slPath = filepath.Join(home, ".claude", "settings.local.json")
				writeFile(t, slPath, tc.settingsLocal)
			}

			report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
			check := findCheck(t, report, "context construction", "duplicate permission entries")
			if check.Status != StatusUnknown {
				t.Fatalf("malformed settings file = %s, want UNKNOWN (never OK-no-duplicates); detail=%s", check.Status, check.Detail)
			}
			if !strings.Contains(check.Detail, sjPath) && (slPath == "" || !strings.Contains(check.Detail, slPath)) {
				t.Errorf("expected the broken file's path named in detail (sjPath=%s slPath=%s):\n%s", sjPath, slPath, check.Detail)
			}
		})
	}
}

// TestDuplicatePermissionEntriesOKWhenSettingsFilesMissing is the companion
// negative case: absence of a settings file is normal (not everyone has a
// settings.local.json), and must not be confused with the unreadable/corrupt
// case above -- both currently report through the same sjErr/slErr path in
// doctorDuplicatePermissions, so this pins that a plain "file not found"
// still resolves to OK rather than UNKNOWN.
func TestDuplicatePermissionEntriesOKWhenSettingsFilesMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	// Neither settings.json nor settings.local.json is written.

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "duplicate permission entries")
	if check.Status != StatusOK {
		t.Fatalf("no settings files present = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// TestContextBudgetWarnsOverThreshold pins the WARN-above-threshold half of
// the always-loaded file budget check.
func TestContextBudgetWarnsOverThreshold(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	big := strings.Repeat("x", 3*1024) // 3KB, over a 1KB threshold
	writeFile(t, filepath.Join(home, ".claude", "CLAUDE.md"), big)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true, ContextBudgetKB: 1})
	check := findCheck(t, report, "context construction", "always-loaded file budgets")
	if check.Status != StatusWarn {
		t.Fatalf("CLAUDE.md over the 1KB threshold = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "OVER 1KB BUDGET") {
		t.Errorf("expected an OVER BUDGET tag in detail:\n%s", check.Detail)
	}
}

// TestContextBudgetOKUnderThreshold pins the OK-under-threshold half, and
// that a missing file (~/.hermes profile MEMORY.md never written here) is
// simply absent from the listing rather than reported as an error.
func TestContextBudgetOKUnderThreshold(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	writeFile(t, filepath.Join(home, ".claude", "CLAUDE.md"), "small")

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true, ContextBudgetKB: 64})
	check := findCheck(t, report, "context construction", "always-loaded file budgets")
	if check.Status != StatusOK {
		t.Fatalf("small CLAUDE.md under threshold = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}

// settingsJSONWithHookCommand builds a valid settings.json document (via
// encoding/json, not string templating) declaring one PreToolUse hook whose
// command embeds scriptPath quoted the way this codebase's real
// hookrun.py-wrapped commands do: `python3 "<path>" dispatch`.
func settingsJSONWithHookCommand(t *testing.T, scriptPath string) string {
	t.Helper()
	doc := map[string]any{
		"hooks": map[string]any{
			"PreToolUse": []any{
				map[string]any{
					"matcher": "*",
					"hooks": []any{
						map[string]any{
							"type":    "command",
							"command": fmt.Sprintf(`python3 "%s" dispatch`, scriptPath),
						},
					},
				},
			},
		},
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal settings fixture: %v", err)
	}
	return string(data)
}

// TestDeadHookCommandsWarns pins the dead-hook-path detection against a
// compound hookrun.py-wrapped command string, the exact shape this
// codebase's own settings.json uses.
func TestDeadHookCommandsWarns(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	missing := filepath.Join(home, ".claude", "hooks", "missing_hook.py")
	settings := settingsJSONWithHookCommand(t, missing)
	writeFile(t, filepath.Join(home, ".claude", "settings.json"), settings)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "dead hook commands")
	if check.Status != StatusWarn {
		t.Fatalf("hook referencing a missing script = %s, want WARN; detail=%s", check.Status, check.Detail)
	}
	if !strings.Contains(check.Detail, "PreToolUse") || !strings.Contains(check.Detail, missing) {
		t.Errorf("expected event name and missing path in detail:\n%s", check.Detail)
	}
}

// TestDeadHookCommandsOKWhenPathExists is the negative case: a hook whose
// referenced script genuinely exists must not be flagged.
func TestDeadHookCommandsOKWhenPathExists(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	realScript := filepath.Join(home, ".claude", "hooks", "real_hook.py")
	writeFile(t, realScript, "# real script")
	settings := settingsJSONWithHookCommand(t, realScript)
	writeFile(t, filepath.Join(home, ".claude", "settings.json"), settings)

	report := RunDoctor(t.TempDir(), DoctorOptions{SkipNetwork: true})
	check := findCheck(t, report, "context construction", "dead hook commands")
	if check.Status != StatusOK {
		t.Fatalf("hook referencing an existing script = %s, want OK; detail=%s", check.Status, check.Detail)
	}
}
