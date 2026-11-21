package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── ResolveURI ────────────────────────────────────────────────────────────────

func TestResolveURIDirectPatterns(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	cases := []struct {
		uri      string
		wantPath string
		wantFrag string
	}{
		// mem — no extension added
		{"cog://mem/semantic/insights/foo.cog.md", root + "/.cog/mem/semantic/insights/foo.cog.md", ""},
		// mem with fragment
		{"cog://mem/semantic/insights/foo.cog.md#The-Seed", root + "/.cog/mem/semantic/insights/foo.cog.md", "The-Seed"},
		// spec — .cog.md suffix appended
		{"cog://spec/my-spec", root + "/.cog/specs/my-spec.cog.md", ""},
		// status — .json suffix
		{"cog://status/kernel", root + "/.cog/status/kernel.json", ""},
		// ontology — .cog.md suffix
		{"cog://ontology/crystal", root + "/.cog/ontology/crystal.cog.md", ""},
		// conf / config aliases
		{"cog://conf/kernel.yaml", root + "/.cog/config/kernel.yaml", ""},
		{"cog://config/identity.yaml", root + "/.cog/config/identity.yaml", ""},
		// kernel raw
		{"cog://kernel/config/identity.yaml", root + "/.cog/config/identity.yaml", ""},
		// docs
		{"cog://docs/framework-status.md", root + "/.cog/docs/framework-status.md", ""},
		// hooks
		{"cog://hooks/session-start.sh", root + "/.cog/hooks/session-start.sh", ""},
		// work
		{"cog://work/sprint-1.md", root + "/.cog/work/sprint-1.md", ""},
		// handoff (singular — appends .md)
		{"cog://handoff/2024-01-hand", root + "/.cog/handoffs/2024-01-hand.md", ""},
	}

	for _, tc := range cases {
		t.Run(tc.uri, func(t *testing.T) {
			t.Parallel()
			res, err := ResolveURI(root, tc.uri)
			if err != nil {
				t.Fatalf("ResolveURI(%q): %v", tc.uri, err)
			}
			if filepath.ToSlash(res.Path) != filepath.ToSlash(tc.wantPath) {
				t.Errorf("Path = %q; want %q", res.Path, tc.wantPath)
			}
			if res.Fragment != tc.wantFrag {
				t.Errorf("Fragment = %q; want %q", res.Fragment, tc.wantFrag)
			}
		})
	}
}

func TestResolveURISingleton(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://crystal")
	if err != nil {
		t.Fatalf("ResolveURI: %v", err)
	}
	want := root + "/.cog/ledger/crystal.json"
	if filepath.ToSlash(res.Path) != want {
		t.Errorf("Path = %q; want %q", res.Path, want)
	}
}

func TestResolveURIDirectoryPattern(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://roles/guardian")
	if err != nil {
		t.Fatalf("ResolveURI: %v", err)
	}
	if !strings.HasSuffix(res.Path, "/") {
		t.Errorf("directory pattern must return a path ending in '/'; got %q", res.Path)
	}
	if !strings.Contains(res.Path, "roles/guardian") {
		t.Errorf("path %q should contain 'roles/guardian'", res.Path)
	}
}

func TestResolveURIExtBase(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://skills/commit")
	if err != nil {
		t.Fatalf("ResolveURI: %v", err)
	}
	if !strings.Contains(filepath.ToSlash(res.Path), ".claude/skills/commit") {
		t.Errorf("expected .claude/skills path, got %q", res.Path)
	}
}

func TestResolveURIInvalidScheme(t *testing.T) {
	t.Parallel()
	_, err := ResolveURI("/workspace", "https://example.com/foo")
	if err == nil {
		t.Error("expected error for non-cog:// URI")
	}
}

func TestResolveURIUnknownType(t *testing.T) {
	t.Parallel()
	_, err := ResolveURI("/workspace", "cog://nonexistent/foo")
	if err == nil {
		t.Error("expected error for unknown cog:// type")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention unknown type; got %v", err)
	}
}

func TestResolveURIFragmentOnly(t *testing.T) {
	t.Parallel()
	// A URI that is just a type with no path but has a fragment.
	res, err := ResolveURI("/workspace", "cog://mem/#anchor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Fragment != "anchor" {
		t.Errorf("Fragment = %q; want %q", res.Fragment, "anchor")
	}
}

// ── PathToURI ─────────────────────────────────────────────────────────────────

func TestPathToURIMemory(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	got, err := PathToURI(root, root+"/.cog/mem/semantic/insights/foo.cog.md")
	if err != nil {
		t.Fatalf("PathToURI: %v", err)
	}
	want := "cog:mem/semantic/insights/foo.cog.md"
	if got != want {
		t.Errorf("got %q; want %q", got, want)
	}
}

func TestPathToURIConfig(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	got, err := PathToURI(root, root+"/.cog/config/kernel.yaml")
	if err != nil {
		t.Fatalf("PathToURI: %v", err)
	}
	if !strings.HasPrefix(got, "cog:conf/") {
		t.Errorf("got %q; want cog:conf/ prefix", got)
	}
}

func TestPathToURIAgents(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	got, err := PathToURI(root, root+"/.cog/bin/agents/cog.md")
	if err != nil {
		t.Fatalf("PathToURI: %v", err)
	}
	// .md stripped for agents
	want := "cog:agents/cog"
	if got != want {
		t.Errorf("got %q; want %q", got, want)
	}
}

func TestPathToURIRelativePath(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	// Relative path should be treated as workspace-relative.
	got, err := PathToURI(root, ".cog/mem/semantic/foo.cog.md")
	if err != nil {
		t.Fatalf("PathToURI: %v", err)
	}
	if !strings.HasPrefix(got, "cog:mem/") {
		t.Errorf("got %q; want cog:mem/ prefix", got)
	}
}

func TestPathToURIUnknownPath(t *testing.T) {
	t.Parallel()
	_, err := PathToURI("/workspace", "/workspace/unrelated/file.txt")
	if err == nil {
		t.Error("expected error for path outside workspace")
	}
}

// ── ExtractInlineRefs ─────────────────────────────────────────────────────────

func TestExtractInlineRefsBasic(t *testing.T) {
	t.Parallel()
	// Test with bare cog: form (ADR-067 canonical)
	content := `See cog:mem/semantic/foo.cog.md and cog:conf/kernel.yaml for details.`
	refs := ExtractInlineRefs(content)
	if len(refs) != 2 {
		t.Fatalf("got %d refs; want 2: %v", len(refs), refs)
	}
	// Sorted output.
	if refs[0] != "cog:conf/kernel.yaml" {
		t.Errorf("refs[0] = %q; want cog:conf/kernel.yaml", refs[0])
	}
	if refs[1] != "cog:mem/semantic/foo.cog.md" {
		t.Errorf("refs[1] = %q; want cog:mem/semantic/foo.cog.md", refs[1])
	}
}

func TestExtractInlineRefsDeduplicated(t *testing.T) {
	t.Parallel()
	content := `cog:mem/foo.md appears here and cog:mem/foo.md again here.`
	refs := ExtractInlineRefs(content)
	if len(refs) != 1 {
		t.Errorf("got %d refs; want 1 (deduplicated): %v", len(refs), refs)
	}
}

func TestExtractInlineRefsWithFragment(t *testing.T) {
	t.Parallel()
	content := `Read cog:mem/doc.cog.md#the-seed for context.`
	refs := ExtractInlineRefs(content)
	if len(refs) != 1 {
		t.Fatalf("got %d refs; want 1: %v", len(refs), refs)
	}
	if !strings.Contains(refs[0], "#the-seed") {
		t.Errorf("fragment not captured in ref %q", refs[0])
	}
}

func TestExtractInlineRefsEmpty(t *testing.T) {
	t.Parallel()
	if refs := ExtractInlineRefs("no URIs here"); refs != nil {
		t.Errorf("expected nil; got %v", refs)
	}
	if refs := ExtractInlineRefs(""); refs != nil {
		t.Errorf("expected nil for empty string; got %v", refs)
	}
}

func TestExtractInlineRefsInMarkdownLink(t *testing.T) {
	t.Parallel()
	content := `[link](cog:mem/semantic/foo.cog.md) and plain cog:mem/bar.cog.md`
	refs := ExtractInlineRefs(content)
	// Both should be found; parenthesis is not captured by the pattern.
	if len(refs) != 2 {
		t.Errorf("got %d refs; want 2: %v", len(refs), refs)
	}
}

// ── ResolveURI — glob pattern ─────────────────────────────────────────────────

func TestResolveURIGlobPattern(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	adrDir := filepath.Join(root, ".cog", "adr")
	if err := os.MkdirAll(adrDir, 0755); err != nil {
		t.Fatal(err)
	}
	// Create a file that matches the ADR glob: 001-*.md
	adrFile := filepath.Join(adrDir, "001-use-go.md")
	if err := os.WriteFile(adrFile, []byte("# ADR 001"), 0644); err != nil {
		t.Fatal(err)
	}

	res, err := ResolveURI(root, "cog://adr/001")
	if err != nil {
		t.Fatalf("ResolveURI: %v", err)
	}
	if res.Path != adrFile {
		t.Errorf("Path = %q; want %q", res.Path, adrFile)
	}
}

func TestResolveURIGlobNoMatch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// ADR directory doesn't exist — glob should find no files.
	_, err := ResolveURI(root, "cog://adr/999")
	if err == nil {
		t.Error("expected error when no glob matches")
	}
}

// TestResolveURISanitizesNTFSIllegalChars regression-tests myrgic/cogos#489
// round 5: resolveProjection rejected literal ".." and absolute paths but
// never escaped NTFS-illegal characters, so a colon-bearing uriPath (the
// exact #489 repro shape — a ledger session ID like "http:cog") reached
// filepath.Join with the raw colon intact via every one of the "direct",
// "directory", and "glob" branches.
func TestResolveURISanitizesNTFSIllegalChars(t *testing.T) {
	t.Parallel()
	root := "/workspace"

	cases := []struct {
		name    string
		uri     string
		wantNot string // substring that must NOT appear in the resolved path
	}{
		{"ledger directory pattern", "cog://ledger/http:cog", "http:cog"},
		{"mem direct pattern", "cog://mem/http:cog.cog.md", "http:cog.cog.md"},
		{"role directory pattern", "cog://role/http:cog", "http:cog"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := ResolveURI(root, tc.uri)
			if err != nil {
				t.Fatalf("ResolveURI(%q): %v", tc.uri, err)
			}
			if strings.Contains(res.Path, tc.wantNot) {
				t.Errorf("ResolveURI(%q).Path = %q still contains the raw colon-bearing component %q — NTFS-illegal, myrgic/cogos#489",
					tc.uri, res.Path, tc.wantNot)
			}
			// pkg/pathsafe.SanitizeComponent percent-escapes ':' as "%3A".
			if !strings.Contains(res.Path, "%3A") {
				t.Errorf("ResolveURI(%q).Path = %q missing expected sanitized form containing %q", tc.uri, res.Path, "%3A")
			}
		})
	}
}

// TestResolveURIStillWorksForOrdinaryPaths guards the sanitization fix
// against being overly strict: ordinary multi-segment, extension-bearing
// paths must resolve exactly as before.
func TestResolveURIStillWorksForOrdinaryPaths(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://mem/semantic/insights/foo.cog.md")
	if err != nil {
		t.Fatalf("ResolveURI: %v", err)
	}
	want := root + "/.cog/mem/semantic/insights/foo.cog.md"
	if filepath.ToSlash(res.Path) != want {
		t.Errorf("Path = %q; want %q", res.Path, want)
	}
}

// ── cogURIPattern ─────────────────────────────────────────────────────────────

func TestCogURIPatternDoesNotMatchHTTPS(t *testing.T) {
	t.Parallel()
	content := `See https://example.com/foo and cog:mem/bar.md`
	refs := ExtractInlineRefs(content)
	for _, r := range refs {
		if strings.HasPrefix(r, "https://") {
			t.Errorf("https:// URI incorrectly matched: %q", r)
		}
	}
	if len(refs) != 1 {
		t.Errorf("expected 1 cog: ref; got %d: %v", len(refs), refs)
	}
}

// ── Registered aliases (URI fix U3) ──────────────────────────────────────────

func TestResolveURIAliasMemory(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://memory/semantic/insights/foo.cog.md")
	if err != nil {
		t.Fatalf("cog://memory: %v", err)
	}
	want := filepath.Join(root, ".cog", "mem", "semantic", "insights", "foo.cog.md")
	if res.Path != want {
		t.Errorf("cog://memory path = %q, want %q", res.Path, want)
	}
}

func TestResolveURIAliasIdentityCard(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://I/")
	if err != nil {
		t.Fatalf("cog://I/: %v", err)
	}
	want := filepath.Join(root, ".cog", "bin", "agents", "identities", "identity_cog_interface.md")
	if res.Path != want {
		t.Errorf("cog://I/ path = %q, want %q", res.Path, want)
	}
}

func TestResolveURIAliasArchitecture(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://architecture/adrs/ADR-foo.cog.md")
	if err != nil {
		t.Fatalf("cog://architecture: %v", err)
	}
	want := filepath.Join(root, "architecture", "adrs", "ADR-foo.cog.md")
	if res.Path != want {
		t.Errorf("cog://architecture path = %q, want %q", res.Path, want)
	}
}

func TestResolveURIAliasRFC(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog://rfc/RFC-foo.cog.md")
	if err != nil {
		t.Fatalf("cog://rfc: %v", err)
	}
	want := filepath.Join(root, "architecture", "rfcs", "RFC-foo.cog.md")
	if res.Path != want {
		t.Errorf("cog://rfc path = %q, want %q", res.Path, want)
	}
}

func TestResolveURIAliasSkillsResolvesToSkillsDir(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	res, err := ResolveURI(root, "cog:skill/dispatch-agent")
	if err != nil {
		t.Fatalf("cog:skill: %v", err)
	}
	want := filepath.Join(root, ".claude", "skills", "dispatch-agent") + string(filepath.Separator)
	if res.Path != want {
		t.Errorf("cog:skill path = %q, want %q", res.Path, want)
	}
}

// cog:secret is a vault REFERENCE scheme (cli_doctor_secrets.go:25). It must
// never resolve to a filesystem path or value: ResolveURI has to reject it.
func TestResolveURISecretIsNotResolvable(t *testing.T) {
	t.Parallel()
	root := "/workspace"
	for _, uri := range []string{"cog:secret/discord/cog", "cog://secret/discord/cog"} {
		res, err := ResolveURI(root, uri)
		if err == nil {
			t.Errorf("%s resolved to %+v; a secret reference must not resolve to a path/value", uri, res)
		}
		if res != nil {
			t.Errorf("%s returned a non-nil resolution %+v alongside the error", uri, res)
		}
	}
}
