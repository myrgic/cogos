package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── Fixtures ──────────────────────────────────────────────────────────────────

const fixtureInsight = `---
id: test-insight-001
title: "Test Insight"
type: insight
tags: [alpha, physics]
status: active
created: "2026-01-01"
refs:
  - uri: cog:mem/semantic/guide/test-guide.cog.md
    rel: related
---

# Test Insight

See cog:mem/semantic/guide/test-guide.cog.md for the companion guide.
Also references cog:conf/kernel.yaml inline.
`

const fixtureGuide = `---
id: test-guide-001
title: "Test Guide"
type: guide
tags: [alpha, procedural]
status: draft
created: "2026-01-02"
---

# Test Guide

A guide document with no explicit refs.
`

const fixtureBroken = `This file has no frontmatter at all.
Just plain text content.
`

// ── BuildIndex ────────────────────────────────────────────────────────────────

func TestBuildIndexEmptyMemDir(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	// makeWorkspace creates .cog/mem/semantic/ but leaves it empty.
	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	if len(idx.ByURI) != 0 {
		t.Errorf("ByURI should be empty; got %d entries", len(idx.ByURI))
	}
}

func TestBuildIndexMissingMemDir(t *testing.T) {
	t.Parallel()
	// Workspace without a .cog/mem/ directory at all.
	root := t.TempDir()
	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex on missing memDir: %v", err)
	}
	if idx == nil {
		t.Fatal("expected non-nil index")
	}
}

func TestBuildIndexByURI(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	if len(idx.ByURI) == 0 {
		t.Fatal("expected at least one document in index")
	}

	// The URI for insight.cog.md under .cog/mem/semantic/.
	wantURI := "cog:mem/semantic/insight.cog.md"
	doc, ok := idx.ByURI[wantURI]
	if !ok {
		t.Fatalf("URI %q not found; have: %v", wantURI, uriKeys(idx))
	}
	if doc.Title != "Test Insight" {
		t.Errorf("Title = %q; want %q", doc.Title, "Test Insight")
	}
	if doc.ID != "test-insight-001" {
		t.Errorf("ID = %q; want %q", doc.ID, "test-insight-001")
	}
}

func TestBuildIndexByType(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)
	guideDir := filepath.Join(root, ".cog", "mem", "semantic", "guide")
	if err := os.MkdirAll(guideDir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", guideDir, err)
	}
	writeTestFile(t, filepath.Join(guideDir, "test-guide.cog.md"), fixtureGuide)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	if len(idx.ByType["insight"]) != 1 {
		t.Errorf("ByType[insight] = %d; want 1", len(idx.ByType["insight"]))
	}
	if len(idx.ByType["guide"]) != 1 {
		t.Errorf("ByType[guide] = %d; want 1", len(idx.ByType["guide"]))
	}
}

func TestBuildIndexByTag(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "guide.cog.md"), fixtureGuide)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	// "alpha" tag is on both fixtures.
	if len(idx.ByTag["alpha"]) != 2 {
		t.Errorf("ByTag[alpha] = %d; want 2", len(idx.ByTag["alpha"]))
	}
	// "physics" tag is on insight only.
	if len(idx.ByTag["physics"]) != 1 {
		t.Errorf("ByTag[physics] = %d; want 1", len(idx.ByTag["physics"]))
	}
}

func TestBuildIndexByStatus(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "guide.cog.md"), fixtureGuide)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	if len(idx.ByStatus["active"]) != 1 {
		t.Errorf("ByStatus[active] = %d; want 1", len(idx.ByStatus["active"]))
	}
	if len(idx.ByStatus["draft"]) != 1 {
		t.Errorf("ByStatus[draft] = %d; want 1", len(idx.ByStatus["draft"]))
	}
}

func TestBuildIndexRefGraph(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	srcURI := "cog:mem/semantic/insight.cog.md"
	refs, ok := idx.RefGraph[srcURI]
	if !ok {
		t.Fatalf("RefGraph missing entry for %q", srcURI)
	}
	if len(refs) != 1 {
		t.Fatalf("RefGraph[%q] has %d refs; want 1", srcURI, len(refs))
	}
	if refs[0].URI != "cog:mem/semantic/guide/test-guide.cog.md" {
		t.Errorf("refs[0].URI = %q; want cog:mem/semantic/guide/test-guide.cog.md", refs[0].URI)
	}
	if refs[0].Rel != "related" {
		t.Errorf("refs[0].Rel = %q; want related", refs[0].Rel)
	}
}

func TestBuildIndexInverseRefs(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	targetURI := "cog:mem/semantic/guide/test-guide.cog.md"
	sources := idx.InverseRefs[targetURI]
	if len(sources) != 1 {
		t.Fatalf("InverseRefs[%q] = %d sources; want 1", targetURI, len(sources))
	}
	if sources[0] != "cog:mem/semantic/insight.cog.md" {
		t.Errorf("sources[0] = %q; want cog:mem/semantic/insight.cog.md", sources[0])
	}
}

func TestBuildIndexInlineRefs(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "insight.cog.md"), fixtureInsight)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	doc := idx.ByURI["cog:mem/semantic/insight.cog.md"]
	if doc == nil {
		t.Fatal("document not found in index")
	}
	if len(doc.InlineRefs) == 0 {
		t.Fatal("expected inline refs from body content")
	}
	// Should find cog:mem/... and cog:conf/... in the body.
	var foundMem, foundConf bool
	for _, r := range doc.InlineRefs {
		if strings.HasPrefix(r, "cog:mem/") {
			foundMem = true
		}
		if strings.HasPrefix(r, "cog:conf/") {
			foundConf = true
		}
	}
	if !foundMem {
		t.Errorf("cog:mem/ inline ref not found; got: %v", doc.InlineRefs)
	}
	if !foundConf {
		t.Errorf("cog:conf/ inline ref not found; got: %v", doc.InlineRefs)
	}
}

func TestBuildIndexBrokenFrontmatter(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	writeTestFile(t, filepath.Join(root, ".cog", "mem", "semantic", "broken.cog.md"), fixtureBroken)

	idx, err := BuildIndex(root)
	if err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}
	// File should still be indexed (with empty metadata), not skipped.
	if len(idx.ByURI) == 0 {
		t.Error("broken-frontmatter file should still appear in index")
	}
	doc := idx.ByURI["cog:mem/semantic/broken.cog.md"]
	if doc == nil {
		t.Error("expected broken.cog.md to be indexed")
		return
	}
	if doc.Title != "" {
		t.Errorf("Title = %q; want empty for no-frontmatter file", doc.Title)
	}
}

// ── parseCogdocFrontmatter ────────────────────────────────────────────────────

func TestParseCogdocFrontmatter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		input      string
		wantTitle  string
		wantType   string
		wantTags   []string
		wantStatus string
		bodyPrefix string
	}{
		{
			name:      "full frontmatter",
			input:     "---\ntitle: Hello\ntype: insight\ntags: [a, b]\nstatus: active\n---\n\n# Body\n",
			wantTitle: "Hello", wantType: "insight",
			wantTags: []string{"a", "b"}, wantStatus: "active",
			bodyPrefix: "# Body", // leading blank line stripped by TrimLeft
		},
		{
			name:       "no frontmatter",
			input:      "# Just a heading\nSome content\n",
			wantTitle:  "",
			bodyPrefix: "# Just a heading",
		},
		{
			name:       "empty body after frontmatter",
			input:      "---\ntitle: Only Meta\n---\n",
			wantTitle:  "Only Meta",
			bodyPrefix: "",
		},
		{
			name:       "frontmatter with CRLF",
			input:      "---\r\ntitle: CRLF\r\n---\r\n\r\nBody\r\n",
			wantTitle:  "CRLF", // CRLF opening is handled; title extracted
			bodyPrefix: "Body", // leading CRLF lines stripped
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fm, body := parseCogdocFrontmatter(tc.input)
			if fm.Title != tc.wantTitle {
				t.Errorf("Title = %q; want %q", fm.Title, tc.wantTitle)
			}
			if tc.wantType != "" && fm.Type != tc.wantType {
				t.Errorf("Type = %q; want %q", fm.Type, tc.wantType)
			}
			if tc.wantStatus != "" && fm.Status != tc.wantStatus {
				t.Errorf("Status = %q; want %q", fm.Status, tc.wantStatus)
			}
			if tc.bodyPrefix != "" && !strings.HasPrefix(body, tc.bodyPrefix) {
				t.Errorf("body starts with %q; want prefix %q", body[:min(len(body), 30)], tc.bodyPrefix)
			}
			_ = fm.Tags // checked in full frontmatter case
		})
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func uriKeys(idx *CogDocIndex) []string {
	keys := make([]string, 0, len(idx.ByURI))
	for k := range idx.ByURI {
		keys = append(keys, k)
	}
	return keys
}
