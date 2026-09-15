package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadNucleusHappyPath(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	n := mustLoadNucleus(t, cfg)

	if n.Name == "" {
		t.Error("Name is empty")
	}
	if n.Role == "" {
		t.Error("Role is empty")
	}
	if n.Card == "" {
		t.Error("Card is empty")
	}
	if n.WorkspaceRoot != root {
		t.Errorf("WorkspaceRoot = %q; want %q", n.WorkspaceRoot, root)
	}
	if n.LoadedAt.IsZero() {
		t.Error("LoadedAt is zero")
	}
}

func TestLoadNucleusIdentityName(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	n := mustLoadNucleus(t, cfg)

	// The testdata identity card has name: Test.
	if n.Name != "Test" {
		t.Errorf("Name = %q; want Test", n.Name)
	}
	if n.Role != "unit-test-identity" {
		t.Errorf("Role = %q; want unit-test-identity", n.Role)
	}
}

func TestLoadNucleusFallsBackToDefault(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// No .cog/config/identity.yaml — should fall back to embedded default.
	cfg := &Config{
		WorkspaceRoot: root,
		CogDir:        filepath.Join(root, ".cog"),
	}
	n, err := LoadNucleus(cfg)
	if err != nil {
		t.Fatalf("expected fallback to embedded default, got error: %v", err)
	}
	if n.Name != "CogOS" {
		t.Errorf("expected default identity name 'CogOS', got %q", n.Name)
	}
}

func TestLoadNucleusMissingCardFallsBack(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".cog", "config"), 0755); err != nil {
		t.Fatal(err)
	}
	// Config points to a nonexistent identity — should fall back to embedded default.
	writeTestFile(t, filepath.Join(root, ".cog", "config", "identity.yaml"),
		"default_identity: nobody\nidentity_directory: identities\n")

	cfg := &Config{WorkspaceRoot: root, CogDir: filepath.Join(root, ".cog")}
	n, err := LoadNucleus(cfg)
	if err != nil {
		t.Fatalf("expected fallback to embedded default, got error: %v", err)
	}
	if n.Name != "CogOS" {
		t.Errorf("expected fallback name 'CogOS', got %q", n.Name)
	}
}

func TestLoadNucleusMalformedFrontmatter(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)

	// Overwrite the identity card with no frontmatter.
	cardPath := filepath.Join(root, "projects", "cog_lab_package", "identities", "identity_test.md")
	writeTestFile(t, cardPath, "# No Frontmatter\nJust body text.\n")

	n, err := LoadNucleus(cfg)
	if err != nil {
		t.Fatalf("LoadNucleus with no frontmatter: %v", err)
	}
	// Name falls back to the identity name from config ("test").
	if n.Name == "" {
		t.Error("Name should fall back to config identity name")
	}
	// Card body should contain the text.
	if !strings.Contains(n.Card, "Just body text") {
		t.Errorf("Card body missing expected text; got %q", n.Card)
	}
}

func TestNucleusSummary(t *testing.T) {
	t.Parallel()
	n := makeNucleus("Cog", "workspace-guardian")
	s := n.Summary()
	if !strings.Contains(s, "Cog") {
		t.Errorf("Summary missing name; got %q", s)
	}
	if !strings.Contains(s, "workspace-guardian") {
		t.Errorf("Summary missing role; got %q", s)
	}
}

func TestParseIdentityFrontmatter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		content  string
		wantFM   identityFrontmatter
		wantBody string
	}{
		{
			name:     "full frontmatter",
			content:  "---\nname: Cog\nrole: guardian\n---\n# Body\n",
			wantFM:   identityFrontmatter{Name: "Cog", Role: "guardian"},
			wantBody: "# Body\n",
		},
		{
			name:     "no frontmatter",
			content:  "# Just body\n",
			wantFM:   identityFrontmatter{},
			wantBody: "# Just body\n",
		},
		{
			name:     "empty document",
			content:  "",
			wantFM:   identityFrontmatter{},
			wantBody: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fm, body := parseIdentityFrontmatter(tc.content)
			if fm.Name != tc.wantFM.Name {
				t.Errorf("Name = %q; want %q", fm.Name, tc.wantFM.Name)
			}
			if fm.Role != tc.wantFM.Role {
				t.Errorf("Role = %q; want %q", fm.Role, tc.wantFM.Role)
			}
			if body != tc.wantBody {
				t.Errorf("body = %q; want %q", body, tc.wantBody)
			}
		})
	}
}
