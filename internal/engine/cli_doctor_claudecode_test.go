//go:build !windows

package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDoctorClaudeCode_MissingWarns(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir()) // empty dir: no claude, no claude-code

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g)
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusWarn {
		t.Fatalf("want one WARN, got %+v", g.Checks)
	}
	d := g.Checks[0].Detail
	for _, want := range []string{"not on PATH", claudeCodeVersionFallback, "claude.ai/install.sh", "ensure-claude-code.sh"} {
		if !strings.Contains(d, want) {
			t.Errorf("detail missing %q:\n%s", want, d)
		}
	}
}

func TestDoctorClaudeCode_PresentReportsVersion(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	script := "#!/bin/sh\necho '9.8.7 (Claude Code)'\n"
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g)
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusOK {
		t.Fatalf("want one OK, got %+v", g.Checks)
	}
	if !strings.Contains(g.Checks[0].Detail, "9.8.7") {
		t.Errorf("want detected version in detail, got %q", g.Checks[0].Detail)
	}
}
