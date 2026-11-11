//go:build !windows

package engine

import (
	"strings"
	"testing"
)

// Kernel PATH not discoverable and claude absent from the shell: WARN with the fallback version.
func TestDoctorClaudeCode_MissingWarns(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir()) // empty dir: no claude, no claude-code
	opts := DoctorOptions{host: newFakeProc(t).host(home), kernelEndpoint: "http://127.0.0.1:6931"}

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusWarn {
		t.Fatalf("want one WARN, got %+v", g.Checks)
	}
	d := g.Checks[0].Detail
	for _, want := range []string{"not found", claudeCodeVersionFallback, "claude.ai/install.sh", "ensure-claude-code.sh"} {
		if !strings.Contains(d, want) {
			t.Errorf("detail missing %q:\n%s", want, d)
		}
	}
}
