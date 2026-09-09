package engine

import (
	"strings"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// TestWorktreeReconcilerTypeIsASafeRegistryKey is the regression for the live
// defect: Type() interpolated the absolute repo root, and StatePath turned it
// into a nested directory tree inside .cog/config.
func TestWorktreeReconcilerTypeIsASafeRegistryKey(t *testing.T) {
	for _, root := range []string{
		"/Users/example/workspaces/cog",
		`C:\Users\example\workspaces\cog`,
		"/tmp/a b/weird:name",
		"/",
	} {
		r := &WorktreeReconciler{RepoRoot: root}
		got := r.Type()
		if err := reconcile.ValidateInstanceName(got); err != nil {
			t.Errorf("Type() for repoRoot %q = %q, invalid registry key: %v", root, got, err)
		}
		if !strings.HasPrefix(got, "worktree-reconciler/") {
			t.Errorf("Type() = %q, want worktree-reconciler/ prefix", got)
		}
	}
}

// Distinct roots that share a basename must not collide onto one state file.
func TestInstanceSlugDisambiguatesSameBasename(t *testing.T) {
	a := (&WorktreeReconciler{RepoRoot: "/home/a/cog"}).Type()
	b := (&WorktreeReconciler{RepoRoot: "/home/b/cog"}).Type()
	if a == b {
		t.Fatalf("distinct repo roots produced the same registry key: %q", a)
	}
}

// The key must be stable across calls, or every boot orphans the prior state.
func TestInstanceSlugIsStable(t *testing.T) {
	root := "/Users/example/workspaces/cog"
	if a, b := (&WorktreeReconciler{RepoRoot: root}).Type(), (&WorktreeReconciler{RepoRoot: root}).Type(); a != b {
		t.Fatalf("Type() not stable: %q != %q", a, b)
	}
}
