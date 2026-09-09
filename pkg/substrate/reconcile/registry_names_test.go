package reconcile

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateInstanceName_RejectsTheLiveCorruption pins the exact key that
// produced a real nested directory tree inside a workspace config dir:
// WorktreeReconciler.Type() returned "worktree-reconciler:" + absolute repo
// root, and StatePath joined it into a path. This is the regression that
// motivated the validator, so it gets its own named test.
func TestValidateInstanceName_RejectsTheLiveCorruption(t *testing.T) {
	bad := "worktree-reconciler:/Users/example/workspaces/cog"
	if err := ValidateInstanceName(bad); err == nil {
		t.Fatalf("ValidateInstanceName(%q) = nil, want error", bad)
	}
	// And prove why it mattered: unvalidated, this key escapes its own
	// directory level inside the config tree.
	p := StatePath("/ws", bad)
	if !strings.Contains(p, "/Users/example") {
		t.Fatalf("expected the raw path to leak into StatePath, got %q", p)
	}
}

func TestValidateInstanceName(t *testing.T) {
	valid := []string{
		"agent",
		"component",
		"lms-model-state/lmstudio-eclipse",
		"lineage-projection-bibliography",
		"worktree-reconciler/cog-1a2b3c4d",
		"mlx-supervised/ornith_35b",
	}
	for _, name := range valid {
		if err := ValidateInstanceName(name); err != nil {
			t.Errorf("ValidateInstanceName(%q) = %v, want nil", name, err)
		}
	}

	invalid := map[string]string{
		"":                  "empty",
		"/absolute":         "absolute path",
		"../escape":         "parent traversal",
		"a/../../b":         "traversal via middle segment",
		"a/b/c":             "too many segments",
		"a//b":              "empty segment",
		"has:colon":         "colon is not portable to Windows",
		`back\slash`:        "backslash separator",
		"star*":             "glob char illegal on Windows",
		"quo\"te":           "quote illegal on Windows",
		"pipe|d":            "pipe illegal on Windows",
		".":                 "dot segment",
		"..":                "dotdot segment",
		"trail/":            "trailing empty segment",
		"\\\\server\\share": "UNC path",
	}
	for name, why := range invalid {
		if err := ValidateInstanceName(name); err == nil {
			t.Errorf("ValidateInstanceName(%q) = nil, want error (%s)", name, why)
		}
	}
}

// TestValidateInstanceName_StatePathStaysLocal is the property the validator
// exists to guarantee: any accepted key resolves inside the config directory.
func TestValidateInstanceName_StatePathStaysLocal(t *testing.T) {
	root := filepath.FromSlash("/ws")
	base := filepath.Join(root, ".cog", "config")
	for _, name := range []string{"agent", "lms-model-state/lmstudio-eclipse", "worktree-reconciler/cog-1a2b3c4d"} {
		if err := ValidateInstanceName(name); err != nil {
			t.Fatalf("precondition: %q should be valid: %v", name, err)
		}
		rel, err := filepath.Rel(base, StatePath(root, name))
		if err != nil {
			t.Fatalf("Rel(%q): %v", name, err)
		}
		if strings.HasPrefix(rel, "..") {
			t.Errorf("StatePath for %q escaped the config dir: rel=%q", name, rel)
		}
	}
}

func TestUpsertProviderRefusesUnsafeName(t *testing.T) {
	ResetProviders()
	defer ResetProviders()

	UpsertProvider("worktree-reconciler:/Users/x/repo", nil)
	if HasProvider("worktree-reconciler:/Users/x/repo") {
		t.Fatal("UpsertProvider registered a provider with an unsafe name")
	}
	if got := len(ListProviders()); got != 0 {
		t.Fatalf("registry has %d providers after refused upsert, want 0", got)
	}
}

func TestUnregisterProvider(t *testing.T) {
	ResetProviders()
	defer ResetProviders()

	if UnregisterProvider("absent") {
		t.Error("UnregisterProvider(absent) = true, want false")
	}

	UpsertProvider("agent", nil)
	if !HasProvider("agent") {
		t.Fatal("precondition: agent should be registered")
	}
	if !UnregisterProvider("agent") {
		t.Error("UnregisterProvider(agent) = false, want true")
	}
	if HasProvider("agent") {
		t.Error("agent still registered after UnregisterProvider")
	}
	if UnregisterProvider("agent") {
		t.Error("second UnregisterProvider(agent) = true, want false")
	}
}
