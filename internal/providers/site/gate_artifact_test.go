package site

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGateArtifactBlocksBuildTimeLeak is the regression for the deploy-target
// exposure found 2026-09-01.
//
// The five myrgic.* repos are public Pages deploy targets, force-pushed
// wholesale by Deploy(). They were exempted from CI governance as
// "machine-managed" — but Deploy() runs `bash build.sh` on the OPERATOR's
// machine and publishes whatever it produces. A build script that embeds
// $HOME, `pwd`, or tool output injects machine-local data that never existed
// in the `sites` repo, so a gate on the source cannot see it, and CI on the
// target cannot stop it (Pages serves `main` the instant the push lands).
//
// This test proves the gate catches content that appears only in the built
// artifact.
func TestGateArtifactBlocksBuildTimeLeak(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)

	artifact := t.TempDir()
	// Simulate a build script that interpolated the operator's home directory
	// into generated output — the exact mod3 #146 shape, in a file no reviewer
	// reads as configuration.
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"),
		[]byte(`<!-- built from /Users/slowbro/workspaces/myrgic/sites -->`), 0o644))

	err := gateArtifact(context.Background(), artifact)
	if err == nil {
		t.Fatal("gate ALLOWED an artifact containing the operator home path; " +
			"a build-time leak would publish to a public Pages repo")
	}
	if !strings.Contains(err.Error(), "BLOCKED") {
		t.Fatalf("expected a BLOCKED verdict, got: %v", err)
	}

	// The staged policy file must not survive into the published tree.
	if _, statErr := os.Stat(filepath.Join(artifact, ".cogpublic")); statErr == nil {
		t.Error("gate left .cogpublic in the artifact; it would be published")
	}
}

// TestGateArtifactAllowsCleanArtifact is the positive control. A gate that has
// only ever blocked is as untrustworthy as one that has only ever passed — it
// could be refusing everything.
func TestGateArtifactAllowsCleanArtifact(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)

	artifact := t.TempDir()
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"),
		[]byte("<!doctype html><title>myrgic</title>"), 0o644))
	must(t, os.WriteFile(filepath.Join(artifact, "CNAME"),
		[]byte("myrgic.com"), 0o644))

	if err := gateArtifact(context.Background(), artifact); err != nil {
		t.Fatalf("gate BLOCKED a clean artifact: %v", err)
	}
}

// TestGateArtifactFailsClosed: every inability to run must abort the deploy.
// A check that cannot run must never be mistaken for a check that passed —
// the whole lesson of the .cogpublic that declared guards nothing executed.
func TestGateArtifactFailsClosed(t *testing.T) {
	t.Setenv("COGOS_REPO_ROOT", t.TempDir()) // no guard, no policy

	artifact := t.TempDir()
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"), []byte("clean"), 0o644))

	if err := gateArtifact(context.Background(), artifact); err == nil {
		t.Fatal("gate PASSED with no guard installed; it must fail closed")
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, ".cogpublic")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("no .cogpublic above the test working directory")
		}
		dir = parent
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
